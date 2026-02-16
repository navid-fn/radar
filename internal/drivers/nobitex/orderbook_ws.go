// Package nobitex provides WebSocket depth data scraping for Nobitex exchange.
//
// # Depth WebSocket Response Format
//
//	{
//	  "asks": [
//	    ["35077909990", "0.009433"],
//	    ["35078000000", "0.000274"],
//	    ["35078009660", "0.00057"]
//	  ],
//	  "bids": [
//	    ["35020080080", "0.185784"],
//	    ["35020070060", "0.086916"],
//	    ["35020030010", "0.000071"]
//	  ],
//	  "lastTradePrice": "35077909990",
//	  "lastUpdate": 1726581829816
//	}
//
// # Collection Strategy
//
// The scraper maintains a continuous WebSocket connection but only sends
// orderbook snapshots to Kafka at 5-minute intervals (e.g., 12:00, 12:05, 12:10).
//
// How it works:
//   - Stays connected to WebSocket continuously
//   - Receives and stores the latest depth data for each symbol
//   - At each 5-minute mark, sends the current state as a snapshot
//   - Ignores intermediate updates between interval marks
package nobitex

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "nobitex/radar/internal/proto"
	"nobitex/radar/internal/scraper"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

// snapshotInterval defines how often to send snapshots
// TODO: we can use config for reading/changing this interval later
const snapshotInterval = 5 * time.Minute

// NobitexDepthWS handles WebSocket-based depth data collection from Nobitex.
// It maintains a continuous connection but only sends snapshots every minute.
type NobitexDepthWS struct {
	sender *scraper.Sender
	logger *slog.Logger

	// depthStore holds the latest depth data for each symbol
	// Key: normalized symbol (e.g., "BTC/IRT")
	depthStore map[string]*pb.OrderBookSnapshot
	mu         sync.RWMutex

	// lastSnapshotTime tracks the last interval we sent snapshots
	// to ensure we only send once per snapshotInterval
	lastSnapshotTime time.Time
}

func NewNobitexOrderbookScraper(writer scraper.MessageWriter, logger *slog.Logger) *NobitexDepthWS {
	return &NobitexDepthWS{
		sender:     scraper.NewSender(writer, logger),
		logger:     logger.With("scraper", "nobitex-orderbook-ws"),
		depthStore: make(map[string]*pb.OrderBookSnapshot),
	}
}

func NewNobitexDepthScraper(writer scraper.MessageWriter, logger *slog.Logger) *NobitexDepthWS {
	return NewNobitexOrderbookScraper(writer, logger)
}

// Name returns the scraper identifier used for logging and metrics.
func (n *NobitexDepthWS) Name() string { return "nobitex-orderbook" }

// Run starts the depth data collection with 5-minute snapshots.
//
// The scraper:
//  1. Connects to WebSocket and stays connected
//  2. Continuously receives and stores depth updates
//  3. Sends snapshots to Kafka only at 5-minute boundaries (12:00, 12:05, etc.)
//
// The method blocks until the context is cancelled.
func (n *NobitexDepthWS) Run(ctx context.Context) error {
	n.logger.Info("starting Nobitex depth WebSocket scraper",
		"snapshot_interval", snapshotInterval)

	// Fetch available markets
	markets, err := fetchMarkets()
	if err != nil {
		return fmt.Errorf("fetch markets: %w", err)
	}
	if len(markets) == 0 {
		return fmt.Errorf("no markets found")
	}

	// Chunk markets to respect connection limits
	chunks := scraper.ChunkSlice(markets, maxSymbolsPerConn)

	// Run WebSocket workers - they stay connected continuously
	scraper.RunWorkers(ctx, chunks, "nobitex-depth", func() *scraper.WSClient {
		return n.createClient()
	}, n.logger)

	return nil
}

// createClient creates a WebSocket client with minute-interval snapshot handler.
func (n *NobitexDepthWS) createClient() *scraper.WSClient {
	config := scraper.WSConfig{URL: wsURL, PingDisabled: true}
	handler := scraper.WSHandler{
		OnConnect:   n.onConnect,
		OnSubscribe: n.onSubscribe,
		OnMessage:   n.onMessage,
	}
	return scraper.NewWSClient(config, handler, n.sender, n.logger)
}

func (n *NobitexDepthWS) onConnect(conn *websocket.Conn) error {
	return conn.WriteJSON(map[string]any{"id": 1, "connect": map[string]any{}})
}

func (n *NobitexDepthWS) onSubscribe(conn *websocket.Conn, symbols []string) error {
	for _, sym := range symbols {
		msg := map[string]any{"id": 2, "subscribe": map[string]any{"channel": depthChannelName + sym}}
		if err := conn.WriteJSON(msg); err != nil {
			return err
		}
	}
	return nil
}

// onMessage handles incoming WebSocket messages.
// It updates the depth store and sends snapshots at minute boundaries.
func (n *NobitexDepthWS) onMessage(conn *websocket.Conn, message []byte) ([]proto.Message, error) {
	// Parse and store the depth data
	scanner := bufio.NewScanner(bytes.NewReader(message))
	for scanner.Scan() {
		if snapshot := n.parseLine(conn, scanner.Bytes()); snapshot != nil {
			// Store the latest snapshot for this symbol
			n.mu.Lock()
			n.depthStore[snapshot.Symbol] = snapshot
			n.mu.Unlock()
		}
	}

	// Check if we should send snapshots (at snapshotInterval boundary)
	now := time.Now()
	currentInterval := now.Truncate(snapshotInterval)

	n.mu.Lock()
	shouldSend := !currentInterval.Equal(n.lastSnapshotTime)
	if shouldSend {
		n.lastSnapshotTime = currentInterval
	}
	n.mu.Unlock()

	// Send snapshots at interval boundary
	if shouldSend {
		n.sendMinuteSnapshots(now)
	}

	// Don't return data here - we handle sending in sendMinuteSnapshots
	return nil, nil
}

// sendMinuteSnapshots sends all current depth snapshots to Kafka.
// Called once per snapshotInterval at the interval boundary.
func (n *NobitexDepthWS) sendMinuteSnapshots(snapshotTime time.Time) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	// Convert to UTC and truncate to snapshotInterval boundary
	intervalTime := snapshotTime.UTC().Truncate(snapshotInterval)
	timeStr := intervalTime.Format(time.RFC3339)

	sentCount := 0

	for symbol, snapshot := range n.depthStore {
		// Update the snapshot with minute-boundary timestamp
		snapshotToSend := &pb.OrderBookSnapshot{
			Id:         scraper.GenerateSnapShotID("nobitex", symbol, timeStr),
			Exchange:   "nobitex",
			Symbol:     symbol,
			LastUpdate: timeStr,
			Bids:       snapshot.Bids,
			Asks:       snapshot.Asks,
		}

		if err := n.sender.SendOrderBookSnapShot(context.Background(), snapshotToSend); err != nil {
			// TODO: add metric
			n.logger.Error("failed to send snapshot", "symbol", symbol, "error", err)
			continue
		}

		sentCount++
	}
}

func (n *NobitexDepthWS) parseLine(conn *websocket.Conn, line []byte) *pb.OrderBookSnapshot {
	if len(line) == 0 {
		return nil
	}

	var msg map[string]any
	if json.Unmarshal(line, &msg) != nil {
		return nil
	}

	if len(msg) == 0 {
		conn.WriteJSON(map[string]any{})
		return nil
	}
	if _, ok := msg["ping"]; ok {
		conn.WriteJSON(map[string]any{"pong": map[string]any{}})
		return nil
	}
	if _, ok := msg["connect"]; ok {
		return nil
	}
	if _, ok := msg["subscribe"]; ok {
		return nil
	}

	push, _ := msg["push"].(map[string]any)
	if push == nil {
		return nil
	}

	pub, _ := push["pub"].(map[string]any)
	if pub == nil {
		return nil
	}

	dataMap, _ := pub["data"].(map[string]any)
	if dataMap == nil {
		return nil
	}

	jsonBytes, err := json.Marshal(dataMap)
	if err != nil {
		n.logger.Debug("marshaling json", "error", err)
		return nil
	}

	var data depthResponse
	json.Unmarshal(jsonBytes, &data)

	channel, _ := push["channel"].(string)

	var symbol string
	_, symbol, find := strings.Cut(channel, depthChannelName)

	if !find {
		return nil
	}

	return n.createDepth(data, symbol)
}

func (n *NobitexDepthWS) createDepth(data depthResponse, symbol string) *pb.OrderBookSnapshot {
	lastUpdateInt := data.LastUpdate.(float64)
	lastUpdate := scraper.TimestampToRFC3339(int64(lastUpdateInt))
	cleanedSymbol := scraper.NormalizeSymbol("nobitex", symbol)

	asks := []*pb.OrderLevel{}
	for _, a := range data.Asks {
		price, _ := strconv.ParseFloat(a[0], 64)
		if price == 0 {
			continue
		}
		volume, _ := strconv.ParseFloat(a[1], 64)
		if volume == 0 {
			continue
		}

		asks = append(asks, &pb.OrderLevel{Price: price, Volume: volume})
	}

	bids := []*pb.OrderLevel{}
	for _, b := range data.Bids {
		price, _ := strconv.ParseFloat(b[0], 64)
		if price == 0 {
			continue
		}
		volume, _ := strconv.ParseFloat(b[1], 64)
		if volume == 0 {
			continue
		}
		bids = append(asks, &pb.OrderLevel{Price: scraper.NormalizePrice(cleanedSymbol, price), Volume: volume})
	}
	return &pb.OrderBookSnapshot{
		Id:         scraper.GenerateSnapShotID("nobitex", cleanedSymbol, lastUpdate),
		LastUpdate: lastUpdate,
		Bids:       bids,
		Asks:       asks,
		Symbol:     cleanedSymbol,
		Exchange:   "nobitex",
	}
}
