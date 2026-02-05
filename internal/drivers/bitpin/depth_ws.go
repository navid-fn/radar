// doc: https://docs.bitpin.ir/v1/docs/websocket-beta/orderbook
// sample data return
//
//	{
//	  "asks": [["<price>", "<amount>"]],
//	  "bids": [["<price>", "<amount>"]],
//	  "volume_ask": "<decimal>",
//	  "volume_bid": "<decimal>",
//	  "price": "<decimal>",
//	  "symbol": "<string>",
//	  "event": "market_data",
//	  "event_time": "%Y-%m-%dT%H:%M:%S.%fZ"
//	}
//
//	{
//	  "asks": [["51000", "2000.00000000"]],
//	  "bids": [["50000", "1000.00000000"]],
//	  "volume_ask": "2000.0",
//	  "volume_bid": "1000.0",
//	  "price": "50000.0",
//	  "symbol": "BTC_IRT",
//	  "event_time": "2024-08-14T14:37:54.487062Z",
//	  "event": "market_data"
//	}
package bitpin

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	pb "nobitex/radar/internal/proto"
	"nobitex/radar/internal/scraper"

	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

// snapshotInterval defines how often to send snapshots (every 1 minute)
// TODO: we can use config for reading/changing this interval later
const snapshotInterval = 1 * time.Minute

type BitpinDepthWS struct {
	sender     *scraper.Sender
	logger     *slog.Logger
	depthStore map[string]*pb.OrderBookSnapshot
	mu         sync.RWMutex

	// lastSnapshotMinute tracks the last minute we sent snapshots
	// to ensure we only send once per minute
	lastSnapshotMinute int
}

func NewBitpinWsDepthScraper(kafkaWriter *kafka.Writer, logger *slog.Logger) *BitpinDepthWS {
	return &BitpinDepthWS{
		sender:     scraper.NewSender(kafkaWriter, logger),
		logger:     logger.With("scraper", "bitpin-ws-depth"),
		depthStore: make(map[string]*pb.OrderBookSnapshot),

		// when DepthWs start, we dont check minute, we store snapshot then store minute
		// then update it for get next snapshot for next minute
		lastSnapshotMinute: -1,
	}
}

func (b *BitpinDepthWS) Name() string { return "bitpin-ws-depth" }

// Run starts the depth data collection with 1-minute snapshots.
//
// The scraper:
//  1. Connects to WebSocket and stays connected
//  2. Continuously receives and stores depth updates
//  3. Sends snapshots to Kafka only at minute boundaries (12:00, 12:01, etc.)
//
// The method blocks until the context is cancelled.
func (b *BitpinDepthWS) Run(ctx context.Context) error {
	b.logger.Info("starting Bitpin depth WebSocket scraper",
		"snapshot_interval", snapshotInterval)
	markets, err := fetchMarkets(b.logger)
	if err != nil {
		return err
	}
	if len(markets) == 0 {
		return fmt.Errorf("no markets found")
	}

	chunks := scraper.ChunkSlice(markets, maxSymbolsPerConn)
	scraper.RunWorkers(ctx, chunks, "bitpin", func() *scraper.WSClient {
		return b.createClient()
	}, b.logger)

	return nil
}

func (b *BitpinDepthWS) createClient() *scraper.WSClient {
	config := scraper.WSConfig{URL: wsURL, PingDisabled: true}
	handler := scraper.WSHandler{
		OnConnect:   b.onConnect,
		OnSubscribe: b.onSubscribe,
		OnMessage:   b.onMessage,
	}
	return scraper.NewWSClient(config, handler, b.sender, b.logger)
}

func (b *BitpinDepthWS) onConnect(conn *websocket.Conn) error {
	return conn.WriteJSON(map[string]any{"id": 1, "connect": map[string]any{}})
}

func (b *BitpinDepthWS) onSubscribe(conn *websocket.Conn, symbols []string) error {
	for i, sym := range symbols {
		msg := map[string]any{
			"id":        i + 2,
			"subscribe": map[string]any{"channel": "orderbook:" + sym},
		}
		if err := conn.WriteJSON(msg); err != nil {
			return err
		}
	}
	return nil
}

// onMessage handles incoming WebSocket messages.
// It updates the depth store and sends snapshots at minute boundaries.
func (b *BitpinDepthWS) onMessage(conn *websocket.Conn, message []byte) ([]byte, error) {
	// Parse and store the depth data
	scanner := bufio.NewScanner(bytes.NewReader(message))
	for scanner.Scan() {
		if snapshot := b.parseLine(conn, scanner.Bytes()); snapshot != nil {
			// Store the latest snapshot for this symbol
			b.mu.Lock()
			b.depthStore[snapshot.Symbol] = snapshot
			b.mu.Unlock()
		}
	}

	// Check if we should send snapshots (at minute boundary)
	now := time.Now()
	currentMinute := now.Minute()

	b.mu.Lock()
	shouldSend := currentMinute != b.lastSnapshotMinute
	if shouldSend {
		b.lastSnapshotMinute = currentMinute
	}
	b.mu.Unlock()

	// Send all snapshots at minute boundary
	if shouldSend {
		b.sendMinuteSnapshots(now)
	}

	// Don't return data here - we handle sending in sendMinuteSnapshots
	return nil, nil
}

// sendMinuteSnapshots sends all current depth snapshots to Kafka.
// Called once per minute at the minute boundary.
func (b *BitpinDepthWS) sendMinuteSnapshots(snapshotTime time.Time) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Convert to UTC and truncate to minute boundary (e.g., 12:05:00 UTC)
	minuteTime := snapshotTime.UTC().Truncate(time.Minute)
	timeStr := minuteTime.Format(time.RFC3339)

	sentCount := 0

	for symbol, snapshot := range b.depthStore {
		// Update the snapshot with minute-boundary timestamp
		snapshotToSend := &pb.OrderBookSnapshot{
			Id:         scraper.GenerateSnapShotID("bitpin", symbol, timeStr),
			Exchange:   "bitpin",
			Symbol:     symbol,
			LastUpdate: timeStr,
			Bids:       snapshot.Bids,
			Asks:       snapshot.Asks,
		}

		// Serialize and send
		data, err := proto.Marshal(snapshotToSend)
		if err != nil {
			b.logger.Error("Failed to marshal snapshot", "symbol", symbol, "error", err)
			continue
		}

		if err := b.sender.Send(context.Background(), data); err != nil {
			// TODO: add metric
			b.logger.Error("failed to send snapshot", "symbol", symbol, "error", err)
			continue
		}

		sentCount++
	}

}

func (b *BitpinDepthWS) parseLine(conn *websocket.Conn, message []byte) *pb.OrderBookSnapshot {
	var msg map[string]any
	if json.Unmarshal(message, &msg) != nil {
		return nil
	}

	if len(msg) == 0 || msg["ping"] != nil {
		conn.WriteJSON(map[string]any{})
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
		b.logger.Debug("marshaling json", "error", err)
		return nil
	}

	var data depthResponse
	json.Unmarshal(jsonBytes, &data)

	return b.createDepth(data)
}

func (b *BitpinDepthWS) createDepth(data depthResponse) *pb.OrderBookSnapshot {
	cleanedSymbol := scraper.NormalizeSymbol("bitpin", data.Symbol)

	asks := []*pb.OrderLevel{}
	for _, a := range data.Asks {
		price, _ := strconv.ParseFloat(a[0], 64)
		if price == 0 {
			continue
		}
		volume, _ := strconv.ParseFloat(a[0], 64)
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
		volume, _ := strconv.ParseFloat(b[0], 64)
		if volume == 0 {
			continue
		}
		bids = append(asks, &pb.OrderLevel{Price: price, Volume: volume})
	}
	return &pb.OrderBookSnapshot{
		Id:         scraper.GenerateSnapShotID("bitpin", cleanedSymbol, data.EventTime),
		LastUpdate: data.EventTime,
		Bids:       bids,
		Asks:       asks,
		Symbol:     cleanedSymbol,
		Exchange:   "bitpin",
	}
}
