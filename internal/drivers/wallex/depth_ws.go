// Package wallex provides WebSocket depth data scraping for Wallex exchange.
//
// # Collection Strategy
//
// The scraper maintains a continuous WebSocket connection but only sends
// orderbook snapshots to Kafka at 1-minute intervals (e.g., 12:00, 12:01, 12:02).
//
// How it works:
//   - Stays connected to WebSocket continuously
//   - Receives and stores the latest depth data for each symbol
//   - At each minute mark, sends the current state as a snapshot
//   - Ignores intermediate updates between minute marks
//
// # WebSocket Channels
//
// Wallex uses separate channels for buy and sell depth:
//   - {symbol}@buyDepth: Buy-side orderbook updates
//   - {symbol}@sellDepth: Sell-side orderbook updates
//
// Both sides are aggregated into a single snapshot per symbol per minute.
package wallex

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	pb "nobitex/radar/internal/proto"
	"nobitex/radar/internal/scraper"

	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

// snapshotInterval defines how often to send snapshots (every 1 minute)
const snapshotInterval = 1 * time.Minute

// symbolDepth holds the latest depth data for a single symbol.
// Updated continuously but only sent at minute intervals.
type symbolDepth struct {
	bids       []*pb.OrderLevel
	asks       []*pb.OrderLevel
	lastUpdate time.Time
}

// WallexDepthWS handles WebSocket-based depth data collection from Wallex.
// It maintains a continuous connection but only sends snapshots every minute.
type WallexDepthWS struct {
	sender *scraper.Sender
	logger *slog.Logger

	// depthStore holds the latest depth data for each symbol
	// Key: normalized symbol (e.g., "BTC/TMN")
	depthStore map[string]*symbolDepth
	mu         sync.RWMutex

	// lastSnapshotMinute tracks the last minute we sent snapshots
	// to ensure we only send once per minute
	lastSnapshotMinute int
}

func NewWallexDepthScraper(kafkaWriter *kafka.Writer, logger *slog.Logger) *WallexDepthWS {
	return &WallexDepthWS{
		sender:             scraper.NewSender(kafkaWriter, logger),
		logger:             logger.With("scraper", "wallex-depth-ws"),
		depthStore:         make(map[string]*symbolDepth),
		lastSnapshotMinute: -1,
	}
}

// Name returns the scraper identifier used for logging and metrics.
func (w *WallexDepthWS) Name() string { return "wallex-depth" }

// Run starts the depth data collection with 1-minute snapshots.
//
// The scraper:
//  1. Connects to WebSocket and stays connected
//  2. Continuously receives and stores depth updates
//  3. Sends snapshots to Kafka only at minute boundaries (12:00, 12:01, etc.)
//
// The method blocks until the context is cancelled.
func (w *WallexDepthWS) Run(ctx context.Context) error {
	w.logger.Info("starting Wallex depth WebSocket scraper",
		"snapshot_interval", snapshotInterval)

	// Fetch available markets
	markets, err := fetchMarkets()
	if err != nil {
		return err
	}
	if len(markets) == 0 {
		return fmt.Errorf("no markets found")
	}

	// Chunk markets to respect connection limits
	chunks := scraper.ChunkSlice(markets, maxSymbolsPerConnDepth)

	// Run WebSocket workers - they stay connected continuously
	scraper.RunWorkers(ctx, chunks, "wallex-depth", func() *scraper.WSClient {
		return w.createClient()
	}, w.logger)

	return nil
}

// createClient creates a WebSocket client with minute-interval snapshot handler.
func (w *WallexDepthWS) createClient() *scraper.WSClient {
	config := scraper.WSConfig{URL: wsURL, PingDisabled: true}
	handler := scraper.WSHandler{
		OnSubscribe: w.onSubscribe,
		OnMessage:   w.onMessage,
	}
	return scraper.NewWSClient(config, handler, w.sender, w.logger)
}

// onSubscribe subscribes to both buyDepth and sellDepth channels for all symbols.
func (w *WallexDepthWS) onSubscribe(conn *websocket.Conn, symbols []string) error {
	for _, sym := range symbols {
		// Subscribe to sell depth channel
		msg := []any{"subscribe", map[string]string{"channel": sym + "@sellDepth"}}
		if err := conn.WriteJSON(msg); err != nil {
			return err
		}

		// Subscribe to buy depth channel
		msg = []any{"subscribe", map[string]string{"channel": sym + "@buyDepth"}}
		if err := conn.WriteJSON(msg); err != nil {
			return err
		}
	}
	return nil
}

// onMessage handles incoming WebSocket messages.
// It updates the depth store and sends snapshots at minute boundaries.
func (w *WallexDepthWS) onMessage(conn *websocket.Conn, message []byte) ([]byte, error) {
	// Skip connection acknowledgment messages
	msgStr := strings.TrimSpace(string(message))
	if strings.Contains(msgStr, `"sid":`) {
		return nil, nil
	}

	var raw []json.RawMessage
	if json.Unmarshal(message, &raw) != nil || len(raw) < 2 {
		return nil, nil
	}

	var channel string
	if err := json.Unmarshal(raw[0], &channel); err != nil {
		return nil, nil
	}

	var orders []map[string]float64
	if err := json.Unmarshal(raw[1], &orders); err != nil {
		return nil, nil
	}

	// Parse channel: {symbol}@{buyDepth|sellDepth}
	parts := strings.Split(channel, "@")
	if len(parts) != 2 {
		return nil, nil
	}

	symbol := scraper.NormalizeSymbol("wallex", parts[0])
	isSellSide := strings.Contains(parts[1], "sell")

	// Convert orders to OrderLevel
	levels := make([]*pb.OrderLevel, 0, len(orders))
	for _, order := range orders {
		levels = append(levels, &pb.OrderLevel{
			Price:  order["price"],
			Volume: order["quantity"],
		})
	}

	// Update depth store
	w.updateDepthStore(symbol, levels, isSellSide)

	// Check if we should send snapshots (at minute boundary)
	now := time.Now()
	currentMinute := now.Minute()

	w.mu.Lock()
	shouldSend := currentMinute != w.lastSnapshotMinute
	if shouldSend {
		w.lastSnapshotMinute = currentMinute
	}
	w.mu.Unlock()

	// Send all snapshots at minute boundary
	if shouldSend {
		w.sendMinuteSnapshots(now)
	}

	// Don't return data here - we handle sending in sendMinuteSnapshots
	return nil, nil
}

// updateDepthStore updates the stored depth data for a symbol.
func (w *WallexDepthWS) updateDepthStore(symbol string, levels []*pb.OrderLevel, isSellSide bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, exists := w.depthStore[symbol]; !exists {
		w.depthStore[symbol] = &symbolDepth{}
	}

	if isSellSide {
		w.depthStore[symbol].bids = levels
	} else {
		w.depthStore[symbol].asks = levels
	}
	w.depthStore[symbol].lastUpdate = time.Now().UTC()
}

// sendMinuteSnapshots sends all current depth snapshots to Kafka.
// Called once per minute at the minute boundary.
func (w *WallexDepthWS) sendMinuteSnapshots(snapshotTime time.Time) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	// Convert to UTC and truncate to minute boundary (e.g., 12:05:00 UTC)
	minuteTime := snapshotTime.UTC().Truncate(time.Minute)
	timeStr := minuteTime.Format(time.RFC3339)

	sentCount := 0
	skippedCount := 0

	for symbol, depth := range w.depthStore {
		// Only send if we have both sides
		if len(depth.bids) == 0 && len(depth.asks) == 0 {
			skippedCount++
			continue
		}

		snapshot := &pb.OrderBookSnapshot{
			Id:         scraper.GenerateSnapShotID("wallex", symbol, timeStr),
			Exchange:   "wallex",
			Symbol:     symbol,
			LastUpdate: timeStr,
			Bids:       depth.bids,
			Asks:       depth.asks,
		}

		// Serialize and send
		data, err := proto.Marshal(snapshot)
		if err != nil {
			w.logger.Error("failed to marshal snapshot", "symbol", symbol, "error", err)
			continue
		}

		if err := w.sender.Send(context.Background(), data); err != nil {
			w.logger.Error("failed to send snapshot", "symbol", symbol, "error", err)
			continue
		}

		sentCount++
	}
}
