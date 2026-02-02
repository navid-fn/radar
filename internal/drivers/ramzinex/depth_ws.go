//doc
// response example

// {
// 'push': {
// 'channel': 'orderbook:11',
//
//		'pub': {
//			'data': '{"buys": [["35077909990", "0.009433"], ["35078000000", "0.000274"], ["35078009660", "0.00057"]],
//								"sells": [["35020080080", "0.185784"], ["35020070060", "0.086916"], ["35020030010", "0.000071"]],
//								"lastTradePrice": "35077909990",
//								"lastUpdate": 1726581829816}',
//
//				'offset': 49890
//	}}}
package ramzinex

import (
	"bufio"
	"bytes"
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
// TODO: we can use config for reading/changing this interval later
const snapshotInterval = 1 * time.Minute

type RamzinexDepthWS struct {
	sender       *scraper.Sender
	logger       *slog.Logger
	pairIDToName map[int]string

	// depthStore holds the latest depth data for each symbol
	// Key: normalized symbol (e.g., "BTC/IRT")
	depthStore map[string]*pb.OrderBookSnapshot
	mu         sync.RWMutex

	// lastSnapshotMinute tracks the last minute we sent snapshots
	// to ensure we only send once per minute
	lastSnapshotMinute int
}

func NewRamzinexDepthScraper(kafkaWriter *kafka.Writer, logger *slog.Logger) *RamzinexDepthWS {
	return &RamzinexDepthWS{
		sender:       scraper.NewSender(kafkaWriter, logger),
		logger:       logger.With("scraper", "ramzinex-depth-ws"),
		pairIDToName: make(map[int]string),

		depthStore: make(map[string]*pb.OrderBookSnapshot),

		// when DepthWs start, we dont check minute, we store snapshot then store minute
		// then update it for get next snapshot for next minute
		lastSnapshotMinute: -1,
	}
}

func (r *RamzinexDepthWS) Name() string { return "ramzinex-depth-ws" }

func (r *RamzinexDepthWS) fetchPairs() ([]pairDetail, error) {
	pairs, pairMap, err := fetchPairs()
	if err != nil {
		return nil, err
	}
	r.pairIDToName = pairMap
	return pairs, nil
}

func (r *RamzinexDepthWS) Run(ctx context.Context) error {
	r.logger.Info("starting Nobitex depth WebSocket scraper",
		"snapshot_interval", snapshotInterval)
	pairs, err := r.fetchPairs()
	if err != nil {
		return err
	}
	if len(pairs) == 0 {
		return fmt.Errorf("no pairs found")
	}

	var chunks [][]string
	var currentChunk []string
	for _, p := range pairs {
		currentChunk = append(currentChunk, fmt.Sprintf("%d", p.ID))
		if len(currentChunk) >= maxSymbolsPerConn {
			chunks = append(chunks, currentChunk)
			currentChunk = nil
		}
	}
	if len(currentChunk) > 0 {
		chunks = append(chunks, currentChunk)
	}

	scraper.RunWorkers(ctx, chunks, "ramzinex", func() *scraper.WSClient {
		return r.createClient()
	}, r.logger)

	return nil
}

func (r *RamzinexDepthWS) createClient() *scraper.WSClient {
	config := scraper.WSConfig{URL: wsURL, PingDisabled: true}
	handler := scraper.WSHandler{
		OnConnect:   r.onConnect,
		OnSubscribe: r.onSubscribe,
		OnMessage:   r.onMessage,
	}
	return scraper.NewWSClient(config, handler, r.sender, r.logger)
}

func (r *RamzinexDepthWS) onConnect(conn *websocket.Conn) error {
	msg := map[string]any{"connect": map[string]string{"name": "js"}, "id": 1}
	if err := conn.WriteJSON(msg); err != nil {
		return err
	}
	time.Sleep(500 * time.Millisecond)
	return nil
}

func (r *RamzinexDepthWS) onSubscribe(conn *websocket.Conn, pairIDs []string) error {
	for i, pairID := range pairIDs {
		msg := map[string]any{
			"id":        i + 2,
			"subscribe": map[string]any{"channel": "orderbook:" + pairID, "recover": false},
		}
		if err := conn.WriteJSON(msg); err != nil {
			return err
		}
		time.Sleep(200 * time.Millisecond)
	}
	return nil
}

// onMessage handles incoming WebSocket messages.
// It updates the depth store and sends snapshots at minute boundaries.
func (r *RamzinexDepthWS) onMessage(conn *websocket.Conn, message []byte) ([]byte, error) {
	// Parse and store the depth data
	scanner := bufio.NewScanner(bytes.NewReader(message))
	for scanner.Scan() {
		if snapshot := r.parseLine(conn, scanner.Bytes()); snapshot != nil {
			// Store the latest snapshot for this symbol
			r.mu.Lock()
			r.depthStore[snapshot.Symbol] = snapshot
			r.mu.Unlock()
		}
	}

	// Check if we should send snapshots (at minute boundary)
	now := time.Now()
	currentMinute := now.Minute()

	r.mu.Lock()
	shouldSend := currentMinute != r.lastSnapshotMinute
	if shouldSend {
		r.lastSnapshotMinute = currentMinute
	}
	r.mu.Unlock()

	// Send all snapshots at minute boundary
	if shouldSend {
		r.sendMinuteSnapshots(now)
	}

	// Don't return data here - we handle sending in sendMinuteSnapshots
	return nil, nil
}

// sendMinuteSnapshots sends all current depth snapshots to Kafka.
// Called once per minute at the minute boundary.
func (r *RamzinexDepthWS) sendMinuteSnapshots(snapshotTime time.Time) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Convert to UTC and truncate to minute boundary (e.g., 12:05:00 UTC)
	minuteTime := snapshotTime.UTC().Truncate(time.Minute)
	timeStr := minuteTime.Format(time.RFC3339)

	sentCount := 0

	for symbol, snapshot := range r.depthStore {
		// Update the snapshot with minute-boundary timestamp
		snapshotToSend := &pb.OrderBookSnapshot{
			Id:         scraper.GenerateSnapShotID("ramzinex", symbol, timeStr),
			Exchange:   "ramzinex",
			Symbol:     symbol,
			LastUpdate: timeStr,
			Bids:       snapshot.Bids,
			Asks:       snapshot.Asks,
		}

		// Serialize and send
		data, err := proto.Marshal(snapshotToSend)
		if err != nil {
			r.logger.Error("Failed to marshal snapshot", "symbol", symbol, "error", err)
			continue
		}

		if err := r.sender.Send(context.Background(), data); err != nil {
			// TODO: add metric
			r.logger.Error("failed to send snapshot", "symbol", symbol, "error", err)
			continue
		}

		sentCount++
	}

}
func (r *RamzinexDepthWS) parseLine(conn *websocket.Conn, message []byte) *pb.OrderBookSnapshot {
	msgStr := strings.TrimSpace(string(message))
	if msgStr == "{}" || msgStr == "{}\n" {
		conn.WriteJSON(map[string]any{})
		return nil
	}

	var msg map[string]any
	if json.Unmarshal(message, &msg) != nil {
		return nil
	}
	if len(msg) == 0 {
		conn.WriteJSON(map[string]any{})
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
		fmt.Println(err)
		return nil
	}

	var data depthResponse
	json.Unmarshal(jsonBytes, &data)

	channel, _ := push["channel"].(string)
	parts := strings.Split(channel, ":")
	if len(parts) != 2 {
		return nil
	}

	var pairID int
	fmt.Sscanf(parts[1], "%d", &pairID)
	pairName, ok := r.pairIDToName[pairID]
	if !ok {
		return nil
	}

	return r.createDepth(data, strings.ToUpper(pairName))
}

func (r *RamzinexDepthWS) createDepth(data depthResponse, symbol string) *pb.OrderBookSnapshot {
	cleanedSymbol := scraper.NormalizeSymbol("ramzinex", symbol)

	asks := []*pb.OrderLevel{}
	for _, a := range data.Asks {
		price, _ := a[0].(float64)
		if price == 0 {
			continue
		}
		volume, _ := a[0].(float64)
		if volume == 0 {
			continue
		}

		asks = append(asks, &pb.OrderLevel{Price: scraper.NormalizePrice(cleanedSymbol, price), Volume: volume})
	}

	bids := []*pb.OrderLevel{}
	for _, b := range data.Bids {
		price, _ := b[0].(float64)
		if price == 0 {
			continue
		}
		volume, _ := b[0].(float64)
		if volume == 0 {
			continue
		}
		bids = append(asks, &pb.OrderLevel{Price: scraper.NormalizePrice(cleanedSymbol, price), Volume: volume})
	}

	lastUpdate := time.Now().UTC().Format(time.RFC3339)

	return &pb.OrderBookSnapshot{
		Id:         scraper.GenerateSnapShotID("ramzinex", cleanedSymbol, lastUpdate),
		LastUpdate: lastUpdate,
		Bids:       bids,
		Asks:       asks,
		Symbol:     cleanedSymbol,
		Exchange:   "ramzinex",
	}
}
