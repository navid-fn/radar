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
	"google.golang.org/protobuf/proto"
)

// snapshotInterval defines how often to send snapshots
// TODO: we can use config for reading/changing this interval later
const snapshotInterval = 5 * time.Minute

type BitpinOrderbookWSScraper struct {
	sender     *scraper.Sender
	logger     *slog.Logger
	depthStore map[string]*pb.OrderBookSnapshot
	mu         sync.RWMutex

	// lastSnapshotTime tracks the last interval we sent snapshots
	// to ensure we only send once per snapshotInterval
	lastSnapshotTime time.Time
}

func NewBitpinOrderbookScraper(writer scraper.MessageWriter, logger *slog.Logger) *BitpinOrderbookWSScraper {
	return &BitpinOrderbookWSScraper{
		sender:     scraper.NewSender(writer, logger),
		logger:     logger.With("scraper", "bitpin-orderbook-ws"),
		depthStore: make(map[string]*pb.OrderBookSnapshot),
	}
}

func NewBitpinWsDepthScraper(writer scraper.MessageWriter, logger *slog.Logger) *BitpinOrderbookWSScraper {
	return NewBitpinOrderbookScraper(writer, logger)
}

func (b *BitpinOrderbookWSScraper) Name() string { return "bitpin-orderbook-ws" }

func (b *BitpinOrderbookWSScraper) Run(ctx context.Context) error {
	b.logger.Info("starting Bitpin depth WebSocket scraper",
		"snapshot_interval", snapshotInterval)
	markets, err := fetchMarkets(ctx, b.logger)
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

func (b *BitpinOrderbookWSScraper) createClient() *scraper.WSClient {
	config := scraper.WSConfig{URL: wsURL, PingDisabled: true}
	handler := scraper.WSHandler{
		OnConnect:   b.onConnect,
		OnSubscribe: b.onSubscribe,
		OnMessage:   b.onMessage,
	}
	return scraper.NewWSClient(config, handler, b.sender, b.logger)
}

func (b *BitpinOrderbookWSScraper) onConnect(conn *websocket.Conn) error {
	return conn.WriteJSON(map[string]any{"id": 1, "connect": map[string]any{}})
}

func (b *BitpinOrderbookWSScraper) onSubscribe(conn *websocket.Conn, symbols []string) error {
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

func (b *BitpinOrderbookWSScraper) onMessage(conn *websocket.Conn, message []byte) ([]proto.Message, error) {
	scanner := bufio.NewScanner(bytes.NewReader(message))
	for scanner.Scan() {
		if snapshot := b.parseLine(conn, scanner.Bytes()); snapshot != nil {
			b.mu.Lock()
			b.depthStore[snapshot.Symbol] = snapshot
			b.mu.Unlock()
		}
	}

	now := time.Now()
	currentInterval := now.Truncate(snapshotInterval)

	b.mu.Lock()
	shouldSend := !currentInterval.Equal(b.lastSnapshotTime)
	if shouldSend {
		b.lastSnapshotTime = currentInterval
	}
	b.mu.Unlock()

	if shouldSend {
		b.sendMinuteSnapshots(now)
	}

	return nil, nil
}

func (b *BitpinOrderbookWSScraper) sendMinuteSnapshots(snapshotTime time.Time) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	intervalTime := snapshotTime.UTC().Truncate(snapshotInterval)
	timeStr := intervalTime.Format(time.RFC3339)

	for symbol, snapshot := range b.depthStore {
		snapshotToSend := &pb.OrderBookSnapshot{
			Id:         scraper.GenerateSnapShotID("bitpin", symbol, timeStr),
			Exchange:   "bitpin",
			Symbol:     symbol,
			LastUpdate: timeStr,
			Bids:       snapshot.Bids,
			Asks:       snapshot.Asks,
		}

		if err := b.sender.SendOrderBookSnapShot(context.Background(), snapshotToSend); err != nil {
			b.logger.Error("failed to send snapshot", "symbol", symbol, "error", err)
			continue
		}
	}
}

func (b *BitpinOrderbookWSScraper) parseLine(conn *websocket.Conn, message []byte) *pb.OrderBookSnapshot {
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

func (b *BitpinOrderbookWSScraper) createDepth(data depthResponse) *pb.OrderBookSnapshot {
	cleanedSymbol := scraper.NormalizeSymbol("bitpin", data.Symbol)

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
