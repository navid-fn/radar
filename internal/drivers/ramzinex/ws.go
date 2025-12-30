package ramzinex

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

const (
	wsURL             = "wss://websocket.ramzinex.com/websocket"
	maxSymbolsPerConn = 100
)

type RamzinexWS struct {
	sender       *scraper.Sender
	logger       *slog.Logger
	pairIDToName map[int]string
	usdtPrice    float64
	usdtMu       sync.RWMutex
}

func NewRamzinexScraper(kafkaWriter *kafka.Writer, logger *slog.Logger) *RamzinexWS {
	return &RamzinexWS{
		sender:       scraper.NewSender(kafkaWriter, logger),
		logger:       logger.With("scraper", "ramzinex-ws"),
		pairIDToName: make(map[int]string),
		usdtPrice:    float64(getLatestUSDTPrice()),
	}
}

func (r *RamzinexWS) Name() string { return "ramzinex" }

func (r *RamzinexWS) Run(ctx context.Context) error {
	r.logger.Info("Starting Ramzinex WebSocket scraper", "usdtPrice", r.usdtPrice)

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

func (r *RamzinexWS) createClient() *scraper.WSClient {
	config := scraper.WSConfig{URL: wsURL, PingDisabled: true}
	handler := scraper.WSHandler{
		OnConnect:   r.onConnect,
		OnSubscribe: r.onSubscribe,
		OnMessage:   r.onMessage,
	}
	return scraper.NewWSClient(config, handler, r.sender, r.logger)
}

func (r *RamzinexWS) onConnect(conn *websocket.Conn) error {
	msg := map[string]any{"connect": map[string]string{"name": "js"}, "id": 1}
	if err := conn.WriteJSON(msg); err != nil {
		return err
	}
	time.Sleep(500 * time.Millisecond)
	return nil
}

func (r *RamzinexWS) onSubscribe(conn *websocket.Conn, pairIDs []string) error {
	for i, pairID := range pairIDs {
		msg := map[string]any{
			"id":        i + 2,
			"subscribe": map[string]any{"channel": "last-trades:" + pairID, "recover": true},
		}
		if err := conn.WriteJSON(msg); err != nil {
			return err
		}
		time.Sleep(200 * time.Millisecond)
	}
	r.logger.Info("Subscribed", "count", len(pairIDs))
	return nil
}

func (r *RamzinexWS) onMessage(conn *websocket.Conn, message []byte) ([]byte, error) {
	msgStr := strings.TrimSpace(string(message))
	if msgStr == "{}" || msgStr == "{}\n" {
		conn.WriteJSON(map[string]any{})
		return nil, nil
	}

	var msg map[string]any
	if json.Unmarshal(message, &msg) != nil {
		return nil, nil
	}
	if len(msg) == 0 {
		conn.WriteJSON(map[string]any{})
		return nil, nil
	}

	push, _ := msg["push"].(map[string]any)
	if push == nil {
		return nil, nil
	}
	pub, _ := push["pub"].(map[string]any)
	if pub == nil {
		return nil, nil
	}
	data, _ := pub["data"].([]any)
	if data == nil {
		return nil, nil
	}

	channel, _ := push["channel"].(string)
	parts := strings.Split(channel, ":")
	if len(parts) != 2 {
		return nil, nil
	}

	var pairID int
	fmt.Sscanf(parts[1], "%d", &pairID)
	pairName, ok := r.pairIDToName[pairID]
	if !ok {
		return nil, nil
	}

	var trades []*pb.TradeData
	for _, item := range data {
		row, ok := item.([]any)
		if !ok || len(row) < 6 {
			continue
		}

		cleanedSymbol := cleanSymbol(strings.ToUpper(pairName))
		cleanedPrice := cleanPrice(cleanedSymbol, row[0].(float64))
		volume := row[1].(float64)
		tradeTime := convertTimeToRFC3339(row[2].(string))
		side := row[3].(string)
		id := row[5].(string)

		if cleanedSymbol == "USDT/IRT" {
			r.usdtMu.Lock()
			r.usdtPrice = cleanedPrice
			r.usdtMu.Unlock()
		}

		trade := &pb.TradeData{
			Id:        id,
			Exchange:  "ramzinex",
			Symbol:    cleanedSymbol,
			Price:     cleanedPrice,
			Volume:    volume,
			Quantity:  cleanedPrice * volume,
			Side:      side,
			Time:      tradeTime,
			UsdtPrice: r.usdtPrice,
		}
		trades = append(trades, trade)
	}

	if len(trades) == 0 {
		return nil, nil
	}
	return proto.Marshal(&pb.TradeDataBatch{Trades: trades})

}

func (r *RamzinexWS) fetchPairs() ([]pairDetail, error) {
	pairs, pairMap, err := fetchPairs(r.logger)
	if err != nil {
		return nil, err
	}
	r.pairIDToName = pairMap
	return pairs, nil
}
