package wallex

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"

	pb "nobitex/radar/internal/proto"
	"nobitex/radar/internal/scraper"

	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

type WallexWS struct {
	sender    *scraper.Sender
	logger    *slog.Logger
	usdtPrice float64
	usdtMu    sync.RWMutex
}

func NewWallexScraper(kafkaWriter *kafka.Writer, logger *slog.Logger) *WallexWS {
	return &WallexWS{
		sender:    scraper.NewSender(kafkaWriter, logger),
		logger:    logger.With("scraper", "wallex-ws"),
	}
}

func (w *WallexWS) Name() string { return "wallex" }

func (w *WallexWS) Run(ctx context.Context) error {
	w.usdtPrice = getLatestUSDTPrice()
	w.logger.Info("Starting Wallex WebSocket scraper", "usdtPrice", w.usdtPrice)

	markets, err := fetchMarkets(w.logger)
	if err != nil {
		return err
	}
	if len(markets) == 0 {
		return fmt.Errorf("no markets found")
	}

	chunks := scraper.ChunkSlice(markets, maxSymbolsPerConn)
	scraper.RunWorkers(ctx, chunks, "wallex", func() *scraper.WSClient {
		return w.createClient()
	}, w.logger)

	return nil
}

func (w *WallexWS) createClient() *scraper.WSClient {
	config := scraper.WSConfig{URL: wsURL, PingDisabled: true}
	handler := scraper.WSHandler{
		OnSubscribe: w.onSubscribe,
		OnMessage:   w.onMessage,
	}
	return scraper.NewWSClient(config, handler, w.sender, w.logger)
}

func (w *WallexWS) onSubscribe(conn *websocket.Conn, symbols []string) error {
	for _, sym := range symbols {
		msg := []any{"subscribe", map[string]string{"channel": sym + "@trade"}}
		if err := conn.WriteJSON(msg); err != nil {
			return err
		}
	}
	w.logger.Info("Subscribed", "count", len(symbols))
	return nil
}

func (w *WallexWS) onMessage(conn *websocket.Conn, message []byte) ([]byte, error) {
	msgStr := strings.TrimSpace(string(message))
	if strings.Contains(msgStr, `"sid":`) {
		return nil, nil
	}

	var raw []any
	if json.Unmarshal(message, &raw) != nil || len(raw) < 2 {
		return nil, nil
	}

	channel, _ := raw[0].(string)
	data, _ := raw[1].(map[string]any)
	if data == nil {
		return nil, nil
	}

	parts := strings.Split(channel, "@")
	symbol := scraper.NormalizeSymbol("wallex", parts[0])

	side := "buy"
	if isBuy, ok := data["isBuyOrder"].(bool); ok && !isBuy {
		side = "sell"
	}

	volume, _ := strconv.ParseFloat(data["quantity"].(string), 64)
	price, _ := strconv.ParseFloat(data["price"].(string), 64)
	timestamp := data["timestamp"].(string)

	if symbol == "USDT/IRT" {
		w.usdtMu.Lock()
		w.usdtPrice = price
		w.usdtMu.Unlock()
	}

	trade := &pb.TradeData{
		Id:        scraper.GenerateTradeID("wallex", symbol, timestamp, price, volume, side),
		Exchange:  "wallex",
		Symbol:    symbol,
		Price:     price,
		Volume:    volume,
		Quantity:  price * volume,
		Side:      side,
		Time:      timestamp,
		UsdtPrice: w.usdtPrice,
	}

	return proto.Marshal(trade)
}
