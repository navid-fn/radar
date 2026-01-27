package bitpin

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"sync"

	pb "nobitex/radar/internal/proto"
	"nobitex/radar/internal/scraper"

	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

const (
	wsURL             = "wss://centrifugo.bitpin.ir/connection/websocket"
	maxSymbolsPerConn = 100
)

type BitpinWS struct {
	sender    *scraper.Sender
	logger    *slog.Logger
	usdtPrice float64
	usdtMu    sync.RWMutex
}

func NewBitpinScraper(kafkaWriter *kafka.Writer, logger *slog.Logger) *BitpinWS {
	return &BitpinWS{
		sender: scraper.NewSender(kafkaWriter, logger),
		logger: logger.With("scraper", "bitpin-ws"),
	}
}

func (b *BitpinWS) Name() string { return "bitpin" }

func (b *BitpinWS) Run(ctx context.Context) error {
	b.usdtPrice = getLatestUSDTPrice()
	b.logger.Info("Starting Bitpin WebSocket scraper", "usdtPrice", b.usdtPrice)
	symbols, err := fetchMarkets(b.logger)
	if err != nil {
		return err
	}
	if len(symbols) == 0 {
		return fmt.Errorf("no markets found")
	}

	chunks := scraper.ChunkSlice(symbols, maxSymbolsPerConn)
	scraper.RunWorkers(ctx, chunks, "bitpin", func() *scraper.WSClient {
		return b.createClient()
	}, b.logger)

	return nil
}

func (b *BitpinWS) createClient() *scraper.WSClient {
	config := scraper.WSConfig{URL: wsURL, PingDisabled: true}
	handler := scraper.WSHandler{
		OnConnect:   b.onConnect,
		OnSubscribe: b.onSubscribe,
		OnMessage:   b.onMessage,
	}
	return scraper.NewWSClient(config, handler, b.sender, b.logger)
}

func (b *BitpinWS) onConnect(conn *websocket.Conn) error {
	return conn.WriteJSON(map[string]any{"id": 1, "connect": map[string]any{}})
}

func (b *BitpinWS) onSubscribe(conn *websocket.Conn, symbols []string) error {
	for i, sym := range symbols {
		msg := map[string]any{
			"id":        i + 2,
			"subscribe": map[string]any{"channel": "matches:" + sym},
		}
		if err := conn.WriteJSON(msg); err != nil {
			return err
		}
	}
	b.logger.Info("Subscribed", "count", len(symbols))
	return nil
}

func (b *BitpinWS) onMessage(conn *websocket.Conn, message []byte) ([]byte, error) {
	var msg map[string]any
	if json.Unmarshal(message, &msg) != nil {
		return nil, nil
	}

	if len(msg) == 0 || msg["ping"] != nil {
		conn.WriteJSON(map[string]any{})
		return nil, nil
	}

	if _, ok := msg["connect"]; ok {
		return nil, nil
	}
	if _, ok := msg["subscribe"]; ok {
		return nil, nil
	}

	push, _ := msg["push"].(map[string]any)
	if push == nil {
		return nil, nil
	}

	channel, _ := push["channel"].(string)
	pub, _ := push["pub"].(map[string]any)
	if pub == nil {
		return nil, nil
	}

	dataField, _ := pub["data"].(map[string]any)
	if dataField == nil {
		return nil, nil
	}

	event, _ := dataField["event"].(string)
	if event != "matches_update" {
		return nil, nil
	}

	symbol := ""
	if len(channel) > 8 {
		symbol = channel[8:]
	}

	matches, ok := dataField["matches"].([]tradeMatch)
	if !ok {
		return nil, nil
	}

	var trades []*pb.TradeData
	for _, m := range matches {
		tradeTime := scraper.FloatTimestampToRFC3339(m.Time)
		price, _ := strconv.ParseFloat(m.Price, 64)
		volume, _ := strconv.ParseFloat(m.BaseAmount, 64)

		if volume == 0 && price > 0 {
			// check if quote_amount has value
			// sometimes the base_amount return 0.00
			quoteAmountStr := m.QuoteAmount
			quoteAmount, err := strconv.ParseFloat(quoteAmountStr, 64)
			if err == nil {
				volume = quoteAmount / price
			}
		}
		cleanedSymbol := scraper.NormalizeSymbol("bitpin", symbol)

		if cleanedSymbol == "USDT/IRT" {
			b.usdtMu.Lock()
			b.usdtPrice = price
			b.usdtMu.Unlock()
		}

		trade := &pb.TradeData{
			Id:        m.ID,
			Exchange:  "bitpin",
			Symbol:    cleanedSymbol,
			Price:     price,
			Volume:    volume,
			Quantity:  volume * price,
			Side:      m.Side,
			Time:      tradeTime,
			UsdtPrice: b.usdtPrice,
		}
		trades = append(trades, trade)
	}

	if len(trades) == 0 {
		return nil, nil
	}
	return proto.Marshal(&pb.TradeDataBatch{Trades: trades})
}
