package nobitex

import (
	"bufio"
	"bytes"
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

type NobitexWS struct {
	sender    *scraper.Sender
	logger    *slog.Logger
	usdtPrice float64
	usdtMu    sync.RWMutex
}

func NewNobitexScraper(kafkaWriter *kafka.Writer, logger *slog.Logger) *NobitexWS {
	return &NobitexWS{
		sender:    scraper.NewSender(kafkaWriter, logger),
		logger:    logger.With("scraper", "nobitex-ws"),
	}
}

func (n *NobitexWS) Name() string { return "nobitex" }

func (n *NobitexWS) Run(ctx context.Context) error {
	n.usdtPrice = getLatestUSDTPrice()
	n.logger.Info("Starting Nobitex WebSocket scraper", "usdtPrice", n.usdtPrice)

	markets, err := fetchMarkets(n.logger)
	if err != nil {
		return fmt.Errorf("fetch markets: %w", err)
	}
	if len(markets) == 0 {
		return fmt.Errorf("no markets found")
	}

	chunks := scraper.ChunkSlice(markets, maxSymbolsPerConn)
	scraper.RunWorkers(ctx, chunks, "nobitex", func() *scraper.WSClient {
		return n.createClient()
	}, n.logger)

	return nil
}

func (n *NobitexWS) createClient() *scraper.WSClient {
	config := scraper.WSConfig{URL: wsURL, PingDisabled: true}
	handler := scraper.WSHandler{
		OnConnect:   n.onConnect,
		OnSubscribe: n.onSubscribe,
		OnMessage:   n.onMessage,
	}
	return scraper.NewWSClient(config, handler, n.sender, n.logger)
}

func (n *NobitexWS) onConnect(conn *websocket.Conn) error {
	return conn.WriteJSON(map[string]any{"id": 1, "connect": map[string]any{}})
}

func (n *NobitexWS) onSubscribe(conn *websocket.Conn, symbols []string) error {
	for i, sym := range symbols {
		msg := map[string]any{"id": i + 2, "subscribe": map[string]any{"channel": "public:trades-" + sym}}
		if err := conn.WriteJSON(msg); err != nil {
			return err
		}
	}
	n.logger.Info("Subscribed", "count", len(symbols))
	return nil
}

func (n *NobitexWS) onMessage(conn *websocket.Conn, message []byte) ([]byte, error) {
	var trades []*pb.TradeData
	scanner := bufio.NewScanner(bytes.NewReader(message))
	for scanner.Scan() {
		if trade := n.parseLine(conn, scanner.Bytes()); trade != nil {
			trades = append(trades, trade)
		}
	}
	if len(trades) == 0 {
		return nil, nil
	}
	return proto.Marshal(&pb.TradeDataBatch{Trades: trades})
}

func (n *NobitexWS) parseLine(conn *websocket.Conn, line []byte) *pb.TradeData {
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
	data, _ := pub["data"].(map[string]any)
	if data == nil {
		return nil
	}

	channel, _ := push["channel"].(string)
	symbol := ""
	if len(channel) > 14 {
		symbol = channel[14:]
	}

	return n.createTrade(data, symbol)
}

func (n *NobitexWS) createTrade(data map[string]any, symbol string) *pb.TradeData {
	priceStr := getStringValue(data, "price")
	volumeStr := getStringValue(data, "volume")
	if priceStr == "" || volumeStr == "" || symbol == "" {
		return nil
	}

	price, _ := strconv.ParseFloat(priceStr, 64)
	volume, _ := strconv.ParseFloat(volumeStr, 64)
	if price <= 0 || volume <= 0 {
		return nil
	}

	cleanedSymbol := scraper.NormalizeSymbol("nobitex", symbol)
	cleanedPrice := scraper.NormalizePrice(cleanedSymbol, price)

	if cleanedSymbol == "USDT/IRT" {
		n.usdtMu.Lock()
		n.usdtPrice = cleanedPrice
		n.usdtMu.Unlock()
	}

	n.usdtMu.RLock()
	usdtPrice := n.usdtPrice
	n.usdtMu.RUnlock()

	return &pb.TradeData{
		Id:        scraper.GenerateTradeID("nobitex", cleanedSymbol, getTimeValue(data, "time"), cleanedPrice, volume, getStringValue(data, "type")),
		Exchange:  "nobitex",
		Symbol:    cleanedSymbol,
		Price:     cleanedPrice,
		Volume:    volume,
		Quantity:  volume * cleanedPrice,
		Side:      getStringValue(data, "type"),
		Time:      getTimeValue(data, "time"),
		UsdtPrice: usdtPrice,
	}
}
