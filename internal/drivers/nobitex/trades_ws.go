package nobitex

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"nobitex/radar/configs"
	"nobitex/radar/internal/scraper"

	"github.com/gorilla/websocket"
)

const (
	NobitexWsUrl         = "wss://ws.nobitex.ir/connection/websocket"
	MaxSubsPerConnection = 100
)

type NobitexScraper struct {
	*scraper.BaseScraper
	wsWorker  *scraper.BaseWebSocketWorker
	usdtPrice float64
	usdtMu    sync.RWMutex
}

func NewNobitexScraper(cfg *configs.Config) *NobitexScraper {
	config := scraper.NewConfig("nobitex", MaxSubsPerConnection, cfg)
	baseScraper := scraper.NewBaseScraper(config)

	ns := &NobitexScraper{
		BaseScraper: baseScraper,
		usdtPrice:   getLatestUSDTPrice(),
	}

	wsConfig := scraper.DefaultWebSocketConfig(NobitexWsUrl)
	wsConfig.DisableClientPing = true
	wsConfig.HandshakeTimeout = 15 * time.Second
	ns.wsWorker = scraper.NewBaseWebSocketWorker(wsConfig, ns.Logger, ns.SendToKafka)
	ns.wsWorker.SendToKafkaCtx = ns.SendToKafkaWithContext

	ns.wsWorker.OnConnect = func(conn *websocket.Conn) error {
		connectMsg := map[string]any{
			"id":      1,
			"connect": map[string]any{},
		}
		if err := ns.wsWorker.WriteJSON(conn, connectMsg); err != nil {
			return fmt.Errorf("failed to send connect message: %w", err)
		}
		return nil
	}

	ns.wsWorker.OnMessage = func(conn *websocket.Conn, message []byte) ([]byte, error) {
		var allTrades []scraper.KafkaData

		scanner := bufio.NewScanner(bytes.NewReader(message))
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}

			trades := ns.processLine(conn, line)
			allTrades = append(allTrades, trades...)
		}

		if len(allTrades) > 0 {
			protoData, err := scraper.SerializeProtoBatch(allTrades)
			if err != nil {
				ns.Logger.Error("Failed to serialize", "error", err)
				return nil, nil
			}
			return protoData, nil
		}

		return nil, nil
	}

	ns.wsWorker.OnSubscribe = func(conn *websocket.Conn, symbols []string) error {
		for i, symbol := range symbols {
			subscriptionMsg := map[string]any{
				"id": i + 2,
				"subscribe": map[string]any{
					"channel": fmt.Sprintf("public:trades-%s", symbol),
				},
			}
			if err := ns.wsWorker.WriteJSON(conn, subscriptionMsg); err != nil {
				return fmt.Errorf("failed to subscribe to %s: %w", symbol, err)
			}
		}
		ns.Logger.Info("Subscribed to symbols", "count", len(symbols))
		return nil
	}

	return ns
}

func (ns *NobitexScraper) processLine(conn *websocket.Conn, line []byte) []scraper.KafkaData {
	var msg map[string]any
	if err := json.Unmarshal(line, &msg); err != nil {
		return nil
	}

	if len(msg) == 0 {
		ns.wsWorker.WriteJSON(conn, map[string]any{})
		return nil
	}

	if _, ok := msg["ping"]; ok {
		ns.wsWorker.WriteJSON(conn, map[string]any{"pong": map[string]any{}})
		return nil
	}

	if _, ok := msg["connect"]; ok {
		ns.Logger.Info("Connected to Nobitex")
		return nil
	}
	if _, ok := msg["subscribe"]; ok {
		return nil
	}

	if errData, ok := msg["error"]; ok {
		ns.Logger.Error("Server error", "error", errData)
		return nil
	}

	push, ok := msg["push"].(map[string]any)
	if !ok {
		ns.Logger.Debug("Message without push", "msg", msg)
		return nil
	}

	channel, _ := push["channel"].(string)

	pub, _ := push["pub"].(map[string]any)
	if pub == nil {
		ns.Logger.Debug("Push without pub", "channel", channel, "push", push)
		return nil
	}

	data, _ := pub["data"].(map[string]any)
	if data == nil {
		ns.Logger.Debug("Pub without data", "channel", channel, "pub", pub)
		return nil
	}

	symbol := ""
	if len(channel) > 14 {
		symbol = channel[14:]
	}

	trade := ns.parseTrade(data, symbol)
	if trade == nil {
		return nil
	}

	return []scraper.KafkaData{*trade}
}

func (ns *NobitexScraper) parseTrade(tradeMap map[string]any, symbol string) *scraper.KafkaData {
	priceStr := getStringValue(tradeMap, "price")
	volumeStr := getStringValue(tradeMap, "volume")
	sideStr := getStringValue(tradeMap, "type")
	timeValue := getTimeValue(tradeMap, "time")

	// Validate required fields
	if priceStr == "" || volumeStr == "" || symbol == "" {
		ns.Logger.Debug("Invalid trade: missing fields",
			"price", priceStr, "volume", volumeStr, "symbol", symbol, "data", tradeMap)
		return nil
	}

	price, err := strconv.ParseFloat(priceStr, 64)
	if err != nil || price <= 0 {
		ns.Logger.Debug("Invalid trade: bad price", "priceStr", priceStr, "symbol", symbol)
		return nil
	}

	volume, err := strconv.ParseFloat(volumeStr, 64)
	if err != nil || volume <= 0 {
		ns.Logger.Debug("Invalid trade: bad volume", "volumeStr", volumeStr, "symbol", symbol)
		return nil
	}

	cleanedSymbol := cleanSymbol(symbol)
	cleanedPrice := cleanPrice(cleanedSymbol, price)

	if cleanedSymbol == "USDT/IRT" {
		ns.usdtMu.Lock()
		ns.usdtPrice = cleanedPrice
		ns.usdtMu.Unlock()
	}

	ns.usdtMu.RLock()
	currentUSDTPrice := ns.usdtPrice
	ns.usdtMu.RUnlock()

	data := &scraper.KafkaData{
		Exchange:  "nobitex",
		Symbol:    cleanedSymbol,
		Price:     cleanedPrice,
		Volume:    volume,
		Quantity:  volume * cleanedPrice,
		Side:      sideStr,
		Time:      timeValue,
		USDTPrice: currentUSDTPrice,
	}

	data.ID = scraper.GenerateTradeID(data)
	return data
}

func (ns *NobitexScraper) Name() string {
	return "nobitex"
}

func (ns *NobitexScraper) FetchMarkets() ([]string, error) {
	resp, err := http.Get(MarketAPI)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var symbols []string
	var marketData marketDataAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&marketData); err != nil {
		return nil, fmt.Errorf("error decoding API response: %w", err)
	}

	for pair, stats := range marketData.Stats {
		if !stats.IsClosed {
			symbols = append(symbols, transformPair(pair))
		}
	}

	sort.Strings(symbols)
	ns.Logger.Info("Fetched symbols from Nobitex", "count", len(symbols))
	return symbols, nil
}

func (ns *NobitexScraper) Run(ctx context.Context) error {
	ns.Logger.Info("Starting Nobitex WebSocket Scraper...")

	if err := ns.InitKafkaProducer(); err != nil {
		return fmt.Errorf("failed to initialize Kafka producer: %w", err)
	}
	defer ns.CloseKafkaProducer()

	markets, err := ns.FetchMarkets()
	if err != nil {
		return fmt.Errorf("could not fetch markets: %w", err)
	}

	if len(markets) == 0 {
		return fmt.Errorf("no markets found to subscribe to")
	}

	ns.Logger.Info("Current USDT/IRT price", "price", ns.usdtPrice)

	marketChunks := scraper.ChunkMarkets(markets, ns.Config.MaxSubsPerConnection)
	ns.Logger.Info("Divided markets into chunks",
		"total", len(markets),
		"chunks", len(marketChunks),
		"chunkSize", ns.Config.MaxSubsPerConnection)

	return scraper.RunWithGracefulShutdown(ns.Logger, func(ctx context.Context, wg *sync.WaitGroup) {
		for i, chunk := range marketChunks {
			wg.Add(1)
			go ns.wsWorker.RunWorker(ctx, chunk, wg, "NobitexWorker")

			if i < len(marketChunks)-1 {
				time.Sleep(5 * time.Second)
			}
		}
	})
}
