package wallex

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"nobitex/radar/configs"
	"nobitex/radar/internal/scraper"

	"github.com/gorilla/websocket"
)

const (
	WallexAPIURL         = "https://api.wallex.ir/hector/web/v1/markets"
	WallexWSURL          = "wss://api.wallex.ir/ws"
	MaxSubsPerConnection = 50
)

type WallexScraper struct {
	*scraper.BaseScraper
	wsWorker *scraper.BaseWebSocketWorker

	usdtPrice float64
	usdtMu    sync.RWMutex
}

func NewWallexScraper(cfg *configs.Config) *WallexScraper {
	config := scraper.NewConfig("wallex", MaxSubsPerConnection, cfg)
	baseScraper := scraper.NewBaseScraper(config)

	ws := &WallexScraper{
		BaseScraper: baseScraper,
		usdtPrice:   getLatestUSDTPrice(),
	}

	wsConfig := scraper.DefaultWebSocketConfig(WallexWSURL)
	wsConfig.HandshakeTimeout = 4 * time.Second

	wsConfig.MaxConnectionDuration = 30 * time.Minute
	wsConfig.MaxPongCount = 100
	wsConfig.PongExtension = 30 * time.Second
	wsConfig.MaxMessagesPerChannel = 50
	wsConfig.DisableClientPing = true

	ws.wsWorker = scraper.NewBaseWebSocketWorker(wsConfig, ws.Logger, ws.SendToKafka)
	ws.wsWorker.SendToKafkaCtx = ws.SendToKafkaWithContext

	ws.wsWorker.OnSubscribe = func(conn *websocket.Conn, symbols []string) error {
		ws.Logger.Info("Subscribing to markets for trades", "count", len(symbols))
		for _, symbol := range symbols {
			subscriptionMsg := []any{"subscribe", map[string]string{"channel": fmt.Sprintf("%s@trade", symbol)}}
			if err := ws.wsWorker.WriteJSON(conn, subscriptionMsg); err != nil {
				return fmt.Errorf("failed to send subscription message: %w", err)
			}
		}
		ws.Logger.Info("Subscriptions sent")
		return nil
	}
	ws.wsWorker.OnMessage = func(conn *websocket.Conn, message []byte) ([]byte, error) {
		messageStr := strings.TrimSpace(string(message))
		if strings.Contains(messageStr, `"sid":`) {
			return nil, nil
		}
		var rawMessage []any
		err := json.Unmarshal(message, &rawMessage)
		if err != nil {
			return nil, nil
		}

		parts := strings.Split(rawMessage[0].(string), "@")
		tradeData := rawMessage[1].(map[string]any)
		symbol := cleanSymbol(parts[0])
		side := "buy"
		if !tradeData["isBuyOrder"].(bool) {
			side = "sell"
		}

		volume, _ := strconv.ParseFloat(tradeData["quantity"].(string), 64)
		price, _ := strconv.ParseFloat(tradeData["price"].(string), 64)
		quantity := price * volume

		if symbol == "USDT/IRT" {
			ws.usdtMu.Lock()
			ws.usdtPrice = price
			ws.usdtMu.Unlock()
		}

		data := scraper.KafkaData{
			Symbol:    symbol,
			Side:      side,
			Exchange:  "wallex",
			Price:     price,
			Volume:    volume,
			Quantity:  quantity,
			Time:      tradeData["timestamp"].(string),
			USDTPrice: ws.usdtPrice,
		}
		data.ID = scraper.GenerateTradeID(&data)

		// change json to proto
		kafkaMsgBytes, _ := scraper.SerializeProto(&data)
		return kafkaMsgBytes, nil
	}

	return ws
}

func (ws *WallexScraper) Name() string {
	return "wallex"
}

func (ws *WallexScraper) FetchMarkets() ([]string, error) {
	resp, err := http.Get(WallexAPIURL)
	if err != nil {
		return nil, fmt.Errorf("error fetching markets from Wallex API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned status code: %d", resp.StatusCode)
	}

	var marketAPIresposnse apiMarketResponse
	if err := json.NewDecoder(resp.Body).Decode(&marketAPIresposnse); err != nil {
		return nil, fmt.Errorf("error decoding API response: %w", err)
	}

	var markets []string
	for _, market := range marketAPIresposnse.Result.Markets {
		if market.Enabled && market.Symbol == "1INCHTMN" {
			markets = append(markets, market.Symbol)
		}
	}

	sort.Strings(markets)

	ws.Logger.Info("Fetched unique markets from Wallex API", "count", len(markets))
	return markets, nil
}

func (ws *WallexScraper) Run(ctx context.Context) error {
	ws.Logger.Info("Starting Wallex Crawler (trades mode)...")

	if err := ws.InitKafkaProducer(); err != nil {
		return fmt.Errorf("failed to initialize Kafka producer: %w", err)
	}
	defer ws.CloseKafkaProducer()

	markets, err := ws.FetchMarkets()
	if err != nil {
		return fmt.Errorf("could not fetch markets: %w", err)
	}

	if len(markets) == 0 {
		return fmt.Errorf("no markets found to subscribe to")
	}

	marketChunks := scraper.ChunkMarkets(markets, ws.Config.MaxSubsPerConnection)
	ws.Logger.Info("Divided markets into chunks",
		"total", len(markets),
		"chunks", len(marketChunks),
		"chunkSize", ws.Config.MaxSubsPerConnection)

	return scraper.RunWithGracefulShutdown(ws.Logger, func(ctx context.Context, wg *sync.WaitGroup) {
		for _, chunk := range marketChunks {
			wg.Add(1)
			go ws.wsWorker.RunWorker(ctx, chunk, wg, "WallexWorker")
		}
	})
}
