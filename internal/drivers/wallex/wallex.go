package wallex

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/navid-fn/radar/internal/crawler"
)

const (
	WallexAPIURL         = "https://api.wallex.ir/hector/web/v1/markets"
	WallexWSURL          = "wss://api.wallex.ir/ws"
	MaxSubsPerConnection = 20
)

type Market struct {
	Symbol string `json:"symbol"`
}

type APIResponse struct {
	Result struct {
		Markets []Market `json:"markets"`
	} `json:"result"`
}

type WallexCrawler struct {
	*crawler.BaseCrawler
	wsWorker *crawler.BaseWebSocketWorker
}

// NewWallexCrawler creates a new Wallex crawler instance for trades
func NewWallexCrawler() *WallexCrawler {
	config := crawler.NewConfig("wallex", MaxSubsPerConnection)
	baseCrawler := crawler.NewBaseCrawler(config)

	wc := &WallexCrawler{
		BaseCrawler: baseCrawler,
	}

	// Setup WebSocket worker for trades
	wsConfig := crawler.DefaultWebSocketConfig(WallexWSURL)
	wsConfig.HandshakeTimeout = 4 * time.Second

	wc.wsWorker = crawler.NewBaseWebSocketWorker(wsConfig, wc.Logger, wc.SendToKafka)

	// Configure subscription for trades
	wc.wsWorker.OnSubscribe = func(conn *websocket.Conn, symbols []string) error {
		wc.Logger.Infof("Subscribing to %d markets for trades...", len(symbols))
		for _, symbol := range symbols {
			conn.SetWriteDeadline(time.Now().Add(wsConfig.WriteTimeout))
			subscriptionMsg := []any{"subscribe", map[string]string{"channel": fmt.Sprintf("%s@trade", symbol)}}
			if err := conn.WriteJSON(subscriptionMsg); err != nil {
				return fmt.Errorf("failed to send subscription message: %w", err)
			}
		}
		wc.Logger.Info("Subscriptions sent")
		return nil
	}

	return wc
}

// GetName returns the exchange name
func (wc *WallexCrawler) GetName() string {
	return "wallex"
}

// FetchMarkets fetches all trading markets from Wallex API
func (wc *WallexCrawler) FetchMarkets() ([]string, error) {
	resp, err := http.Get(WallexAPIURL)
	if err != nil {
		return nil, fmt.Errorf("error fetching markets from Wallex API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned status code: %d", resp.StatusCode)
	}

	var apiResponse APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResponse); err != nil {
		return nil, fmt.Errorf("error decoding API response: %w", err)
	}

	markets := make([]string, len(apiResponse.Result.Markets))
	for i, market := range apiResponse.Result.Markets {
		markets[i] = market.Symbol
	}

	sort.Strings(markets)

	wc.Logger.Infof("Fetched %d unique markets from Wallex API", len(markets))
	return markets, nil
}

// Run starts the Wallex crawler
func (wc *WallexCrawler) Run(ctx context.Context) error {
	wc.Logger.Info("Starting Wallex Crawler (trades mode)...")

	// Initialize Kafka producer
	if err := wc.InitKafkaProducer(); err != nil {
		return fmt.Errorf("failed to initialize Kafka producer: %w", err)
	}
	defer wc.CloseKafkaProducer()

	wc.StartDeliveryReport()

	// Fetch markets
	markets, err := wc.FetchMarkets()
	if err != nil {
		return fmt.Errorf("could not fetch markets: %w", err)
	}

	if len(markets) == 0 {
		return fmt.Errorf("no markets found to subscribe to")
	}

	// Chunk markets
	marketChunks := crawler.ChunkMarkets(markets, wc.Config.MaxSubsPerConnection)
	wc.Logger.Infof("Divided %d markets into %d chunks of ~%d",
		len(markets), len(marketChunks), wc.Config.MaxSubsPerConnection)

	// Start workers
	return crawler.RunWithGracefulShutdown(wc.Logger, func(ctx context.Context, wg *sync.WaitGroup) {
		for _, chunk := range marketChunks {
			wg.Add(1)
			go wc.wsWorker.RunWorker(ctx, chunk, wg, "WallexWorker")
		}
	})
}
