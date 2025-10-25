package bitpin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/navid-fn/radar/internal/crawler"
)

const (
	BitpinAPIURL         = "https://api.bitpin.ir/api/v1/mkt/markets/"
	BitpinWSURL          = "wss://ws.bitpin.ir"
	MaxSubsPerConnection = 40
)

type Market struct {
	Symbol    string `json:"symbol"`
	Tradeable bool   `json:"tradable"`
}

type BitpinCrawler struct {
	*crawler.BaseCrawler
	wsWorker *crawler.BaseWebSocketWorker
}

// NewBitpinCrawler creates a new Bitpin crawler instance
func NewBitpinCrawler() *BitpinCrawler {
	config := crawler.NewConfig("bitpin", MaxSubsPerConnection)
	baseCrawler := crawler.NewBaseCrawler(config)

	bc := &BitpinCrawler{
		BaseCrawler: baseCrawler,
	}

	// Setup WebSocket worker with Bitpin-specific configuration
	wsConfig := crawler.DefaultWebSocketConfig(BitpinWSURL)
	bc.wsWorker = crawler.NewBaseWebSocketWorker(wsConfig, bc.Logger, bc.SendToKafka)

	// Set Bitpin-specific OnMessage handler to filter PONG messages
	bc.wsWorker.OnMessage = func(message []byte) ([]byte, error) {
		messageStr := string(message)
		if messageStr == `{"message":"PONG"}` {
			bc.Logger.Debug("PONG received")
			return nil, nil // Skip PONG messages
		}
		return message, nil
	}

	// Set Bitpin-specific OnSubscribe handler
	bc.wsWorker.OnSubscribe = func(conn *websocket.Conn, symbols []string) error {
		subscriptionMsg := map[string]any{
			"method":  "sub_to_market_data",
			"symbols": symbols,
		}

		conn.SetWriteDeadline(time.Now().Add(wsConfig.WriteTimeout))
		if err := conn.WriteJSON(subscriptionMsg); err != nil {
			return fmt.Errorf("failed to send subscription message: %w", err)
		}

		bc.Logger.Infof("Subscribed to %d symbols", len(symbols))
		return nil
	}

	return bc
}

// GetName returns the exchange name
func (bc *BitpinCrawler) GetName() string {
	return "bitpin"
}

// FetchMarkets fetches all tradeable markets from Bitpin API
func (bc *BitpinCrawler) FetchMarkets() ([]string, error) {
	var markets []string

	resp, err := http.Get(BitpinAPIURL)
	if err != nil {
		return nil, fmt.Errorf("error fetching markets from Bitpin API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading API response: %w", err)
	}

	var apiResponse []Market
	if err = json.Unmarshal(body, &apiResponse); err != nil {
		return nil, fmt.Errorf("error unmarshaling API response: %w", err)
	}

	for _, market := range apiResponse {
		if market.Tradeable {
			markets = append(markets, market.Symbol)
		}
	}

	sort.Strings(markets)

	bc.Logger.Infof("Fetched %d unique markets from Bitpin API", len(markets))
	return markets, nil
}

// Run starts the Bitpin crawler
func (bc *BitpinCrawler) Run(ctx context.Context) error {
	bc.Logger.Info("Starting Bitpin Crawler...")

	// Initialize Kafka producer
	if err := bc.InitKafkaProducer(); err != nil {
		return fmt.Errorf("failed to initialize Kafka producer: %w", err)
	}
	defer bc.CloseKafkaProducer()

	bc.StartDeliveryReport()

	// Fetch markets
	markets, err := bc.FetchMarkets()
	if err != nil {
		return fmt.Errorf("could not fetch markets: %w", err)
	}

	if len(markets) == 0 {
		return fmt.Errorf("no markets found to subscribe to")
	}

	// Chunk markets
	marketChunks := crawler.ChunkMarkets(markets, bc.Config.MaxSubsPerConnection)
	bc.Logger.Infof("Divided %d markets into %d chunks of ~%d",
		len(markets), len(marketChunks), bc.Config.MaxSubsPerConnection)

	// Start workers with graceful shutdown
	return crawler.RunWithGracefulShutdown(bc.Logger, func(ctx context.Context, wg *sync.WaitGroup) {
		for i, chunk := range marketChunks {
			wg.Add(1)
			go bc.wsWorker.RunWorker(ctx, chunk, wg, "BitpinWorker")

			// Stagger worker startup
			if i < len(marketChunks)-1 {
				time.Sleep(1 * time.Second)
			}
		}
	})
}
