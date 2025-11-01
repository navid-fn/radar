package coingecko

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/navid-fn/radar/internal/crawler"
)

const (
	BaseURL          = "https://api.coingecko.com/api/v3"
	ExchangeID       = "binance"
	PollingInterval  = 5 * time.Minute
	RequestTimeout   = 30 * time.Second
	RateLimit        = 15 * time.Second // 4 requests per minute = 15 seconds between requests
	MaxRetries       = 3                // Maximum retry attempts for rate limit
	RateLimitBackoff = 60 * time.Second // Wait 60 seconds when rate limited
)

type TickerResponse struct {
	Tickers []Ticker `json:"tickers"`
}

type Ticker struct {
	Base            string        `json:"base"`
	Target          string        `json:"target"`
	ConvertedLast   ConvertedData `json:"converted_last"`
	ConvertedVolume ConvertedData `json:"converted_volume"`
	LastFetchAt     string        `json:"last_fetch_at"`
	Volume          float64       `json:"volume"`
}

type ConvertedData struct {
	USD float64 `json:"usd"`
}

type TickerData struct {
	Exchange string  `json:"exchange"`
	Symbol   string  `json:"symbol"`
	Price    float64 `json:"price"`
	Volume   float64 `json:"volume"`
	Quantity float64 `json:"quantity"`
	Side     string  `json:"side"`
	Time     string  `json:"time"`
}

type CoinGeckoCrawler struct {
	*crawler.BaseCrawler
	httpClient *http.Client
}

func NewCoinGeckoCrawler() *CoinGeckoCrawler {
	config := crawler.NewConfig("coingecko", 0)
	baseCrawler := crawler.NewBaseCrawler(config)

	return &CoinGeckoCrawler{
		BaseCrawler: baseCrawler,
		httpClient:  &http.Client{Timeout: RequestTimeout},
	}
}

func (cgc *CoinGeckoCrawler) GetName() string {
	return "coingecko-binance"
}

func (cgc *CoinGeckoCrawler) Run(ctx context.Context) error {
	cgc.Logger.Info("Starting CoinGecko Binance Crawler...")

	if err := cgc.InitKafkaProducer(); err != nil {
		return fmt.Errorf("failed to initialize Kafka: %w", err)
	}
	defer func() {
		cgc.Logger.Info("Closing Kafka producer...")
		cgc.CloseKafkaProducer()
		cgc.Logger.Info("Kafka producer closed")
	}()

	cgc.StartDeliveryReport()

	cgc.Logger.Info("Starting data fetch...")
	if err := cgc.fetchAndSend(ctx); err != nil {
		cgc.Logger.Errorf("Fetch failed: %v", err)
		return fmt.Errorf("failed to fetch and send data: %w", err)
	}

	cgc.Logger.Info("Waiting for Kafka to flush remaining messages...")
	time.Sleep(2 * time.Second)

	cgc.Logger.Info("All pages processed successfully. Shutting down gracefully...")
	return nil
}

func (cgc *CoinGeckoCrawler) fetchAndSend(ctx context.Context) error {
	cgc.Logger.Info("Fetching tickers from CoinGecko...")

	page := 1
	totalSent := 0

	for {
		tickers, err := cgc.fetchPage(page)
		if err != nil {
			return err
		}

		if len(tickers) == 0 {
			break
		}

		cgc.Logger.Infof("Fetched page %d: %d tickers", page, len(tickers))

		// Process and send USD pairs immediately
		sent := cgc.sendUSDPairs(tickers)
		totalSent += sent
		cgc.Logger.Infof("Sent %d USD pairs from page %d to Kafka", sent, page)

		// Check if this is the last page
		if len(tickers) < 100 {
			break
		}

		page++
		time.Sleep(RateLimit) // Rate limit: 4 requests per minute
	}

	cgc.Logger.Infof("Completed: Total %d USD pairs sent to Kafka", totalSent)
	return nil
}

func (cgc *CoinGeckoCrawler) sendUSDPairs(tickers []Ticker) int {
	sent := 0

	for _, ticker := range tickers {
		if !strings.EqualFold(ticker.Target, "USDT") {
			continue
		}
		data := TickerData{
			Symbol:   fmt.Sprintf("%s/%s", ticker.Base, "USDT"),
			Price:    ticker.ConvertedLast.USD,
			Volume:   ticker.ConvertedVolume.USD,
			Time:     ticker.LastFetchAt,
			Exchange: "binance",
			Quantity: ticker.ConvertedVolume.USD,
			Side:     "all",
		}
		jsonData, err := json.Marshal(data)
		if err != nil {
			cgc.Logger.Errorf("Failed to marshal %s: %v", ticker.Base, err)
			continue
		}

		if err := cgc.SendToKafka(jsonData); err != nil {
			cgc.Logger.Errorf("Failed to send %s to Kafka: %v", ticker.Base, err)
			continue
		}

		sent++
	}

	return sent
}

func (cgc *CoinGeckoCrawler) fetchPage(page int) ([]Ticker, error) {
	url := fmt.Sprintf("%s/exchanges/%s/tickers?page=%d", BaseURL, ExchangeID, page)

	var lastErr error
	for attempt := 0; attempt < MaxRetries; attempt++ {
		if attempt > 0 {
			cgc.Logger.Warnf("Retry attempt %d/%d for page %d", attempt, MaxRetries, page)
		}

		resp, err := cgc.httpClient.Get(url)
		if err != nil {
			lastErr = fmt.Errorf("HTTP request failed: %w", err)
			time.Sleep(5 * time.Second)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()

		if err != nil {
			lastErr = fmt.Errorf("failed to read body: %w", err)
			time.Sleep(5 * time.Second)
			continue
		}

		// Handle rate limiting (429)
		if resp.StatusCode == http.StatusTooManyRequests {
			cgc.Logger.Warnf("Rate limited on page %d, waiting %v before retry...", page, RateLimitBackoff)
			time.Sleep(RateLimitBackoff)
			lastErr = fmt.Errorf("rate limited")
			continue
		}

		// Handle other errors
		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
			time.Sleep(5 * time.Second)
			continue
		}

		var tickerResp TickerResponse
		if err := json.Unmarshal(body, &tickerResp); err != nil {
			lastErr = fmt.Errorf("failed to unmarshal: %w", err)
			time.Sleep(5 * time.Second)
			continue
		}

		return tickerResp.Tickers, nil
	}

	return nil, fmt.Errorf("max retries exceeded for page %d: %w", page, lastErr)
}
