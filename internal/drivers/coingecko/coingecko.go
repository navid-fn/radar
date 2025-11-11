package coingecko

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"nobitex/radar/internal/crawler"
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

	cgc.Logger.Info("Starting initial data fetch...")
	if err := cgc.fetchAndSend(ctx); err != nil {
		if err == context.Canceled {
			cgc.Logger.Info("Initial fetch cancelled")
			return nil
		}
		cgc.Logger.Error("Initial fetch failed", "error", err)
	}

	ticker := time.NewTicker(PollingInterval)
	defer ticker.Stop()

	cgc.Logger.Info("Polling started", "interval", PollingInterval)

	for {
		select {
		case <-ctx.Done():
			cgc.Logger.Info("Shutdown signal received. Stopping CoinGecko crawler...")
			time.Sleep(2 * time.Second)
			return nil
		case <-ticker.C:
			cgc.Logger.Info("Starting scheduled data fetch...")
			if err := cgc.fetchAndSend(ctx); err != nil {
				if err == context.Canceled {
					cgc.Logger.Info("Fetch cancelled")
					return nil
				}
				cgc.Logger.Error("Fetch failed", "error", err)
			}
		}
	}
}

func (cgc *CoinGeckoCrawler) fetchAndSend(ctx context.Context) error {
	cgc.Logger.Info("Fetching tickers from CoinGecko...")

	page := 1
	totalSent := 0

	for {
		select {
		case <-ctx.Done():
			cgc.Logger.Info("Context cancelled, stopping fetch...")
			return ctx.Err()
		default:
		}

		tickers, err := cgc.fetchPage(page)
		if err != nil {
			return err
		}

		if len(tickers) == 0 {
			break
		}

		cgc.Logger.Info("Fetched page", "page", page, "tickers", len(tickers))

		sent := cgc.sendUSDPairs(tickers)
		totalSent += sent
		cgc.Logger.Info("Sent USD pairs to Kafka", "count", sent, "page", page)

		if len(tickers) < 100 {
			break
		}

		page++

		select {
		case <-ctx.Done():
			cgc.Logger.Info("Context cancelled during rate limit wait...")
			return ctx.Err()
		case <-time.After(RateLimit):
		}
	}

	cgc.Logger.Info("Completed sending USD pairs to Kafka", "total", totalSent)
	return nil
}

func (cgc *CoinGeckoCrawler) sendUSDPairs(tickers []Ticker) int {
	sent := 0

	for _, ticker := range tickers {
		if !strings.EqualFold(ticker.Target, "USDT") {
			continue
		}
		data := crawler.KafkaData{
			Exchange: "binance",
			Symbol:   fmt.Sprintf("%s/%s", ticker.Base, "USDT"),
			Price:    ticker.ConvertedLast.USD,
			Volume:   ticker.Volume,
			Time:     ticker.LastFetchAt,
			Quantity: ticker.ConvertedVolume.USD,
			Side:     "all",
		}
		jsonData, err := json.Marshal(data)
		if err != nil {
			cgc.Logger.Error("Failed to marshal", "ticker", ticker.Base, "error", err)
			continue
		}

		if err := cgc.SendToKafka(jsonData); err != nil {
			cgc.Logger.Error("Failed to send to Kafka", "ticker", ticker.Base, "error", err)
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
			cgc.Logger.Warn("Retry attempt", "attempt", attempt, "maxRetries", MaxRetries, "page", page)
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

		if resp.StatusCode == http.StatusTooManyRequests {
			cgc.Logger.Warn("Rate limited, waiting before retry", "page", page, "backoff", RateLimitBackoff)
			time.Sleep(RateLimitBackoff)
			lastErr = fmt.Errorf("rate limited")
			continue
		}

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
