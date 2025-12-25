package coingecko

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"nobitex/radar/configs"
	"nobitex/radar/internal/scraper"
)

const (
	BaseURL          = "https://api.coingecko.com/api/v3"
	PollingInterval  = 24 * time.Hour
	RequestTimeout   = 30 * time.Second
	RateLimit        = 15 * time.Second // 4 requests per minute = 15 seconds between requests
	MaxRetries       = 10               // Maximum retry attempts for rate limit
	RateLimitBackoff = 60 * time.Second // Wait 60 seconds when rate limited
)

type CoinGeckoScraper struct {
	*scraper.BaseScraper
	httpClient   *http.Client
	Exchanges    *[]string
	ScheduleHour int
}

func NewCoinGeckoScraper(cfg *configs.Config) *CoinGeckoScraper {
	config := scraper.NewConfig("coingecko", 0, cfg)
	baseScraper := scraper.NewBaseScraper(config)

	return &CoinGeckoScraper{
		BaseScraper:  baseScraper,
		httpClient:   &http.Client{Timeout: RequestTimeout},
		Exchanges:    &cfg.CoingeckoCfg.ExchangesID,
		ScheduleHour: cfg.CoingeckoCfg.ScheduleHour,
	}
}

func (cgc *CoinGeckoScraper) Name() string {
	return "coingecko"
}

func (cgc *CoinGeckoScraper) Run(ctx context.Context) error {
	cgc.Logger.Info("Starting CoinGecko Crawler...")

	if err := cgc.InitKafkaProducer(); err != nil {
		return fmt.Errorf("failed to initialize Kafka: %w", err)
	}
	if cgc.Exchanges == nil {
		return fmt.Errorf("failed to run, no exchange set for coingecko")
	}
	defer func() {
		cgc.Logger.Info("Closing Kafka producer...")
		cgc.CloseKafkaProducer()
		cgc.Logger.Info("Kafka producer closed")
	}()

	// Calculate time until next scheduled run
	nextRun := cgc.calculateNextRun()
	waitDuration := time.Until(nextRun)

	cgc.Logger.Info("CoinGecko scheduled",
		"scheduleHour", cgc.ScheduleHour,
		"nextRun", nextRun.Format(time.RFC3339),
		"waitDuration", waitDuration.Round(time.Second),
	)

	// Wait until scheduled time
	select {
	case <-ctx.Done():
		cgc.Logger.Info("Shutdown signal received before first run")
		return nil
	case <-time.After(waitDuration):
		cgc.Logger.Info("Starting scheduled data fetch...")
		if err := cgc.fetchAndSend(ctx); err != nil {
			if err == context.Canceled {
				cgc.Logger.Info("Fetch cancelled")
				return nil
			}
			cgc.Logger.Error("Fetch failed", "error", err)
		}
	}

	// Run every 24 hours after first scheduled run
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

// calculateNextRun calculates the next run time based on the schedule hour in Asia/Tehran timezone
func (cgc *CoinGeckoScraper) calculateNextRun() time.Time {
	tehran, err := time.LoadLocation("Asia/Tehran")
	if err != nil {
		cgc.Logger.Warn("Failed to load Asia/Tehran timezone, using UTC", "error", err)
		tehran = time.UTC
	}

	now := time.Now().In(tehran)
	nextRun := time.Date(now.Year(), now.Month(), now.Day(), cgc.ScheduleHour, 0, 0, 0, tehran)

	// If scheduled time already passed today, schedule for tomorrow
	if now.After(nextRun) {
		nextRun = nextRun.Add(24 * time.Hour)
	}

	return nextRun
}

func (cgc *CoinGeckoScraper) fetchAndSend(ctx context.Context) error {
	cgc.Logger.Info("Fetching tickers from CoinGecko...")
	cgc.Logger.Info("Info", "exchanges", *cgc.Exchanges)

	totalSent := 0
	for _, e := range *cgc.Exchanges {
		page := 1
		for {
			select {
			case <-ctx.Done():
				cgc.Logger.Info("Context cancelled, stopping fetch...")
				return ctx.Err()
			default:
			}
			tickers, err := cgc.fetchPage(page, e)
			if err != nil {
				return err
			}

			if len(tickers) == 0 {
				break
			}

			cgc.Logger.Info("Fetched page", "exchange", e, "page", page, "tickers", len(tickers))

			sent := cgc.sendUSDPairs(tickers, e)
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
	}

	cgc.Logger.Info("Completed sending USD pairs to Kafka", "total", totalSent)
	return nil
}

func (cgc *CoinGeckoScraper) sendUSDPairs(tickers []Ticker, exchange string) int {
	sent := 0

	for _, ticker := range tickers {
		if !strings.EqualFold(ticker.Target, "USDT") {
			continue
		}
		volume := ticker.Volume
		if volume == 0 {
			volume = ticker.ConvertedVolume.USD
		}

		data := scraper.KafkaData{
			Exchange: exchange,
			Symbol:   fmt.Sprintf("%s/%s", ticker.Base, "USDT"),
			Price:    ticker.ConvertedLast.USD,
			Volume:   volume,
			Time:     ticker.LastFetchAt,
			Quantity: ticker.ConvertedVolume.USD,
			Side:     "all",
		}
		data.ID = scraper.GenerateTradeID(&data)

		// change json to proto
		protoData, err := scraper.SerializeProto(&data)
		if err != nil {
			cgc.Logger.Error("Failed to serialize", "ticker", ticker.Base, "error", err)
			continue
		}

		if err := cgc.SendToKafka(protoData); err != nil {
			cgc.Logger.Error("Failed to send to Kafka", "ticker", ticker.Base, "error", err)
			continue
		}

		sent++
	}

	return sent
}

func (cgc *CoinGeckoScraper) fetchPage(page int, exchange string) ([]Ticker, error) {
	url := fmt.Sprintf("%s/exchanges/%s/tickers?page=%d&order=base_target", BaseURL, exchange, page)

	var lastErr error
	for attempt := range MaxRetries {
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
