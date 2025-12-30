package coingecko

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"nobitex/radar/configs"
	"nobitex/radar/internal/proto"
	"nobitex/radar/internal/scraper"

	"github.com/segmentio/kafka-go"
)

const (
	baseURL         = "https://api.coingecko.com/api/v3"
	pollingInterval = 24 * time.Hour
	requestTimeout  = 30 * time.Second
	rateLimit       = 15 * time.Second
	maxRetries      = 10
	rateLimitWait   = 60 * time.Second
)

type CoinGeckoScraper struct {
	sender       *scraper.Sender
	logger       *slog.Logger
	httpClient   *http.Client
	exchanges    []string
	scheduleHour int
}

type TickerResponse struct {
	Tickers  []Ticker `json:"tickers"`
	Exchange string   `json:"-"`
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

func NewCoinGeckoScraper(kafkaWriter *kafka.Writer, logger *slog.Logger, cfg *configs.CoingeckoConfigs) *CoinGeckoScraper {
	return &CoinGeckoScraper{
		sender:       scraper.NewSender(kafkaWriter, logger),
		logger:       logger.With("scraper", "coingecko"),
		httpClient:   &http.Client{Timeout: requestTimeout},
		exchanges:    cfg.ExchangesID,
		scheduleHour: cfg.ScheduleHour,
	}
}

func (c *CoinGeckoScraper) Name() string { return "coingecko" }

func (c *CoinGeckoScraper) Run(ctx context.Context) error {
	c.logger.Info("Starting CoinGecko scraper", "exchanges", c.exchanges, "scheduleHour", c.scheduleHour)

	if len(c.exchanges) == 0 {
		return fmt.Errorf("no exchanges configured")
	}

	nextRun := c.nextRunTime()
	c.logger.Info("Scheduled", "nextRun", nextRun.Format(time.RFC3339))

	select {
	case <-ctx.Done():
		return nil
	case <-time.After(time.Until(nextRun)):
		if err := c.fetchAndSend(ctx); err != nil && ctx.Err() == nil {
			c.logger.Error("Fetch failed", "error", err)
		}
	}

	ticker := time.NewTicker(pollingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := c.fetchAndSend(ctx); err != nil && ctx.Err() == nil {
				c.logger.Error("Fetch failed", "error", err)
			}
		}
	}
}

func (c *CoinGeckoScraper) nextRunTime() time.Time {
	loc, err := time.LoadLocation("Asia/Tehran")
	if err != nil {
		loc = time.UTC
	}

	now := time.Now().In(loc)
	next := time.Date(now.Year(), now.Month(), now.Day(), c.scheduleHour, 0, 0, 0, loc)
	if now.After(next) {
		next = next.Add(24 * time.Hour)
	}
	return next
}

func (c *CoinGeckoScraper) fetchAndSend(ctx context.Context) error {
	c.logger.Info("Fetching tickers...")

	totalSent := 0
	for _, exchange := range c.exchanges {
		page := 1
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			tickers, err := c.fetchPage(page, exchange)
			if err != nil {
				return err
			}

			if len(tickers) == 0 {
				break
			}

			c.logger.Info("Fetched page", "exchange", exchange, "page", page, "tickers", len(tickers))

			sent := c.sendUSDPairs(ctx, tickers, exchange)
			totalSent += sent

			if len(tickers) < 100 {
				break
			}

			page++

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(rateLimit):
			}
		}
	}

	c.logger.Info("Completed", "totalSent", totalSent)
	return nil
}

func (c *CoinGeckoScraper) sendUSDPairs(ctx context.Context, tickers []Ticker, exchange string) int {
	sent := 0

	for _, ticker := range tickers {
		if !strings.EqualFold(ticker.Target, "USDT") {
			continue
		}

		volume := ticker.Volume
		if volume == 0 {
			volume = ticker.ConvertedVolume.USD
		}

		symbol := fmt.Sprintf("%s/USDT", ticker.Base)
		trade := &proto.TradeData{
			Id:       scraper.GenerateTradeID(exchange, symbol, ticker.LastFetchAt, ticker.ConvertedLast.USD, volume, "all"),
			Exchange: exchange,
			Symbol:   symbol,
			Price:    ticker.ConvertedLast.USD,
			Volume:   volume,
			Quantity: ticker.ConvertedVolume.USD,
			Side:     "all",
			Time:     ticker.LastFetchAt,
		}

		if err := c.sender.SendTrade(ctx, trade); err != nil {
			c.logger.Debug("Send error", "error", err)
			continue
		}
		sent++
	}

	return sent
}

func (c *CoinGeckoScraper) fetchPage(page int, exchange string) ([]Ticker, error) {
	url := fmt.Sprintf("%s/exchanges/%s/tickers?page=%d&order=base_target", baseURL, exchange, page)

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			c.logger.Warn("Retry", "attempt", attempt, "page", page)
		}

		resp, err := c.httpClient.Get(url)
		if err != nil {
			lastErr = err
			time.Sleep(5 * time.Second)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			time.Sleep(5 * time.Second)
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			c.logger.Warn("Rate limited, waiting", "backoff", rateLimitWait)
			time.Sleep(rateLimitWait)
			lastErr = fmt.Errorf("rate limited")
			continue
		}

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("status %d", resp.StatusCode)
			time.Sleep(5 * time.Second)
			continue
		}

		var data TickerResponse
		if err := json.Unmarshal(body, &data); err != nil {
			lastErr = err
			time.Sleep(5 * time.Second)
			continue
		}

		return data.Tickers, nil
	}

	return nil, fmt.Errorf("max retries exceeded: %w", lastErr)
}
