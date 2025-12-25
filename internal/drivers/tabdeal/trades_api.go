package tabdeal

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"nobitex/radar/configs"
	"nobitex/radar/internal/scraper"

	"golang.org/x/time/rate"
)

const (
	TabdealTradesURL = "https://api1.tabdeal.org/r/api/v1/trades"
	TabdealMarketURL = "https://api1.tabdeal.org/r/api/v1/exchangeInfo"
	TradesLimit      = 10

	// Rate limiting constants
	APIRateLimitPerMinute  = 60
	SafetyMargin           = 0.9
	DesiredPollingInterval = 1.0
	BurstSize              = 5

	// Backoff for errors
	ErrorBackoff       = 5 * time.Second
	GatewayBackoff     = 30 * time.Second // Longer backoff for 502/503/504
	MaxConsecErrors    = 5
	ConsecErrorBackoff = 60 * time.Second
)

type TabdealScraper struct {
	*scraper.BaseScraper
	rateLimiter *rate.Limiter
	usdtPrice   float64
	usdtMu      sync.RWMutex
}

func NewTabdealScraper(cfg *configs.Config) *TabdealScraper {
	config := scraper.NewConfig("tabdeal", 0, cfg)
	baseCrawler := scraper.NewBaseScraper(config)

	return &TabdealScraper{
		BaseScraper: baseCrawler,
		usdtPrice:   getLatestUSDTPrice(),
	}
}

func (ts *TabdealScraper) calculateOptimalRate(symbolCount int) float64 {
	maxRatePerSecond := (APIRateLimitPerMinute * SafetyMargin) / 60.0
	desiredRate := float64(symbolCount) / DesiredPollingInterval

	optimalRate := desiredRate
	if optimalRate > maxRatePerSecond {
		optimalRate = maxRatePerSecond
	}

	minRate := 0.1
	if optimalRate < minRate {
		optimalRate = minRate
	}

	return optimalRate
}

func (ts *TabdealScraper) setupRateLimiter(symbolCount int) {
	optimalRate := ts.calculateOptimalRate(symbolCount)
	ts.rateLimiter = rate.NewLimiter(rate.Limit(optimalRate), BurstSize)

	actualPollingInterval := float64(symbolCount) / optimalRate
	requestsPerMinute := optimalRate * 60

	separator := strings.Repeat("=", 70)
	ts.Logger.Info(separator)
	ts.Logger.Info("Tabdeal Rate Limit Configuration")
	ts.Logger.Info("  - Total symbols", "count", symbolCount)
	ts.Logger.Info("  - Calculated rate",
		"requestsPerSecond", fmt.Sprintf("%.2f", optimalRate),
		"requestsPerMinute", fmt.Sprintf("%.1f", requestsPerMinute))
	ts.Logger.Info("  - API limit",
		"requestsPerMinute", APIRateLimitPerMinute,
		"safetyMargin", fmt.Sprintf("%.0f%%", SafetyMargin*100))
	ts.Logger.Info("  - Polling interval",
		"seconds", fmt.Sprintf("%.1f", actualPollingInterval))
	ts.Logger.Info("  - USDT price", "price", int64(ts.usdtPrice))
	ts.Logger.Info(separator)
}

func (ts *TabdealScraper) Name() string {
	return "tabdeal"
}

func (ts *TabdealScraper) FetchMarkets() ([]string, error) {
	resp, err := http.Get(TabdealMarketURL)
	if err != nil {
		return nil, fmt.Errorf("error fetching markets: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %w", err)
	}

	// Check for gateway errors (502, 503, 504)
	if resp.StatusCode >= 500 {
		ts.Logger.Warn("Gateway error fetching markets",
			"status", resp.StatusCode,
			"isHTML", isHTMLResponse(body))
		return nil, fmt.Errorf("gateway error: status %d", resp.StatusCode)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned status code: %d", resp.StatusCode)
	}

	// Check if response is HTML (error page) instead of JSON
	if isHTMLResponse(body) {
		return nil, fmt.Errorf("received HTML error page instead of JSON")
	}

	var apiResponse []market
	if err = json.Unmarshal(body, &apiResponse); err != nil {
		ts.Logger.Debug("Failed to parse response", "body", string(body[:min(200, len(body))]))
		return nil, fmt.Errorf("error unmarshaling API response: %w", err)
	}

	var symbols []string
	for _, r := range apiResponse {
		if r.Status == "TRADING" {
			symbols = append(symbols, r.Symbol)
		}
	}

	ts.Logger.Info("Fetched active trading symbols", "count", len(symbols))
	return symbols, nil
}

// isHTMLResponse checks if the response body looks like HTML
func isHTMLResponse(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	trimmed := strings.TrimSpace(string(body))
	return strings.HasPrefix(trimmed, "<!") ||
		strings.HasPrefix(trimmed, "<html") ||
		strings.HasPrefix(trimmed, "<HTML") ||
		strings.Contains(trimmed[:min(500, len(trimmed))], "<!DOCTYPE")
}

// fetchTradesError wraps error with additional context for backoff decisions
type fetchTradesError struct {
	err            error
	isGatewayError bool
}

func (e *fetchTradesError) Error() string {
	return e.err.Error()
}

func (ts *TabdealScraper) fetchTrades(ctx context.Context, symbol string) error {
	// Wait for rate limiter
	if err := ts.rateLimiter.Wait(ctx); err != nil {
		return err // Context cancelled
	}

	url := fmt.Sprintf("%s?symbol=%s&limit=%d", TabdealTradesURL, symbol, TradesLimit)

	resp, err := http.Get(url)
	if err != nil {
		return &fetchTradesError{err: fmt.Errorf("error calling API: %w", err), isGatewayError: false}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return &fetchTradesError{err: fmt.Errorf("error reading response body: %w", err), isGatewayError: false}
	}

	// Check for gateway errors (502, 503, 504)
	if resp.StatusCode >= 500 {
		ts.Logger.Warn("Gateway error",
			"symbol", symbol,
			"status", resp.StatusCode,
			"isHTML", isHTMLResponse(body))
		return &fetchTradesError{
			err:            fmt.Errorf("gateway error: status %d", resp.StatusCode),
			isGatewayError: true,
		}
	}

	if resp.StatusCode != http.StatusOK {
		return &fetchTradesError{
			err:            fmt.Errorf("API error: status %d", resp.StatusCode),
			isGatewayError: resp.StatusCode >= 500,
		}
	}

	// Check if response is HTML (error page) instead of JSON
	if isHTMLResponse(body) {
		ts.Logger.Warn("Received HTML error page", "symbol", symbol)
		return &fetchTradesError{
			err:            fmt.Errorf("received HTML error page instead of JSON"),
			isGatewayError: true,
		}
	}

	var trades []tradesInfo
	if err = json.Unmarshal(body, &trades); err != nil {
		ts.Logger.Debug("Failed to parse trades response",
			"symbol", symbol,
			"bodyPreview", string(body[:min(200, len(body))]))
		return &fetchTradesError{
			err:            fmt.Errorf("error unmarshaling API response: %w", err),
			isGatewayError: false,
		}
	}

	for _, t := range trades {
		volume, _ := strconv.ParseFloat(t.Qty, 64)
		price, _ := strconv.ParseFloat(t.Price, 64)
		quantity := volume * price
		tradeID := strconv.FormatInt(int64(t.ID), 10)
		side := "sell"
		if t.Buyer {
			side = "buy"
		}
		cleanedSymbol := cleanSymbol(symbol)

		if cleanedSymbol == "USDT/IRT" {
			ts.usdtMu.Lock()
			ts.usdtPrice = price
			ts.usdtMu.Unlock()
		}

		tradeTime := scraper.TurnTimeStampToTime(t.Time, false)

		data := scraper.KafkaData{
			Exchange:  "tabdeal",
			ID:        tradeID,
			Side:      side,
			Volume:    volume,
			Symbol:    cleanedSymbol,
			Price:     price,
			Quantity:  quantity,
			Time:      tradeTime.Format(time.RFC3339),
			USDTPrice: ts.usdtPrice,
		}

		protoData, err := scraper.SerializeProto(&data)
		if err != nil {
			ts.Logger.Error("Error serializing trade", "error", err)
			continue
		}

		if err := ts.SendToKafka(protoData); err != nil {
			ts.Logger.Error("Failed to send to Kafka", "error", err)
			continue
		}
	}
	return nil
}

func (ts *TabdealScraper) Run(ctx context.Context) error {
	ts.Logger.Info("Starting Tabdeal Crawler...")

	if err := ts.InitKafkaProducer(); err != nil {
		return fmt.Errorf("failed to initialize Kafka producer: %w", err)
	}
	defer ts.CloseKafkaProducer()

	symbols, err := ts.FetchMarkets()
	if err != nil {
		return fmt.Errorf("could not fetch markets: %w", err)
	}

	if len(symbols) == 0 {
		return fmt.Errorf("no symbols found to track")
	}

	// Setup rate limiter based on symbol count
	ts.setupRateLimiter(len(symbols))

	return scraper.RunWithGracefulShutdown(ts.Logger, func(ctx context.Context, wg *sync.WaitGroup) {
		for _, symbol := range symbols {
			wg.Add(1)
			go func(sym string) {
				defer wg.Done()
				ts.Logger.Info("Starting trade fetcher for symbol", "symbol", sym)

				consecErrors := 0

				for {
					select {
					case <-ctx.Done():
						ts.Logger.Info("Stopping trade fetcher for symbol", "symbol", sym)
						return
					default:
					}

					err := ts.fetchTrades(ctx, sym)
					if err != nil {
						// Check if context was cancelled (graceful shutdown)
						if ctx.Err() != nil {
							ts.Logger.Info("Context cancelled, stopping", "symbol", sym)
							return
						}

						consecErrors++

						// Determine backoff duration based on error type
						backoff := ErrorBackoff
						if ftErr, ok := err.(*fetchTradesError); ok && ftErr.isGatewayError {
							backoff = GatewayBackoff
							ts.Logger.Warn("Gateway error, backing off",
								"symbol", sym,
								"backoff", backoff,
								"consecErrors", consecErrors)
						} else {
							ts.Logger.Error("Error fetching trades",
								"symbol", sym,
								"error", err,
								"consecErrors", consecErrors)
						}

						// Extended backoff after many consecutive errors
						if consecErrors >= MaxConsecErrors {
							backoff = ConsecErrorBackoff
							ts.Logger.Warn("Too many consecutive errors, extended backoff",
								"symbol", sym,
								"backoff", backoff)
						}

						// Sleep with context check
						select {
						case <-ctx.Done():
							ts.Logger.Info("Context cancelled during backoff", "symbol", sym)
							return
						case <-time.After(backoff):
						}
					} else {
						// Reset consecutive error counter on success
						consecErrors = 0
					}
				}
			}(symbol)
		}
	})
}
