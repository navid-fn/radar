package nobitex

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"nobitex/radar/configs"
	"nobitex/radar/internal/scraper"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	APIRateLimitPerMinute  = 60
	SafetyMargin           = 0.98
	DesiredPollingInterval = 1.0
	BurstSize              = 10
)

type NobitexAPIScraper struct {
	*scraper.BaseScraper
	httpConfig *scraper.HTTPConfig
	usdtPrice  float64
	usdtMu     sync.RWMutex
}

func NewNobitexAPIScraper(cfg *configs.Config) *NobitexAPIScraper {
	config := scraper.NewConfig("nobitex-api", 0, cfg)
	baseCrawler := scraper.NewBaseScraper(config)

	return &NobitexAPIScraper{
		BaseScraper: baseCrawler,
		usdtPrice:   getLatestUSDTPrice(),
	}
}

func (ns *NobitexAPIScraper) Name() string {
	return "nobitex-api"
}

func transformPair(pair string) string {
	parts := strings.Split(pair, "-")
	for i, part := range parts {
		parts[i] = strings.ToUpper(part)
	}
	if len(parts) > 1 && parts[len(parts)-1] == "RLS" {
		parts[len(parts)-1] = "IRT"
	}
	return strings.Join(parts, "")
}

func (ns *NobitexAPIScraper) FetchMarkets() ([]string, error) {
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

	ns.Logger.Info("Fetched symbols from Nobitex", "count", len(symbols))
	return symbols, nil
}

func (ns *NobitexAPIScraper) calculateOptimalRate(symbolCount int) float64 {
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

func (ns *NobitexAPIScraper) setupRateLimiter(symbolCount int) {
	optimalRate := ns.calculateOptimalRate(symbolCount)
	ns.httpConfig = scraper.DefaultHTTPConfig(LatestTradeAPI, optimalRate)
	ns.httpConfig.RateLimiter = rate.NewLimiter(rate.Limit(optimalRate), BurstSize)

	actualPollingInterval := float64(symbolCount) / optimalRate
	requestsPerMinute := optimalRate * 60

	separator := "=" + strings.Repeat("=", 70)
	ns.Logger.Info(separator)
	ns.Logger.Info("Dynamic Rate Limit Configuration")
	ns.Logger.Info("  - Total symbols", "count", symbolCount)
	ns.Logger.Info("  - Calculated rate",
		"requestsPerSecond", fmt.Sprintf("%.2f", optimalRate),
		"requestsPerMinute", fmt.Sprintf("%.1f", requestsPerMinute))
	ns.Logger.Info("  - API limit",
		"requestsPerMinute", APIRateLimitPerMinute,
		"safetyMargin", fmt.Sprintf("%.0f%%", SafetyMargin*100),
		"effectiveLimit", fmt.Sprintf("%.1f", APIRateLimitPerMinute*SafetyMargin))
	ns.Logger.Info("  - Polling interval",
		"seconds", fmt.Sprintf("%.1f", actualPollingInterval),
		"minutes", fmt.Sprintf("%.2f", actualPollingInterval/60))

	instancesFor1Sec := int(float64(symbolCount) / (APIRateLimitPerMinute * SafetyMargin / 60))

	if actualPollingInterval > 60 {
		ns.Logger.Warn("PERFORMANCE WARNING: Long polling interval",
			"intervalMinutes", fmt.Sprintf("%.1f", actualPollingInterval/60),
			"instancesNeeded", instancesFor1Sec)
	} else if actualPollingInterval > 10 {
		ns.Logger.Warn("Polling interval",
			"seconds", fmt.Sprintf("%.1f", actualPollingInterval))
	} else {
		ns.Logger.Info("Polling rate is acceptable", "symbols", symbolCount)
	}
	ns.Logger.Info("USDT price", "USDT price", int64(ns.usdtPrice))

	ns.Logger.Info(separator)
}

func (ns *NobitexAPIScraper) fetchTrades(ctx context.Context, symbol string) error {
	if err := ns.httpConfig.RateLimiter.Wait(ctx); err != nil {
		return err
	}

	resp, err := http.Get(fmt.Sprintf("%s%s", LatestTradeAPI, symbol))
	if err != nil {
		ns.Logger.Error("Error calling API", "symbol", symbol, "error", err)
		time.Sleep(2 * time.Second)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		ns.Logger.Error("API returned error status", "symbol", symbol, "status", resp.StatusCode)
		time.Sleep(2 * time.Second)
		return fmt.Errorf("API returned status %d", resp.StatusCode)
	}

	var tradeData tradeAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&tradeData); err != nil {
		ns.Logger.Error("Error decoding API response", "symbol", symbol, "error", err)
		return err
	}

	if tradeData.Status != "ok" {
		ns.Logger.Warn("API returned non-ok status", "symbol", symbol, "status", tradeData.Status)
		return nil
	}

	sentCount := 0

	for _, t := range tradeData.Trades {
		cleanedSymbol := cleanSymbol(symbol)

		price, _ := strconv.ParseFloat(t.Price, 64)
		cleanedPrice := cleanPrice(cleanedSymbol, price)

		volume, _ := strconv.ParseFloat(t.Volume, 64)

		if cleanedSymbol == "USDT/IRT" {
			ns.usdtMu.Lock()
			ns.usdtPrice = cleanedPrice 
			ns.usdtMu.Unlock()
		}
		tradeTime := scraper.TurnTimeStampToTime(t.Time, false)

		data := scraper.KafkaData{
			Exchange:  "nobitex",
			Symbol:    cleanedSymbol,
			Volume:    volume,
			Time:      tradeTime.Format(time.RFC3339),
			Price:     cleanedPrice,
			Quantity:  volume * cleanedPrice,
			Side:      t.Side,
			USDTPrice: ns.usdtPrice,
		}

		data.ID = scraper.GenerateTradeID(&data)
		// change json to proto
		protoData, err := scraper.SerializeProto(&data)
		if err != nil {
			ns.Logger.Error("Error serializing trade", "symbol", symbol, "error", err)
			continue
		}

		if err := ns.SendToKafka(protoData); err != nil {
			ns.Logger.Error("Failed to send to Kafka", "symbol", symbol, "error", err)
			continue
		}

		sentCount++
	}

	return nil
}

func (ns *NobitexAPIScraper) Run(ctx context.Context) error {
	ns.Logger.Info("Starting Nobitex Crawler...")

	if err := ns.InitKafkaProducer(); err != nil {
		return fmt.Errorf("failed to initialize Kafka producer: %w", err)
	}
	defer ns.CloseKafkaProducer()

	symbols, err := ns.FetchMarkets()
	if err != nil {
		return fmt.Errorf("could not fetch markets: %w", err)
	}

	if len(symbols) == 0 {
		return fmt.Errorf("no symbols found to track")
	}

	ns.setupRateLimiter(len(symbols))

	return scraper.RunWithGracefulShutdown(ns.Logger, func(ctx context.Context, wg *sync.WaitGroup) {
		for _, symbol := range symbols {
			wg.Add(1)
			go func(sym string) {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						ns.Logger.Info("Stopping trade fetcher for symbol", "symbol", sym)
						return
					default:
						if err := ns.fetchTrades(ctx, sym); err != nil {
						}
					}
				}
			}(symbol)
		}
	})
}
