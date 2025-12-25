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

	"golang.org/x/time/rate"
)

const (
	MarketAPI              = WallexAPIURL
	LatestTradeAPI         = "https://api.wallex.ir/v1/trades"
	APIRateLimitPerMinute  = 60
	SafetyMargin           = 0.98
	DesiredPollingInterval = 1.0
	BurstSize              = 10
)

type WallexAPIScraper struct {
	*scraper.BaseScraper
	httpConfig *scraper.HTTPConfig

	usdtPrice float64
	usdtMu    sync.RWMutex
}

func NewWallexAPIScraper(cfg *configs.Config) *WallexAPIScraper {
	config := scraper.NewConfig("wallex-api", 0, cfg)
	baseScraper := scraper.NewBaseScraper(config)

	return &WallexAPIScraper{
		BaseScraper: baseScraper,
		usdtPrice:   getLatestUSDTPrice(),
	}
}

func (wc *WallexAPIScraper) Name() string {
	return "wallex-api"
}

func (wc *WallexAPIScraper) FetchMarkets() ([]string, error) {
	resp, err := http.Get(WallexAPIURL)
	if err != nil {
		return nil, fmt.Errorf("error fetching markets from Wallex API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("FecthMarket API returned status code: %d", resp.StatusCode)
	}

	var response apiMarketResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("FetchMarket error decoding API response: %w", err)
	}

	var markets []string
	for _, market := range response.Result.Markets {
		if market.Enabled {
			markets = append(markets, market.Symbol)
		}
	}

	sort.Strings(markets)

	wc.Logger.Info("Fetched unique markets from Wallex API", "count", len(markets))
	return markets, nil
}

func (wc *WallexAPIScraper) calculateOptimalRate(symbolCount int) float64 {
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

func (wc *WallexAPIScraper) setupRateLimiter(symbolCount int) {
	optimalRate := wc.calculateOptimalRate(symbolCount)
	wc.httpConfig = scraper.DefaultHTTPConfig(LatestTradeAPI, optimalRate)
	wc.httpConfig.RateLimiter = rate.NewLimiter(rate.Limit(optimalRate), BurstSize)

	actualPollingInterval := float64(symbolCount) / optimalRate
	requestsPerMinute := optimalRate * 60

	separator := "=" + strings.Repeat("=", 70)
	wc.Logger.Info(separator)
	wc.Logger.Info("Dynamic Rate Limit Configuration")
	wc.Logger.Info("  - Total symbols", "count", symbolCount)
	wc.Logger.Info("  - Calculated rate",
		"requestsPerSecond", fmt.Sprintf("%.2f", optimalRate),
		"requestsPerMinute", fmt.Sprintf("%.1f", requestsPerMinute))
	wc.Logger.Info("  - API limit",
		"requestsPerMinute", APIRateLimitPerMinute,
		"safetyMargin", fmt.Sprintf("%.0f%%", SafetyMargin*100),
		"effectiveLimit", fmt.Sprintf("%.1f", APIRateLimitPerMinute*SafetyMargin))
	wc.Logger.Info("  - Polling interval",
		"seconds", fmt.Sprintf("%.1f", actualPollingInterval),
		"minutes", fmt.Sprintf("%.2f", actualPollingInterval/60))

	instancesFor1Sec := int(float64(symbolCount) / (APIRateLimitPerMinute * SafetyMargin / 60))

	if actualPollingInterval > 60 {
		wc.Logger.Warn("PERFORMANCE WARNING: Long polling interval",
			"intervalMinutes", fmt.Sprintf("%.1f", actualPollingInterval/60),
			"instancesNeeded", instancesFor1Sec)
	} else if actualPollingInterval > 10 {
		wc.Logger.Warn("Polling interval",
			"seconds", fmt.Sprintf("%.1f", actualPollingInterval))
	} else {
		wc.Logger.Info("Polling rate is acceptable", "symbols", symbolCount)
	}

	wc.Logger.Info(separator)
}

func (ws *WallexAPIScraper) fetchTrades(ctx context.Context, symbol string) error {
	if err := ws.httpConfig.RateLimiter.Wait(ctx); err != nil {
		return err
	}

	resp, err := http.Get(fmt.Sprintf("%s?symbol=%s", LatestTradeAPI, symbol))
	if err != nil {
		ws.Logger.Error("Error calling API", "symbol", symbol, "error", err)
		time.Sleep(2 * time.Second)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		ws.Logger.Error("API returned error status", "symbol", symbol, "status", resp.StatusCode)
		return fmt.Errorf("API returned status %d", resp.StatusCode)
	}

	var tradeData tradeAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&tradeData); err != nil {
		ws.Logger.Error("Error decoding API response", "symbol", symbol, "error", err)
		return err
	}

	if !tradeData.Status {
		ws.Logger.Warn("API returned non-ok status", "symbol", symbol, "status", tradeData.Status)
		return nil
	}

	sentCount := 0

	for _, t := range tradeData.Result.Trades {
		t.Symbol = cleanSymbol(symbol)
		t.Exchange = "wallex"

		volume, _ := strconv.ParseFloat(t.Volume, 64)
		price, _ := strconv.ParseFloat(t.Price, 64)

		var side string
		if t.Side {
			side = "buy"
		} else {
			side = "sell"
		}

		if t.Symbol == "USDT/IRT" {
			ws.usdtMu.Lock()
			ws.usdtPrice = price
			ws.usdtMu.Unlock()
		}

		data := scraper.KafkaData{
			Exchange:  t.Exchange,
			Symbol:    t.Symbol,
			Volume:    volume,
			Time:      t.Time,
			Price:     price,
			Quantity:  volume * price,
			Side:      side,
			USDTPrice: ws.usdtPrice,
		}

		data.ID = scraper.GenerateTradeID(&data)

		protoData, err := scraper.SerializeProto(&data)

		if err != nil {
			ws.Logger.Error("Error serializing trade", "symbol", symbol, "error", err)
			continue
		}

		if err := ws.SendToKafka(protoData); err != nil {
			ws.Logger.Error("Failed to send to Kafka", "symbol", symbol, "error", err)
			continue
		}

		sentCount++
	}

	return nil
}

func (wc *WallexAPIScraper) Run(ctx context.Context) error {
	wc.Logger.Info("Starting Wallex API Scraper...")

	if err := wc.InitKafkaProducer(); err != nil {
		return fmt.Errorf("failed to initialize Kafka producer: %w", err)
	}
	defer wc.CloseKafkaProducer()

	symbols, err := wc.FetchMarkets()
	if err != nil {
		return fmt.Errorf("could not fetch markets: %w", err)
	}

	if len(symbols) == 0 {
		return fmt.Errorf("no symbols found to track")
	}

	wc.setupRateLimiter(len(symbols))

	return scraper.RunWithGracefulShutdown(wc.Logger, func(ctx context.Context, wg *sync.WaitGroup) {
		for _, symbol := range symbols {
			wg.Add(1)
			go func(sym string) {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						wc.Logger.Info("Stopping trade fetcher for symbol", "symbol", sym)
						return
					default:
						if err := wc.fetchTrades(ctx, sym); err != nil {
						}
					}
				}
			}(symbol)
		}
	})
}
