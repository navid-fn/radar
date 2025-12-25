package bitpin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
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
	LatestTradeAPI         = "https://api.bitpin.org/api/v1/mth/matches/%s/"
	APIRateLimitPerMinute  = 60
	SafetyMargin           = 0.98
	DesiredPollingInterval = 1.0
	BurstSize              = 10
)

type BitpinAPIScraper struct {
	*scraper.BaseScraper
	httpConfig   *scraper.HTTPConfig
	pairIDToName map[int]string

	usdtPrice float64
	usdtMu    sync.RWMutex
}

func NewBitpinAPIScraper(cfg *configs.Config) *BitpinAPIScraper {
	config := scraper.NewConfig("bitpin-api", 0, cfg)
	BaseScraper := scraper.NewBaseScraper(config)

	return &BitpinAPIScraper{
		BaseScraper: BaseScraper,
		usdtPrice:   getLatestUSDTPrice(),
	}
}

func (bs *BitpinAPIScraper) Name() string {
	return "bitpin-api"
}

func (bs *BitpinAPIScraper) FetchMarkets() ([]string, error) {
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

	var apiResponse []market
	if err = json.Unmarshal(body, &apiResponse); err != nil {
		return nil, fmt.Errorf("error unmarshaling API response: %w", err)
	}

	for _, market := range apiResponse {
		if market.Tradeable && market.Symbol == "1INCH_IRT" {
			markets = append(markets, market.Symbol)
		}
	}

	sort.Strings(markets)

	bs.Logger.Info("Fetched unique markets from Bitpin API", "count", len(markets))
	return markets, nil
}

func (bs *BitpinAPIScraper) calculateOptimalRate(symbolCount int) float64 {
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

func (bs *BitpinAPIScraper) setupRateLimiter(symbolCount int) {
	optimalRate := bs.calculateOptimalRate(symbolCount)
	bs.httpConfig = scraper.DefaultHTTPConfig(LatestTradeAPI, optimalRate)
	bs.httpConfig.RateLimiter = rate.NewLimiter(rate.Limit(optimalRate), BurstSize)

	bs.Logger.Info("INFO", "USDT PRICE:", bs.usdtPrice)

	actualPollingInterval := float64(symbolCount) / optimalRate
	requestsPerMinute := optimalRate * 60

	separator := "=" + strings.Repeat("=", 70)
	bs.Logger.Info(separator)
	bs.Logger.Info("Dynamic Rate Limit Configuration")
	bs.Logger.Info("  - Total symbols", "count", symbolCount)
	bs.Logger.Info("  - Calculated rate",
		"requestsPerSecond", fmt.Sprintf("%.2f", optimalRate),
		"requestsPerMinute", fmt.Sprintf("%.1f", requestsPerMinute))
	bs.Logger.Info("  - API limit",
		"requestsPerMinute", APIRateLimitPerMinute,
		"safetyMargin", fmt.Sprintf("%.0f%%", SafetyMargin*100),
		"effectiveLimit", fmt.Sprintf("%.1f", APIRateLimitPerMinute*SafetyMargin))
	bs.Logger.Info("  - Polling interval",
		"seconds", fmt.Sprintf("%.1f", actualPollingInterval),
		"minutes", fmt.Sprintf("%.2f", actualPollingInterval/60))

	instancesFor1Sec := int(float64(symbolCount) / (APIRateLimitPerMinute * SafetyMargin / 60))

	if actualPollingInterval > 60 {
		bs.Logger.Warn("PERFORMANCE WARNING: Long polling interval",
			"intervalMinutes", fmt.Sprintf("%.1f", actualPollingInterval/60),
			"instancesNeeded", instancesFor1Sec)
	} else if actualPollingInterval > 10 {
		bs.Logger.Warn("Polling interval",
			"seconds", fmt.Sprintf("%.1f", actualPollingInterval))
	} else {
		bs.Logger.Info("Polling rate is acceptable", "symbols", symbolCount)
	}

	bs.Logger.Info(separator)
}

func (bs *BitpinAPIScraper) fetchTrades(ctx context.Context, symbol string) error {
	if err := bs.httpConfig.RateLimiter.Wait(ctx); err != nil {
		return err
	}

	resp, err := http.Get(fmt.Sprintf(LatestTradeAPI, symbol))
	if err != nil {
		bs.Logger.Error("Error calling API", "symbol", symbol, "error", err)
		time.Sleep(2 * time.Second)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bs.Logger.Error("API returned error status", "symbol", symbol, "status", resp.StatusCode)
		return fmt.Errorf("API returned status %d", resp.StatusCode)
	}
	var tradeData []map[string]any

	if err := json.NewDecoder(resp.Body).Decode(&tradeData); err != nil {
		bs.Logger.Error("Error decoding API response", "symbol", symbol, "error", err)
		return err
	}

	sentCount := 0

	for _, t := range tradeData {
		id := t["id"].(string)
		side := t["side"].(string)
		priceString := t["price"].(string)
		volumeString := t["base_amount"].(string)

		price, _ := strconv.ParseFloat(priceString, 64)
		volume, _ := strconv.ParseFloat(volumeString, 64)

		quantity := price * volume
		timeStamp := t["time"].(float64)

		sec, dec := math.Modf(timeStamp)
		nsec := int64(math.Round(dec * 1e9))
		tradeTime := time.Unix(int64(sec), nsec)

		cleanedSymbol := cleanSymbol(symbol)

		if cleanedSymbol == "USDT/IRT" {
			bs.usdtMu.Lock()
			bs.usdtPrice = price
			bs.usdtMu.Unlock()
		}

		data := scraper.KafkaData{
			Exchange:  "bitpin",
			Symbol:    cleanedSymbol,
			Side:      side,
			ID:        id,
			Price:     price,
			Volume:    volume,
			Quantity:  quantity,
			Time:      tradeTime.Format(time.RFC3339),
			USDTPrice: bs.usdtPrice,
		}

		protoData, err := scraper.SerializeProto(&data)
		if err != nil {
			bs.Logger.Error("Error serializing trade", "symbol", symbol, "error", err)
			continue
		}

		if err := bs.SendToKafka(protoData); err != nil {
			bs.Logger.Error("Failed to send to Kafka", "symbol", symbol, "error", err)
			continue
		}
		sentCount++
	}

	return nil
}

func (bs *BitpinAPIScraper) Run(ctx context.Context) error {
	bs.Logger.Info("Starting Bitpin API Crawler...")

	if err := bs.InitKafkaProducer(); err != nil {
		return fmt.Errorf("failed to initialize Kafka producer: %w", err)
	}
	defer bs.CloseKafkaProducer()

	symbols, err := bs.FetchMarkets()
	if err != nil {
		return fmt.Errorf("could not fetch markets: %w", err)
	}

	if len(symbols) == 0 {
		return fmt.Errorf("no symbols found to track")
	}

	bs.setupRateLimiter(len(symbols))

	return scraper.RunWithGracefulShutdown(bs.Logger, func(ctx context.Context, wg *sync.WaitGroup) {
		for _, symbol := range symbols {
			wg.Add(1)
			go func(sym string) {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					default:
						if err := bs.fetchTrades(ctx, sym); err != nil {
						}
					}
				}
			}(symbol)
		}
	})
}
