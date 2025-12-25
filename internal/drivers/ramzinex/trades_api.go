package ramzinex

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"nobitex/radar/configs"
	"nobitex/radar/internal/scraper"

	"golang.org/x/time/rate"
)

const (
	MarketAPI              = RamzinexAPIUrl
	LatestTradeAPI         = "https://publicapi.ramzinex.com/exchange/api/v1.0/exchange/orderbooks/%d/trades"
	APIRateLimitPerMinute  = 60
	SafetyMargin           = 0.98
	DesiredPollingInterval = 1.0
	BurstSize              = 10
)

type RamzinexAPIScraper struct {
	*scraper.BaseScraper
	httpConfig   *scraper.HTTPConfig
	pairIDToName map[int]string

	usdtPrice float64
	usdtMu    sync.RWMutex
}

func NewRamzinexAPIScraper(cfg *configs.Config) *RamzinexAPIScraper {
	config := scraper.NewConfig("ramzinex-api", 0, cfg)
	baseScraper := scraper.NewBaseScraper(config)

	return &RamzinexAPIScraper{
		BaseScraper:  baseScraper,
		pairIDToName: make(map[int]string),
		usdtPrice:    float64(getLatestUSDTPrice()),
	}
}

func (rs *RamzinexAPIScraper) Name() string {
	return "ramzinex-api"
}

func (rs *RamzinexAPIScraper) FetchMarkets() ([]pairDetail, error) {
	resp, err := http.Get(RamzinexAPIUrl)
	if err != nil {
		return nil, fmt.Errorf("error fetching pairs from Ramzinex API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading API response: %w", err)
	}

	var pairDataApi apiPairResponse
	if err = json.Unmarshal(body, &pairDataApi); err != nil {
		return nil, fmt.Errorf("error unmarshaling API response: %w", err)
	}

	if pairDataApi.Status != 0 {
		return nil, &APIError{}
	}

	var pairs []pairDetail

	for _, pd := range pairDataApi.Data.Pairs {
		pairDetail := pairDetail{
			ID:   pd.ID,
			Name: pd.Name.En,
		}
		pairs = append(pairs, pairDetail)
		rs.pairIDToName[pd.ID] = pd.Name.En
	}

	rs.Logger.Info("Fetched pairs from Ramzinex API", "count", len(pairs))
	return pairs, nil
}

func (rs *RamzinexAPIScraper) calculateOptimalRate(symbolCount int) float64 {
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

func (rs *RamzinexAPIScraper) setupRateLimiter(symbolCount int) {
	rs.Logger.Info("INFO", "USDT PRICE:", rs.usdtPrice)
	optimalRate := rs.calculateOptimalRate(symbolCount)
	rs.httpConfig = scraper.DefaultHTTPConfig(LatestTradeAPI, optimalRate)
	rs.httpConfig.RateLimiter = rate.NewLimiter(rate.Limit(optimalRate), BurstSize)

	actualPollingInterval := float64(symbolCount) / optimalRate
	requestsPerMinute := optimalRate * 60

	separator := "=" + strings.Repeat("=", 70)
	rs.Logger.Info(separator)
	rs.Logger.Info("Dynamic Rate Limit Configuration")
	rs.Logger.Info("  - Total symbols", "count", symbolCount)
	rs.Logger.Info("  - Calculated rate",
		"requestsPerSecond", fmt.Sprintf("%.2f", optimalRate),
		"requestsPerMinute", fmt.Sprintf("%.1f", requestsPerMinute))
	rs.Logger.Info("  - API limit",
		"requestsPerMinute", APIRateLimitPerMinute,
		"safetyMargin", fmt.Sprintf("%.0f%%", SafetyMargin*100),
		"effectiveLimit", fmt.Sprintf("%.1f", APIRateLimitPerMinute*SafetyMargin))
	rs.Logger.Info("  - Polling interval",
		"seconds", fmt.Sprintf("%.1f", actualPollingInterval),
		"minutes", fmt.Sprintf("%.2f", actualPollingInterval/60))

	instancesFor1Sec := int(float64(symbolCount) / (APIRateLimitPerMinute * SafetyMargin / 60))

	if actualPollingInterval > 60 {
		rs.Logger.Warn("PERFORMANCE WARNING: Long polling interval",
			"intervalMinutes", fmt.Sprintf("%.1f", actualPollingInterval/60),
			"instancesNeeded", instancesFor1Sec)
	} else if actualPollingInterval > 10 {
		rs.Logger.Warn("Polling interval",
			"seconds", fmt.Sprintf("%.1f", actualPollingInterval))
	} else {
		rs.Logger.Info("Polling rate is acceptable", "symbols", symbolCount)
	}

	rs.Logger.Info(separator)
}

func (rs *RamzinexAPIScraper) fetchTrades(ctx context.Context, symbol int) error {
	if err := rs.httpConfig.RateLimiter.Wait(ctx); err != nil {
		return err
	}

	resp, err := http.Get(fmt.Sprintf(LatestTradeAPI, symbol))
	if err != nil {
		rs.Logger.Error("Error calling API", "symbol", symbol, "error", err)
		time.Sleep(2 * time.Second)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		rs.Logger.Error("API returned error status", "symbol", symbol, "status", resp.StatusCode)
		return fmt.Errorf("API returned status %d", resp.StatusCode)
	}

	var tradeData latestAPIData
	if err := json.NewDecoder(resp.Body).Decode(&tradeData); err != nil {
		rs.Logger.Error("Error decoding API response", "symbol", symbol, "error", err)
		return err
	}

	if tradeData.Status != 0 {
		rs.Logger.Warn("API returned non-ok status", "symbol", symbol, "status", tradeData.Status)
		return nil
	}

	sentCount := 0

	for _, t := range tradeData.Data {
		row, ok := t.([]any)
		if !ok || len(row) < 6 {
			continue
		}

		cleanedSymbol := cleanSymbol(strings.ToUpper(rs.pairIDToName[symbol]))
		id := row[5].(string)
		side := row[3].(string)
		cleanedPrice := cleanPrice(cleanedSymbol, row[0].(float64))

		volume := row[1].(float64)
		quantity := cleanedPrice * volume
		time := row[2].(string)

		if cleanedSymbol == "USDT/IRT" {
			rs.usdtMu.Lock()
			rs.usdtPrice = cleanedPrice
			rs.usdtMu.Unlock()
		}

		data := scraper.KafkaData{
			Exchange:  "ramzinex",
			Symbol:    cleanedSymbol,
			Side:      side,
			ID:        id,
			Price:     cleanedPrice,
			Volume:    volume,
			Quantity:  quantity,
			Time:      time,
			USDTPrice: rs.usdtPrice,
		}

		// turn json to proto
		protoData, err := scraper.SerializeProto(&data)
		if err != nil {
			rs.Logger.Error("Error serializing trade", "symbol", symbol, "error", err)
			continue
		}

		if err := rs.SendToKafka(protoData); err != nil {
			rs.Logger.Error("Failed to send to Kafka", "symbol", symbol, "error", err)
			continue
		}
		sentCount++
	}

	return nil
}

func (rs *RamzinexAPIScraper) Run(ctx context.Context) error {
	rs.Logger.Info("Starting Ramzinex API Crawler...")

	if err := rs.InitKafkaProducer(); err != nil {
		return fmt.Errorf("failed to initialize Kafka producer: %w", err)
	}
	defer rs.CloseKafkaProducer()

	symbols, err := rs.FetchMarkets()
	if err != nil {
		return fmt.Errorf("could not fetch markets: %w", err)
	}

	if len(symbols) == 0 {
		return fmt.Errorf("no symbols found to track")
	}

	rs.setupRateLimiter(len(symbols))

	return scraper.RunWithGracefulShutdown(rs.Logger, func(ctx context.Context, wg *sync.WaitGroup) {
		for _, symbol := range symbols {
			wg.Add(1)
			go func(sym int) {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					default:
						if err := rs.fetchTrades(ctx, sym); err != nil {
						}
					}
				}
			}(symbol.ID)
		}
	})
}
