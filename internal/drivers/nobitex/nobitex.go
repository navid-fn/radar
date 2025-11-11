package nobitex

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"nobitex/radar/internal/crawler"
	"nobitex/radar/utils"
	"golang.org/x/time/rate"
)

const (
	LatestTradeAPI         = "https://apiv2.nobitex.ir/v2/trades/"
	MarketAPI              = "https://apiv2.nobitex.ir/market/stats"
	APIRateLimitPerMinute  = 60
	SafetyMargin           = 0.98
	DesiredPollingInterval = 1.0
	BurstSize              = 10
)

type TradeData struct {
	Symbol   string `json:"symbol"`
	Exchange string `json:"exchange"`
	Time     int64  `json:"time"`
	Price    string `json:"price"`
	Volume   string `json:"volume"`
	Side     string `json:"type"`
}

func (t TradeData) MarshalJSON() ([]byte, error) {
	type AliasTradesInfo TradeData
	return json.Marshal(&struct {
		AliasTradesInfo
		Time time.Time `json:"time"`
	}{
		AliasTradesInfo: AliasTradesInfo(t),
		Time:            time.UnixMilli(t.Time),
	})
}

type TradeAPIResponse struct {
	Status string      `json:"status"`
	Trades []TradeData `json:"trades"`
}

type SymbolData struct {
	IsClosed bool `json:"isClosed"`
}

type MarketDataAPIResponse struct {
	Status string
	Stats  map[string]SymbolData
}

type NobitexCrawler struct {
	*crawler.BaseCrawler
	httpConfig   *crawler.HTTPConfig
}

func NewNobitexCrawler() *NobitexCrawler {
	config := crawler.NewConfig("nobitex", 0)
	baseCrawler := crawler.NewBaseCrawler(config)

	return &NobitexCrawler{
		BaseCrawler:  baseCrawler,
	}
}

func (nc *NobitexCrawler) GetName() string {
	return "nobitex"
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

func (nc *NobitexCrawler) FetchMarkets() ([]string, error) {
	resp, err := http.Get(MarketAPI)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var symbols []string
	var marketData MarketDataAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&marketData); err != nil {
		return nil, fmt.Errorf("error decoding API response: %w", err)
	}

	for pair, stats := range marketData.Stats {
		if !stats.IsClosed {
			symbols = append(symbols, transformPair(pair))
		}
	}

	nc.Logger.Info("Fetched symbols from Nobitex", "count", len(symbols))
	return symbols, nil
}

func (nc *NobitexCrawler) calculateOptimalRate(symbolCount int) float64 {
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

func (nc *NobitexCrawler) setupRateLimiter(symbolCount int) {
	optimalRate := nc.calculateOptimalRate(symbolCount)
	nc.httpConfig = crawler.DefaultHTTPConfig(LatestTradeAPI, optimalRate)
	nc.httpConfig.RateLimiter = rate.NewLimiter(rate.Limit(optimalRate), BurstSize)

	actualPollingInterval := float64(symbolCount) / optimalRate
	requestsPerMinute := optimalRate * 60

	separator := "=" + strings.Repeat("=", 70)
	nc.Logger.Info(separator)
	nc.Logger.Info("Dynamic Rate Limit Configuration")
	nc.Logger.Info("  - Total symbols", "count", symbolCount)
	nc.Logger.Info("  - Calculated rate",
		"requestsPerSecond", fmt.Sprintf("%.2f", optimalRate),
		"requestsPerMinute", fmt.Sprintf("%.1f", requestsPerMinute))
	nc.Logger.Info("  - API limit",
		"requestsPerMinute", APIRateLimitPerMinute,
		"safetyMargin", fmt.Sprintf("%.0f%%", SafetyMargin*100),
		"effectiveLimit", fmt.Sprintf("%.1f", APIRateLimitPerMinute*SafetyMargin))
	nc.Logger.Info("  - Polling interval",
		"seconds", fmt.Sprintf("%.1f", actualPollingInterval),
		"minutes", fmt.Sprintf("%.2f", actualPollingInterval/60))

	instancesFor1Sec := int(float64(symbolCount) / (APIRateLimitPerMinute * SafetyMargin / 60))

	if actualPollingInterval > 60 {
		nc.Logger.Warn("PERFORMANCE WARNING: Long polling interval",
			"intervalMinutes", fmt.Sprintf("%.1f", actualPollingInterval/60),
			"instancesNeeded", instancesFor1Sec)
	} else if actualPollingInterval > 10 {
		nc.Logger.Warn("Polling interval",
			"seconds", fmt.Sprintf("%.1f", actualPollingInterval))
	} else {
		nc.Logger.Info("Polling rate is acceptable", "symbols", symbolCount)
	}

	nc.Logger.Info(separator)
}

func (nc *NobitexCrawler) fetchTrades(ctx context.Context, symbol string) error {
	if err := nc.httpConfig.RateLimiter.Wait(ctx); err != nil {
		return err
	}

	resp, err := http.Get(fmt.Sprintf("%s%s", LatestTradeAPI, symbol))
	if err != nil {
		nc.Logger.Error("Error calling API", "symbol", symbol, "error", err)
		time.Sleep(2 * time.Second)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		nc.Logger.Error("API returned error status", "symbol", symbol, "status", resp.StatusCode)
		time.Sleep(2 * time.Second)
		return fmt.Errorf("API returned status %d", resp.StatusCode)
	}

	var tradeData TradeAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&tradeData); err != nil {
		nc.Logger.Error("Error decoding API response", "symbol", symbol, "error", err)
		return err
	}

	if tradeData.Status != "ok" {
		nc.Logger.Warn("API returned non-ok status", "symbol", symbol, "status", tradeData.Status)
		return nil
	}

	sentCount := 0

	for _, t := range tradeData.Trades {
		t.Symbol = symbol
		t.Exchange = "nobitex"

		volume, _ := strconv.ParseFloat(t.Volume, 64)
		price, _ := strconv.ParseFloat(t.Price, 64)

		tradeTime := utils.TurnTimeStampToTime(t.Time, false)

		data := crawler.KafkaData{
			Exchange: t.Exchange,
			Symbol:   t.Symbol,
			Volume:   volume,
			Time:     tradeTime.Format(time.RFC3339),
			Price:    price,
			Quantity: volume * price,
			Side:     t.Side,
		}

		jsonData, err := json.Marshal(data)
		if err != nil {
			nc.Logger.Error("Error marshaling trade", "symbol", symbol, "error", err)
			continue
		}

		if err := nc.SendToKafka(jsonData); err != nil {
			nc.Logger.Error("Failed to send to Kafka", "symbol", symbol, "error", err)
			continue
		}

		sentCount++
	}

	return nil
}

func (nc *NobitexCrawler) Run(ctx context.Context) error {
	nc.Logger.Info("Starting Nobitex Crawler...")

	if err := nc.InitKafkaProducer(); err != nil {
		return fmt.Errorf("failed to initialize Kafka producer: %w", err)
	}
	defer nc.CloseKafkaProducer()

	symbols, err := nc.FetchMarkets()
	if err != nil {
		return fmt.Errorf("could not fetch markets: %w", err)
	}

	if len(symbols) == 0 {
		return fmt.Errorf("no symbols found to track")
	}

	nc.setupRateLimiter(len(symbols))

	return crawler.RunWithGracefulShutdown(nc.Logger, func(ctx context.Context, wg *sync.WaitGroup) {
		for _, symbol := range symbols {
			wg.Add(1)
			go func(sym string) {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						nc.Logger.Info("Stopping trade fetcher for symbol", "symbol", sym)
						return
					default:
						if err := nc.fetchTrades(ctx, sym); err != nil {
						}
					}
				}
			}(symbol)
		}
	})
}
