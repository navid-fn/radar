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

	"github.com/navid-fn/radar/internal/crawler"
	"github.com/navid-fn/radar/utils"
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
	tradeTracker *crawler.TradeTracker
	httpConfig   *crawler.HTTPConfig
}

func NewNobitexCrawler() *NobitexCrawler {
	config := crawler.NewConfig("nobitex", 0)
	baseCrawler := crawler.NewBaseCrawler(config)

	return &NobitexCrawler{
		BaseCrawler:  baseCrawler,
		tradeTracker: crawler.NewTradeTracker(),
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

	nc.Logger.Infof("Fetched %d symbols from Nobitex", len(symbols))
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
	nc.Logger.Info("Dynamic Rate Limit Configuration:")
	nc.Logger.Infof("  - Total symbols: %d", symbolCount)
	nc.Logger.Infof("  - Calculated rate: %.2f requests/second (%.1f/minute)", optimalRate, requestsPerMinute)
	nc.Logger.Infof("  - API limit: %d requests/minute (using %.0f%% = %.1f/min)",
		APIRateLimitPerMinute, SafetyMargin*100, (APIRateLimitPerMinute * SafetyMargin))
	nc.Logger.Infof("  - Each symbol polled every: ~%.1f seconds (%.2f minutes)", actualPollingInterval, actualPollingInterval/60)

	instancesFor1Sec := int(float64(symbolCount) / (APIRateLimitPerMinute * SafetyMargin / 60))

	if actualPollingInterval > 60 {
		nc.Logger.Warn("")
		nc.Logger.Warnf("⚠️  PERFORMANCE WARNING: Polling interval is %.1f minutes per symbol!", actualPollingInterval/60)
		nc.Logger.Warnf("📋 Need ~%d instances for 1-second polling", instancesFor1Sec)
	} else if actualPollingInterval > 10 {
		nc.Logger.Warnf("⚠️  Polling interval: %.1f seconds per symbol", actualPollingInterval)
	} else {
		nc.Logger.Infof("✅ Polling rate is acceptable for %d symbols", symbolCount)
	}

	nc.Logger.Info(separator)
}

func (nc *NobitexCrawler) fetchTrades(ctx context.Context, symbol string) error {
	if err := nc.httpConfig.RateLimiter.Wait(ctx); err != nil {
		return err
	}

	resp, err := http.Get(fmt.Sprintf("%s%s", LatestTradeAPI, symbol))
	if err != nil {
		nc.Logger.Errorf("Error calling API for %s: %v", symbol, err)
		time.Sleep(2 * time.Second)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		nc.Logger.Errorf("API returned status %d for %s", resp.StatusCode, symbol)
		time.Sleep(2 * time.Second)
		return fmt.Errorf("API returned status %d", resp.StatusCode)
	}

	var tradeData TradeAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&tradeData); err != nil {
		nc.Logger.Errorf("Error decoding API response for %s: %v", symbol, err)
		return err
	}

	if tradeData.Status != "ok" {
		nc.Logger.Warnf("API returned non-ok status for %s: %s", symbol, tradeData.Status)
		return nil
	}

	totalTrades := len(tradeData.Trades)
	duplicatesCount := 0
	sentCount := 0

	for _, t := range tradeData.Trades {
		t.Symbol = symbol
		t.Exchange = "nobitex"

		tradeHash := crawler.CreateTradeHash(t.Time, t.Price, t.Volume, t.Side)

		if nc.tradeTracker.IsTradeProcessed(symbol, tradeHash) {
			duplicatesCount++
			continue
		}

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
			nc.Logger.Errorf("Error marshaling trade for %s: %v", symbol, err)
			continue
		}

		if err := nc.SendToKafka(jsonData); err != nil {
			nc.Logger.Errorf("Failed to send to Kafka for %s: %v", symbol, err)
			continue
		}

		nc.tradeTracker.MarkTradeProcessed(symbol, tradeHash)
		sentCount++
	}

	if totalTrades > 0 {
		if duplicatesCount > 0 {
			nc.Logger.Infof("[%s] Filtered %d duplicates out of %d trades, sent %d unique trades",
				symbol, duplicatesCount, totalTrades, sentCount)
		} else {
			nc.Logger.Debugf("[%s] Sent %d new trades", symbol, sentCount)
		}
	}

	return nil
}

func (nc *NobitexCrawler) Run(ctx context.Context) error {
	nc.Logger.Info("Starting Nobitex Crawler...")

	if err := nc.InitKafkaProducer(); err != nil {
		return fmt.Errorf("failed to initialize Kafka producer: %w", err)
	}
	defer nc.CloseKafkaProducer()

	nc.StartDeliveryReport()

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
						nc.Logger.Infof("Stopping trade fetcher for symbol: %s", sym)
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
