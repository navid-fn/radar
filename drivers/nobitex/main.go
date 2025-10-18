package main

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

const (
	LatestTradeAPI     = "https://apiv2.nobitex.ir/v2/trades/"
	MarketAPI          = "https://apiv2.nobitex.ir/market/stats"
	TradesLimit        = 2
	DefaultKafkaBroker = "localhost:9092"
	KafkaTopic         = "radar_trades"

	// Rate limit: Nobitex API allows 60 requests per minute
	APIRateLimitPerMinute = 60
	SafetyMargin          = 0.98 // Use 98% of the limit (58.8 req/min) - increased for high symbol count
	
	// Desired polling interval per symbol (in seconds)
	// This is the target - actual rate will be calculated dynamically
	// With 327 symbols, polling every 1 sec would require 327 req/sec (impossible)
	// Realistic target: 5-10 seconds per symbol with current API limits
	DesiredPollingInterval = 1.0
	
	BurstSize = 10 // Increased burst for better handling of symbol spikes
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

func transformPair(pair string) string {
	// Split by '-', uppercase each part
	parts := strings.Split(pair, "-")
	for i, part := range parts {
		parts[i] = strings.ToUpper(part)
	}
	if len(parts) > 1 && parts[len(parts)-1] == "RLS" {
		parts[len(parts)-1] = "IRT"
	}
	// Join back with '-'
	return strings.Join(parts, "")
}

func GetNobitexMarkets() ([]string, error) {
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

	return symbols, nil
}

type TradeTracker struct {
	seenTradeHashes map[string]map[string]bool // symbol -> hash -> seen
	mu              sync.RWMutex
}

func newTradeTracker() *TradeTracker {
	return &TradeTracker{
		seenTradeHashes: make(map[string]map[string]bool),
	}
}

func (tt *TradeTracker) isTradeProcessed(symbol string, tradeHash string) bool {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	if symbolMap, exists := tt.seenTradeHashes[symbol]; exists {
		return symbolMap[tradeHash]
	}
	return false
}

func (tt *TradeTracker) markTradeProcessed(symbol string, tradeHash string) {
	tt.mu.Lock()
	defer tt.mu.Unlock()

	if tt.seenTradeHashes[symbol] == nil {
		tt.seenTradeHashes[symbol] = make(map[string]bool)
	}
	tt.seenTradeHashes[symbol][tradeHash] = true

	// Keep only last 1000 hashes per symbol to prevent memory growth
	if len(tt.seenTradeHashes[symbol]) > 1000 {
		count := 0
		for hash := range tt.seenTradeHashes[symbol] {
			if count < 500 {
				delete(tt.seenTradeHashes[symbol], hash)
				count++
			} else {
				break
			}
		}
	}
}

type NobitexProducer struct {
	kafkaProducer *kafka.Producer
	kafkaBroker   string
	logger        *logrus.Logger
	rateLimiter   *rate.Limiter
	tradeTracker  *TradeTracker
}

func NewNobitexProducer() *NobitexProducer {
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = DefaultKafkaBroker
	}

	return &NobitexProducer{
		kafkaBroker:  kafkaBroker,
		logger:       logger,
		tradeTracker: newTradeTracker(),
		// rateLimiter will be set dynamically based on symbol count
	}
}

// calculateOptimalRate determines the best request rate based on symbol count
func (np *NobitexProducer) calculateOptimalRate(symbolCount int) float64 {
	// Maximum safe rate per second (90% of API limit)
	maxRatePerSecond := (APIRateLimitPerMinute * SafetyMargin) / 60.0

	// Desired rate based on polling interval (symbols / interval)
	desiredRate := float64(symbolCount) / DesiredPollingInterval

	// Use the minimum of desired rate and max safe rate
	optimalRate := desiredRate
	if optimalRate > maxRatePerSecond {
		optimalRate = maxRatePerSecond
	}

	// Ensure we have at least some minimum rate
	minRate := 0.1
	if optimalRate < minRate {
		optimalRate = minRate
	}

	return optimalRate
}

// setupRateLimiter creates and configures the rate limiter based on symbol count
func (np *NobitexProducer) setupRateLimiter(symbolCount int) {
	optimalRate := np.calculateOptimalRate(symbolCount)
	np.rateLimiter = rate.NewLimiter(rate.Limit(optimalRate), BurstSize)

	actualPollingInterval := float64(symbolCount) / optimalRate
	requestsPerMinute := optimalRate * 60

	np.logger.Infof("=" + strings.Repeat("=", 70))
	np.logger.Infof("Dynamic Rate Limit Configuration:")
	np.logger.Infof("  - Total symbols: %d", symbolCount)
	np.logger.Infof("  - Calculated rate: %.2f requests/second (%.1f/minute)", optimalRate, requestsPerMinute)
	np.logger.Infof("  - API limit: %d requests/minute (using %.0f%% = %.1f/min)",
		APIRateLimitPerMinute, SafetyMargin*100, (APIRateLimitPerMinute * SafetyMargin))
	np.logger.Infof("  - Each symbol polled every: ~%.1f seconds (%.2f minutes)", actualPollingInterval, actualPollingInterval/60)

	// Calculate how many instances needed for 1-second polling
	instancesFor1Sec := int(float64(symbolCount) / (APIRateLimitPerMinute * SafetyMargin / 60))
	
	if actualPollingInterval > 60 {
		np.logger.Warnf("")
		np.logger.Warnf("⚠️  PERFORMANCE WARNING: Polling interval is %.1f minutes per symbol!", actualPollingInterval/60)
		np.logger.Warnf("")
		np.logger.Warnf("📋 RECOMMENDED SOLUTIONS:")
		np.logger.Warnf("")
		np.logger.Warnf("1. 🚀 Run Multiple Instances (BEST for %d symbols):", symbolCount)
		np.logger.Warnf("   - Need ~%d instances for 1-second polling per symbol", instancesFor1Sec)
		np.logger.Warnf("   - Each instance: ~%d symbols", symbolCount/instancesFor1Sec)
		np.logger.Warnf("   - Split symbols via ENV var or modify GetNobitexMarkets()")
		np.logger.Warnf("")
		np.logger.Warnf("2. 🎯 Filter to Important Symbols:")
		np.logger.Warnf("   - Track only high-volume pairs (BTC, ETH, USDT, etc.)")
		np.logger.Warnf("   - Reduce to ~50-60 symbols for sub-minute polling")
		np.logger.Warnf("")
		np.logger.Warnf("3. ✅ Accept Current Rate:")
		np.logger.Warnf("   - Keep all %d symbols with %.1f min polling", symbolCount, actualPollingInterval/60)
		np.logger.Warnf("   - Suitable for low-frequency analysis")
		np.logger.Warnf("")
	} else if actualPollingInterval > 10 {
		np.logger.Warnf("")
		np.logger.Warnf("⚠️  Polling interval: %.1f seconds per symbol", actualPollingInterval)
		np.logger.Warnf("💡 For faster updates, consider running %d instances (~%d symbols each)", 
			instancesFor1Sec, symbolCount/instancesFor1Sec)
		np.logger.Warnf("")
	} else {
		np.logger.Infof("✅ Polling rate is acceptable for %d symbols", symbolCount)
	}
	
	np.logger.Infof("=" + strings.Repeat("=", 70))
}

// createTradeHash generates a unique hash for a trade based on its attributes
func (np *NobitexProducer) createTradeHash(t TradeData) string {
	hashInput := fmt.Sprintf("%d:%s:%s:%s", t.Time, t.Price, t.Volume, t.Side)
	hash := md5.Sum([]byte(hashInput))
	return fmt.Sprintf("%x", hash)
}

func (np *NobitexProducer) initKafkaProducer() error {
	config := kafka.ConfigMap{
		"bootstrap.servers": np.kafkaBroker,
	}

	producer, err := kafka.NewProducer(&config)
	if err != nil {
		return fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	np.kafkaProducer = producer
	np.logger.Info("Kafka Producer initialized successfully")
	return nil
}

func (np *NobitexProducer) fetchTrades(ctx context.Context, symbol string) {
	for {
		select {
		case <-ctx.Done():
			np.logger.Infof("Stopping trade fetcher for symbol: %s", symbol)
			return
		default:
			// Wait for rate limiter before making request
			if err := np.rateLimiter.Wait(ctx); err != nil {
				np.logger.Errorf("Rate limiter error for %s: %v", symbol, err)
				return
			}

			resp, err := http.Get(fmt.Sprintf("%s%s", LatestTradeAPI, symbol))
			if err != nil {
				np.logger.Errorf("Error calling API for %s: %v", symbol, err)
				time.Sleep(2 * time.Second)
				continue
			}

			if resp.StatusCode != http.StatusOK {
				np.logger.Errorf("API returned status %d for %s", resp.StatusCode, symbol)
				resp.Body.Close()
				time.Sleep(2 * time.Second)
				continue
			}

			var tradeData TradeAPIResponse
			if err := json.NewDecoder(resp.Body).Decode(&tradeData); err != nil {
				np.logger.Errorf("Error decoding API response for %s: %v", symbol, err)
				resp.Body.Close()
				continue
			}
			resp.Body.Close()

			if tradeData.Status != "ok" {
				np.logger.Warnf("API returned non-ok status for %s: %s", symbol, tradeData.Status)
				continue
			}

			totalTrades := len(tradeData.Trades)
			duplicatesCount := 0
			sentCount := 0

			for _, t := range tradeData.Trades {
				t.Symbol = symbol
				t.Exchange = "nobitex"

				// Create hash and check for duplicates
				tradeHash := np.createTradeHash(t)

				if np.tradeTracker.isTradeProcessed(symbol, tradeHash) {
					duplicatesCount++
					continue
				}

				jsonData, err := t.MarshalJSON()
				if err != nil {
					np.logger.Errorf("Error marshaling trade for %s: %v", symbol, err)
					continue
				}

				topic := KafkaTopic
				err = np.kafkaProducer.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
					Value:          jsonData,
				}, nil)

				if err != nil {
					np.logger.Errorf("Failed to send to Kafka for %s: %v", symbol, err)
					continue
				}

				// Mark as processed after successful send
				np.tradeTracker.markTradeProcessed(symbol, tradeHash)
				sentCount++
			}

			if totalTrades > 0 {
				if duplicatesCount > 0 {
					np.logger.Infof("[%s] Filtered %d duplicates out of %d trades, sent %d unique trades",
						symbol, duplicatesCount, totalTrades, sentCount)
				} else {
					np.logger.Debugf("[%s] Sent %d new trades", symbol, sentCount)
				}
			}
		}
	}
}

func (np *NobitexProducer) deliveryReport() {
	go func() {
		for e := range np.kafkaProducer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					np.logger.Errorf("Message delivery failed: %v", ev.TopicPartition.Error)
				}
			}
		}
	}()
}

func (np *NobitexProducer) Run() error {
	np.logger.Info("Starting Nobitex Producer...")

	if err := np.initKafkaProducer(); err != nil {
		return fmt.Errorf("failed to initialize Kafka producer: %w", err)
	}
	defer np.kafkaProducer.Close()

	np.deliveryReport()

	// Fetch available markets
	symbols, err := GetNobitexMarkets()
	if err != nil {
		return fmt.Errorf("could not fetch markets: %w", err)
	}

	if len(symbols) == 0 {
		return fmt.Errorf("no symbols found to track")
	}

	np.logger.Infof("Fetched %d symbols from Nobitex", len(symbols))

	// Setup dynamic rate limiter based on symbol count
	np.setupRateLimiter(len(symbols))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		np.logger.Info("Received shutdown signal, gracefully shutting down...")
		cancel()
	}()

	var wg sync.WaitGroup
	for _, symbol := range symbols {
		wg.Add(1)
		go func(sym string) {
			defer wg.Done()
			np.fetchTrades(ctx, sym)
		}(symbol)
	}

	np.logger.Info("All workers started, waiting for completion...")
	wg.Wait()
	np.logger.Info("All workers completed")

	return nil
}

func main() {
	producer := NewNobitexProducer()

	if err := producer.Run(); err != nil {
		producer.logger.Fatalf("Application failed: %v", err)
	}
}
