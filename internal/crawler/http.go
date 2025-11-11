package crawler

import (
	"context"
	"crypto/md5"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type HTTPConfig struct {
	BaseURL        string
	RateLimiter    *rate.Limiter
	PollingDelay   time.Duration
	RequestTimeout time.Duration
}

func DefaultHTTPConfig(baseURL string, requestsPerSecond float64) *HTTPConfig {
	return &HTTPConfig{
		BaseURL:        baseURL,
		RateLimiter:    rate.NewLimiter(rate.Limit(requestsPerSecond), 10),
		PollingDelay:   1 * time.Second,
		RequestTimeout: 10 * time.Second,
	}
}

type BaseHTTPWorker struct {
	Config      *HTTPConfig
	Logger      *slog.Logger
	SendToKafka func([]byte) error
}

func NewBaseHTTPWorker(config *HTTPConfig, logger *slog.Logger, sendToKafka func([]byte) error) *BaseHTTPWorker {
	return &BaseHTTPWorker{
		Config:      config,
		Logger:      logger,
		SendToKafka: sendToKafka,
	}
}

func (hw *BaseHTTPWorker) RunWorker(
	ctx context.Context,
	symbol string,
	wg *sync.WaitGroup,
	fetchFunc func(ctx context.Context, symbol string) error,
) {
	defer wg.Done()

	hw.Logger.Info("Starting HTTP worker for symbol", "symbol", symbol)

	for {
		select {
		case <-ctx.Done():
			hw.Logger.Info("Stopping HTTP worker for symbol", "symbol", symbol)
			return
		default:
			if err := hw.Config.RateLimiter.Wait(ctx); err != nil {
				hw.Logger.Error("Rate limiter error", "symbol", symbol, "error", err)
				return
			}
			if err := fetchFunc(ctx, symbol); err != nil {
				hw.Logger.Error("Error fetching data", "symbol", symbol, "error", err)
				time.Sleep(2 * time.Second)
				continue
			}
		}
	}
}

type TradeTracker struct {
	seenTradeHashes map[string]map[string]bool
	mu              sync.RWMutex
}

func NewTradeTracker() *TradeTracker {
	return &TradeTracker{
		seenTradeHashes: make(map[string]map[string]bool),
	}
}

func (tt *TradeTracker) IsTradeProcessed(symbol string, tradeHash string) bool {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	if symbolMap, exists := tt.seenTradeHashes[symbol]; exists {
		return symbolMap[tradeHash]
	}
	return false
}

func (tt *TradeTracker) MarkTradeProcessed(symbol string, tradeHash string) {
	tt.mu.Lock()
	defer tt.mu.Unlock()

	if tt.seenTradeHashes[symbol] == nil {
		tt.seenTradeHashes[symbol] = make(map[string]bool)
	}
	tt.seenTradeHashes[symbol][tradeHash] = true

	// Keep only last 1000 hashes per symbol to prevent memory growth
	if len(tt.seenTradeHashes[symbol]) > 100 {
		count := 0
		for hash := range tt.seenTradeHashes[symbol] {
			if count < 25 {
				delete(tt.seenTradeHashes[symbol], hash)
				count++
			} else {
				break
			}
		}
	}
}

// CreateTradeHash generates a unique hash for a trade
func CreateTradeHash(attributes ...any) string {
	hashInput := fmt.Sprint(attributes...)
	hash := md5.Sum([]byte(hashInput))
	return fmt.Sprintf("%x", hash)
}

// IDTracker tracks the last seen trade ID for each symbol
type IDTracker struct {
	lastSeenTradeID map[string]int
	sleepDuration   map[string]time.Duration
	mu              sync.RWMutex
}

// NewIDTracker creates a new IDTracker
func NewIDTracker(minSleep, maxSleep, sleepIncrement time.Duration) *IDTracker {
	return &IDTracker{
		lastSeenTradeID: make(map[string]int),
		sleepDuration:   make(map[string]time.Duration),
	}
}

// GetLastSeenID returns the last seen trade ID for a symbol
func (it *IDTracker) GetLastSeenID(symbol string) int {
	it.mu.RLock()
	defer it.mu.RUnlock()
	return it.lastSeenTradeID[symbol]
}

// UpdateLastSeenID updates the last seen trade ID for a symbol
func (it *IDTracker) UpdateLastSeenID(symbol string, tradeID int) {
	it.mu.Lock()
	defer it.mu.Unlock()
	if tradeID > it.lastSeenTradeID[symbol] {
		it.lastSeenTradeID[symbol] = tradeID
	}
}

// GetSleepDuration returns the sleep duration for a symbol
func (it *IDTracker) GetSleepDuration(symbol string, defaultDuration time.Duration) time.Duration {
	it.mu.RLock()
	defer it.mu.RUnlock()
	if duration, exists := it.sleepDuration[symbol]; exists {
		return duration
	}
	return defaultDuration
}

// IncreaseSleep increases the sleep duration for a symbol
func (it *IDTracker) IncreaseSleep(symbol string, increment, maxSleep, defaultSleep time.Duration) {
	it.mu.Lock()
	defer it.mu.Unlock()

	currentSleep := it.sleepDuration[symbol]
	if currentSleep == 0 {
		currentSleep = defaultSleep
	}

	newSleep := currentSleep + increment
	if newSleep > maxSleep {
		newSleep = maxSleep
	}

	it.sleepDuration[symbol] = newSleep
}

// DecreaseSleep decreases the sleep duration for a symbol
func (it *IDTracker) DecreaseSleep(symbol string, decrement, minSleep, defaultSleep time.Duration) {
	it.mu.Lock()
	defer it.mu.Unlock()

	currentSleep := it.sleepDuration[symbol]
	if currentSleep == 0 {
		it.sleepDuration[symbol] = defaultSleep
		return
	}

	newSleep := currentSleep - decrement
	if newSleep < minSleep {
		newSleep = minSleep
	}

	it.sleepDuration[symbol] = newSleep
}



