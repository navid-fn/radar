package scraper

import (
	"context"
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
