// Package scraper provides core types and utilities for building exchange scrapers.
// It includes interfaces, Kafka message sending, and common helper functions.
package scraper

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"

	pb "nobitex/radar/internal/proto"
)

// HTTPClient is a shared HTTP client with timeout for all scrapers.
// Using a shared client enables connection pooling and prevents resource leaks.
// Timeout is set to 10 seconds to prevent hanging on unresponsive APIs.
var HTTPClient = &http.Client{
	Timeout: 10 * time.Second,
}

// Scraper is the interface that all exchange scrapers must implement.
// Each scraper runs in its own goroutine and publishes trades to Kafka.
type Scraper interface {
	// Run starts the scraper and blocks until context is cancelled.
	// It should handle reconnection and error recovery internally.
	Run(ctx context.Context) error

	// Name returns a unique identifier for this scraper (e.g., "nobitex-ws").
	Name() string
}

// MessageWriter is the interface for writing messages to a backend (Kafka, debug, etc.).
// *kafka.Writer satisfies this interface.
type MessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// Sender handles serializing and sending trade data to a message backend.
// It wraps a MessageWriter with protobuf serialization.
type Sender struct {
	writer MessageWriter
	logger *slog.Logger
}

// NewSender creates a new sender with the given writer and logger.
func NewSender(writer MessageWriter, logger *slog.Logger) *Sender {
	return &Sender{writer: writer, logger: logger}
}

// Send sends raw bytes to Kafka.
// Returns nil if context was cancelled (graceful shutdown).
func (s *Sender) Send(ctx context.Context, data []byte) error {
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err := s.writer.WriteMessages(writeCtx, kafka.Message{Value: data})
	if err != nil && ctx.Err() == nil {
		return fmt.Errorf("kafka write failed: %w", err)
	}
	return nil
}

// SendTrade serializes a single trade to protobuf and sends it to Kafka.
// Use this for API scrapers that fetch one trade at a time.
func (s *Sender) SendTrade(ctx context.Context, trade *pb.TradeData) error {
	data, err := proto.Marshal(trade)
	if err != nil {
		return fmt.Errorf("serialize failed: %w", err)
	}
	return s.Send(ctx, data)
}

// SendTrades serializes multiple trades as a TradeDataBatch and sends to Kafka.
// Use this for WebSocket scrapers that receive multiple trades per message.
func (s *Sender) SendTrades(ctx context.Context, trades []*pb.TradeData) error {
	data, err := proto.Marshal(&pb.TradeDataBatch{Trades: trades})
	if err != nil {
		return fmt.Errorf("serialize batch failed: %w", err)
	}
	return s.Send(ctx, data)
}

// GenerateTradeID creates a deterministic unique ID for a trade.
// Used when the exchange doesn't provide a trade ID.
// The ID is a SHA1 hash of: exchange-symbol-time-price-volume-side
func GenerateTradeID(exchange, symbol, tradeTime string, price, volume float64, side string) string {
	unique := fmt.Sprintf("%s-%s-%s-%f-%f-%s", exchange, symbol, tradeTime, price, volume, side)
	hash := sha1.Sum([]byte(unique))
	return hex.EncodeToString(hash[:])
}

// ChunkSlice splits a slice into chunks of the specified size.
// Useful for distributing symbols across multiple WebSocket connections
// when exchanges limit subscriptions per connection.
func ChunkSlice[T any](items []T, size int) [][]T {
	if size < 1 {
		panic("ChunkSlice: size must be greater than 0")
	}

	length := len(items)
	if length == 0 {
		return nil
	}
	capacity := (length + size - 1) / size
	chunks := make([][]T, 0, capacity)

	for i := 0; i < length; i += size {
		end := min(i+size, length)
		chunks = append(chunks, items[i:end])
	}

	return chunks
}

// TimestampToRFC3339 converts a Unix millisecond timestamp to RFC3339 string.
// Used for normalizing trade timestamps from different exchange formats.
func TimestampToRFC3339(ms int64) string {
	return time.UnixMilli(ms).UTC().Format(time.RFC3339)
}

// FloatTimestampToRFC3339 converts a Unix timestamp (seconds with optional decimals) to RFC3339.
// Handles float timestamps like 1703123456.789 where decimals are sub-second precision.
// Used for APIs that return timestamps as float64 (e.g., bitpin).
func FloatTimestampToRFC3339(ts float64) string {
	sec := int64(ts)
	nsec := int64((ts - float64(sec)) * 1e9)
	return time.Unix(sec, nsec).UTC().Format(time.RFC3339)
}

// AnyToRFC3339 converts various time formats to RFC3339.
// Handles: string (returns as-is), float64/int64 (treats as milliseconds).
// Used for WebSocket messages where time format varies.
func AnyToRFC3339(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case float64:
		return time.UnixMilli(int64(val)).UTC().Format(time.RFC3339)
	case int64:
		return time.UnixMilli(val).UTC().Format(time.RFC3339)
	}
	return time.Now().UTC().Format(time.RFC3339)
}

// NewRateLimiter creates a rate limiter for API scrapers.
// Default is 60 requests/minute (standard for most exchanges).
// Uses 95% of limit as safety margin to avoid hitting rate limit errors.
// Burst allows catching up after idle periods.
func NewRateLimiter(requestsPerMinute int) *rate.Limiter {
	const safetyMargin = 0.95
	rps := float64(requestsPerMinute) * safetyMargin / 60.0
	burst := max(1, requestsPerMinute/10) // 10% of limit as burst
	return rate.NewLimiter(rate.Limit(rps), burst)
}

// DefaultRateLimiter creates a rate limiter for 60 requests/minute APIs.
// This is the standard limit for most Iranian exchanges.
func DefaultRateLimiter() *rate.Limiter {
	return NewRateLimiter(60)
}

// SendOHLC serializes a single OHLC candle to protobuf and sends it to Kafka.
func (s *Sender) SendOHLC(ctx context.Context, ohlc *pb.OHLCData) error {
	data, err := proto.Marshal(ohlc)
	if err != nil {
		return fmt.Errorf("serialize OHLC failed: %w", err)
	}
	return s.Send(ctx, data)
}

func (s *Sender) SendOHLCBatch(ctx context.Context, candels []*pb.OHLCData) error {
	data, err := proto.Marshal(&pb.OHLCDataBatch{Candles: candels})
	if err != nil {
		return fmt.Errorf("serialize batch failed: %w", err)
	}
	return s.Send(ctx, data)
}

// GenerateOHLCID creates a unique ID for an OHLC candle.
// Format: exchange-symbol-interval-openTime
func GenerateOHLCID(exchange, symbol, interval, openTime string) string {
	return fmt.Sprintf("%s-%s-%s-%s", exchange, symbol, interval, openTime)
}

// ToMidnight returns the start of day (00:00:00) for the given time in UTC.
func ToMidnight(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// UnixToRFC3339 converts a Unix timestamp (seconds) to RFC3339 string.
func UnixToRFC3339(ts int64) string {
	return time.Unix(ts, 0).UTC().Format(time.RFC3339)
}

// DoWithRetry executes an HTTP request with retry on timeout/connection errors.
// Uses exponential backoff: baseDelay * 2^attempt (e.g., 2s, 4s, 8s).
// Returns the response and error from the last attempt.
func DoWithRetry(ctx context.Context, req *http.Request, maxRetries int, baseDelay time.Duration) (*http.Response, error) {
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff: baseDelay * 2^(attempt-1)
			delay := baseDelay * time.Duration(1<<(attempt-1))
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}

		resp, err := HTTPClient.Do(req.Clone(ctx))
		if err == nil {
			return resp, nil
		}
		lastErr = err

		// Don't retry if context was cancelled
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}

	return nil, fmt.Errorf("after %d retries: %w", maxRetries, lastErr)
}

func GenerateSnapShotID(source, symbol, lastUpdate string) string {
	return fmt.Sprintf("%s-%s-%s", source, symbol, lastUpdate)
}

func (s *Sender) SendOrderBookSnapShot(ctx context.Context, orderBookSnapshot *pb.OrderBookSnapshot) error {
	data, err := proto.Marshal(orderBookSnapshot)
	if err != nil {
		return fmt.Errorf("serialize failed: %w", err)
	}
	return s.Send(ctx, data)
}
