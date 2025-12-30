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

// Sender handles serializing and sending trade data to Kafka.
// It wraps a kafka.Writer with protobuf serialization.
type Sender struct {
	writer *kafka.Writer
	logger *slog.Logger
}

// NewSender creates a new Kafka sender with the given writer and logger.
func NewSender(writer *kafka.Writer, logger *slog.Logger) *Sender {
	return &Sender{writer: writer, logger: logger}
}

// Send sends raw bytes to Kafka with a 5-second timeout.
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
