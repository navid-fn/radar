package scraper

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"

	pb "nobitex/radar/internal/proto"
)

// Scraper is the interface all scrapers must implement
type Scraper interface {
	Run(ctx context.Context) error
	Name() string
}

// Sender handles sending trades to Kafka
type Sender struct {
	writer *kafka.Writer
	logger *slog.Logger
}

// NewSender creates a new Kafka sender
func NewSender(writer *kafka.Writer, logger *slog.Logger) *Sender {
	return &Sender{writer: writer, logger: logger}
}

// Send sends raw bytes to Kafka
func (s *Sender) Send(ctx context.Context, data []byte) error {
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err := s.writer.WriteMessages(writeCtx, kafka.Message{Value: data})
	if err != nil && ctx.Err() == nil {
		return fmt.Errorf("kafka write failed: %w", err)
	}
	return nil
}

// SendTrade serializes and sends a single trade
func (s *Sender) SendTrade(ctx context.Context, trade *pb.TradeData) error {
	data, err := proto.Marshal(trade)
	if err != nil {
		return fmt.Errorf("serialize failed: %w", err)
	}
	return s.Send(ctx, data)
}

// SendTrades serializes and sends multiple trades as a batch
func (s *Sender) SendTrades(ctx context.Context, trades []*pb.TradeData) error {
	data, err := proto.Marshal(&pb.TradeDataBatch{Trades: trades})
	if err != nil {
		return fmt.Errorf("serialize batch failed: %w", err)
	}
	return s.Send(ctx, data)
}

// GenerateTradeID creates a unique ID for a trade based on its properties
func GenerateTradeID(exchange, symbol, tradeTime string, price, volume float64, side string) string {
	unique := fmt.Sprintf("%s-%s-%s-%f-%f-%s", exchange, symbol, tradeTime, price, volume, side)
	hash := sha1.Sum([]byte(unique))
	return hex.EncodeToString(hash[:])
}

// ChunkSlice splits a slice into chunks of specified size
// its good to split market for connecting to websocket
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

// TimestampToRFC3339 converts millisecond timestamp to RFC3339 string
func TimestampToRFC3339(ms int64) string {
	return time.UnixMilli(ms).UTC().Format(time.RFC3339)
}
