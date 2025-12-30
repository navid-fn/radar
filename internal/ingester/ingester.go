// Package ingester consumes trade data from Kafka and persists it to ClickHouse.
// It handles batching, retry logic, and graceful shutdown.
package ingester

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"nobitex/radar/internal/models"
	pb "nobitex/radar/internal/proto"
	"nobitex/radar/internal/storage"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

// Config holds ingester configuration parameters.
type Config struct {
	// BatchSize is the maximum number of trades to accumulate before flushing to DB.
	BatchSize int

	// BatchTimeout is the maximum time to wait before flushing, even if batch isn't full.
	BatchTimeout time.Duration
}

// Ingester consumes trades from Kafka and writes them to ClickHouse in batches.
// It implements at-least-once delivery: trades are only committed to Kafka
// after successful database insertion.
type Ingester struct {
	reader  *kafka.Reader
	storage storage.TradeStorage
	logger  *slog.Logger
	cfg     Config
}

// NewIngester creates a new Ingester with the provided dependencies.
// Uses dependency injection for testability - it receives tools, doesn't create them.
func NewIngester(reader *kafka.Reader, storage storage.TradeStorage, logger *slog.Logger, cfg Config) *Ingester {
	return &Ingester{
		reader:  reader,
		storage: storage,
		logger:  logger,
		cfg:     cfg,
	}
}

// Start runs the main ingestion loop. It blocks until context is cancelled.
// On shutdown, it attempts to flush any remaining buffered trades.
//
// The loop:
//  1. Fetches messages from Kafka
//  2. Parses protobuf into Trade models
//  3. Accumulates trades until batch is full or timeout
//  4. Inserts batch to ClickHouse (with retry on failure)
//  5. Commits Kafka offsets only after successful DB insert
func (ig *Ingester) Start(ctx context.Context) error {
	ig.logger.Info("Starting Ingester Loop", "batch_size", ig.cfg.BatchSize)

	// Initialize reusable buffers to reduce GC pressure
	batchTrades := make([]*models.Trade, 0, ig.cfg.BatchSize)
	batchMsgs := make([]kafka.Message, 0, ig.cfg.BatchSize)

	ticker := time.NewTicker(ig.cfg.BatchTimeout)
	defer ticker.Stop()

	// flush writes accumulated trades to DB and commits Kafka offsets
	flush := func() error {
		if len(batchTrades) == 0 {
			return nil
		}

		// Retry loop: never drop data, keep retrying until DB accepts it
		for {
			if err := ig.storage.CreateTrades(ctx, batchTrades); err != nil {
				ig.logger.Error("DB Insert Failed (Retrying in 2s)", "error", err)

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(2 * time.Second):
					continue
				}
			}
			break
		}

		// Commit Kafka offsets AFTER successful DB insert (at-least-once)
		if err := ig.reader.CommitMessages(ctx, batchMsgs...); err != nil {
			ig.logger.Warn("Failed to commit offsets", "error", err)
		}

		// Clear buffers while keeping allocated capacity
		batchTrades = batchTrades[:0]
		batchMsgs = batchMsgs[:0]
		ticker.Reset(ig.cfg.BatchTimeout)
		return nil
	}

	// Main event loop
	for {
		select {
		case <-ctx.Done():
			return flush() // Flush remaining trades on shutdown

		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}

		default:
			// Fetch with short timeout to remain responsive to ticker/shutdown
			fetchCtx, cancel := context.WithTimeout(ctx, ig.cfg.BatchTimeout)
			m, err := ig.reader.FetchMessage(fetchCtx)
			cancel()

			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					continue
				}
				if errors.Is(err, context.Canceled) {
					return nil
				}
				ig.logger.Error("Kafka Fetch Error", "error", err)
				time.Sleep(time.Second)
				continue
			}

			trades, err := ig.parseMessage(m)
			if err != nil {
				continue
			}

			batchTrades = append(batchTrades, trades...)
			batchMsgs = append(batchMsgs, m)

			if len(batchTrades) >= ig.cfg.BatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}

// parseMessage deserializes a Kafka message into Trade models.
// Supports both single TradeData and TradeDataBatch formats.
//
// Important: Must try single format first because protobuf wire types
// can cause batch parsing to incorrectly "succeed" with garbage data.
func (ig *Ingester) parseMessage(msg kafka.Message) ([]*models.Trade, error) {
	// Try single message first (most common case from API scrapers)
	var single pb.TradeData
	if err := proto.Unmarshal(msg.Value, &single); err == nil && single.Id != "" && single.Exchange != "" {
		return ig.convertProtoList([]*pb.TradeData{&single})
	}

	// Try batch message (from WebSocket scrapers)
	var batch pb.TradeDataBatch
	if err := proto.Unmarshal(msg.Value, &batch); err == nil && len(batch.Trades) > 0 {
		if batch.Trades[0].Id != "" && batch.Trades[0].Exchange != "" {
			return ig.convertProtoList(batch.Trades)
		}
	}

	return nil, fmt.Errorf("unknown protobuf format")
}

// convertProtoList transforms protobuf trades to database models.
// Skips invalid trades but continues processing valid ones.
func (ig *Ingester) convertProtoList(list []*pb.TradeData) ([]*models.Trade, error) {
	result := make([]*models.Trade, 0, len(list))
	for _, item := range list {
		t, err := ig.transform(item)
		if err != nil {
			ig.logger.Warn("Trade validation failed")
			continue
		}
		result = append(result, t)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("no valid trades found")
	}
	return result, nil
}

// transform converts a single protobuf trade to a database model.
// Validates required fields and checks for corrupted numeric data.
func (ig *Ingester) transform(p *pb.TradeData) (*models.Trade, error) {
	if p.Id == "" || p.Exchange == "" || p.Symbol == "" {
		return nil, fmt.Errorf("missing required fields: id=%q exchange=%q symbol=%q", p.Id, p.Exchange, p.Symbol)
	}

	// NaN/Inf indicates corrupted protobuf data
	if math.IsNaN(p.Price) || math.IsInf(p.Price, 0) ||
		math.IsNaN(p.Volume) || math.IsInf(p.Volume, 0) {
		return nil, fmt.Errorf("corrupted numeric data detected")
	}

	if p.Price <= 0 {
		return nil, fmt.Errorf("invalid price: %v", p.Price)
	}

	if p.Volume <= 0 {
		return nil, fmt.Errorf("invalid volume: %v", p.Volume)
	}

	eventTime, err := time.Parse(time.RFC3339, p.Time)
	if err != nil {
		eventTime = time.Now()
	}

	return &models.Trade{
		TradeID:     p.Id,
		Source:      p.Exchange,
		Symbol:      p.Symbol,
		Side:        p.Side,
		Price:       p.Price,
		BaseAmount:  p.Volume,
		QuoteAmount: p.Price * p.Volume,
		EventTime:   eventTime,
		InsertedAt:  time.Now(),
		USDTPrice:   p.UsdtPrice,
	}, nil
}
