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

type Config struct {
	BatchSize    int
	BatchTimeout time.Duration
}

type Ingester struct {
	reader  *kafka.Reader
	storage storage.TradeStorage
	logger  *slog.Logger
	cfg     Config
}

// It receives the tools it needs, it doesn't create them.
func NewIngester(reader *kafka.Reader, storage storage.TradeStorage, logger *slog.Logger, cfg Config) *Ingester {
	return &Ingester{
		reader:  reader,
		storage: storage,
		logger:  logger,
		cfg:     cfg,
	}
}

// Start is a blocking function that runs the main loop
func (ig *Ingester) Start(ctx context.Context) error {
	ig.logger.Info("Starting Ingester Loop", "batch_size", ig.cfg.BatchSize)

	// 1. Initialize Buffers
	// We reuse these arrays to reduce Garbage Collection (GC) pressure
	batchTrades := make([]*models.Trade, 0, ig.cfg.BatchSize)
	batchMsgs := make([]kafka.Message, 0, ig.cfg.BatchSize)

	ticker := time.NewTicker(ig.cfg.BatchTimeout)
	defer ticker.Stop()

	// 2. Define Flush Logic
	flush := func() error {
		if len(batchTrades) == 0 {
			return nil
		}

		// Retry Loop: We NEVER drop data. We retry until DB accepts it.
		for {
			if err := ig.storage.CreateTrades(batchTrades); err != nil {
				ig.logger.Error("DB Insert Failed (Retrying in 2s)", "error", err)

				select {
				case <-ctx.Done():
					return ctx.Err() // Shutdown requested during retry
				case <-time.After(2 * time.Second):
					continue // Retry
				}
			}
			break // Success
		}

		// Commit Kafka offsets AFTER DB insert
		if err := ig.reader.CommitMessages(ctx, batchMsgs...); err != nil {
			ig.logger.Warn("Failed to commit offsets", "error", err)
		}

		// Clear buffers (keep capacity)
		batchTrades = batchTrades[:0]
		batchMsgs = batchMsgs[:0]
		ticker.Reset(ig.cfg.BatchTimeout)
		return nil
	}

	// 3. Main Event Loop
	for {
		select {
		case <-ctx.Done():
			return flush() // Try to flush one last time on shutdown

		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}

		default:
			// Fetch with short timeout so we can check ticker/ctx often
			fetchCtx, cancel := context.WithTimeout(ctx, ig.cfg.BatchTimeout)
			m, err := ig.reader.FetchMessage(fetchCtx)
			cancel()

			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					continue // No new messages, loop around
				}
				if errors.Is(err, context.Canceled) {
					return nil
				}
				ig.logger.Error("Kafka Fetch Error", "error", err)
				time.Sleep(time.Second) // Backoff
				continue
			}

			// Parse & Accumulate
			trades, err := ig.parseMessage(m)
			if err != nil {
				// Log more details about garbage data
				rawPreview := string(m.Value)
				if len(rawPreview) > 200 {
					rawPreview = rawPreview[:200]
				}
				continue
			}

			batchTrades = append(batchTrades, trades...)
			batchMsgs = append(batchMsgs, m)

			// Flush if buffer is full
			if len(batchTrades) >= ig.cfg.BatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}

// parseMessage unifies Single and Batch proto handling
func (ig *Ingester) parseMessage(msg kafka.Message) ([]*models.Trade, error) {
	// Try Single message FIRST (most common case)
	// Important: Must try single before batch because TradeData field 1 (string id)
	// and TradeDataBatch field 1 (repeated TradeData) have same wire type,
	// causing batch parsing to incorrectly "succeed" with garbage data
	var single pb.TradeData
	if err := proto.Unmarshal(msg.Value, &single); err == nil && single.Id != "" && single.Exchange != "" {
		return ig.convertProtoList([]*pb.TradeData{&single})
	}

	// Try Batch message (for scrapers that send batches)
	var batch pb.TradeDataBatch
	if err := proto.Unmarshal(msg.Value, &batch); err == nil && len(batch.Trades) > 0 {
		// Validate first trade has required fields (not garbage)
		if batch.Trades[0].Id != "" && batch.Trades[0].Exchange != "" {
			return ig.convertProtoList(batch.Trades)
		}
	}

	return nil, fmt.Errorf("unknown protobuf format")
}

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

func (ig *Ingester) transform(p *pb.TradeData) (*models.Trade, error) {
	// Validate required string fields
	if p.Id == "" || p.Exchange == "" || p.Symbol == "" {
		return nil, fmt.Errorf("missing required fields: id=%q exchange=%q symbol=%q", p.Id, p.Exchange, p.Symbol)
	}

	// Check for NaN/Inf (telltale signs of corrupted protobuf data)
	if math.IsNaN(p.Price) || math.IsInf(p.Price, 0) ||
		math.IsNaN(p.Volume) || math.IsInf(p.Volume, 0) {
		return nil, fmt.Errorf("corrupted numeric data detected")
	}

	// Simple validation: just check for positive values
	// IRT prices can be very small for cheap coins, and volumes can be very large
	if p.Price <= 0 {
		return nil, fmt.Errorf("invalid price: %v", p.Price)
	}

	if p.Volume <= 0 {
		return nil, fmt.Errorf("invalid volume: %v", p.Volume)
	}

	// Fast time parsing (fallback to Now if fails)
	eventTime, err := time.Parse(time.RFC3339, p.Time)
	if err != nil {
		fmt.Printf("parse error for %s %s", p.Exchange, p.Time)
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
