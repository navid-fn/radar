// Package ingester provides Kafka-to-ClickHouse data ingestion.
// This file handles trade data ingestion only.
package ingester

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	pb "nobitex/radar/internal/proto"
	"nobitex/radar/internal/storage/models"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

// TradeStorage defines the interface for persisting trade data.
type TradeStorage interface {
	CreateTrades(ctx context.Context, trades []*models.Trade) error
}

// TradeIngesterConfig holds trade ingester configuration.
type TradeIngesterConfig struct {
	BatchSize    int
	BatchTimeout time.Duration
}

// TradeIngester consumes trades from Kafka and writes to ClickHouse.
type TradeIngester struct {
	reader  *kafka.Reader
	storage TradeStorage
	logger  *slog.Logger
	cfg     TradeIngesterConfig
}

// NewTradeIngester creates a new trade ingester.
func NewTradeIngester(
	reader *kafka.Reader,
	storage TradeStorage,
	logger *slog.Logger,
	cfg TradeIngesterConfig,
) *TradeIngester {
	return &TradeIngester{
		reader:  reader,
		storage: storage,
		logger:  logger,
		cfg:     cfg,
	}
}

// Start runs the trade ingestion loop until context is cancelled.
func (ti *TradeIngester) Start(ctx context.Context) error {
	ti.logger.Info("Starting Trade Ingester", "batch_size", ti.cfg.BatchSize)

	batch := make([]*models.Trade, 0, ti.cfg.BatchSize)
	msgs := make([]kafka.Message, 0, ti.cfg.BatchSize)

	ticker := time.NewTicker(ti.cfg.BatchTimeout)
	defer ticker.Stop()

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		for {
			if err := ti.storage.CreateTrades(ctx, batch); err != nil {
				ti.logger.Error("DB insert failed, retrying", "error", err, "count", len(batch))
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(2 * time.Second):
					continue
				}
			}
			break
		}

		if err := ti.reader.CommitMessages(ctx, msgs...); err != nil {
			ti.logger.Warn("Failed to commit offsets", "error", err)
		}

		ti.logger.Debug("Flushed trades", "count", len(batch))
		batch = batch[:0]
		msgs = msgs[:0]
		ticker.Reset(ti.cfg.BatchTimeout)
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return flush()

		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}

		default:
			fetchCtx, cancel := context.WithTimeout(ctx, ti.cfg.BatchTimeout)
			m, err := ti.reader.FetchMessage(fetchCtx)
			cancel()

			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					continue
				}
				if errors.Is(err, context.Canceled) {
					return nil
				}
				ti.logger.Error("Kafka fetch error", "error", err)
				select {
				case <-ctx.Done():
					return flush()
				case <-time.After(time.Second):
				}
				continue
			}

			trade, err := ti.parseMessage(m)
			if err != nil {
				ti.logger.Debug("Parse error", "error", err)
				continue
			}

			batch = append(batch, trade)
			msgs = append(msgs, m)

			if len(batch) >= ti.cfg.BatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}

// parseMessage deserializes a Kafka message into Trade models.
func (ti *TradeIngester) parseMessage(msg kafka.Message) (*models.Trade, error) {
	var single pb.TradeData
	if err := proto.Unmarshal(
		msg.Value,
		&single,
	); err == nil && single.Id != "" && single.Exchange != "" &&
		single.Symbol != "" {
		return ti.convert(&single)
	}

	return nil, fmt.Errorf("unknown message format")
}

// convert transforms protobuf trades to database models.
func (ti *TradeIngester) convert(trade *pb.TradeData) (*models.Trade, error) {
	t, err := ti.transform(trade)
	if err != nil {
		ti.logger.Warn("Trade validation failed", "error", err, "id", trade.Id)
		return nil, err
	}
	return t, nil
}

// transform converts a protobuf trade to a database model.
func (ti *TradeIngester) transform(p *pb.TradeData) (*models.Trade, error) {
	if p.Id == "" || p.Exchange == "" || p.Symbol == "" {
		return nil, fmt.Errorf("missing required fields")
	}

	if math.IsNaN(p.Price) || math.IsInf(p.Price, 0) ||
		math.IsNaN(p.Volume) || math.IsInf(p.Volume, 0) {
		return nil, fmt.Errorf("corrupted numeric data")
	}

	if p.Price <= 0 || p.Volume <= 0 {
		return nil, fmt.Errorf("invalid price or volume")
	}

	eventTime, err := time.Parse(time.RFC3339, p.Time)
	if err != nil {
		return nil, fmt.Errorf("time has error")
	}

	return &models.Trade{
		TradeID:     p.Id,
		Source:      p.Exchange,
		Symbol:      p.Symbol,
		Side:        p.Side,
		Price:       p.Price,
		BaseAmount:  p.Volume,
		QuoteAmount: p.Quantity,
		EventTime:   eventTime,
		InsertedAt:  time.Now(),
		USDTPrice:   p.UsdtPrice,
	}, nil
}
