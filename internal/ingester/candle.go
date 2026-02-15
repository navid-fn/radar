// Package ingester provides Kafka-to-ClickHouse data ingestion.
// This file handles candle data ingestion only.
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

// CandleStorage defines the interface for persisting candle data.
type CandleStorage interface {
	CreateCandle(ctx context.Context, candles []*models.Candle) error
}

// CandleIngesterConfig holds candle ingester configuration.
type CandleIngesterConfig struct {
	BatchSize    int
	BatchTimeout time.Duration
}

// CandleIngester consumes candle data from Kafka and writes to ClickHouse.
type CandleIngester struct {
	reader  *kafka.Reader
	storage CandleStorage
	logger  *slog.Logger
	cfg     CandleIngesterConfig
}

// NewCandleIngester creates a new candle ingester.
func NewCandleIngester(
	reader *kafka.Reader,
	storage CandleStorage,
	logger *slog.Logger,
	cfg CandleIngesterConfig,
) *CandleIngester {
	return &CandleIngester{
		reader:  reader,
		storage: storage,
		logger:  logger,
		cfg:     cfg,
	}
}

// Start runs the candle ingestion loop until context is cancelled.
func (ci *CandleIngester) Start(ctx context.Context) error {
	ci.logger.Info("starting candle ingester", "batch_size", ci.cfg.BatchSize)

	batch := make([]*models.Candle, 0, ci.cfg.BatchSize)
	msgs := make([]kafka.Message, 0, ci.cfg.BatchSize)

	ticker := time.NewTicker(ci.cfg.BatchTimeout)
	defer ticker.Stop()

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		for {
			if err := ci.storage.CreateCandle(ctx, batch); err != nil {
				ci.logger.Error("DB insert failed, retrying", "error", err, "count", len(batch))
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(2 * time.Second):
					continue
				}
			}
			break
		}

		if err := ci.reader.CommitMessages(ctx, msgs...); err != nil {
			ci.logger.Warn("failed to commit offsets", "error", err)
		}

		batch = batch[:0]
		msgs = msgs[:0]
		ticker.Reset(ci.cfg.BatchTimeout)
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
			fetchCtx, cancel := context.WithTimeout(ctx, ci.cfg.BatchTimeout)
			m, err := ci.reader.FetchMessage(fetchCtx)
			cancel()

			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					continue
				}
				if errors.Is(err, context.Canceled) {
					return nil
				}
				ci.logger.Error("Kafka fetch error", "error", err)
				select {
				case <-ctx.Done():
					return flush()
				case <-time.After(time.Second):
				}
				continue
			}

			candle, err := ci.parseMessage(m)
			if err != nil {
				ci.logger.Debug("parse error", "error", err)
				continue
			}

			batch = append(batch, candle)
			msgs = append(msgs, m)

			if len(batch) >= ci.cfg.BatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}

// parseMessage deserializes a Kafka message into candle models.
func (ci *CandleIngester) parseMessage(msg kafka.Message) (*models.Candle, error) {
	// Try single candle first.
	var candle pb.CandleData
	if err := proto.Unmarshal(
		msg.Value,
		&candle,
	); err == nil && candle.Id != "" && candle.Exchange != "" &&
		candle.Interval != "" {
		return ci.convert(&candle)
	}

	return nil, fmt.Errorf("unknown message format")
}

// convert transforms protobuf candle data to database models.
func (ci *CandleIngester) convert(candle *pb.CandleData) (*models.Candle, error) {
	c, err := ci.transform(candle)
	if err != nil {
		ci.logger.Warn("candle validation failed", "error", err, "id", candle.Id)
		return nil, err
	}
	return c, nil
}

// transform converts a protobuf candle to a database model.
func (ci *CandleIngester) transform(p *pb.CandleData) (*models.Candle, error) {
	if p.Id == "" || p.Exchange == "" || p.Symbol == "" || p.Interval == "" {
		return nil, fmt.Errorf("missing required fields")
	}

	// Check for corrupted data
	if math.IsNaN(p.Open) || math.IsInf(p.Open, 0) ||
		math.IsNaN(p.High) || math.IsInf(p.High, 0) ||
		math.IsNaN(p.Low) || math.IsInf(p.Low, 0) ||
		math.IsNaN(p.Close) || math.IsInf(p.Close, 0) {
		return nil, fmt.Errorf("corrupted numeric data")
	}

	// Allow zero prices (some markets may have no trades)
	if p.High < p.Low {
		return nil, fmt.Errorf("invalid candle: high < low")
	}

	openTime, err := time.Parse(time.RFC3339, p.OpenTime)
	if err != nil {
		openTime = time.Now()
	}

	return &models.Candle{
		ID:         p.Id,
		Source:     p.Exchange,
		Symbol:     p.Symbol,
		Interval:   p.Interval,
		Open:       p.Open,
		High:       p.High,
		Low:        p.Low,
		Close:      p.Close,
		Volume:     p.Volume,
		USDTPrice:  p.UsdtPrice,
		OpenTime:   openTime,
		InsertedAt: time.Now(),
	}, nil
}

// Backward-compatible aliases.
type (
	OHLCIngesterConfig = CandleIngesterConfig
	OHLCIngester       = CandleIngester
	OHLCStorage        = CandleStorage
)

func NewOHLCIngester(
	reader *kafka.Reader,
	storage OHLCStorage,
	logger *slog.Logger,
	cfg OHLCIngesterConfig,
) *OHLCIngester {
	return NewCandleIngester(reader, storage, logger, cfg)
}
