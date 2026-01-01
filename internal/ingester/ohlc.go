// Package ingester provides Kafka-to-ClickHouse data ingestion.
// This file handles OHLC (candlestick) data ingestion only.
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

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

// OHLCStorage defines the interface for persisting OHLC data.
type OHLCStorage interface {
	CreateOHLC(ctx context.Context, candles []*models.OHLC) error
}

// OHLCIngesterConfig holds OHLC ingester configuration.
type OHLCIngesterConfig struct {
	BatchSize    int
	BatchTimeout time.Duration
}

// OHLCIngester consumes OHLC data from Kafka and writes to ClickHouse.
type OHLCIngester struct {
	reader  *kafka.Reader
	storage OHLCStorage
	logger  *slog.Logger
	cfg     OHLCIngesterConfig
}

// NewOHLCIngester creates a new OHLC ingester.
func NewOHLCIngester(reader *kafka.Reader, storage OHLCStorage, logger *slog.Logger, cfg OHLCIngesterConfig) *OHLCIngester {
	return &OHLCIngester{
		reader:  reader,
		storage: storage,
		logger:  logger,
		cfg:     cfg,
	}
}

// Start runs the OHLC ingestion loop until context is cancelled.
func (oi *OHLCIngester) Start(ctx context.Context) error {
	oi.logger.Info("Starting OHLC Ingester", "batch_size", oi.cfg.BatchSize)

	batch := make([]*models.OHLC, 0, oi.cfg.BatchSize)
	msgs := make([]kafka.Message, 0, oi.cfg.BatchSize)

	ticker := time.NewTicker(oi.cfg.BatchTimeout)
	defer ticker.Stop()

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		for {
			if err := oi.storage.CreateOHLC(ctx, batch); err != nil {
				oi.logger.Error("DB insert failed, retrying", "error", err, "count", len(batch))
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(2 * time.Second):
					continue
				}
			}
			break
		}

		if err := oi.reader.CommitMessages(ctx, msgs...); err != nil {
			oi.logger.Warn("Failed to commit offsets", "error", err)
		}

		batch = batch[:0]
		msgs = msgs[:0]
		ticker.Reset(oi.cfg.BatchTimeout)
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
			fetchCtx, cancel := context.WithTimeout(ctx, oi.cfg.BatchTimeout)
			m, err := oi.reader.FetchMessage(fetchCtx)
			cancel()

			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					continue
				}
				if errors.Is(err, context.Canceled) {
					return nil
				}
				oi.logger.Error("Kafka fetch error", "error", err)
				select {
				case <-ctx.Done():
					return flush()
				case <-time.After(time.Second):
				}
				continue
			}

			candles, err := oi.parseMessage(m)
			if err != nil {
				oi.logger.Debug("Parse error", "error", err)
				continue
			}

			batch = append(batch, candles...)
			msgs = append(msgs, m)

			if len(batch) >= oi.cfg.BatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}

// parseMessage deserializes a Kafka message into OHLC models.
func (oi *OHLCIngester) parseMessage(msg kafka.Message) ([]*models.OHLC, error) {
	// Try single OHLC first
	var single pb.OHLCData
	if err := proto.Unmarshal(msg.Value, &single); err == nil && single.Id != "" && single.Exchange != "" && single.Interval != "" {
		return oi.convertList([]*pb.OHLCData{&single})
	}

	// Try batch
	var batch pb.OHLCDataBatch
	if err := proto.Unmarshal(msg.Value, &batch); err == nil && len(batch.Candles) > 0 {
		if batch.Candles[0].Id != "" && batch.Candles[0].Exchange != "" {
			return oi.convertList(batch.Candles)
		}
	}

	return nil, fmt.Errorf("unknown message format")
}

// convertList transforms protobuf OHLC to database models.
func (oi *OHLCIngester) convertList(list []*pb.OHLCData) ([]*models.OHLC, error) {
	result := make([]*models.OHLC, 0, len(list))
	for _, p := range list {
		o, err := oi.transform(p)
		if err != nil {
			oi.logger.Warn("OHLC validation failed", "error", err, "id", p.Id)
			continue
		}
		result = append(result, o)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("no valid OHLC")
	}
	return result, nil
}

// transform converts a protobuf OHLC to a database model.
func (oi *OHLCIngester) transform(p *pb.OHLCData) (*models.OHLC, error) {
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
		return nil, fmt.Errorf("invalid OHLC: high < low")
	}

	openTime, err := time.Parse(time.RFC3339, p.OpenTime)
	if err != nil {
		openTime = time.Now()
	}

	return &models.OHLC{
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


