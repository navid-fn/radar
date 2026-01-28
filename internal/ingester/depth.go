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

	"nobitex/radar/internal/models"
	pb "nobitex/radar/internal/proto"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

// TradeStorage defines the interface for persisting trade data.
type DepthStorage interface {
	CreateDepths(ctx context.Context, depths []*models.Depth) error
}

// DepthIngesterConfig holds trade ingester configuration.
type DepthIngesterConfig struct {
	BatchSize    int
	BatchTimeout time.Duration
}

// DepthIngester consumes trades from Kafka and writes to ClickHouse.
type DepthIngester struct {
	reader  *kafka.Reader
	storage DepthStorage
	logger  *slog.Logger
	cfg     DepthIngesterConfig
}

// NewDepthIngester creates a new trade ingester.
func NewDepthIngester(reader *kafka.Reader, storage DepthStorage, logger *slog.Logger, cfg DepthIngesterConfig) *DepthIngester {
	return &DepthIngester{
		reader:  reader,
		storage: storage,
		logger:  logger,
		cfg:     cfg,
	}
}

// Start runs the trade ingestion loop until context is cancelled.
func (ti *DepthIngester) Start(ctx context.Context) error {
	ti.logger.Info("starting depth ingester", "batch_size", ti.cfg.BatchSize)

	batch := make([]*models.Depth, 0, ti.cfg.BatchSize)
	msgs := make([]kafka.Message, 0, ti.cfg.BatchSize)

	ticker := time.NewTicker(ti.cfg.BatchTimeout)
	defer ticker.Stop()

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		for {
			if err := ti.storage.CreateDepths(ctx, batch); err != nil {
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

			depths, err := ti.parseMessage(m)
			if err != nil {
				ti.logger.Debug("Parse error", "error", err)
				continue
			}

			batch = append(batch, depths...)
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
func (di *DepthIngester) parseMessage(msg kafka.Message) ([]*models.Depth, error) {
	// Try single trade first
	var orderBookSnapshot pb.OrderBookSnapshot
	if err := proto.Unmarshal(msg.Value, &orderBookSnapshot); err == nil && orderBookSnapshot.Id != "" && orderBookSnapshot.Exchange != "" && orderBookSnapshot.Symbol != "" {
		return di.convert(&orderBookSnapshot)
	}

	return nil, fmt.Errorf("unknown message format")
}

// convert transforms protobuf orderbook to database model depth.
func (di *DepthIngester) convert(snapshot *pb.OrderBookSnapshot) ([]*models.Depth, error) {
	asks := make([]*models.Depth, 0, len(snapshot.Asks))
	bids := make([]*models.Depth, 0, len(snapshot.Bids))

	for _, a := range snapshot.Asks {
		t, err := di.transform(a, "ask")
		if err != nil {
			di.logger.Warn("depth validation failed", "error", err, "id", snapshot.Id)
			continue
		}
		asks = append(asks, t)
	}

	for _, b := range snapshot.Bids {
		t, err := di.transform(b, "bid")
		if err != nil {
			di.logger.Warn("depth validation failed", "error", err, "id", snapshot.Id)
			continue
		}

		bids = append(bids, t)
	}

	result := append(asks, bids...)
	for _, t := range result {
		// add extra data that was in snapshot
		t.SnapshotID = snapshot.Id
		t.Symbol = snapshot.Symbol
		t.Source = snapshot.Exchange

		lastUpdateTime, err := time.Parse(time.RFC3339, snapshot.LastUpdate)
		if err != nil {
			continue
		}
		t.LastUpdate = lastUpdateTime

	}
	return result, nil
}

// transform converts a protobuf trade to a database model.
func (ti *DepthIngester) transform(p *pb.OrderLevel, side string) (*models.Depth, error) {
	if math.IsNaN(p.Price) || math.IsInf(p.Price, 0) ||
		math.IsNaN(p.Volume) || math.IsInf(p.Volume, 0) {
		return nil, fmt.Errorf("corrupted numeric data")
	}

	if p.Price <= 0 || p.Volume <= 0 {
		return nil, fmt.Errorf("invalid price or volume")
	}

	return &models.Depth{
		Side:   side,
		Price:  p.Price,
		Volume: p.Volume,
	}, nil
}
