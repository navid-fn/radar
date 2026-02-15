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

// OrderbookStorage defines the interface for persisting orderbook data.
type OrderbookStorage interface {
	CreateOrderbook(ctx context.Context, orders []*models.Orderbook) error
}

// OrderbookIngesterConfig holds orderbook ingester configuration.
type OrderbookIngesterConfig struct {
	BatchSize    int
	BatchTimeout time.Duration
}

// OrderbookIngester consumes orderbook snapshots from Kafka and writes to ClickHouse.
type OrderbookIngester struct {
	reader  *kafka.Reader
	storage OrderbookStorage
	logger  *slog.Logger
	cfg     OrderbookIngesterConfig
}

// NewOrderbookIngester creates a new orderbook ingester.
func NewOrderbookIngester(
	reader *kafka.Reader,
	storage OrderbookStorage,
	logger *slog.Logger,
	cfg OrderbookIngesterConfig,
) *OrderbookIngester {
	return &OrderbookIngester{
		reader:  reader,
		storage: storage,
		logger:  logger,
		cfg:     cfg,
	}
}

// Start runs the orderbook ingestion loop until context is cancelled.
func (oi *OrderbookIngester) Start(ctx context.Context) error {
	oi.logger.Info("starting orderbook ingester", "batch_size", oi.cfg.BatchSize)

	batch := make([]*models.Orderbook, 0, oi.cfg.BatchSize)
	msgs := make([]kafka.Message, 0, oi.cfg.BatchSize)

	ticker := time.NewTicker(oi.cfg.BatchTimeout)
	defer ticker.Stop()

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		for {
			if err := oi.storage.CreateOrderbook(ctx, batch); err != nil {
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

		oi.logger.Debug("flushed orderbook rows", "count", len(batch))
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

			orders, err := oi.parseMessage(m)
			if err != nil {
				oi.logger.Debug("Parse error", "error", err)
				continue
			}

			batch = append(batch, orders...)
			msgs = append(msgs, m)

			if len(batch) >= oi.cfg.BatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}

// parseMessage deserializes a Kafka message into Trade models.
func (oi *OrderbookIngester) parseMessage(msg kafka.Message) ([]*models.Orderbook, error) {
	var orderBookSnapshot pb.OrderBookSnapshot
	if err := proto.Unmarshal(msg.Value, &orderBookSnapshot); err == nil && orderBookSnapshot.Id != "" && orderBookSnapshot.Exchange != "" && orderBookSnapshot.Symbol != "" {
		return oi.convert(&orderBookSnapshot)
	}

	return nil, fmt.Errorf("unknown message format")
}

// convert transforms protobuf orderbook snapshots to database model orderbook rows.
func (oi *OrderbookIngester) convert(snapshot *pb.OrderBookSnapshot) ([]*models.Orderbook, error) {
	asks := make([]*models.Orderbook, 0, len(snapshot.Asks))
	bids := make([]*models.Orderbook, 0, len(snapshot.Bids))

	for _, a := range snapshot.Asks {
		t, err := oi.transform(a, "ask")
		if err != nil {
			oi.logger.Warn("orderbook validation failed", "error", err, "id", snapshot.Id)
			continue
		}
		asks = append(asks, t)
	}

	for _, b := range snapshot.Bids {
		t, err := oi.transform(b, "bid")
		if err != nil {
			oi.logger.Warn("orderbook validation failed", "error", err, "id", snapshot.Id)
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
func (oi *OrderbookIngester) transform(p *pb.OrderLevel, side string) (*models.Orderbook, error) {
	if math.IsNaN(p.Price) || math.IsInf(p.Price, 0) ||
		math.IsNaN(p.Volume) || math.IsInf(p.Volume, 0) {
		return nil, fmt.Errorf("corrupted numeric data")
	}

	if p.Price <= 0 || p.Volume <= 0 {
		return nil, fmt.Errorf("invalid price or volume")
	}

	return &models.Orderbook{
		Side:   side,
		Price:  p.Price,
		Volume: p.Volume,
	}, nil
}

// Backward-compatible aliases.
type DepthStorage = OrderbookStorage
type DepthIngesterConfig = OrderbookIngesterConfig
type DepthIngester = OrderbookIngester

func NewDepthIngester(reader *kafka.Reader, storage DepthStorage, logger *slog.Logger, cfg DepthIngesterConfig) *DepthIngester {
	return NewOrderbookIngester(reader, storage, logger, cfg)
}
