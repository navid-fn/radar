// Package ingester provides Kafka-to-ClickHouse data ingestion.
// This file handles commission data ingestion only.
package ingester

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"nobitex/radar/internal/models"
	pb "nobitex/radar/internal/proto"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

// CommissionStorage defines the interface for persisting Commission data.
type CommissionStorage interface {
	CreateCommissions(ctx context.Context, commissions []*models.Commission) error
}

type CommissionIngesterConfig struct {
	BatchSize    int
	BatchTimeout time.Duration
}

// CommissionIngester consumes Commission data from Kafka and writes to ClickHouse.
type CommissionIngester struct {
	reader  *kafka.Reader
	storage CommissionStorage
	logger  *slog.Logger
	cfg     CommissionIngesterConfig
}

// NewCommissionIngester creates a new Commission ingester.
func NewCommissionIngester(reader *kafka.Reader, storage CommissionStorage, logger *slog.Logger) *CommissionIngester {
	return &CommissionIngester{
		reader:  reader,
		storage: storage,
		logger:  logger,
		// set batch size and timeout manually. 300 message is going to be fine because its going to be same everywhere
		cfg: CommissionIngesterConfig{
			BatchSize:    300,
			BatchTimeout: 3 * time.Second,
		},
	}
}

// Start runs the Commission ingestion loop until context is cancelled.
func (ci *CommissionIngester) Start(ctx context.Context) error {
	ci.logger.Info("starting commission ingester")

	batch := make([]*models.Commission, 0, ci.cfg.BatchSize)
	msgs := make([]kafka.Message, 0, ci.cfg.BatchSize)

	ticker := time.NewTicker(ci.cfg.BatchTimeout)
	defer ticker.Stop()

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		for {
			if err := ci.storage.CreateCommissions(ctx, batch); err != nil {
				ci.logger.Error("db insert failed, retrying", "error", err, "count", len(batch))
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

			commissions, err := ci.parseMessage(m)
			if err != nil {
				ci.logger.Debug("Parse error", "error", err)
				continue
			}

			batch = append(batch, commissions...)
			msgs = append(msgs, m)

			if len(batch) >= ci.cfg.BatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}

// parseMessage deserializes a Kafka message into Commission models.
func (ci *CommissionIngester) parseMessage(msg kafka.Message) ([]*models.Commission, error) {
	// Try single commission first
	var single pb.CommissionData
	if err := proto.Unmarshal(msg.Value, &single); err == nil {
		return ci.convertList([]*pb.CommissionData{&single})
	}

	// Try batch
	var batch pb.CommissionDataBatch
	if err := proto.Unmarshal(msg.Value, &batch); err == nil && len(batch.Commissions) > 0 {
		return ci.convertList(batch.Commissions)
	}

	return nil, fmt.Errorf("unknown message format")
}

// convertList transforms protobuf Commission to database models.
func (ci *CommissionIngester) convertList(list []*pb.CommissionData) ([]*models.Commission, error) {
	result := make([]*models.Commission, 0, len(list))
	for _, p := range list {
		o, err := ci.transform(p)
		if err != nil {
			ci.logger.Warn("commission validation failed", "error", err)
			continue
		}
		result = append(result, o)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("no valid commission")
	}
	return result, nil
}

// transform converts a protobuf commission to a database model.
func (ci *CommissionIngester) transform(p *pb.CommissionData) (*models.Commission, error) {
	return &models.Commission{
		Source: p.Exchange,
		Symbol: p.Symbol,
		Taker:  p.Taker,
		Maker:  p.Maker,
	}, nil
}
