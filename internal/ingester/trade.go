// Package ingester provides Kafka-to-ClickHouse data ingestion.
// This file handles trade data ingestion only.
package ingester

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	"nobitex/radar/internal/models"
	pb "nobitex/radar/internal/proto"

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
func NewTradeIngester(reader *kafka.Reader, storage TradeStorage, logger *slog.Logger, cfg TradeIngesterConfig) *TradeIngester {
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

			trades, err := ti.parseMessage(m)
			if err != nil {
				ti.logger.Debug("Parse error", "error", err)
				continue
			}

			batch = append(batch, trades...)
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
func (ti *TradeIngester) parseMessage(msg kafka.Message) ([]*models.Trade, error) {
	// Try single trade first
	var single pb.TradeData
	if err := proto.Unmarshal(msg.Value, &single); err == nil && single.Id != "" && single.Exchange != "" && single.Symbol != "" {
		// Skip OHLC messages that ended up in trade topic (legacy data)
		if isOHLCMessage(&single) {
			return nil, fmt.Errorf("skipping OHLC message in trade topic")
		}
		return ti.convertList([]*pb.TradeData{&single})
	}

	// Try batch
	var batch pb.TradeDataBatch
	if err := proto.Unmarshal(msg.Value, &batch); err == nil && len(batch.Trades) > 0 {
		if batch.Trades[0].Id != "" && batch.Trades[0].Exchange != "" {
			return ti.convertList(batch.Trades)
		}
	}

	return nil, fmt.Errorf("unknown message format")
}

// isOHLCMessage detects if a message is actually OHLC data parsed as Trade.
// OHLC IDs contain interval markers like "-1d-", "-4h-", "-1h-".
// Also, trades always have a side (buy/sell), OHLC never does.
func isOHLCMessage(t *pb.TradeData) bool {
	// Check for interval pattern in ID
	if strings.Contains(t.Id, "-1d-") || strings.Contains(t.Id, "-4h-") ||
		strings.Contains(t.Id, "-1h-") || strings.Contains(t.Id, "-15m-") {
		return true
	}
	return false
}

// convertList transforms protobuf trades to database models.
func (ti *TradeIngester) convertList(list []*pb.TradeData) ([]*models.Trade, error) {
	result := make([]*models.Trade, 0, len(list))
	for _, p := range list {
		t, err := ti.transform(p)
		if err != nil {
			ti.logger.Warn("Trade validation failed", "error", err, "id", p.Id)
			continue
		}
		result = append(result, t)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("no valid trades")
	}
	return result, nil
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
