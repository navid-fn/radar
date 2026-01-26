// Package storage provides database storage implementations for trade data.
package storage

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"nobitex/radar/internal/models"
)

// Storage defines the interface for persisting trade and OHLC data.
// Implementations must be safe for concurrent use.
type Storage interface {
	// CreateTrades inserts a batch of trades into the database.
	CreateTrades(ctx context.Context, trades []*models.Trade) error

	// CreateOHLC inserts a batch of OHLC candles into the database.
	CreateOHLC(ctx context.Context, candles []*models.OHLC) error

	// CreateDepths inserts a batch of depths into the database
	CreateDepths(ctx context.Context, depths []*models.Depth) error

	// Close releases database connection resources.
	Close() error
}

// TradeStorage is an alias for backward compatibility.
type TradeStorage = Storage

// clickhouseStorage implements TradeStorage using native ClickHouse driver.
// Uses batch inserts for high-throughput data ingestion.
type clickhouseStorage struct {
	conn driver.Conn
}

// NewClickHouseStorage creates a new ClickHouse storage connection.
// It parses the DSN, opens a connection, and verifies connectivity with a ping.
// Returns an error if connection cannot be established within 5 seconds.
func NewClickHouseStorage(dsn string) (TradeStorage, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, err
	}

	// Test connection with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		return nil, err
	}

	return &clickhouseStorage{conn: conn}, nil
}

// CreateTrades inserts trades using ClickHouse batch insert.
// Batch insert is significantly faster than individual inserts for ClickHouse.
// All trades in the batch share the same inserted_at timestamp.
func (s *clickhouseStorage) CreateTrades(ctx context.Context, trades []*models.Trade) error {
	if len(trades) == 0 {
		return nil
	}

	batch, err := s.conn.PrepareBatch(ctx, `
		INSERT INTO trade (
			trade_id, source, symbol, side, 
			price, base_amount, quote_amount, usdt_price,
			event_time, inserted_at
		)
	`)
	if err != nil {
		return err
	}

	now := time.Now()
	for _, t := range trades {
		err := batch.Append(
			t.TradeID,
			t.Source,
			t.Symbol,
			t.Side,
			t.Price,
			t.BaseAmount,
			t.QuoteAmount,
			t.USDTPrice,
			t.EventTime,
			now,
		)
		if err != nil {
			return err
		}
	}

	return batch.Send()
}

// CreateOHLC inserts OHLC candles using ClickHouse batch insert.
func (s *clickhouseStorage) CreateOHLC(ctx context.Context, candles []*models.OHLC) error {
	if len(candles) == 0 {
		return nil
	}

	batch, err := s.conn.PrepareBatch(ctx, `
		INSERT INTO ohlc (
			id, source, symbol, interval,
			open, high, low, close, volume, usdt_price,
			open_time, inserted_at
		)
	`)
	if err != nil {
		return err
	}

	now := time.Now()
	for _, c := range candles {
		err := batch.Append(
			c.ID,
			c.Source,
			c.Symbol,
			c.Interval,
			c.Open,
			c.High,
			c.Low,
			c.Close,
			c.Volume,
			c.USDTPrice,
			c.OpenTime,
			now,
		)
		if err != nil {
			return err
		}
	}

	return batch.Send()
}

// Close closes the ClickHouse connection.
func (s *clickhouseStorage) Close() error {
	return s.conn.Close()
}

// CreateOHLC inserts OHLC candles using ClickHouse batch insert.
func (s *clickhouseStorage) CreateDepths(ctx context.Context, depths []*models.Depth) error {
	if len(depths) == 0 {
		return nil
	}

	batch, err := s.conn.PrepareBatch(ctx, `
		INSERT INTO depth (
			snapshot_id, source, symbol,
			price, volume, side,
			last_update, inserted_at
		)
	`)
	if err != nil {
		return err
	}

	now := time.Now()
	for _, d := range depths {
		err := batch.Append(
			d.SnapshotID,
			d.Source,
			d.Symbol,
			d.Price,
			d.Volume,
			d.Side,
			d.LastUpdate,
			now,
		)
		if err != nil {
			return err
		}
	}

	return batch.Send()
}
