package models

import "time"

// Candle represents a single candlestick record in the ClickHouse database.
type Candle struct {
	// ID is a unique identifier: exchange-symbol-interval-openTime
	ID string `json:"id"`

	// Source is the exchange name (e.g., "nobitex", "wallex").
	Source string `json:"source"`

	// Symbol is the normalized trading pair (e.g., "BTC/IRT", "ETH/USDT").
	Symbol string `json:"symbol"`

	// Interval is the candle timeframe: "1d", "4h", "1h", "15m", etc.
	Interval string `json:"interval"`

	// Open is the opening price of the candle.
	Open float64 `json:"open"`

	// High is the highest price during the candle period.
	High float64 `json:"high"`

	// Low is the lowest price during the candle period.
	Low float64 `json:"low"`

	// Close is the closing price of the candle.
	Close float64 `json:"close"`

	// Volume is the trading volume during the candle period.
	Volume float64 `json:"volume"`

	// USDTPrice is the USDT/IRT price at candle close.
	USDTPrice float64 `json:"usdt_price"`

	// OpenTime is when the candle opened.
	OpenTime time.Time `json:"open_time"`

	// InsertedAt is when the record was inserted into our database.
	InsertedAt time.Time `json:"inserted_at"`
}
