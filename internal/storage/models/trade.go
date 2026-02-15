// Package models defines the domain models used across the application.
package models

import "time"

// Trade represents a single trade record in the ClickHouse database.
// This is the canonical format used for storage, normalized from
// exchange-specific formats received via Kafka.
type Trade struct {
	// TradeID is a unique identifier for this trade.
	// Either provided by the exchange or generated via SHA1 hash.
	TradeID string `json:"trade_id"`

	// Source is the exchange name (e.g., "nobitex", "wallex").
	Source string `json:"source"`

	// Symbol is the normalized trading pair (e.g., "BTC/IRT", "ETH/USDT").
	Symbol string `json:"symbol"`

	// Side is the trade direction: "buy" or "sell" or "all".
	Side string `json:"side"`

	// Price is the trade price in quote currency (IRT or USDT).
	// For IRT pairs, this is in Toman (not Rial).
	Price float64 `json:"price"`

	// BaseAmount is the quantity of base currency traded.
	BaseAmount float64 `json:"base_amount"`

	// QuoteAmount is the total value in quote currency (Price * BaseAmount).
	QuoteAmount float64 `json:"quote_amount"`

	// USDTPrice is the USDT/IRT price at the time of trade.
	// Used for converting IRT values to USD for analytics.
	USDTPrice float64 `json:"usdt_price"`

	// EventTime is when the trade occurred on the exchange.
	EventTime time.Time `json:"event_time"`

	// InsertedAt is when the trade was inserted into our database.
	InsertedAt time.Time `json:"inserted_at"`
}
