package models

import "time"

// represents a single candlestick record in the ClickHouse database.
type Depth struct {
	// Metadata
	// Helps group all rows from a single HTTP request together
	SnapshotID string `json:"snapshot_id"`

	// Source is the exchange name (e.g., "nobitex", "wallex").
	Source string `json:"source"`

	// Symbol is the normalized trading pair (e.g., "BTC/IRT", "ETH/USDT").
	Symbol string `json:"symbol"`

	// Symbol is the normalized trading pair (e.g., "BTC/IRT", "ETH/USDT").
	Side string `json:"side"`

	// Price of order for that market in orderbook.
	Price float64 `json:"price"`

	// Volume is the order volume in orderbook.
	Volume float64 `json:"volume"`

	// UpdatedAt is time that api return when they updated their orderbook response.
	LastUpdate time.Time `json:"last_update"`

	// InsertedAt is when the record was inserted into our database.
	InsertedAt time.Time `json:"inserted_at"`
}
