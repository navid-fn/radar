package models

import "time"

// Commision represents a single commission record in the ClickHouse database.
type Commission struct {
	// Source is the exchange name (e.g., "nobitex", "wallex").
	Source string `json:"source"`

	// Symbol is the normalized trading pair (e.g., "BTC/IRT", "ETH/USDT").
	Symbol string `json:"symbol"`

	// maker price fee based on market
	Maker float64 `json:"maker"`

	// Taker price fee based on market
	Taker float64 `json:"taker"`

	// InsertedAt is when the commission was inserted into our database.
	InsertedAt time.Time `json:"inserted_at"`
}
