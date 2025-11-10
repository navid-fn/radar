package model

import "time"

type Trade struct {
	TradeID     string    `gorm:"column:trade_id;primaryKey" json:"trade_id"`
	Source      string    `gorm:"column:source;primaryKey" json:"source"`
	Symbol      string    `gorm:"column:symbol" json:"symbol"`
	Side        string    `gorm:"column:side" json:"side"`
	Price       float64   `gorm:"column:price;type:Float64" json:"price"`
	BaseAmount  float64   `gorm:"column:base_amount;type:Float64" json:"base_amount"`
	QuoteAmount float64   `gorm:"column:quote_amount;type:Float64" json:"quote_amount"`
	EventTime   time.Time `gorm:"column:event_time;type:DateTime('Asia/Tehran')" json:"event_time"`
	InsertedAt  time.Time `gorm:"column:inserted_at;type:DateTime('Asia/Tehran');default:now()" json:"inserted_at"`
}

func (Trade) TableName() string {
	return "trade"
}

func (Trade) TableOptions() string {
	return "ENGINE = ReplacingMergeTree() ORDER BY (source, trade_id)"
}
