package repository

import (
	"github.com/navid-fn/radar/internal/model"
	"gorm.io/gorm"
)

type TradeRepository interface {
	CreateTrade(*model.Trade) error
	CreateTrades([]*model.Trade) error
}

type gormTradeRepository struct {
	db *gorm.DB
}

func NewGormTradeRepository(db *gorm.DB) TradeRepository {
	return &gormTradeRepository{db: db}
}

func (r *gormTradeRepository) CreateTrade(trade *model.Trade) error {
	return r.db.Create(trade).Error
}

func (r *gormTradeRepository) CreateTrades(trades []*model.Trade) error {
	if len(trades) == 0 {
		return nil
	}
	return r.db.Create(trades).Error
}

