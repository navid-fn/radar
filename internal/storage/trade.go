package storage

import (
	"nobitex/radar/internal/models"
	"gorm.io/gorm"
)

type TradeStorage interface {
	CreateTrades([]*models.Trade) error
}

type gormTradeStorage struct {
	db *gorm.DB
}

func NewGormTradeStorage(db *gorm.DB) TradeStorage {
	return &gormTradeStorage{db: db}
}

func (s *gormTradeStorage) CreateTrades(trade []*models.Trade) error {
	return s.db.Create(trade).Error
}


