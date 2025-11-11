package service

import (
	"nobitex/radar/internal/model"
	"nobitex/radar/internal/repository"
)

type TradesService struct {
	repo repository.TradeRepository
}

func NewTradesService(repo repository.TradeRepository) *TradesService {
	return &TradesService{
		repo: repo,
	}
}

func (ts *TradesService) CreateTrade(trade *model.Trade) error {
	return ts.repo.CreateTrade(trade)
}
