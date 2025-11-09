package service

import (
	"github.com/navid-fn/radar/server/internal/model"
	"github.com/navid-fn/radar/server/internal/repository"
)

type TradesService struct {
	repo repository.TradeRepository
}

func NewTradesService(repo repository.TradeRepository) *TradesService {
	return &TradesService{
		repo: repo,
	}
}

func (ts *TradesService) GetLastTenTrades() []model.Trade {
	trades := ts.repo.GetLatestTrades(10)
	return trades
}

func (ts *TradesService) GetCountTrades(source string) int64 {
	return ts.repo.GetTradesCount(source)
}

func (ts *TradesService) GetCountTradesPerSource() map[string]int{
	return ts.repo.GetTradeCountGroupBySource()
}

func (ts *TradesService) GetLastTradesPerSource() map[string][]model.Trade {
	validSources := []string{"binance", "nobitex", "wallex", "ramzinex", "bitpin", "tabdeal"}
	return ts.repo.GetLatestTradesGroupBySources(validSources, 10)
}
