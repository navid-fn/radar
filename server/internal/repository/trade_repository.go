package repository

import (
	"log"

	"github.com/navid-fn/radar/server/internal/model"
	"gorm.io/gorm"
)

type TradeRepository interface {
	GetLatestTrades(limit int) []model.Trade
	GetTradesCount(source string) int64
	GetLatestTradesGroupBySources(sources []string, limit int) map[string][]model.Trade
	GetTradeCountGroupBySource() map[string]int
}

type gormTradeRepository struct {
	db *gorm.DB
}

func NewGormTradeRepository(db *gorm.DB) TradeRepository {
	return &gormTradeRepository{db: db}
}

func (gtr *gormTradeRepository) GetLatestTrades(limit int) []model.Trade {
	var trades []model.Trade
	err := gtr.db.Order("event_time desc").Limit(limit).Find(&trades).Error
	if err != nil {
		log.Fatalf("error occoured: %v", err)
		return []model.Trade{}
	}
	return trades
}

func (gtr *gormTradeRepository) GetTradesCount(source string) int64 {
	var count int64
	query := gtr.db.Model(&model.Trade{})
	if source != "" {
		query.Where("source = ?", source)
	}
	if err := query.Count(&count).Error; err != nil {
		log.Fatalf("error occoured: %v", err)

		return 0
	}
	return count
}

func (gtr *gormTradeRepository) GetLatestTradesGroupBySources(sources []string, limit int) map[string][]model.Trade {
	subQuery := gtr.db.Model(&model.Trade{}).
		Select("*, ROW_NUMBER() OVER (PARTITION BY source ORDER BY event_time DESC) as rn").
		Where("source IN (?)", sources)

	var flatTrades []model.Trade
	err := gtr.db.Table("(?) as ranked_trades", subQuery).
		Where("rn <= ?", limit).
		Order("source, event_time DESC").
		Find(&flatTrades).Error

	if err != nil {
		return make(map[string][]model.Trade)
	}

	results := make(map[string][]model.Trade)
	for _, trade := range flatTrades {
		results[trade.Source] = append(results[trade.Source], trade)
	}

	return results
}

func (gtr *gormTradeRepository) GetTradeCountGroupBySource() map[string]int {
	type SourceCount struct {
		Source string
		Count  int
	}
	var sourceCountResult []SourceCount
	err := gtr.db.Model(&model.Trade{}).Select("source, count(*) as count").Group("source").Scan(&sourceCountResult).Error
	if err != nil {
		log.Printf("Error for query: %v", err)
		return make(map[string]int)
	}
	result := make(map[string]int, len(sourceCountResult))
	for _, r := range sourceCountResult {
		result[r.Source] = r.Count
	}
	return result
}
