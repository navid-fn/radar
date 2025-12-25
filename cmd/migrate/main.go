package main

import (
	"nobitex/radar/configs"

	"github.com/pressly/goose/v3"
	"gorm.io/driver/clickhouse"
	"gorm.io/gorm"
)

func main() {
	cfg := configs.Load()
	db, err := gorm.Open(clickhouse.Open(cfg.ClickHouseDSN), &gorm.Config{})

	if err != nil {
		cfg.Logger.Error("Failed to connect to database", "error", err)
		return
	}
	sqlDB, err := db.DB()
	if err != nil {
		cfg.Logger.Error("Failed to get sql.DB", "error", err)
		return
	}
	if err := goose.SetDialect("clickhouse"); err != nil {
		cfg.Logger.Error("Goose: failed to set dialect", "error", err)
		return
	}
	cfg.Logger.Info("Running database migrations...")
	if err := goose.Up(sqlDB, "internal/migrations"); err != nil {
		cfg.Logger.Error("Goose migration failed", "error", err)
		return
	}
	cfg.Logger.Info("Migrations completed successfully")
}
