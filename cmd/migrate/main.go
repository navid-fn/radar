package main

import (
	"log/slog"
	"nobitex/radar/internal/consumer/config"

	"github.com/pressly/goose/v3"
	"gorm.io/driver/clickhouse"
	"gorm.io/gorm"
)

func main() {
	cfg := config.Load()

	db, err := gorm.Open(clickhouse.Open(cfg.ClickHouseDSN), &gorm.Config{})
	if err != nil {
		slog.Error("Failed to connect to database", "error", err)
		return
	}
	sqlDB, err := db.DB()
	if err != nil {
		slog.Error("Failed to get sql.DB", "error", err)
		return
	}
	if err := goose.SetDialect("clickhouse"); err != nil {
		slog.Error("Goose: failed to set dialect", "error", err)
		return
	}
	slog.Info("Running database migrations...")
	if err := goose.Up(sqlDB, "internal/migrations"); err != nil {
		slog.Error("Goose migration failed", "error", err)
		return
	}
	slog.Info("Migrations completed successfully")
}
