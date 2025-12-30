package main

import (
	"log/slog"
	"nobitex/radar/configs"
	"os"

	"github.com/pressly/goose/v3"
	"gorm.io/driver/clickhouse"
	"gorm.io/gorm"
)

func main() {
	cfg := configs.AppLoad()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	db, err := gorm.Open(clickhouse.Open(cfg.DBDSN), &gorm.Config{})

	if err != nil {
		logger.Error("Failed to connect to database", "error", err)
		return
	}
	sqlDB, err := db.DB()
	if err != nil {
		logger.Error("Failed to get sql.DB", "error", err)
		return
	}
	if err := goose.SetDialect("clickhouse"); err != nil {
		logger.Error("Goose: failed to set dialect", "error", err)
		return
	}
	logger.Info("Running database migrations...")
	if err := goose.Up(sqlDB, "internal/migrations"); err != nil {
		logger.Error("Goose migration failed", "error", err)
		return
	}
	logger.Info("Migrations completed successfully")
}
