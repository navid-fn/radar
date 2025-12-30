package main

import (
	"database/sql"
	"log/slog"
	"os"

	"nobitex/radar/configs"

	_ "github.com/ClickHouse/clickhouse-go/v2" // ClickHouse driver
	"github.com/pressly/goose/v3"
)

func main() {
	cfg := configs.AppLoad()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Connect using native ClickHouse driver
	db, err := sql.Open("clickhouse", cfg.DBDSN)
	if err != nil {
		logger.Error("Failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	// Verify connection
	if err := db.Ping(); err != nil {
		logger.Error("Failed to ping database", "error", err)
		os.Exit(1)
	}

	if err := goose.SetDialect("clickhouse"); err != nil {
		logger.Error("Goose: failed to set dialect", "error", err)
		os.Exit(1)
	}

	logger.Info("Running database migrations...")
	if err := goose.Up(db, "internal/migrations"); err != nil {
		logger.Error("Goose migration failed", "error", err)
		os.Exit(1)
	}

	logger.Info("Migrations completed successfully")
}
