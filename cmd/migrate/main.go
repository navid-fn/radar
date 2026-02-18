package main

import (
	"database/sql"
	"flag"
	"log/slog"
	"os"

	"nobitex/radar/configs"

	_ "github.com/ClickHouse/clickhouse-go/v2" // ClickHouse driver
	"github.com/pressly/goose/v3"
)

func main() {
	cfg := configs.LoadConfigs()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// migrations movement, up or down
	migrations_type := flag.String("type", "up", "migrations to move (up or down)")
	flag.Parse()

	// Connect using native ClickHouse driver
	db, err := sql.Open("clickhouse", cfg.ClichhouseDSN)
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

	switch *migrations_type {
	case "up":
		{
			if err := goose.Up(db, "internal/migrations"); err != nil {
				logger.Error("Goose migration failed", "error", err)
				os.Exit(1)
			}
		}
	case "down":
		{
			if err := goose.Down(db, "internal/migrations"); err != nil {
				logger.Error("Goose migration failed", "error", err)
				os.Exit(1)
			}
		}

	default:
		{
			if err := goose.Up(db, "internal/migrations"); err != nil {
				logger.Error("Goose migration failed", "error", err)
				os.Exit(1)
			}
		}

	}

	logger.Info("Migrations completed successfully")
}
