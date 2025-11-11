package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"nobitex/radar/internal/consumer"
	"nobitex/radar/internal/consumer/config"
	"nobitex/radar/internal/repository"

	"github.com/pressly/goose/v3"
	"gorm.io/driver/clickhouse"
	"gorm.io/gorm"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	cfg := config.Load()
	db, err := gorm.Open(clickhouse.Open(cfg.ClickHouseDSN), &gorm.Config{})
	if err != nil {
		logger.Error("Failed to connect to database", "error", err)
		os.Exit(1)
	}

	migrateFlag := flag.Bool("migrate", false, "Run database migrations and exit")
	flag.Parse()

	if *migrateFlag {
		sqlDB, err := db.DB()
		if err != nil {
			logger.Error("Failed to get sql.DB", "error", err)
			os.Exit(1)
		}
		if err := goose.SetDialect("clickhouse"); err != nil {
			logger.Error("Goose: failed to set dialect", "error", err)
			os.Exit(1)
		}
		logger.Info("Running database migrations...")
		if err := goose.Up(sqlDB, "internal/migrations"); err != nil {
			logger.Error("Goose migration failed", "error", err)
			os.Exit(1)
		}
		logger.Info("Migrations completed successfully")
		return
	}

	tradeRepo := repository.NewGormTradeRepository(db)

	kafkaConsumer := consumer.NewConsumer(cfg, tradeRepo, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := kafkaConsumer.Start(ctx); err != nil {
			logger.Error("Consumer error", "error", err)
		}
	}()

	sig := <-sigChan
	logger.Info("Received signal, initiating graceful shutdown", "signal", sig)

	cancel()

	logger.Info("Application stopped successfully")
}
