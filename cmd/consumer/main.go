package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"nobitex/radar/internal/consumer"
	"nobitex/radar/internal/consumer/config"
	"nobitex/radar/internal/repository"

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
