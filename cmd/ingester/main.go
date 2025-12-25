package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"nobitex/radar/configs"
	"nobitex/radar/internal/ingester"
	"nobitex/radar/internal/storage"

	"gorm.io/driver/clickhouse"
	"gorm.io/gorm"
)

func main() {
	cfg := configs.Load()
	db, err := gorm.Open(clickhouse.Open(cfg.ClickHouseDSN), &gorm.Config{})

	tradeStorage := storage.NewGormTradeStorage(db)
	if err != nil {
		cfg.Logger.Error("Failed to connect to database", "error", err)
		os.Exit(1)
	}

	kafkaConsumer := ingester.NewIngester(cfg, tradeStorage)

	ctx, cancel := context.WithCancel(context.Background())

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	shutdown := func(sig os.Signal) {
		cfg.Logger.Info("Received signal, initiating graceful shutdown", "signal", sig)
		cancel()
		cfg.Logger.Info("Application stopped successfully")
	}

	go func() {
		if err := kafkaConsumer.Start(ctx); err != nil {
			cfg.Logger.Error("Consumer error", "error", err)
		}
	}()

	sig := <-sigChan
	shutdown(sig)
}
