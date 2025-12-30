package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"nobitex/radar/configs"
	"nobitex/radar/internal/ingester"
	"nobitex/radar/internal/storage"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	appConfig := configs.AppLoad()

	// Connect to ClickHouse
	tradeStorage, err := storage.NewClickHouseStorage(appConfig.DBDSN)
	if err != nil {
		logger.Error("Failed to connect to ClickHouse", "error", err)
		os.Exit(1)
	}
	defer tradeStorage.Close()

	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{appConfig.KafkaTrade.Broker},
		Topic:          appConfig.KafkaTrade.Topic,
		GroupID:        appConfig.KafkaTrade.GroupID,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: 0,    // Important: We handle commits manually in Ingester!
	})
	defer kafkaReader.Close()

	svc := ingester.NewIngester(
		kafkaReader,
		tradeStorage,
		logger,
		ingester.Config{
			BatchSize:    appConfig.Ingester.BatchSize,
			BatchTimeout: time.Duration(appConfig.Ingester.BatchTimeoutSeconds) * time.Second,
		},
	)

	// Run with Graceful Shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("Ingester started successfully")

	if err := svc.Start(ctx); err != nil {
		logger.Error("Ingester stopped with error", "error", err)
		os.Exit(1)
	}

	logger.Info("Ingester shutdown complete")
}
