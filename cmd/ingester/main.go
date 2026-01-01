package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
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
	store, err := storage.NewClickHouseStorage(appConfig.DBDSN)
	if err != nil {
		logger.Error("Failed to connect to ClickHouse", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	// Trade Kafka reader
	tradeReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{appConfig.KafkaTrade.Broker},
		Topic:          appConfig.KafkaTrade.Topic,
		GroupID:        appConfig.KafkaTrade.GroupID,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		CommitInterval: 0,
	})
	defer tradeReader.Close()

	// OHLC Kafka reader
	ohlcReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{appConfig.KafkaOHLC.Broker},
		Topic:          appConfig.KafkaOHLC.Topic,
		GroupID:        appConfig.KafkaOHLC.GroupID,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		CommitInterval: 0,
	})
	defer ohlcReader.Close()

	// Create ingesters
	tradeIngester := ingester.NewTradeIngester(
		tradeReader,
		store,
		logger.With("ingester", "trade"),
		ingester.TradeIngesterConfig{
			BatchSize:    appConfig.Ingester.BatchSize,
			BatchTimeout: time.Duration(appConfig.Ingester.BatchTimeoutSeconds) * time.Second,
		},
	)

	ohlcIngester := ingester.NewOHLCIngester(
		ohlcReader,
		store,
		logger.With("ingester", "ohlc"),
		ingester.OHLCIngesterConfig{
			BatchSize:    appConfig.Ingester.BatchSize,
			BatchTimeout: time.Duration(appConfig.Ingester.BatchTimeoutSeconds) * time.Second,
		},
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("Ingester started",
		"trade_topic", appConfig.KafkaTrade.Topic,
		"ohlc_topic", appConfig.KafkaOHLC.Topic,
	)

	// Run both ingesters in parallel
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := tradeIngester.Start(ctx); err != nil && ctx.Err() == nil {
			logger.Error("Trade ingester error", "error", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := ohlcIngester.Start(ctx); err != nil && ctx.Err() == nil {
			logger.Error("OHLC ingester error", "error", err)
		}
	}()

	wg.Wait()
	logger.Info("All ingesters shutdown complete")
}
