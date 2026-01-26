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

	// trade ingester kafka reader and ingester
	tradeReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{appConfig.KafkaTrade.Broker},
		Topic:          appConfig.KafkaTrade.Topic,
		GroupID:        appConfig.KafkaTrade.GroupID,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		CommitInterval: 0,
	})

	tradeIngester := ingester.NewTradeIngester(
		tradeReader,
		store,
		logger.With("ingester", "trade"),
		ingester.TradeIngesterConfig{
			BatchSize:    appConfig.Ingester.BatchSize,
			BatchTimeout: time.Duration(appConfig.Ingester.BatchTimeoutSeconds) * time.Second,
		},
	)

	// OHLC ingester kafka reader and ingester
	ohlcReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{appConfig.KafkaOHLC.Broker},
		Topic:          appConfig.KafkaOHLC.Topic,
		GroupID:        appConfig.KafkaOHLC.GroupID,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		CommitInterval: 0,
	})

	ohlcIngester := ingester.NewOHLCIngester(
		ohlcReader,
		store,
		logger.With("ingester", "ohlc"),
		ingester.OHLCIngesterConfig{
			BatchSize:    appConfig.Ingester.BatchSize,
			BatchTimeout: time.Duration(appConfig.Ingester.BatchTimeoutSeconds) * time.Second,
		},
	)

	// OHLC ingester kafka reader and ingester
	depthReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{appConfig.KafkaDepth.Broker},
		Topic:          appConfig.KafkaDepth.Topic,
		GroupID:        appConfig.KafkaDepth.GroupID,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		CommitInterval: 0,
	})

	depthIngester := ingester.NewDepthIngester(
		depthReader,
		store,
		logger.With("ingester", "depths"),
		ingester.DepthIngesterConfig{
			BatchSize:    appConfig.Ingester.BatchSize,
			BatchTimeout: time.Duration(appConfig.Ingester.BatchTimeoutSeconds) * time.Second,
		},
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	// shutdown function to close all connections
	shutdown := func() {
		// close database connection
		defer store.Close()

		// close kafka readers
		defer tradeReader.Close()
		defer ohlcReader.Close()
		defer depthReader.Close()

		defer stop()
	}
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
			logger.Error("trade ingester error", "error", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := ohlcIngester.Start(ctx); err != nil && ctx.Err() == nil {
			logger.Error("ohlc ingester error", "error", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := depthIngester.Start(ctx); err != nil && ctx.Err() == nil {
			logger.Error("depth ingester error", "error", err)
		}
	}()

	defer shutdown()

	wg.Wait()
	logger.Info("All ingesters shutdown complete")
}
