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

	// candle ingester kafka reader and ingester
	candleReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{appConfig.KafkaCandle.Broker},
		Topic:          appConfig.KafkaCandle.Topic,
		GroupID:        appConfig.KafkaCandle.GroupID,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		CommitInterval: 0,
	})

	candleIngester := ingester.NewCandleIngester(
		candleReader,
		store,
		logger.With("ingester", "candle"),
		ingester.CandleIngesterConfig{
			BatchSize:    appConfig.Ingester.BatchSize,
			BatchTimeout: time.Duration(appConfig.Ingester.BatchTimeoutSeconds) * time.Second,
		},
	)

	// orderbook ingester kafka reader and ingester
	orderbookReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{appConfig.KafkaOrderbook.Broker},
		Topic:          appConfig.KafkaOrderbook.Topic,
		GroupID:        appConfig.KafkaOrderbook.GroupID,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		CommitInterval: 0,
	})

	orderbookIngester := ingester.NewOrderbookIngester(
		orderbookReader,
		store,
		logger.With("ingester", "orderbook"),
		ingester.OrderbookIngesterConfig{
			BatchSize:    appConfig.Ingester.BatchSize,
			BatchTimeout: time.Duration(appConfig.Ingester.BatchTimeoutSeconds) * time.Second,
		},
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	// shutdown function to close all connections
	shutdown := func() {
		// close database connection
		store.Close()

		// close kafka readers
		tradeReader.Close()
		candleReader.Close()
		orderbookReader.Close()

		defer stop()
	}
	logger.Info("Ingester started",
		"trade_topic", appConfig.KafkaTrade.Topic,
		"candle_topic", appConfig.KafkaCandle.Topic,
		"orderbook_topic", appConfig.KafkaOrderbook.Topic,
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
		if err := candleIngester.Start(ctx); err != nil && ctx.Err() == nil {
			logger.Error("candle ingester error", "error", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := orderbookIngester.Start(ctx); err != nil && ctx.Err() == nil {
			logger.Error("orderbook ingester error", "error", err)
		}
	}()

	defer shutdown()

	wg.Wait()
	logger.Info("All ingesters shutdown complete")
}
