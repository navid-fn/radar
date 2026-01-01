package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"nobitex/radar/configs"
	"nobitex/radar/internal/drivers/bitpin"
	"nobitex/radar/internal/drivers/coingecko"
	"nobitex/radar/internal/drivers/nobitex"
	"nobitex/radar/internal/drivers/ramzinex"
	"nobitex/radar/internal/drivers/tabdeal"
	"nobitex/radar/internal/drivers/wallex"
	"nobitex/radar/internal/scraper"

	"github.com/segmentio/kafka-go"
)

func main() {
	appConfig := configs.AppLoad()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Kafka writer for trades
	tradeWriter := &kafka.Writer{
		Addr:         kafka.TCP(appConfig.KafkaTrade.Broker),
		Topic:        appConfig.KafkaTrade.Topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		Async:        true,
		Compression:  kafka.Zstd,
	}
	defer tradeWriter.Close()


	// Register trade scrapers scrapers
	tradeScrapers := []scraper.Scraper{
		nobitex.NewNobitexScraper(tradeWriter, logger),
		nobitex.NewNobitexAPIScraper(tradeWriter, logger),
		wallex.NewWallexScraper(tradeWriter, logger),
		wallex.NewWallexAPIScraper(tradeWriter, logger),
		ramzinex.NewRamzinexScraper(tradeWriter, logger),
		ramzinex.NewRamzinexAPIScraper(tradeWriter, logger),
		bitpin.NewBitpinScraper(tradeWriter, logger),
		bitpin.NewBitpinAPIScraper(tradeWriter, logger),
		tabdeal.NewTabdealScraper(tradeWriter, logger),
		coingecko.NewCoinGeckoScraper(tradeWriter, logger, &appConfig.Coingecko),
	}
	tradeScrapers = []scraper.Scraper{}

	// Kafka writer for OHLC (separate topic)
	ohlcWriter := &kafka.Writer{
		Addr:         kafka.TCP(appConfig.KafkaOHLC.Broker),
		Topic:        appConfig.KafkaOHLC.Topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		Async:        true,
		Compression:  kafka.Zstd,
	}
	defer ohlcWriter.Close()

	// Register OHLC scrapers
	ohlcScrapers := []scraper.Scraper{
		// nobitex.NewNobitexOHLCScraper(ohlcWriter, logger),
		wallex.NewWallexOHLCScraper(ohlcWriter, logger),
	}

	scrapers := append(tradeScrapers, ohlcScrapers...)

	// Setup graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("Starting scrapers",
		"trade_topic", appConfig.KafkaTrade.Topic,
		"ohlc_topic", appConfig.KafkaOHLC.Topic,
	)

	// Start all scrapers
	var wg sync.WaitGroup
	for _, s := range scrapers {
		wg.Add(1)
		go func(s scraper.Scraper) {
			defer wg.Done()
			logger.Info("Starting scraper", "name", s.Name())
			if err := s.Run(ctx); err != nil && ctx.Err() == nil {
				logger.Error("Scraper failed", "name", s.Name(), "error", err)
			}
		}(s)
	}

	// Wait for context cancellation (signal received)
	<-ctx.Done()
	logger.Warn("Shutdown signal received, stopping scrapers...")

	// Wait for all scrapers to finish
	wg.Wait()
	logger.Info("All scrapers stopped")
}
