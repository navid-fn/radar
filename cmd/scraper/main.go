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

	kafkaWriter := &kafka.Writer{
		Addr:         kafka.TCP(appConfig.KafkaTrade.Broker),
		Topic:        appConfig.KafkaTrade.Topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		Async:        true,
	}
	defer kafkaWriter.Close()

	// Register all scrapers
	scrapers := []scraper.Scraper{
		// Nobitex
		nobitex.NewNobitexScraper(kafkaWriter, logger),
		nobitex.NewNobitexAPIScraper(kafkaWriter, logger),

		// Wallex
		wallex.NewWallexScraper(kafkaWriter, logger),
		wallex.NewWallexAPIScraper(kafkaWriter, logger),

		// Ramzinex
		ramzinex.NewRamzinexScraper(kafkaWriter, logger),
		ramzinex.NewRamzinexAPIScraper(kafkaWriter, logger),

		// Bitpin
		bitpin.NewBitpinScraper(kafkaWriter, logger),
		bitpin.NewBitpinAPIScraper(kafkaWriter, logger),

		// Tabdeal
		tabdeal.NewTabdealScraper(kafkaWriter, logger),

		// CoinGecko
		coingecko.NewCoinGeckoScraper(kafkaWriter, logger, &appConfig.Coingecko),
	}

	scrapers = []scraper.Scraper{
		// Ramzinex
		ramzinex.NewRamzinexScraper(kafkaWriter, logger),
		ramzinex.NewRamzinexAPIScraper(kafkaWriter, logger),
	}

	// Setup graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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
