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

	logger.Info("scraper service started ...")

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

	// Register trade scrapers scrapers

	tradeScrapers := []scraper.Scraper{
		nobitex.NewNobitexScraper(tradeWriter, logger),
		nobitex.NewNobitexAPIScraper(tradeWriter, logger),
		wallex.NewWallexScraper(tradeWriter, logger),
		wallex.NewWallexAPIScraper(tradeWriter, logger),
		ramzinex.NewRamzinexScraper(tradeWriter, logger),
		ramzinex.NewRamzinexAPIScraper(tradeWriter, logger),
		bitpin.NewBitpinScraper(tradeWriter, logger),
		bitpin.NewBitpinHttpScraper(tradeWriter, logger),
		tabdeal.NewTabdealScraper(tradeWriter, logger),
		coingecko.NewCoinGeckoScraper(tradeWriter, logger, &appConfig.Coingecko),
	}

	// Kafka writer for candle (separate topic)
	candleWriter := &kafka.Writer{
		Addr:         kafka.TCP(appConfig.KafkaCandle.Broker),
		Topic:        appConfig.KafkaCandle.Topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		Async:        true,
		Compression:  kafka.Zstd,
	}

	// Register candle scrapers
	candleScrapers := []scraper.Scraper{
		nobitex.NewNobitexCandleScraper(candleWriter, logger),
		wallex.NewWallexCandleScraper(candleWriter, logger),
		ramzinex.NewRamzinexCandleScraper(candleWriter, logger),
		bitpin.NewBitpinCandleScraper(candleWriter, logger),
		tabdeal.NewTabdealCandleScraper(candleWriter, logger),
	}

	// Kafka writer for orderbook (separate topic)
	orderbookWriter := &kafka.Writer{
		Addr:         kafka.TCP(appConfig.KafkaOrderbook.Broker),
		Topic:        appConfig.KafkaOrderbook.Topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		Async:        true,
		Compression:  kafka.Zstd,
	}

	// Register orderbook scrapers
	orderbookScrapers := []scraper.Scraper{
		nobitex.NewNobitexOrderbookScraper(orderbookWriter, logger),
		wallex.NewWallexOrderbookScraper(orderbookWriter, logger),
		bitpin.NewBitpinOrderbookScraper(orderbookWriter, logger),
		ramzinex.NewRamzinexOrderbookScraper(orderbookWriter, logger),
	}

	scrapers := append(tradeScrapers, candleScrapers...)
	scrapers = append(scrapers, orderbookScrapers...)

	// Setup graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	logger.Info("Starting scrapers",
		"trade_topic", appConfig.KafkaTrade.Topic,
		"candle_topic", appConfig.KafkaCandle.Topic,
		"orderbook_topic", appConfig.KafkaOrderbook.Topic,
	)

	// Start all scrapers
	var wg sync.WaitGroup
	for _, s := range scrapers {
		wg.Add(1)
		go func(s scraper.Scraper) {
			defer wg.Done()
			logger.Info("starting scraper", "name", s.Name())
			if err := s.Run(ctx); err != nil && ctx.Err() == nil {
				logger.Error("scraper failed", "name", s.Name(), "error", err)
			}
		}(s)
	}

	// shutdown function
	shutdown := func() {
		// stop kafka writers
		tradeWriter.Close()
		candleWriter.Close()
		orderbookWriter.Close()
		// context signal stop
		stop()
	}
	defer shutdown()

	// Wait for context cancellation (signal received)
	<-ctx.Done()
	logger.Warn("shutdown signal received, stopping scrapers...")

	// Wait for all scrapers to finish
	wg.Wait()
	logger.Info("all scrapers stopped")
}
