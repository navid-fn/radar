package main

import (
	"context"
	"fmt"
	"nobitex/radar/configs"
	// "nobitex/radar/internal/drivers/bitpin"
	// "nobitex/radar/internal/drivers/coingecko"
	// "nobitex/radar/internal/drivers/nobitex"
	// "nobitex/radar/internal/drivers/ramzinex"
	// "nobitex/radar/internal/drivers/tabdeal"
	"nobitex/radar/internal/drivers/wallex"
	"nobitex/radar/internal/scraper"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {
	cfg := configs.Load()
	if cfg.CoingeckoCfg == nil {
		panic("coingecko not configed")
	}
	scrapers := make([]scraper.Scraper, 0)

	// add new driver here
	// nobitex scraper
	// scrapers = append(scrapers, nobitex.NewNobitexScraper(cfg))
	// scrapers = append(scrapers, nobitex.NewNobitexAPIScraper(cfg))
	// //
	// // wallex scraper
	scrapers = append(scrapers, wallex.NewWallexScraper(cfg))
	// scrapers = append(scrapers, wallex.NewWallexAPIScraper(cfg))
	//
	// // ramzinex scraper
	// scrapers = append(scrapers, ramzinex.NewRamzinexScraper(cfg))
	// scrapers = append(scrapers, ramzinex.NewRamzinexAPIScraper(cfg))
	// //
	// // tabdeal scraper
	// scrapers = append(scrapers, tabdeal.NewTabdealScraper(cfg))
	// //
	// // bitpin scraper
	// scrapers = append(scrapers, bitpin.NewBitpinScraper(cfg))
	// scrapers = append(scrapers, bitpin.NewBitpinAPIScraper(cfg))
	//
	// // coingecko scraper
	// scrapers = append(scrapers, coingecko.NewCoinGeckoScraper(cfg))
	//
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup
	errs := make(chan error, len(scrapers))

	for _, c := range scrapers {
		wg.Add(1)
		go func(c scraper.Scraper) {
			defer wg.Done()
			cfg.Logger.Info("Initialized crawler", "name", c.Name())
			if err := c.Run(ctx); err != nil {
				errs <- fmt.Errorf("%s failed: %w", c.Name(), err)
			}
		}(c)
	}

	select {
	case sig := <-sigCh:
		cfg.Logger.Warn("Received signal, shutting down", "signal", sig)
		cancel()
	case err := <-errs:
		cfg.Logger.Error("A crawler exited with error, shutting down", "error", err)
		cancel()
	}

	wg.Wait()
	cfg.Logger.Info("All crawlers stopped. Bye.")
}
