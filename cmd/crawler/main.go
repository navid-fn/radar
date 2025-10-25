package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/navid-fn/radar/internal/crawler"
	"github.com/navid-fn/radar/internal/drivers/bitpin"
	"github.com/navid-fn/radar/internal/drivers/coingecko"
	"github.com/navid-fn/radar/internal/drivers/nobitex"
	"github.com/navid-fn/radar/internal/drivers/ramzinex"
	"github.com/navid-fn/radar/internal/drivers/tabdeal"
	"github.com/navid-fn/radar/internal/drivers/wallex"
)

func main() {
	var exchange string

	flag.StringVar(&exchange, "exchange", "", "Exchange to crawl: bitpin, nobitex, ramzinex, tabdeal, wallex (required)")
	flag.Parse()

	if exchange == "" {
		fmt.Fprintf(os.Stderr, "Error: -exchange flag is required\n")
		fmt.Fprintf(os.Stderr, "Usage: %s -exchange <name>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nAvailable exchanges:\n")
		fmt.Fprintf(os.Stderr, "  - bitpin\n")
		fmt.Fprintf(os.Stderr, "  - nobitex\n")
		fmt.Fprintf(os.Stderr, "  - ramzinex\n")
		fmt.Fprintf(os.Stderr, "  - tabdeal\n")
		fmt.Fprintf(os.Stderr, "  - wallex\n")
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s -exchange bitpin\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -exchange nobitex\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -exchange wallex\n", os.Args[0])
		os.Exit(1)
	}

	if exchange == "coingecko" {
		coingecko.FetechUSDTVolumeChange()
		return
	}

	logger := crawler.NewLogger()
	logger.Infof("Starting crawler for exchange: %s", exchange)

	var c crawler.Crawler

	switch exchange {
	case "bitpin":
		c = bitpin.NewBitpinCrawler()
	case "nobitex":
		c = nobitex.NewNobitexCrawler()
	case "ramzinex":
		c = ramzinex.NewRamzinexCrawler()
	case "tabdeal":
		c = tabdeal.NewTabdealCrawler()
	case "wallex":
		c = wallex.NewWallexCrawler()
	default:
		logger.Fatalf("Unknown exchange: %s", exchange)
	}

	logger.Infof("Initialized %s crawler", c.GetName())

	// Run the crawler
	ctx := context.Background()
	if err := c.Run(ctx); err != nil {
		logger.Fatalf("Crawler failed: %v", err)
	}
}
