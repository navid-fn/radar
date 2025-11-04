package main

import (
    "context"
    "flag"
    "fmt"
    "os"
    "os/signal"
    "strings"
    "sync"
    "syscall"

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
    var exchanges string

    flag.StringVar(&exchange, "exchange", "", "Single exchange to crawl: bitpin, nobitex, ramzinex, tabdeal, wallex, coingecko")
    flag.StringVar(&exchanges, "exchanges", "", "Comma-separated list of exchanges to crawl concurrently (e.g. bitpin,ramzinex,wallex)")
    flag.Parse()

    logger := crawler.NewLogger()

    selected := []string{}
    if exchanges != "" {
        // Parse comma-separated list
        parts := strings.Split(exchanges, ",")
        for _, p := range parts {
            name := strings.TrimSpace(p)
            if name != "" {
                selected = append(selected, name)
            }
        }
    } else if exchange != "" {
        selected = append(selected, exchange)
    } else {
        // No selection provided
        fmt.Fprintf(os.Stderr, "Error: either -exchange or -exchanges flag is required\n")
        fmt.Fprintf(os.Stderr, "Usage: %s -exchange <name> OR -exchanges <a,b,c>\n", os.Args[0])
        fmt.Fprintf(os.Stderr, "\nAvailable exchanges:\n")
        fmt.Fprintf(os.Stderr, "  - bitpin\n")
        fmt.Fprintf(os.Stderr, "  - nobitex\n")
        fmt.Fprintf(os.Stderr, "  - ramzinex\n")
        fmt.Fprintf(os.Stderr, "  - tabdeal\n")
        fmt.Fprintf(os.Stderr, "  - wallex\n")
        fmt.Fprintf(os.Stderr, "  - coingecko\n")
        fmt.Fprintf(os.Stderr, "\nExamples:\n")
        fmt.Fprintf(os.Stderr, "  %s -exchange bitpin\n", os.Args[0])
        fmt.Fprintf(os.Stderr, "  %s -exchanges ramzinex,wallex\n", os.Args[0])
        fmt.Fprintf(os.Stderr, "  %s -exchanges bitpin,nobitex,coingecko\n", os.Args[0])
        os.Exit(1)
    }

    // Build crawlers
    crawlers := make([]crawler.Crawler, 0, len(selected))
    for _, name := range selected {
        switch name {
        case "bitpin":
            crawlers = append(crawlers, bitpin.NewBitpinCrawler())
        case "nobitex":
            crawlers = append(crawlers, nobitex.NewNobitexCrawler())
        case "ramzinex":
            crawlers = append(crawlers, ramzinex.NewRamzinexCrawler())
        case "tabdeal":
            crawlers = append(crawlers, tabdeal.NewTabdealCrawler())
        case "wallex":
            crawlers = append(crawlers, wallex.NewWallexCrawler())
        case "coingecko":
            crawlers = append(crawlers, coingecko.NewCoinGeckoCrawler())
        default:
            logger.Fatalf("Unknown exchange: %s", name)
        }
    }

    logger.Infof("Starting crawlers: %s", strings.Join(selected, ", "))

    // Context with graceful shutdown on SIGINT/SIGTERM
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

    var wg sync.WaitGroup
    errs := make(chan error, len(crawlers))

    for _, c := range crawlers {
        wg.Add(1)
        go func(c crawler.Crawler) {
            defer wg.Done()
            logger.Infof("Initialized %s crawler", c.GetName())
            if err := c.Run(ctx); err != nil {
                errs <- fmt.Errorf("%s failed: %w", c.GetName(), err)
            }
        }(c)
    }

    // Wait for a signal or an error
    select {
    case sig := <-sigCh:
        logger.Warnf("Received signal: %v. Shutting down...", sig)
        cancel()
    case err := <-errs:
        logger.Errorf("A crawler exited with error: %v. Shutting down...", err)
        cancel()
    }

    wg.Wait()
    logger.Info("All crawlers stopped. Bye.")
}
