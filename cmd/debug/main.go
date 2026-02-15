package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"nobitex/radar/internal/drivers/bitpin"
	"nobitex/radar/internal/drivers/nobitex"
	"nobitex/radar/internal/drivers/ramzinex"
	"nobitex/radar/internal/drivers/tabdeal"
	"nobitex/radar/internal/drivers/wallex"
	pb "nobitex/radar/internal/proto"
	"nobitex/radar/internal/scraper"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

// scraperType represents the available scraper categories.
type scraperType string

const (
	scraperTypeTrade     scraperType = "trade"
	scraperTypeCandle    scraperType = "candle"
	scraperTypeOrderbook scraperType = "orderbook"
)

// validExchanges lists all supported exchange names.
var validExchanges = []string{"nobitex", "wallex", "ramzinex", "bitpin", "tabdeal"}

// DebugWriter implements scraper.MessageWriter.
// It filters by symbol and prints actual sample data to the terminal.
type DebugWriter struct {
	dataType scraperType
	symbol   string // normalized symbol to filter (e.g. "BTC/IRT")
	total    atomic.Int64
	matched  atomic.Int64
}

func createDebugWriter(dataType scraperType, symbol string) *DebugWriter {
	return &DebugWriter{
		dataType: dataType,
		symbol:   symbol,
	}
}

// WriteMessages satisfies scraper.MessageWriter.
func (d *DebugWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	for _, msg := range msgs {
		d.processMessage(msg.Value)
	}
	return nil
}

func (d *DebugWriter) processMessage(data []byte) {
	switch d.dataType {
	case scraperTypeTrade:
		d.processTrade(data)
	case scraperTypeCandle:
		d.processCandle(data)
	case scraperTypeOrderbook:
		d.processOrderbook(data)
	}
}

func (d *DebugWriter) matchesSymbol(symbol string) bool {
	return strings.EqualFold(d.symbol, symbol)
}

func (d *DebugWriter) processTrade(data []byte) {
	// Try single trade (API scrapers send individual trades)
	trade := &pb.TradeData{}
	if err := proto.Unmarshal(data, trade); err == nil && trade.GetExchange() != "" {
		d.total.Add(1)
		if !d.matchesSymbol(trade.GetSymbol()) {
			return
		}
		d.matched.Add(1)
	}
}

func (d *DebugWriter) processCandle(data []byte) {
	ohlc := &pb.CandleData{}
	if err := proto.Unmarshal(data, ohlc); err == nil && ohlc.GetExchange() != "" {
		d.total.Add(1)
		if !d.matchesSymbol(ohlc.GetSymbol()) {
			return
		}
		d.matched.Add(1)
		printCandle(ohlc)
	}
}

func (d *DebugWriter) processOrderbook(data []byte) {
	snapshot := &pb.OrderBookSnapshot{}
	if err := proto.Unmarshal(data, snapshot); err == nil && snapshot.GetExchange() != "" {
		d.total.Add(1)
		if !d.matchesSymbol(snapshot.GetSymbol()) {
			return
		}
		d.matched.Add(1)
		printOrderbook(snapshot)
	}
}

// --- printers ---

// tehranLoc is the Asia/Tehran timezone (IRST, UTC+3:30).
var tehranLoc = func() *time.Location {
	loc, err := time.LoadLocation("Asia/Tehran")
	if err != nil {
		// fallback to fixed offset +3:30
		loc = time.FixedZone("IRST", 3*3600+30*60)
	}
	return loc
}()

// toTehran converts an RFC3339 time string to Tehran local time.
// Returns the original string if parsing fails.
func toTehran(rfc3339 string) string {
	t, err := time.Parse(time.RFC3339, rfc3339)
	if err != nil {
		return rfc3339
	}
	return t.In(tehranLoc).Format("2006-01-02 15:04:05")
}

func printTrade(t *pb.TradeData) {
	fmt.Printf("[TRADE] %-10s %-12s side=%-4s price=%-14.2f volume=%-14.8f quantity=%-14.2f usdt=%-10.2f time=%s\n",
		t.GetExchange(), t.GetSymbol(), t.GetSide(),
		t.GetPrice(), t.GetVolume(), t.GetQuantity(),
		t.GetUsdtPrice(), toTehran(t.GetTime()),
	)
}

func printCandle(c *pb.CandleData) {
	fmt.Printf(
		"[CANDLE] %-10s %-12s interval=%-4s O=%-12.2f H=%-12.2f L=%-12.2f C=%-12.2f vol=%-14.2f usdt=%-10.2f open_time=%s\n",
		c.GetExchange(),
		c.GetSymbol(),
		c.GetInterval(),
		c.GetOpen(),
		c.GetHigh(),
		c.GetLow(),
		c.GetClose(),
		c.GetVolume(),
		c.GetUsdtPrice(),
		toTehran(c.GetOpenTime()),
	)
}

func printOrderbook(s *pb.OrderBookSnapshot) {
	topBid := "N/A"
	topAsk := "N/A"
	if len(s.GetBids()) > 0 {
		b := s.GetBids()[0]
		topBid = fmt.Sprintf("%.2f (vol: %.8f)", b.GetPrice(), b.GetVolume())
	}
	if len(s.GetAsks()) > 0 {
		a := s.GetAsks()[0]
		topAsk = fmt.Sprintf("%.2f (vol: %.8f)", a.GetPrice(), a.GetVolume())
	}
	fmt.Printf("[ORDERBOOK] %-10s %-12s bids=%d asks=%d top_bid=%s top_ask=%s updated=%s\n",
		s.GetExchange(), s.GetSymbol(),
		len(s.GetBids()), len(s.GetAsks()),
		topBid, topAsk, toTehran(s.GetLastUpdate()),
	)
}

// --- main ---

func main() {
	typeFlag := flag.String("type", "", "scraper type: trade, candle, or orderbook")
	exchangeFlag := flag.String("exchange", "", "exchange name (comma-separated). omit for all")
	symbolFlag := flag.String("symbol", "", "symbol to watch (e.g. BTC/IRT, ETH/USDT)")
	flag.Parse()

	if *typeFlag == "" || *symbolFlag == "" {
		fmt.Println(
			"Usage: go run cmd/debug/main.go -type=<trade|candle|orderbook> -symbol=<SYMBOL> [-exchange=<name>]",
		)
		fmt.Println()
		fmt.Println("Flags:")
		fmt.Println("  -type      Required. trade, candle, or orderbook")
		fmt.Println("  -symbol    Required. Symbol to watch (e.g. BTC/IRT, ETH/USDT)")
		fmt.Println("  -exchange  Optional. Filter by exchange (comma-separated).")
		fmt.Println("             Available: nobitex, wallex, ramzinex, bitpin, tabdeal")
		fmt.Println()
		fmt.Println("Examples:")
		fmt.Println("  go run cmd/debug/main.go -type=trade -symbol=BTC/IRT")
		fmt.Println("  go run cmd/debug/main.go -type=trade -symbol=BTC/IRT -exchange=nobitex")
		fmt.Println("  go run cmd/debug/main.go -type=orderbook -symbol=ETH/USDT -exchange=nobitex,wallex")
		os.Exit(1)
	}

	selectedType := scraperType(*typeFlag)
	if selectedType != scraperTypeTrade && selectedType != scraperTypeCandle && selectedType != scraperTypeOrderbook {
		fmt.Printf("Error: invalid type %q. Must be trade, candle, or orderbook.\n", *typeFlag)
		os.Exit(1)
	}

	exchangeFilter := parseExchangeFilter(*exchangeFlag)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	debugWriter := createDebugWriter(selectedType, *symbolFlag)
	scraperList := createScrapers(selectedType, exchangeFilter, debugWriter, logger)

	if len(scraperList) == 0 {
		fmt.Printf("Error: no scrapers found for type=%q exchange=%q\n", selectedType, *exchangeFlag)
		os.Exit(1)
	}

	exchangeLabel := "all"
	if len(exchangeFilter) > 0 {
		names := make([]string, 0, len(exchangeFilter))
		for name := range exchangeFilter {
			names = append(names, name)
		}
		sort.Strings(names)
		exchangeLabel = strings.Join(names, ",")
	}

	logger.Info("starting debug mode",
		"type", string(selectedType),
		"symbol", *symbolFlag,
		"exchanges", exchangeLabel,
		"scrapers", len(scraperList),
	)

	fmt.Printf("\n--- Watching %s data for symbol %q | Press Ctrl+C to stop ---\n\n", selectedType, *symbolFlag)

	// Graceful shutdown via Ctrl+C or SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Start all scrapers in goroutines
	var wg sync.WaitGroup
	for _, s := range scraperList {
		wg.Add(1)
		go func(s scraper.Scraper) {
			defer wg.Done()
			logger.Info("starting scraper", "name", s.Name())
			if err := s.Run(ctx); err != nil && ctx.Err() == nil {
				logger.Error("scraper failed", "name", s.Name(), "error", err)
			}
		}(s)
	}

	// Block until shutdown signal
	<-ctx.Done()
	wg.Wait()

	fmt.Printf("\n--- Done. Total records processed: %d | Matched symbol %q: %d ---\n",
		debugWriter.total.Load(), *symbolFlag, debugWriter.matched.Load())
}

// --- helpers ---

// parseExchangeFilter parses a comma-separated exchange string into a set.
// Returns nil if the input is empty (meaning "all exchanges").
func parseExchangeFilter(raw string) map[string]bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}

	filter := make(map[string]bool)
	for _, name := range strings.Split(raw, ",") {
		name = strings.TrimSpace(strings.ToLower(name))
		if name == "" {
			continue
		}

		valid := false
		for _, v := range validExchanges {
			if name == v {
				valid = true
				break
			}
		}
		if !valid {
			fmt.Printf("Warning: unknown exchange %q (available: %s)\n", name, strings.Join(validExchanges, ", "))
			continue
		}
		filter[name] = true
	}

	if len(filter) == 0 {
		return nil
	}
	return filter
}

// allowed returns true if the exchange passes the filter.
func allowed(filter map[string]bool, exchange string) bool {
	if filter == nil {
		return true
	}
	return filter[exchange]
}

// createScrapers builds the appropriate scrapers for the selected type and exchange filter.
func createScrapers(
	selectedType scraperType,
	exchangeFilter map[string]bool,
	writer scraper.MessageWriter,
	logger *slog.Logger,
) []scraper.Scraper {
	var result []scraper.Scraper

	switch selectedType {
	case scraperTypeTrade:
		if allowed(exchangeFilter, "nobitex") {
			result = append(result,
				nobitex.NewNobitexScraper(writer, logger),
			)
		}
		if allowed(exchangeFilter, "wallex") {
			result = append(result,
				wallex.NewWallexScraper(writer, logger),
			)
		}
		if allowed(exchangeFilter, "ramzinex") {
			result = append(result,
				ramzinex.NewRamzinexScraper(writer, logger),
			)
		}
		if allowed(exchangeFilter, "bitpin") {
			result = append(result,
				bitpin.NewBitpinScraper(writer, logger),
			)
		}
		if allowed(exchangeFilter, "tabdeal") {
			result = append(result,
				tabdeal.NewTabdealHttpScraper(writer, logger),
			)
		}

	case scraperTypeCandle:
		if allowed(exchangeFilter, "nobitex") {
			result = append(result, nobitex.NewNobitexCandleScraper(writer, logger))
		}
		if allowed(exchangeFilter, "wallex") {
			result = append(result, wallex.NewWallexCandleScraper(writer, logger))
		}
		if allowed(exchangeFilter, "ramzinex") {
			result = append(result, ramzinex.NewRamzinexCandleScraper(writer, logger))
		}
		if allowed(exchangeFilter, "bitpin") {
			result = append(result, bitpin.NewBitpinCandleScraper(writer, logger))
		}
		if allowed(exchangeFilter, "tabdeal") {
			result = append(result, tabdeal.NewTabdealCandleScraper(writer, logger))
		}

	case scraperTypeOrderbook:
		if allowed(exchangeFilter, "nobitex") {
			result = append(result, nobitex.NewNobitexOrderbookScraper(writer, logger))
		}
		if allowed(exchangeFilter, "wallex") {
			result = append(result, wallex.NewWallexOrderbookScraper(writer, logger))
		}
		if allowed(exchangeFilter, "bitpin") {
			result = append(result, bitpin.NewBitpinOrderbookScraper(writer, logger))
		}
		if allowed(exchangeFilter, "ramzinex") {
			result = append(result, ramzinex.NewRamzinexOrderbookScraper(writer, logger))
		}
	}

	return result
}
