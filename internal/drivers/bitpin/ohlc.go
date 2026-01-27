// This file implements OHLC (candlestick) data scraping.
// There is no doc for this API, found it in their homepage
//
// Response format:
// [
//
//	{
//		"open":7880000000.0,
//		"close":7997908007.0,
//		"low":"7794557593",
//		"high":"8050000000.0",
//		"volume":"1.087160998021016446803192218",
//		"ts":1736886600.0,
//		"resolution":"1d",
//		"time":1736973000000
//		}, ...
//
// ]
package bitpin

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"nobitex/radar/internal/proto"
	"nobitex/radar/internal/scraper"
	"strconv"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"golang.org/x/time/rate"
)

type OHLCResponse struct {
	Timestamp float64 `json:"ts"`
	Open      float64 `json:"open"`
	High      string  `json:"high"`
	Low       string  `json:"low"`
	Close     float64 `json:"close"`
	Volume    any     `json:"volume"`
}

// BitpinOHLC scrapes OHLC data from Nobitex API.
// Runs daily at 4:30 AM Tehran time, fetches data, then waits for next day.
type BitpinOHLC struct {
	sender      *scraper.Sender
	logger      *slog.Logger
	rateLimiter *rate.Limiter

	usdtPrice float64
	usdtMu    sync.RWMutex
}

// NewBitpinOHLCScraper creates a new Nobitex OHLC scraper.
func NewBitpinOHLCScraper(kafkaWriter *kafka.Writer, logger *slog.Logger) *BitpinOHLC {
	return &BitpinOHLC{
		sender: scraper.NewSender(kafkaWriter, logger),
		logger: logger.With("scraper", "bipin-ohlc"),
	}
}

func (n *BitpinOHLC) Name() string { return "bipin-ohlc" }

// Run starts the OHLC scraper with a daily schedule at 4:30 AM Tehran time.
// It waits until 4:30 AM, fetches all symbols, then waits for next day's 4:30 AM.
func (n *BitpinOHLC) Run(ctx context.Context) error {
	tehran, err := time.LoadLocation("Asia/Tehran")
	if err != nil {
		return fmt.Errorf("failed to load Tehran timezone: %w", err)
	}
	n.logger.Info("Starting Bitpin OHLC scraper (scheduled daily at 4:30 AM Tehran)")
	n.usdtPrice = getLatestUSDTPrice()
	n.logger.Info("Executing initial startup fetch...")
	if err := n.fetchAllSymbols(ctx); err != nil {
		// Log error but don't crash; let the schedule continue
		n.logger.Error("Initial OHLC fetch failed", "error", err)
	}

	for {
		// Calculate next 4:30 AM Tehran
		now := time.Now().In(tehran)
		next := time.Date(now.Year(), now.Month(), now.Day(), 4, 30, 0, 0, tehran)
		if next.Before(now) {
			next = next.Add(24 * time.Hour)
		}

		n.logger.Info("Next OHLC fetch scheduled", "at", next.Format(time.RFC3339), "in", time.Until(next).Round(time.Minute))

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Until(next)):
			n.logger.Info("Starting daily OHLC fetch")
			if err := n.fetchAllSymbols(ctx); err != nil {
				n.logger.Error("OHLC fetch failed", "error", err)
			}
		}
	}
}

// fetchAllSymbols fetches OHLC data for all available symbols.
func (n *BitpinOHLC) fetchAllSymbols(ctx context.Context) error {
	symbols, err := fetchMarkets(n.logger)
	if err != nil {
		return err
	}
	if len(symbols) == 0 {
		return fmt.Errorf("no symbols found")
	}

	n.rateLimiter = scraper.DefaultRateLimiter()
	n.logger.Info("Fetching OHLC for symbols", "count", len(symbols))

	for _, symbol := range symbols {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := n.rateLimiter.Wait(ctx); err != nil {
			return err
		}

		if err := n.fetchOHLC(ctx, symbol); err != nil {
			n.logger.Warn("Failed to fetch OHLC", "symbol", symbol, "error", err)
			continue
		}
	}

	n.logger.Info("OHLC fetch completed", "symbols", len(symbols))
	return nil
}

// fetchOHLC fetches OHLC data for a single symbol (last 30 days).
// Retries up to 3 times on timeout/connection errors with 2 second delay.
func (n *BitpinOHLC) fetchOHLC(ctx context.Context, symbol string) error {
	// Fetch last 30 days of daily OHLC
	fromTimestamp := scraper.ToMidnight(time.Now().AddDate(0, 0, -30)).Unix()
	toTimestamp := scraper.ToMidnight(time.Now()).AddDate(0, 0, -1).Unix()

	url := fmt.Sprintf(ohlcAPI, symbol, fromTimestamp, toTimestamp)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	resp, err := scraper.DoWithRetry(ctx, req, 3, 2*time.Second)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}

	var data []OHLCResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return err
	}

	// Validate parallel arrays have same length
	length := len(data)
	if length == 0 {
		return nil // No data
	}

	cleanedSymbol := scraper.NormalizeSymbol("bitpin", symbol)

	// Update USDT price if this is USDT/IRT
	if cleanedSymbol == "USDT/IRT" && length > 0 {
		n.usdtMu.Lock()
		n.usdtPrice = scraper.NormalizePrice(cleanedSymbol, data[length-1].Close)
		n.usdtMu.Unlock()
	}

	// Convert each candle to proto and send
	for _, d := range data {
		openTime := scraper.UnixToRFC3339(int64(d.Timestamp))
		high, err := strconv.ParseFloat(d.High, 64)
		if err != nil {
			continue
		}
		low, err := strconv.ParseFloat(d.Low, 64)
		if err != nil {
			continue
		}
		var volumeParsed float64

		switch volume := d.Volume.(type) {
		case string:
			{
				volumeParsed, _ = strconv.ParseFloat(volume, 64)
			}
		case float64:
			{
				volumeParsed = volume
			}
		}

		ohlc := &proto.OHLCData{
			Id:        scraper.GenerateOHLCID("bitpin", cleanedSymbol, "1d", openTime),
			Exchange:  "bitpin",
			Symbol:    cleanedSymbol,
			Interval:  "1d",
			Open:      scraper.NormalizePrice(cleanedSymbol, d.Open),
			High:      scraper.NormalizePrice(cleanedSymbol, high),
			Low:       scraper.NormalizePrice(cleanedSymbol, low),
			Close:     scraper.NormalizePrice(cleanedSymbol, d.Close),
			Volume:    volumeParsed,
			UsdtPrice: n.usdtPrice,
			OpenTime:  openTime,
		}

		if err := n.sender.SendOHLC(ctx, ohlc); err != nil {
			n.logger.Debug("Send error", "error", err)
		}
	}

	return nil
}
