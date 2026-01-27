// Package nobitex provides scrapers for Nobitex exchange.
// This file implements OHLC (candlestick) data scraping.
// API Doc: https://apidocs.nobitex.ir/#6ae2dae4a2
//
// Response format:
//
//	{
//	  "s": "ok",
//	  "t": [1562095800, 1562182200],
//	  "o": [146272500, 150551000],
//	  "h": [155869600, 161869500],
//	  "l": [140062400, 150551000],
//	  "c": [151440200, 157000000],
//	  "v": [18.221362316, 9.8592626506]
//	}
package nobitex

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"nobitex/radar/internal/proto"
	"nobitex/radar/internal/scraper"

	"github.com/segmentio/kafka-go"
	"golang.org/x/time/rate"
)

// OHLCResponse represents the Nobitex OHLC API response.
// Arrays are parallel: t[i], o[i], h[i], l[i], c[i], v[i] form one candle.
type OHLCResponse struct {
	Status     string    `json:"s"`
	Timestamps []int64   `json:"t"`
	Opens      []float64 `json:"o"`
	Highs      []float64 `json:"h"`
	Lows       []float64 `json:"l"`
	Closes     []float64 `json:"c"`
	Volumes    []float64 `json:"v"`
}

// NobitexOHLC scrapes OHLC data from Nobitex API.
// Runs daily at 4:30 AM Tehran time, fetches data, then waits for next day.
type NobitexOHLC struct {
	sender      *scraper.Sender
	logger      *slog.Logger
	rateLimiter *rate.Limiter

	usdtPrice float64
	usdtMu    sync.RWMutex
}

// NewNobitexOHLCScraper creates a new Nobitex OHLC scraper.
func NewNobitexOHLCScraper(kafkaWriter *kafka.Writer, logger *slog.Logger) *NobitexOHLC {
	return &NobitexOHLC{
		sender:    scraper.NewSender(kafkaWriter, logger),
		logger:    logger.With("scraper", "nobitex-ohlc"),
	}
}

func (n *NobitexOHLC) Name() string { return "nobitex-ohlc" }

// Run starts the OHLC scraper with a daily schedule at 4:30 AM Tehran time.
// It waits until 4:30 AM, fetches all symbols, then waits for next day's 4:30 AM.
func (n *NobitexOHLC) Run(ctx context.Context) error {
	n.usdtPrice = getLatestUSDTPrice()
	tehran, err := time.LoadLocation("Asia/Tehran")
	if err != nil {
		return fmt.Errorf("failed to load Tehran timezone: %w", err)
	}

	n.logger.Info("Starting Nobitex OHLC scraper (scheduled daily at 4:30 AM Tehran)")

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
func (n *NobitexOHLC) fetchAllSymbols(ctx context.Context) error {
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
func (n *NobitexOHLC) fetchOHLC(ctx context.Context, symbol string) error {
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

	var data OHLCResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return err
	}

	if data.Status != "ok" {
		return fmt.Errorf("API returned status: %s", data.Status)
	}

	// Validate parallel arrays have same length
	length := len(data.Timestamps)
	if length == 0 {
		return nil // No data
	}
	if len(data.Opens) != length || len(data.Highs) != length ||
		len(data.Lows) != length || len(data.Closes) != length || len(data.Volumes) != length {
		return fmt.Errorf("mismatched array lengths in OHLC response")
	}

	cleanedSymbol := scraper.NormalizeSymbol("nobitex", symbol)

	// Update USDT price if this is USDT/IRT
	if cleanedSymbol == "USDT/IRT" && length > 0 {
		n.usdtMu.Lock()
		n.usdtPrice = scraper.NormalizePrice(cleanedSymbol, data.Closes[length-1])
		n.usdtMu.Unlock()
	}

	// Convert each candle to proto and send
	for i := range length {
		openTime := scraper.UnixToRFC3339(data.Timestamps[i])

		ohlc := &proto.OHLCData{
			Id:        scraper.GenerateOHLCID("nobitex", cleanedSymbol, "1d", openTime),
			Exchange:  "nobitex",
			Symbol:    cleanedSymbol,
			Interval:  "1d",
			Open:      scraper.NormalizePrice(cleanedSymbol, data.Opens[i]),
			High:      scraper.NormalizePrice(cleanedSymbol, data.Highs[i]),
			Low:       scraper.NormalizePrice(cleanedSymbol, data.Lows[i]),
			Close:     scraper.NormalizePrice(cleanedSymbol, data.Closes[i]),
			Volume:    data.Volumes[i],
			UsdtPrice: n.usdtPrice,
			OpenTime:  openTime,
		}

		if err := n.sender.SendOHLC(ctx, ohlc); err != nil {
			n.logger.Debug("Send error", "error", err)
		}
	}

	return nil
}
