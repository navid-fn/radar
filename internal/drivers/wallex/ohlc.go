// This file implements OHLC (candlestick) data scraping.
// API Doc: https://api.wallex.ir/v1/udf/history
//
// Response format:
// {
// "s": "ok",
// "t": [
//
//	  1733425200,
//	  1733428800
//	],
//
// "c": [
//
//	  "7209722048.0000000000000000",
//	  "7235725819.0000000000000000"
//	],
//
// "o": [
//
//	  "7286137604.0000000000000000",
//	  "7200151050.0000000000000000"
//	],
//
// "h": [
//
//	  "7286137604.0000000000000000",
//	  "7235725819.0000000000000000"
//	],
//
// "l": [
//
//	  "7209722048.0000000000000000",
//	  "7150000001.0000000000000000"
//	],
//
// "v": [
//
//	    "0.2544820000000000",
//	    "1.0201680000000000"
//	  ]
//	}
package wallex

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"nobitex/radar/internal/proto"
	"nobitex/radar/internal/scraper"

	"github.com/segmentio/kafka-go"
	"golang.org/x/time/rate"
)

// OHLCResponse represents the Wallex OHLC API response.
// Arrays are parallel: t[i], o[i], h[i], l[i], c[i], v[i] form one candle.
type OHLCResponse struct {
	Status     string   `json:"s"`
	Timestamps []int64  `json:"t"`
	Opens      []string `json:"o"`
	Highs      []string `json:"h"`
	Lows       []string `json:"l"`
	Closes     []string `json:"c"`
	Volumes    []string `json:"v"`
}

// WallexOHLC scrapes OHLC data from Nobitex API.
// Runs daily at 4:30 AM Tehran time, fetches data, then waits for next day.
type WallexOHLC struct {
	sender      *scraper.Sender
	logger      *slog.Logger
	rateLimiter *rate.Limiter

	usdtPrice float64
	usdtMu    sync.RWMutex
}

// NewWallexOHLCScraper creates a new Nobitex OHLC scraper.
func NewWallexOHLCScraper(kafkaWriter *kafka.Writer, logger *slog.Logger) *WallexOHLC {
	return &WallexOHLC{
		sender:    scraper.NewSender(kafkaWriter, logger),
		logger:    logger.With("scraper", "wallex-ohlc"),
		usdtPrice: getLatestUSDTPrice(),
	}
}

func (w *WallexOHLC) Name() string { return "wallex-ohlc" }

// Run starts the OHLC scraper with a daily schedule at 4:30 AM Tehran time.
// It waits until 4:30 AM, fetches all symbols, then waits for next day's 4:30 AM.
func (w *WallexOHLC) Run(ctx context.Context) error {
	tehran, err := time.LoadLocation("Asia/Tehran")
	if err != nil {
		return fmt.Errorf("failed to load Tehran timezone: %w", err)
	}

	w.logger.Info("Starting Wallex OHLC scraper (scheduled daily at 4:30 AM Tehran)")

	w.logger.Info("Executing initial startup fetch...")
	if err := w.fetchAllSymbols(ctx); err != nil {
		// Log error but don't crash; let the schedule continue
		w.logger.Error("Initial OHLC fetch failed", "error", err)
	}

	for {
		// Calculate next 4:30 AM Tehran
		now := time.Now().In(tehran)
		next := time.Date(now.Year(), now.Month(), now.Day(), 4, 30, 0, 0, tehran)
		if next.Before(now) {
			next = next.Add(24 * time.Hour)
		}

		w.logger.Info("Next OHLC fetch scheduled", "at", next.Format(time.RFC3339), "in", time.Until(next).Round(time.Minute))

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Until(next)):
			w.logger.Info("Starting daily OHLC fetch")
			if err := w.fetchAllSymbols(ctx); err != nil {
				w.logger.Error("OHLC fetch failed", "error", err)
			}
		}
	}
}

// fetchAllSymbols fetches OHLC data for all available symbols.
func (w *WallexOHLC) fetchAllSymbols(ctx context.Context) error {
	symbols, err := fetchMarkets(w.logger)
	if err != nil {
		return err
	}
	if len(symbols) == 0 {
		return fmt.Errorf("no symbols found")
	}

	w.rateLimiter = scraper.DefaultRateLimiter()
	w.logger.Info("Fetching OHLC for symbols", "count", len(symbols))

	for _, symbol := range symbols {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := w.rateLimiter.Wait(ctx); err != nil {
			return err
		}

		if err := w.fetchOHLC(ctx, symbol); err != nil {
			w.logger.Warn("Failed to fetch OHLC", "symbol", symbol, "error", err)
			continue
		}
	}

	w.logger.Info("OHLC fetch completed", "symbols", len(symbols))
	return nil
}

// fetchOHLC fetches OHLC data for a single symbol (last 30 days).
// Retries up to 3 times on timeout/connection errors with 2 second delay.
func (w *WallexOHLC) fetchOHLC(ctx context.Context, symbol string) error {
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

	cleanedSymbol := scraper.NormalizeSymbol("wallex", symbol)

	// Update USDT price if this is USDT/IRT
	if cleanedSymbol == "USDT/IRT" && length > 0 {
		w.usdtMu.Lock()

		closePrice, _ := strconv.ParseFloat(data.Closes[length-1], 64)
		w.usdtPrice = closePrice
		w.usdtMu.Unlock()
	}

	// Convert each candle to proto and send
	for i := range length {
		openTime := scraper.UnixToRFC3339(data.Timestamps[i])
		openPrice, err := strconv.ParseFloat(data.Opens[i], 64)
		if err != nil {
			continue
		}
		closePrice, err := strconv.ParseFloat(data.Closes[i], 64)
		if err != nil {
			continue
		}
		highPrice, err := strconv.ParseFloat(data.Highs[i], 64)
		if err != nil {
			continue
		}
		lowPrice, err := strconv.ParseFloat(data.Lows[i], 64)

		if err != nil {
			continue
		}
		volume, err := strconv.ParseFloat(data.Volumes[i], 64)
		if err != nil {
			continue
		}

		ohlc := &proto.OHLCData{
			Id:        scraper.GenerateOHLCID("wallex", cleanedSymbol, "1d", openTime),
			Exchange:  "wallex",
			Symbol:    cleanedSymbol,
			Interval:  "1d",
			Open:      scraper.NormalizePrice(cleanedSymbol, openPrice),
			High:      scraper.NormalizePrice(cleanedSymbol, highPrice),
			Low:       scraper.NormalizePrice(cleanedSymbol, lowPrice),
			Close:     scraper.NormalizePrice(cleanedSymbol, closePrice),
			Volume:    volume,
			UsdtPrice: w.usdtPrice,
			OpenTime:  openTime,
		}

		if err := w.sender.SendOHLC(ctx, ohlc); err != nil {
			w.logger.Debug("Send error", "error", err)
		}
	}

	return nil
}
