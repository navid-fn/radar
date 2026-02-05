// This file implements OHLC (candlestick) data scraping.
// There is no doc for this API, found it in their homepage
//
// Response format:
// {
//
//	"c":
//
// [
//
//	40792,
//	40973,
//	40394,
//	40804
//
// ],
// "h":
// [
//
//	41200,
//	41200,
//	41118,
//	41000
//
// ],
// "l":
// [
//
//	40791,
//	40792,
//	40394,
//	40394
//
// ],
// "o":
// [
//
//	40792,
//	40792,
//	40973,
//	40394
//
// ],
// "s": "ok",
// "t":
// [
//
//	1728389453,
//	1728392561,
//	1728396005,
//	1728399681
//
// ],
// "v":
//
//		[
//		    24159.9025,
//		    8416.6203,
//		    4739.7176,
//		    14744.8487
//		]
//	}
package ramzinex

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"nobitex/radar/internal/proto"
	"nobitex/radar/internal/scraper"

	"github.com/segmentio/kafka-go"
	"golang.org/x/time/rate"
)

// OHLCResponse represents the Ramzinex OHLC API response.
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

// RamzinexOHLC scrapes OHLC data from Ramzinex API.
// Runs daily at 4:30 AM Tehran time, fetches data, then waits for next day.
type RamzinexOHLC struct {
	sender         *scraper.Sender
	logger         *slog.Logger
	symbolIDToName map[int]string
	rateLimiter    *rate.Limiter
	usdtPrice      float64
	usdtMu         sync.RWMutex
}

// NewRamzinexOHLCScraper creates a new Ramzinex OHLC scraper.
func NewRamzinexOHLCScraper(kafkaWriter *kafka.Writer, logger *slog.Logger) *RamzinexOHLC {
	return &RamzinexOHLC{
		sender:         scraper.NewSender(kafkaWriter, logger),
		logger:         logger.With("scraper", "ramzinex-ohlc"),
		symbolIDToName: make(map[int]string),
	}
}

func (r *RamzinexOHLC) Name() string { return "ramzinex-ohlc" }

// Run starts the OHLC scraper with a daily schedule at 4:30 AM Tehran time.
// It waits until 4:30 AM, fetches all symbols, then waits for next day's 4:30 AM.
func (r *RamzinexOHLC) Run(ctx context.Context) error {
	r.usdtPrice = float64(getLatestUSDTPrice())
	tehran, err := time.LoadLocation("Asia/Tehran")
	if err != nil {
		return fmt.Errorf("failed to load Tehran timezone: %w", err)
	}

	r.logger.Info("starting Ramzinex OHLC scraper (scheduled daily at 4:30 AM Tehran)")

	if err := r.fetchAllSymbols(ctx); err != nil {
		// Log error but don't crash; let the schedule continue
		r.logger.Error("initial OHLC fetch failed", "error", err)
	}

	for {
		// Calculate next 4:30 AM Tehran
		now := time.Now().In(tehran)
		next := time.Date(now.Year(), now.Month(), now.Day(), 4, 30, 0, 0, tehran)
		if next.Before(now) {
			next = next.Add(24 * time.Hour)
		}

		r.logger.Info("next OHLC fetch scheduled", "at", next.Format(time.RFC3339), "in", time.Until(next).Round(time.Minute))

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Until(next)):
			if err := r.fetchAllSymbols(ctx); err != nil {
				r.logger.Error("OHLC fetch failed", "error", err)
			}
		}
	}
}

// fetchAllSymbols fetches OHLC data for all available symbols.
func (r *RamzinexOHLC) fetchAllSymbols(ctx context.Context) error {
	symbols, _, err := fetchPairs()
	if err != nil {
		return err
	}

	if len(symbols) == 0 {
		return fmt.Errorf("no pairs found")
	}

	r.rateLimiter = scraper.DefaultRateLimiter()

	for _, symbol := range symbols {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := r.rateLimiter.Wait(ctx); err != nil {
			return err
		}

		if err := r.fetchOHLC(ctx, strings.ToUpper(symbol.Name)); err != nil {
			// TODO: add metric
			r.logger.Warn("failed to fetch OHLC", "symbol", symbol, "error", err)
			continue
		}
	}

	r.logger.Info("OHLC fetch completed", "symbols", len(symbols))
	return nil
}

// create url with encoded params
func (r *RamzinexOHLC) createURL(symbol string) string {
	// Fetch last 30 days of daily OHLC
	fromTimestamp := scraper.ToMidnight(time.Now().AddDate(0, 0, -2)).Unix()
	toTimestamp := scraper.ToMidnight(time.Now()).AddDate(0, 0, -1).Unix()

	params := url.Values{}
	params.Add("symbol", symbol)
	params.Add("resolution", "1D")
	params.Add("from", strconv.FormatInt(fromTimestamp, 10))
	params.Add("to", strconv.FormatInt(toTimestamp, 10))

	fullURL := ohlcURL + "?" + params.Encode()

	return fullURL

}

// fetchOHLC fetches OHLC data for a single symbol (last 30 days).
// Retries up to 3 times on timeout/connection errors with 2 second delay.
func (r *RamzinexOHLC) fetchOHLC(ctx context.Context, symbol string) error {
	url := r.createURL(symbol)
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
		// there is no_data returned. No need for send error
		return nil
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

	cleanedSymbol := scraper.NormalizeSymbol("ramzinex", symbol)

	// Update USDT price if this is USDT/IRT
	if cleanedSymbol == "USDT/IRT" && length > 0 {
		r.usdtMu.Lock()
		r.usdtPrice = scraper.NormalizePrice(cleanedSymbol, data.Closes[length-1])
		r.usdtMu.Unlock()
	}

	// Convert each candle to proto and send
	for i := range length {
		openTime := scraper.UnixToRFC3339(data.Timestamps[i])

		ohlc := &proto.OHLCData{
			Id:        scraper.GenerateOHLCID("ramzinex", cleanedSymbol, "1d", openTime),
			Exchange:  "ramzinex",
			Symbol:    cleanedSymbol,
			Interval:  "1d",
			Open:      scraper.NormalizePrice(cleanedSymbol, data.Opens[i]),
			High:      scraper.NormalizePrice(cleanedSymbol, data.Highs[i]),
			Low:       scraper.NormalizePrice(cleanedSymbol, data.Lows[i]),
			Close:     scraper.NormalizePrice(cleanedSymbol, data.Closes[i]),
			Volume:    data.Volumes[i],
			UsdtPrice: r.usdtPrice,
			OpenTime:  openTime,
		}
		if err := r.sender.SendOHLC(ctx, ohlc); err != nil {
			// TODO: add metric
			r.logger.Debug("send error", "error", err)
		}
	}

	return nil
}
