// This file implements OHLC (candlestick) data scraping.
// There is no doc for this API, found it in their homepage
//
// Response format:
//
//	{
//	    "data": [
//	        {
//	            "time": 1769502600,
//	            "low": 87921.68,
//	            "high": 88378.67,
//	            "open": 88240.5,
//	            "close": 88103.73,
//	            "volume": 0.260667
//	        },
//	        ..... ] }
package tabdeal

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"nobitex/radar/internal/proto"
	"nobitex/radar/internal/scraper"

	"github.com/segmentio/kafka-go"
	"golang.org/x/time/rate"
)

// OHLCResponse represents the tabdeal OHLC API response.
type OHLCData struct {
	Timestamp float64 `json:"time"`
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	Volume    float64 `json:"volume"`
}

type OHLCResponse struct {
	Data   []OHLCData `json:"data"`
	NoData bool       `json:"no_data"`
}

// TabdealOHLC scrapes OHLC data from tabdeal API.
// Runs daily at 4:30 AM Tehran time, fetches data, then waits for next day.
type TabdealOHLC struct {
	sender      *scraper.Sender
	logger      *slog.Logger
	rateLimiter *rate.Limiter
}

// NewTabdealOHLCScraper creates a new tabdeal OHLC scraper.
func NewTabdealOHLCScraper(kafkaWriter *kafka.Writer, logger *slog.Logger) *TabdealOHLC {
	return &TabdealOHLC{
		sender: scraper.NewSender(kafkaWriter, logger),
		logger: logger.With("scraper", "tabdeal-ohlc"),
	}
}

func (r *TabdealOHLC) Name() string { return "tabdeal-ohlc" }

// Run starts the OHLC scraper with a daily schedule at 4:30 AM Tehran time.
// It waits until 4:30 AM, fetches all symbols, then waits for next day's 4:30 AM.
func (r *TabdealOHLC) Run(ctx context.Context) error {
	tehran, err := time.LoadLocation("Asia/Tehran")
	if err != nil {
		return fmt.Errorf("failed to load Tehran timezone: %w", err)
	}
	r.logger.Info("starting tabdeal OHLC scraper (scheduled daily at 4:30 AM Tehran)")

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
				r.logger.Error("ohlc fetch failed", "error", err)
			}
		}
	}
}

// fetchAllSymbols fetches OHLC data for all available symbols.
func (r *TabdealOHLC) fetchAllSymbols(ctx context.Context) error {
	symbols, err := fetchMarkets()
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

		if err := r.fetchOHLC(ctx, symbol); err != nil {
			r.logger.Warn("failed to fetch OHLC", "symbol", symbol, "error", err)
			continue
		}
	}

	r.logger.Info("ohlc fetch completed", "symbols", len(symbols))
	return nil
}

// fetchOHLC fetches OHLC data for a single symbol (last 30 days).
// Retries up to 3 times on timeout/connection errors with 2 second delay.
func (r *TabdealOHLC) fetchOHLC(ctx context.Context, symbol string) error {
	// Fetch last 30 days of daily OHLC
	fromTimestamp := scraper.ToMidnight(time.Now().AddDate(0, 0, -30)).Unix()
	toTimestamp := scraper.ToMidnight(time.Now()).AddDate(0, 0, -2).Unix()
	symbolUrl := strings.Replace(scraper.NormalizeSymbol("tabdeal", symbol), "/", "_", 1)
	url := fmt.Sprintf(ohlcURL, symbolUrl, fromTimestamp, toTimestamp)
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

	var candleResponse OHLCResponse
	if err := json.NewDecoder(resp.Body).Decode(&candleResponse); err != nil {
		return err
	}

	if candleResponse.NoData {
		return fmt.Errorf("API returned no data")
	}

	cleanedSymbol := scraper.NormalizeSymbol("tabdeal", symbol)
	var candles []*proto.OHLCData

	for _, data := range candleResponse.Data {
		openTime := scraper.UnixToRFC3339(int64(data.Timestamp))
		ohlc := &proto.OHLCData{
			Id:       scraper.GenerateOHLCID("tabdeal", cleanedSymbol, "1d", openTime),
			Exchange: "tabdeal",
			Symbol:   cleanedSymbol,
			Interval: "1d",
			Open:     data.Open,
			High:     data.High,
			Low:      data.Low,
			Close:    data.Close,
			Volume:   data.Volume,
			OpenTime: openTime,
		}
		candles = append(candles, ohlc)
	}
	// Convert each candle to proto and send
	if err := r.sender.SendOHLCBatch(ctx, candles); err != nil {
		// TODO: add metric
		r.logger.Debug("send error", "error", err)
	}

	return nil
}
