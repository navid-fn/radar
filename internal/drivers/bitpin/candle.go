// This file implements candle data scraping.
// There is no doc for this API, found it in their homepage.
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
	"strconv"
	"sync"
	"time"

	"nobitex/radar/internal/proto"
	"nobitex/radar/internal/scraper"

	"golang.org/x/time/rate"
)

type CandleResponse struct {
	Timestamp float64 `json:"ts"`
	Open      float64 `json:"open"`
	High      string  `json:"high"`
	Low       string  `json:"low"`
	Close     float64 `json:"close"`
	Volume    any     `json:"volume"`
}

// BitpinCandleScraper scrapes candle data from Bitpin API.
// Runs daily at 4:30 AM Tehran time, fetches data, then waits for next day.
type BitpinCandleScraper struct {
	sender      *scraper.Sender
	logger      *slog.Logger
	rateLimiter *rate.Limiter

	usdtPrice float64
	usdtMu    sync.RWMutex
}

// NewBitpinCandleScraper creates a new Bitpin candle scraper.
func NewBitpinCandleScraper(writer scraper.MessageWriter, logger *slog.Logger) *BitpinCandleScraper {
	return &BitpinCandleScraper{
		sender: scraper.NewSender(writer, logger),
		logger: logger.With("scraper", "bitpin-candle"),
	}
}

func NewBitpinCandleScraperScraper(writer scraper.MessageWriter, logger *slog.Logger) *BitpinCandleScraper {
	return NewBitpinCandleScraper(writer, logger)
}

func (n *BitpinCandleScraper) Name() string { return "bitpin-candle" }

func (n *BitpinCandleScraper) Run(ctx context.Context) error {
	tehran, err := time.LoadLocation("Asia/Tehran")
	if err != nil {
		return fmt.Errorf("failed to load Tehran timezone: %w", err)
	}
	n.logger.Info("starting Bitpin candle scraper (scheduled daily at 4:30 AM Tehran)")
	n.usdtPrice = getLatestUSDTPrice()
	if err := n.fetchAllSymbols(ctx); err != nil {
		n.logger.Error("initial candle fetch failed", "error", err)
	}

	for {
		now := time.Now().In(tehran)
		next := time.Date(now.Year(), now.Month(), now.Day(), 4, 30, 0, 0, tehran)
		if next.Before(now) {
			next = next.Add(24 * time.Hour)
		}

		n.logger.Info(
			"next candle fetch scheduled",
			"at",
			next.Format(time.RFC3339),
			"in",
			time.Until(next).Round(time.Minute),
		)

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Until(next)):
			if err := n.fetchAllSymbols(ctx); err != nil {
				n.logger.Error("candle fetch failed", "error", err)
			}
		}
	}
}

func (n *BitpinCandleScraper) fetchAllSymbols(ctx context.Context) error {
	symbols, err := fetchMarkets(ctx, n.logger)
	if err != nil {
		return err
	}
	if len(symbols) == 0 {
		return fmt.Errorf("no symbols found")
	}

	n.rateLimiter = scraper.DefaultRateLimiter()
	n.logger.Info("fetching candles for symbols", "count", len(symbols))

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
			n.logger.Warn("failed to fetch candle", "symbol", symbol, "error", err)
			continue
		}
	}

	n.logger.Info("candle fetch completed", "symbols", len(symbols))
	return nil
}

func (n *BitpinCandleScraper) fetchOHLC(ctx context.Context, symbol string) error {
	fromTimestamp := scraper.ToMidnight(time.Now().AddDate(0, 0, -3)).Unix()
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

	var data []CandleResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return err
	}

	length := len(data)
	if length == 0 {
		return nil
	}

	cleanedSymbol := scraper.NormalizeSymbol("bitpin", symbol)

	if cleanedSymbol == "USDT/IRT" && length > 0 {
		n.usdtMu.Lock()
		n.usdtPrice = scraper.NormalizePrice(cleanedSymbol, data[length-1].Close)
		n.usdtMu.Unlock()
	}

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
			volumeParsed, _ = strconv.ParseFloat(volume, 64)
		case float64:
			volumeParsed = volume
		}

		candle := &proto.CandleData{
			Id:        scraper.GenerateCandleID("bitpin", cleanedSymbol, "1d", openTime),
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

		if err := n.sender.SendCandle(ctx, candle); err != nil {
			n.logger.Debug("send error", "error", err)
		}
	}

	return nil
}
