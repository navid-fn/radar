// This file implements commission data scraping.
// https://docs.bitpin.ir/v1/docs/market-data/commissions
// Response format:
// [
// {"market":5, "symbol":"USDT_IRT", "maker":0.003, "taker":0.0035}, .... 
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
	"time"

	"github.com/segmentio/kafka-go"
)

type CommissionResponse struct {
	Symbol string  `json:"symbol"`
	Taker  float64 `json:"taker"`
	Maker  float64 `json:"maker"`
}

// BitpinCommission scrapes OHLC data from Nobitex API.
// Runs daily at 4:30 AM Tehran time, fetches data, then waits for next day.
type BitpinCommission struct {
	sender *scraper.Sender
	logger *slog.Logger
}

// NewBitpinCommissionScraper creates a new Nobitex OHLC scraper.
func NewBitpinCommissonScraper(kafkaWriter *kafka.Writer, logger *slog.Logger) *BitpinCommission {
	return &BitpinCommission{
		sender: scraper.NewSender(kafkaWriter, logger),
		logger: logger.With("scraper", "bipin-commision"),
	}
}

func (n *BitpinCommission) Name() string { return "bipin-commission" }

// Run starts the OHLC scraper with a daily schedule at 4:30 AM Tehran time.
// It waits until 4:30 AM, fetches all symbols, then waits for next day's 4:30 AM.
func (n *BitpinCommission) Run(ctx context.Context) error {
	tehran, err := time.LoadLocation("Asia/Tehran")
	if err != nil {
		return fmt.Errorf("failed to load Tehran timezone: %w", err)
	}

	n.logger.Info("starting Bitpin OHLC scraper (scheduled daily at 4:30 AM Tehran)")

	n.logger.Info("executing initial startup fetch...")
	if err := n.fetchAllCommissions(ctx); err != nil {
		// Log error but don't crash; let the schedule continue
		n.logger.Error("initial commission fetch failed", "error", err)
	}

	for {
		// Calculate next 4:30 AM Tehran
		now := time.Now().In(tehran)
		next := time.Date(now.Year(), now.Month(), now.Day(), 4, 30, 0, 0, tehran)
		if next.Before(now) {
			next = next.Add(24 * time.Hour)
		}

		n.logger.Info("next commission fetch scheduled", "at", next.Format(time.RFC3339), "in", time.Until(next).Round(time.Minute))

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Until(next)):
			n.logger.Info("starting daily commission fetch")
			if err := n.fetchAllCommissions(ctx); err != nil {
				n.logger.Error("commission fetch failed", "error", err)
			}
		}
	}
}

// fetchAllCommissions fetches OHLC data for all available symbols.
func (n *BitpinCommission) fetchAllCommissions(ctx context.Context) error {

	n.logger.Info("fetching commission for symbols")

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err := n.fetchCommissions(ctx); err != nil {
		n.logger.Warn("failed to fetch commissions", "error", err)
	}

	n.logger.Info("commissions fetch completed")
	return nil
}

// fetchCommissions fetches commissions fee all symbols
// Retries up to 3 times on timeout/connection errors with 2 second delay.
func (n *BitpinCommission) fetchCommissions(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, "GET", commisionAPI, nil)
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

	var data []CommissionResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return err
	}

	// Validate parallel arrays have same length
	length := len(data)
	if length == 0 {
		return nil // No data
	}

	commissions := []*proto.CommissionData{}

	// Convert each candle to proto and send
	for _, d := range data {
		cleanedSymbol := scraper.NormalizeSymbol("bitpin", d.Symbol)

		commision := &proto.CommissionData{
			Taker: d.Taker,
			Maker: d.Maker,
			Symbol: cleanedSymbol,
			Exchange: "bitpin",

		}
		commissions = append(commissions, commision)
	}

	if err := n.sender.SendCommissions(ctx, commissions); err != nil {
		n.logger.Debug("Send error", "error", err)
	}

	return nil
}
