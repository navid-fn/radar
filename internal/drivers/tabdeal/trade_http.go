package tabdeal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"nobitex/radar/internal/proto"
	"nobitex/radar/internal/scraper"
)

const tabdealRequestsPerMinute = 60

type httpStatusError struct {
	statusCode int
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("status %d", e.statusCode)
}

type TabdealHttpScraper struct {
	sender    *scraper.Sender
	logger    *slog.Logger
	usdtPrice float64
	usdtMu    sync.RWMutex
}

func NewTabdealHttpScraper(writer scraper.MessageWriter, logger *slog.Logger) *TabdealHttpScraper {
	return &TabdealHttpScraper{
		sender: scraper.NewSender(writer, logger),
		logger: logger.With("scraper", "tabdeal"),
	}
}

func (t *TabdealHttpScraper) Name() string { return "tabdeal" }

func (t *TabdealHttpScraper) Run(ctx context.Context) error {
	t.usdtPrice = getLatestUSDTPrice()
	symbols, err := fetchMarkets()
	if err != nil {
		return err
	}
	if len(symbols) == 0 {
		return fmt.Errorf("no symbols found")
	}
	t.logger.Info("starting Tabdeal scraper", "usdtPrice", t.usdtPrice, "symbolsCount", len(symbols))

	t.pollSymbolsRoundRobin(ctx, symbols)
	return nil
}

func (t *TabdealHttpScraper) pollSymbolsRoundRobin(ctx context.Context, symbols []string) {
	limiter := scraper.NewRateLimiter(tabdealRequestsPerMinute)
	symbolIdx := 0

	for {
		if len(symbols) == 0 {
			t.logger.Error("stopping tabdeal poller: no symbols left")
			return
		}

		if err := limiter.Wait(ctx); err != nil {
			return
		}

		symbol := symbols[symbolIdx]
		if err := t.fetchTrades(ctx, symbol); err != nil {
			var statusErr *httpStatusError
			if errors.As(err, &statusErr) && statusErr.statusCode != http.StatusTooManyRequests {
				t.logger.Warn(
					"removing symbol from polling due to non-429 status",
					"symbol", symbol,
					"statusCode", statusErr.statusCode,
					"error", err,
				)
				symbols = append(symbols[:symbolIdx], symbols[symbolIdx+1:]...)
				if symbolIdx >= len(symbols) {
					symbolIdx = 0
				}
				continue
			}

			t.logger.Warn("fetch trades failed", "symbol", symbol, "error", err)
		}

		symbolIdx++
		if symbolIdx >= len(symbols) {
			symbolIdx = 0
		}
	}
}

func (t *TabdealHttpScraper) fetchTrades(ctx context.Context, symbol string) error {
	url := fmt.Sprintf("%s?symbol=%s&limit=%d", tradesURL, symbol, limit)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	resp, err := scraper.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode == http.StatusTooManyRequests {
		return &httpStatusError{statusCode: resp.StatusCode}
	}

	if resp.StatusCode != http.StatusOK {
		return &httpStatusError{statusCode: resp.StatusCode}
	}

	if isHTMLResponse(body) {
		return fmt.Errorf("unexpected html response body")
	}

	var trades []tradesInfo
	if err := json.Unmarshal(body, &trades); err != nil {
		return err
	}

	for _, tr := range trades {
		volume, _ := strconv.ParseFloat(tr.Qty, 64)
		price, _ := strconv.ParseFloat(tr.Price, 64)
		tradeID := strconv.FormatInt(int64(tr.ID), 10)
		tradeTime := scraper.TimestampToRFC3339(tr.Time)

		side := "sell"
		if tr.Buyer {
			side = "buy"
		}

		cleanedSymbol := scraper.NormalizeSymbol("tabdeal", symbol)
		if cleanedSymbol == "USDT/IRT" {
			t.usdtMu.Lock()
			t.usdtPrice = price
			t.usdtMu.Unlock()
		}

		trade := &proto.TradeData{
			Id:        tradeID,
			Exchange:  "tabdeal",
			Symbol:    cleanedSymbol,
			Price:     price,
			Volume:    volume,
			Quantity:  volume * price,
			Side:      side,
			Time:      tradeTime,
			UsdtPrice: t.usdtPrice,
		}

		if err := t.sender.SendTrade(ctx, trade); err != nil {
			// TODO: add metric
			t.logger.Debug("send error", "error", err)
		}
	}
	return nil
}

func isHTMLResponse(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	s := strings.TrimSpace(string(body))
	return strings.HasPrefix(s, "<!") || strings.HasPrefix(s, "<html") || strings.HasPrefix(s, "<HTML")
}
