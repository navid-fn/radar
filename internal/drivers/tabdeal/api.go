package tabdeal

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"nobitex/radar/internal/proto"
	"nobitex/radar/internal/scraper"

	"github.com/segmentio/kafka-go"
	"golang.org/x/time/rate"
)

const (
	tradesURL  = "https://api1.tabdeal.org/r/api/v1/trades"
	marketsURL = "https://api1.tabdeal.org/r/api/v1/exchangeInfo"
	limit      = 10
)

type TabdealAPI struct {
	sender      *scraper.Sender
	logger      *slog.Logger
	rateLimiter *rate.Limiter
	usdtPrice   float64
	usdtMu      sync.RWMutex
}

func NewTabdealScraper(kafkaWriter *kafka.Writer, logger *slog.Logger) *TabdealAPI {
	return &TabdealAPI{
		sender:    scraper.NewSender(kafkaWriter, logger),
		logger:    logger.With("scraper", "tabdeal"),
		usdtPrice: getLatestUSDTPrice(),
	}
}

func (t *TabdealAPI) Name() string { return "tabdeal" }

func (t *TabdealAPI) Run(ctx context.Context) error {
	t.logger.Info("Starting Tabdeal scraper", "usdtPrice", t.usdtPrice)

	symbols, err := t.fetchMarkets()
	if err != nil {
		return err
	}
	if len(symbols) == 0 {
		return fmt.Errorf("no symbols found")
	}

	optimalRate := min(float64(len(symbols)), 60.0*0.9/60.0)
	t.rateLimiter = rate.NewLimiter(rate.Limit(optimalRate), 5)
	t.logger.Info("Rate limiter configured", "rate", optimalRate, "symbols", len(symbols))

	var wg sync.WaitGroup
	for _, sym := range symbols {
		wg.Add(1)
		go func(symbol string) {
			defer wg.Done()
			t.pollSymbol(ctx, symbol)
		}(sym)
	}
	wg.Wait()
	return nil
}

func (t *TabdealAPI) pollSymbol(ctx context.Context, symbol string) {
	consecErrors := 0

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := t.rateLimiter.Wait(ctx); err != nil {
			return
		}

		err := t.fetchTrades(ctx, symbol)
		if err != nil {
			consecErrors++
			backoff := 5 * time.Second
			if consecErrors >= 5 {
				backoff = 60 * time.Second
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
		} else {
			consecErrors = 0
		}
	}
}

func (t *TabdealAPI) fetchTrades(ctx context.Context, symbol string) error {
	url := fmt.Sprintf("%s?symbol=%s&limit=%d", tradesURL, symbol, limit)

	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 500 || isHTMLResponse(body) {
		return fmt.Errorf("gateway error: %d", resp.StatusCode)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
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
			t.logger.Debug("Send error", "error", err)
		}
	}
	return nil
}

func (t *TabdealAPI) fetchMarkets() ([]string, error) {
	resp, err := http.Get(marketsURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 500 || isHTMLResponse(body) {
		return nil, fmt.Errorf("gateway error")
	}

	var markets []market
	if err := json.Unmarshal(body, &markets); err != nil {
		return nil, err
	}

	var symbols []string
	for _, m := range markets {
		if m.Status == "TRADING" {
			symbols = append(symbols, m.Symbol)
		}
	}
	t.logger.Info("Fetched markets", "count", len(symbols))
	return symbols, nil
}

func isHTMLResponse(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	s := strings.TrimSpace(string(body))
	return strings.HasPrefix(s, "<!") || strings.HasPrefix(s, "<html") || strings.HasPrefix(s, "<HTML")
}
