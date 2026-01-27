package nobitex

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

const latestTradeAPI = "https://apiv2.nobitex.ir/v2/trades/"

type NobitexAPI struct {
	sender      *scraper.Sender
	logger      *slog.Logger
	rateLimiter *rate.Limiter
	usdtPrice   float64
	usdtMu      sync.RWMutex
}

func NewNobitexAPIScraper(kafkaWriter *kafka.Writer, logger *slog.Logger) *NobitexAPI {
	return &NobitexAPI{
		sender:    scraper.NewSender(kafkaWriter, logger),
		logger:    logger.With("scraper", "nobitex-api"),
	}
}

func (n *NobitexAPI) Name() string { return "nobitex-api" }

func (n *NobitexAPI) Run(ctx context.Context) error {
	n.usdtPrice = getLatestUSDTPrice()
	n.logger.Info("Starting Nobitex API scraper")

	symbols, err := fetchMarkets(n.logger)
	if err != nil {
		return err
	}
	if len(symbols) == 0 {
		return fmt.Errorf("no symbols found")
	}

	n.rateLimiter = scraper.DefaultRateLimiter()
	n.logger.Info("Rate limiter configured", "symbols", len(symbols))

	var wg sync.WaitGroup
	for _, sym := range symbols {
		wg.Add(1)
		go func(symbol string) {
			defer wg.Done()
			n.pollSymbol(ctx, symbol)
		}(sym)
	}
	wg.Wait()
	return nil
}

func (n *NobitexAPI) pollSymbol(ctx context.Context, symbol string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := n.rateLimiter.Wait(ctx); err != nil {
			return
		}

		if err := n.fetchTrades(ctx, symbol); err != nil {
			n.logger.Debug("Fetch error", "symbol", symbol, "error", err)
			time.Sleep(2 * time.Second)
		}
	}
}

func (n *NobitexAPI) fetchTrades(ctx context.Context, symbol string) error {
	req, err := http.NewRequestWithContext(ctx, "GET", latestTradeAPI+symbol, nil)
	if err != nil {
		return err
	}

	resp, err := scraper.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}

	var data tradeAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return err
	}

	for _, t := range data.Trades {
		price, _ := strconv.ParseFloat(t.Price, 64)
		volume, _ := strconv.ParseFloat(t.Volume, 64)
		if price <= 0 || volume <= 0 {
			continue
		}

		cleanedSymbol := scraper.NormalizeSymbol("nobitex", symbol)
		cleanedPrice := scraper.NormalizePrice(cleanedSymbol, price)
		tradeTime := scraper.TimestampToRFC3339(t.Time)

		if cleanedSymbol == "USDT/IRT" {
			n.usdtMu.Lock()
			n.usdtPrice = cleanedPrice
			n.usdtMu.Unlock()
		}

		trade := &proto.TradeData{
			Id:        scraper.GenerateTradeID("nobitex", cleanedSymbol, tradeTime, cleanedPrice, volume, t.Side),
			Exchange:  "nobitex",
			Symbol:    cleanedSymbol,
			Price:     cleanedPrice,
			Volume:    volume,
			Quantity:  volume * cleanedPrice,
			Side:      t.Side,
			Time:      tradeTime,
			UsdtPrice: n.usdtPrice,
		}

		if err := n.sender.SendTrade(ctx, trade); err != nil {
			n.logger.Debug("Send error", "error", err)
		}
	}
	return nil
}
