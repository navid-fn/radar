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

type WallexAPI struct {
	sender      *scraper.Sender
	logger      *slog.Logger
	rateLimiter *rate.Limiter
	usdtPrice   float64
	usdtMu      sync.RWMutex
}

func NewWallexAPIScraper(kafkaWriter *kafka.Writer, logger *slog.Logger) *WallexAPI {
	return &WallexAPI{
		sender: scraper.NewSender(kafkaWriter, logger),
		logger: logger.With("scraper", "wallex-api"),
	}
}

func (w *WallexAPI) Name() string { return "wallex-api" }

func (w *WallexAPI) Run(ctx context.Context) error {
	w.usdtPrice = getLatestUSDTPrice()
	w.logger.Info("Starting Wallex API scraper")

	symbols, err := fetchMarkets()
	if err != nil {
		return err
	}
	if len(symbols) == 0 {
		return fmt.Errorf("no symbols found")
	}

	w.rateLimiter = scraper.DefaultRateLimiter()
	w.logger.Info("rate limiter configured", "symbols", len(symbols))

	var wg sync.WaitGroup
	for _, sym := range symbols {
		wg.Add(1)
		go func(symbol string) {
			defer wg.Done()
			w.pollSymbol(ctx, symbol)
		}(sym)
	}
	wg.Wait()
	return nil
}

func (w *WallexAPI) pollSymbol(ctx context.Context, symbol string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := w.rateLimiter.Wait(ctx); err != nil {
			return
		}

		if err := w.fetchTrades(ctx, symbol); err != nil {
			// TODO: add metric
			w.logger.Debug("fetch error", "symbol", symbol, "error", err)
			time.Sleep(2 * time.Second)
		}
	}
}

func (w *WallexAPI) fetchTrades(ctx context.Context, symbol string) error {
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf(tradesAPI, symbol), nil)
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

	if !data.Status {
		return nil
	}

	for _, t := range data.Result.Trades {
		volume, _ := strconv.ParseFloat(t.Volume, 64)
		price, _ := strconv.ParseFloat(t.Price, 64)

		side := "sell"
		if t.Side {
			side = "buy"
		}

		cleanedSymbol := scraper.NormalizeSymbol("wallex", symbol)
		if cleanedSymbol == "USDT/IRT" {
			w.usdtMu.Lock()
			w.usdtPrice = price
			w.usdtMu.Unlock()
		}

		trade := &proto.TradeData{
			Id:        scraper.GenerateTradeID("wallex", cleanedSymbol, t.Time, price, volume, side),
			Exchange:  "wallex",
			Symbol:    cleanedSymbol,
			Price:     price,
			Volume:    volume,
			Quantity:  volume * price,
			Side:      side,
			Time:      t.Time,
			UsdtPrice: w.usdtPrice,
		}

		if err := w.sender.SendTrade(ctx, trade); err != nil {
			w.logger.Debug("Send error", "error", err)
		}
	}
	return nil
}
