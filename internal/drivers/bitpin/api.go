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

	"github.com/segmentio/kafka-go"
	"golang.org/x/time/rate"
)

const tradesAPI = "https://api.bitpin.org/api/v1/mth/matches/%s/"

type BitpinAPI struct {
	sender      *scraper.Sender
	logger      *slog.Logger
	rateLimiter *rate.Limiter
	usdtPrice   float64
	usdtMu      sync.RWMutex
}

func NewBitpinAPIScraper(kafkaWriter *kafka.Writer, logger *slog.Logger) *BitpinAPI {
	return &BitpinAPI{
		sender: scraper.NewSender(kafkaWriter, logger),
		logger: logger.With("scraper", "bitpin-api"),
	}
}

func (b *BitpinAPI) Name() string { return "bitpin-api" }

func (b *BitpinAPI) Run(ctx context.Context) error {
	b.logger.Info("Starting Bitpin API scraper")
	b.usdtPrice = getLatestUSDTPrice()

	symbols, err := fetchMarkets(b.logger)
	if err != nil {
		return err
	}
	if len(symbols) == 0 {
		return fmt.Errorf("no symbols found")
	}

	b.rateLimiter = scraper.DefaultRateLimiter()

	var wg sync.WaitGroup
	for _, sym := range symbols {
		wg.Add(1)
		go func(symbol string) {
			defer wg.Done()
			b.pollSymbol(ctx, symbol)
		}(sym)
	}
	wg.Wait()
	return nil
}

func (b *BitpinAPI) pollSymbol(ctx context.Context, symbol string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := b.rateLimiter.Wait(ctx); err != nil {
			return
		}

		if err := b.fetchTrades(ctx, symbol); err != nil {
			time.Sleep(2 * time.Second)
		}
	}
}

func (b *BitpinAPI) fetchTrades(ctx context.Context, symbol string) error {
	url := fmt.Sprintf(tradesAPI, symbol)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
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

	var tradeMatches []tradeMatch
	if err := json.NewDecoder(resp.Body).Decode(&tradeMatches); err != nil {
		return err
	}

	for _, m := range tradeMatches {
		tradeTime := scraper.FloatTimestampToRFC3339(m.Time)
		price, _ := strconv.ParseFloat(m.Price, 64)
		volume, _ := strconv.ParseFloat(m.BaseAmount, 64)

		if volume == 0 && price > 0 {
			// check if quote_amount has value
			// sometimes the base_amount return 0.00
			quoteAmountStr := m.QuoteAmount
			quoteAmount, err := strconv.ParseFloat(quoteAmountStr, 64)
			if err == nil {
				volume = quoteAmount / price
			}
		}
		cleanedSymbol := scraper.NormalizeSymbol("bitpin", symbol)

		if cleanedSymbol == "USDT/IRT" {
			b.usdtMu.Lock()
			b.usdtPrice = price
			b.usdtMu.Unlock()
		}

		trade := &proto.TradeData{
			Id:        m.ID,
			Exchange:  "bitpin",
			Symbol:    cleanedSymbol,
			Price:     price,
			Volume:    volume,
			Quantity:  volume * price,
			Side:      m.Side,
			Time:      tradeTime,
			UsdtPrice: b.usdtPrice,
		}
		if err := b.sender.SendTrade(ctx, trade); err != nil {
			b.logger.Debug("Send error", "error", err)
		}
	}

	return nil
}
