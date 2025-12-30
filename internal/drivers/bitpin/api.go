package bitpin

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
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
		sender:    scraper.NewSender(kafkaWriter, logger),
		logger:    logger.With("scraper", "bitpin-api"),
		usdtPrice: getLatestUSDTPrice(),
	}
}

func (b *BitpinAPI) Name() string { return "bitpin-api" }

func (b *BitpinAPI) Run(ctx context.Context) error {
	b.logger.Info("Starting Bitpin API scraper")

	symbols, err := fetchMarkets(b.logger)
	if err != nil {
		return err
	}
	if len(symbols) == 0 {
		return fmt.Errorf("no symbols found")
	}

	optimalRate := min(float64(len(symbols)), 60.0*0.98/60.0)
	b.rateLimiter = rate.NewLimiter(rate.Limit(optimalRate), 10)

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

	var trades []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&trades); err != nil {
		return err
	}

	for _, t := range trades {
		id, _ := t["id"].(string)
		side, _ := t["side"].(string)
		priceStr, _ := t["price"].(string)
		volumeStr, _ := t["base_amount"].(string)
		timestamp, _ := t["time"].(float64)

		price, _ := strconv.ParseFloat(priceStr, 64)
		volume, _ := strconv.ParseFloat(volumeStr, 64)

		if volume == 0 && price > 0 {
			quoteAmountStr, _ := t["quote_amount"].(string)
			quoteAmount, err := strconv.ParseFloat(quoteAmountStr, 64)
			if err == nil {
				volume = quoteAmount / price
			}
		}

		sec, dec := math.Modf(timestamp)
		tradeTime := time.Unix(int64(sec), int64(dec*1e9))

		cleanedSymbol := scraper.NormalizeSymbol("bitpin", symbol)

		if cleanedSymbol == "USDT/IRT" {
			b.usdtMu.Lock()
			b.usdtPrice = price
			b.usdtMu.Unlock()
		}

		trade := &proto.TradeData{
			Id:        id,
			Exchange:  "bitpin",
			Symbol:    cleanedSymbol,
			Price:     price,
			Volume:    volume,
			Quantity:  volume * price,
			Side:      side,
			Time:      tradeTime.Format(time.RFC3339),
			UsdtPrice: b.usdtPrice,
		}

		if err := b.sender.SendTrade(ctx, trade); err != nil {
			b.logger.Debug("Send error", "error", err)
		}
	}
	return nil
}
