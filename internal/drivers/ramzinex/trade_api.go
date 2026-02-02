package ramzinex

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"nobitex/radar/internal/proto"
	"nobitex/radar/internal/scraper"

	"github.com/segmentio/kafka-go"
	"golang.org/x/time/rate"
)

type RamzinexAPI struct {
	sender       *scraper.Sender
	logger       *slog.Logger
	pairIDToName map[int]string
	rateLimiter  *rate.Limiter
	usdtPrice    float64
	usdtMu       sync.RWMutex
}

func NewRamzinexAPIScraper(kafkaWriter *kafka.Writer, logger *slog.Logger) *RamzinexAPI {
	return &RamzinexAPI{
		sender:       scraper.NewSender(kafkaWriter, logger),
		logger:       logger.With("scraper", "ramzinex-api"),
		pairIDToName: make(map[int]string),
	}
}

func (r *RamzinexAPI) Name() string { return "ramzinex-api" }

func (r *RamzinexAPI) Run(ctx context.Context) error {
	r.usdtPrice = float64(getLatestUSDTPrice())
	r.logger.Info("starting Ramzinex API scraper")

	pairs, pairMap, err := fetchPairs()
	if err != nil {
		return err
	}
	r.pairIDToName = pairMap

	if len(pairs) == 0 {
		return fmt.Errorf("no pairs found")
	}

	r.rateLimiter = scraper.DefaultRateLimiter()

	var wg sync.WaitGroup
	for _, p := range pairs {
		wg.Add(1)
		go func(pair pairDetail) {
			defer wg.Done()
			r.pollPair(ctx, pair.ID)
		}(p)
	}
	wg.Wait()
	return nil
}

func (r *RamzinexAPI) pollPair(ctx context.Context, pairID int) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := r.rateLimiter.Wait(ctx); err != nil {
			return
		}

		if err := r.fetchTrades(ctx, pairID); err != nil {
			time.Sleep(2 * time.Second)
		}
	}
}

func (r *RamzinexAPI) fetchTrades(ctx context.Context, pairID int) error {
	url := fmt.Sprintf(tradesAPI, pairID)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	resp, err := scraper.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var data latestAPIData
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return err
	}

	pairName := r.pairIDToName[pairID]
	for _, item := range data.Data {
		row, ok := item.([]any)
		if !ok || len(row) < 6 {
			continue
		}

		cleanedSymbol := scraper.NormalizeSymbol("ramzinex", strings.ToUpper(pairName))
		cleanedPrice := scraper.NormalizePrice(cleanedSymbol, row[0].(float64))
		volume := row[1].(float64)
		tradeTime := scraper.FloatTimestampToRFC3339(row[4].(float64))
		side := row[3].(string)
		id := row[5].(string)

		if cleanedSymbol == "USDT/IRT" {
			r.usdtMu.Lock()
			r.usdtPrice = cleanedPrice
			r.usdtMu.Unlock()
		}

		trade := &proto.TradeData{
			Id:        id,
			Exchange:  "ramzinex",
			Symbol:    cleanedSymbol,
			Price:     cleanedPrice,
			Volume:    volume,
			Quantity:  cleanedPrice * volume,
			Side:      side,
			Time:      tradeTime,
			UsdtPrice: r.usdtPrice,
		}

		if err := r.sender.SendTrade(ctx, trade); err != nil {
			// TODO: add metric
			r.logger.Debug("send error", "error", err)
		}
	}
	return nil
}
