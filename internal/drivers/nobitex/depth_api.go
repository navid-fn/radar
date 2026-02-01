// NOTE: we are not using this module for now, but we may use it later
//	{
//	  "asks": [
//	    ["35077909990", "0.009433"],
//	    ["35078000000", "0.000274"],
//	    ["35078009660", "0.00057"]
//	  ],
//	  "bids": [
//	    ["35020080080", "0.185784"],
//	    ["35020070060", "0.086916"],
//	    ["35020030010", "0.000071"]
//	  ],
//	  "lastTradePrice": "35077909990",
//	  "lastUpdate": 1726581829816
//	}

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

type NobitexDepthAPI struct {
	sender      *scraper.Sender
	logger      *slog.Logger
	rateLimiter *rate.Limiter
	usdtPrice   float64
	usdtMu      sync.RWMutex
}

func NewNobitexDepthAPIScraper(kafkaWriter *kafka.Writer, logger *slog.Logger) *NobitexDepthAPI {
	return &NobitexDepthAPI{
		sender:    scraper.NewSender(kafkaWriter, logger),
		logger:    logger.With("scraper", "nobitex-depth-api"),
		usdtPrice: getLatestUSDTPrice(),
	}
}

func (n *NobitexDepthAPI) Name() string { return "nobitex-depth-api" }

func (n *NobitexDepthAPI) Run(ctx context.Context) error {
	n.logger.Info("starting nobitex depth API scraper")

	symbols, err := fetchMarkets()
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

func (n *NobitexDepthAPI) pollSymbol(ctx context.Context, symbol string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := n.rateLimiter.Wait(ctx); err != nil {
			return
		}

		if err := n.fetchDepths(ctx, symbol); err != nil {
			n.logger.Debug("Fetch error", "symbol", symbol, "error", err)
			time.Sleep(2 * time.Second)
		}
	}
}

func (n *NobitexDepthAPI) fetchDepths(ctx context.Context, symbol string) error {
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf(depthAPI, symbol), nil)
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

	var data depthResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return err
	}
	cleanSymbol := scraper.NormalizeSymbol("nobitex", symbol)

	lastUpdateInt, _ := strconv.ParseInt(data.LastUpdate.(string), 10, 64)
	lastUpdate := scraper.TimestampToRFC3339(lastUpdateInt)

	asks := []*proto.OrderLevel{}
	for _, a := range data.Asks {
		price, _ := strconv.ParseFloat(a[0], 64)
		volume, _ := strconv.ParseFloat(a[0], 64)
		asks = append(asks, &proto.OrderLevel{Price: price, Volume: volume})
	}

	bids := []*proto.OrderLevel{}
	for _, b := range data.Bids {
		price, _ := strconv.ParseFloat(b[0], 64)
		volume, _ := strconv.ParseFloat(b[0], 64)
		bids = append(asks, &proto.OrderLevel{Price: price, Volume: volume})
	}

	orderBookSnapShot := &proto.OrderBookSnapshot{
		Id:         scraper.GenerateSnapShotID("nobitex", cleanSymbol, lastUpdate),
		Symbol:     cleanSymbol,
		Exchange:   "nobitex",
		LastUpdate: lastUpdate,
		Asks:       asks,
		Bids:       bids,
	}

	if err := n.sender.SendOrderBookSnapShot(ctx, orderBookSnapShot); err != nil {
		// TODO: add metric
		n.logger.Debug("send error", "error", err)
	}

	return nil
}
