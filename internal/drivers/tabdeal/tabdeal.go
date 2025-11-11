package tabdeal

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"nobitex/radar/internal/crawler"
	"nobitex/radar/utils"
)

const (
	TabdealTradesURL = "https://api1.tabdeal.org/r/api/v1/trades"
	TabdealMarketURL = "https://api1.tabdeal.org/r/api/v1/exchangeInfo"
	TradesLimit      = 2
	MinSleepTime     = 1 * time.Second
	MaxSleepTime     = 30 * time.Second
	SleepIncrement   = 1 * time.Second
	SleepDecrement   = 1 * time.Second
)

type Market struct {
	Symbol        string `json:"symbol"`
	Status        string `json:"status"`
	TabdealSymbol string `json:"tabdealSymbol"`
}

type TradesInfo struct {
	ID       int    `json:"id"`
	Price    string `json:"price"`
	Qty      string `json:"qty"`
	QuoteQty string `json:"quoteqty"`
	Time     int64  `json:"time"`
	Buyer    bool   `json:"isBuyerMaker"`
}

type TabdealCrawler struct {
	*crawler.BaseCrawler
	idTracker *crawler.IDTracker
}

func NewTabdealCrawler() *TabdealCrawler {
	config := crawler.NewConfig("tabdeal", 0)
	baseCrawler := crawler.NewBaseCrawler(config)

	return &TabdealCrawler{
		BaseCrawler: baseCrawler,
		idTracker:   crawler.NewIDTracker(MinSleepTime, MaxSleepTime, SleepIncrement),
	}
}

func (tc *TabdealCrawler) GetName() string {
	return "tabdeal"
}

func (tc *TabdealCrawler) FetchMarkets() ([]string, error) {
	resp, err := http.Get(TabdealMarketURL)
	if err != nil {
		return nil, fmt.Errorf("error fetching markets: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %w", err)
	}

	var apiResponse []Market
	if err = json.Unmarshal(body, &apiResponse); err != nil {
		return nil, fmt.Errorf("error unmarshaling API response: %w", err)
	}

	var symbols []string
	for _, r := range apiResponse {
		if r.Status == "TRADING" {
			symbols = append(symbols, r.Symbol)
		}
	}

	tc.Logger.Info("Fetched active trading symbols", "count", len(symbols))
	return symbols, nil
}

func (tc *TabdealCrawler) fetchTrades(ctx context.Context, symbol string) error {
	url := fmt.Sprintf("%s?symbol=%s&limit=%d", TabdealTradesURL, symbol, TradesLimit)

	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("error calling API: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error reading response body: %w", err)
	}

	var trades []TradesInfo
	if err = json.Unmarshal(body, &trades); err != nil {
		return fmt.Errorf("error unmarshaling API response: %w", err)
	}

	lastSeenID := tc.idTracker.GetLastSeenID(symbol)
	newTradesCount := 0

	for _, t := range trades {
		if t.ID <= lastSeenID {
			continue
		}

		volume, _ := strconv.ParseFloat(t.Qty, 64)
		price, _ := strconv.ParseFloat(t.Price, 64)
		quantity, _ := strconv.ParseFloat(t.QuoteQty, 64)
		trade_id := strconv.FormatInt(int64(t.ID), 10)
		side := "sell"
		if t.Buyer {
			side = "buy"
		}

		tradeTime := utils.TurnTimeStampToTime(t.Time, false)

		data := crawler.KafkaData{
			Exchange: "tabdeal",
			ID:       trade_id,
			Side:     side,
			Volume:   volume,
			Symbol:   symbol,
			Price:    price,
			Quantity: quantity,
			Time:     tradeTime.Format(time.RFC3339),
		}

		jsonData, err := json.Marshal(data)
		if err != nil {
			tc.Logger.Error("Error marshaling trade", "error", err)
			continue
		}

		if err := tc.SendToKafka(jsonData); err != nil {
			tc.Logger.Error("Failed to send to Kafka", "error", err)
			continue
		}

		tc.idTracker.UpdateLastSeenID(symbol, t.ID)
		newTradesCount++
	}

	if newTradesCount > 0 {
		tc.idTracker.DecreaseSleep(symbol, SleepDecrement, MinSleepTime, MinSleepTime)
	} else {
		tc.idTracker.IncreaseSleep(symbol, SleepIncrement, MaxSleepTime, MinSleepTime)
	}

	time.Sleep(tc.idTracker.GetSleepDuration(symbol, MinSleepTime))

	return nil
}

func (tc *TabdealCrawler) Run(ctx context.Context) error {
	tc.Logger.Info("Starting Tabdeal Crawler...")

	if err := tc.InitKafkaProducer(); err != nil {
		return fmt.Errorf("failed to initialize Kafka producer: %w", err)
	}
	defer tc.CloseKafkaProducer()

	symbols, err := tc.FetchMarkets()
	if err != nil {
		return fmt.Errorf("could not fetch markets: %w", err)
	}

	if len(symbols) == 0 {
		return fmt.Errorf("no symbols found to track")
	}

	return crawler.RunWithGracefulShutdown(tc.Logger, func(ctx context.Context, wg *sync.WaitGroup) {
		for _, symbol := range symbols {
			wg.Add(1)
			go func(sym string) {
				defer wg.Done()
				tc.Logger.Info("Starting trade fetcher for symbol", "symbol", sym)

				for {
					select {
					case <-ctx.Done():
						tc.Logger.Info("Stopping trade fetcher for symbol", "symbol", sym)
						return
					default:
						if err := tc.fetchTrades(ctx, sym); err != nil {
							tc.Logger.Error("Error fetching trades", "symbol", sym, "error", err)
							time.Sleep(2 * time.Second)
						}
					}
				}
			}(symbol)
		}
	})
}
