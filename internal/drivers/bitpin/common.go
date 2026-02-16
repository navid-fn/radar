package bitpin

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"

	"nobitex/radar/internal/scraper"
)

const (
	// URL for apis
	baseURL    = "https://api.bitpin.ir"
	marketsAPI = baseURL + "/api/v1/mkt/markets/"
	tickersAPI = baseURL + "/api/v1/mkt/tickers/"
	tradesAPI  = baseURL + "/api/v1/mth/matches/%s/"

	// OHLC wasnt in the doc
	ohlcAPI = baseURL + "/v1/mkt/tv/get_bars/?symbol=%s&from=%d&to=%d&res=1D"

	// URL for ws
	wsURL             = "wss://centrifugo.bitpin.ir/connection/websocket"
	maxSymbolsPerConn = 100
)

type market struct {
	Symbol    string `json:"symbol"`
	Tradeable bool   `json:"tradable"`
}

type ticker struct {
	Symbol string `json:"symbol"`
	Price  string `json:"price"`
}

type tradeMatch struct {
	ID          string  `json:"id"`
	Price       string  `json:"price"`
	BaseAmount  string  `json:"base_amount"`
	QuoteAmount string  `json:"quote_amount"`
	Side        string  `json:"side"`
	Time        float64 `json:"time"`
	Symbol      string  `json:"symbol"`
}

type depthResponse struct {
	EventTime string     `json:"event_time"`
	Bids      [][]string `json:"bids"`
	Asks      [][]string `json:"asks"`
	Symbol    string     `json:"symbol"`
}

func fetchMarkets(ctx context.Context, logger *slog.Logger) ([]string, error) {
	select {
	case <-ctx.Done():
		return nil, nil
	default:
		var resp *http.Response
		var err error
		maxRetry := 3
		for maxRetry > 0 {
			resp, err = scraper.HTTPClient.Get(marketsAPI)
			if err != nil {
				maxRetry -= 1
			} else {
				defer resp.Body.Close()
				break
			}
		}
		if err != nil {
			return nil, err
		}
		body, _ := io.ReadAll(resp.Body)
		var markets []market
		if err := json.Unmarshal(body, &markets); err != nil {
			return nil, err
		}

		var symbols []string
		for _, m := range markets {
			if m.Tradeable {
				symbols = append(symbols, m.Symbol)
			}
		}
		sort.Strings(symbols)
		logger.Info("Fetched markets", "count", len(symbols))
		return symbols, nil
	}
}

func getLatestUSDTPrice() float64 {
	resp, err := scraper.HTTPClient.Get(tickersAPI)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()

	var tickersData []ticker
	if err := json.NewDecoder(resp.Body).Decode(&tickersData); err != nil {
		return 0
	}
	for _, t := range tickersData {
		if t.Symbol == "USDT_IRT" {
			usdtPriceFloat, _ := strconv.ParseFloat(t.Price, 64)
			return usdtPriceFloat
		}
	}
	return 0
}
