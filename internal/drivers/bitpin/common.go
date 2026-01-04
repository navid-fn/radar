package bitpin

import (
	"encoding/json"
	"io"
	"log/slog"
	"sort"
	"strconv"

	"nobitex/radar/internal/scraper"
)

const (
	marketsAPI = "https://api.bitpin.ir/api/v1/mkt/markets/"
	tickersAPI = "https://api.bitpin.ir/api/v1/mkt/tickers/"
	ohlcAPI    = "https://api.bitpin.ir/v1/mkt/tv/get_bars/?symbol=%s&from=%d&to=%d&res=1D"
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
}

func fetchMarkets(logger *slog.Logger) ([]string, error) {
	resp, err := scraper.HTTPClient.Get(marketsAPI)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

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
