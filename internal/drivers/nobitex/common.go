package nobitex

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	latestTradeAPI = "https://apiv2.nobitex.ir/v2/trades/"
	marketAPI      = "https://apiv2.nobitex.ir/market/stats"
	usdtPriceAPI   = "https://apiv2.nobitex.ir/v3/orderbook/USDTIRT"
)

type usdtPrice struct {
	USDTPrice string `json:"lastTradePrice"`
}

type tradeData struct {
	Symbol   string `json:"symbol"`
	Exchange string `json:"exchange"`
	Time     int64  `json:"time"`
	Price    string `json:"price"`
	Volume   string `json:"volume"`
	Side     string `json:"type"`
}

type tradeAPIResponse struct {
	Status string      `json:"status"`
	Trades []tradeData `json:"trades"`
}

type symbolData struct {
	IsClosed bool `json:"isClosed"`
}

type marketDataAPIResponse struct {
	Status string
	Stats  map[string]symbolData
}


// fetch tradeable markets to scrape
func fetchMarkets(logger *slog.Logger) ([]string, error) {
	resp, err := http.Get(marketAPI)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var data marketDataAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}

	var symbols []string
	for pair, stats := range data.Stats {
		if !stats.IsClosed {
			symbols = append(symbols, transformPair(pair))
		}
	}
	sort.Strings(symbols)
	logger.Info("Fetched markets", "count", len(symbols))
	return symbols, nil
}

// transform pair to use it in getting latest trade
// example: btc-rls -> BTCIRT
func transformPair(pair string) string {
	parts := strings.Split(pair, "-")
	for i := range parts {
		parts[i] = strings.ToUpper(parts[i])
	}
	if len(parts) > 1 && parts[len(parts)-1] == "RLS" {
		parts[len(parts)-1] = "IRT"
	}
	return strings.Join(parts, "")
}

func getStringValue(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		switch val := v.(type) {
		case string:
			return val
		case float64:
			return strconv.FormatFloat(val, 'f', -1, 64)
		case int64:
			return strconv.FormatInt(val, 10)
		}
	}
	return ""
}

func getTimeValue(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		switch val := v.(type) {
		case string:
			return val
		case float64:
			t := time.UnixMilli(int64(val))
			return t.Format(time.RFC3339)
		case int64:
			t := time.UnixMilli(val)
			return t.Format(time.RFC3339)
		}
	}
	return time.Now().Format(time.RFC3339)
}

func getLatestUSDTPrice() float64 {
	response, err := http.Get(usdtPriceAPI)
	if err != nil {
		return 0
	}
	defer response.Body.Close()
	var price usdtPrice
	if err := json.NewDecoder(response.Body).Decode(&price); err != nil {
		return 0
	}
	usdtPriceFloat, _ := strconv.ParseFloat(price.USDTPrice, 64)
	return usdtPriceFloat / 10

}

