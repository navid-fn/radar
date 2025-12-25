package nobitex

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	LatestTradeAPI = "https://apiv2.nobitex.ir/v2/trades/"
	MarketAPI      = "https://apiv2.nobitex.ir/market/stats"
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

func (t tradeData) MarshalJSON() ([]byte, error) {
	type AliasTradesInfo tradeData
	return json.Marshal(&struct {
		AliasTradesInfo
		Time time.Time `json:"time"`
	}{
		AliasTradesInfo: AliasTradesInfo(t),
		Time:            time.UnixMilli(t.Time),
	})
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

func cleanSymbol(s string) string {
	if strings.HasSuffix(s, "IRT") {
		s = strings.TrimSuffix(s, "IRT")
		return s + "/IRT"
	} else {
		s = strings.TrimSuffix(s, "USDT")
		return s + "/USDT"
	}
}

func cleanPrice(s string, p float64) float64 {
	if strings.HasSuffix(s, "IRT") {
		return p / 10
	}
	return p
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
	response, err := http.Get("https://apiv2.nobitex.ir/v3/orderbook/USDTIRT")
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
