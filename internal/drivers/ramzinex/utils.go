package ramzinex

import (
	"encoding/json"
	"net/http"
	"strings"
)

type APIError struct{}

func (e *APIError) Error() string {
	return "Failed to call api with status failed"
}

type pairName struct {
	En string `json:"ramzinex"`
}

type pairData struct {
	ID   int
	Name pairName `json:"trading_chart_settings"`
}

type pairDetail struct {
	Name string
	ID   int
}

type apiPairResponse struct {
	Status int `json:"status"`
	Data   struct {
		Pairs []pairData `json:"pairs"`
	} `json:"data"`
}

type latestAPIData struct {
	Data   []any `json:"data"`
	Status int   `json:"status"`
}

func cleanSymbol(s string) string {
	if strings.HasSuffix(s, "IRR") {
		s = strings.TrimSuffix(s, "IRR")
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

func getLatestUSDTPrice() int64 {
	response, err := http.Get("https://publicapi.ramzinex.com/exchange/api/v1.0/exchange/orderbooks/11/market_sell_price")
	if err != nil {
		return 0
	}
	defer response.Body.Close()
	type price struct {
		Data map[string]int64 `json:"data"`
	}
	var priceData price
	if err := json.NewDecoder(response.Body).Decode(&priceData); err != nil {
		return 0
	}
	cleanedPrice :=  priceData.Data["price"] / 10
	return cleanedPrice
}
