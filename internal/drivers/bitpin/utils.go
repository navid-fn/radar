package bitpin

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type market struct {
	Symbol    string `json:"symbol"`
	Tradeable bool   `json:"tradable"`
}

func cleanSymbol(s string) string {
	if strings.HasSuffix(s, "_IRT") {
		s = strings.TrimSuffix(s, "_IRT")
		return s + "/IRT"
	} else {
		s = strings.TrimSuffix(s, "_USDT")
		return s + "/USDT"
	}
}

type ticker struct {
	Symbol string `json:"symbol"`
	Price  string `json:"price"`
}

func getLatestUSDTPrice() float64 {
	response, err := http.Get("https://api.bitpin.ir/api/v1/mkt/tickers/")
	if err != nil {
		time.Sleep(time.Second * 2)
		return getLatestUSDTPrice()
	}
	defer response.Body.Close()
	var tickersData []ticker
	if err := json.NewDecoder(response.Body).Decode(&tickersData); err != nil {
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
