package bitpin

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

const marketsAPI = "https://api.bitpin.ir/api/v1/mkt/markets/"

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

func fetchMarkets(logger *slog.Logger) ([]string, error) {
	resp, err := http.Get(marketsAPI)
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
