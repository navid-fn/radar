package tabdeal

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	"nobitex/radar/internal/scraper"
)

const (
	baseWebAPIURL = "https://api-web.tabdeal.org/"
	usdtPriceURL  = baseWebAPIURL + "r/plots/currencies/dynamic-info/"
	ohlcURL       = baseWebAPIURL + "r/plots/history/?symbol=%s&from=%d&to=%d&resolution=1D"

	// trade urls
	baseAPIURL = "https://api1.tabdeal.org/"
	tradesURL  = baseAPIURL + "r/api/v1/trades"
	marketsURL = baseAPIURL + "r/api/v1/exchangeInfo"
	limit      = 10
)

type market struct {
	Symbol        string `json:"symbol"`
	Status        string `json:"status"`
	TabdealSymbol string `json:"tabdealSymbol"`
}

type tradesInfo struct {
	ID       int    `json:"id"`
	Price    string `json:"price"`
	Qty      string `json:"qty"`
	QuoteQty string `json:"quoteqty"`
	Time     int64  `json:"time"`
	Buyer    bool   `json:"isBuyerMaker"`
}

func fetchMarkets() ([]string, error) {
	resp, err := scraper.HTTPClient.Get(marketsURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 500 || isHTMLResponse(body) {
		return nil, fmt.Errorf("gateway error")
	}

	var markets []market
	if err := json.Unmarshal(body, &markets); err != nil {
		return nil, err
	}

	var symbols []string
	for _, m := range markets {
		if m.Status == "TRADING" {
			symbols = append(symbols, m.Symbol)
		}
	}
	return symbols, nil
}

func getLatestUSDTPrice() float64 {
	resp, err := scraper.HTTPClient.Get(usdtPriceURL)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()

	type ticker struct {
		Price string `json:"price"`
	}

	type currencies struct {
		Currencies map[string]map[string]ticker `json:"currencies"`
	}
	var prices currencies
	if err := json.NewDecoder(resp.Body).Decode(&prices); err != nil {
		return 0
	}
	usdtPriceFloat, _ := strconv.ParseFloat(prices.Currencies["USDT"]["IRT"].Price, 64)
	return usdtPriceFloat
}
