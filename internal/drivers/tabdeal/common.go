package tabdeal

import (
	"encoding/json"
	"strconv"

	"nobitex/radar/internal/scraper"
)

const usdtPriceAPI = "https://api-web.tabdeal.org/r/plots/currencies/dynamic-info/"

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

func getLatestUSDTPrice() float64 {
	resp, err := scraper.HTTPClient.Get(usdtPriceAPI)
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
