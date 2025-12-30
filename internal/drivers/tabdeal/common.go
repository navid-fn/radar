package tabdeal

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
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
	response, err := http.Get(usdtPriceAPI)
	if err != nil {
		fmt.Println("Error Calling api")
		return 0
	}
	defer response.Body.Close()

	type ticker struct {
		Price string `json:"price"`
	}

	type currencies struct {
		Currencies map[string]map[string]ticker `json:"currencies"`
	}
	var prices currencies
	if err := json.NewDecoder(response.Body).Decode(&prices); err != nil {
		return 0
	}
	usdtPriceFloat, _ := strconv.ParseFloat(prices.Currencies["USDT"]["IRT"].Price, 64)
	return usdtPriceFloat

}
