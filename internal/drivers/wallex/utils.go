package wallex

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

type market struct {
	Symbol  string `json:"symbol"`
	Enabled bool   `json:"is_market_type_enable"`
	Price   string `json:"price"`
}

type apiMarketResponse struct {
	Result struct {
		Markets []market `json:"markets"`
	} `json:"result"`
}

type tradeData struct {
	Symbol   string `json:"symbol"`
	Exchange string `json:"-"`
	Time     string `json:"timestamp"`
	Price    string `json:"price"`
	Volume   string `json:"quantity"`
	Side     bool   `json:"isBuyOrder"`
}

type tradeAPIResult struct {
	Trades []tradeData `json:"latestTrades"`
}

type tradeAPIResponse struct {
	Status  bool           `json:"success"`
	Result  tradeAPIResult `json:"result"`
	Message string         `json:"message"`
	Code    int            `json:"code"`
}

func cleanSymbol(s string) string {
	if strings.HasSuffix(s, "TMN") {
		s = strings.TrimSuffix(s, "TMN")
		return s + "/IRT"
	} else {
		s = strings.TrimSuffix(s, "USDT")
		return s + "/USDT"
	}
}

func getLatestUSDTPrice() float64 {
	resp, err := http.Get(WallexAPIURL)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0
	}

	var marketAPIresposnse apiMarketResponse
	if err := json.NewDecoder(resp.Body).Decode(&marketAPIresposnse); err != nil {
		return 0
	}

	for _, market := range marketAPIresposnse.Result.Markets {
		if market.Symbol == "USDTTMN" {
			price, _ := strconv.ParseFloat(market.Price, 64)
			return price
		}
	}
	return 0
}
