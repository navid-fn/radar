package wallex

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
)

const (
	marketsAPI       = "https://api.wallex.ir/hector/web/v1/markets"
	wallexMarketsAPI = "https://api.wallex.ir/hector/web/v1/markets"
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

func fetchMarkets(logger *slog.Logger) ([]string, error) {
	resp, err := http.Get(marketsAPI)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, err
	}

	var data apiMarketResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}

	var markets []string
	for _, m := range data.Result.Markets {
		if m.Enabled {
			markets = append(markets, m.Symbol)
		}
	}
	sort.Strings(markets)
	logger.Info("Fetched markets", "count", len(markets))
	return markets, nil
}

func getLatestUSDTPrice() float64 {
	resp, err := http.Get(wallexMarketsAPI)
	if err != nil {
		return getLatestUSDTPrice()
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return getLatestUSDTPrice()
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

