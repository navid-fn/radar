package wallex

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"

	"nobitex/radar/internal/scraper"
)

const (
	// base url
	baseURL = "https://api.wallex.ir/"

	// api urls for scraps
	marketsAPI = "hector/web/v1/markets"
	ohlcAPI    = "v1/udf/history?symbol=%s&resolution=D&from=%d&to=%d"
	tradesAPI  = "v1/trades"

	// ws url and confs for scrap
	wsURL                  = "wss://api.wallex.ir/ws"
	maxSymbolsPerConn      = 50
	maxSymbolsPerConnDepth = 25

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
	resp, err := scraper.HTTPClient.Get(baseURL + marketsAPI)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}

	var data apiMarketResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}

	var markets []string
	for _, m := range data.Result.Markets {
		markets = append(markets, m.Symbol)
	}
	sort.Strings(markets)
	logger.Info("Fetched markets", "count", len(markets))
	return markets, nil
}

func getLatestUSDTPrice() float64 {
	resp, err := scraper.HTTPClient.Get(baseURL + marketsAPI)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0
	}

	var data apiMarketResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return 0
	}

	for _, m := range data.Result.Markets {
		if m.Symbol == "USDTTMN" {
			price, _ := strconv.ParseFloat(m.Price, 64)
			return price
		}
	}
	return 0
}
