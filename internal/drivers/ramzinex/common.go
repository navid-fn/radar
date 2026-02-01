package ramzinex

import (
	"encoding/json"
	"io"

	"nobitex/radar/internal/scraper"
)

const (
	pairsAPI     = "https://api.ramzinex.com/exchange/api/v2.0/exchange/pairs"
	usdtPriceAPI = "https://publicapi.ramzinex.com/exchange/api/v1.0/exchange/orderbooks/11/market_sell_price"
	ohlcURL      = "https://publicapi.ramzinex.com/exchange/api/v1.0/exchange/chart/tv/v2.0/history"
)

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

// ramzinex uses "2006-01-02 15:04:05" format

func fetchPairs() ([]pairDetail, map[int]string, error) {
	resp, err := scraper.HTTPClient.Get(pairsAPI)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var data apiPairResponse
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, nil, err
	}

	pairMap := make(map[int]string)
	var pairs []pairDetail
	for _, pd := range data.Data.Pairs {
		p := pairDetail{ID: pd.ID, Name: pd.Name.En}
		pairs = append(pairs, p)
		pairMap[pd.ID] = pd.Name.En
	}
	return pairs, pairMap, nil
}

func getLatestUSDTPrice() int64 {
	resp, err := scraper.HTTPClient.Get(usdtPriceAPI)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()

	type price struct {
		Data map[string]int64 `json:"data"`
	}
	var priceData price
	if err := json.NewDecoder(resp.Body).Decode(&priceData); err != nil {
		return 0
	}
	return priceData.Data["price"] / 10
}
