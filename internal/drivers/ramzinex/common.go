package ramzinex

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"
	"strings"
)

const (
	pairsAPI     = "https://api.ramzinex.com/exchange/api/v2.0/exchange/pairs"
	usdtPriceAPI = "https://publicapi.ramzinex.com/exchange/api/v1.0/exchange/orderbooks/11/market_sell_price"
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

// convertTimeToRFC3339 converts ramzinex time format "2006-01-02 15:04:05" to RFC3339
func convertTimeToRFC3339(timeStr string) string {
	t, err := time.Parse("2006-01-02 15:04:05", timeStr)
	if err != nil {
		return time.Now().UTC().Format(time.RFC3339)
	}
	return t.UTC().Format(time.RFC3339)
}

func fetchPairs(logger *slog.Logger) ([]pairDetail, map[int]string, error) {
	resp, err := http.Get(pairsAPI)
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
	logger.Info("Fetched pairs", "count", len(pairs))
	return pairs, pairMap, nil
}

func getLatestUSDTPrice() int64 {
	response, err := http.Get(usdtPriceAPI)
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
	cleanedPrice := priceData.Data["price"] / 10
	return cleanedPrice
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
