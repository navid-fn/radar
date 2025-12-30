package wallex

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sort"
)

const marketsAPI = "https://api.wallex.ir/hector/web/v1/markets"

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



