package bitpin

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sort"
)

const marketsAPI = "https://api.bitpin.ir/api/v1/mkt/markets/"

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



