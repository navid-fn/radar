package nobitex

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sort"
	"strings"
)

const marketAPI = "https://apiv2.nobitex.ir/market/stats"

func fetchMarkets(logger *slog.Logger) ([]string, error) {
	resp, err := http.Get(marketAPI)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var data marketDataAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}

	var symbols []string
	for pair, stats := range data.Stats {
		if !stats.IsClosed {
			symbols = append(symbols, transformPair(pair))
		}
	}
	sort.Strings(symbols)
	logger.Info("Fetched markets", "count", len(symbols))
	return symbols, nil
}

func transformPair(pair string) string {
	parts := strings.Split(pair, "-")
	for i := range parts {
		parts[i] = strings.ToUpper(parts[i])
	}
	if len(parts) > 1 && parts[len(parts)-1] == "RLS" {
		parts[len(parts)-1] = "IRT"
	}
	return strings.Join(parts, "")
}



