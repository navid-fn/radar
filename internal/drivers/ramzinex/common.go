package ramzinex

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"
)

const pairsAPI = "https://api.ramzinex.com/exchange/api/v2.0/exchange/pairs"

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



