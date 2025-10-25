package coingecko

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	// "github.com/navid-fn/radar/utils"
)

const (
	Url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=usd&days=1&interval=daily"
)

// {
//   "prices": [
//     [1729868800000, 67500.0],     // Oct 24, 2025: $67,500
//     [1730073600000, 68200.0]      // Oct 25, 2025: $68,200
//   ],
//   "market_caps": [
//     [1729868800000, 1330000000000],
//     [1730073600000, 1345000000000]
//   ],
//   "total_volumes": [
//     [1729868800000, 25000000000],  // Oct 24: $25B volume
//     [1730073600000, 28000000000]   // Oct 25: $28B volume
//   ]
// }


type MarketChart struct {
    Prices       []any `json:"prices"`       // Array of [int64, float64]
    MarketCaps   []any `json:"market_caps"`  // Array of [int64, float64]
    TotalVolumes []any `json:"total_volumes"` // Array of [int64, float64]
}

func FetechUSDTVolumeChange() {
    url := "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=usd&days=2&interval=daily"
    
    resp, err := http.Get(url)
    if err != nil {
        log.Fatalf("Error fetching data: %v", err)
    }
    defer resp.Body.Close()
    
    if resp.StatusCode != http.StatusOK {
        log.Fatalf("HTTP error: %s", resp.Status)
    }
    
    body, err := io.ReadAll(resp.Body)
    if err != nil {
        log.Fatalf("Error reading response: %v", err)
    }
    
    var chart MarketChart
    if err := json.Unmarshal(body, &chart); err != nil {
        log.Fatalf("Error unmarshaling JSON: %v", err)
    }
    if len(chart.Prices) < 2 {
        log.Fatalf("Invalid Data")
    }

		yesterdayPrice := chart.Prices[0].([]any)
		todayPrice := chart.Prices[1].([]any)
		fmt.Printf("%s", yesterdayPrice...)
		fmt.Printf("%s", todayPrice...)
    
    
}
