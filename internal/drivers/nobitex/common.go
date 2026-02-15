package nobitex

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"nobitex/radar/internal/scraper"
)

const (
	baseUrl = "https://apiv2.nobitex.ir/"

	// depthAPI fetches orderbook data
	// Doc: https://apidocs.nobitex.ir/#54977c5fca
	depthAPI         = baseUrl + "v2/depth/%s"
	marketAPI        = baseUrl + "market/stats"
	usdtPriceAPI     = baseUrl + "v3/orderbook/USDTIRT"
	latestTradeAPI   = baseUrl + "v2/trades/%s"
	depthChannelName = "public:orderbook-"

	// ohlcAPI fetches OHLC data. Params: symbol, from (unix), to (unix)
	// Doc: https://apidocs.nobitex.ir/#6ae2dae4a2
	ohlcAPI = baseUrl + "market/udf/history?symbol=%s&resolution=D&from=%d&to=%d"

	// wsURl is for fetching data from websocket
	// maxSymbolsPerConn is for maximum subscription per one websocket
	wsURL             = "wss://ws.nobitex.ir/connection/websocket"
	maxSymbolsPerConn = 100
	tradeChannelName  = "public:trades-"
)

type usdtPrice struct {
	USDTPrice string `json:"lastTradePrice"`
}

type tradeData struct {
	Symbol   string `json:"symbol"`
	Exchange string `json:"exchange"`
	Time     int64  `json:"time"`
	Price    string `json:"price"`
	Volume   string `json:"volume"`
	Side     string `json:"type"`
}

type tradeAPIResponse struct {
	Status string      `json:"status"`
	Trades []tradeData `json:"trades"`
}

type symbolData struct {
	IsClosed bool `json:"isClosed"`
}

type marketDataAPIResponse struct {
	Status string
	Stats  map[string]symbolData
}

type depthResponse struct {
	LastUpdate any        `json:"lastUpdate"`
	Bids       [][]string `json:"bids"`
	Asks       [][]string `json:"asks"`
}

// fetch tradeable markets to scrape
func fetchMarkets() ([]string, error) {
	resp, err := scraper.HTTPClient.Get(marketAPI)
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
	return symbols, nil
}

// transform pair to use it in getting latest trade
// example: btc-rls -> BTCIRT
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

func getStringValue(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		switch val := v.(type) {
		case string:
			return val
		case float64:
			return strconv.FormatFloat(val, 'f', -1, 64)
		case int64:
			return strconv.FormatInt(val, 10)
		}
	}
	return ""
}

func getTimeValue(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		return scraper.AnyToRFC3339(v)
	}
	return time.Now().UTC().Format(time.RFC3339)
}

func getLatestUSDTPrice() float64 {
	resp, err := scraper.HTTPClient.Get(usdtPriceAPI)
	if err != nil {
		fmt.Println(err)
		return 0
	}
	defer resp.Body.Close()

	var price usdtPrice
	if err := json.NewDecoder(resp.Body).Decode(&price); err != nil {

		fmt.Println(err)
		return 0
	}
	usdtPriceFloat, _ := strconv.ParseFloat(price.USDTPrice, 64)
	return usdtPriceFloat / 10
}
