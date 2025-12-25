package coingecko

type TickerResponse struct {
	Tickers  []Ticker `json:"tickers"`
	Exchange string   `json:"-"`
}

type Ticker struct {
	Base            string        `json:"base"`
	Target          string        `json:"target"`
	ConvertedLast   ConvertedData `json:"converted_last"`
	ConvertedVolume ConvertedData `json:"converted_volume"`
	LastFetchAt     string        `json:"last_fetch_at"`
	Volume          float64       `json:"volume"`
}

type ConvertedData struct {
	USD float64 `json:"usd"`
}
