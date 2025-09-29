package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

const (
	TabdealTradesURL = "https://api1.tabdeal.org/r/api/v1/trades"
	TabdealMarketURL = "https://api1.tabdeal.org/r/api/v1/exchangeInfo"
	TradesLimit = 2
)

type Worker struct {
	message string
	success bool
}

type Market struct {
	Symbol        string `json:"symbol"`
	Status        string `json:"status"`
	TabdealSymbol string `json:"tabdealSymbol"`
}

type TradesInfo struct{
	Id  int `json:"id"`
	Price  string `json:"price"`
	Qty string `json:"qty"`
	QuoteQty string `json:"quoteqty"`
	Time	int `json:"time"`
	Buyer bool `json:"isBuyerMaker"`
}

func getTabdealMarkets() ([]string, error) {
	resp, err := http.Get(TabdealMarketURL)
	if err != nil {
		fmt.Printf("error: %s", err)
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("API returned status code: %d", resp.StatusCode)
		return nil, err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Error reading response: %v\n", err)
		return nil, err
	}

	var apiResponse []Market
	if err = json.Unmarshal(body, &apiResponse); err != nil {
		fmt.Printf("error unmarshaling API response: %s", err)
		return nil, err
	}
	var symbols []string
	for _, r := range apiResponse {
		if r.Status == "TRADING" {
			symbols = append(symbols, r.Symbol)
		}
	}
	return symbols, nil
}

func TabdealTrades(symbol string) ([]TradesInfo, error) {
	url := TabdealTradesURL + fmt.Sprintf("?symbol=%s&limit=%d", symbol, TradesLimit)
	res, err := http.Get(url)
	if err != nil {
		fmt.Printf("error API call: %s", err)
		return nil, err
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Printf("error reading Body of response: %s", err)
	}

	var trades []TradesInfo
	if err = json.Unmarshal(body, &trades); err != nil {
		fmt.Printf("error unmarshaling API response: %s", err)
		return nil, err
	}
	return trades, nil
}

func main() {
	symbols, err := getTabdealMarkets()
	if err != nil {
		fmt.Printf("%s", err)
		return
	}
	for _, s := range symbols {
		trades, err := TabdealTrades(s)
		if err != nil {
			fmt.Printf("%s \n", err)
			return
		}
		fmt.Printf("trades for %s symbol: %s \n", s, trades)	

	}
}
