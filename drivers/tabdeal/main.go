package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

func getTabdealMarkets() ([]Market, error) {
	url := "https://api1.tabdeal.org/r/api/v1/exchangeInfo"
	resp, err := http.Get(url)
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
	fmt.Printf("%s", apiResponse)
	return apiResponse, nil
}

func main() {
	apiResponse, err := getTabdealMarkets()
	if err != nil {
		fmt.Printf("%s", err)
		return
	}
	fmt.Printf("%s", apiResponse)
}
