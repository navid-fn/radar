package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	TabdealTradesURL   = "https://api1.tabdeal.org/r/api/v1/trades"
	TabdealMarketURL   = "https://api1.tabdeal.org/r/api/v1/exchangeInfo"
	TradesLimit        = 2
	DefaultKafkaBroker = "localhost:9092"
	KafkaTopic         = "radar_trades"
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

type TradesInfo struct {
	ID       int    `json:"id"`
	Price    string `json:"price"`
	Qty      string `json:"qty"`
	QuoteQty string `json:"quoteqty"`
	Time     int64  `json:"time"`
	Buyer    bool   `json:"isBuyerMaker"`
	Symbol   string `json:"symbol"`
	Exchange string `json:"exchange"`
}

func (t TradesInfo) MarshalJSON() ([]byte, error) {
	type AliasTradesInfo TradesInfo
	return json.Marshal(&struct {
		AliasTradesInfo
		Time time.Time `json:"time"`
	}{
		AliasTradesInfo: AliasTradesInfo(t),
		Time:            time.UnixMilli(t.Time),
	})
}

type KafkaTabdeal struct {
	kafkaProducer *kafka.Producer
	kafkaBroker   string
}

func newKafkaHandler() *KafkaTabdeal {
	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = DefaultKafkaBroker
	}

	return &KafkaTabdeal{
		kafkaBroker: kafkaBroker,
	}
}

func (k *KafkaTabdeal) initKafkaProducer() error {
	config := kafka.ConfigMap{
		"bootstrap.servers": k.kafkaBroker,
	}

	producer, err := kafka.NewProducer(&config)
	if err != nil {
		return fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	k.kafkaProducer = producer
	return nil
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

func TabdealTrades(symbol string, tradeChan chan<- []byte) {
	fmt.Printf("Starting API call for symbol: %s \n", symbol)

	url := TabdealTradesURL + fmt.Sprintf("?symbol=%s&limit=%d", symbol, TradesLimit)
	for {
		res, err := http.Get(url)
		if err != nil {
			fmt.Printf("error API call: %s", err)
		}
		body, err := io.ReadAll(res.Body)
		if err != nil {
			fmt.Printf("error reading Body of response: %s \n", err)
			return
		}

		var trades []TradesInfo
		if err = json.Unmarshal(body, &trades); err != nil {
			fmt.Printf("error unmarshaling API response: %s \n, body: %s", err, body)
			return
		}
		for _, t := range trades {
			t.Symbol = symbol
			t.Exchange = "tabdeal"
			jsonData, err := t.MarshalJSON()
			if err != nil {
				fmt.Printf("error marshaling trade: %s \n", err)
			}
			tradeChan <- jsonData
		}
		time.Sleep(2 * time.Second)
	}
}

func main() {
	symbols, err := getTabdealMarkets()
	fmt.Printf("Length of active symbols are: %d \n", len(symbols))
	if err != nil {
		fmt.Printf("%s", err)
		return
	}

	kafkaHandler := newKafkaHandler()
	err = kafkaHandler.initKafkaProducer()
	if err != nil {
		fmt.Println("Error in kafka: %s", err)
	}

	tradeChan := make(chan []byte, 100)

	for _, s := range symbols {
		go TabdealTrades(s, tradeChan)
	}

	for {
		res := <-tradeChan

		topic := KafkaTopic
		err = kafkaHandler.kafkaProducer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          res,
		}, nil)
		if err != nil {
			fmt.Println("Error in sending to kafka:", err)
		}
	}
}
