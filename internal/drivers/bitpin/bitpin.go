package bitpin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/navid-fn/radar/internal/crawler"
)

const (
	BitpinAPIURL         = "https://api.bitpin.ir/api/v1/mkt/markets/"
	BitpinWSURL          = "wss://ws.bitpin.ir"
	MaxSubsPerConnection = 40
)

type Market struct {
	Symbol    string `json:"symbol"`
	Tradeable bool   `json:"tradable"`
}

type MessageType struct {
	Event string `json:"event"`
}
type MatcheData struct{
	ID string `json:"id"`
	Price string `json:"price"`
	Volume string `json:"base_amount"`
	Quantity string `json:"quate_amount"`
	Side string `json:"side"`
}

type MatchesUpdate struct {
	Matches []MatcheData `json:"matches"`
	Symbol string `json:"symbol"`
	Time string `json:"event_time"`
}


type BitpinCrawler struct {
	*crawler.BaseCrawler
	wsWorker *crawler.BaseWebSocketWorker
}

func NewBitpinCrawler() *BitpinCrawler {
	config := crawler.NewConfig("bitpin", MaxSubsPerConnection)
	baseCrawler := crawler.NewBaseCrawler(config)

	bc := &BitpinCrawler{
		BaseCrawler: baseCrawler,
	}

	wsConfig := crawler.DefaultWebSocketConfig(BitpinWSURL)
	bc.wsWorker = crawler.NewBaseWebSocketWorker(wsConfig, bc.Logger, bc.SendToKafka)

	bc.wsWorker.OnMessage = func(message []byte) ([]byte, error) {

		messageStr := string(message)
		if messageStr == `{"message":"PONG"}` {
			bc.Logger.Debug("PONG received")
			return nil, nil
		}
		var messageType MessageType
		err := json.Unmarshal(message, &messageType)
		if err != nil {
			return nil, nil
		}

		if messageType.Event == "matches_update" {
			var matchesUpdate MatchesUpdate
			err := json.Unmarshal(message, &matchesUpdate)
			if err != nil {
				return nil, nil
			}

			
			var messageToSend []crawler.KafkaData
			for _, t := range matchesUpdate.Matches {
				volume, _:= strconv.ParseFloat(t.Volume, 64)
				price, _ := strconv.ParseFloat(t.Price, 64)
				quantity, _ := strconv.ParseFloat(t.Quantity, 64)
				match_time := matchesUpdate.Time

				data := crawler.KafkaData {
					Exchange: "bitpin",
					Side: t.Side,
					Volume: volume,
					Price: price,
					Quantity: quantity,
					Time: match_time,
				}
				messageToSend = append(messageToSend, data)
			}
			jsonData, err := json.Marshal(messageToSend)
			if err != nil {
				return nil, nil
			}
			return jsonData, nil
		}
		return nil, nil
	}

	bc.wsWorker.OnSubscribe = func(conn *websocket.Conn, symbols []string) error {
		subscriptionMsg := map[string]any{
			"method":  "sub_to_market_data",
			"symbols": symbols,
		}

		conn.SetWriteDeadline(time.Now().Add(wsConfig.WriteTimeout))
		if err := conn.WriteJSON(subscriptionMsg); err != nil {
			return fmt.Errorf("failed to send subscription message: %w", err)
		}

		bc.Logger.Infof("Subscribed to %d symbols", len(symbols))
		return nil
	}

	return bc
}

func (bc *BitpinCrawler) GetName() string {
	return "bitpin"
}

func (bc *BitpinCrawler) FetchMarkets() ([]string, error) {
	var markets []string

	resp, err := http.Get(BitpinAPIURL)
	if err != nil {
		return nil, fmt.Errorf("error fetching markets from Bitpin API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading API response: %w", err)
	}

	var apiResponse []Market
	if err = json.Unmarshal(body, &apiResponse); err != nil {
		return nil, fmt.Errorf("error unmarshaling API response: %w", err)
	}

	for _, market := range apiResponse {
		if market.Tradeable {
			markets = append(markets, market.Symbol)
		}
	}

	sort.Strings(markets)

	bc.Logger.Infof("Fetched %d unique markets from Bitpin API", len(markets))
	return markets, nil
}

func (bc *BitpinCrawler) Run(ctx context.Context) error {
	bc.Logger.Info("Starting Bitpin Crawler...")

	if err := bc.InitKafkaProducer(); err != nil {
		return fmt.Errorf("failed to initialize Kafka producer: %w", err)
	}
	defer bc.CloseKafkaProducer()

	bc.StartDeliveryReport()

	markets, err := bc.FetchMarkets()
	if err != nil {
		return fmt.Errorf("could not fetch markets: %w", err)
	}

	if len(markets) == 0 {
		return fmt.Errorf("no markets found to subscribe to")
	}

	marketChunks := crawler.ChunkMarkets(markets, bc.Config.MaxSubsPerConnection)
	bc.Logger.Infof("Divided %d markets into %d chunks of ~%d",
		len(markets), len(marketChunks), bc.Config.MaxSubsPerConnection)

	return crawler.RunWithGracefulShutdown(bc.Logger, func(ctx context.Context, wg *sync.WaitGroup) {
		for i, chunk := range marketChunks {
			wg.Add(1)
			go bc.wsWorker.RunWorker(ctx, chunk, wg, "BitpinWorker")

			if i < len(marketChunks)-1 {
				time.Sleep(1 * time.Second)
			}
		}
	})
}


