package ramzinex

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/navid-fn/radar/internal/crawler"
)

const (
	RamzinexAPIUrl       = "https://api.ramzinex.com/exchange/api/v2.0/exchange/pairs"
	RamzinexWSUrl        = "wss://websocket.ramzinex.com/websocket"
	MaxSubsPerConnection = 20
)

type APIError struct{}

func (e *APIError) Error() string {
	return "Failed to call api with status failed"
}

type PairName struct {
	Fa string
	En string
}

type PairData struct {
	ID   int
	Name PairName
}

type PairDetail struct {
	Name string
	ID   int
}

type APIResponse struct {
	Status int `json:"status"`
	Data   struct {
		Pairs []PairData `json:"pairs"`
	} `json:"data"`
}

type RamzinexCrawler struct {
	*crawler.BaseCrawler
	wsWorker     *crawler.BaseWebSocketWorker
	pairIDToName map[int]string
}

func NewRamzinexCrawler() *RamzinexCrawler {
	config := crawler.NewConfig("ramzinex", MaxSubsPerConnection)
	baseCrawler := crawler.NewBaseCrawler(config)

	rc := &RamzinexCrawler{
		BaseCrawler:  baseCrawler,
		pairIDToName: make(map[int]string),
	}

	wsConfig := crawler.DefaultWebSocketConfig(RamzinexWSUrl)
	rc.wsWorker = crawler.NewBaseWebSocketWorker(wsConfig, rc.Logger, rc.SendToKafka)

	rc.wsWorker.OnConnect = func(conn *websocket.Conn) error {
		connectMsg := map[string]any{
			"connect": map[string]string{
				"name": "js",
			},
			"id": 1,
		}
		if err := rc.wsWorker.WriteJSON(conn, connectMsg); err != nil {
			return fmt.Errorf("failed to send connect message: %w", err)
		}
		rc.Logger.Info("Sent connect message")
		time.Sleep(500 * time.Millisecond)
		return nil
	}

	rc.wsWorker.OnMessage = func(conn *websocket.Conn, message []byte) ([]byte, error) {
		messageStr := strings.TrimSpace(string(message))

		if messageStr == "{}" || messageStr == "{}\n" || messageStr == "{}\r\n" {
			if err := rc.wsWorker.SendPong(conn); err != nil {
				rc.Logger.Errorf("Failed to send pong: %v", err)
			}
			return nil, nil
		}

		var emptyCheck map[string]any
		if err := json.Unmarshal(message, &emptyCheck); err == nil && len(emptyCheck) == 0 {
			if err := rc.wsWorker.SendPong(conn); err != nil {
				rc.Logger.Errorf("Failed to send pong: %v", err)
			}
			return nil, nil
		}

		var msgCheck map[string]any
		if err := json.Unmarshal(message, &msgCheck); err == nil {
			if errMsg, hasError := msgCheck["error"]; hasError {
				rc.Logger.Warnf("Received error from server: %v", errMsg)
				return nil, nil
			}

			if pushData, hasPush := msgCheck["push"].(map[string]any); hasPush {
				if pub, hasPub := pushData["pub"].(map[string]any); hasPub {
					if data, hasData := pub["data"]; hasData {
						if channelStr, ok := pushData["channel"].(string); ok {
							parts := strings.Split(channelStr, ":")
							if len(parts) == 2 {
								var pairID int
								if _, err := fmt.Sscanf(parts[1], "%d", &pairID); err == nil {
									if pairName, found := rc.pairIDToName[pairID]; found {
										// Process trades directly without duplication check
										tradesArray, ok := data.([]any)
										if ok && len(tradesArray) > 0 {
											var listOfKafkaMsg []crawler.KafkaData
											for _, trade := range tradesArray {
												row, ok := trade.([]any)
												if !ok || len(row) < 6 {
													continue
												}

												id := row[5].(string)
												side := row[3].(string)
												price := row[0].(float64)
												volume := row[1].(float64)
												quantity := price * volume
												time := row[2].(string)

												dataToSendKafka := crawler.KafkaData{
													Exchange: "ramzinex",
													Symbol:   pairName,
													Side:     side,
													ID:       id,
													Price:    price,
													Volume:   volume,
													Quantity: quantity,
													Time:     time,
												}
												listOfKafkaMsg = append(listOfKafkaMsg, dataToSendKafka)
											}

											if len(listOfKafkaMsg) > 0 {
												kafkaMsgBytes, _ := json.Marshal(listOfKafkaMsg)
												return kafkaMsgBytes, nil
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
		return nil, nil
	}

	return rc
}

func (rc *RamzinexCrawler) GetName() string {
	return "ramzinex"
}

func (rc *RamzinexCrawler) FetchMarkets() ([]PairDetail, error) {
	resp, err := http.Get(RamzinexAPIUrl)
	if err != nil {
		return nil, fmt.Errorf("error fetching pairs from Ramzinex API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading API response: %w", err)
	}

	var pairDataApi APIResponse
	if err = json.Unmarshal(body, &pairDataApi); err != nil {
		return nil, fmt.Errorf("error unmarshaling API response: %w", err)
	}

	if pairDataApi.Status != 0 {
		return nil, &APIError{}
	}

	var pairs []PairDetail
	for _, pd := range pairDataApi.Data.Pairs {
		pairDetail := PairDetail{
			ID:   pd.ID,
			Name: pd.Name.En,
		}
		pairs = append(pairs, pairDetail)
		rc.pairIDToName[pd.ID] = pd.Name.En
	}

	rc.Logger.Infof("Fetched %d pairs from Ramzinex API", len(pairs))
	return pairs, nil
}

func chunkPairs(pairs []PairDetail, chunkSize int) [][]PairDetail {
	var chunks [][]PairDetail
	for i := 0; i < len(pairs); i += chunkSize {
		end := i + chunkSize
		if end > len(pairs) {
			end = len(pairs)
		}
		chunks = append(chunks, pairs[i:end])
	}
	return chunks
}

func (rc *RamzinexCrawler) Run(ctx context.Context) error {
	rc.Logger.Info("Starting Ramzinex Crawler...")

	if err := rc.InitKafkaProducer(); err != nil {
		return fmt.Errorf("failed to initialize Kafka producer: %w", err)
	}
	defer rc.CloseKafkaProducer()

	rc.StartDeliveryReport()

	pairs, err := rc.FetchMarkets()
	if err != nil {
		return fmt.Errorf("could not fetch pairs: %w", err)
	}

	if len(pairs) == 0 {
		return fmt.Errorf("no pairs found to subscribe to")
	}

	pairChunks := chunkPairs(pairs, rc.Config.MaxSubsPerConnection)
	rc.Logger.Infof("Divided %d pairs into %d chunks of ~%d",
		len(pairs), len(pairChunks), rc.Config.MaxSubsPerConnection)

	return crawler.RunWithGracefulShutdown(rc.Logger, func(ctx context.Context, wg *sync.WaitGroup) {
		for i, chunk := range pairChunks {
			subscriptionID := 2
			rc.wsWorker.OnSubscribe = func(conn *websocket.Conn, _ []string) error {
				for _, pair := range chunk {
					channel := fmt.Sprintf("last-trades:%d", pair.ID)
					subscriptionMsg := map[string]any{
						"id": subscriptionID,
						"subscribe": map[string]any{
							"channel": channel,
							"recover": true,
						},
					}

					if err := rc.wsWorker.WriteJSON(conn, subscriptionMsg); err != nil {
						return fmt.Errorf("failed to send subscription message for %s: %w", pair.Name, err)
					}

					time.Sleep(200 * time.Millisecond)
					subscriptionID++
				}
				rc.Logger.Infof("Successfully subscribed to %d channels", len(chunk))
				return nil
			}

			symbolsChunk := make([]string, len(chunk))
			for j, pair := range chunk {
				symbolsChunk[j] = pair.Name
			}

			wg.Add(1)
			go rc.wsWorker.RunWorker(ctx, symbolsChunk, wg, "RamzinexWorker")

			if i < len(pairChunks)-1 {
				time.Sleep(1 * time.Second)
			}
		}
	})
}
