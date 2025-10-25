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
	tradeTracker *crawler.TradeTracker
	pairIDToName map[int]string
}

// NewRamzinexCrawler creates a new Ramzinex crawler instance
func NewRamzinexCrawler() *RamzinexCrawler {
	config := crawler.NewConfig("ramzinex", MaxSubsPerConnection)
	baseCrawler := crawler.NewBaseCrawler(config)

	rc := &RamzinexCrawler{
		BaseCrawler:  baseCrawler,
		tradeTracker: crawler.NewTradeTracker(),
		pairIDToName: make(map[int]string),
	}

	// Setup WebSocket worker with Ramzinex-specific configuration
	wsConfig := crawler.DefaultWebSocketConfig(RamzinexWSUrl)
	rc.wsWorker = crawler.NewBaseWebSocketWorker(wsConfig, rc.Logger, rc.SendToKafka)

	// Set Ramzinex-specific OnConnect handler (Centrifuge protocol)
	rc.wsWorker.OnConnect = func(conn *websocket.Conn) error {
		connectMsg := map[string]any{
			"connect": map[string]string{
				"name": "js",
			},
			"id": 1,
		}
		conn.SetWriteDeadline(time.Now().Add(wsConfig.WriteTimeout))
		if err := conn.WriteJSON(connectMsg); err != nil {
			return fmt.Errorf("failed to send connect message: %w", err)
		}
		rc.Logger.Info("Sent connect message")
		time.Sleep(500 * time.Millisecond)
		return nil
	}

	// Set Ramzinex-specific OnMessage handler to filter duplicates and format messages
	rc.wsWorker.OnMessage = func(message []byte) ([]byte, error) {
		messageStr := string(message)

		// Check if it's a ping message (empty JSON {})
		if messageStr == "{}" || messageStr == "{}\n" || len(strings.TrimSpace(messageStr)) == 2 {
			// Respond with pong (handled in OnMessage to customize response)
			return nil, nil
		}

		// Parse message
		var msgCheck map[string]any
		if err := json.Unmarshal(message, &msgCheck); err == nil {
			// Check if it's an error message
			if errMsg, hasError := msgCheck["error"]; hasError {
				rc.Logger.Warnf("Received error from server: %v", errMsg)
				return nil, nil
			}

			// Check if it's a push message with trade data
			if pushData, hasPush := msgCheck["push"].(map[string]any); hasPush {
				if pub, hasPub := pushData["pub"].(map[string]any); hasPub {
					if data, hasData := pub["data"]; hasData {
						// Extract channel and replace ID with Name
						if channelStr, ok := pushData["channel"].(string); ok {
							// Parse channel to get pair ID (e.g., "last-trades:10")
							parts := strings.Split(channelStr, ":")
							if len(parts) == 2 {
								var pairID int
								if _, err := fmt.Sscanf(parts[1], "%d", &pairID); err == nil {
									if pairName, found := rc.pairIDToName[pairID]; found {
										channelName := fmt.Sprintf("last-trades:%s", pairName)

										// Filter duplicate trades
										filteredTrades := rc.filterDuplicateTrades(channelName, data)

										if len(filteredTrades) > 0 {
											// Create new message with pair name and filtered data
											kafkaMsg := map[string]any{
												"channel": channelName,
												"data":    filteredTrades,
											}

											kafkaMsgBytes, _ := json.Marshal(kafkaMsg)
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

		return nil, nil
	}

	return rc
}

// GetName returns the exchange name
func (rc *RamzinexCrawler) GetName() string {
	return "ramzinex"
}

// FetchMarkets fetches all tradeable pairs from Ramzinex API
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
		// Store ID to Name mapping
		rc.pairIDToName[pd.ID] = pd.Name.En
	}

	rc.Logger.Infof("Fetched %d pairs from Ramzinex API", len(pairs))
	return pairs, nil
}

// filterDuplicateTrades filters duplicate trades based on hash
func (rc *RamzinexCrawler) filterDuplicateTrades(channel string, data any) []interface{} {
	var filteredTrades []any

	tradesArray, ok := data.([]any)
	if !ok {
		return filteredTrades
	}

	totalTrades := len(tradesArray)
	duplicatesCount := 0

	for _, trade := range tradesArray {
		tradeArray, ok := trade.([]any)
		if !ok || len(tradeArray) < 6 {
			continue
		}

		// Extract hash from index 5
		tradeHash, ok := tradeArray[5].(string)
		if !ok || tradeHash == "" {
			filteredTrades = append(filteredTrades, trade)
			continue
		}

		// Check if already processed
		if rc.tradeTracker.IsTradeProcessed(channel, tradeHash) {
			duplicatesCount++
			continue
		}

		// Mark as processed and include in filtered trades
		rc.tradeTracker.MarkTradeProcessed(channel, tradeHash)
		filteredTrades = append(filteredTrades, trade)
	}

	if duplicatesCount > 0 {
		rc.Logger.Infof("Channel %s - Filtered %d duplicates out of %d trades, sending %d unique trades",
			channel, duplicatesCount, totalTrades, len(filteredTrades))
	}

	return filteredTrades
}

// chunkPairs splits pairs into smaller chunks
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

// Run starts the Ramzinex crawler
func (rc *RamzinexCrawler) Run(ctx context.Context) error {
	rc.Logger.Info("Starting Ramzinex Crawler...")

	// Initialize Kafka producer
	if err := rc.InitKafkaProducer(); err != nil {
		return fmt.Errorf("failed to initialize Kafka producer: %w", err)
	}
	defer rc.CloseKafkaProducer()

	rc.StartDeliveryReport()

	// Fetch pairs
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

	// Convert PairDetail chunks to string chunks for WebSocket worker
	// and setup custom subscribe handler
	return crawler.RunWithGracefulShutdown(rc.Logger, func(ctx context.Context, wg *sync.WaitGroup) {
		for i, chunk := range pairChunks {
			// Setup custom OnSubscribe for this chunk
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

					conn.SetWriteDeadline(time.Now().Add(rc.wsWorker.Config.WriteTimeout))
					if err := conn.WriteJSON(subscriptionMsg); err != nil {
						return fmt.Errorf("failed to send subscription message for %s: %w", pair.Name, err)
					}

					time.Sleep(200 * time.Millisecond)
					subscriptionID++
				}
				rc.Logger.Infof("Successfully subscribed to %d channels", len(chunk))
				return nil
			}

			// Convert to string slice for worker
			symbolsChunk := make([]string, len(chunk))
			for j, pair := range chunk {
				symbolsChunk[j] = pair.Name
			}

			wg.Add(1)
			go rc.wsWorker.RunWorker(ctx, symbolsChunk, wg, "RamzinexWorker")

			// Stagger worker startup
			if i < len(pairChunks)-1 {
				time.Sleep(1 * time.Second)
			}
		}
	})
}
