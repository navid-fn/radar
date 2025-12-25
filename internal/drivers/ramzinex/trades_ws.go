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

	"nobitex/radar/configs"
	"nobitex/radar/internal/scraper"

	"github.com/gorilla/websocket"
)

const (
	RamzinexAPIUrl       = "https://api.ramzinex.com/exchange/api/v2.0/exchange/pairs"
	RamzinexWSUrl        = "wss://websocket.ramzinex.com/websocket"
	MaxSubsPerConnection = 100
)

type RamzinexScraper struct {
	*scraper.BaseScraper
	wsWorker     *scraper.BaseWebSocketWorker
	pairIDToName map[int]string

	usdtPrice float64
	usdtMu    sync.RWMutex
}

func NewRamzinexScraper(cfg *configs.Config) *RamzinexScraper {
	config := scraper.NewConfig("ramzinex", MaxSubsPerConnection, cfg)
	baseScraper := scraper.NewBaseScraper(config)

	rs := &RamzinexScraper{
		BaseScraper:  baseScraper,
		pairIDToName: make(map[int]string),
		usdtPrice:    float64(getLatestUSDTPrice()),
	}

	rs.Logger.Info("INFO", "USDT PRICE:", rs.usdtPrice)

	wsConfig := scraper.DefaultWebSocketConfig(RamzinexWSUrl)
	wsConfig.DisableClientPing = true // Ramzinex uses protocol-level pings (JSON {}), not WebSocket pings
	rs.wsWorker = scraper.NewBaseWebSocketWorker(wsConfig, rs.Logger, rs.SendToKafka)
	rs.wsWorker.SendToKafkaCtx = rs.SendToKafkaWithContext

	rs.wsWorker.OnConnect = func(conn *websocket.Conn) error {
		connectMsg := map[string]any{
			"connect": map[string]string{
				"name": "js",
			},
			"id": 1,
		}
		if err := rs.wsWorker.WriteJSON(conn, connectMsg); err != nil {
			return fmt.Errorf("failed to send connect message: %w", err)
		}
		rs.Logger.Info("Sent connect message")
		time.Sleep(500 * time.Millisecond)
		return nil
	}

	rs.wsWorker.OnMessage = func(conn *websocket.Conn, message []byte) ([]byte, error) {
		messageStr := strings.TrimSpace(string(message))

		if messageStr == "{}" || messageStr == "{}\n" || messageStr == "{}\r\n" {
			if err := rs.wsWorker.SendPong(conn); err != nil {
				rs.Logger.Error("Failed to send pong", "error", err)
			}
			return nil, nil
		}

		var emptyCheck map[string]any
		if err := json.Unmarshal(message, &emptyCheck); err == nil && len(emptyCheck) == 0 {
			if err := rs.wsWorker.SendPong(conn); err != nil {
				rs.Logger.Error("Failed to send pong", "error", err)
			}
			return nil, nil
		}

		var msgCheck map[string]any
		if err := json.Unmarshal(message, &msgCheck); err == nil {
			if errMsg, hasError := msgCheck["error"]; hasError {
				rs.Logger.Warn("Received error from server", "error", errMsg)
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
									if pairName, found := rs.pairIDToName[pairID]; found {
										// Process trades directly without duplication check
										tradesArray, ok := data.([]any)
										if ok && len(tradesArray) > 0 {
											var listOfKafkaMsg []scraper.KafkaData
											for _, trade := range tradesArray {
												row, ok := trade.([]any)
												if !ok || len(row) < 6 {
													continue
												}

												id := row[5].(string)
												side := row[3].(string)
												cleanedSymbol := cleanSymbol(strings.ToUpper(pairName))
												cleanedPrice := cleanPrice(cleanedSymbol, row[0].(float64))

												volume := row[1].(float64)
												quantity := cleanedPrice * volume
												time := row[2].(string)

												if cleanedSymbol == "USDT/IRT" {
													rs.usdtMu.Lock()
													rs.usdtPrice =cleanedPrice 
													rs.usdtMu.Unlock()
												}

												dataToSendKafka := scraper.KafkaData{
													Exchange:  "ramzinex",
													Symbol:    cleanedSymbol,
													Side:      side,
													ID:        id,
													Price:     cleanedPrice,
													Volume:    volume,
													Quantity:  quantity,
													Time:      time,
													USDTPrice: rs.usdtPrice,
												}
												listOfKafkaMsg = append(listOfKafkaMsg, dataToSendKafka)
											}

											if len(listOfKafkaMsg) > 0 {
												kafkaMsgBytes, _ := scraper.SerializeProtoBatch(listOfKafkaMsg)
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

	return rs
}

func (rs *RamzinexScraper) Name() string {
	return "ramzinex"
}

func (rs *RamzinexScraper) FetchMarkets() ([]pairDetail, error) {
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

	var pairDataApi apiPairResponse
	if err = json.Unmarshal(body, &pairDataApi); err != nil {
		return nil, fmt.Errorf("error unmarshaling API response: %w", err)
	}

	if pairDataApi.Status != 0 {
		return nil, &APIError{}
	}

	var pairs []pairDetail
	for _, pd := range pairDataApi.Data.Pairs {
		pairDetail := pairDetail{
			ID:   pd.ID,
			Name: pd.Name.En,
		}
		pairs = append(pairs, pairDetail)
		rs.pairIDToName[pd.ID] = pd.Name.En
	}

	rs.Logger.Info("Fetched pairs from Ramzinex API", "count", len(pairs))
	return pairs, nil
}

func chunkPairs(pairs []pairDetail, chunkSize int) [][]pairDetail {
	var chunks [][]pairDetail
	for i := 0; i < len(pairs); i += chunkSize {
		end := i + chunkSize
		if min(end, len(pairs)) != end {
			end = len(pairs)
		}
		chunks = append(chunks, pairs[i:end])
	}
	return chunks
}

func (rs *RamzinexScraper) Run(ctx context.Context) error {
	rs.Logger.Info("Starting Ramzinex Scraper...")

	if err := rs.InitKafkaProducer(); err != nil {
		return fmt.Errorf("failed to initialize Kafka producer: %w", err)
	}
	defer rs.CloseKafkaProducer()

	pairs, err := rs.FetchMarkets()
	if err != nil {
		return fmt.Errorf("could not fetch pairs: %w", err)
	}

	if len(pairs) == 0 {
		return fmt.Errorf("no pairs found to subscribe to")
	}

	pairChunks := chunkPairs(pairs, rs.Config.MaxSubsPerConnection)
	rs.Logger.Info("Divided pairs into chunks",
		"total", len(pairs),
		"chunks", len(pairChunks),
		"chunkSize", rs.Config.MaxSubsPerConnection)

	return scraper.RunWithGracefulShutdown(rs.Logger, func(ctx context.Context, wg *sync.WaitGroup) {
		for i, chunk := range pairChunks {
			subscriptionID := 2
			rs.wsWorker.OnSubscribe = func(conn *websocket.Conn, _ []string) error {
				for _, pair := range chunk {
					channel := fmt.Sprintf("last-trades:%d", pair.ID)
					subscriptionMsg := map[string]any{
						"id": subscriptionID,
						"subscribe": map[string]any{
							"channel": channel,
							"recover": true,
						},
					}

					if err := rs.wsWorker.WriteJSON(conn, subscriptionMsg); err != nil {
						return fmt.Errorf("failed to send subscription message for %s: %w", pair.Name, err)
					}

					time.Sleep(200 * time.Millisecond)
					subscriptionID++
				}
				rs.Logger.Info("Successfully subscribed to channels", "count", len(chunk))
				return nil
			}

			symbolsChunk := make([]string, len(chunk))
			for j, pair := range chunk {
				symbolsChunk[j] = pair.Name
			}

			wg.Add(1)
			go rs.wsWorker.RunWorker(ctx, symbolsChunk, wg, "RamzinexWorker")

			if i < len(pairChunks)-1 {
				time.Sleep(1 * time.Second)
			}
		}
	})
}
