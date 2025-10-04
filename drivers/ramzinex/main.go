package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

const (
	RamzinexAPIUrl       = "https://api.ramzinex.com/exchange/api/v2.0/exchange/pairs"
	RamzinexWSUrl        = "wss://websocket.ramzinex.com/websocket"
	DefaultKafkaBroker   = "localhost:9092"
	KafkaTopic           = "radar_trades"
	MaxSubsPerConnection = 20

	// Connection timeouts and intervals
	InitialReconnectDelay = 1 * time.Second
	MaxReconnectDelay     = 30 * time.Second
	HandshakeTimeout      = 10 * time.Second
	ReadTimeout           = 60 * time.Second
	WriteTimeout          = 10 * time.Second
	PingInterval          = 30 * time.Second
	PongTimeout           = 10 * time.Second

	// Connection health
	MaxConsecutiveErrors = 5
	HealthCheckInterval  = 5 * time.Second
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

type APIRespnse struct {
	Status int `json:"status"`
	Data   struct {
		Pairs []PairData `json:"pairs"`
	} `json:"data"`
}

type RamzinexProducer struct {
	kafkaProducer *kafka.Producer
	logger        *logrus.Logger
	kafkaBroker   string
	tradeTracker  *TradeTracker
}

type TradeTracker struct {
	seenTradeHashes map[string]map[string]bool
	mu              sync.RWMutex
}

func newTradeTracker() *TradeTracker {
	return &TradeTracker{
		seenTradeHashes: make(map[string]map[string]bool),
	}
}

func (tt *TradeTracker) isTradeProcessed(channel string, tradeHash string) bool {
	tt.mu.RLock()
	defer tt.mu.RUnlock()

	if channelMap, exists := tt.seenTradeHashes[channel]; exists {
		return channelMap[tradeHash]
	}
	return false
}

func (tt *TradeTracker) markTradeProcessed(channel string, tradeHash string) {
	tt.mu.Lock()
	defer tt.mu.Unlock()

	if tt.seenTradeHashes[channel] == nil {
		tt.seenTradeHashes[channel] = make(map[string]bool)
	}
	tt.seenTradeHashes[channel][tradeHash] = true

	// Keep only last 1000 hashes per channel to prevent memory growth
	if len(tt.seenTradeHashes[channel]) > 1000 {
		// Remove oldest entries (simple approach: clear and keep recent)
		count := 0
		for hash := range tt.seenTradeHashes[channel] {
			if count < 500 {
				delete(tt.seenTradeHashes[channel], hash)
				count++
			} else {
				break
			}
		}
	}
}

func NewRamzinexProducer() *RamzinexProducer {
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = DefaultKafkaBroker
	}

	return &RamzinexProducer{
		logger:       logger,
		kafkaBroker:  kafkaBroker,
		tradeTracker: newTradeTracker(),
	}
}

func (rp *RamzinexProducer) initKafkaProducer() error {
	config := kafka.ConfigMap{
		"bootstrap.servers": rp.kafkaBroker,
	}

	producer, err := kafka.NewProducer(&config)
	if err != nil {
		return fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	rp.kafkaProducer = producer
	rp.logger.Info("Kafka Producer initialized successfully")
	return nil
}

func (rp *RamzinexProducer) getPairs() ([]PairDetail, error) {
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

	var pairDataApi APIRespnse
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
	}

	rp.logger.Infof("Fetched %d pairs from Ramzinex API", len(pairs))
	return pairs, nil
}

func (rp *RamzinexProducer) chunkPairs(pairs []PairDetail, chunkSize int) [][]PairDetail {
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

func (rp *RamzinexProducer) deliveryReport() {
	go func() {
		for e := range rp.kafkaProducer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					rp.logger.Errorf("Message delivery failed: %v", ev.TopicPartition.Error)
				}
			}
		}
	}()
}

func (rp *RamzinexProducer) filterDuplicateTrades(channel string, data any, workerID string) []interface{} {
	var filteredTrades []any

	// Data should be an array of trade arrays
	tradesArray, ok := data.([]any)
	if !ok {
		return filteredTrades
	}

	totalTrades := len(tradesArray)
	duplicatesCount := 0

	for _, trade := range tradesArray {
		tradeArray, ok := trade.([]any)
		if !ok || len(tradeArray) < 6 {
			// Invalid trade format, skip
			continue
		}

		// Extract hash from index 5
		tradeHash, ok := tradeArray[5].(string)
		if !ok || tradeHash == "" {
			// No hash available, include the trade (better to send than lose data)
			filteredTrades = append(filteredTrades, trade)
			continue
		}

		// Check if already processed
		if rp.tradeTracker.isTradeProcessed(channel, tradeHash) {
			duplicatesCount++
			continue
		}

		// Mark as processed and include in filtered trades
		rp.tradeTracker.markTradeProcessed(channel, tradeHash)
		filteredTrades = append(filteredTrades, trade)
	}

	if duplicatesCount > 0 {
		rp.logger.Infof("[%s] Channel %s - Filtered %d duplicates out of %d trades, sending %d unique trades",
			workerID, channel, duplicatesCount, totalTrades, len(filteredTrades))
	}

	return filteredTrades
}

func (rp *RamzinexProducer) websocketWorker(ctx context.Context, pairsChunk []PairDetail, wg *sync.WaitGroup) {
	defer wg.Done()

	workerID := fmt.Sprintf("Worker-%s", pairsChunk[0].Name)
	rp.logger.Infof("[%s] Starting for %d pairs", workerID, len(pairsChunk))

	reconnectDelay := InitialReconnectDelay
	consecutiveErrors := 0

	for {
		select {
		case <-ctx.Done():
			rp.logger.Infof("[%s] Shutting down due to context cancellation", workerID)
			return
		default:
			if err := rp.handleWebSocketConnection(ctx, workerID, pairsChunk); err != nil {
				consecutiveErrors++
				rp.logger.Errorf("[%s] WebSocket error (%d/%d): %v. Reconnecting in %v...",
					workerID, consecutiveErrors, MaxConsecutiveErrors, err, reconnectDelay)

				// Exponential backoff with max limit
				if reconnectDelay < MaxReconnectDelay {
					reconnectDelay *= 2
					if reconnectDelay > MaxReconnectDelay {
						reconnectDelay = MaxReconnectDelay
					}
				}

				// If too many consecutive errors, wait longer
				if consecutiveErrors >= MaxConsecutiveErrors {
					rp.logger.Warnf("[%s] Too many consecutive errors, extending delay", workerID)
					reconnectDelay = MaxReconnectDelay
				}

				select {
				case <-ctx.Done():
					return
				case <-time.After(reconnectDelay):
					continue
				}
			} else {
				// Reset on successful connection
				consecutiveErrors = 0
				reconnectDelay = InitialReconnectDelay
			}
		}
	}
}

func (rp *RamzinexProducer) handleWebSocketConnection(ctx context.Context, workerID string, pairsChunk []PairDetail) error {
	u, err := url.Parse(RamzinexWSUrl)
	if err != nil {
		return fmt.Errorf("invalid WebSocket URL: %w", err)
	}

	dialer := websocket.Dialer{
		HandshakeTimeout: HandshakeTimeout,
	}

	conn, _, err := dialer.Dial(u.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to connect to WebSocket: %w", err)
	}
	defer conn.Close()

	rp.logger.Infof("[%s] Connected to WebSocket", workerID)

	// Create context for this connection
	connCtx, connCancel := context.WithCancel(ctx)
	defer connCancel()

	// Send connect message as required by Ramzinex (Centrifuge protocol)
	connectMsg := map[string]any{
		"connect": map[string]string{
			"name": "js",
		},
		"id": 1,
	}
	conn.SetWriteDeadline(time.Now().Add(WriteTimeout))
	if err := conn.WriteJSON(connectMsg); err != nil {
		return fmt.Errorf("failed to send connect message: %w", err)
	}
	rp.logger.Infof("[%s] Sent connect message", workerID)

	// Small delay after connect before subscribing
	time.Sleep(500 * time.Millisecond)

	// Create a map of pair ID to pair Name for later use
	pairIDToName := make(map[int]string)
	for _, pair := range pairsChunk {
		pairIDToName[pair.ID] = pair.Name
	}

	// Subscribe to all pairs in this chunk
	// Start subscription IDs from 2 since connect used ID 1
	subscriptionID := 2
	for _, pair := range pairsChunk {
		channel := fmt.Sprintf("last-trades:%d", pair.ID)
		subscriptionMsg := map[string]any{
			"id": subscriptionID,
			"subscribe": map[string]any{
				"channel": channel,
				"recover": true,
			},
		}

		conn.SetWriteDeadline(time.Now().Add(WriteTimeout))
		if err := conn.WriteJSON(subscriptionMsg); err != nil {
			return fmt.Errorf("failed to send subscription message for %s: %w", pair.Name, err)
		}

		// Increase delay between subscriptions to avoid rate limiting
		time.Sleep(200 * time.Millisecond)
		subscriptionID++
	}

	rp.logger.Infof("[%s] Successfully subscribed to %d channels", workerID, len(pairsChunk))

	pingTicker := time.NewTicker(PingInterval)
	defer pingTicker.Stop()

	healthTicker := time.NewTicker(HealthCheckInterval)
	defer healthTicker.Stop()

	readErrors := make(chan error, 1)
	messages := make(chan []byte, 100)

	// Pong handler
	pongReceived := make(chan bool, 1)
	lastPongTime := time.Now()

	conn.SetPongHandler(func(string) error {
		select {
		case pongReceived <- true:
		default:
		}
		lastPongTime = time.Now()
		return nil
	})

	conn.SetPingHandler(func(message string) error {
		err := conn.WriteControl(websocket.PongMessage, []byte(message), time.Now().Add(WriteTimeout))
		if err != nil {
			rp.logger.Errorf("[%s] Failed to send pong: %v", workerID, err)
		}
		return err
	})

	go func() {
		defer close(messages)
		defer close(readErrors)

		for {
			select {
			case <-connCtx.Done():
				return
			default:
				conn.SetReadDeadline(time.Now().Add(ReadTimeout))
				_, message, err := conn.ReadMessage()
				if err != nil {
					select {
					case readErrors <- err:
					case <-connCtx.Done():
					}
					return
				}

				select {
				case messages <- message:
				case <-connCtx.Done():
					return
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			rp.logger.Infof("[%s] Context cancelled, closing connection", workerID)
			return nil

		case err := <-readErrors:
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure, websocket.CloseNormalClosure) {
				return fmt.Errorf("WebSocket read error: %w", err)
			}
			if err != nil {
				return fmt.Errorf("connection error: %w", err)
			}

		case message := <-messages:
			messageStr := string(message)

			// Check if it's a ping message (empty JSON {})
			if messageStr == "{}" || messageStr == "{}\n" || len(strings.TrimSpace(messageStr)) == 2 {
				// Respond with pong
				conn.SetWriteDeadline(time.Now().Add(WriteTimeout))
				pongMsg := map[string]any{}
				if err := conn.WriteJSON(pongMsg); err != nil {
					rp.logger.Errorf("[%s] Failed to send pong: %v", workerID, err)
				}
				continue
			}

			// Parse message
			var msgCheck map[string]any
			if err := json.Unmarshal(message, &msgCheck); err == nil {
				// Check if it's an error message
				if errMsg, hasError := msgCheck["error"]; hasError {
					rp.logger.Warnf("[%s] Received error from server: %v", workerID, errMsg)
					continue
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
										if pairName, found := pairIDToName[pairID]; found {
											channelName := fmt.Sprintf("last-trades:%s", pairName)

											// Filter duplicate trades
											filteredTrades := rp.filterDuplicateTrades(channelName, data, workerID)

											if len(filteredTrades) > 0 {
												// Create new message with pair name and filtered data
												kafkaMsg := map[string]any{
													"channel": channelName,
													"data":    filteredTrades,
												}

												kafkaMsgBytes, _ := json.Marshal(kafkaMsg)
												topic := KafkaTopic
												err = rp.kafkaProducer.Produce(&kafka.Message{
													TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
													Value:          kafkaMsgBytes,
												}, nil)

												if err != nil {
													rp.logger.Errorf("[%s] Failed to send to Kafka: %v", workerID, err)
												}
											}
											continue
										}
									}
								}
							}
						}
					}
				}
			}

		case <-pingTicker.C:
			// Send ping to keep connection alive
			conn.SetWriteDeadline(time.Now().Add(WriteTimeout))
			if err := conn.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
				return fmt.Errorf("failed to send ping: %w", err)
			}

			// Wait for pong with timeout
			go func() {
				select {
				case <-pongReceived:
					// Pong received, connection is healthy
				case <-time.After(PongTimeout):
					rp.logger.Warnf("[%s] Pong timeout, connection may be unhealthy", workerID)
				case <-connCtx.Done():
					return
				}
			}()

		case <-healthTicker.C:
			// Check connection health
			timeSinceLastPong := time.Since(lastPongTime)
			if timeSinceLastPong > (PingInterval + PongTimeout) {
				return fmt.Errorf("connection appears unhealthy, last pong was %v ago", timeSinceLastPong)
			}
		}
	}
}

func (rp *RamzinexProducer) Run() error {
	rp.logger.Info("Starting Ramzinex Producer...")
	if err := rp.initKafkaProducer(); err != nil {
		return fmt.Errorf("failed to initialize Kafka producer: %w", err)
	}

	defer rp.kafkaProducer.Close()

	rp.deliveryReport()

	// Fetch pairs
	pairs, err := rp.getPairs()
	if err != nil {
		return fmt.Errorf("could not fetch pairs: %w", err)
	}

	if len(pairs) == 0 {
		return fmt.Errorf("no pairs found to subscribe to")
	}

	pairChunks := rp.chunkPairs(pairs, MaxSubsPerConnection)
	rp.logger.Infof("Divided %d pairs into %d chunks of ~%d", len(pairs), len(pairChunks), MaxSubsPerConnection)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		rp.logger.Info("Received shutdown signal, gracefully shutting down...")
		cancel()
	}()

	var wg sync.WaitGroup
	for i, chunk := range pairChunks {
		wg.Add(1)
		go rp.websocketWorker(ctx, chunk, &wg)

		// Stagger worker startup
		if i < len(pairChunks)-1 {
			time.Sleep(1 * time.Second)
		}
	}

	rp.logger.Info("All workers started, waiting for completion...")
	wg.Wait()
	rp.logger.Info("All workers completed")

	return nil
}

func main() {
	producer := NewRamzinexProducer()

	if err := producer.Run(); err != nil {
		producer.logger.Fatalf("Application failed: %v", err)
	}
}
