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
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

// Configuration constants
const (
	BitpinAPIURL         = "https://api.bitpin.ir/api/v1/mkt/markets/"
	BitpinWSURL          = "wss://ws.bitpin.ir"
	DefaultKafkaBroker   = "localhost:9092"
	KafkaTopic           = "radar_trades"
	MaxSubsPerConnection = 40

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

type Market struct {
	Symbol    string `json:"symbol"`
	Tradeable bool   `json:"tradable"`
}

type BitpinProducer struct {
	kafkaProducer     *kafka.Producer
	logger            *logrus.Logger
	kafkaBroker       string
}

func NewBitpinProducer() *BitpinProducer {
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = DefaultKafkaBroker
	}

	// Initialize persistence manager
	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = "./data"
	}

	return &BitpinProducer{
		logger:            logger,
		kafkaBroker:       kafkaBroker,
	}
}

// initKafkaProducer initializes the Kafka producer
func (wp *BitpinProducer) initKafkaProducer() error {
	config := kafka.ConfigMap{
		"bootstrap.servers": wp.kafkaBroker,
	}

	producer, err := kafka.NewProducer(&config)
	if err != nil {
		return fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	wp.kafkaProducer = producer
	wp.logger.Info("Kafka Producer initialized successfully")
	return nil
}

func (wp *BitpinProducer) getMarkets() ([]string, error) {
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

	markets = nil // Reset markets slice

	for _, market := range apiResponse {
		if market.Tradeable {
			markets = append(markets, market.Symbol)
		}
	}

	sort.Strings(markets)

	wp.logger.Infof("Fetched %d unique markets from Bitpin API", len(markets))
	return markets, nil
}

func (wp *BitpinProducer) chunkMarkets(markets []string, chunkSize int) [][]string {
	var chunks [][]string
	for i := 0; i < len(markets); i += chunkSize {
		end := i + chunkSize
		if end > len(markets) {
			end = len(markets)
		}
		chunks = append(chunks, markets[i:end])
	}
	return chunks
}

func (wp *BitpinProducer) deliveryReport() {
	go func() {
		for e := range wp.kafkaProducer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					wp.logger.Errorf("Message delivery failed: %v", ev.TopicPartition.Error)
				}
			}
		}
	}()
}

func (wp *BitpinProducer) websocketWorker(ctx context.Context, symbolsChunk []string, wg *sync.WaitGroup) {
	defer wg.Done()

	workerID := fmt.Sprintf("Worker-%s", symbolsChunk[0])
	wp.logger.Infof("[%s] Starting for %d symbols", workerID, len(symbolsChunk))

	reconnectDelay := InitialReconnectDelay
	consecutiveErrors := 0

	for {
		select {
		case <-ctx.Done():
			wp.logger.Infof("[%s] Shutting down due to context cancellation", workerID)
			return
		default:
			if err := wp.handleWebSocketConnection(ctx, workerID, symbolsChunk); err != nil {
				consecutiveErrors++
				wp.logger.Errorf("[%s] WebSocket error (%d/%d): %v. Reconnecting in %v...",
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
					wp.logger.Warnf("[%s] Too many consecutive errors, extending delay", workerID)
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

func (wp *BitpinProducer) handleWebSocketConnection(ctx context.Context, workerID string, symbolsChunk []string) error {
	u, err := url.Parse(BitpinWSURL)
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

	wp.logger.Infof("[%s] Connected to WebSocket", workerID)

	// Create context for this connection
	connCtx, connCancel := context.WithCancel(ctx)
	defer connCancel()

	// Channel for pong responses
	pongReceived := make(chan bool, 1)
	lastPongTime := time.Now()

	// Set up pong handler
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
			wp.logger.Errorf("[%s] Failed to send pong: %v", workerID, err)
		}
		return err
	})

	subscriptionMsg := map[string]any{
		"method":  "sub_to_market_data",
		"symbols": symbolsChunk,
	}

	conn.SetWriteDeadline(time.Now().Add(WriteTimeout))
	if err := conn.WriteJSON(subscriptionMsg); err != nil {
		return fmt.Errorf("failed to send subscription message: %w", err)
	}

	pingTicker := time.NewTicker(PingInterval)
	defer pingTicker.Stop()

	healthTicker := time.NewTicker(HealthCheckInterval)
	defer healthTicker.Stop()

	readErrors := make(chan error, 1)
	messages := make(chan []byte, 100)

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
			wp.logger.Infof("[%s] Context cancelled, closing connection", workerID)
			return nil

		case err := <-readErrors:
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure, websocket.CloseNormalClosure) {
				return fmt.Errorf("WebSocket read error: %w", err)
			}
			if err != nil {
				return fmt.Errorf("connection error: %w", err)
			}

		case message := <-messages:
			// Check for PONG messages (like in Python version)
			messageStr := string(message)
			if messageStr == `{"message":"PONG"}` {
				wp.logger.Infof("[%s] < PONG received", workerID)
				continue
			}

			// Send message to Kafka with fault tolerance
			topic := KafkaTopic
			err = wp.kafkaProducer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          message,
			}, nil)

			if err != nil {
				wp.logger.Errorf("[%s] Failed to produce message to Kafka: %v", workerID, err)
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
					wp.logger.Warnf("[%s] Pong timeout, connection may be unhealthy", workerID)
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

func (wp *BitpinProducer) Run() error {
	wp.logger.Info("Starting BitPin Producer...")
	if err := wp.initKafkaProducer(); err != nil {
		return fmt.Errorf("failed to initialize Kafka producer: %w", err)
	}

	defer wp.kafkaProducer.Close()

	wp.deliveryReport()

	// Fetch markets with fault tolerance
	markets, err := wp.getMarkets()
	if err != nil {
		return fmt.Errorf("could not fetch markets after retries: %w", err)
	}

	if len(markets) == 0 {
		return fmt.Errorf("no markets found to subscribe to")
	}

	marketChunks := wp.chunkMarkets(markets, MaxSubsPerConnection)
	wp.logger.Infof("Divided %d markets into %d chunks of ~%d", len(markets), len(marketChunks), MaxSubsPerConnection)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		wp.logger.Info("Received shutdown signal, gracefully shutting down...")
		cancel()
	}()

	// Start periodic cleanup of old persistence files

	var wg sync.WaitGroup
	for i, chunk := range marketChunks {
		wg.Add(1)
		go wp.websocketWorker(ctx, chunk, &wg)

		// Stagger worker startup
		if i < len(marketChunks)-1 {
			time.Sleep(1 * time.Second)
		}
	}

	wp.logger.Info("All workers started, waiting for completion...")
	wg.Wait()
	wp.logger.Info("All workers completed")

	return nil
}

func main() {
	producer := NewBitpinProducer()

	if err := producer.Run(); err != nil {
		producer.logger.Fatalf("Application failed: %v", err)
	}
}
