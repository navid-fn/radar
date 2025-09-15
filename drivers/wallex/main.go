package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	WallexAPIURL           = "https://api.wallex.ir/hector/web/v1/markets"
	WallexWSURL            = "wss://api.wallex.ir/ws"
	DefaultKafkaBroker     = "localhost:9092"
	KafkaTopic             = "radar_trades"
	MaxSubsPerConnection   = 40
	ReconnectDelay         = 5 * time.Second
	WebSocketTimeout       = 30 * time.Second
)

// Market represents a trading market from the API
type Market struct {
	Symbol string `json:"symbol"`
}

// APIResponse represents the response from Wallex API
type APIResponse struct {
	Result struct {
		Markets []Market `json:"markets"`
	} `json:"result"`
}

// WallexProducer handles the main application logic
type WallexProducer struct {
	kafkaProducer *kafka.Producer
	logger        *logrus.Logger
	kafkaBroker   string
}

// NewWallexProducer creates a new instance of WallexProducer
func NewWallexProducer() *WallexProducer {
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = DefaultKafkaBroker
	}

	return &WallexProducer{
		logger:      logger,
		kafkaBroker: kafkaBroker,
	}
}

// initKafkaProducer initializes the Kafka producer
func (wp *WallexProducer) initKafkaProducer() error {
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

// getMarkets fetches all trading markets from Wallex API
func (wp *WallexProducer) getMarkets() ([]string, error) {
	resp, err := http.Get(WallexAPIURL)
	if err != nil {
		return nil, fmt.Errorf("error fetching markets from Wallex API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API returned status code: %d", resp.StatusCode)
	}

	var apiResponse APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResponse); err != nil {
		return nil, fmt.Errorf("error decoding API response: %w", err)
	}

	markets := make([]string, len(apiResponse.Result.Markets))
	for i, market := range apiResponse.Result.Markets {
		markets[i] = market.Symbol
	}

	// Sort for consistent chunking
	sort.Strings(markets)

	wp.logger.Infof("Fetched %d unique markets from Wallex API", len(markets))
	return markets, nil
}

// chunkMarkets splits markets into smaller chunks
func (wp *WallexProducer) chunkMarkets(markets []string, chunkSize int) [][]string {
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

// deliveryReport handles Kafka message delivery reports
func (wp *WallexProducer) deliveryReport() {
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

// websocketWorker manages a single WebSocket connection for a chunk of symbols
func (wp *WallexProducer) websocketWorker(ctx context.Context, symbolsChunk []string, wg *sync.WaitGroup) {
	defer wg.Done()

	workerID := fmt.Sprintf("Worker-%s", symbolsChunk[0])
	wp.logger.Infof("[%s] Starting for %d symbols", workerID, len(symbolsChunk))

	for {
		select {
		case <-ctx.Done():
			wp.logger.Infof("[%s] Shutting down due to context cancellation", workerID)
			return
		default:
			if err := wp.handleWebSocketConnection(ctx, workerID, symbolsChunk); err != nil {
				wp.logger.Errorf("[%s] WebSocket error: %v. Reconnecting in %v...", workerID, err, ReconnectDelay)
				
				select {
				case <-ctx.Done():
					return
				case <-time.After(ReconnectDelay):
					continue
				}
			}
		}
	}
}

// handleWebSocketConnection handles a single WebSocket connection lifecycle
func (wp *WallexProducer) handleWebSocketConnection(ctx context.Context, workerID string, symbolsChunk []string) error {
	u, err := url.Parse(WallexWSURL)
	if err != nil {
		return fmt.Errorf("invalid WebSocket URL: %w", err)
	}

	dialer := websocket.Dialer{
		HandshakeTimeout: WebSocketTimeout,
	}

	conn, _, err := dialer.Dial(u.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to connect to WebSocket: %w", err)
	}
	defer conn.Close()

	wp.logger.Infof("[%s] Connected to WebSocket", workerID)

	// Set read deadline
	conn.SetReadDeadline(time.Now().Add(WebSocketTimeout))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(WebSocketTimeout))
		return nil
	})

	// Subscribe to all symbols in this chunk
	wp.logger.Infof("[%s] Subscribing to %d markets...", workerID, len(symbolsChunk))
	for _, symbol := range symbolsChunk {
		subscriptionMsg := []interface{}{"subscribe", map[string]string{"channel": fmt.Sprintf("%s@trade", symbol)}}
		if err := conn.WriteJSON(subscriptionMsg); err != nil {
			return fmt.Errorf("failed to send subscription message: %w", err)
		}
	}
	wp.logger.Infof("[%s] Subscriptions sent", workerID)

	// Handle incoming messages
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			_, message, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					return fmt.Errorf("WebSocket read error: %w", err)
				}
				return err
			}

			// Send message to Kafka
			topic := KafkaTopic
			err = wp.kafkaProducer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          message,
			}, nil)

			if err != nil {
				wp.logger.Errorf("[%s] Failed to produce message to Kafka: %v", workerID, err)
			}
		}
	}
}

// Run starts the main application
func (wp *WallexProducer) Run() error {
	// Initialize Kafka producer
	if err := wp.initKafkaProducer(); err != nil {
		return fmt.Errorf("failed to initialize Kafka producer: %w", err)
	}
	defer wp.kafkaProducer.Close()

	// Start delivery report handler
	wp.deliveryReport()

	// Fetch markets
	markets, err := wp.getMarkets()
	if err != nil {
		return fmt.Errorf("could not fetch markets: %w", err)
	}

	if len(markets) == 0 {
		return fmt.Errorf("no markets found to subscribe to")
	}

	// Chunk markets
	marketChunks := wp.chunkMarkets(markets, MaxSubsPerConnection)
	wp.logger.Infof("Divided %d markets into %d chunks of ~%d", len(markets), len(marketChunks), MaxSubsPerConnection)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		wp.logger.Info("Received shutdown signal, gracefully shutting down...")
		cancel()
	}()

	// Start workers
	var wg sync.WaitGroup
	for _, chunk := range marketChunks {
		wg.Add(1)
		go wp.websocketWorker(ctx, chunk, &wg)
	}

	wp.logger.Info("All workers started, waiting for completion...")
	wg.Wait()
	wp.logger.Info("All workers completed")

	return nil
}

func main() {
	producer := NewWallexProducer()
	
	if err := producer.Run(); err != nil {
		producer.logger.Fatalf("Application failed: %v", err)
	}
}
