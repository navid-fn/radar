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
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gorilla/websocket"
	"github.com/navid-fn/radar/pkg/faulttolerance"
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
	apiCircuitBreaker *faulttolerance.CircuitBreaker
	wsCircuitBreaker  *faulttolerance.CircuitBreaker
	retryer           *faulttolerance.Retryer
	healthMonitor     *faulttolerance.HealthMonitor
	persistence       *faulttolerance.PersistenceManager
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

	// Initialize circuit breakers
	apiCircuitBreaker := faulttolerance.NewCircuitBreaker(
		faulttolerance.CircuitBreakerConfig{
			MaxFailures:      5,
			Timeout:          60 * time.Second,
			SuccessThreshold: 3,
			Name:             "BitpinAPI",
		}, logger)

	wsCircuitBreaker := faulttolerance.NewCircuitBreaker(
		faulttolerance.CircuitBreakerConfig{
			MaxFailures:      3,
			Timeout:          30 * time.Second,
			SuccessThreshold: 2,
			Name:             "BitpinWebSocket",
		}, logger)

	// Initialize retryer
	retryConfig := faulttolerance.DefaultRetryConfig("BitpinOperations")
	retryConfig.MaxAttempts = 3
	retryConfig.BaseDelay = 2 * time.Second
	retryer := faulttolerance.NewRetryer(retryConfig, logger)

	// Initialize health monitor
	healthMonitor := faulttolerance.NewHealthMonitor(logger, 30*time.Second)

	// Initialize persistence manager
	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = "./data"
	}
	persistence, err := faulttolerance.NewPersistenceManager(dataDir, 1000, 10*time.Second, logger)
	if err != nil {
		logger.Errorf("Failed to initialize persistence manager: %v", err)
		persistence = nil
	}

	return &BitpinProducer{
		logger:            logger,
		kafkaBroker:       kafkaBroker,
		apiCircuitBreaker: apiCircuitBreaker,
		wsCircuitBreaker:  wsCircuitBreaker,
		retryer:           retryer,
		healthMonitor:     healthMonitor,
		persistence:       persistence,
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

	err := wp.retryer.ExecuteWithCircuitBreaker(context.Background(), wp.apiCircuitBreaker, func() error {
		resp, err := http.Get(BitpinAPIURL)
		if err != nil {
			return fmt.Errorf("error fetching markets from Bitpin API: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("API returned status code: %d", resp.StatusCode)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error reading API response: %w", err)
		}

		var apiResponse []Market
		if err = json.Unmarshal(body, &apiResponse); err != nil {
			return fmt.Errorf("error unmarshaling API response: %w", err)
		}

		markets = nil // Reset markets slice
		for _, market := range apiResponse {
			if market.Tradeable {
				markets = append(markets, market.Symbol)
			}
		}

		sort.Strings(markets)
		return nil
	})

	if err != nil {
		return nil, err
	}

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

				// Store message for later retry if persistence is available
				if wp.persistence != nil {
					wp.persistence.StoreMessage(message)
					wp.logger.Debugf("[%s] Message stored for later retry", workerID)
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
	wp.logger.Info("Starting BitPin Producer with fault tolerance...")

	// Start persistence manager if available
	if wp.persistence != nil {
		wp.persistence.Start()
		defer wp.persistence.Stop()

		// Attempt to recover messages from previous runs
		if recoveredMessages, err := wp.persistence.RecoverMessages(24 * time.Hour); err == nil && len(recoveredMessages) > 0 {
			wp.logger.Infof("Recovered %d messages from previous runs", len(recoveredMessages))
			// TODO: Implement message replay logic
		}
	}

	// Start health monitor
	wp.setupHealthChecks()
	wp.healthMonitor.Start()
	defer wp.healthMonitor.Stop()

	// Start health check HTTP server
	healthPort := 8080
	if portStr := os.Getenv("HEALTH_PORT"); portStr != "" {
		if p, err := strconv.Atoi(portStr); err == nil {
			healthPort = p
		}
	}
	wp.healthMonitor.StartHTTPServer(healthPort)

	// Initialize Kafka producer with retries
	err := wp.retryer.Execute(context.Background(), func() error {
		return wp.initKafkaProducer()
	})
	if err != nil {
		return fmt.Errorf("failed to initialize Kafka producer after retries: %w", err)
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
	if wp.persistence != nil {
		go func() {
			ticker := time.NewTicker(1 * time.Hour)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if err := wp.persistence.CleanupOldFiles(7 * 24 * time.Hour); err != nil {
						wp.logger.Warnf("Failed to cleanup old persistence files: %v", err)
					}
				}
			}
		}()
	}

	// Start workers with staggered startup to avoid overwhelming the server
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

func (wp *BitpinProducer) setupHealthChecks() {
	wp.healthMonitor.AddCheck("kafka", func(ctx context.Context) error {
		if wp.kafkaProducer == nil {
			return fmt.Errorf("kafka producer not initialized")
		}

		metadata, err := wp.kafkaProducer.GetMetadata(nil, false, 5000)
		if err != nil {
			return fmt.Errorf("failed to get kafka metadata: %w", err)
		}

		if len(metadata.Brokers) == 0 {
			return fmt.Errorf("no kafka brokers available")
		}

		return nil
	})

	wp.healthMonitor.AddCheck("bitpin_api", func(ctx context.Context) error {
		return wp.apiCircuitBreaker.Execute(ctx, func() error {
			client := &http.Client{Timeout: 10 * time.Second}
			resp, err := client.Get(BitpinAPIURL)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("API returned status %d", resp.StatusCode)
			}

			return nil
		})
	})

	wp.healthMonitor.AddCheck("api_circuit_breaker", func(ctx context.Context) error {
		state := wp.apiCircuitBreaker.GetState()
		if state == faulttolerance.StateOpen {
			return fmt.Errorf("API circuit breaker is open")
		}
		return nil
	})

	wp.healthMonitor.AddCheck("ws_circuit_breaker", func(ctx context.Context) error {
		state := wp.wsCircuitBreaker.GetState()
		if state == faulttolerance.StateOpen {
			return fmt.Errorf("WebSocket circuit breaker is open")
		}
		return nil
	})

	if wp.persistence != nil {
		wp.healthMonitor.AddCheck("persistence", func(ctx context.Context) error {
			stats := wp.persistence.GetStats()
			if bufferSize, ok := stats["buffer_size"].(int); ok && bufferSize > 900 {
				return fmt.Errorf("persistence buffer nearly full: %d", bufferSize)
			}
			return nil
		})
	}
}

func main() {
	producer := NewBitpinProducer()

	if err := producer.Run(); err != nil {
		producer.logger.Fatalf("Application failed: %v", err)
	}
}
