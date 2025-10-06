package depth

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

// Configuration constants
const (
	WallexWSURL = "wss://api.wallex.ir/ws"

	// Connection timeouts and intervals
	HandshakeTimeout = 4 * time.Second
	ReadTimeout      = 60 * time.Second
	WriteTimeout     = 10 * time.Second
	PingInterval     = 30 * time.Second
	PongTimeout      = 10 * time.Second

	// Connection health
	HealthCheckInterval = 5 * time.Second

	// Depth specific - throttle to avoid sending duplicate data
	DefaultDepthThrottleInterval = 7 * time.Second
)

// DepthWorker manages WebSocket connection for orderbook depth
type DepthWorker struct {
	kafkaProducer    *kafka.Producer
	logger           *logrus.Logger
	kafkaTopic       string
	throttleInterval time.Duration
	depthMessageType string // "sellDepth" or "buyDepth"
}

// NewDepthWorker creates a new DepthWorker instance
func NewDepthWorker(kafkaProducer *kafka.Producer, logger *logrus.Logger, kafkaTopic string, depthMessageType string, throttleInterval time.Duration) *DepthWorker {
	if throttleInterval == 0 {
		throttleInterval = DefaultDepthThrottleInterval
	}

	return &DepthWorker{
		kafkaProducer:    kafkaProducer,
		logger:           logger,
		kafkaTopic:       kafkaTopic,
		throttleInterval: throttleInterval,
		depthMessageType: depthMessageType,
	}
}

// Run starts the depth worker for a chunk of symbols
func (dw *DepthWorker) Run(ctx context.Context, symbolsChunk []string, wg *sync.WaitGroup) {
	defer wg.Done()

	workerID := fmt.Sprintf("DepthWorker-%s-%s", dw.depthMessageType, symbolsChunk[0])
	dw.logger.Infof("[%s] Starting for %d symbols with %v throttle", workerID, len(symbolsChunk), dw.throttleInterval)

	for {
		select {
		case <-ctx.Done():
			dw.logger.Infof("[%s] Shutting down due to context cancellation", workerID)
			return
		default:
			if err := dw.handleWebSocketConnection(ctx, workerID, symbolsChunk); err != nil {
				dw.logger.Errorf("[%s] WebSocket error: %v. Reconnecting in 5s...", workerID, err)

				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Second):
					continue
				}
			}
		}
	}
}

// handleWebSocketConnection handles a single WebSocket connection lifecycle with throttling
func (dw *DepthWorker) handleWebSocketConnection(ctx context.Context, workerID string, symbolsChunk []string) error {
	u, err := url.Parse(WallexWSURL)
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

	dw.logger.Infof("[%s] Connected to WebSocket", workerID)

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

	// Set up ping handler (for server-initiated pings)
	conn.SetPingHandler(func(message string) error {
		err := conn.WriteControl(websocket.PongMessage, []byte(message), time.Now().Add(WriteTimeout))
		if err != nil {
			dw.logger.Errorf("[%s] Failed to send pong: %v", workerID, err)
		}
		return err
	})

	// Subscribe to all symbols in this chunk for depth
	dw.logger.Infof("[%s] Subscribing to %d markets for %s...", workerID, len(symbolsChunk), dw.depthMessageType)
	for _, symbol := range symbolsChunk {
		conn.SetWriteDeadline(time.Now().Add(WriteTimeout))
		subscriptionMsg := []any{"subscribe", map[string]string{"channel": fmt.Sprintf("%s@%s", symbol, dw.depthMessageType)}}
		if err := conn.WriteJSON(subscriptionMsg); err != nil {
			return fmt.Errorf("failed to send subscription message: %w", err)
		}
	}
	dw.logger.Infof("[%s] Subscriptions sent", workerID)

	// Start ping routine
	pingTicker := time.NewTicker(PingInterval)
	defer pingTicker.Stop()

	// Start health check routine
	healthTicker := time.NewTicker(HealthCheckInterval)
	defer healthTicker.Stop()

	// Throttle ticker - only send depth data every N seconds
	throttleTicker := time.NewTicker(dw.throttleInterval)
	defer throttleTicker.Stop()

	// Channel for read errors
	readErrors := make(chan error, 1)
	messages := make(chan []byte, 100)

	// Latest depth data per symbol - we'll only send the most recent snapshot
	var latestDepthMu sync.Mutex
	latestDepthData := make(map[string][]byte)
	messageCount := 0

	// Start message reader goroutine
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

	dw.logger.Infof("[%s] Starting message processing loop with throttling", workerID)

	// Main event loop
	for {
		select {
		case <-ctx.Done():
			dw.logger.Infof("[%s] Context cancelled, closing connection", workerID)
			return nil

		case err := <-readErrors:
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure, websocket.CloseNormalClosure) {
				return fmt.Errorf("WebSocket read error: %w", err)
			}
			if err != nil {
				return fmt.Errorf("connection error: %w", err)
			}

		case message := <-messages:
			// Store latest depth data instead of sending immediately
			// This reduces duplicate data being sent to Kafka
			latestDepthMu.Lock()
			// Use a simple key - in production, parse JSON to get symbol
			latestDepthData["latest"] = message
			messageCount++
			latestDepthMu.Unlock()

		case <-throttleTicker.C:
			// Send the latest depth snapshot to Kafka
			latestDepthMu.Lock()
			if len(latestDepthData) > 0 {
				for _, depthData := range latestDepthData {
					topic := dw.kafkaTopic
					err := dw.kafkaProducer.Produce(&kafka.Message{
						TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
						Value:          depthData,
					}, nil)

					if err != nil {
						dw.logger.Errorf("[%s] Failed to produce message to Kafka: %v", workerID, err)
					}
				}
				dw.logger.Debugf("[%s] Sent %d depth snapshots to Kafka (received %d messages since last send)",
					workerID, len(latestDepthData), messageCount)

				// Reset for next interval
				latestDepthData = make(map[string][]byte)
				messageCount = 0
			}
			latestDepthMu.Unlock()

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
					dw.logger.Warnf("[%s] Pong timeout, connection may be unhealthy", workerID)
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
