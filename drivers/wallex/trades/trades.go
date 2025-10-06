package trades

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
)

// TradesWorker manages WebSocket connection for trades
type TradesWorker struct {
	kafkaProducer *kafka.Producer
	logger        *logrus.Logger
	kafkaTopic    string
}

// NewTradesWorker creates a new TradesWorker instance
func NewTradesWorker(kafkaProducer *kafka.Producer, logger *logrus.Logger, kafkaTopic string) *TradesWorker {
	return &TradesWorker{
		kafkaProducer: kafkaProducer,
		logger:        logger,
		kafkaTopic:    kafkaTopic,
	}
}

// Run starts the trades worker for a chunk of symbols
func (tw *TradesWorker) Run(ctx context.Context, symbolsChunk []string, wg *sync.WaitGroup) {
	defer wg.Done()

	workerID := fmt.Sprintf("TradesWorker-%s", symbolsChunk[0])
	tw.logger.Infof("[%s] Starting for %d symbols", workerID, len(symbolsChunk))

	for {
		select {
		case <-ctx.Done():
			tw.logger.Infof("[%s] Shutting down due to context cancellation", workerID)
			return
		default:
			if err := tw.handleWebSocketConnection(ctx, workerID, symbolsChunk); err != nil {
				tw.logger.Errorf("[%s] WebSocket error: %v. Reconnecting in 5s...", workerID, err)

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

// handleWebSocketConnection handles a single WebSocket connection lifecycle
func (tw *TradesWorker) handleWebSocketConnection(ctx context.Context, workerID string, symbolsChunk []string) error {
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

	tw.logger.Infof("[%s] Connected to WebSocket", workerID)

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
			tw.logger.Errorf("[%s] Failed to send pong: %v", workerID, err)
		}
		return err
	})

	// Subscribe to all symbols in this chunk for trades
	tw.logger.Infof("[%s] Subscribing to %d markets for trades...", workerID, len(symbolsChunk))
	for _, symbol := range symbolsChunk {
		conn.SetWriteDeadline(time.Now().Add(WriteTimeout))
		subscriptionMsg := []any{"subscribe", map[string]string{"channel": fmt.Sprintf("%s@trade", symbol)}}
		if err := conn.WriteJSON(subscriptionMsg); err != nil {
			return fmt.Errorf("failed to send subscription message: %w", err)
		}
	}
	tw.logger.Infof("[%s] Subscriptions sent", workerID)

	// Start ping routine
	pingTicker := time.NewTicker(PingInterval)
	defer pingTicker.Stop()

	// Start health check routine
	healthTicker := time.NewTicker(HealthCheckInterval)
	defer healthTicker.Stop()

	// Channel for read errors
	readErrors := make(chan error, 1)
	messages := make(chan []byte, 100)

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

	tw.logger.Infof("[%s] Starting message processing loop", workerID)

	// Main event loop
	for {
		select {
		case <-ctx.Done():
			tw.logger.Infof("[%s] Context cancelled, closing connection", workerID)
			return nil

		case err := <-readErrors:
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure, websocket.CloseNormalClosure) {
				return fmt.Errorf("WebSocket read error: %w", err)
			}
			if err != nil {
				return fmt.Errorf("connection error: %w", err)
			}

		case message := <-messages:
			// Send message to Kafka immediately (trades are real-time)
			topic := tw.kafkaTopic
			err = tw.kafkaProducer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          message,
			}, nil)

			if err != nil {
				tw.logger.Errorf("[%s] Failed to produce message to Kafka: %v", workerID, err)
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
					tw.logger.Warnf("[%s] Pong timeout, connection may be unhealthy", workerID)
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
