package crawler

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

// WebSocketConfig holds WebSocket-specific configuration
type WebSocketConfig struct {
	URL              string
	HandshakeTimeout time.Duration
	ReadTimeout      time.Duration
	WriteTimeout     time.Duration
	PingInterval     time.Duration
	PongTimeout      time.Duration
}

// DefaultWebSocketConfig returns a default WebSocket configuration
func DefaultWebSocketConfig(wsURL string) *WebSocketConfig {
	return &WebSocketConfig{
		URL:              wsURL,
		HandshakeTimeout: HandshakeTimeout,
		ReadTimeout:      ReadTimeout,
		WriteTimeout:     WriteTimeout,
		PingInterval:     PingInterval,
		PongTimeout:      PongTimeout,
	}
}

// BaseWebSocketWorker provides common WebSocket functionality
type BaseWebSocketWorker struct {
	Config      *WebSocketConfig
	Logger      *logrus.Logger
	SendToKafka func([]byte) error
	OnMessage   func([]byte) ([]byte, error) // Optional message transformation
	OnConnect   func(*websocket.Conn) error  // Optional connection setup
	OnSubscribe func(*websocket.Conn, []string) error
}

// NewBaseWebSocketWorker creates a new BaseWebSocketWorker
func NewBaseWebSocketWorker(config *WebSocketConfig, logger *logrus.Logger, sendToKafka func([]byte) error) *BaseWebSocketWorker {
	return &BaseWebSocketWorker{
		Config:      config,
		Logger:      logger,
		SendToKafka: sendToKafka,
	}
}

// RunWorker starts a WebSocket worker for a chunk of symbols
func (bw *BaseWebSocketWorker) RunWorker(
	ctx context.Context,
	symbolsChunk []string,
	wg *sync.WaitGroup,
	workerPrefix string,
) {
	defer wg.Done()

	workerID := fmt.Sprintf("%s-%s", workerPrefix, symbolsChunk[0])
	bw.Logger.Infof("[%s] Starting for %d symbols", workerID, len(symbolsChunk))

	reconnectDelay := InitialReconnectDelay
	consecutiveErrors := 0

	for {
		select {
		case <-ctx.Done():
			bw.Logger.Infof("[%s] Shutting down due to context cancellation", workerID)
			return
		default:
			if err := bw.HandleConnection(ctx, workerID, symbolsChunk); err != nil {
				consecutiveErrors++
				bw.Logger.Errorf("[%s] WebSocket error (%d/%d): %v. Reconnecting in %v...",
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
					bw.Logger.Warnf("[%s] Too many consecutive errors, extending delay", workerID)
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

// HandleConnection manages a single WebSocket connection lifecycle
func (bw *BaseWebSocketWorker) HandleConnection(ctx context.Context, workerID string, symbolsChunk []string) error {
	u, err := url.Parse(bw.Config.URL)
	if err != nil {
		return fmt.Errorf("invalid WebSocket URL: %w", err)
	}

	dialer := websocket.Dialer{
		HandshakeTimeout: bw.Config.HandshakeTimeout,
	}

	conn, _, err := dialer.Dial(u.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to connect to WebSocket: %w", err)
	}
	defer conn.Close()

	bw.Logger.Infof("[%s] Connected to WebSocket", workerID)

	// Create context for this connection
	connCtx, connCancel := context.WithCancel(ctx)
	defer connCancel()

	// Optional: Execute custom connection logic
	if bw.OnConnect != nil {
		if err := bw.OnConnect(conn); err != nil {
			return fmt.Errorf("failed to execute OnConnect: %w", err)
		}
	}

	// Setup ping/pong handlers
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
		err := conn.WriteControl(websocket.PongMessage, []byte(message), time.Now().Add(bw.Config.WriteTimeout))
		if err != nil {
			bw.Logger.Errorf("[%s] Failed to send pong: %v", workerID, err)
		}
		return err
	})

	// Subscribe to symbols
	if bw.OnSubscribe != nil {
		if err := bw.OnSubscribe(conn, symbolsChunk); err != nil {
			return fmt.Errorf("failed to subscribe: %w", err)
		}
	}

	// Setup tickers
	pingTicker := time.NewTicker(bw.Config.PingInterval)
	defer pingTicker.Stop()

	healthTicker := time.NewTicker(HealthCheckInterval)
	defer healthTicker.Stop()

	// Setup channels
	readErrors := make(chan error, 1)
	messages := make(chan []byte, 100)

	// Start message reader
	go func() {
		defer close(messages)
		defer close(readErrors)

		for {
			select {
			case <-connCtx.Done():
				return
			default:
				conn.SetReadDeadline(time.Now().Add(bw.Config.ReadTimeout))
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

	// Main event loop
	for {
		select {
		case <-ctx.Done():
			bw.Logger.Infof("[%s] Context cancelled, closing connection", workerID)
			return nil

		case err := <-readErrors:
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure, websocket.CloseNormalClosure) {
				return fmt.Errorf("WebSocket read error: %w", err)
			}
			if err != nil {
				return fmt.Errorf("connection error: %w", err)
			}

		case message := <-messages:
			// Optional message transformation
			var finalMessage []byte
			if bw.OnMessage != nil {
				transformed, err := bw.OnMessage(message)
				if err != nil {
					bw.Logger.Errorf("[%s] Failed to transform message: %v", workerID, err)
					continue
				}
				if transformed == nil {
					// nil means skip this message
					continue
				}
				finalMessage = transformed
			} else {
				finalMessage = message
			}

			// Send to Kafka
			if err := bw.SendToKafka(finalMessage); err != nil {
				bw.Logger.Errorf("[%s] Failed to send to Kafka: %v", workerID, err)
			}

		case <-pingTicker.C:
			conn.SetWriteDeadline(time.Now().Add(bw.Config.WriteTimeout))
			if err := conn.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
				return fmt.Errorf("failed to send ping: %w", err)
			}

			// Wait for pong with timeout
			go func() {
				select {
				case <-pongReceived:
					// Pong received
				case <-time.After(bw.Config.PongTimeout):
					bw.Logger.Warnf("[%s] Pong timeout, connection may be unhealthy", workerID)
				case <-connCtx.Done():
					return
				}
			}()

		case <-healthTicker.C:
			timeSinceLastPong := time.Since(lastPongTime)
			if timeSinceLastPong > (bw.Config.PingInterval + bw.Config.PongTimeout) {
				return fmt.Errorf("connection appears unhealthy, last pong was %v ago", timeSinceLastPong)
			}
		}
	}
}


