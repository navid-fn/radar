package crawler

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"
	"log/slog"

	"github.com/gorilla/websocket"
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
	Config           *WebSocketConfig
	Logger           *slog.Logger
	SendToKafka      func([]byte) error
	SendToKafkaCtx   func(context.Context, []byte) error // Context-aware version
	OnMessage        func(*websocket.Conn, []byte) ([]byte, error) // Optional message transformation, receives conn and message
	OnConnect        func(*websocket.Conn) error                   // Optional connection setup
	OnSubscribe      func(*websocket.Conn, []string) error
	writeMutex       sync.Mutex // Protects all writes (per connection, but shared for safety)
}

// NewBaseWebSocketWorker creates a new BaseWebSocketWorker
func NewBaseWebSocketWorker(config *WebSocketConfig, logger *slog.Logger, sendToKafka func([]byte) error) *BaseWebSocketWorker {
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
	bw.Logger.Info("Starting WebSocket worker", "workerID", workerID, "symbols", len(symbolsChunk))

	reconnectDelay := InitialReconnectDelay
	consecutiveErrors := 0

	for {
		select {
		case <-ctx.Done():
			bw.Logger.Info("Shutting down due to context cancellation", "workerID", workerID)
			return
		default:
			if err := bw.HandleConnection(ctx, workerID, symbolsChunk); err != nil {
				consecutiveErrors++
				bw.Logger.Error("WebSocket error, reconnecting",
					"workerID", workerID,
					"consecutiveErrors", consecutiveErrors,
					"maxErrors", MaxConsecutiveErrors,
					"error", err,
					"reconnectDelay", reconnectDelay)

				// Exponential backoff with max limit
				if reconnectDelay < MaxReconnectDelay {
					reconnectDelay *= 2
					if reconnectDelay > MaxReconnectDelay {
						reconnectDelay = MaxReconnectDelay
					}
				}

				// If too many consecutive errors, wait longer
				if consecutiveErrors >= MaxConsecutiveErrors {
					bw.Logger.Warn("Too many consecutive errors, extending delay", "workerID", workerID)
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

	bw.Logger.Info("Connected to WebSocket", "workerID", workerID)

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
		bw.writeMutex.Lock()
		defer bw.writeMutex.Unlock()
		err := conn.WriteControl(websocket.PongMessage, []byte(message), time.Now().Add(bw.Config.WriteTimeout))
		if err != nil {
			bw.Logger.Error("Failed to send pong", "workerID", workerID, "error", err)
		}
		return err
	})

	if bw.OnSubscribe != nil {
		if err := bw.OnSubscribe(conn, symbolsChunk); err != nil {
			return fmt.Errorf("failed to subscribe: %w", err)
		}
	}

	pingTicker := time.NewTicker(bw.Config.PingInterval)
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
			bw.Logger.Info("Context cancelled, closing connection", "workerID", workerID)
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
				transformed, err := bw.OnMessage(conn, message)
				if err != nil {
					bw.Logger.Error("Failed to transform message", "workerID", workerID, "error", err)
					continue
				}
				if transformed == nil {
					// nil means skip this message (e.g., ping/pong handled)
					continue
				}
				finalMessage = transformed
			} else {
				finalMessage = message
			}

			if len(finalMessage) > 0 {
				// Use context-aware version if available, otherwise fallback
				var err error
				if bw.SendToKafkaCtx != nil {
					err = bw.SendToKafkaCtx(ctx, finalMessage)
				} else {
					err = bw.SendToKafka(finalMessage)
				}
				
				if err != nil {
					// Only log error if context is not cancelled
					if ctx.Err() == nil {
						bw.Logger.Error("Failed to send to Kafka", "workerID", workerID, "error", err)
					}
				}
			}

		case <-pingTicker.C:
			bw.writeMutex.Lock()
			conn.SetWriteDeadline(time.Now().Add(bw.Config.WriteTimeout))
			err := conn.WriteMessage(websocket.PingMessage, []byte{})
			bw.writeMutex.Unlock()
			if err != nil {
				return fmt.Errorf("failed to send ping: %w", err)
			}

			// Wait for pong with timeout
			go func() {
				select {
				case <-pongReceived:
					// Pong received
				case <-time.After(bw.Config.PongTimeout):
					bw.Logger.Warn("Pong timeout, connection may be unhealthy", "workerID", workerID)
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

// SendPong sends a pong message (empty JSON) to the WebSocket connection
func (bw *BaseWebSocketWorker) SendPong(conn *websocket.Conn) error {
	bw.writeMutex.Lock()
	defer bw.writeMutex.Unlock()

	if conn == nil {
		return fmt.Errorf("no connection provided")
	}

	conn.SetWriteDeadline(time.Now().Add(bw.Config.WriteTimeout))
	if err := conn.WriteMessage(websocket.TextMessage, []byte("{}")); err != nil {
		return err
	}

	return nil
}

func (bw *BaseWebSocketWorker) WriteJSON(conn *websocket.Conn, v any) error {
	bw.writeMutex.Lock()
	defer bw.writeMutex.Unlock()

	conn.SetWriteDeadline(time.Now().Add(bw.Config.WriteTimeout))
	return conn.WriteJSON(v)
}

func (bw *BaseWebSocketWorker) WriteMessage(conn *websocket.Conn, messageType int, data []byte) error {
	bw.writeMutex.Lock()
	defer bw.writeMutex.Unlock()

	conn.SetWriteDeadline(time.Now().Add(bw.Config.WriteTimeout))
	return conn.WriteMessage(messageType, data)
}
