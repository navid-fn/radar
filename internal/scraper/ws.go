package scraper

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// WebSocket timeouts
const (
	wsHandshakeTimeout = 10 * time.Second
	wsReadTimeout      = 60 * time.Second
	wsWriteTimeout     = 10 * time.Second
	wsPingInterval     = 30 * time.Second
	wsReconnectMin     = 1 * time.Second
	wsReconnectMax     = 30 * time.Second
)

// WSConfig holds WebSocket connection settings
type WSConfig struct {
	URL           string
	Headers       http.Header
	PingDisabled  bool          // Set true if server sends pings
	PingInterval  time.Duration // Custom ping interval (0 = default 30s)
	ReadTimeout   time.Duration // Custom read timeout (0 = default 60s)
}

// WSHandler defines callbacks for WebSocket events
type WSHandler struct {
	// OnConnect is called after connection is established (optional)
	OnConnect func(conn *websocket.Conn) error

	// OnSubscribe is called to subscribe to symbols (required for multi-symbol)
	OnSubscribe func(conn *websocket.Conn, symbols []string) error

	// OnMessage processes incoming messages and returns data to send to Kafka
	// Return nil to skip sending, return data to send to Kafka
	OnMessage func(conn *websocket.Conn, msg []byte) ([]byte, error)
}

// WSClient manages a WebSocket connection with auto-reconnect
type WSClient struct {
	config  WSConfig
	handler WSHandler
	sender  *Sender
	logger  *slog.Logger
	mu      sync.Mutex
}

// NewWSClient creates a new WebSocket client
func NewWSClient(config WSConfig, handler WSHandler, sender *Sender, logger *slog.Logger) *WSClient {
	// Apply defaults
	if config.PingInterval == 0 {
		config.PingInterval = wsPingInterval
	}
	if config.ReadTimeout == 0 {
		config.ReadTimeout = wsReadTimeout
	}

	return &WSClient{
		config:  config,
		handler: handler,
		sender:  sender,
		logger:  logger,
	}
}

// Run starts the WebSocket connection with auto-reconnect
// Blocks until context is cancelled
func (c *WSClient) Run(ctx context.Context, symbols []string) error {
	reconnectDelay := wsReconnectMin

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		err := c.connect(ctx, symbols)
		if err == nil {
			reconnectDelay = wsReconnectMin
			continue
		}

		// Don't log if shutting down
		if ctx.Err() != nil {
			return nil
		}

		c.logger.Warn("WebSocket disconnected, reconnecting",
			"error", err,
			"delay", reconnectDelay,
		)

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(reconnectDelay):
		}

		// Exponential backoff
		reconnectDelay *= 2
		if reconnectDelay > wsReconnectMax {
			reconnectDelay = wsReconnectMax
		}
	}
}

// connect establishes a single WebSocket connection
func (c *WSClient) connect(ctx context.Context, symbols []string) error {
	dialer := websocket.Dialer{HandshakeTimeout: wsHandshakeTimeout}

	conn, _, err := dialer.DialContext(ctx, c.config.URL, c.config.Headers)
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}
	defer conn.Close()

	c.logger.Info("WebSocket connected", "url", c.config.URL)

	// Setup ping/pong
	conn.SetPongHandler(func(string) error { return nil })

	// OnConnect callback
	if c.handler.OnConnect != nil {
		if err := c.handler.OnConnect(conn); err != nil {
			return fmt.Errorf("onConnect failed: %w", err)
		}
	}

	// Subscribe to symbols
	if c.handler.OnSubscribe != nil && len(symbols) > 0 {
		if err := c.handler.OnSubscribe(conn, symbols); err != nil {
			return fmt.Errorf("subscribe failed: %w", err)
		}
	}

	return c.readLoop(ctx, conn)
}

// readLoop handles reading messages and sending pings
func (c *WSClient) readLoop(ctx context.Context, conn *websocket.Conn) error {
	// Message channel
	messages := make(chan []byte, 100)
	readErr := make(chan error, 1)

	// Reader goroutine
	go func() {
		defer close(messages)
		for {
			conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout))
			_, msg, err := conn.ReadMessage()
			if err != nil {
				select {
				case readErr <- err:
				default:
				}
				return
			}
			select {
			case messages <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Ping ticker (if enabled)
	var pingTicker *time.Ticker
	var pingChan <-chan time.Time
	if !c.config.PingDisabled {
		pingTicker = time.NewTicker(c.config.PingInterval)
		defer pingTicker.Stop()
		pingChan = pingTicker.C
	}

	// Main loop
	for {
		select {
		case <-ctx.Done():
			return nil

		case err := <-readErr:
			return fmt.Errorf("read error: %w", err)

		case msg := <-messages:
			if c.handler.OnMessage == nil {
				continue
			}

			data, err := c.handler.OnMessage(conn, msg)
			if err != nil {
				c.logger.Debug("OnMessage error", "error", err)
				continue
			}

			if data != nil {
				if err := c.sender.Send(ctx, data); err != nil {
					c.logger.Error("Kafka send failed", "error", err)
				}
			}

		case <-pingChan:
			c.mu.Lock()
			conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
			err := conn.WriteMessage(websocket.PingMessage, nil)
			c.mu.Unlock()
			if err != nil {
				return fmt.Errorf("ping failed: %w", err)
			}
		}
	}
}

// WriteJSON sends a JSON message (thread-safe)
func (c *WSClient) WriteJSON(conn *websocket.Conn, v any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
	return conn.WriteJSON(v)
}

// RunWorkers runs multiple WebSocket workers for chunked symbols
func RunWorkers(
	ctx context.Context,
	symbolChunks [][]string,
	workerName string,
	createClient func() *WSClient,
	logger *slog.Logger,
) {
	var wg sync.WaitGroup

	for i, chunk := range symbolChunks {
		wg.Add(1)
		go func(idx int, symbols []string) {
			defer wg.Done()

			client := createClient()
			workerID := fmt.Sprintf("%s-%d", workerName, idx)
			logger.Info("Starting worker", "id", workerID, "symbols", len(symbols))

			if err := client.Run(ctx, symbols); err != nil {
				logger.Error("Worker stopped", "id", workerID, "error", err)
			}
		}(i, chunk)

		// Stagger worker starts
		if i < len(symbolChunks)-1 {
			time.Sleep(2 * time.Second)
		}
	}

	wg.Wait()
}



