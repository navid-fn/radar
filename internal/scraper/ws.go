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

// WebSocket connection timeouts and limits
const (
	wsHandshakeTimeout = 10 * time.Second // Maximum time for WebSocket handshake
	wsReadTimeout      = 60 * time.Second // Maximum time waiting for a message
	wsWriteTimeout     = 10 * time.Second // Maximum time to write a message
	wsPingInterval     = 30 * time.Second // How often to send ping frames
	wsReconnectMin     = 1 * time.Second  // Initial reconnect delay
	wsReconnectMax     = 30 * time.Second // Maximum reconnect delay (exponential backoff cap)
)

// WSConfig holds WebSocket connection settings.
type WSConfig struct {
	// URL is the WebSocket endpoint (e.g., "wss://api.exchange.com/ws")
	URL string

	// Headers are optional HTTP headers for the handshake (e.g., auth tokens)
	Headers http.Header

	// PingDisabled should be true if the server sends pings (we only need pong)
	PingDisabled bool

	// PingInterval overrides the default ping interval (30s). Set to 0 for default.
	PingInterval time.Duration

	// ReadTimeout overrides the default read timeout (60s). Set to 0 for default.
	ReadTimeout time.Duration
}

// WSHandler defines callbacks for WebSocket lifecycle events.
// The driver implements these to handle exchange-specific protocols.
type WSHandler struct {
	// OnConnect is called immediately after WebSocket connection is established.
	// Use this for initial handshakes required by the exchange (e.g., auth).
	// Optional - set to nil if no handshake is needed.
	OnConnect func(conn *websocket.Conn) error

	// OnSubscribe is called to subscribe to market symbols.
	// The symbols slice contains the markets this worker is responsible for.
	// Optional - set to nil if subscription happens in OnConnect.
	OnSubscribe func(conn *websocket.Conn, symbols []string) error

	// OnMessage is called for each incoming WebSocket message.
	// Return serialized protobuf bytes to send to Kafka, or nil to skip.
	// The driver is responsible for parsing exchange-specific message formats.
	OnMessage func(conn *websocket.Conn, msg []byte) ([]byte, error)
}

// WSClient manages a WebSocket connection with automatic reconnection.
// It handles connection lifecycle, ping/pong, and forwards messages to Kafka.
type WSClient struct {
	config  WSConfig
	handler WSHandler
	sender  *Sender
	logger  *slog.Logger
	mu      sync.Mutex // Protects concurrent writes to WebSocket
	// TODO: add symbols to client to check what symbols in
	// websocket got error more
	// symbols  []string
}

// NewWSClient creates a new WebSocket client with the given configuration.
// Default timeouts are applied if not specified in config.
func NewWSClient(config WSConfig, handler WSHandler, sender *Sender, logger *slog.Logger) *WSClient {
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

// Run starts the WebSocket client and blocks until context is cancelled.
// It automatically reconnects on disconnection with exponential backoff.
// Returns nil on graceful shutdown (context cancelled).
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
			reconnectDelay = wsReconnectMin // Reset on successful connection
			continue
		}

		if ctx.Err() != nil {
			return nil // Graceful shutdown
		}

		// TODO: handle this warnings, we see a lot of this in running
		// we can use some metrics for it too, or pass the symbols that we
		// subscribe in whis ws channel to check later
		// c.logger.Warn("WebSocket disconnected, reconnecting",
		// 	"error", err,
		// 	"delay", reconnectDelay,
		// )

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(reconnectDelay):
		}

		// Exponential backoff with cap
		reconnectDelay *= 2
		if reconnectDelay > wsReconnectMax {
			reconnectDelay = wsReconnectMax
		}
	}
}

// connect establishes a single WebSocket connection and runs until disconnection.
func (c *WSClient) connect(ctx context.Context, symbols []string) error {
	dialer := websocket.Dialer{HandshakeTimeout: wsHandshakeTimeout}

	conn, _, err := dialer.DialContext(ctx, c.config.URL, c.config.Headers)
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}
	defer conn.Close()

	// TODO: for ws that constantly disconnect and connect, we see this message
	// a lot and its make our logs dirty. Think about how to solve it later
	// c.logger.Info("WebSocket connected", "url", c.config.URL)

	conn.SetPongHandler(func(string) error { return nil })

	if c.handler.OnConnect != nil {
		if err := c.handler.OnConnect(conn); err != nil {
			return fmt.Errorf("onConnect failed: %w", err)
		}
	}

	if c.handler.OnSubscribe != nil && len(symbols) > 0 {
		if err := c.handler.OnSubscribe(conn, symbols); err != nil {
			return fmt.Errorf("subscribe failed: %w", err)
		}
	}

	return c.readLoop(ctx, conn)
}

// readLoop is the main message processing loop.
// It reads messages, processes them via OnMessage, and sends results to Kafka.
// Also handles sending ping frames if not disabled.
func (c *WSClient) readLoop(ctx context.Context, conn *websocket.Conn) error {
	messages := make(chan []byte, 100)
	readErr := make(chan error, 1)

	// Spawn reader goroutine (blocking reads can't be cancelled)
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

	// Setup ping ticker if needed
	var pingTicker *time.Ticker
	var pingChan <-chan time.Time
	if !c.config.PingDisabled {
		pingTicker = time.NewTicker(c.config.PingInterval)
		defer pingTicker.Stop()
		pingChan = pingTicker.C
	}

	for {
		select {
		case <-ctx.Done():
			return nil

		case err := <-readErr:
			// TODO: add metrics?
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
					// TODO: add metrics?
					c.logger.Error("Kafka send failed", "error", err)
				}
			}

		case <-pingChan:
			c.mu.Lock()
			conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
			err := conn.WriteMessage(websocket.PingMessage, nil)
			c.mu.Unlock()
			if err != nil {
				// TODO: add metrics?
				return fmt.Errorf("ping failed: %w", err)
			}
		}
	}
}

// WriteJSON sends a JSON message to the WebSocket connection.
// Thread-safe - can be called from OnMessage handler.
func (c *WSClient) WriteJSON(conn *websocket.Conn, v any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
	return conn.WriteJSON(v)
}

// RunWorkers starts multiple WebSocket workers, one per symbol chunk.
// Use this when an exchange limits subscriptions per connection.
// Workers are started with 2-second stagger to avoid rate limiting.
func RunWorkers(
	ctx context.Context,
	symbolChunks [][]string,
	workerName string,
	createClient func() *WSClient,
	logger *slog.Logger,
) {
	var wg sync.WaitGroup

	logger.Info("starting workers", "workers", len(symbolChunks))

	for i, chunk := range symbolChunks {
		wg.Add(1)
		go func(idx int, symbols []string) {
			defer wg.Done()

			client := createClient()

			if err := client.Run(ctx, symbols); err != nil {
				logger.Error("worker stopped", "error", err)
			}
		}(i, chunk)

		// Stagger worker starts to avoid rate limiting
		if i < len(symbolChunks)-1 {
			time.Sleep(2 * time.Second)
		}
	}

	wg.Wait()
}
