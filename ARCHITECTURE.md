# Radar Crawler Architecture

## Overview

This document describes the refactored architecture of the Radar cryptocurrency exchange crawler. The system is designed to collect real-time trade and order book data from multiple Iranian cryptocurrency exchanges using both WebSocket and HTTP/REST API protocols.

## Directory Structure

```
radar/
├── cmd/
│   └── crawler/
│       └── main.go                 # Main entry point for all crawlers
├── internal/
│   ├── crawler/                    # Common crawler infrastructure
│   │   ├── interface.go            # Core interfaces and types
│   │   ├── common.go               # Shared utilities and configuration
│   │   ├── websocket.go            # Base WebSocket worker implementation
│   │   └── http.go                 # Base HTTP/API worker implementation
│   └── drivers/                    # Exchange-specific implementations
│       ├── bitpin/
│       │   └── bitpin.go
│       ├── nobitex/
│       │   └── nobitex.go
│       ├── ramzinex/
│       │   └── ramzinex.go
│       ├── tabdeal/
│       │   └── tabdeal.go
│       └── wallex/
│           └── wallex.go
└── go.mod                          # Go module definition
```

## Architecture Principles

### 1. DRY (Don't Repeat Yourself)
- Common WebSocket connection handling is abstracted into `BaseWebSocketWorker`
- Common HTTP polling logic is abstracted into `BaseHTTPWorker`
- Kafka producer initialization and message delivery are handled by `BaseCrawler`
- Configuration and utilities are centralized in `internal/crawler/common.go`

### 2. Interface-Based Design
- All crawlers implement the `Crawler` interface
- Exchange-specific behavior is customized through callbacks and composition
- Easy to add new exchanges by implementing the interface

### 3. Separation of Concerns
- **Protocol Layer**: WebSocket and HTTP workers handle network communication
- **Exchange Layer**: Exchange drivers handle market-specific logic
- **Data Layer**: Kafka producers handle data persistence
- **Control Layer**: Context-based graceful shutdown

## Core Components

### 1. Interfaces (`internal/crawler/interface.go`)

#### `Crawler` Interface
```go
type Crawler interface {
    Run(ctx context.Context) error
    GetName() string
}
```

All exchange crawlers implement this interface, allowing them to be used interchangeably.

#### `Config` Struct
```go
type Config struct {
    ExchangeName         string
    KafkaBroker          string
    KafkaTopic           string
    Logger               *logrus.Logger
    MaxSubsPerConnection int
}
```

Holds common configuration that can be customized per exchange.

### 2. Base Crawler (`internal/crawler/common.go`)

Provides common functionality:
- Kafka producer initialization and management
- Delivery report handling
- Message sending to Kafka
- Logger creation
- Configuration management

### 3. WebSocket Worker (`internal/crawler/websocket.go`)

`BaseWebSocketWorker` handles:
- Connection establishment and management
- Automatic reconnection with exponential backoff
- Ping/pong health monitoring
- Message reading and processing
- Graceful shutdown

**Customization Points** (callbacks):
- `OnConnect`: Custom connection initialization
- `OnSubscribe`: Exchange-specific subscription logic
- `OnMessage`: Message transformation/filtering

### 4. HTTP Worker (`internal/crawler/http.go`)

`BaseHTTPWorker` provides:
- Rate-limited HTTP polling
- Trade deduplication tracking (hash-based and ID-based)
- Adaptive polling intervals

**Utilities**:
- `TradeTracker`: Hash-based duplicate detection
- `IDTracker`: ID-based tracking with adaptive sleep

## Exchange Implementations

### 1. Bitpin (WebSocket)
- **Protocol**: WebSocket
- **Features**: Real-time trade data
- **Customization**: Filters PONG messages, uses custom subscription format

### 2. Nobitex (HTTP/REST)
- **Protocol**: HTTP polling with rate limiting
- **Features**: Dynamic rate limiting based on symbol count, hash-based deduplication
- **Rate Limit**: 60 requests/minute (uses 98% = 58.8/min)

### 3. Ramzinex (WebSocket with Centrifuge)
- **Protocol**: WebSocket (Centrifuge protocol)
- **Features**: Trade data with duplicate filtering
- **Customization**: Sends connect message, maps pair IDs to names

### 4. Tabdeal (HTTP with ID tracking)
- **Protocol**: HTTP polling
- **Features**: ID-based duplicate prevention, adaptive polling
- **Optimization**: Increases polling interval when no new trades

### 5. Wallex (WebSocket - Dual Mode)
- **Protocol**: WebSocket
- **Modes**: 
  - `trades`: Real-time trade data
  - `depth`: Order book depth (throttled)
- **Features**: Throttled depth updates to reduce duplicate data

## Data Flow

### WebSocket-Based Exchanges (Bitpin, Ramzinex, Wallex)

```
Exchange WebSocket Server
         ↓
   WebSocket Connection
         ↓
   BaseWebSocketWorker
         ↓
   OnMessage (optional transformation)
         ↓
   Kafka Producer
         ↓
   Kafka Topic (radar_trades / radar_depths)
```

### HTTP-Based Exchanges (Nobitex, Tabdeal)

```
Exchange REST API
         ↓
   HTTP Request (rate-limited)
         ↓
   Response Processing
         ↓
   Duplicate Filtering
         ↓
   Kafka Producer
         ↓
   Kafka Topic (radar_trades)
```

## Usage

### Building

```bash
# Build the crawler
go build -o bin/crawler ./cmd/crawler
```

### Running

```bash
# Run Bitpin crawler
./bin/crawler -exchange bitpin

# Run Nobitex crawler
./bin/crawler -exchange nobitex

# Run Wallex in trades mode
./bin/crawler -exchange wallex -mode trades

# Run Wallex in depth mode with custom throttle
./bin/crawler -exchange wallex -mode depth -throttle 10
```

### Environment Variables

```bash
# Kafka broker address (default: localhost:9092)
export KAFKA_BROKER=kafka:9092

# Optional: Data directory for persistence
export DATA_DIR=./data
```

## Configuration

### Connection Parameters

All exchanges use these default constants (can be customized):

```go
InitialReconnectDelay = 1 * time.Second
MaxReconnectDelay     = 30 * time.Second
HandshakeTimeout      = 10 * time.Second
ReadTimeout           = 60 * time.Second
WriteTimeout          = 10 * time.Second
PingInterval          = 30 * time.Second
PongTimeout           = 10 * time.Second
MaxConsecutiveErrors  = 5
HealthCheckInterval   = 5 * time.Second
```

### Exchange-Specific Limits

| Exchange  | Max Subscriptions/Connection | Rate Limit        |
|-----------|------------------------------|-------------------|
| Bitpin    | 40                           | N/A (WebSocket)   |
| Nobitex   | N/A                          | 60 req/min        |
| Ramzinex  | 20                           | N/A (WebSocket)   |
| Tabdeal   | N/A                          | Adaptive polling  |
| Wallex    | 20                           | N/A (WebSocket)   |

## Adding a New Exchange

To add a new exchange crawler:

1. **Create driver file**: `internal/drivers/{exchange}/{exchange}.go`

2. **Define crawler struct**:
```go
type NewExchangeCrawler struct {
    *crawler.BaseCrawler
    // Add exchange-specific fields
}
```

3. **Implement constructor**:
```go
func NewNewExchangeCrawler() *NewExchangeCrawler {
    config := crawler.NewConfig("newexchange", "radar_trades", 20)
    baseCrawler := crawler.NewBaseCrawler(config)
    
    return &NewExchangeCrawler{
        BaseCrawler: baseCrawler,
    }
}
```

4. **Implement Crawler interface**:
```go
func (nc *NewExchangeCrawler) GetName() string {
    return "newexchange"
}

func (nc *NewExchangeCrawler) Run(ctx context.Context) error {
    // Implementation
}
```

5. **For WebSocket**: Use `BaseWebSocketWorker` and customize with callbacks

6. **For HTTP**: Use `BaseHTTPWorker` or implement custom polling logic

7. **Register in main.go**: Add case to switch statement in `cmd/crawler/main.go`

## Error Handling

### WebSocket Connections
- Automatic reconnection with exponential backoff
- Maximum consecutive errors tracking
- Health monitoring via ping/pong
- Graceful degradation on errors

### HTTP Requests
- Rate limiting to respect API limits
- Retry logic with delays
- Duplicate detection to prevent data loss
- Adaptive polling based on activity

## Graceful Shutdown

All crawlers support graceful shutdown:
- SIGINT/SIGTERM signal handling
- Context cancellation propagation
- Worker cleanup via WaitGroup
- Kafka producer flushing

## Performance Considerations

### WebSocket
- Multiple connections for high symbol counts
- Chunking to stay within subscription limits
- Buffered channels to prevent blocking

### HTTP
- Dynamic rate limiting based on symbol count
- Deduplication to reduce downstream processing
- Adaptive polling to optimize API usage

## Monitoring and Logging

All crawlers use structured logging (logrus):
- Worker identification in logs
- Connection state changes
- Error tracking with context
- Performance metrics (rate limits, polling intervals)

## Future Improvements

1. **Metrics**: Add Prometheus metrics for monitoring
2. **Persistence**: Add optional local persistence for fault tolerance
3. **Configuration**: Support external configuration files
4. **Health Checks**: HTTP endpoint for health monitoring
5. **Dynamic Reconfiguration**: Support for runtime configuration changes
6. **Testing**: Add comprehensive unit and integration tests

## Dependencies

- `github.com/confluentinc/confluent-kafka-go/v2` - Kafka client
- `github.com/gorilla/websocket` - WebSocket client
- `github.com/sirupsen/logrus` - Structured logging
- `golang.org/x/time/rate` - Rate limiting

## License

[Your License Here]

