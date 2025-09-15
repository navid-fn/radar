# Radar Go - WebSocket to Kafka Producer

This is a Go rewrite of the original Python WebSocket-to-Kafka producer for Wallex trading data.

## Features

- Fetches trading markets from Wallex API
- Establishes multiple WebSocket connections (chunked by 40 subscriptions per connection)
- Subscribes to trade channels for all markets
- Forwards all WebSocket messages to Kafka topic `radar_trades`
- Automatic reconnection with exponential backoff
- Graceful shutdown handling
- Structured logging with logrus

## Configuration

The application uses environment variables for configuration:

- `KAFKA_BROKER`: Kafka broker address (default: `localhost:9092`)

## Running Locally

### Prerequisites

- Go 1.21 or higher
- Kafka running on localhost:9092 (or configure `KAFKA_BROKER`)

### Build and Run

```bash
# Install dependencies
go mod tidy

# Build the application
go build -o radar-go main.go

# Run the application
./radar-go

# Or run directly
go run main.go
```

## Running with Docker

### Build and run the Go service

```bash
# Build and start the service with docker-compose
docker-compose up --build radar-go

# Or build and run standalone
docker build -f Dockerfile.go -t radar-go .
docker run --network radar_dev-network -e KAFKA_BROKER="kafka:29092" radar-go
```

### Full stack with docker-compose

```bash
# Start all services (Kafka, Zookeeper, ClickHouse, and Radar-Go)
docker-compose up -d

# View logs
docker-compose logs -f radar-go

# Stop services
docker-compose down
```

## Architecture

The Go application follows the same architecture as the Python version:

1. **Market Fetching**: Fetches all available trading pairs from Wallex API
2. **Connection Pooling**: Divides markets into chunks of 40 symbols per WebSocket connection
3. **Worker Pool**: Creates a goroutine for each WebSocket connection
4. **Message Forwarding**: All received messages are forwarded to Kafka topic `radar_trades`
5. **Error Handling**: Automatic reconnection on WebSocket failures

## Performance Improvements

Compared to the Python version, the Go implementation offers:

- Lower memory footprint
- Better concurrent handling of multiple WebSocket connections
- Faster JSON processing
- More efficient Kafka producer with better batching
- Built-in structured logging

## Monitoring

The application uses structured logging with the following log levels:
- `INFO`: General application flow and connection status
- `ERROR`: Connection errors and Kafka publishing failures
- `FATAL`: Critical errors that cause application shutdown

## Graceful Shutdown

The application handles `SIGINT` and `SIGTERM` signals for graceful shutdown:
1. Stops accepting new connections
2. Closes existing WebSocket connections
3. Flushes remaining Kafka messages
4. Exits cleanly
