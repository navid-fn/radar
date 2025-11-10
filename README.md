# Radar - Cryptocurrency Trading Data Aggregator

A high-performance system for collecting and storing cryptocurrency trading data using Go, Kafka, and ClickHouse.

## Quick Start

1. Copy the environment file:
   ```bash
   cp env.example .env
   ```

2. Start the services:
   ```bash
   docker-compose up -d
   ```

3. Check service status:
   ```bash
   docker-compose ps
   ```

## Services

- **ClickHouse**: `localhost:8123` (HTTP), `localhost:9000` (TCP)
- **Kafka**: `localhost:9092`
- **Zookeeper**: `localhost:2181`

## Configuration

Modify the `.env` file to customize:
- Database credentials
- Port mappings
- Kafka settings
- Zookeeper configuration

## Management Commands

```bash
# Start services
docker-compose up -d

# Stop services
docker-compose down

# View logs
docker-compose logs -f [service_name]

# Reset data (removes volumes)
docker-compose down -v
```

## Default Credentials

- **ClickHouse**: `default/password`

## Testing ClickHouse

```bash
# Test HTTP interface
curl "http://localhost:8123/ping"

# Test with authentication
curl -u default:password "http://localhost:8123/?query=SELECT%20version()"

# Test with POST request
echo "SELECT version()" | curl --data-binary @- "http://localhost:8123/?user=default&password=password"
```

## Kafka Consumer

The consumer reads trading data from Kafka and stores it in ClickHouse with support for multiple concurrent workers and batch processing.

### Configuration

Configure the consumer via environment variables in `.env`:

```env
# Kafka Consumer Configuration
KAFKA_BROKER=localhost:9092
KAFKA_TOPIC=radar_trades
KAFKA_GROUP_ID=clickhouse-consumers-v3
WORKER_COUNT=1
BATCH_SIZE=200
BATCH_TIMEOUT_SECONDS=5

# Database Configuration
CLICKHOUSE_HOST=localhost
CLICKHOUSE_TCP_PORT=9000
CLICKHOUSE_DB=default
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=password
```

### Running the Consumer

1. **Run Migrations** (first time only):
   ```bash
   go run cmd/consumer/main.go -migrate
   ```

2. **Start Consumer**:
   ```bash
   go run cmd/consumer/main.go
   ```

### Features

- **Multiple Workers**: Process messages concurrently using configurable worker pool (`WORKER_COUNT`)
- **Batch Processing**: Accumulate trades and insert in batches for better performance (`BATCH_SIZE`)
- **Timeout-Based Flush**: Automatically flush partial batches after timeout (`BATCH_TIMEOUT_SECONDS`)
- **Graceful Shutdown**: Clean shutdown on SIGTERM/SIGINT with proper resource cleanup and batch flushing
- **Error Handling**: Comprehensive error logging with automatic retry on transient failures
- **Message Commit**: Manual commit after successful batch processing to ensure data integrity
- **Flexible JSON Parsing**: Automatically handles both single trade objects and arrays of trades
- **Data Transformation**: Automatically transforms `crawler.KafkaData` format to `model.Trade` format

### Message Format

The consumer expects Kafka messages in the `crawler.KafkaData` format, which will be automatically transformed to `model.Trade` for database storage.

**Single Trade:**
```json
{
  "ID": "12345",
  "exchange": "binance",
  "symbol": "BTCUSDT",
  "side": "buy",
  "price": 45000.50,
  "volume": 0.1,
  "quantity": 4500.05,
  "time": "2024-01-01T12:00:00Z"
}
```

**Array of Trades:**
```json
[
  {
    "ID": "12345",
    "exchange": "binance",
    "symbol": "BTCUSDT",
    "side": "buy",
    "price": 45000.50,
    "volume": 0.1,
    "quantity": 4500.05,
    "time": "2024-01-01T12:00:00Z"
  },
  {
    "ID": "12346",
    "exchange": "binance",
    "symbol": "ETHUSDT",
    "side": "sell",
    "price": 3000.25,
    "volume": 1.5,
    "quantity": 4500.38,
    "time": "2024-01-01T12:00:01Z"
  }
]
```

**Field Mapping:**
- `ID` → `trade_id`
- `exchange` → `source`
- `symbol` → `symbol`
- `side` → `side`
- `price` → `price`
- `volume` → `base_amount`
- `quantity` → `quote_amount`
- `time` → `event_time`

### Scaling & Optimization

**Worker Count (`WORKER_COUNT`)**:
- **Low traffic**: 1-3 workers
- **Medium traffic**: 3-5 workers
- **High traffic**: 5-10 workers

**Batch Size (`BATCH_SIZE`)**:
- **Small batches** (50-100): Lower latency, more frequent database writes
- **Medium batches** (200-500): Balanced throughput and latency (recommended)
- **Large batches** (500-1000): Higher throughput, increased latency

**Batch Timeout (`BATCH_TIMEOUT_SECONDS`)**:
- **Low timeout** (1-3s): Better for low-traffic scenarios with strict latency requirements
- **Medium timeout** (5-10s): Balanced approach (recommended)
- **High timeout** (15-30s): Maximize batch utilization in high-traffic scenarios

Monitor your system resources and Kafka consumer lag to determine optimal settings. The consumer will:
- Flush when `BATCH_SIZE` is reached (full batch)
- Flush when `BATCH_TIMEOUT_SECONDS` elapses (partial batch)
- Flush remaining trades on graceful shutdown
