# Radar - Multi-Worker Kafka Consumer Deployment Guide

## Architecture Overview

This setup implements a production-ready data pipeline with:
- **2 Wallex WebSocket Producers** (trades + depth)
- **5 Multi-Worker Kafka Consumers** (3 for trades, 2 for depth)
- **Kafka** for message streaming
- **ClickHouse** for time-series data storage

## Services

### Data Producers
- `wallex-trades`: Real-time trades WebSocket → Kafka (`radar_trades` topic)
- `wallex-depth`: Throttled orderbook depth (7s) → Kafka (`radar_depths` topic)

### Data Consumers (Multi-Worker)
- `trades-consumer-1/2/3`: Parallel workers consuming from `radar_trades`
  - All share same consumer group → automatic partition load balancing
  - Each worker handles different Kafka partitions
  - Inserts into ClickHouse `trades.trades_master` table
  
- `depth-consumer-1/2`: Parallel workers consuming from `radar_depths`
  - Shares consumer group for load distribution
  - Inserts into ClickHouse `trades.orderbook_depth` table (array format)

### Infrastructure
- `kafka`: Message broker (port 9092)
- `zookeeper`: Kafka coordination (port 2181)
- `clickhouse`: Analytical database (HTTP: 8123, Native: 9000)

## Multi-Worker Benefits

### Why Multiple Workers?

1. **Parallel Processing**: Each worker processes different Kafka partitions simultaneously
2. **Higher Throughput**: 3 trades workers can process 3x more messages than 1 worker
3. **Fault Tolerance**: If one worker crashes, others continue processing
4. **Load Balancing**: Kafka automatically distributes partitions across consumer group members
5. **Scalability**: Easy to add/remove workers by adjusting service count

### How It Works

```
Kafka Topic: radar_trades (3 partitions)
├─ Partition 0 → trades-consumer-1
├─ Partition 1 → trades-consumer-2
└─ Partition 2 → trades-consumer-3

All consumers share group: "clickhouse-trades-consumers-v1"
```

## Quick Start

### 1. Configuration

```bash
# Copy environment file
cp env.example .env

# Edit as needed (optional - defaults work for local)
nano .env
```

### 2. Start All Services

```bash
# Start infrastructure (Kafka + ClickHouse)
docker-compose up -d kafka clickhouse

# Wait 30s for Kafka to be ready
sleep 30

# Start producers
docker-compose up -d wallex-trades wallex-depth

# Start consumers (multi-worker)
docker-compose up -d trades-consumer-1 trades-consumer-2 trades-consumer-3
docker-compose up -d depth-consumer-1 depth-consumer-2
```

### 3. Monitor

```bash
# View all logs
docker-compose logs -f

# View specific service
docker-compose logs -f trades-consumer-1

# Check service health
docker-compose ps

# View consumer group lag
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group clickhouse-trades-consumers-v1
```

## Scaling Workers

### Add More Trades Workers

```bash
# In docker-compose.yml, duplicate a trades-consumer section:
trades-consumer-4:
  # ... same config as trades-consumer-1
  container_name: trades-consumer-4

# Start it
docker-compose up -d trades-consumer-4
```

**Important**: Total workers should ≤ number of Kafka partitions for best distribution.
Currently configured: 3 partitions per topic (see `KAFKA_NUM_PARTITIONS` in `.env`)

### Reduce Workers

```bash
# Stop and remove a worker
docker-compose stop trades-consumer-3
docker-compose rm -f trades-consumer-3

# Remaining workers auto-rebalance partitions
```

## Performance Tuning

### Batch Size
Controls how many records to accumulate before writing to ClickHouse:
```yaml
environment:
  - BATCH_SIZE=500  # Higher = more throughput, more memory
```

### Batch Timeout
Maximum seconds to wait before flushing partial batch:
```yaml
environment:
  - BATCH_TIMEOUT_SECONDS=5  # Lower = more real-time, more writes
```

### Kafka Partitions
More partitions = more parallelism:
```bash
# In .env
KAFKA_NUM_PARTITIONS=6  # Allows up to 6 workers per consumer group
```

## Verify Data Flow

### 1. Check Producers
```bash
# Producers should be sending messages
docker-compose logs wallex-trades | grep "Subscriptions sent"
docker-compose logs wallex-depth | grep "Starting message processing"
```

### 2. Check Kafka Topics
```bash
# List topics
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

# Check message count
docker exec -it kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic radar_trades
```

### 3. Check Consumers
```bash
# Consumers should be inserting batches
docker-compose logs trades-consumer-1 | grep "Inserted"
docker-compose logs depth-consumer-1 | grep "Inserted"
```

### 4. Query ClickHouse
```bash
# Access ClickHouse client
docker exec -it clickhouse clickhouse-client

# Check trades
SELECT count(*) FROM trades.trades_master;
SELECT * FROM trades.trades_master LIMIT 10;

# Check depth
SELECT count(*) FROM trades.orderbook_depth;
SELECT * FROM trades.orderbook_depth LIMIT 10;
```

## Troubleshooting

### Consumers Not Starting
```bash
# Check if ClickHouse is ready
docker exec -it clickhouse clickhouse-client --query "SELECT 1"

# Check if Kafka is ready
docker exec -it kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

### No Data in ClickHouse
```bash
# Verify tables exist
docker exec -it clickhouse clickhouse-client --query "SHOW TABLES FROM trades"

# Check consumer group offset
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group clickhouse-trades-consumers-v1
```

### High Consumer Lag
- Add more workers (up to partition count)
- Increase `BATCH_SIZE` for higher throughput
- Check ClickHouse write performance

### Duplicate Data
- All workers in same group use same `KAFKA_GROUP_ID`
- ReplacingMergeTree engine deduplicates on `(source, trade_id)`

## Production Recommendations

### Resource Limits
Add to docker-compose.yml:
```yaml
deploy:
  resources:
    limits:
      cpus: '1.0'
      memory: 512M
    reservations:
      cpus: '0.5'
      memory: 256M
```

### Monitoring
- Use Prometheus + Grafana for metrics
- Monitor consumer lag with Kafka Manager
- Set up alerts for failed health checks

### Data Retention
ClickHouse tables have 90-day TTL (configurable):
```sql
-- Modify in consumer setup_database()
TTL toDateTime(timestamp) + INTERVAL 90 DAY
```

## Advanced: Custom Workers

### Run Consumer Locally (Development)
```bash
# Set environment variables
export KAFKA_BROKER=localhost:9092
export CLICKHOUSE_HOST=localhost
export KAFKA_TOPIC=radar_trades
export KAFKA_GROUP_ID=my-dev-group

# Run directly
python kafka/consumer.py

# Or with virtualenv
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python kafka/consumer.py
```

### Initialize Database Schema
```bash
# Drop and recreate (WARNING: deletes all data)
docker exec -it trades-consumer-1 python kafka/consumer.py --drop_database
docker exec -it depth-consumer-1 python kafka/consumer_depth_improved.py --drop_database
```

## Network Architecture

```
┌─────────────────┐     ┌─────────────────┐
│ wallex-trades   │────▶│  Kafka Broker   │
│ wallex-depth    │     │  (dev-network)  │
└─────────────────┘     └────────┬────────┘
                                 │
                    ┌────────────┼────────────┐
                    ▼            ▼            ▼
              ┌──────────┐ ┌──────────┐ ┌──────────┐
              │ trades-1 │ │ trades-2 │ │ trades-3 │
              │ depth-1  │ │ depth-2  │ └──────────┘
              └────┬─────┘ └────┬─────┘
                   │            │
                   └─────┬──────┘
                         ▼
                 ┌───────────────┐
                 │  ClickHouse   │
                 │ (dev-network) │
                 └───────────────┘
```

## Cleanup

```bash
# Stop all services
docker-compose down

# Remove volumes (WARNING: deletes all data)
docker-compose down -v

# Remove images
docker rmi wallex-producer:latest radar-consumer:latest
```

## Support

For issues or questions:
1. Check logs: `docker-compose logs -f <service-name>`
2. Verify health: `docker-compose ps`
3. Check network: `docker network inspect radar_dev-network`


