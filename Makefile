.PHONY: help build start stop restart logs clean health monitor kafka-topics clickhouse-cli

# Default target
help:
	@echo "Radar - Multi-Worker Pipeline Management"
	@echo ""
	@echo "Available commands:"
	@echo "  make build          - Build all Docker images"
	@echo "  make start          - Start all services"
	@echo "  make start-infra    - Start only infrastructure (Kafka + ClickHouse)"
	@echo "  make start-producers - Start WebSocket producers"
	@echo "  make start-consumers - Start consumer workers"
	@echo "  make stop           - Stop all services"
	@echo "  make restart        - Restart all services"
	@echo "  make logs           - View logs from all services"
	@echo "  make logs-trades    - View logs from trades consumers"
	@echo "  make logs-depth     - View logs from depth consumers"
	@echo "  make health         - Check service health status"
	@echo "  make monitor        - Real-time monitoring of all services"
	@echo "  make kafka-topics   - List Kafka topics"
	@echo "  make kafka-lag      - Check consumer group lag"
	@echo "  make clickhouse-cli - Open ClickHouse CLI"
	@echo "  make clickhouse-query-trades - Query trades count"
	@echo "  make clickhouse-query-depth - Query depth count"
	@echo "  make clean          - Stop and remove all containers"
	@echo "  make clean-all      - Clean everything including volumes"
	@echo "  make scale-trades N=5 - Scale trades consumers to N workers"
	@echo ""

# Build images
build:
	@echo "Building Docker images..."
	docker-compose build

# Start services
start:
	@echo "Starting all services..."
	docker-compose up -d
	@echo "Waiting for services to be ready..."
	@sleep 5
	@make health

start-infra:
	@echo "Starting infrastructure services..."
	docker-compose up -d zookeeper kafka clickhouse
	@echo "Waiting for Kafka to be ready (30s)..."
	@sleep 3

start-producers:
	@echo "Starting WebSocket producers..."
	docker-compose up -d wallex-trades wallex-depth 

start-consumers:
	@echo "Starting consumer workers..."
	docker-compose up -d trades-consumer-1 trades-consumer-2 trades-consumer-3 depth-consumer-1 depth-consumer-2 
	 

# Stop services
stop:
	@echo "Stopping all services..."
	docker-compose stop

# Restart services
restart:
	@echo "Restarting all services..."
	docker-compose restart

# View logs
logs:
	docker-compose logs -f

logs-trades:
	docker-compose logs -f trades-consumer-1 trades-consumer-2 trades-consumer-3

logs-depth:
	docker-compose logs -f depth-consumer-1 depth-consumer-2

logs-producers:
	docker-compose logs -f wallex-trades wallex-depth

# Health check
health:
	@echo "Service Health Status:"
	@docker-compose ps

# Monitor (live updates)
monitor:
	@watch -n 2 'docker-compose ps && echo "" && docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}"'

# Kafka operations
kafka-topics:
	@echo "Kafka Topics:"
	@docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

kafka-lag:
	@echo "Consumer Group Lag (Trades):"
	@docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group clickhouse-trades-consumers-v1
	@echo ""
	@echo "Consumer Group Lag (Depth):"
	@docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group clickhouse-depth-consumers-v1

kafka-create-topics:
	@echo "Creating Kafka topics..."
	@docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --create --topic radar_trades --partitions 3 --replication-factor 1 --if-not-exists
	@docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --create --topic radar_depths --partitions 3 --replication-factor 1 --if-not-exists

# ClickHouse operations
clickhouse-cli:
	@echo "Opening ClickHouse CLI..."
	docker exec -it clickhouse clickhouse-client

clickhouse-query-trades:
	@echo "Trades Count:"
	@docker exec -it clickhouse clickhouse-client --query "SELECT count(*) as total_trades FROM trades.trades_master"
	@echo ""
	@echo "Latest 5 Trades:"
	@docker exec -it clickhouse clickhouse-client --query "SELECT symbol, side, price, base_amount, event_time FROM trades.trades_master ORDER BY event_time DESC LIMIT 5"

clickhouse-query-depth:
	@echo "Depth Snapshots Count:"
	@docker exec -it clickhouse clickhouse-client --query "SELECT count(*) as total_snapshots FROM trades.orderbook_depth"
	@echo ""
	@echo "Latest 5 Depth Snapshots:"
	@docker exec -it clickhouse clickhouse-client --query "SELECT symbol, side, levels, timestamp FROM trades.orderbook_depth ORDER BY timestamp DESC LIMIT 5"

clickhouse-reset:
	@echo "WARNING: This will delete all data!"
	@read -p "Are you sure? (yes/no): " confirm; \
	if [ "$$confirm" = "yes" ]; then \
		docker exec -it clickhouse clickhouse-client --query "DROP DATABASE IF EXISTS trades"; \
		echo "Database dropped. Restart consumers to recreate."; \
	else \
		echo "Cancelled."; \
	fi

# Cleanup
clean:
	@echo "Stopping and removing containers..."
	docker-compose down

clean-all:
	@echo "WARNING: This will remove all data!"
	@read -p "Are you sure? (yes/no): " confirm; \
	if [ "$$confirm" = "yes" ]; then \
		docker-compose down -v; \
		echo "All containers and volumes removed."; \
	else \
		echo "Cancelled."; \
	fi

# Scaling (requires docker-compose scale support)
scale-trades:
	@echo "Note: To scale, you need to add more service definitions in docker-compose.yml"
	@echo "Current trades consumers: 3"
	@echo "To add more, copy a trades-consumer section and increment the number."

# Development helpers
dev-setup:
	@echo "Setting up development environment..."
	@test -f .env || cp env.example .env
	@echo "Created .env file (edit if needed)"
	@make build
	@make start-infra
	@echo "Infrastructure ready. Run 'make start-producers' and 'make start-consumers' when ready."

# Full pipeline start
start-pipeline: start-infra
	@echo "Starting full pipeline..."
	@make start-producers
	@sleep 10
	@make start-consumers
	@echo "Pipeline started. Use 'make monitor' to track."

# Quick status
status:
	@echo "=== Service Status ==="
	@docker-compose ps
	@echo ""
	@echo "=== Kafka Topics ==="
	@docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null || echo "Kafka not ready"
	@echo ""
	@echo "=== Data Counts ==="
	@docker exec -it clickhouse clickhouse-client --query "SELECT 'Trades:' as table, count(*) as count FROM trades.trades_master" 2>/dev/null || echo "ClickHouse not ready"
	@docker exec -it clickhouse clickhouse-client --query "SELECT 'Depth:' as table, count(*) as count FROM trades.orderbook_depth" 2>/dev/null || echo "ClickHouse not ready"

