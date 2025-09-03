# Development Stack: ClickHouse + Kafka + Zookeeper

A simple Docker Compose setup for local development with ClickHouse, Kafka, and Zookeeper.

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
