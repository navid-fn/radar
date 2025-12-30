# Radar

A trade data collection pipeline for Iranian cryptocurrency exchanges.

## What It Does

Radar scrapes real-time trade data from multiple exchanges and stores it in ClickHouse for analysis. It helps compare trading volumes, trade counts, and market activity across exchanges.

**Supported Exchanges:**
- Nobitex (WebSocket + API)
- Wallex (WebSocket + API)
- Ramzinex (WebSocket + API)
- Bitpin (WebSocket + API)
- Tabdeal (API)
- CoinGecko (API - for international data)

## Architecture

```
┌──────────────┐     ┌─────────┐     ┌────────────┐     ┌────────────┐
│   Scrapers   │ --> │  Kafka  │ --> │  Ingester  │ --> │ ClickHouse │
│ (WS + API)   │     │         │     │            │     │            │
└──────────────┘     └─────────┘     └────────────┘     └────────────┘
```

- **Scrapers**: Connect to exchanges, normalize data, send to Kafka
- **Kafka**: Message buffer between scrapers and ingester
- **Ingester**: Batches trades and inserts into ClickHouse
- **ClickHouse**: Stores trades with deduplication

## Quick Start

### Prerequisites

- Go 1.21+
- Docker & Docker Compose
- Make (optional)

### 1. Start Infrastructure

```bash
docker-compose up -d kafka clickhouse
```

### 2. Configure

```bash
cp env.example .env
# Edit .env with your settings
```

### 3. Run Migrations

```bash
go run cmd/migrate/main.go
```

### 4. Start Services

```bash
# Terminal 1: Start scraper
go run cmd/scraper/main.go

# Terminal 2: Start ingester
go run cmd/ingester/main.go
```

## Configuration

All config is via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKER` | localhost:9092 | Kafka broker address |
| `KAFKA_TRADE_TOPIC` | radar_trades | Topic for trade data |
| `CLICKHOUSE_HOST` | localhost | ClickHouse host |
| `CLICKHOUSE_TCP_PORT` | 9000 | ClickHouse port |
| `BATCH_SIZE` | 200 | Ingester batch size |

See `env.example` for full list.

## Project Structure

```
cmd/
├── scraper/      # Main scraper application
├── ingester/     # Kafka to ClickHouse ingester
└── migrate/      # Database migrations

internal/
├── drivers/      # Exchange-specific scrapers
│   ├── nobitex/
│   ├── wallex/
│   ├── ramzinex/
│   ├── bitpin/
│   ├── tabdeal/
│   └── coingecko/
├── scraper/      # Shared scraper utilities
├── ingester/     # Ingester logic
├── storage/      # ClickHouse storage
├── models/       # Data models
└── proto/        # Protobuf definitions
```

## Adding a New Driver

1. Create a new folder: `internal/drivers/myexchange/`

2. Create these files:

```
internal/drivers/myexchange/
├── common.go    # Shared types, fetchMarkets(), getLatestUSDTPrice()
├── ws.go        # WebSocket scraper (if available)
└── api.go       # API scraper
```

3. Implement the `scraper.Scraper` interface:

```go
type Scraper interface {
    Run(ctx context.Context) error
    Name() string
}
```

4. Register in `cmd/scraper/main.go`:

```go
scrapers := []scraper.Scraper{
    // ... existing scrapers
    myexchange.NewMyExchangeScraper(kafkaWriter, logger),
}
```

5. Add symbol normalization rules in `internal/scraper/normalizer.go`:

```go
var DefaultSymbolRules = map[string][]SymbolRule{
    // ... existing rules
    "myexchange": {
        {Suffix: "TMN", Replacement: "/IRT"},
        {Suffix: "USDT", Replacement: "/USDT"},
    },
}
```

## Data Format

All trades are normalized to:

| Field | Description |
|-------|-------------|
| `trade_id` | Unique ID from exchange or generated |
| `source` | Exchange name |
| `symbol` | Normalized pair (e.g., `BTC/IRT`) |
| `side` | `buy`, `sell`, or `all` |
| `price` | Price in quote currency (Toman for IRT) |
| `base_amount` | Quantity traded |
| `usdt_price` | USDT/IRT rate at trade time |

## License

MIT

