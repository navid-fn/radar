# Wallex WebSocket Driver

This driver connects to Wallex WebSocket API to fetch real-time trading data and orderbook depth information.

## Architecture

The code is now separated into modular components:

- **`main.go`**: Entry point, handles initialization, market fetching, and worker orchestration
- **`trades/trades.go`**: Real-time trades processing (no throttling)
- **`depth/depth.go`**: Orderbook depth processing with throttling to reduce duplicate data

## Features

### Trades Mode
- Real-time trade data streaming
- Immediate Kafka publishing for each trade
- No throttling (every trade is unique and important)

### Depth Mode
- Orderbook snapshot streaming
- **Throttled publishing** to reduce duplicate data
- Configurable throttle interval (default: 7 seconds)
- Only sends the latest snapshot within each interval

## Usage

### Build

```bash
go build -o wallex-bin
```

### Run Trades Mode (Real-time)

```bash
./wallex-bin --mode trades
```

This will:
- Subscribe to all market trades via WebSocket
- Send each trade immediately to Kafka topic `radar_trades`
- No throttling applied

### Run Depth Mode (Throttled)

```bash
./wallex-bin --mode depth
```

This will:
- Subscribe to all market orderbook depth (sellDepth) via WebSocket
- Throttle updates to every 7 seconds (default)
- Send only the latest snapshot to Kafka topic `radar_depths`

### Custom Throttle Interval

```bash
./wallex-bin --mode depth --throttle 10
```

This sets the throttle interval to 10 seconds.

## Environment Variables

- `KAFKA_BROKER`: Kafka broker address (default: `localhost:9092`)

## Why Throttling for Orderbook?

Orderbook/depth data can be very high-frequency and contain a lot of repeated information:
- The same price levels might appear multiple times with minimal changes
- Without throttling, you'd send thousands of near-identical snapshots
- Throttling reduces:
  - Kafka load and storage
  - Network bandwidth
  - Downstream processing requirements
  
By throttling to 5-10 seconds, you still get fresh orderbook data while avoiding redundant updates.

## Best Practices

1. **For Trades**: Run without throttling (`--mode trades`)
   - Each trade is unique and time-sensitive
   - Real-time updates are critical for trade analysis

2. **For Depth**: Run with throttling (`--mode depth --throttle 7`)
   - Orderbook updates frequently with similar data
   - 5-10 second intervals provide good balance between freshness and efficiency

## Architecture Decisions

### Separation of Concerns
- Trades and depth are now in separate files/modules
- Each module handles its own WebSocket connection logic
- Shared configuration in main.go

### Throttling Implementation
- Uses a `ticker` that fires every N seconds
- Collects all incoming messages in memory
- Only sends the latest snapshot when ticker fires
- This approach:
  - Reduces Kafka writes by ~90-95%
  - Ensures data is still reasonably fresh
  - Prevents overwhelming downstream consumers

### Connection Management
- Each worker manages its own WebSocket connection
- Automatic reconnection with exponential backoff
- Health checks with ping/pong
- Graceful shutdown handling

## Extending

To add support for both `sellDepth` and `buyDepth`:

```go
// In main.go Run() method, modify the depth case:
case "depth":
    // Sell depth
    sellWorker := depth.NewDepthWorker(wp.kafkaProducer, wp.logger, wp.kafkaTopic, "sellDepth", wp.throttleInterval)
    for _, chunk := range marketChunks {
        wg.Add(1)
        go sellWorker.Run(ctx, chunk, &wg)
    }
    
    // Buy depth
    buyWorker := depth.NewDepthWorker(wp.kafkaProducer, wp.logger, wp.kafkaTopic, "buyDepth", wp.throttleInterval)
    for _, chunk := range marketChunks {
        wg.Add(1)
        go buyWorker.Run(ctx, chunk, &wg)
    }
```
