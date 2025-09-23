# Fault Tolerance Implementation

This document describes the comprehensive fault tolerance mechanisms implemented in both BitPin and Wallex WebSocket producers.

## 🛡️ Fault Tolerance Features

### 1. **Circuit Breaker Pattern**
- **Purpose**: Prevents cascading failures by stopping requests to failing services
- **Implementation**: Separate circuit breakers for API calls and WebSocket connections
- **States**: 
  - `CLOSED`: Normal operation
  - `OPEN`: Service is failing, requests are blocked
  - `HALF_OPEN`: Testing if service has recovered

**Configuration:**
- API Circuit Breaker: 5 failures → 60s timeout → 3 successes to close
- WebSocket Circuit Breaker: 3 failures → 30s timeout → 2 successes to close

### 2. **Retry Mechanism with Exponential Backoff**
- **Exponential Backoff**: Delays increase exponentially (1s, 2s, 4s, 8s...)
- **Jitter**: Random variation to prevent thundering herd
- **Max Attempts**: Configurable (default: 3-5)
- **Max Delay**: Capped at 30 seconds

### 3. **Health Monitoring**
- **Comprehensive Checks**: Kafka, API endpoints, circuit breakers, persistence
- **HTTP Endpoints**: 
  - `/health` - Detailed health status
  - `/health/ready` - Readiness probe
  - `/health/live` - Liveness probe
- **Automatic Recovery**: Self-healing mechanisms

### 4. **Data Persistence**
- **Local Buffer**: Messages stored locally during Kafka failures
- **Automatic Recovery**: Replay messages on startup
- **Cleanup**: Old persistence files automatically removed
- **Configurable**: Buffer size, flush intervals

### 5. **Graceful Degradation**
- **Partial Failures**: System continues operating with reduced functionality
- **Staggered Startup**: Workers start with delays to avoid overwhelming servers
- **Resource Management**: Automatic cleanup and resource optimization

## 🔧 Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKER` | `localhost:9092` | Kafka broker address |
| `DATA_DIR` | `./data` | Directory for persistence files |
| `HEALTH_PORT` | `8080` | Health check server port |
| `LOG_LEVEL` | `info` | Logging level |

### Circuit Breaker Configuration

```go
// API Circuit Breaker
apiCircuitBreaker := faulttolerance.NewCircuitBreaker(
    faulttolerance.CircuitBreakerConfig{
        MaxFailures:      5,           // Open after 5 failures
        Timeout:          60 * time.Second, // Wait 60s before half-open
        SuccessThreshold: 3,           // Close after 3 successes
        Name:             "BitpinAPI",
    }, logger)
```

### Retry Configuration

```go
retryConfig := faulttolerance.DefaultRetryConfig("BitpinOperations")
retryConfig.MaxAttempts = 3              // Maximum retry attempts
retryConfig.BaseDelay = 2 * time.Second  // Initial delay
retryConfig.MaxDelay = 30 * time.Second  // Maximum delay
retryConfig.JitterRange = 0.1            // 10% jitter
```

## 📊 Health Monitoring

### Health Check Endpoints

```bash
# Get detailed health status
curl http://localhost:8080/health

# Check if service is ready
curl http://localhost:8080/health/ready

# Check if service is alive
curl http://localhost:8080/health/live
```

### Example Health Response

```json
{
  "status": "healthy",
  "timestamp": "2025-01-20T10:30:00Z",
  "checks": {
    "kafka": {
      "status": "healthy",
      "last_check": "2025-01-20T10:29:45Z",
      "duration": "15ms"
    },
    "bitpin_api": {
      "status": "healthy",
      "last_check": "2025-01-20T10:29:50Z",
      "duration": "120ms"
    },
    "api_circuit_breaker": {
      "status": "healthy",
      "last_check": "2025-01-20T10:29:55Z"
    }
  }
}
```

## 🚨 Failure Scenarios & Recovery

### 1. Kafka Connection Loss
- **Detection**: Health checks fail, Kafka metadata requests timeout
- **Response**: Messages buffered locally via persistence manager
- **Recovery**: Automatic retry with exponential backoff, replay buffered messages

### 2. API Endpoint Failures
- **Detection**: HTTP errors, timeouts, non-200 status codes
- **Response**: Circuit breaker opens, blocks further requests
- **Recovery**: Automatic retry after timeout, gradual service restoration

### 3. WebSocket Connection Issues
- **Detection**: Connection drops, ping/pong failures, read/write errors
- **Response**: Exponential backoff reconnection, circuit breaker protection
- **Recovery**: Automatic reconnection with staggered startup

### 4. Memory/Resource Exhaustion
- **Detection**: Persistence buffer near full, health checks fail
- **Response**: Graceful degradation, increased flush frequency
- **Recovery**: Automatic cleanup, resource optimization

## 📈 Monitoring & Alerting

### Key Metrics to Monitor

1. **Circuit Breaker States**
   - API circuit breaker status
   - WebSocket circuit breaker status
   - State transition frequency

2. **Health Check Status**
   - Overall system health
   - Individual component health
   - Response times

3. **Persistence Metrics**
   - Buffer utilization
   - Flush frequency
   - Recovery message count

4. **Connection Metrics**
   - WebSocket connection count
   - Reconnection frequency
   - Message throughput

### Alerting Thresholds

- **Critical**: Circuit breaker open for > 5 minutes
- **Warning**: Health check failures > 3 consecutive
- **Info**: Persistence buffer > 80% full

## 🐳 Docker Integration

### Updated Health Checks

```dockerfile
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8080/health/ready || exit 1
```

### Environment Configuration

```yaml
# docker-compose.yml
environment:
  - KAFKA_BROKER=kafka:9092
  - DATA_DIR=/app/data
  - HEALTH_PORT=8080
  - LOG_LEVEL=info
volumes:
  - ./data:/app/data  # Persistence volume
ports:
  - "8080:8080"      # Health check port
```

## 🔄 Recovery Procedures

### Manual Recovery Steps

1. **Check Health Status**
   ```bash
   curl http://localhost:8080/health
   ```

2. **Reset Circuit Breakers** (if needed)
   - Restart the application
   - Wait for automatic timeout and recovery

3. **Replay Persisted Messages**
   - Messages are automatically replayed on startup
   - Check logs for recovery status

4. **Monitor Recovery**
   ```bash
   # Watch logs
   docker-compose logs -f bitpin-producer
   
   # Monitor health
   watch -n 5 'curl -s http://localhost:8080/health | jq'
   ```

## 🎯 Best Practices

### Deployment
1. **Gradual Rollout**: Deploy to staging first, monitor health metrics
2. **Resource Limits**: Set appropriate memory/CPU limits
3. **Persistent Volumes**: Mount data directories for persistence
4. **Load Balancing**: Use multiple instances with proper load balancing

### Monitoring
1. **Proactive Alerts**: Set up alerts before failures occur
2. **Dashboard**: Create monitoring dashboards for key metrics
3. **Log Aggregation**: Centralize logs for better troubleshooting
4. **Regular Testing**: Test failure scenarios regularly

### Configuration
1. **Environment-Specific**: Different configs for dev/staging/prod
2. **Secret Management**: Use proper secret management for sensitive data
3. **Resource Tuning**: Adjust timeouts and limits based on environment
4. **Documentation**: Keep configuration documentation updated

This fault tolerance implementation ensures high availability, automatic recovery, and graceful degradation under various failure scenarios.
