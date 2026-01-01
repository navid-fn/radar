// Package configs provides application configuration loaded from environment variables.
// All configuration is externalized via environment variables for 12-factor app compliance.
package configs

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

// AppConfig holds all application configuration.
// Load it once at startup using AppLoad().
type AppConfig struct {
	// DBDSN is the ClickHouse connection string.
	DBDSN string

	// Ingester contains settings for the Kafka-to-ClickHouse ingester.
	Ingester IngesterConfig

	// KafkaTrade contains Kafka connection settings for trade data.
	KafkaTrade KafkaConfig

	// KafkaOHLC contains Kafka connection settings for OHLC data.
	KafkaOHLC KafkaConfig

	// Coingecko contains settings for the CoinGecko scraper.
	Coingecko CoingeckoConfigs
}

// KafkaTradeConfig holds Kafka connection settings for trades.
type KafkaConfig struct {
	// Broker is the Kafka broker address (e.g., "localhost:9092").
	Broker string

	// Topic is the Kafka topic for trade data.
	Topic string

	// GroupID is the consumer group ID for the ingester.
	GroupID string
}


// IngesterConfig holds settings for batch processing.
type IngesterConfig struct {
	// BatchSize is the maximum number of trades to accumulate before flushing.
	BatchSize int

	// BatchTimeoutSeconds is the maximum seconds to wait before flushing.
	BatchTimeoutSeconds int
}

// CoingeckoConfigs holds CoinGecko API scraper settings.
type CoingeckoConfigs struct {
	// ExchangesID is a list of CoinGecko exchange IDs to scrape (comma-separated in env).
	ExchangesID []string

	// ScheduleHour is the hour of day (0-23) to run the daily scrape.
	// Uses Asia/Tehran timezone. Default: 0 (midnight).
	ScheduleHour int
}

// getDatabaseDSN constructs the ClickHouse DSN from environment variables.
func getDatabaseDSN() string {
	dbUser := getEnv("CLICKHOUSE_USER", "user")
	dbPassword := getEnv("CLICKHOUSE_PASSWORD", "password")
	dbHost := getEnv("CLICKHOUSE_HOST", "localhost")
	dbPort := getEnv("CLICKHOUSE_TCP_PORT", "9000")
	dbName := getEnv("CLICKHOUSE_DB", "db")

	return fmt.Sprintf(
		"clickhouse://%s:%s@%s:%s/%s?dial_timeout=10s&read_timeout=20s",
		dbUser, dbPassword, dbHost, dbPort, dbName,
	)
}

// getCoingeckoConfigs loads CoinGecko settings from environment.
func getCoingeckoConfigs() CoingeckoConfigs {
	exchangesID := getEnv("COINGECKO_EXCHANGES", "")
	var exchanges []string
	if exchangesID != "" {
	exchanges = strings.Split(exchangesID, ",")
	}

	scheduleHour := getEnvInt("COINGECKO_SCHEDULE_HOUR", 0)
	if scheduleHour < 0 || scheduleHour > 23 {
		scheduleHour = 0
	}

	return CoingeckoConfigs{
		ExchangesID:  exchanges,
		ScheduleHour: scheduleHour,
	}
}

// AppLoad loads all application configuration from environment variables.
// It attempts to load a .env file first (for local development).
// Call this once at application startup.
func AppLoad() *AppConfig {
	_ = godotenv.Load() // Ignore error - .env is optional

	return &AppConfig{
		KafkaTrade: KafkaConfig{
			Broker:  getEnv("KAFKA_BROKER", "localhost:9092"),
			Topic:   getEnv("KAFKA_TRADE_TOPIC", "radar_trades"),
			GroupID: getEnv("KAFKA_TRADE_GROUP_ID", "radar-trade-consumer"),
		},
		KafkaOHLC: KafkaConfig{
			Broker:  getEnv("KAFKA_BROKER", "localhost:9092"),
			Topic:   getEnv("KAFKA_OHLC_TOPIC", "radar_ohlc"),
			GroupID: getEnv("KAFKA_OHLC_GROUP_ID", "radar-ohlc-consumer"),
		},
		DBDSN: getDatabaseDSN(),
		Ingester: IngesterConfig{
			BatchSize:           getEnvInt("BATCH_SIZE", 200),
			BatchTimeoutSeconds: getEnvInt("BATCH_TIMEOUT_SECONDS", 5),
		},
		Coingecko: getCoingeckoConfigs(),
	}
}

// getEnv returns the environment variable value or a default.
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

// getEnvInt returns the environment variable as int or a default.
func getEnvInt(key string, defaultValue int) int {
	valueStr := getEnv(key, "")
	if valueStr == "" {
		return defaultValue
	}
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return defaultValue
	}
	return value
}
