package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

type Config struct {
	ClickHouseDSN        string
	ServerPort           string
	Debug                string
	WorkerCount          int
	BatchSize            int
	BatchTimeoutSeconds  int
	KafkaConfig          kafka.ReaderConfig
}

func Load() *Config {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	
	err := godotenv.Load()
	if err != nil {
		logger.Info("No .env file found, using environment variables")
	}

	dsn := fmt.Sprintf("clickhouse://%s:%s@%s:%s/%s?dial_timeout=10s&read_timeout=20s",
		getEnv("CLICKHOUSE_USER", "default"),
		getEnv("CLICKHOUSE_PASSWORD", ""),
		getEnv("CLICKHOUSE_HOST", "localhost"),
		getEnv("CLICKHOUSE_TCP_PORT", "9000"),
		getEnv("CLICKHOUSE_DB", "default"),
	)

	workerCount := getEnvAsInt("WORKER_COUNT", 1)
	batchSize := getEnvAsInt("BATCH_SIZE", 200)
	batchTimeoutSeconds := getEnvAsInt("BATCH_TIMEOUT_SECONDS", 5)

	kafkaReadConfig := kafka.ReaderConfig{
		Brokers:        []string{getEnv("KAFKA_BROKER", "localhost:9092")},
		Topic:          getEnv("KAFKA_TOPIC", "radar_trades"),
		GroupID:        getEnv("KAFKA_GROUP_ID", "clickhouse-consumers-v3"),
		MinBytes:       10e3,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	}

	return &Config{
		ClickHouseDSN:       dsn,
		ServerPort:          getEnv("SERVER_PORT", "8080"),
		Debug:               getEnv("DEBUG", "True"),
		WorkerCount:         workerCount,
		BatchSize:           batchSize,
		BatchTimeoutSeconds: batchTimeoutSeconds,
		KafkaConfig:         kafkaReadConfig,
	}
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	valueStr := getEnv(key, "")
	if valueStr == "" {
		return defaultValue
	}
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}))
		logger.Warn("Invalid integer value, using default", "key", key, "default", defaultValue)
		return defaultValue
	}
	return value
}
