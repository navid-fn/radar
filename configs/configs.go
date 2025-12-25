package configs

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

type KafkaConfig struct {
	WorkerCount         int
	BatchSize           int
	BatchTimeoutSeconds int
	Topic               string
	Broker              string
	GroupID             string
}

type Config struct {
	ClickHouseDSN string
	Debug         string
	Logger        *slog.Logger
	KafkaConfigs  KafkaConfig
	CoingeckoCfg  *CoingeckoConfigs
}

type CoingeckoConfigs struct {
	ExchangesID  []string
	ScheduleHour int // Hour of day to run (0-23), default 0 (midnight)
}

func LoadKafka() *KafkaConfig {
	workerCount := getEnvAsInt("WORKER_COUNT", 1)
	batchSize := getEnvAsInt("BATCH_SIZE", 200)
	batchTimeoutSeconds := getEnvAsInt("BATCH_TIMEOUT_SECONDS", 5)

	broker := getEnv("KAFKA_BROKER", "localhost:9092")
	topic := getEnv("KAFKA_TOPIC", "radar_trades")
	groupID := getEnv("KAFKA_GROUP_ID", "radar-consumer")

	return &KafkaConfig{
		WorkerCount:         workerCount,
		BatchSize:           batchSize,
		BatchTimeoutSeconds: batchTimeoutSeconds,
		Broker:              broker,
		Topic:               topic,
		GroupID:             groupID,
	}
}

func GetKafkaReaderConfig(kafkaConfig *KafkaConfig) *kafka.ReaderConfig {
	return &kafka.ReaderConfig{
		Brokers:        []string{kafkaConfig.Broker},
		Topic:          kafkaConfig.Topic,
		GroupID:        kafkaConfig.GroupID,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
	}
}

func GetKafkaWriter(kafkaConfig *KafkaConfig) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(kafkaConfig.Broker),
		Topic:        kafkaConfig.Topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		Async:        false,
		RequiredAcks: kafka.RequireOne,
		Compression:  kafka.Snappy,
	}
}

func loadDatabase() string {
	return fmt.Sprintf("clickhouse://%s:%s@%s:%s/%s?dial_timeout=10s&read_timeout=20s",
		getEnv("CLICKHOUSE_USER", "default"),
		getEnv("CLICKHOUSE_PASSWORD", ""),
		getEnv("CLICKHOUSE_HOST", "localhost"),
		getEnv("CLICKHOUSE_TCP_PORT", "9000"),
		getEnv("CLICKHOUSE_DB", "default"),
	)
}

func getCoingeckoConfigs() *CoingeckoConfigs {
	exchangesID := getEnv("COINGECKO_EXCHANGES", "")
	if exchangesID == "" {
		return nil
	}
	var exchanges []string
	exchanges = strings.Split(exchangesID, ",")
	scheduleHour := getEnvAsInt("COINGECKO_SCHEDULE_HOUR", 0) // Default: midnight (0:00)
	if scheduleHour < 0 || scheduleHour > 23 {
		scheduleHour = 0
	}
	return &CoingeckoConfigs{
		ExchangesID:  exchanges,
		ScheduleHour: scheduleHour,
	}
}

func Load() *Config {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	err := godotenv.Load()
	if err != nil {
		logger.Info("No .env file found, using environment variables")
	}
	dsn := loadDatabase()

	return &Config{
		ClickHouseDSN: dsn,
		Debug:         getEnv("DEBUG", "True"),
		Logger:        logger,
		KafkaConfigs:  *LoadKafka(),
		CoingeckoCfg:  getCoingeckoConfigs(),
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
