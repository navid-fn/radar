package configs

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type AppConfig struct {
	DBDSN      string
	Ingester   IngesterConfig
	KafkaTrade KafkaTradeConfig
	Coingecko  CoingeckoConfigs
}

type KafkaTradeConfig struct {
	Broker  string
	Topic   string
	GroupID string
}

type IngesterConfig struct {
	BatchSize           int
	BatchTimeoutSeconds int
}

type CoingeckoConfigs struct {
	ExchangesID  []string
	ScheduleHour int // Hour of day to run (0-23), default 0 (midnight)
}

func getDatabaseDSN() string {
	dbUser := getEnv("CLICKHOUSE_USER", "user")
	dbPassword := getEnv("CLICKHOUSE_PASSWORD", "password")
	dbHost := getEnv("CLICKHOUSE_HOST", "localhost")
	dbPort := getEnv("CLICKHOUSE_TCP_PORT", "9000")
	dbName := getEnv("CLICKHOUSE_DB", "db")

	dsn := fmt.Sprintf("clickhouse://%s:%s@%s:%s/%s?dial_timeout=10s&read_timeout=20s", dbUser, dbPassword, dbHost, dbPort, dbName)
	return dsn
}

func getCoingeckoConfigs() CoingeckoConfigs {
	exchangesID := getEnv("COINGECKO_EXCHANGES", "")
	var exchanges []string
	exchanges = strings.Split(exchangesID, ",")
	scheduleHour := getEnvInt("COINGECKO_SCHEDULE_HOUR", 0) // Default: midnight (0:00)
	if scheduleHour < 0 || scheduleHour > 23 {
		scheduleHour = 0
	}
	return CoingeckoConfigs{
		ExchangesID:  exchanges,
		ScheduleHour: scheduleHour,
	}
}

func AppLoad() *AppConfig {
	_ = godotenv.Load()

	return &AppConfig{
		KafkaTrade: KafkaTradeConfig{
			Broker:  getEnv("KAFKA_BROKER", "localhost:9092"),
			Topic:   getEnv("KAFKA_TRADE_TOPIC", "radar_trades"),
			GroupID: getEnv("KAFKA_TRADE_GROUP_ID", "radar-consumer"),
		},
		DBDSN: getDatabaseDSN(),
		Ingester: IngesterConfig{
			BatchSize:           getEnvInt("BATCH_SIZE", 200),
			BatchTimeoutSeconds: getEnvInt("BATCH_TIMEOUT_SECONDS", 5),
		},
		Coingecko: getCoingeckoConfigs(),
	}
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

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
