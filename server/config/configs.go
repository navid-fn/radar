package config

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
	"log"
)

type Config struct {
	ClickHouseDSN string
	ServerPort    string
	DebugMode     string
}

func Load() *Config {
	err := godotenv.Load()
	if err != nil {
		log.Println("No .env file found, using environment variables")
	}

	dsn := fmt.Sprintf("clickhouse://%s:%s@%s:%s/%s?dial_timeout=10s&read_timeout=20s",
		getEnv("CLICKHOUSE_USER", "default"),
		getEnv("CLICKHOUSE_PASSWORD", ""),
		getEnv("CLICKHOUSE_HOST", "localhost"),
		getEnv("CLICKHOUSE_TCP_PORT", "9000"),
		getEnv("CLICKHOUSE_DB", "default"),
	)

	return &Config{
		ClickHouseDSN: dsn,
		ServerPort:    getEnv("SERVER_PORT", "8080"),
		DebugMode:     getEnv("DEBUGMODE", "True"),
	}
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
