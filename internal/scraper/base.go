package scraper

import (
	"context"
	"fmt"
	"log/slog"
	"nobitex/radar/configs"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	// WebSocket connection timeouts and intervals
	InitialReconnectDelay = 1 * time.Second
	MaxReconnectDelay     = 30 * time.Second
	HandshakeTimeout      = 5 * time.Second
	ReadTimeout           = 60 * time.Second
	WriteTimeout          = 10 * time.Second
	PingInterval          = 30 * time.Second
	PongTimeout           = 10 * time.Second

	// Connection health
	MaxConsecutiveErrors = 5
	HealthCheckInterval  = 5 * time.Second
)

type Scraper interface {
	Run(ctx context.Context) error
	Name() string
}

type ScraperConfig struct {
	ExchangeName         string
	MaxSubsPerConnection int
	KafkaConfig          *configs.KafkaConfig
	Logger               *slog.Logger
}


type BaseScraper struct {
	Config      *ScraperConfig
	KafkaWriter *kafka.Writer
	Logger      *slog.Logger
}

func NewConfig(exchangeName string, maxSubs int, conf *configs.Config) *ScraperConfig {
	return &ScraperConfig{
		ExchangeName:         exchangeName,
		KafkaConfig:          &conf.KafkaConfigs,
		Logger:               conf.Logger,
		MaxSubsPerConnection: maxSubs,
	}
}

func NewBaseScraper(config *ScraperConfig) *BaseScraper {
	return &BaseScraper{
		Config: config,
		Logger: config.Logger,
	}
}


func (bc *BaseScraper) InitKafkaProducer() error {
	writer := configs.GetKafkaWriter(bc.Config.KafkaConfig)

	bc.KafkaWriter = writer
	bc.Logger.Info("Kafka Producer initialized successfully")
	return nil
}

func (bc *BaseScraper) CloseKafkaProducer() {
	if bc.KafkaWriter != nil {
		if err := bc.KafkaWriter.Close(); err != nil {
			bc.Logger.Error("Error closing Kafka producer", "error", err)
		} else {
			bc.Logger.Info("Kafka Producer closed")
		}
	}
}

func (bc *BaseScraper) SendToKafka(message []byte) error {
	return bc.SendToKafkaWithContext(context.Background(), message)
}

func (bc *BaseScraper) SendToKafkaWithContext(ctx context.Context, message []byte) error {
	msg := kafka.Message{
		Value: message,
	}

	// Use context with timeout for graceful shutdown
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	err := bc.KafkaWriter.WriteMessages(writeCtx, msg)
	if err != nil {
		// Don't log error if context was cancelled (shutdown in progress)
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("failed to send message to Kafka: %w", err)
	}

	return nil
}

// Turn slice of markets to small chunks
// ["BTC", "USDT", "ETH", ...] -> [["BTC", "USDT"], ...]
func ChunkMarkets(markets []string, chunkSize int) [][]string {
	var chunks [][]string
	for i := 0; i < len(markets); i += chunkSize {
		end := i + chunkSize
		if max(end, len(markets)) == end {
			end = len(markets)
		}
		chunks = append(chunks, markets[i:end])
	}
	return chunks
}

func RunWithGracefulShutdown(
	logger *slog.Logger,
	startWorkers func(ctx context.Context, wg *sync.WaitGroup),
) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		logger.Info("Received shutdown signal, gracefully shutting down...")
		cancel()
	}()

	// Start workers
	var wg sync.WaitGroup
	startWorkers(ctx, &wg)

	logger.Info("All workers started")
	wg.Wait()

	return nil
}

