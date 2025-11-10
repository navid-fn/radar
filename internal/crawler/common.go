package crawler

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

const (
	// Default values
	DefaultKafkaBroker   = "localhost:9092"
	DefaultKafkaTopic    = "radar_trades"
	MaxSubsPerConnection = 20

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

func NewLogger() *logrus.Logger {
	logger := logrus.New()
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
	return logger
}

func NewConfig(exchangeName string, maxSubs int) *Config {
	kafkaBroker := os.Getenv("KAFKA_BROKER")
	kafkaTopic := os.Getenv("KAFKA_TOPIC")

	if kafkaBroker == "" {
		kafkaBroker = DefaultKafkaBroker
	}
	if kafkaTopic == "" {
		kafkaTopic = DefaultKafkaTopic
	}

	if maxSubs == 0 {
		maxSubs = MaxSubsPerConnection
	}

	return &Config{
		ExchangeName:         exchangeName,
		KafkaBroker:          kafkaBroker,
		KafkaTopic:           kafkaTopic,
		Logger:               NewLogger(),
		MaxSubsPerConnection: maxSubs,
	}
}

type KafkaData struct {
	ID       string  `json:"ID"`
	Exchange string  `json:"exchange"`
	Symbol   string  `json:"symbol"`
	Price    float64 `json:"price"`
	Volume   float64 `json:"volume"`
	Quantity float64 `json:"quantity"`
	Side     string  `json:"side"`
	Time     string  `json:"time"`
}

func NewBaseCrawler(config *Config) *BaseCrawler {
	return &BaseCrawler{
		Config: config,
		Logger: config.Logger,
	}
}

// initialize kafka which create a producer
func (bc *BaseCrawler) InitKafkaProducer() error {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(bc.Config.KafkaBroker),
		Topic:        bc.Config.KafkaTopic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		Async:        false,
		RequiredAcks: kafka.RequireOne,
		Compression:  kafka.Snappy,
	}

	bc.KafkaWriter = writer
	bc.Logger.Info("Kafka Producer initialized successfully")
	return nil
}

func (bc *BaseCrawler) CloseKafkaProducer() {
	if bc.KafkaWriter != nil {
		if err := bc.KafkaWriter.Close(); err != nil {
			bc.Logger.Errorf("Error closing Kafka producer: %v", err)
		} else {
			bc.Logger.Info("Kafka Producer closed")
		}
	}
}

func (bc *BaseCrawler) SendToKafka(message []byte) error {
	return bc.SendToKafkaWithContext(context.Background(), message)
}

func (bc *BaseCrawler) SendToKafkaWithContext(ctx context.Context, message []byte) error {
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
	logger *logrus.Logger,
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
