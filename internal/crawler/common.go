package crawler

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
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
	config := kafka.ConfigMap{
		"bootstrap.servers": bc.Config.KafkaBroker,
	}

	producer, err := kafka.NewProducer(&config)
	if err != nil {
		return fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	bc.KafkaProducer = producer
	bc.Logger.Info("Kafka Producer initialized successfully")
	return nil
}

// Check Events channel of kafka. if error is occurred, it will send error to our logger
func (bc *BaseCrawler) StartDeliveryReport() {
	go func() {
		for e := range bc.KafkaProducer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					bc.Logger.Errorf("Message delivery failed: %v", ev.TopicPartition.Error)
				}
			}
		}
	}()
}

func (bc *BaseCrawler) CloseKafkaProducer() {
	if bc.KafkaProducer != nil {
		bc.KafkaProducer.Close()
		bc.Logger.Info("Kafka Producer closed")
	}
}

func (bc *BaseCrawler) SendToKafka(message []byte) error {
	topic := bc.Config.KafkaTopic
	return bc.KafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          message,
	}, nil)
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
