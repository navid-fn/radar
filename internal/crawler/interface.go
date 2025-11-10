package crawler

import (
	"context"
	"sync"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

// Base setting of crawlers
type Config struct {
	ExchangeName         string
	KafkaBroker          string
	KafkaTopic           string
	Logger               *logrus.Logger
	MaxSubsPerConnection int
}

type Crawler interface {
	Run(ctx context.Context) error
	GetName() string
}

type MarketFetcher interface {
	FetchMarkets() ([]string, error)
}

type WebSocketWorker interface {
	HandleConnection(ctx context.Context, workerID string, chunk []string) error
	ChunkMarkets(markets []string, chunkSize int) [][]string
}

type HTTPWorker interface {
	FetchData(ctx context.Context, symbol string) error
}

type BaseCrawler struct {
	Config      *Config
	KafkaWriter *kafka.Writer
	Logger      *logrus.Logger
}

type Worker interface {
	Run(ctx context.Context, chunk []string, wg *sync.WaitGroup)
}
