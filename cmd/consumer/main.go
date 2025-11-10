package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/navid-fn/radar/internal/consumer"
	"github.com/navid-fn/radar/internal/consumer/config"
	"github.com/navid-fn/radar/internal/repository"
	"github.com/pressly/goose/v3"
	"gorm.io/driver/clickhouse"
	"gorm.io/gorm"
)

func main() {
	cfg := config.Load()
	db, err := gorm.Open(clickhouse.Open(cfg.ClickHouseDSN), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	migrateFlag := flag.Bool("migrate", false, "Run database migrations and exit")
	flag.Parse()

	if *migrateFlag {
		sqlDB, err := db.DB()
		if err != nil {
			log.Fatalf("Failed to get sql.DB: %v", err)
		}
		if err := goose.SetDialect("clickhouse"); err != nil {
			log.Fatalf("Goose: failed to set dialect: %v", err)
		}
		log.Println("Running database migrations...")
		if err := goose.Up(sqlDB, "internal/migrations"); err != nil {
			log.Fatalf("Goose migration failed: %v", err)
		}
		log.Println("Migrations completed successfully")
		return
	}

	tradeRepo := repository.NewGormTradeRepository(db)

	kafkaConsumer := consumer.NewConsumer(cfg, tradeRepo)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := kafkaConsumer.Start(ctx); err != nil {
			log.Printf("Consumer error: %v\n", err)
		}
	}()

	sig := <-sigChan
	log.Printf("Received signal: %v. Initiating graceful shutdown...\n", sig)

	cancel()

	log.Println("Application stopped successfully")
}

