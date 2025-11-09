package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/navid-fn/radar/server/config"
	"github.com/navid-fn/radar/server/internal/handler"
	"github.com/navid-fn/radar/server/internal/repository"
	"github.com/navid-fn/radar/server/internal/router"
	"github.com/navid-fn/radar/server/internal/service"
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
		if err := goose.Up(sqlDB, "server/migrations"); err != nil {
			log.Fatalf("Goose migration failed: %v", err)
		}
	}

	tradeRepo := repository.NewGormTradeRepository(db)
	tradeService := service.NewTradesService(tradeRepo)
	tradeHandler := handler.NewTradeHandler(tradeService)

	routerConfig := &router.Config{
		TradeHandler: tradeHandler,
	}

	router := router.NewRouter(routerConfig)

	router.Run(fmt.Sprintf(":%s", cfg.ServerPort))
}
