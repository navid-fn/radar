package router

import (
	"github.com/gin-gonic/gin"
	"github.com/navid-fn/radar/server/internal/handler"
)


type Config struct {
	TradeHandler *handler.TradeHandler
}

func NewRouter(cfg *Config) *gin.Engine {
	router := gin.Default()

	api := router.Group("/v1/")
	registerTradeRoutes(api, cfg.TradeHandler)

	return router
}
