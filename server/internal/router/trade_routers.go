package router

import (
	"github.com/gin-gonic/gin"
	"github.com/navid-fn/radar/server/internal/handler"
)

func registerTradeRoutes(router *gin.RouterGroup, tradeHandler *handler.TradeHandler) {
	trades := router.Group("/trades")
	{
		trades.GET("/latest", tradeHandler.GetLatest)
		trades.GET("/count", tradeHandler.GetCount)
	}
}
