package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/navid-fn/radar/server/internal/service"
)

type TradeHandler struct {
	tradeService *service.TradesService
}

func NewTradeHandler(service *service.TradesService) *TradeHandler {
	return &TradeHandler{
		tradeService: service,
	}
}

func (h *TradeHandler) GetLatest(c *gin.Context) {
	allSource := c.Query("allSource")
	if allSource == "true" {
		c.JSON(http.StatusOK, h.tradeService.GetLastTradesPerSource())
	} else {
		trades := h.tradeService.GetLastTenTrades()
		c.JSON(http.StatusOK, trades)
	}
}

func (h *TradeHandler) GetCount(c *gin.Context) {
	var message any
	source := c.Query("source")
	if source == "all" {
		message = h.tradeService.GetCountTradesPerSource()
	} else {
		count := h.tradeService.GetCountTrades(source)
		if source != "" {
			message = gin.H{source: count}
		} else {
			message = gin.H{"count": count}
		}

	}
	c.JSON(http.StatusOK, message)
}
