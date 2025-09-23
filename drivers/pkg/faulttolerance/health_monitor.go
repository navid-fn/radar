package faulttolerance

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// HealthStatus represents the health status of a component
type HealthStatus string

const (
	HealthStatusHealthy   HealthStatus = "healthy"
	HealthStatusDegraded  HealthStatus = "degraded"
	HealthStatusUnhealthy HealthStatus = "unhealthy"
)

// HealthCheck represents a single health check
type HealthCheck struct {
	Name      string                          `json:"name"`
	Status    HealthStatus                    `json:"status"`
	LastCheck time.Time                       `json:"last_check"`
	Duration  time.Duration                   `json:"duration"`
	Error     string                          `json:"error,omitempty"`
	Details   map[string]interface{}          `json:"details,omitempty"`
	CheckFunc func(ctx context.Context) error `json:"-"`
}

// HealthMonitor monitors the health of various components
type HealthMonitor struct {
	checks   map[string]*HealthCheck
	mutex    sync.RWMutex
	logger   *logrus.Logger
	interval time.Duration
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewHealthMonitor creates a new health monitor
func NewHealthMonitor(logger *logrus.Logger, interval time.Duration) *HealthMonitor {
	if interval <= 0 {
		interval = 30 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &HealthMonitor{
		checks:   make(map[string]*HealthCheck),
		logger:   logger,
		interval: interval,
		ctx:      ctx,
		cancel:   cancel,
	}
}

// AddCheck adds a health check
func (hm *HealthMonitor) AddCheck(name string, checkFunc func(ctx context.Context) error) {
	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	hm.checks[name] = &HealthCheck{
		Name:      name,
		Status:    HealthStatusHealthy,
		CheckFunc: checkFunc,
		Details:   make(map[string]interface{}),
	}

	hm.logger.Infof("Added health check: %s", name)
}

// Start starts the health monitoring
func (hm *HealthMonitor) Start() {
	hm.wg.Add(1)
	go hm.monitorLoop()
	hm.logger.Info("Health monitor started")
}

// Stop stops the health monitoring
func (hm *HealthMonitor) Stop() {
	hm.cancel()
	hm.wg.Wait()
	hm.logger.Info("Health monitor stopped")
}

// monitorLoop runs the health checks periodically
func (hm *HealthMonitor) monitorLoop() {
	defer hm.wg.Done()

	ticker := time.NewTicker(hm.interval)
	defer ticker.Stop()

	// Run initial health checks
	hm.runAllChecks()

	for {
		select {
		case <-hm.ctx.Done():
			return
		case <-ticker.C:
			hm.runAllChecks()
		}
	}
}

// runAllChecks runs all registered health checks
func (hm *HealthMonitor) runAllChecks() {
	hm.mutex.RLock()
	checks := make([]*HealthCheck, 0, len(hm.checks))
	for _, check := range hm.checks {
		checks = append(checks, check)
	}
	hm.mutex.RUnlock()

	var wg sync.WaitGroup
	for _, check := range checks {
		wg.Add(1)
		go func(check *HealthCheck) {
			defer wg.Done()
			hm.runCheck(check)
		}(check)
	}
	wg.Wait()
}

// runCheck runs a single health check
func (hm *HealthMonitor) runCheck(check *HealthCheck) {
	if check.CheckFunc == nil {
		return
	}

	start := time.Now()
	ctx, cancel := context.WithTimeout(hm.ctx, 10*time.Second)
	defer cancel()

	err := check.CheckFunc(ctx)
	duration := time.Since(start)

	hm.mutex.Lock()
	defer hm.mutex.Unlock()

	check.LastCheck = start
	check.Duration = duration

	if err != nil {
		oldStatus := check.Status
		check.Status = HealthStatusUnhealthy
		check.Error = err.Error()

		if oldStatus != HealthStatusUnhealthy {
			hm.logger.Errorf("Health check '%s' failed: %v", check.Name, err)
		}
	} else {
		oldStatus := check.Status
		check.Status = HealthStatusHealthy
		check.Error = ""

		if oldStatus != HealthStatusHealthy {
			hm.logger.Infof("Health check '%s' recovered", check.Name)
		}
	}

	// Update details
	check.Details["duration_ms"] = duration.Milliseconds()
	check.Details["last_check"] = check.LastCheck.Format(time.RFC3339)
}

// GetHealth returns the current health status
func (hm *HealthMonitor) GetHealth() map[string]*HealthCheck {
	hm.mutex.RLock()
	defer hm.mutex.RUnlock()

	result := make(map[string]*HealthCheck)
	for name, check := range hm.checks {
		// Create a copy without the CheckFunc
		result[name] = &HealthCheck{
			Name:      check.Name,
			Status:    check.Status,
			LastCheck: check.LastCheck,
			Duration:  check.Duration,
			Error:     check.Error,
			Details:   check.Details,
		}
	}

	return result
}

// GetOverallHealth returns the overall health status
func (hm *HealthMonitor) GetOverallHealth() HealthStatus {
	hm.mutex.RLock()
	defer hm.mutex.RUnlock()

	if len(hm.checks) == 0 {
		return HealthStatusHealthy
	}

	hasUnhealthy := false
	hasDegraded := false

	for _, check := range hm.checks {
		switch check.Status {
		case HealthStatusUnhealthy:
			hasUnhealthy = true
		case HealthStatusDegraded:
			hasDegraded = true
		}
	}

	if hasUnhealthy {
		return HealthStatusUnhealthy
	}
	if hasDegraded {
		return HealthStatusDegraded
	}

	return HealthStatusHealthy
}

// StartHTTPServer starts an HTTP server for health checks
func (hm *HealthMonitor) StartHTTPServer(port int) {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		health := hm.GetHealth()
		overall := hm.GetOverallHealth()

		response := map[string]interface{}{
			"status":    overall,
			"checks":    health,
			"timestamp": time.Now().Format(time.RFC3339),
		}

		w.Header().Set("Content-Type", "application/json")

		// Set HTTP status code based on health
		switch overall {
		case HealthStatusHealthy:
			w.WriteHeader(http.StatusOK)
		case HealthStatusDegraded:
			w.WriteHeader(http.StatusOK) // Still OK but with warnings
		case HealthStatusUnhealthy:
			w.WriteHeader(http.StatusServiceUnavailable)
		}

		json.NewEncoder(w).Encode(response)
	})

	mux.HandleFunc("/health/ready", func(w http.ResponseWriter, r *http.Request) {
		overall := hm.GetOverallHealth()
		if overall == HealthStatusUnhealthy {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("Not Ready"))
		} else {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("Ready"))
		}
	})

	mux.HandleFunc("/health/live", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Live"))
	})

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	go func() {
		hm.logger.Infof("Health check server starting on port %d", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			hm.logger.Errorf("Health check server error: %v", err)
		}
	}()

	// Graceful shutdown
	go func() {
		<-hm.ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			hm.logger.Errorf("Health check server shutdown error: %v", err)
		}
	}()
}
