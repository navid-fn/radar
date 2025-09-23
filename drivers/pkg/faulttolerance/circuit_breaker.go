package faulttolerance

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// CircuitBreakerState represents the current state of the circuit breaker
type CircuitBreakerState int

const (
	StateClosed CircuitBreakerState = iota
	StateOpen
	StateHalfOpen
)

func (s CircuitBreakerState) String() string {
	switch s {
	case StateClosed:
		return "CLOSED"
	case StateOpen:
		return "OPEN"
	case StateHalfOpen:
		return "HALF_OPEN"
	default:
		return "UNKNOWN"
	}
}

// CircuitBreakerConfig holds configuration for the circuit breaker
type CircuitBreakerConfig struct {
	MaxFailures      int           // Maximum consecutive failures before opening
	Timeout          time.Duration // Time to wait before transitioning from Open to Half-Open
	SuccessThreshold int           // Consecutive successes needed to close from Half-Open
	Name             string        // Name for logging
}

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	config          CircuitBreakerConfig
	state           CircuitBreakerState
	failures        int
	successes       int
	lastFailureTime time.Time
	mutex           sync.RWMutex
	logger          *logrus.Logger
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(config CircuitBreakerConfig, logger *logrus.Logger) *CircuitBreaker {
	if config.MaxFailures <= 0 {
		config.MaxFailures = 5
	}
	if config.Timeout <= 0 {
		config.Timeout = 60 * time.Second
	}
	if config.SuccessThreshold <= 0 {
		config.SuccessThreshold = 3
	}
	if config.Name == "" {
		config.Name = "CircuitBreaker"
	}

	return &CircuitBreaker{
		config: config,
		state:  StateClosed,
		logger: logger,
	}
}

var (
	ErrCircuitBreakerOpen = errors.New("circuit breaker is open")
	ErrTooManyRequests    = errors.New("too many requests when circuit breaker is half-open")
)

// Execute runs the given function with circuit breaker protection
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func() error) error {
	if !cb.canExecute() {
		return ErrCircuitBreakerOpen
	}

	err := fn()
	cb.recordResult(err)
	return err
}

// canExecute determines if the circuit breaker allows execution
func (cb *CircuitBreaker) canExecute() bool {
	cb.mutex.RLock()
	defer cb.mutex.RUnlock()

	switch cb.state {
	case StateClosed:
		return true
	case StateOpen:
		// Check if timeout has passed to transition to half-open
		if time.Since(cb.lastFailureTime) > cb.config.Timeout {
			cb.mutex.RUnlock()
			cb.mutex.Lock()
			// Double-check after acquiring write lock
			if cb.state == StateOpen && time.Since(cb.lastFailureTime) > cb.config.Timeout {
				cb.setState(StateHalfOpen)
				cb.successes = 0
				cb.logger.Infof("[%s] Circuit breaker transitioning to HALF_OPEN", cb.config.Name)
			}
			cb.mutex.Unlock()
			cb.mutex.RLock()
			return cb.state == StateHalfOpen
		}
		return false
	case StateHalfOpen:
		// Allow limited requests in half-open state
		return true
	default:
		return false
	}
}

// recordResult records the result of an execution
func (cb *CircuitBreaker) recordResult(err error) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	if err != nil {
		cb.failures++
		cb.successes = 0
		cb.lastFailureTime = time.Now()

		switch cb.state {
		case StateClosed:
			if cb.failures >= cb.config.MaxFailures {
				cb.setState(StateOpen)
				cb.logger.Warnf("[%s] Circuit breaker OPENED after %d failures", cb.config.Name, cb.failures)
			}
		case StateHalfOpen:
			cb.setState(StateOpen)
			cb.logger.Warnf("[%s] Circuit breaker reopened from HALF_OPEN due to failure", cb.config.Name)
		}
	} else {
		cb.failures = 0
		cb.successes++

		if cb.state == StateHalfOpen && cb.successes >= cb.config.SuccessThreshold {
			cb.setState(StateClosed)
			cb.logger.Infof("[%s] Circuit breaker CLOSED after %d successes", cb.config.Name, cb.successes)
		}
	}
}

// setState changes the circuit breaker state
func (cb *CircuitBreaker) setState(state CircuitBreakerState) {
	if cb.state != state {
		oldState := cb.state
		cb.state = state
		cb.logger.Infof("[%s] Circuit breaker state changed: %s -> %s", cb.config.Name, oldState, state)
	}
}

// GetState returns the current state of the circuit breaker
func (cb *CircuitBreaker) GetState() CircuitBreakerState {
	cb.mutex.RLock()
	defer cb.mutex.RUnlock()
	return cb.state
}

// GetStats returns current statistics
func (cb *CircuitBreaker) GetStats() map[string]interface{} {
	cb.mutex.RLock()
	defer cb.mutex.RUnlock()

	return map[string]interface{}{
		"state":           cb.state.String(),
		"failures":        cb.failures,
		"successes":       cb.successes,
		"lastFailureTime": cb.lastFailureTime,
		"name":            cb.config.Name,
	}
}
