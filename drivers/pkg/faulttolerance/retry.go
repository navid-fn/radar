package faulttolerance

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/sirupsen/logrus"
)

// RetryConfig holds configuration for retry mechanisms
type RetryConfig struct {
	MaxAttempts     int           // Maximum number of retry attempts
	BaseDelay       time.Duration // Base delay for exponential backoff
	MaxDelay        time.Duration // Maximum delay between retries
	Multiplier      float64       // Multiplier for exponential backoff
	JitterRange     float64       // Jitter range (0.0 to 1.0)
	Name            string        // Name for logging
	RetryableErrors []error       // Specific errors that should trigger retries
}

// DefaultRetryConfig returns a default retry configuration
func DefaultRetryConfig(name string) RetryConfig {
	return RetryConfig{
		MaxAttempts: 5,
		BaseDelay:   1 * time.Second,
		MaxDelay:    30 * time.Second,
		Multiplier:  2.0,
		JitterRange: 0.1,
		Name:        name,
	}
}

// RetryableFunc is a function that can be retried
type RetryableFunc func() error

// Retryer handles retry logic with exponential backoff and jitter
type Retryer struct {
	config RetryConfig
	logger *logrus.Logger
	rng    *rand.Rand
}

// NewRetryer creates a new retryer
func NewRetryer(config RetryConfig, logger *logrus.Logger) *Retryer {
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = 3
	}
	if config.BaseDelay <= 0 {
		config.BaseDelay = 1 * time.Second
	}
	if config.MaxDelay <= 0 {
		config.MaxDelay = 30 * time.Second
	}
	if config.Multiplier <= 1.0 {
		config.Multiplier = 2.0
	}
	if config.JitterRange < 0 || config.JitterRange > 1.0 {
		config.JitterRange = 0.1
	}
	if config.Name == "" {
		config.Name = "Retryer"
	}

	return &Retryer{
		config: config,
		logger: logger,
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Execute executes the function with retry logic
func (r *Retryer) Execute(ctx context.Context, fn RetryableFunc) error {
	var lastErr error

	for attempt := 1; attempt <= r.config.MaxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := fn()
		if err == nil {
			if attempt > 1 {
				r.logger.Infof("[%s] Operation succeeded on attempt %d", r.config.Name, attempt)
			}
			return nil
		}

		lastErr = err

		// Check if error is retryable
		if !r.isRetryable(err) {
			r.logger.Errorf("[%s] Non-retryable error: %v", r.config.Name, err)
			return err
		}

		if attempt == r.config.MaxAttempts {
			r.logger.Errorf("[%s] All %d attempts failed, last error: %v", r.config.Name, attempt, err)
			break
		}

		delay := r.calculateDelay(attempt)
		r.logger.Warnf("[%s] Attempt %d failed: %v. Retrying in %v...", r.config.Name, attempt, err, delay)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			continue
		}
	}

	return fmt.Errorf("max retry attempts (%d) exceeded: %w", r.config.MaxAttempts, lastErr)
}

// calculateDelay calculates the delay for the next retry with exponential backoff and jitter
func (r *Retryer) calculateDelay(attempt int) time.Duration {
	// Exponential backoff: baseDelay * multiplier^(attempt-1)
	delay := float64(r.config.BaseDelay) * math.Pow(r.config.Multiplier, float64(attempt-1))

	// Apply maximum delay limit
	if delay > float64(r.config.MaxDelay) {
		delay = float64(r.config.MaxDelay)
	}

	// Add jitter to avoid thundering herd problem
	if r.config.JitterRange > 0 {
		jitter := r.rng.Float64() * r.config.JitterRange * delay
		if r.rng.Float64() < 0.5 {
			delay -= jitter
		} else {
			delay += jitter
		}
	}

	// Ensure minimum delay
	if delay < float64(r.config.BaseDelay) {
		delay = float64(r.config.BaseDelay)
	}

	return time.Duration(delay)
}

// isRetryable checks if an error should trigger a retry
func (r *Retryer) isRetryable(err error) bool {
	// If no specific retryable errors are defined, retry all errors
	if len(r.config.RetryableErrors) == 0 {
		return true
	}

	// Check if error matches any retryable error
	for _, retryableErr := range r.config.RetryableErrors {
		if err == retryableErr {
			return true
		}
	}

	return false
}

// ExecuteWithCircuitBreaker combines retry logic with circuit breaker
func (r *Retryer) ExecuteWithCircuitBreaker(ctx context.Context, cb *CircuitBreaker, fn RetryableFunc) error {
	return r.Execute(ctx, func() error {
		return cb.Execute(ctx, fn)
	})
}
