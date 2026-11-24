package gateway

import (
	"sync"
	"time"

	"github.com/besednjak/draftea-payments-challenge/internal/application/ports"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
)

// CircuitState represents the state of a per-provider circuit breaker.
type CircuitState string

const (
	StateClosed   CircuitState = "CLOSED"
	StateOpen     CircuitState = "OPEN"
	StateHalfOpen CircuitState = "HALF_OPEN"
)

// CircuitBreakerConfig tunes thresholds and timeouts for CircuitBreaker.
type CircuitBreakerConfig struct {
	Provider             string
	WindowSize           int
	FailureRateThreshold float64
	ConsecutiveTimeouts  int
	OpenTimeout          time.Duration
	HalfOpenMaxProbes    int
	Clock                events.Clock
	OnStateChange        func(provider string, from, to CircuitState)
}

func (c CircuitBreakerConfig) withDefaults() CircuitBreakerConfig {
	if c.WindowSize <= 0 {
		c.WindowSize = 20
	}
	if c.FailureRateThreshold <= 0 {
		c.FailureRateThreshold = 0.5
	}
	if c.ConsecutiveTimeouts <= 0 {
		c.ConsecutiveTimeouts = 5
	}
	if c.OpenTimeout <= 0 {
		c.OpenTimeout = 30 * time.Second
	}
	if c.HalfOpenMaxProbes <= 0 {
		c.HalfOpenMaxProbes = 3
	}
	if c.Clock == nil {
		c.Clock = events.DefaultClock
	}
	return c
}

// CircuitBreaker protects calls to an external provider with Closed/Open/Half-Open states.
// Trips open when >FailureRateThreshold of the last WindowSize calls fail OR after
// ConsecutiveTimeouts timeouts in a row.
type CircuitBreaker struct {
	cfg CircuitBreakerConfig

	mu                  sync.Mutex
	state               CircuitState
	openedAt            time.Time
	window              []bool // true = failure, false = success
	windowIdx           int
	windowCount         int
	consecutiveTimeouts int
	halfOpenInFlight    int
	halfOpenSuccesses   int
}

// NewCircuitBreaker constructs a CircuitBreaker starting in StateClosed.
func NewCircuitBreaker(cfg CircuitBreakerConfig) *CircuitBreaker {
	cfg = cfg.withDefaults()
	return &CircuitBreaker{
		cfg:    cfg,
		state:  StateClosed,
		window: make([]bool, cfg.WindowSize),
	}
}

// State returns the current recorded state of the breaker.
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

// Allow checks whether an outbound request is permitted to execute.
// Returns ports.ErrCircuitOpen when the circuit is open or half-open probes are saturated.
func (cb *CircuitBreaker) Allow() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		return nil

	case StateOpen:
		if cb.cfg.Clock().Sub(cb.openedAt) >= cb.cfg.OpenTimeout {
			cb.transitionLocked(StateHalfOpen)
			cb.halfOpenSuccesses = 0
			cb.halfOpenInFlight = 1
			return nil
		}
		return ports.ErrCircuitOpen

	case StateHalfOpen:
		if cb.halfOpenInFlight+cb.halfOpenSuccesses >= cb.cfg.HalfOpenMaxProbes {
			return ports.ErrCircuitOpen
		}
		cb.halfOpenInFlight++
		return nil

	default:
		return ports.ErrCircuitOpen
	}
}

// RecordSuccess records a technically healthy response from the provider.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.consecutiveTimeouts = 0

	switch cb.state {
	case StateClosed:
		cb.recordWindowLocked(false)

	case StateHalfOpen:
		if cb.halfOpenInFlight > 0 {
			cb.halfOpenInFlight--
		}
		cb.halfOpenSuccesses++
		if cb.halfOpenSuccesses >= cb.cfg.HalfOpenMaxProbes {
			cb.resetWindowLocked()
			cb.halfOpenInFlight = 0
			cb.halfOpenSuccesses = 0
			cb.transitionLocked(StateClosed)
		}
	}
}

// RecordFailure records a technical failure (timeout or 5xx/network error).
// Business declines must NOT be recorded as circuit breaker failures.
func (cb *CircuitBreaker) RecordFailure(isTimeout bool) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if isTimeout {
		cb.consecutiveTimeouts++
	} else {
		cb.consecutiveTimeouts = 0
	}

	switch cb.state {
	case StateClosed:
		cb.recordWindowLocked(true)
		if cb.shouldOpenLocked() {
			cb.openedAt = cb.cfg.Clock()
			cb.transitionLocked(StateOpen)
		}

	case StateHalfOpen:
		cb.halfOpenInFlight = 0
		cb.halfOpenSuccesses = 0
		cb.openedAt = cb.cfg.Clock()
		cb.transitionLocked(StateOpen)
	}
}

func (cb *CircuitBreaker) recordWindowLocked(failed bool) {
	cb.window[cb.windowIdx] = failed
	cb.windowIdx = (cb.windowIdx + 1) % cb.cfg.WindowSize
	if cb.windowCount < cb.cfg.WindowSize {
		cb.windowCount++
	}
}

func (cb *CircuitBreaker) resetWindowLocked() {
	for i := range cb.window {
		cb.window[i] = false
	}
	cb.windowIdx = 0
	cb.windowCount = 0
	cb.consecutiveTimeouts = 0
}

func (cb *CircuitBreaker) shouldOpenLocked() bool {
	if cb.consecutiveTimeouts >= cb.cfg.ConsecutiveTimeouts {
		return true
	}
	if cb.windowCount < cb.cfg.WindowSize {
		return false
	}
	failures := 0
	for _, failed := range cb.window {
		if failed {
			failures++
		}
	}
	return float64(failures)/float64(cb.windowCount) > cb.cfg.FailureRateThreshold
}

func (cb *CircuitBreaker) transitionLocked(to CircuitState) {
	if cb.state == to {
		return
	}
	from := cb.state
	cb.state = to
	if cb.cfg.OnStateChange != nil {
		cb.cfg.OnStateChange(cb.cfg.Provider, from, to)
	}
}
