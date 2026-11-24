package ports

import (
	"context"
	"time"
)

// HealthStatus captures the outcome of a single dependency health check.
type HealthStatus struct {
	Name      string    `json:"name"`
	Healthy   bool      `json:"healthy"`
	Message   string    `json:"message,omitempty"`
	CheckedAt time.Time `json:"checked_at"`
}

// HealthChecker is implemented by infrastructure components participating in /readyz checks.
type HealthChecker interface {
	Name() string
	Check(ctx context.Context) error
}

// MetricsRecorder captures operational and business telemetry without coupling
// application services to a concrete metrics backend.
type MetricsRecorder interface {
	RecordPaymentOutcome(status string, amount int64, currency string)
	RecordPaymentLatency(d time.Duration)
	RecordInsufficientFunds(walletID string)
	RecordRefund(amount int64, currency string)
	RecordGatewayAttempt(provider, outcome string, d time.Duration)
	RecordCircuitBreakerState(provider, state string)
	RecordDLQMessage(topic string)
	RecordOptimisticLockRetry(streamType string)
}
