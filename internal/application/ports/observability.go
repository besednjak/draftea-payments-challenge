package ports

import (
	"context"
	"io"
	"log/slog"
	"time"
)

type correlationCtxKey string

const (
	ctxKeyCorrelationID correlationCtxKey = "correlation_id"
	ctxKeyPaymentID     correlationCtxKey = "payment_id"
	ctxKeyWalletID      correlationCtxKey = "wallet_id"
)

// WithCorrelation attaches tracing identifiers to ctx without coupling callers
// to a concrete observability implementation.
func WithCorrelation(ctx context.Context, correlationID, paymentID, walletID string) context.Context {
	if correlationID != "" {
		ctx = context.WithValue(ctx, ctxKeyCorrelationID, correlationID)
	}
	if paymentID != "" {
		ctx = context.WithValue(ctx, ctxKeyPaymentID, paymentID)
	}
	if walletID != "" {
		ctx = context.WithValue(ctx, ctxKeyWalletID, walletID)
	}
	return ctx
}

// LoggerFromContext enriches base with any correlation fields stored in ctx.
func LoggerFromContext(ctx context.Context, base *slog.Logger) *slog.Logger {
	if base == nil {
		base = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	if ctx == nil {
		return base
	}

	var args []any
	if value, ok := ctx.Value(ctxKeyCorrelationID).(string); ok && value != "" {
		args = append(args, "correlation_id", value)
	}
	if value, ok := ctx.Value(ctxKeyPaymentID).(string); ok && value != "" {
		args = append(args, "payment_id", value)
	}
	if value, ok := ctx.Value(ctxKeyWalletID).(string); ok && value != "" {
		args = append(args, "wallet_id", value)
	}
	if len(args) == 0 {
		return base
	}
	return base.With(args...)
}

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

// MetricsRecorder captures operational telemetry called by application and infrastructure
// components. Business metrics are collected choreographically from domain events.
type MetricsRecorder interface {
	RecordGatewayAttempt(provider, outcome string, elapsed time.Duration)
	RecordCircuitBreakerState(provider, state string)
	RecordDLQMessage(topic string)
	RecordOptimisticLockRetry(streamType string)
}
