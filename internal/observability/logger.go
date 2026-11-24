package observability

import (
	"context"
	"io"
	"log/slog"
)

type ctxKey string

const (
	ctxKeyCorrelationID ctxKey = "correlation_id"
	ctxKeyPaymentID     ctxKey = "payment_id"
	ctxKeyWalletID      ctxKey = "wallet_id"
)

// NewLogger creates a structured JSON slog.Logger.
func NewLogger(output io.Writer, level slog.Level) *slog.Logger {
	if output == nil {
		output = io.Discard
	}
	return slog.New(slog.NewJSONHandler(output, &slog.HandlerOptions{
		Level: level,
	}))
}

// WithCorrelation attaches payment/wallet tracing fields to ctx.
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
		base = NewLogger(io.Discard, slog.LevelInfo)
	}
	if ctx == nil {
		return base
	}

	var args []any
	if v, ok := ctx.Value(ctxKeyCorrelationID).(string); ok && v != "" {
		args = append(args, "correlation_id", v)
	}
	if v, ok := ctx.Value(ctxKeyPaymentID).(string); ok && v != "" {
		args = append(args, "payment_id", v)
	}
	if v, ok := ctx.Value(ctxKeyWalletID).(string); ok && v != "" {
		args = append(args, "wallet_id", v)
	}
	if len(args) == 0 {
		return base
	}
	return base.With(args...)
}
