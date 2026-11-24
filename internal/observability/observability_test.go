package observability_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/besednjak/draftea-payments-challenge/internal/application/ports"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
	"github.com/besednjak/draftea-payments-challenge/internal/observability"
)

type stubHealthChecker struct {
	name string
	err  error
}

func (s stubHealthChecker) Name() string                { return s.name }
func (s stubHealthChecker) Check(context.Context) error { return s.err }

func TestMetricsCollectorChoreographyAndDeduplication(t *testing.T) {
	collector := observability.NewCollector()
	startTime := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

	initiatedEnv, err := events.NewEnvelope(
		events.EventPaymentInitiated, events.AggregateTypePayment, "pay_1", 1,
		events.PaymentInitiatedPayload{PaymentID: "pay_1", Amount: events.Money{Amount: 5000, Currency: "ARS"}},
		events.Metadata{EventID: "evt_init_1", OccurredAt: startTime}, nil, nil,
	)
	if err != nil {
		t.Fatalf("NewEnvelope initiated failed: %v", err)
	}
	completedEnv, err := events.NewEnvelope(
		events.EventPaymentCompleted, events.AggregateTypePayment, "pay_1", 4,
		events.PaymentCompletedPayload{PaymentID: "pay_1", CapturedAmount: 5000, Currency: "ARS"},
		events.Metadata{EventID: "evt_comp_1", OccurredAt: startTime.Add(150 * time.Millisecond)}, nil, nil,
	)
	if err != nil {
		t.Fatalf("NewEnvelope completed failed: %v", err)
	}
	walletRejectedEnv, err := events.NewEnvelope(
		events.EventFundsReservationRejected, events.AggregateTypeWallet, "wal_1", 2,
		events.FundsReservationRejectedPayload{PaymentID: "pay_2", Reason: events.ReasonInsufficientFunds},
		events.Metadata{EventID: "evt_wrej_2", OccurredAt: startTime}, nil, nil,
	)
	if err != nil {
		t.Fatalf("NewEnvelope walletRejected failed: %v", err)
	}
	paymentRejectedEnv, err := events.NewEnvelope(
		events.EventPaymentRejected, events.AggregateTypePayment, "pay_2", 2,
		events.PaymentRejectedPayload{PaymentID: "pay_2", Reason: events.ReasonInsufficientFunds},
		events.Metadata{EventID: "evt_prej_2", OccurredAt: startTime.Add(50 * time.Millisecond)}, nil, nil,
	)
	if err != nil {
		t.Fatalf("NewEnvelope paymentRejected failed: %v", err)
	}

	ctx := context.Background()
	// Deliver completedEnv twice to verify at-least-once deduplication by EventID.
	for _, env := range []events.Envelope{initiatedEnv, completedEnv, completedEnv, walletRejectedEnv, paymentRejectedEnv} {
		if err := collector.HandleEvent(ctx, env); err != nil {
			t.Fatalf("HandleEvent failed: %v", err)
		}
	}
	collector.RecordCircuitBreakerState("mockpay", "OPEN")
	collector.RecordOptimisticLockRetry("wallet")

	report := collector.Snapshot()

	if report.PaymentsInitiated != 1 || report.PaymentsCompleted != 1 || report.PaymentsRejected != 1 {
		t.Fatalf("unexpected counts (possible duplicate inflation): %+v", report)
	}
	if report.SuccessRate != 0.5 {
		t.Fatalf("expected SuccessRate=0.5, got %v", report.SuccessRate)
	}
	if report.VolumeByCurrency["ARS"] != 5000 {
		t.Fatalf("expected ARS volume 5000, got %d", report.VolumeByCurrency["ARS"])
	}
	if report.InsufficientFundsCount != 1 {
		t.Fatalf("expected InsufficientFundsCount=1, got %d", report.InsufficientFundsCount)
	}
	if report.AvgPaymentLatency != 150*time.Millisecond {
		t.Fatalf("expected AvgPaymentLatency=150ms, got %v", report.AvgPaymentLatency)
	}
	if report.CircuitBreakerStates["mockpay"] != "OPEN" {
		t.Fatalf("expected circuit breaker state OPEN, got %q", report.CircuitBreakerStates["mockpay"])
	}
}

func TestHealthRegistryReadiness(t *testing.T) {
	registry := observability.NewHealthRegistry(nil)
	registry.Register(stubHealthChecker{name: "eventstore"})
	registry.Register(stubHealthChecker{name: "eventbus"})

	if !registry.Liveness().Healthy {
		t.Fatal("expected liveness healthy")
	}

	ready, statuses := registry.Readiness(context.Background())
	if !ready || len(statuses) != 2 {
		t.Fatalf("expected ready=true len=2, got ready=%v statuses=%+v", ready, statuses)
	}

	registry.Register(stubHealthChecker{name: "gateway", err: errors.New("connection refused")})
	ready, statuses = registry.Readiness(context.Background())
	if ready || len(statuses) != 3 || statuses[2].Healthy {
		t.Fatalf("expected ready=false with unhealthy gateway, got ready=%v statuses=%+v", ready, statuses)
	}
}

func TestStructuredLoggerCorrelation(t *testing.T) {
	var output bytes.Buffer
	baseLogger := observability.NewLogger(&output, slog.LevelInfo)
	ctx := ports.WithCorrelation(context.Background(), "pay_42", "pay_42", "wal_9")

	ports.LoggerFromContext(ctx, baseLogger).Info("payment completed")

	var parsed map[string]any
	if err := json.Unmarshal(output.Bytes(), &parsed); err != nil {
		t.Fatalf("invalid JSON log output: %v (%s)", err, output.String())
	}
	if parsed["correlation_id"] != "pay_42" || parsed["payment_id"] != "pay_42" || parsed["wallet_id"] != "wal_9" {
		t.Fatalf("missing correlation fields: %+v", parsed)
	}
}
