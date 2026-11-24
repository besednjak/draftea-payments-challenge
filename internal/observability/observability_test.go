package observability_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
	"github.com/besednjak/draftea-payments-challenge/internal/observability"
)

type stubChecker struct {
	name string
	err  error
}

func (s stubChecker) Name() string            { return s.name }
func (s stubChecker) Check(context.Context) error { return s.err }

func TestMetricsCollectorChoreographyAndSuccessRate(t *testing.T) {
	col := observability.NewCollector()
	t0 := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

	initEnv, _ := events.NewEnvelope(
		events.EventPaymentInitiated, events.AggregateTypePayment, "pay_1", 1,
		events.PaymentInitiatedPayload{PaymentID: "pay_1", Amount: events.Money{Amount: 5000, Currency: "ARS"}},
		events.Metadata{OccurredAt: t0}, nil, nil,
	)
	compEnv, _ := events.NewEnvelope(
		events.EventPaymentCompleted, events.AggregateTypePayment, "pay_1", 4,
		events.PaymentCompletedPayload{PaymentID: "pay_1", CapturedAmount: 5000, Currency: "ARS"},
		events.Metadata{OccurredAt: t0.Add(150 * time.Millisecond)}, nil, nil,
	)
	rejWalletEnv, _ := events.NewEnvelope(
		events.EventFundsReservationRejected, events.AggregateTypeWallet, "wal_1", 2,
		events.FundsReservationRejectedPayload{PaymentID: "pay_2", Reason: events.ReasonInsufficientFunds},
		events.Metadata{OccurredAt: t0}, nil, nil,
	)
	rejPayEnv, _ := events.NewEnvelope(
		events.EventPaymentRejected, events.AggregateTypePayment, "pay_2", 2,
		events.PaymentRejectedPayload{PaymentID: "pay_2", Reason: events.ReasonInsufficientFunds},
		events.Metadata{OccurredAt: t0.Add(50 * time.Millisecond)}, nil, nil,
	)

	ctx := context.Background()
	for _, env := range []events.Envelope{initEnv, compEnv, rejWalletEnv, rejPayEnv} {
		if err := col.HandleEvent(ctx, env); err != nil {
			t.Fatalf("HandleEvent failed: %v", err)
		}
	}

	col.RecordCircuitBreakerState("mockpay", "OPEN")
	col.RecordOptimisticLockRetry("wallet")

	rep := col.Snapshot()
	if rep.PaymentsInitiated != 1 || rep.PaymentsCompleted != 1 || rep.PaymentsRejected != 1 {
		t.Fatalf("unexpected counts: %+v", rep)
	}
	if rep.SuccessRate != 0.5 {
		t.Fatalf("expected SuccessRate=0.5, got %v", rep.SuccessRate)
	}
	if rep.VolumeByCurrency["ARS"] != 5000 {
		t.Fatalf("expected ARS volume 5000, got %d", rep.VolumeByCurrency["ARS"])
	}
	if rep.InsufficientFundsCount != 1 {
		t.Fatalf("expected InsufficientFundsCount=1, got %d", rep.InsufficientFundsCount)
	}
	if rep.AvgPaymentLatency != 150*time.Millisecond {
		t.Fatalf("expected AvgPaymentLatency=150ms, got %v", rep.AvgPaymentLatency)
	}
	if rep.CircuitBreakerStates["mockpay"] != "OPEN" {
		t.Fatalf("expected circuit breaker state OPEN, got %q", rep.CircuitBreakerStates["mockpay"])
	}
}

func TestHealthRegistryReadiness(t *testing.T) {
	reg := observability.NewHealthRegistry(nil)
	reg.Register(stubChecker{name: "eventstore"})
	reg.Register(stubChecker{name: "eventbus"})

	if !reg.Liveness().Healthy {
		t.Fatal("expected liveness healthy")
	}

	ready, statuses := reg.Readiness(context.Background())
	if !ready || len(statuses) != 2 {
		t.Fatalf("expected ready=true len=2, got ready=%v statuses=%+v", ready, statuses)
	}

	reg.Register(stubChecker{name: "gateway", err: errors.New("connection refused")})
	ready, statuses = reg.Readiness(context.Background())
	if ready || len(statuses) != 3 || statuses[2].Healthy {
		t.Fatalf("expected ready=false with unhealthy gateway, got ready=%v statuses=%+v", ready, statuses)
	}
}

func TestStructuredLoggerCorrelation(t *testing.T) {
	var buf bytes.Buffer
	base := observability.NewLogger(&buf, slog.LevelInfo)
	ctx := observability.WithCorrelation(context.Background(), "pay_42", "pay_42", "wal_9")

	observability.LoggerFromContext(ctx, base).Info("payment completed")

	var parsed map[string]any
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Fatalf("invalid JSON log output: %v (%s)", err, buf.String())
	}
	if parsed["correlation_id"] != "pay_42" || parsed["wallet_id"] != "wal_9" {
		t.Fatalf("missing correlation fields: %+v", parsed)
	}
}
