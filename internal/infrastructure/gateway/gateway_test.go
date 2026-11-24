package gateway_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/besednjak/draftea-payments-challenge/internal/application/ports"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
	"github.com/besednjak/draftea-payments-challenge/internal/infrastructure/eventstore"
	"github.com/besednjak/draftea-payments-challenge/internal/infrastructure/gateway"
)

type capturePublisher struct {
	mu           sync.Mutex
	envelopes    []events.Envelope
	failFirstN   int32
	publishCalls atomic.Int32
}

func (c *capturePublisher) Publish(_ context.Context, _, _ string, env events.Envelope) error {
	if c.publishCalls.Add(1) <= c.failFirstN {
		return errors.New("transient publish failure")
	}
	c.mu.Lock()
	c.envelopes = append(c.envelopes, env)
	c.mu.Unlock()
	return nil
}

func (c *capturePublisher) list() []events.Envelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	copied := make([]events.Envelope, len(c.envelopes))
	copy(copied, c.envelopes)
	return copied
}

func mustSubmitCommandEnvelope(t *testing.T, paymentID string) events.Envelope {
	t.Helper()
	env, err := events.NewEnvelope(
		events.CommandSubmitPayment,
		events.AggregateTypePayment,
		paymentID,
		1,
		events.SubmitPaymentCommand{
			PaymentID: paymentID,
			Provider:  "mockpay",
			ServiceID: "svc_1",
			Amount:    events.Money{Amount: 5000, Currency: "ARS"},
		},
		events.Metadata{CorrelationID: paymentID},
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("NewEnvelope failed: %v", err)
	}
	return env
}

func TestCircuitBreakerConsecutiveTimeoutsAndRecovery(t *testing.T) {
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	var transitions []string

	breaker := gateway.NewCircuitBreaker(gateway.CircuitBreakerConfig{
		Provider:            "mockpay",
		ConsecutiveTimeouts: 5,
		OpenTimeout:         30 * time.Second,
		HalfOpenMaxProbes:   3,
		Clock:               func() time.Time { return now },
		OnStateChange: func(_ string, from, to gateway.CircuitState) {
			transitions = append(transitions, string(from)+"->"+string(to))
		},
	})

	for i := 0; i < 4; i++ {
		if err := breaker.Allow(); err != nil {
			t.Fatalf("unexpected Allow error at %d: %v", i, err)
		}
		breaker.RecordFailure(true)
	}
	if breaker.State() != gateway.StateClosed {
		t.Fatalf("expected CLOSED after 4 timeouts, got %s", breaker.State())
	}

	breaker.RecordFailure(true)
	if breaker.State() != gateway.StateOpen {
		t.Fatalf("expected OPEN after 5 timeouts, got %s", breaker.State())
	}
	if err := breaker.Allow(); !errors.Is(err, ports.ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen while OPEN, got %v", err)
	}

	now = now.Add(30 * time.Second)
	if err := breaker.Allow(); err != nil {
		t.Fatalf("expected first half-open probe allowed, got %v", err)
	}
	if breaker.State() != gateway.StateHalfOpen {
		t.Fatalf("expected HALF_OPEN, got %s", breaker.State())
	}

	breaker.RecordFailure(true)
	if breaker.State() != gateway.StateOpen {
		t.Fatalf("expected HALF_OPEN -> OPEN on probe failure, got %s", breaker.State())
	}

	now = now.Add(30 * time.Second)
	for i := 0; i < 3; i++ {
		if err := breaker.Allow(); err != nil {
			t.Fatalf("probe %d should be allowed: %v", i+1, err)
		}
		breaker.RecordSuccess()
	}
	if breaker.State() != gateway.StateClosed {
		t.Fatalf("expected CLOSED after 3 successful probes, got %s", breaker.State())
	}

	expected := []string{"CLOSED->OPEN", "OPEN->HALF_OPEN", "HALF_OPEN->OPEN", "OPEN->HALF_OPEN", "HALF_OPEN->CLOSED"}
	if len(transitions) != len(expected) {
		t.Fatalf("got transitions %v, want %v", transitions, expected)
	}
	for index := range expected {
		if transitions[index] != expected[index] {
			t.Fatalf("transition[%d]=%s, want %s", index, transitions[index], expected[index])
		}
	}
}

func TestCircuitBreakerSlidingWindowFailureRate(t *testing.T) {
	breaker := gateway.NewCircuitBreaker(gateway.CircuitBreakerConfig{
		WindowSize:           20,
		FailureRateThreshold: 0.5,
		ConsecutiveTimeouts:  100,
	})

	for i := 0; i < 10; i++ {
		breaker.RecordSuccess()
		breaker.RecordFailure(false)
	}
	if breaker.State() != gateway.StateClosed {
		t.Fatalf("expected CLOSED at exactly 50%% failure rate, got %s", breaker.State())
	}

	breaker.RecordFailure(false)
	if breaker.State() != gateway.StateOpen {
		t.Fatalf("expected OPEN at 55%% failure rate, got %s", breaker.State())
	}
}

func TestFullJitterDelayBounds(t *testing.T) {
	base := 500 * time.Millisecond
	capDelay := 10 * time.Second

	if got := gateway.FullJitterDelay(0, base, capDelay, func() float64 { return 1.0 }); got != 500*time.Millisecond {
		t.Fatalf("attempt 0 max: got %v want 500ms", got)
	}
	if got := gateway.FullJitterDelay(2, base, capDelay, func() float64 { return 0.5 }); got != time.Second {
		t.Fatalf("attempt 2 mid: got %v want 1s", got)
	}
	if got := gateway.FullJitterDelay(10, base, capDelay, func() float64 { return 1.0 }); got != capDelay {
		t.Fatalf("attempt 10 capped: got %v want %v", got, capDelay)
	}
}

func TestAdapterOpenCircuitEmitsNotSentWithoutCallingGateway(t *testing.T) {
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	breaker := gateway.NewCircuitBreaker(gateway.CircuitBreakerConfig{
		ConsecutiveTimeouts: 1,
		OpenTimeout:         30 * time.Second,
		Clock:               func() time.Time { return now },
	})
	breaker.RecordFailure(true)

	mockGateway := gateway.NewMockGateway(gateway.MockBehavior{SubmitOutcome: ports.GatewayOutcomeApproved})
	publisher := &capturePublisher{}
	adapter := gateway.NewAdapter(gateway.AdapterConfig{
		Gateway:   mockGateway,
		Publisher: publisher,
		Breaker:   breaker,
		Retry: gateway.RetryConfig{
			MaxAttempts: 3,
			Sleep:       func(context.Context, time.Duration) error { return nil },
		},
	})

	err := adapter.HandleCommand(context.Background(), mustSubmitCommandEnvelope(t, "pay_cb_open"))

	if err != nil {
		t.Fatalf("HandleCommand failed: %v", err)
	}
	if mockGateway.SubmitCalls("pay_cb_open") != 0 {
		t.Fatalf("expected 0 outbound calls when circuit is open, got %d", mockGateway.SubmitCalls("pay_cb_open"))
	}
	emitted := publisher.list()
	if len(emitted) != 1 || emitted[0].Type != events.EventGatewaySubmissionNotSent {
		t.Fatalf("expected GatewaySubmissionNotSent, got %+v", emitted)
	}
}

func TestAdapterRetriesTimeoutsAndNeverReleasesOnMidRetryCircuitTrip(t *testing.T) {
	mockGateway := gateway.NewMockGateway(gateway.MockBehavior{AlwaysTimeout: true})
	publisher := &capturePublisher{}
	breaker := gateway.NewCircuitBreaker(gateway.CircuitBreakerConfig{
		ConsecutiveTimeouts: 2,
		OpenTimeout:         30 * time.Second,
	})
	adapter := gateway.NewAdapter(gateway.AdapterConfig{
		Gateway:   mockGateway,
		Publisher: publisher,
		Breaker:   breaker,
		Inbox:     eventstore.NewInMemoryInboxStore(),
		Retry: gateway.RetryConfig{
			MaxAttempts:    3,
			BaseDelay:      time.Millisecond,
			MaxDelay:       5 * time.Millisecond,
			AttemptTimeout: time.Second,
			Sleep:          func(context.Context, time.Duration) error { return nil },
		},
	})

	err := adapter.HandleCommand(context.Background(), mustSubmitCommandEnvelope(t, "pay_timeout"))

	if err != nil {
		t.Fatalf("HandleCommand failed: %v", err)
	}
	if mockGateway.SubmitCalls("pay_timeout") != 2 {
		t.Fatalf("expected 2 attempts before breaker opened, got %d", mockGateway.SubmitCalls("pay_timeout"))
	}
	emitted := publisher.list()
	if len(emitted) != 1 || emitted[0].Type != events.EventGatewayOutcomeUnknown {
		t.Fatalf("expected GatewayOutcomeUnknown, got %+v", emitted)
	}
}

func TestAdapterInboxRollbackAllowsRetryOnTransientFailure(t *testing.T) {
	mockGateway := gateway.NewMockGateway(gateway.MockBehavior{SubmitOutcome: ports.GatewayOutcomeApproved})
	publisher := &capturePublisher{failFirstN: 1}
	adapter := gateway.NewAdapter(gateway.AdapterConfig{
		Gateway:   mockGateway,
		Publisher: publisher,
		Inbox:     eventstore.NewInMemoryInboxStore(),
	})
	commandEnv := mustSubmitCommandEnvelope(t, "pay_inbox_retry")

	// First delivery fails while publishing; inbox mark must be reverted.
	if err := adapter.HandleCommand(context.Background(), commandEnv); err == nil {
		t.Fatal("expected first attempt to fail on transient publish error")
	}

	// Second delivery with the same EventID must NOT be discarded as duplicate.
	if err := adapter.HandleCommand(context.Background(), commandEnv); err != nil {
		t.Fatalf("retry attempt should succeed after inbox rollback: %v", err)
	}
	if len(publisher.list()) != 1 {
		t.Fatalf("expected 1 emitted event after retry, got %d", len(publisher.list()))
	}

	// Third delivery with the same EventID must now be deduplicated.
	if err := adapter.HandleCommand(context.Background(), commandEnv); err != nil {
		t.Fatalf("duplicate delivery failed: %v", err)
	}
	if len(publisher.list()) != 1 {
		t.Fatalf("expected duplicate delivery to be ignored, got %d emitted events", len(publisher.list()))
	}
}

func TestAdapterWebhookHMACDeduplicationAndRollback(t *testing.T) {
	secret := "super-secret-hmac-key"
	webhookBody, err := json.Marshal(gateway.WebhookPayload{
		ProviderEventID: "whevt_1",
		PaymentID:       "pay_wh_1",
		Provider:        "mockpay",
		ProviderRef:     "prov_999",
		Status:          string(ports.GatewayOutcomeApproved),
	})
	if err != nil {
		t.Fatalf("marshal webhook failed: %v", err)
	}
	validSignature := gateway.SignWebhook(secret, webhookBody)

	t.Run("rejects invalid HMAC signature", func(t *testing.T) {
		adapter := gateway.NewAdapter(gateway.AdapterConfig{
			Gateway:       gateway.NewMockGateway(gateway.MockBehavior{}),
			Publisher:     &capturePublisher{},
			WebhookSecret: secret,
		})

		err := adapter.HandleWebhook(context.Background(), "bad-sig", webhookBody)

		if !errors.Is(err, gateway.ErrInvalidWebhookSignature) {
			t.Fatalf("expected ErrInvalidWebhookSignature, got %v", err)
		}
	})

	t.Run("reverts deduplication mark when publishing fails so provider retry succeeds", func(t *testing.T) {
		publisher := &capturePublisher{failFirstN: 1}
		adapter := gateway.NewAdapter(gateway.AdapterConfig{
			Gateway:       gateway.NewMockGateway(gateway.MockBehavior{}),
			Publisher:     publisher,
			WebhookSecret: secret,
		})

		if err := adapter.HandleWebhook(context.Background(), validSignature, webhookBody); err == nil {
			t.Fatal("expected first webhook attempt to fail on transient publish error")
		}
		if err := adapter.HandleWebhook(context.Background(), validSignature, webhookBody); err != nil {
			t.Fatalf("second webhook attempt should succeed after rollback: %v", err)
		}
		if err := adapter.HandleWebhook(context.Background(), validSignature, webhookBody); err != nil {
			t.Fatalf("third duplicate webhook attempt failed: %v", err)
		}

		emitted := publisher.list()
		if len(emitted) != 1 || emitted[0].Type != events.EventGatewayPaymentApproved {
			t.Fatalf("expected exactly 1 GatewayPaymentApproved event, got %d", len(emitted))
		}
	})
}

func TestAdapterCorruptPayloadIsNonRetryable(t *testing.T) {
	adapter := gateway.NewAdapter(gateway.AdapterConfig{
		Gateway:   gateway.NewMockGateway(gateway.MockBehavior{}),
		Publisher: &capturePublisher{},
	})
	corruptEnv := events.Envelope{
		EventID:       "evt-corrupt",
		Type:          events.CommandSubmitPayment,
		AggregateType: events.AggregateTypeGateway,
		AggregateID:   "pay_bad",
		Payload:       json.RawMessage(`{corrupt`),
	}

	err := adapter.HandleCommand(context.Background(), corruptEnv)

	if err == nil || !ports.IsNonRetryable(err) {
		t.Fatalf("expected non-retryable decode error, got %v", err)
	}
}

func mustRefundCommandEnvelope(t *testing.T, paymentID, refundID string) events.Envelope {
	t.Helper()
	env, err := events.NewEnvelope(
		events.CommandRefundPayment,
		events.AggregateTypeGateway,
		paymentID,
		0,
		events.RefundPaymentCommand{
			PaymentID:   paymentID,
			RefundID:    refundID,
			Provider:    "mockpay",
			ProviderRef: "prov_1",
			Amount:      events.Money{Amount: 2000, Currency: "ARS"},
		},
		events.Metadata{CorrelationID: paymentID},
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("NewEnvelope refund failed: %v", err)
	}
	return env
}

func mustCheckStatusEnvelope(t *testing.T, paymentID string) events.Envelope {
	t.Helper()
	env, err := events.NewEnvelope(
		events.CommandCheckPaymentStatus,
		events.AggregateTypeGateway,
		paymentID,
		0,
		events.CheckPaymentStatusCommand{
			PaymentID: paymentID,
			Provider:  "mockpay",
		},
		events.Metadata{CorrelationID: paymentID},
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("NewEnvelope check status failed: %v", err)
	}
	return env
}

func TestAdapterHandleRefundPaymentResilience(t *testing.T) {
	fastRetry := gateway.RetryConfig{
		MaxAttempts:    3,
		BaseDelay:      time.Millisecond,
		MaxDelay:       5 * time.Millisecond,
		AttemptTimeout: time.Second,
		Sleep:          func(context.Context, time.Duration) error { return nil },
	}

	t.Run("explicit provider decline emits GatewayRefundDeclined without tripping circuit", func(t *testing.T) {
		mockGateway := gateway.NewMockGateway(gateway.MockBehavior{RefundDeclined: true})
		breaker := gateway.NewCircuitBreaker(gateway.CircuitBreakerConfig{ConsecutiveTimeouts: 1})
		publisher := &capturePublisher{}
		adapter := gateway.NewAdapter(gateway.AdapterConfig{
			Gateway:   mockGateway,
			Publisher: publisher,
			Breaker:   breaker,
			Retry:     fastRetry,
		})

		err := adapter.HandleCommand(context.Background(), mustRefundCommandEnvelope(t, "pay_1", "ref_declined"))

		if err != nil {
			t.Fatalf("HandleCommand failed: %v", err)
		}
		if mockGateway.RefundCalls("ref_declined") != 1 {
			t.Fatalf("expected 1 refund call on business decline, got %d", mockGateway.RefundCalls("ref_declined"))
		}
		if breaker.State() != gateway.StateClosed {
			t.Fatalf("business decline must not trip circuit breaker, got %s", breaker.State())
		}
		emitted := publisher.list()
		if len(emitted) != 1 || emitted[0].Type != events.EventGatewayRefundDeclined {
			t.Fatalf("expected GatewayRefundDeclined, got %+v", emitted)
		}
	})

	t.Run("transient timeout retries and succeeds on later attempt", func(t *testing.T) {
		mockGateway := gateway.NewMockGateway(gateway.MockBehavior{RefundFailFirstNTimeouts: 2})
		publisher := &capturePublisher{}
		adapter := gateway.NewAdapter(gateway.AdapterConfig{
			Gateway:   mockGateway,
			Publisher: publisher,
			Retry:     fastRetry,
		})

		err := adapter.HandleCommand(context.Background(), mustRefundCommandEnvelope(t, "pay_1", "ref_retry_ok"))

		if err != nil {
			t.Fatalf("HandleCommand failed: %v", err)
		}
		if mockGateway.RefundCalls("ref_retry_ok") != 3 {
			t.Fatalf("expected 3 attempts, got %d", mockGateway.RefundCalls("ref_retry_ok"))
		}
		emitted := publisher.list()
		if len(emitted) != 1 || emitted[0].Type != events.EventGatewayRefundApproved {
			t.Fatalf("expected GatewayRefundApproved, got %+v", emitted)
		}
	})

	t.Run("persistent timeout returns error without falsely emitting GatewayRefundDeclined", func(t *testing.T) {
		mockGateway := gateway.NewMockGateway(gateway.MockBehavior{RefundAlwaysTimeout: true})
		publisher := &capturePublisher{}
		adapter := gateway.NewAdapter(gateway.AdapterConfig{
			Gateway:   mockGateway,
			Publisher: publisher,
			Retry:     fastRetry,
		})

		err := adapter.HandleCommand(context.Background(), mustRefundCommandEnvelope(t, "pay_1", "ref_timeout"))

		if err == nil {
			t.Fatal("expected error on uncertain refund timeout")
		}
		if mockGateway.RefundCalls("ref_timeout") != 3 {
			t.Fatalf("expected 3 retry attempts, got %d", mockGateway.RefundCalls("ref_timeout"))
		}
		if len(publisher.list()) != 0 {
			t.Fatalf("timeout must never emit definitive GatewayRefundDeclined, got %+v", publisher.list())
		}
	})

	t.Run("open circuit returns retryable error without calling gateway or emitting decline", func(t *testing.T) {
		breaker := gateway.NewCircuitBreaker(gateway.CircuitBreakerConfig{ConsecutiveTimeouts: 1})
		breaker.RecordFailure(true)
		mockGateway := gateway.NewMockGateway(gateway.MockBehavior{})
		publisher := &capturePublisher{}
		adapter := gateway.NewAdapter(gateway.AdapterConfig{
			Gateway:   mockGateway,
			Publisher: publisher,
			Breaker:   breaker,
			Retry:     fastRetry,
		})

		err := adapter.HandleCommand(context.Background(), mustRefundCommandEnvelope(t, "pay_1", "ref_cb_open"))

		if !errors.Is(err, ports.ErrCircuitOpen) || ports.IsNonRetryable(err) {
			t.Fatalf("expected retryable ErrCircuitOpen, got %v", err)
		}
		if mockGateway.RefundCalls("ref_cb_open") != 0 {
			t.Fatalf("expected 0 calls when circuit open, got %d", mockGateway.RefundCalls("ref_cb_open"))
		}
		if emitted := publisher.list(); len(emitted) != 0 {
			t.Fatalf("open circuit must not emit GatewayRefundDeclined, got %+v", emitted)
		}
	})
}

func TestAdapterHandleCheckStatusOutcomes(t *testing.T) {
	cases := []struct {
		name          string
		behavior      gateway.MockBehavior
		wantEventType string
	}{
		{"approved status", gateway.MockBehavior{StatusOutcome: ports.GatewayOutcomeApproved}, events.EventGatewayPaymentApproved},
		{"declined status", gateway.MockBehavior{StatusOutcome: ports.GatewayOutcomeDeclined, DeclineReason: "EXPIRED"}, events.EventGatewayPaymentDeclined},
		{"not found status", gateway.MockBehavior{StatusOutcome: ports.GatewayOutcomeNotFound}, events.EventGatewayPaymentDeclined},
		{"pending status", gateway.MockBehavior{StatusOutcome: ports.GatewayOutcomePending}, events.EventGatewayStatusPending},
		{"unavailable error maps to pending", gateway.MockBehavior{AlwaysUnavailable: true}, events.EventGatewayStatusPending},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			publisher := &capturePublisher{}
			adapter := gateway.NewAdapter(gateway.AdapterConfig{
				Gateway:   gateway.NewMockGateway(tc.behavior),
				Publisher: publisher,
			})

			err := adapter.HandleCommand(context.Background(), mustCheckStatusEnvelope(t, "pay_status"))

			if err != nil {
				t.Fatalf("HandleCommand failed: %v", err)
			}
			emitted := publisher.list()
			if len(emitted) != 1 || emitted[0].Type != tc.wantEventType {
				t.Fatalf("got %+v, want event type %s", emitted, tc.wantEventType)
			}
		})
	}
}
