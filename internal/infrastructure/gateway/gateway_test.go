package gateway_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/besednjak/draftea-payments-challenge/internal/application/ports"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
	"github.com/besednjak/draftea-payments-challenge/internal/infrastructure/eventstore"
	"github.com/besednjak/draftea-payments-challenge/internal/infrastructure/gateway"
)

type capturePublisher struct {
	mu   sync.Mutex
	envs []events.Envelope
}

func (c *capturePublisher) Publish(_ context.Context, _, _ string, env events.Envelope) error {
	c.mu.Lock()
	c.envs = append(c.envs, env)
	c.mu.Unlock()
	return nil
}

func (c *capturePublisher) list() []events.Envelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]events.Envelope, len(c.envs))
	copy(out, c.envs)
	return out
}

func TestCircuitBreakerConsecutiveTimeoutsAndRecovery(t *testing.T) {
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	var transitions []string

	cb := gateway.NewCircuitBreaker(gateway.CircuitBreakerConfig{
		Provider:            "mockpay",
		ConsecutiveTimeouts: 5,
		OpenTimeout:         30 * time.Second,
		HalfOpenMaxProbes:   3,
		Clock:               func() time.Time { return now },
		OnStateChange: func(_ string, from, to gateway.CircuitState) {
			transitions = append(transitions, string(from)+"->"+string(to))
		},
	})

	// 4 consecutive timeouts keep circuit CLOSED.
	for i := 0; i < 4; i++ {
		if err := cb.Allow(); err != nil {
			t.Fatalf("unexpected Allow error at %d: %v", i, err)
		}
		cb.RecordFailure(true)
	}
	if cb.State() != gateway.StateClosed {
		t.Fatalf("expected CLOSED after 4 timeouts, got %s", cb.State())
	}

	// 5th consecutive timeout trips CLOSED -> OPEN.
	cb.RecordFailure(true)
	if cb.State() != gateway.StateOpen {
		t.Fatalf("expected OPEN after 5 timeouts, got %s", cb.State())
	}
	if err := cb.Allow(); !errors.Is(err, ports.ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen while OPEN, got %v", err)
	}

	// Advance clock by 30s: OPEN -> HALF_OPEN on Allow.
	now = now.Add(30 * time.Second)
	if err := cb.Allow(); err != nil {
		t.Fatalf("expected first half-open probe allowed, got %v", err)
	}
	if cb.State() != gateway.StateHalfOpen {
		t.Fatalf("expected HALF_OPEN, got %s", cb.State())
	}

	// Failed probe immediately re-opens circuit.
	cb.RecordFailure(true)
	if cb.State() != gateway.StateOpen {
		t.Fatalf("expected HALF_OPEN -> OPEN on probe failure, got %s", cb.State())
	}

	// Wait another 30s and pass 3 successful probes -> CLOSED.
	now = now.Add(30 * time.Second)
	for i := 0; i < 3; i++ {
		if err := cb.Allow(); err != nil {
			t.Fatalf("probe %d should be allowed: %v", i+1, err)
		}
		cb.RecordSuccess()
	}
	if cb.State() != gateway.StateClosed {
		t.Fatalf("expected CLOSED after 3 successful probes, got %s", cb.State())
	}

	want := []string{"CLOSED->OPEN", "OPEN->HALF_OPEN", "HALF_OPEN->OPEN", "OPEN->HALF_OPEN", "HALF_OPEN->CLOSED"}
	if len(transitions) != len(want) {
		t.Fatalf("got transitions %v, want %v", transitions, want)
	}
	for i := range want {
		if transitions[i] != want[i] {
			t.Fatalf("transition[%d]=%s, want %s", i, transitions[i], want[i])
		}
	}
}

func TestCircuitBreakerSlidingWindowFailureRate(t *testing.T) {
	cb := gateway.NewCircuitBreaker(gateway.CircuitBreakerConfig{
		WindowSize:           20,
		FailureRateThreshold: 0.5,
		ConsecutiveTimeouts:  100, // disable timeout shortcut to isolate window rule
	})

	// 10 successes + 10 non-timeout failures = 50% (threshold is strictly > 50%).
	for i := 0; i < 10; i++ {
		cb.RecordSuccess()
		cb.RecordFailure(false)
	}
	if cb.State() != gateway.StateClosed {
		t.Fatalf("expected CLOSED at exactly 50%% failure rate, got %s", cb.State())
	}

	// Next failure replaces oldest success -> 11/20 = 55% -> OPEN.
	cb.RecordFailure(false)
	if cb.State() != gateway.StateOpen {
		t.Fatalf("expected OPEN at 55%% failure rate, got %s", cb.State())
	}
}

func TestFullJitterDelayBounds(t *testing.T) {
	base := 500 * time.Millisecond
	capDelay := 10 * time.Second

	if got := gateway.FullJitterDelay(0, base, capDelay, func() float64 { return 1.0 }); got != 500*time.Millisecond {
		t.Fatalf("attempt 0 max: got %v want 500ms", got)
	}
	if got := gateway.FullJitterDelay(2, base, capDelay, func() float64 { return 0.5 }); got != 1*time.Second {
		t.Fatalf("attempt 2 mid: got %v want 1s", got)
	}
	if got := gateway.FullJitterDelay(10, base, capDelay, func() float64 { return 1.0 }); got != capDelay {
		t.Fatalf("attempt 10 capped: got %v want %v", got, capDelay)
	}
}

func TestAdapterOpenCircuitEmitsNotSentWithoutCallingGateway(t *testing.T) {
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	cb := gateway.NewCircuitBreaker(gateway.CircuitBreakerConfig{
		ConsecutiveTimeouts: 1,
		OpenTimeout:         30 * time.Second,
		Clock:               func() time.Time { return now },
	})
	cb.RecordFailure(true) // Trip open before command arrives.

	mockGW := gateway.NewMockGateway(gateway.MockBehavior{SubmitOutcome: ports.GatewayOutcomeApproved})
	pub := &capturePublisher{}
	adapter := gateway.NewAdapter(gateway.AdapterConfig{
		Gateway:   mockGW,
		Publisher: pub,
		Breaker:   cb,
		Retry: gateway.RetryConfig{
			MaxAttempts: 3,
			Sleep:       func(context.Context, time.Duration) error { return nil },
		},
	})

	cmdEnv, _ := events.NewEnvelope(
		events.CommandSubmitPayment,
		events.AggregateTypePayment,
		"pay_cb_open",
		1,
		events.SubmitPaymentCommand{
			PaymentID: "pay_cb_open",
			Provider:  "mockpay",
			ServiceID: "svc_1",
			Amount:    events.Money{Amount: 1000, Currency: "ARS"},
		},
		events.Metadata{CorrelationID: "pay_cb_open"},
		nil,
		nil,
	)

	if err := adapter.HandleCommand(context.Background(), cmdEnv); err != nil {
		t.Fatalf("HandleCommand failed: %v", err)
	}
	if mockGW.SubmitCalls("pay_cb_open") != 0 {
		t.Fatalf("expected 0 outbound calls when circuit is open, got %d", mockGW.SubmitCalls("pay_cb_open"))
	}
	emitted := pub.list()
	if len(emitted) != 1 || emitted[0].Type != events.EventGatewaySubmissionNotSent {
		t.Fatalf("expected GatewaySubmissionNotSent, got %+v", emitted)
	}
}

func TestAdapterRetriesTimeoutsAndNeverReleasesOnMidRetryCircuitTrip(t *testing.T) {
	mockGW := gateway.NewMockGateway(gateway.MockBehavior{AlwaysTimeout: true})
	pub := &capturePublisher{}
	cb := gateway.NewCircuitBreaker(gateway.CircuitBreakerConfig{
		ConsecutiveTimeouts: 2, // Trips after 2nd timeout mid-retry!
		OpenTimeout:         30 * time.Second,
	})
	adapter := gateway.NewAdapter(gateway.AdapterConfig{
		Gateway:   mockGW,
		Publisher: pub,
		Breaker:   cb,
		Inbox:     eventstore.NewInMemoryInboxStore(),
		Retry: gateway.RetryConfig{
			MaxAttempts:    3,
			BaseDelay:      time.Millisecond,
			MaxDelay:       5 * time.Millisecond,
			AttemptTimeout: time.Second,
			Sleep:          func(context.Context, time.Duration) error { return nil },
		},
	})

	cmdEnv, _ := events.NewEnvelope(
		events.CommandSubmitPayment,
		events.AggregateTypePayment,
		"pay_timeout",
		1,
		events.SubmitPaymentCommand{
			PaymentID: "pay_timeout",
			Provider:  "mockpay",
			ServiceID: "svc_1",
			Amount:    events.Money{Amount: 5000, Currency: "ARS"},
		},
		events.Metadata{CorrelationID: "pay_timeout"},
		nil,
		nil,
	)

	if err := adapter.HandleCommand(context.Background(), cmdEnv); err != nil {
		t.Fatalf("HandleCommand failed: %v", err)
	}
	// Attempt 1 and 2 timed out (tripping CB), attempt 3 blocked by CB, MUST emit GatewayOutcomeUnknown (NOT NotSent!).
	if mockGW.SubmitCalls("pay_timeout") != 2 {
		t.Fatalf("expected 2 attempts before CB opened, got %d", mockGW.SubmitCalls("pay_timeout"))
	}
	emitted := pub.list()
	if len(emitted) != 1 || emitted[0].Type != events.EventGatewayOutcomeUnknown {
		t.Fatalf("expected GatewayOutcomeUnknown, got %+v", emitted)
	}
}

func TestAdapterWebhookHMACAndDeduplication(t *testing.T) {
	secret := "super-secret-hmac-key"
	pub := &capturePublisher{}
	adapter := gateway.NewAdapter(gateway.AdapterConfig{
		Gateway:       gateway.NewMockGateway(gateway.MockBehavior{}),
		Publisher:     pub,
		WebhookSecret: secret,
	})

	body, _ := json.Marshal(gateway.WebhookPayload{
		ProviderEventID: "whevt_1",
		PaymentID:       "pay_wh_1",
		Provider:        "mockpay",
		ProviderRef:     "prov_999",
		Status:          string(ports.GatewayOutcomeApproved),
	})

	// Invalid signature must fail.
	if err := adapter.HandleWebhook(context.Background(), "bad-sig", body); !errors.Is(err, gateway.ErrInvalidWebhookSignature) {
		t.Fatalf("expected ErrInvalidWebhookSignature, got %v", err)
	}

	// Valid signature emits GatewayPaymentApproved.
	sig := gateway.SignWebhook(secret, body)
	if err := adapter.HandleWebhook(context.Background(), sig, body); err != nil {
		t.Fatalf("valid webhook failed: %v", err)
	}
	// Duplicate provider_event_id is deduplicated.
	if err := adapter.HandleWebhook(context.Background(), sig, body); err != nil {
		t.Fatalf("duplicate webhook failed: %v", err)
	}

	emitted := pub.list()
	if len(emitted) != 1 || emitted[0].Type != events.EventGatewayPaymentApproved {
		t.Fatalf("expected 1 GatewayPaymentApproved event, got %d", len(emitted))
	}
}
