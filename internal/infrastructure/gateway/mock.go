package gateway

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/besednjak/draftea-payments-challenge/internal/application/ports"
)

var (
	// ErrInvalidWebhookSignature is returned when a webhook fails HMAC verification.
	ErrInvalidWebhookSignature = errors.New("invalid webhook HMAC signature")
)

// SignWebhook computes the hex-encoded HMAC-SHA256 signature for a raw webhook payload.
func SignWebhook(secret string, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

// VerifyWebhook validates signature against secret and payload using constant-time comparison.
func VerifyWebhook(secret string, payload []byte, signature string) bool {
	expected := SignWebhook(secret, payload)
	return hmac.Equal([]byte(expected), []byte(signature))
}

// MockBehavior scripts how MockGateway responds for a payment (or by default).
type MockBehavior struct {
	// FailFirstNTimeouts causes the first N SubmitPayment calls to return ErrGatewayTimeout.
	FailFirstNTimeouts int
	// AlwaysTimeout causes every SubmitPayment call to return ErrGatewayTimeout.
	AlwaysTimeout bool
	// AlwaysUnavailable causes every SubmitPayment call to return ErrGatewayUnavailable.
	AlwaysUnavailable bool
	// Latency adds artificial delay before responding (respecting ctx cancellation).
	Latency time.Duration
	// SubmitOutcome is the outcome when SubmitPayment succeeds (default: APPROVED).
	SubmitOutcome ports.GatewayOutcome
	// StatusOutcome is the outcome returned by CheckStatus during reconciliation (default: APPROVED).
	StatusOutcome ports.GatewayOutcome
	// DeclineReason is attached when outcome is DECLINED.
	DeclineReason string
	// ProviderRef overrides the generated provider reference.
	ProviderRef string
	// RefundDeclined causes RefundPayment to return Approved=false.
	RefundDeclined bool
}

// MockGateway is a configurable in-memory external payment provider simulator.
type MockGateway struct {
	mu           sync.Mutex
	defaultRule  MockBehavior
	rules        map[string]MockBehavior
	submitCalls  map[string]int
	statusCalls  map[string]int
	refundCalls  map[string]int
	unhealthyErr error
}

// NewMockGateway creates a MockGateway that approves payments by default.
func NewMockGateway(defaultRule MockBehavior) *MockGateway {
	if defaultRule.SubmitOutcome == "" {
		defaultRule.SubmitOutcome = ports.GatewayOutcomeApproved
	}
	if defaultRule.StatusOutcome == "" {
		defaultRule.StatusOutcome = ports.GatewayOutcomeApproved
	}
	return &MockGateway{
		defaultRule: defaultRule,
		rules:       make(map[string]MockBehavior),
		submitCalls: make(map[string]int),
		statusCalls: make(map[string]int),
		refundCalls: make(map[string]int),
	}
}

// SetRule overrides the behavior for a specific paymentID.
func (m *MockGateway) SetRule(paymentID string, rule MockBehavior) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if rule.SubmitOutcome == "" {
		rule.SubmitOutcome = m.defaultRule.SubmitOutcome
	}
	if rule.StatusOutcome == "" {
		rule.StatusOutcome = m.defaultRule.StatusOutcome
	}
	m.rules[paymentID] = rule
}

// SetDefaultRule updates the fallback behavior for payments without a specific rule.
func (m *MockGateway) SetDefaultRule(rule MockBehavior) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if rule.SubmitOutcome == "" {
		rule.SubmitOutcome = ports.GatewayOutcomeApproved
	}
	if rule.StatusOutcome == "" {
		rule.StatusOutcome = ports.GatewayOutcomeApproved
	}
	m.defaultRule = rule
}

// SetHealthy toggles the health check status of the mock gateway.
func (m *MockGateway) SetHealthy(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unhealthyErr = err
}

func (m *MockGateway) SubmitCalls(paymentID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.submitCalls[paymentID]
}

func (m *MockGateway) StatusCalls(paymentID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.statusCalls[paymentID]
}

func (m *MockGateway) ruleForLocked(paymentID string) MockBehavior {
	if rule, exists := m.rules[paymentID]; exists {
		return rule
	}
	return m.defaultRule
}

// SubmitPayment simulates an idempotent POST /payments call to the external provider.
func (m *MockGateway) SubmitPayment(ctx context.Context, req ports.GatewaySubmitRequest) (ports.GatewaySubmitResult, error) {
	m.mu.Lock()
	m.submitCalls[req.PaymentID]++
	attempt := m.submitCalls[req.PaymentID]
	rule := m.ruleForLocked(req.PaymentID)
	m.mu.Unlock()

	if rule.Latency > 0 {
		if err := defaultSleep(ctx, rule.Latency); err != nil {
			return ports.GatewaySubmitResult{}, ports.ErrGatewayTimeout
		}
	}
	if err := ctx.Err(); err != nil {
		return ports.GatewaySubmitResult{}, ports.ErrGatewayTimeout
	}

	if rule.AlwaysTimeout || attempt <= rule.FailFirstNTimeouts {
		return ports.GatewaySubmitResult{}, ports.ErrGatewayTimeout
	}
	if rule.AlwaysUnavailable {
		return ports.GatewaySubmitResult{}, ports.ErrGatewayUnavailable
	}

	ref := rule.ProviderRef
	if ref == "" {
		ref = fmt.Sprintf("prov_%s", req.PaymentID)
	}
	return ports.GatewaySubmitResult{
		Outcome:     rule.SubmitOutcome,
		ProviderRef: ref,
		Reason:      rule.DeclineReason,
	}, nil
}

// CheckStatus simulates GET /payments/{id} for reconciliation during GATEWAY_UNKNOWN.
func (m *MockGateway) CheckStatus(ctx context.Context, paymentID, _ string) (ports.GatewayStatusResult, error) {
	m.mu.Lock()
	m.statusCalls[paymentID]++
	rule := m.ruleForLocked(paymentID)
	m.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return ports.GatewayStatusResult{}, ports.ErrGatewayTimeout
	}
	if rule.AlwaysUnavailable {
		return ports.GatewayStatusResult{}, ports.ErrGatewayUnavailable
	}

	ref := rule.ProviderRef
	if ref == "" {
		ref = fmt.Sprintf("prov_%s", paymentID)
	}
	return ports.GatewayStatusResult{
		Outcome:     rule.StatusOutcome,
		ProviderRef: ref,
		Reason:      rule.DeclineReason,
	}, nil
}

// RefundPayment simulates POST /refunds using RefundID as idempotency key.
func (m *MockGateway) RefundPayment(ctx context.Context, req ports.GatewayRefundRequest) (ports.GatewayRefundResult, error) {
	m.mu.Lock()
	m.refundCalls[req.RefundID]++
	rule := m.ruleForLocked(req.PaymentID)
	m.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return ports.GatewayRefundResult{}, ports.ErrGatewayTimeout
	}
	if rule.RefundDeclined {
		return ports.GatewayRefundResult{
			Approved: false,
			Reason:   "REFUND_DECLINED",
		}, nil
	}
	ref := req.ProviderRef
	if ref == "" {
		ref = fmt.Sprintf("prov_ref_%s", req.RefundID)
	}
	return ports.GatewayRefundResult{
		Approved:    true,
		ProviderRef: ref,
	}, nil
}

// Name implements ports.HealthChecker.
func (m *MockGateway) Name() string { return "gateway" }

// Check implements ports.HealthChecker.
func (m *MockGateway) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.unhealthyErr
}
