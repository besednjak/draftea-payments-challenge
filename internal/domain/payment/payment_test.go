package payment_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/payment"
)

func deterministicOpts() (payment.Option, payment.Option) {
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	seq := 0
	clock := func() time.Time {
		now = now.Add(time.Second)
		return now
	}
	idGen := func() string {
		seq++
		return fmt.Sprintf("pevt-%03d", seq)
	}
	return payment.WithClock(clock), payment.WithIDGenerator(idGen)
}

func mustInitiatePayment(t *testing.T, amount int64) *payment.Payment {
	t.Helper()
	clockOpt, idOpt := deterministicOpts()
	p, err := payment.Initiate(payment.InitiateParams{
		PaymentID: "pay_1",
		WalletID:  "wal_1",
		UserID:    "usr_1",
		ServiceID: "svc_electricity",
		Provider:  "mockpay",
		Amount:    events.Money{Amount: amount, Currency: "ARS"},
	}, events.Metadata{}, clockOpt, idOpt)
	if err != nil {
		t.Fatalf("Initiate failed: %v", err)
	}
	return p
}

func TestPaymentInitiateValidation(t *testing.T) {
	valid := payment.InitiateParams{
		PaymentID: "pay_1",
		WalletID:  "wal_1",
		UserID:    "usr_1",
		ServiceID: "svc_1",
		Provider:  "mockpay",
		Amount:    events.Money{Amount: 5000, Currency: "ARS"},
	}

	cases := []struct {
		name    string
		mutate  func(*payment.InitiateParams)
		wantErr error
	}{
		{"missing payment id", func(p *payment.InitiateParams) { p.PaymentID = "" }, payment.ErrInvalidPaymentID},
		{"missing wallet id", func(p *payment.InitiateParams) { p.WalletID = "" }, payment.ErrInvalidWalletID},
		{"missing user id", func(p *payment.InitiateParams) { p.UserID = "" }, payment.ErrInvalidUserID},
		{"missing service id", func(p *payment.InitiateParams) { p.ServiceID = "" }, payment.ErrInvalidServiceID},
		{"missing provider", func(p *payment.InitiateParams) { p.Provider = "" }, payment.ErrInvalidProvider},
		{"invalid amount", func(p *payment.InitiateParams) { p.Amount.Amount = 0 }, events.ErrInvalidAmount},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			params := valid
			tc.mutate(&params)
			if _, err := payment.Initiate(params, events.Metadata{}); !errors.Is(err, tc.wantErr) {
				t.Fatalf("got %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestPaymentHappyPathAndRehydrate(t *testing.T) {
	p := mustInitiatePayment(t, 15000)
	if p.Status() != payment.StatusPending || !p.Status().IsActiveSaga() || p.Status().IsTerminal() {
		t.Fatalf("unexpected initial status: %s", p.Status())
	}

	meta := events.Metadata{CorrelationID: "pay_1"}
	if err := p.MarkFundsReserved(35000, meta); err != nil {
		t.Fatalf("MarkFundsReserved failed: %v", err)
	}
	if p.Status() != payment.StatusFundsReserved {
		t.Fatalf("got %s, want FUNDS_RESERVED", p.Status())
	}

	if err := p.MarkSubmitted("prov_ref_123", meta); err != nil {
		t.Fatalf("MarkSubmitted failed: %v", err)
	}
	if p.Status() != payment.StatusSubmitted || p.ProviderRef() != "prov_ref_123" {
		t.Fatalf("unexpected submitted state: status=%s ref=%q", p.Status(), p.ProviderRef())
	}

	if err := p.MarkCompleted("", meta); err != nil {
		t.Fatalf("MarkCompleted failed: %v", err)
	}
	if p.Status() != payment.StatusCompleted || p.CapturedAmount() != 15000 || p.ProviderRef() != "prov_ref_123" {
		t.Fatalf("unexpected completed state: status=%s captured=%d ref=%q", p.Status(), p.CapturedAmount(), p.ProviderRef())
	}
	if p.Status().IsActiveSaga() {
		t.Fatal("COMPLETED should not be treated as an active in-flight saga")
	}

	history := p.UncommittedEvents()
	if len(history) != 4 {
		t.Fatalf("expected 4 events, got %d", len(history))
	}
	if history[0].CorrelationID != "pay_1" {
		t.Fatalf("expected default correlation_id=pay_1, got %q", history[0].CorrelationID)
	}

	rebuilt, err := payment.Rehydrate(history)
	if err != nil {
		t.Fatalf("Rehydrate failed: %v", err)
	}
	if rebuilt.Status() != p.Status() || rebuilt.CapturedAmount() != p.CapturedAmount() || rebuilt.Version() != p.Version() {
		t.Fatalf("rehydrated mismatch: got (%s, %d, v%d) want (%s, %d, v%d)",
			rebuilt.Status(), rebuilt.CapturedAmount(), rebuilt.Version(),
			p.Status(), p.CapturedAmount(), p.Version())
	}
}

func TestPaymentRejectedPathIsTerminal(t *testing.T) {
	p := mustInitiatePayment(t, 10000)
	meta := events.Metadata{CorrelationID: "pay_1"}

	if err := p.MarkRejected(events.ReasonInsufficientFunds, meta); err != nil {
		t.Fatalf("MarkRejected failed: %v", err)
	}
	if p.Status() != payment.StatusRejected || !p.Status().IsTerminal() || p.FailureReason() != events.ReasonInsufficientFunds {
		t.Fatalf("unexpected rejected state: status=%s reason=%q", p.Status(), p.FailureReason())
	}

	if err := p.MarkFundsReserved(0, meta); !errors.Is(err, payment.ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition from REJECTED, got %v", err)
	}
	if err := p.MarkCompleted("ref", meta); !errors.Is(err, payment.ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition from REJECTED, got %v", err)
	}
}

func TestPaymentTimeoutReconciliationAndManualReview(t *testing.T) {
	t.Run("timeout -> gateway_unknown -> manual_review -> completed", func(t *testing.T) {
		p := mustInitiatePayment(t, 5000)
		meta := events.Metadata{CorrelationID: "pay_1"}

		if err := p.MarkFundsReserved(1000, meta); err != nil {
			t.Fatalf("MarkFundsReserved failed: %v", err)
		}
		if err := p.MarkGatewayUnknown(events.ReasonGatewayTimeout, meta); err != nil {
			t.Fatalf("MarkGatewayUnknown failed: %v", err)
		}
		if p.Status() != payment.StatusGatewayUnknown || !p.Status().IsActiveSaga() {
			t.Fatalf("unexpected status %s", p.Status())
		}
		if err := p.MarkManualReview(events.ReasonReconcileExpired, meta); err != nil {
			t.Fatalf("MarkManualReview failed: %v", err)
		}
		if p.Status() != payment.StatusManualReview || p.Status().IsActiveSaga() {
			t.Fatalf("unexpected manual review status %s", p.Status())
		}
		if err := p.MarkCompleted("prov_manual_ok", meta); err != nil {
			t.Fatalf("MarkCompleted from MANUAL_REVIEW failed: %v", err)
		}
		if p.Status() != payment.StatusCompleted || p.FailureReason() != "" {
			t.Fatalf("unexpected completed state: status=%s reason=%q", p.Status(), p.FailureReason())
		}
	})

	t.Run("submitted -> gateway_unknown -> failed", func(t *testing.T) {
		p := mustInitiatePayment(t, 5000)
		meta := events.Metadata{CorrelationID: "pay_1"}

		_ = p.MarkFundsReserved(1000, meta)
		_ = p.MarkSubmitted("ref_1", meta)
		if err := p.MarkGatewayUnknown("webhook_timeout", meta); err != nil {
			t.Fatalf("MarkGatewayUnknown from SUBMITTED failed: %v", err)
		}
		if err := p.MarkFailed(events.ReasonGatewayDeclined, meta); err != nil {
			t.Fatalf("MarkFailed from GATEWAY_UNKNOWN failed: %v", err)
		}
		if p.Status() != payment.StatusFailed || !p.Status().IsTerminal() {
			t.Fatalf("unexpected failed state: %s", p.Status())
		}
	})
}

func TestPaymentPartialAndFullRefunds(t *testing.T) {
	p := mustInitiatePayment(t, 10000)
	meta := events.Metadata{CorrelationID: "pay_1"}

	// Cannot refund before COMPLETED.
	if err := p.RecordRefund("ref_early", events.Money{Amount: 1000, Currency: "ARS"}, meta); !errors.Is(err, payment.ErrInvalidTransition) {
		t.Fatalf("got %v, want ErrInvalidTransition", err)
	}

	_ = p.MarkFundsReserved(0, meta)
	_ = p.MarkCompleted("prov_sync_ok", meta)

	// Currency mismatch rejected.
	if err := p.RecordRefund("ref_usd", events.Money{Amount: 1000, Currency: "USD"}, meta); !errors.Is(err, payment.ErrCurrencyMismatch) {
		t.Fatalf("got %v, want ErrCurrencyMismatch", err)
	}

	// Partial refund keeps status COMPLETED.
	if err := p.RecordRefund("ref_1", events.Money{Amount: 4000, Currency: "ARS"}, meta); err != nil {
		t.Fatalf("partial refund failed: %v", err)
	}
	if p.Status() != payment.StatusCompleted || p.RefundedTotal() != 4000 || p.RefundableAmount() != 6000 {
		t.Fatalf("unexpected state after partial refund: status=%s refunded=%d refundable=%d",
			p.Status(), p.RefundedTotal(), p.RefundableAmount())
	}

	// Duplicate refundID is idempotent.
	vBeforeDup := p.Version()
	if err := p.RecordRefund("ref_1", events.Money{Amount: 4000, Currency: "ARS"}, meta); err != nil {
		t.Fatalf("duplicate refund failed: %v", err)
	}
	if p.Version() != vBeforeDup || p.RefundedTotal() != 4000 {
		t.Fatalf("duplicate refund altered state: v=%d refunded=%d", p.Version(), p.RefundedTotal())
	}

	// Refund exceeding remaining captured amount is rejected.
	if err := p.RecordRefund("ref_too_much", events.Money{Amount: 6001, Currency: "ARS"}, meta); !errors.Is(err, payment.ErrRefundExceedsCaptured) {
		t.Fatalf("got %v, want ErrRefundExceedsCaptured", err)
	}

	// Remaining refund transitions to terminal REFUNDED.
	if err := p.RecordRefund("ref_2", events.Money{Amount: 6000, Currency: "ARS"}, meta); err != nil {
		t.Fatalf("final refund failed: %v", err)
	}
	if p.Status() != payment.StatusRefunded || !p.Status().IsTerminal() || p.RefundableAmount() != 0 {
		t.Fatalf("unexpected state after full refund: status=%s refundable=%d", p.Status(), p.RefundableAmount())
	}

	// Rehydrating preserves refunded total and terminal status.
	rebuilt, err := payment.Rehydrate(p.UncommittedEvents())
	if err != nil {
		t.Fatalf("Rehydrate after refunds failed: %v", err)
	}
	if rebuilt.Status() != payment.StatusRefunded || rebuilt.RefundedTotal() != 10000 {
		t.Fatalf("rehydrated refund mismatch: status=%s refunded=%d", rebuilt.Status(), rebuilt.RefundedTotal())
	}
}

func TestPaymentInvalidTransitionsAndBaseVersion(t *testing.T) {
	p := mustInitiatePayment(t, 5000)
	p.ClearUncommitted()
	if p.BaseVersion() != 1 {
		t.Fatalf("expected BaseVersion=1, got %d", p.BaseVersion())
	}

	if err := p.MarkSubmitted("ref", events.Metadata{}); !errors.Is(err, payment.ErrInvalidTransition) {
		t.Fatalf("PENDING -> SUBMITTED should fail, got %v", err)
	}
	if err := p.MarkCompleted("ref", events.Metadata{}); !errors.Is(err, payment.ErrInvalidTransition) {
		t.Fatalf("PENDING -> COMPLETED should fail, got %v", err)
	}
	if err := p.MarkFailed("reason", events.Metadata{}); !errors.Is(err, payment.ErrInvalidTransition) {
		t.Fatalf("PENDING -> FAILED should fail, got %v", err)
	}
}
