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
	initiatedPayment, err := payment.Initiate(payment.InitiateParams{
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
	return initiatedPayment
}

func TestPaymentInitiateValidation(t *testing.T) {
	validParams := payment.InitiateParams{
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
		{"missing payment ID", func(params *payment.InitiateParams) { params.PaymentID = "" }, payment.ErrInvalidPaymentID},
		{"missing wallet ID", func(params *payment.InitiateParams) { params.WalletID = "" }, payment.ErrInvalidWalletID},
		{"missing user ID", func(params *payment.InitiateParams) { params.UserID = "" }, payment.ErrInvalidUserID},
		{"missing service ID", func(params *payment.InitiateParams) { params.ServiceID = "" }, payment.ErrInvalidServiceID},
		{"missing provider", func(params *payment.InitiateParams) { params.Provider = "" }, payment.ErrInvalidProvider},
		{"invalid amount", func(params *payment.InitiateParams) { params.Amount.Amount = 0 }, events.ErrInvalidAmount},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			params := validParams
			tc.mutate(&params)

			_, err := payment.Initiate(params, events.Metadata{})

			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("got %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestPaymentHappyPathPreservesCreatedAtAndRehydrates(t *testing.T) {
	pay := mustInitiatePayment(t, 15000)
	initialCreatedAt := pay.CreatedAt()
	meta := events.Metadata{CorrelationID: "pay_1"}

	if initialCreatedAt.IsZero() {
		t.Fatal("expected CreatedAt to be set on Initiate")
	}
	if pay.Status() != payment.StatusPending || !pay.Status().IsActiveSaga() || pay.Status().IsTerminal() {
		t.Fatalf("unexpected initial status: %s", pay.Status())
	}

	if err := pay.MarkFundsReserved(35000, meta); err != nil {
		t.Fatalf("MarkFundsReserved failed: %v", err)
	}
	if err := pay.MarkSubmitted("prov_ref_123", meta); err != nil {
		t.Fatalf("MarkSubmitted failed: %v", err)
	}
	if err := pay.MarkCompleted("", meta); err != nil {
		t.Fatalf("MarkCompleted failed: %v", err)
	}

	if pay.Status() != payment.StatusCompleted || pay.CapturedAmount() != 15000 || pay.ProviderRef() != "prov_ref_123" {
		t.Fatalf("unexpected completed state: status=%s captured=%d ref=%q", pay.Status(), pay.CapturedAmount(), pay.ProviderRef())
	}
	if !pay.CreatedAt().Equal(initialCreatedAt) {
		t.Fatalf("CreatedAt changed across transitions: got %v, want %v", pay.CreatedAt(), initialCreatedAt)
	}
	if pay.Status().IsActiveSaga() {
		t.Fatal("COMPLETED should not be treated as an active in-flight saga")
	}

	history := pay.UncommittedEvents()
	rebuilt, err := payment.Rehydrate(history)
	if err != nil {
		t.Fatalf("Rehydrate failed: %v", err)
	}
	if rebuilt.Status() != pay.Status() || rebuilt.CapturedAmount() != pay.CapturedAmount() || !rebuilt.CreatedAt().Equal(initialCreatedAt) {
		t.Fatalf("rehydrated mismatch: status=%s captured=%d createdAt=%v",
			rebuilt.Status(), rebuilt.CapturedAmount(), rebuilt.CreatedAt())
	}
}

func TestPaymentRejectedPathIsTerminal(t *testing.T) {
	pay := mustInitiatePayment(t, 10000)
	meta := events.Metadata{CorrelationID: "pay_1"}

	if err := pay.MarkRejected(events.ReasonInsufficientFunds, meta); err != nil {
		t.Fatalf("MarkRejected failed: %v", err)
	}
	if pay.Status() != payment.StatusRejected || !pay.Status().IsTerminal() || pay.FailureReason() != events.ReasonInsufficientFunds {
		t.Fatalf("unexpected rejected state: status=%s reason=%q", pay.Status(), pay.FailureReason())
	}
	if err := pay.MarkFundsReserved(0, meta); !errors.Is(err, payment.ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition from REJECTED, got %v", err)
	}
	if err := pay.MarkCompleted("ref", meta); !errors.Is(err, payment.ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition from REJECTED, got %v", err)
	}
}

func TestPaymentTimeoutReconciliationAndManualReview(t *testing.T) {
	t.Run("timeout -> GATEWAY_UNKNOWN -> MANUAL_REVIEW -> COMPLETED", func(t *testing.T) {
		pay := mustInitiatePayment(t, 5000)
		meta := events.Metadata{CorrelationID: "pay_1"}

		if err := pay.MarkFundsReserved(1000, meta); err != nil {
			t.Fatalf("MarkFundsReserved failed: %v", err)
		}
		if err := pay.MarkGatewayUnknown(events.ReasonGatewayTimeout, meta); err != nil {
			t.Fatalf("MarkGatewayUnknown failed: %v", err)
		}
		if err := pay.MarkManualReview(events.ReasonReconcileExpired, meta); err != nil {
			t.Fatalf("MarkManualReview failed: %v", err)
		}
		if pay.Status() != payment.StatusManualReview || pay.Status().IsActiveSaga() {
			t.Fatalf("unexpected manual review status: %s", pay.Status())
		}
		if err := pay.MarkCompleted("prov_manual_ok", meta); err != nil {
			t.Fatalf("MarkCompleted from MANUAL_REVIEW failed: %v", err)
		}
		if pay.Status() != payment.StatusCompleted || pay.FailureReason() != "" {
			t.Fatalf("unexpected completed state: status=%s reason=%q", pay.Status(), pay.FailureReason())
		}
	})

	t.Run("SUBMITTED -> GATEWAY_UNKNOWN -> FAILED", func(t *testing.T) {
		pay := mustInitiatePayment(t, 5000)
		meta := events.Metadata{CorrelationID: "pay_1"}

		if err := pay.MarkFundsReserved(1000, meta); err != nil {
			t.Fatalf("MarkFundsReserved failed: %v", err)
		}
		if err := pay.MarkSubmitted("ref_1", meta); err != nil {
			t.Fatalf("MarkSubmitted failed: %v", err)
		}
		if err := pay.MarkGatewayUnknown("webhook_timeout", meta); err != nil {
			t.Fatalf("MarkGatewayUnknown from SUBMITTED failed: %v", err)
		}
		if err := pay.MarkFailed(events.ReasonGatewayDeclined, meta); err != nil {
			t.Fatalf("MarkFailed from GATEWAY_UNKNOWN failed: %v", err)
		}
		if pay.Status() != payment.StatusFailed || !pay.Status().IsTerminal() {
			t.Fatalf("unexpected failed state: %s", pay.Status())
		}
	})
}

func TestPaymentPartialAndFullRefunds(t *testing.T) {
	pay := mustInitiatePayment(t, 10000)
	meta := events.Metadata{CorrelationID: "pay_1"}

	if err := pay.RecordRefund("ref_early", events.Money{Amount: 1000, Currency: "ARS"}, meta); !errors.Is(err, payment.ErrInvalidTransition) {
		t.Fatalf("got %v, want ErrInvalidTransition", err)
	}
	if err := pay.MarkFundsReserved(0, meta); err != nil {
		t.Fatalf("MarkFundsReserved failed: %v", err)
	}
	if err := pay.MarkCompleted("prov_sync_ok", meta); err != nil {
		t.Fatalf("MarkCompleted failed: %v", err)
	}

	if err := pay.RecordRefund("ref_usd", events.Money{Amount: 1000, Currency: "USD"}, meta); !errors.Is(err, payment.ErrCurrencyMismatch) {
		t.Fatalf("got %v, want ErrCurrencyMismatch", err)
	}
	if err := pay.RecordRefund("ref_1", events.Money{Amount: 4000, Currency: "ARS"}, meta); err != nil {
		t.Fatalf("partial refund failed: %v", err)
	}
	if pay.Status() != payment.StatusCompleted || pay.RefundedTotal() != 4000 || pay.RefundableAmount() != 6000 {
		t.Fatalf("unexpected state after partial refund: status=%s refunded=%d refundable=%d",
			pay.Status(), pay.RefundedTotal(), pay.RefundableAmount())
	}

	versionBeforeDup := pay.Version()
	if err := pay.RecordRefund("ref_1", events.Money{Amount: 4000, Currency: "ARS"}, meta); err != nil {
		t.Fatalf("duplicate refund failed: %v", err)
	}
	if pay.Version() != versionBeforeDup || pay.RefundedTotal() != 4000 {
		t.Fatalf("duplicate refund altered state: version=%d refunded=%d", pay.Version(), pay.RefundedTotal())
	}

	if err := pay.RecordRefund("ref_too_much", events.Money{Amount: 6001, Currency: "ARS"}, meta); !errors.Is(err, payment.ErrRefundExceedsCaptured) {
		t.Fatalf("got %v, want ErrRefundExceedsCaptured", err)
	}
	if err := pay.RecordRefund("ref_2", events.Money{Amount: 6000, Currency: "ARS"}, meta); err != nil {
		t.Fatalf("final refund failed: %v", err)
	}
	if pay.Status() != payment.StatusRefunded || !pay.Status().IsTerminal() || pay.RefundableAmount() != 0 {
		t.Fatalf("unexpected state after full refund: status=%s refundable=%d", pay.Status(), pay.RefundableAmount())
	}

	// Rehydrating preserves refunded total and terminal status.
	rebuilt, err := payment.Rehydrate(pay.UncommittedEvents())
	if err != nil {
		t.Fatalf("Rehydrate after refunds failed: %v", err)
	}
	if rebuilt.Status() != payment.StatusRefunded || rebuilt.RefundedTotal() != 10000 {
		t.Fatalf("rehydrated refund mismatch: status=%s refunded=%d", rebuilt.Status(), rebuilt.RefundedTotal())
	}
}

func TestPaymentInFlightRefundReservationAndDecline(t *testing.T) {
	pay := mustInitiatePayment(t, 10000)
	meta := events.Metadata{CorrelationID: "pay_1"}
	if err := pay.MarkFundsReserved(0, meta); err != nil {
		t.Fatalf("MarkFundsReserved failed: %v", err)
	}
	if err := pay.MarkCompleted("prov_1", meta); err != nil {
		t.Fatalf("MarkCompleted failed: %v", err)
	}

	if err := pay.RequestRefund("ref_1", events.Money{Amount: 7000, Currency: "ARS"}, meta); err != nil {
		t.Fatalf("RequestRefund failed: %v", err)
	}
	if pay.PendingRefundTotal() != 7000 || pay.RefundableAmount() != 3000 || !pay.HasPendingRefund("ref_1") {
		t.Fatalf("unexpected pending refund state: pending=%d refundable=%d", pay.PendingRefundTotal(), pay.RefundableAmount())
	}

	// Second concurrent refund exceeding remaining refundable (3000) must fail.
	if err := pay.RequestRefund("ref_2", events.Money{Amount: 7000, Currency: "ARS"}, meta); !errors.Is(err, payment.ErrRefundExceedsCaptured) {
		t.Fatalf("got %v, want ErrRefundExceedsCaptured", err)
	}

	// Explicit gateway decline frees the in-flight reservation.
	if err := pay.DeclineRefund("ref_1", "REFUND_DECLINED", meta); err != nil {
		t.Fatalf("DeclineRefund failed: %v", err)
	}
	if pay.PendingRefundTotal() != 0 || pay.RefundableAmount() != 10000 || pay.HasPendingRefund("ref_1") {
		t.Fatalf("expected reservation released after decline: pending=%d refundable=%d", pay.PendingRefundTotal(), pay.RefundableAmount())
	}

	// Subsequent refund up to full captured amount succeeds and settles cleanly.
	if err := pay.RequestRefund("ref_2", events.Money{Amount: 7000, Currency: "ARS"}, meta); err != nil {
		t.Fatalf("RequestRefund ref_2 failed: %v", err)
	}
	if err := pay.RecordRefund("ref_2", events.Money{Amount: 7000, Currency: "ARS"}, meta); err != nil {
		t.Fatalf("RecordRefund ref_2 failed: %v", err)
	}
	if pay.PendingRefundTotal() != 0 || pay.RefundedTotal() != 7000 || pay.RefundableAmount() != 3000 {
		t.Fatalf("unexpected state after settling ref_2: pending=%d refunded=%d refundable=%d",
			pay.PendingRefundTotal(), pay.RefundedTotal(), pay.RefundableAmount())
	}
}

func TestPaymentInvalidTransitionsAndBaseVersion(t *testing.T) {
	pay := mustInitiatePayment(t, 5000)
	pay.ClearUncommitted()

	if pay.BaseVersion() != 1 {
		t.Fatalf("expected BaseVersion=1, got %d", pay.BaseVersion())
	}
	if err := pay.MarkSubmitted("ref", events.Metadata{}); !errors.Is(err, payment.ErrInvalidTransition) {
		t.Fatalf("PENDING -> SUBMITTED should fail, got %v", err)
	}
	if err := pay.MarkCompleted("ref", events.Metadata{}); !errors.Is(err, payment.ErrInvalidTransition) {
		t.Fatalf("PENDING -> COMPLETED should fail, got %v", err)
	}
	if err := pay.MarkFailed("reason", events.Metadata{}); !errors.Is(err, payment.ErrInvalidTransition) {
		t.Fatalf("PENDING -> FAILED should fail, got %v", err)
	}
}
