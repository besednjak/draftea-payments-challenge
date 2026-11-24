package wallet_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/wallet"
)

func deterministicOpts() (wallet.Option, wallet.Option) {
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	seq := 0
	clock := func() time.Time {
		now = now.Add(time.Second)
		return now
	}
	idGen := func() string {
		seq++
		return fmt.Sprintf("evt-%03d", seq)
	}
	return wallet.WithClock(clock), wallet.WithIDGenerator(idGen)
}

func mustCreateFundedWallet(t *testing.T, amount int64) *wallet.Wallet {
	t.Helper()
	clockOpt, idOpt := deterministicOpts()
	w, err := wallet.Create("wal_1", "usr_1", "ARS", events.Metadata{}, clockOpt, idOpt)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if err := w.Deposit("dep_1", events.Money{Amount: amount, Currency: "ARS"}, events.Metadata{}); err != nil {
		t.Fatalf("Deposit failed: %v", err)
	}
	return w
}

func TestWalletCreateValidation(t *testing.T) {
	if _, err := wallet.Create("", "usr_1", "ARS", events.Metadata{}); !errors.Is(err, wallet.ErrInvalidWalletID) {
		t.Fatalf("got %v, want ErrInvalidWalletID", err)
	}
	if _, err := wallet.Create("wal_1", "", "ARS", events.Metadata{}); !errors.Is(err, wallet.ErrInvalidUserID) {
		t.Fatalf("got %v, want ErrInvalidUserID", err)
	}
	if _, err := wallet.Create("wal_1", "usr_1", "INVALID", events.Metadata{}); !errors.Is(err, events.ErrInvalidCurrency) {
		t.Fatalf("got %v, want ErrInvalidCurrency", err)
	}
}

func TestWalletReserveCaptureHappyPath(t *testing.T) {
	w := mustCreateFundedWallet(t, 10000)
	w.ClearUncommitted()

	if w.BaseVersion() != 2 || w.Version() != 2 {
		t.Fatalf("expected version=2 baseVersion=2, got v=%d base=%d", w.Version(), w.BaseVersion())
	}

	meta := events.Metadata{CorrelationID: "pay_1"}
	if err := w.Reserve("pay_1", events.Money{Amount: 4000, Currency: "ARS"}, meta); err != nil {
		t.Fatalf("Reserve failed: %v", err)
	}
	if w.Balance() != 10000 || w.Held() != 4000 || w.Available() != 6000 {
		t.Fatalf("unexpected balances after reserve: balance=%d held=%d avail=%d", w.Balance(), w.Held(), w.Available())
	}
	if w.BaseVersion() != 2 || w.Version() != 3 {
		t.Fatalf("expected baseVersion=2 version=3, got base=%d v=%d", w.BaseVersion(), w.Version())
	}

	// Duplicate Reserve must be an idempotent no-op.
	if err := w.Reserve("pay_1", events.Money{Amount: 4000, Currency: "ARS"}, meta); err != nil {
		t.Fatalf("duplicate Reserve failed: %v", err)
	}
	if w.Version() != 3 || w.Held() != 4000 {
		t.Fatalf("duplicate Reserve altered state: v=%d held=%d", w.Version(), w.Held())
	}

	if err := w.Capture("pay_1", events.Money{Amount: 4000, Currency: "ARS"}, meta); err != nil {
		t.Fatalf("Capture failed: %v", err)
	}
	if w.Balance() != 6000 || w.Held() != 0 || w.Available() != 6000 {
		t.Fatalf("unexpected balances after capture: balance=%d held=%d avail=%d", w.Balance(), w.Held(), w.Available())
	}
	if _, has := w.HasReservation("pay_1"); has {
		t.Fatal("expected reservation to be cleared after capture")
	}

	// Duplicate Capture must be an idempotent no-op.
	if err := w.Capture("pay_1", events.Money{Amount: 4000, Currency: "ARS"}, meta); err != nil {
		t.Fatalf("duplicate Capture failed: %v", err)
	}
	if w.Version() != 4 || w.Balance() != 6000 {
		t.Fatalf("duplicate Capture altered state: v=%d balance=%d", w.Version(), w.Balance())
	}
}

func TestWalletReserveRejections(t *testing.T) {
	w := mustCreateFundedWallet(t, 5000)
	w.ClearUncommitted()

	// Insufficient funds emits FundsReservationRejected and keeps balance intact.
	if err := w.Reserve("pay_big", events.Money{Amount: 8000, Currency: "ARS"}, events.Metadata{CorrelationID: "pay_big"}); err != nil {
		t.Fatalf("unexpected error on insufficient funds: %v", err)
	}
	if w.Balance() != 5000 || w.Held() != 0 || w.Available() != 5000 {
		t.Fatalf("balance changed on rejection: b=%d h=%d a=%d", w.Balance(), w.Held(), w.Available())
	}
	uncommitted := w.UncommittedEvents()
	if len(uncommitted) != 1 || uncommitted[0].Type != events.EventFundsReservationRejected {
		t.Fatalf("expected 1 FundsReservationRejected event, got %+v", uncommitted)
	}
	rej, err := events.DecodePayload[events.FundsReservationRejectedPayload](uncommitted[0])
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if rej.Reason != events.ReasonInsufficientFunds || rej.AvailableBalance != 5000 {
		t.Fatalf("unexpected rejection payload: %+v", rej)
	}

	// Currency mismatch emits FundsReservationRejected with CURRENCY_MISMATCH.
	w.ClearUncommitted()
	if err := w.Reserve("pay_usd", events.Money{Amount: 1000, Currency: "USD"}, events.Metadata{CorrelationID: "pay_usd"}); err != nil {
		t.Fatalf("unexpected error on currency mismatch: %v", err)
	}
	uncommitted = w.UncommittedEvents()
	if len(uncommitted) != 1 || uncommitted[0].Type != events.EventFundsReservationRejected {
		t.Fatalf("expected FundsReservationRejected, got %+v", uncommitted)
	}
	rej, _ = events.DecodePayload[events.FundsReservationRejectedPayload](uncommitted[0])
	if rej.Reason != events.ReasonCurrencyMismatch {
		t.Fatalf("got reason=%q, want %q", rej.Reason, events.ReasonCurrencyMismatch)
	}
}

func TestWalletReleaseAndIdempotency(t *testing.T) {
	w := mustCreateFundedWallet(t, 10000)
	meta := events.Metadata{CorrelationID: "pay_rel"}

	if err := w.Reserve("pay_rel", events.Money{Amount: 3000, Currency: "ARS"}, meta); err != nil {
		t.Fatalf("Reserve failed: %v", err)
	}
	if err := w.Release("pay_rel", events.ReasonCircuitOpen, meta); err != nil {
		t.Fatalf("Release failed: %v", err)
	}
	if w.Balance() != 10000 || w.Held() != 0 || w.Available() != 10000 {
		t.Fatalf("unexpected balances after release: b=%d h=%d a=%d", w.Balance(), w.Held(), w.Available())
	}
	vAfterRelease := w.Version()

	// Duplicate Release must be idempotent.
	if err := w.Release("pay_rel", events.ReasonCircuitOpen, meta); err != nil {
		t.Fatalf("duplicate Release failed: %v", err)
	}
	if w.Version() != vAfterRelease {
		t.Fatalf("duplicate Release emitted extra event: got v=%d want %d", w.Version(), vAfterRelease)
	}

	// Releasing a non-existent reservation returns ErrReservationNotFound.
	if err := w.Release("pay_unknown", "test", meta); !errors.Is(err, wallet.ErrReservationNotFound) {
		t.Fatalf("got err=%v, want ErrReservationNotFound", err)
	}
}

func TestWalletCaptureErrorsAndCannotReleaseCaptured(t *testing.T) {
	w := mustCreateFundedWallet(t, 10000)
	meta := events.Metadata{CorrelationID: "pay_1"}

	if err := w.Capture("pay_missing", events.Money{Amount: 100, Currency: "ARS"}, meta); !errors.Is(err, wallet.ErrReservationNotFound) {
		t.Fatalf("got %v, want ErrReservationNotFound", err)
	}

	if err := w.Reserve("pay_1", events.Money{Amount: 2000, Currency: "ARS"}, meta); err != nil {
		t.Fatalf("Reserve failed: %v", err)
	}
	if err := w.Capture("pay_1", events.Money{Amount: 1500, Currency: "ARS"}, meta); !errors.Is(err, wallet.ErrCaptureAmountMismatch) {
		t.Fatalf("got %v, want ErrCaptureAmountMismatch", err)
	}
	if err := w.Capture("pay_1", events.Money{Amount: 2000, Currency: "USD"}, meta); !errors.Is(err, wallet.ErrCurrencyMismatch) {
		t.Fatalf("got %v, want ErrCurrencyMismatch", err)
	}

	if err := w.Capture("pay_1", events.Money{Amount: 2000, Currency: "ARS"}, meta); err != nil {
		t.Fatalf("Capture failed: %v", err)
	}
	if err := w.Release("pay_1", "too_late", meta); !errors.Is(err, wallet.ErrReservationAlreadyCaptured) {
		t.Fatalf("got %v, want ErrReservationAlreadyCaptured", err)
	}
}

func TestWalletRefundAndDepositIdempotency(t *testing.T) {
	w := mustCreateFundedWallet(t, 10000)
	vAfterCreateAndDeposit := w.Version()

	// Duplicate deposit with same referenceID is a no-op.
	if err := w.Deposit("dep_1", events.Money{Amount: 10000, Currency: "ARS"}, events.Metadata{}); err != nil {
		t.Fatalf("duplicate Deposit failed: %v", err)
	}
	if w.Balance() != 10000 || w.Version() != vAfterCreateAndDeposit {
		t.Fatalf("duplicate Deposit altered state: balance=%d v=%d", w.Balance(), w.Version())
	}

	// Refund credits balance and is idempotent per refundID.
	if err := w.Refund("pay_1", "ref_1", events.Money{Amount: 2500, Currency: "ARS"}, events.Metadata{CorrelationID: "pay_1"}); err != nil {
		t.Fatalf("Refund failed: %v", err)
	}
	if w.Balance() != 12500 {
		t.Fatalf("expected balance=12500, got %d", w.Balance())
	}
	vAfterRefund := w.Version()
	if err := w.Refund("pay_1", "ref_1", events.Money{Amount: 2500, Currency: "ARS"}, events.Metadata{CorrelationID: "pay_1"}); err != nil {
		t.Fatalf("duplicate Refund failed: %v", err)
	}
	if w.Balance() != 12500 || w.Version() != vAfterRefund {
		t.Fatalf("duplicate Refund altered state: balance=%d v=%d", w.Balance(), w.Version())
	}
}

func TestWalletRehydrateFromHistoryAndSnapshot(t *testing.T) {
	w := mustCreateFundedWallet(t, 10000)
	_ = w.Reserve("pay_1", events.Money{Amount: 3000, Currency: "ARS"}, events.Metadata{CorrelationID: "pay_1"})
	snap := w.TakeSnapshot() // at version 3 (balance=10000, held=3000)

	// Mutate snapshot copy to verify isolation.
	snapCopy := snap
	snapCopy.Reservations["pay_fake"] = 999
	if _, ok := w.HasReservation("pay_fake"); ok {
		t.Fatal("snapshot mutation leaked into aggregate")
	}
	delete(snap.Reservations, "pay_fake")

	_ = w.Capture("pay_1", events.Money{Amount: 3000, Currency: "ARS"}, events.Metadata{CorrelationID: "pay_1"})
	_ = w.Reserve("pay_2", events.Money{Amount: 2000, Currency: "ARS"}, events.Metadata{CorrelationID: "pay_2"})
	_ = w.Refund("pay_1", "ref_1", events.Money{Amount: 500, Currency: "ARS"}, events.Metadata{CorrelationID: "pay_1"})

	history := w.UncommittedEvents()

	// 1. Rehydrate from full event history.
	fromHistory, err := wallet.Rehydrate(nil, history)
	if err != nil {
		t.Fatalf("Rehydrate from full history failed: %v", err)
	}
	if fromHistory.Balance() != 7500 || fromHistory.Held() != 2000 || fromHistory.Available() != 5500 || fromHistory.Version() != w.Version() {
		t.Fatalf("mismatch from history: b=%d h=%d a=%d v=%d", fromHistory.Balance(), fromHistory.Held(), fromHistory.Available(), fromHistory.Version())
	}

	// 2. Rehydrate from snapshot + full history (older events must be skipped cleanly).
	fromSnap, err := wallet.Rehydrate(&snap, history)
	if err != nil {
		t.Fatalf("Rehydrate from snapshot failed: %v", err)
	}
	if fromSnap.Balance() != fromHistory.Balance() || fromSnap.Held() != fromHistory.Held() || fromSnap.Version() != fromHistory.Version() {
		t.Fatalf("snapshot rehydration diverged: got (b=%d h=%d v=%d), want (b=%d h=%d v=%d)",
			fromSnap.Balance(), fromSnap.Held(), fromSnap.Version(),
			fromHistory.Balance(), fromHistory.Held(), fromHistory.Version())
	}
	if amt, ok := fromSnap.HasReservation("pay_2"); !ok || amt != 2000 {
		t.Fatalf("expected active reservation pay_2=2000, got (%d, %v)", amt, ok)
	}
}

func TestWalletApplyOutOfSequenceVersion(t *testing.T) {
	w := mustCreateFundedWallet(t, 1000)
	env, err := events.NewEnvelope(
		events.EventFundsDeposited,
		events.AggregateTypeWallet,
		w.ID(),
		99, // wrong version
		events.FundsDepositedPayload{WalletID: w.ID(), Amount: events.Money{Amount: 100, Currency: "ARS"}},
		events.Metadata{},
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("NewEnvelope failed: %v", err)
	}
	if err := w.Apply(env); !errors.Is(err, wallet.ErrEventVersionMismatch) {
		t.Fatalf("got %v, want ErrEventVersionMismatch", err)
	}
}
