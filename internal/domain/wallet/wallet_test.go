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
	cases := []struct {
		name     string
		walletID string
		userID   string
		currency string
		wantErr  error
	}{
		{"missing wallet ID", "", "usr_1", "ARS", wallet.ErrInvalidWalletID},
		{"missing user ID", "wal_1", "", "ARS", wallet.ErrInvalidUserID},
		{"invalid currency", "wal_1", "usr_1", "INVALID", events.ErrInvalidCurrency},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := wallet.Create(tc.walletID, tc.userID, tc.currency, events.Metadata{})

			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("got %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestWalletReserveAndCaptureHappyPath(t *testing.T) {
	fundedWallet := mustCreateFundedWallet(t, 10000)
	fundedWallet.ClearUncommitted()
	meta := events.Metadata{CorrelationID: "pay_1"}

	if err := fundedWallet.Reserve("pay_1", events.Money{Amount: 4000, Currency: "ARS"}, meta); err != nil {
		t.Fatalf("Reserve failed: %v", err)
	}
	if fundedWallet.Balance() != 10000 || fundedWallet.Held() != 4000 || fundedWallet.Available() != 6000 {
		t.Fatalf("unexpected balances after reserve: balance=%d held=%d available=%d",
			fundedWallet.Balance(), fundedWallet.Held(), fundedWallet.Available())
	}
	if fundedWallet.BaseVersion() != 2 || fundedWallet.Version() != 3 {
		t.Fatalf("expected baseVersion=2 version=3, got base=%d version=%d",
			fundedWallet.BaseVersion(), fundedWallet.Version())
	}

	if err := fundedWallet.Reserve("pay_1", events.Money{Amount: 4000, Currency: "ARS"}, meta); err != nil {
		t.Fatalf("duplicate Reserve failed: %v", err)
	}
	if fundedWallet.Version() != 3 || fundedWallet.Held() != 4000 {
		t.Fatalf("duplicate Reserve altered state: version=%d held=%d", fundedWallet.Version(), fundedWallet.Held())
	}

	if err := fundedWallet.Capture("pay_1", events.Money{Amount: 4000, Currency: "ARS"}, meta); err != nil {
		t.Fatalf("Capture failed: %v", err)
	}
	if fundedWallet.Balance() != 6000 || fundedWallet.Held() != 0 || fundedWallet.Available() != 6000 {
		t.Fatalf("unexpected balances after capture: balance=%d held=%d available=%d",
			fundedWallet.Balance(), fundedWallet.Held(), fundedWallet.Available())
	}
	if _, active := fundedWallet.HasReservation("pay_1"); active {
		t.Fatal("expected reservation to be cleared after capture")
	}

	if err := fundedWallet.Capture("pay_1", events.Money{Amount: 4000, Currency: "ARS"}, meta); err != nil {
		t.Fatalf("duplicate Capture failed: %v", err)
	}
	if fundedWallet.Version() != 4 || fundedWallet.Balance() != 6000 {
		t.Fatalf("duplicate Capture altered state: version=%d balance=%d", fundedWallet.Version(), fundedWallet.Balance())
	}
}

func TestWalletReserveRejections(t *testing.T) {
	t.Run("insufficient funds emits FundsReservationRejected", func(t *testing.T) {
		fundedWallet := mustCreateFundedWallet(t, 5000)
		fundedWallet.ClearUncommitted()

		err := fundedWallet.Reserve("pay_big", events.Money{Amount: 8000, Currency: "ARS"}, events.Metadata{CorrelationID: "pay_big"})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if fundedWallet.Balance() != 5000 || fundedWallet.Held() != 0 || fundedWallet.Available() != 5000 {
			t.Fatalf("balance changed on rejection: balance=%d held=%d available=%d",
				fundedWallet.Balance(), fundedWallet.Held(), fundedWallet.Available())
		}
		uncommitted := fundedWallet.UncommittedEvents()
		if len(uncommitted) != 1 || uncommitted[0].Type != events.EventFundsReservationRejected {
			t.Fatalf("expected 1 FundsReservationRejected event, got %+v", uncommitted)
		}
		rejection, err := events.DecodePayload[events.FundsReservationRejectedPayload](uncommitted[0])
		if err != nil {
			t.Fatalf("DecodePayload failed: %v", err)
		}
		if rejection.Reason != events.ReasonInsufficientFunds || rejection.AvailableBalance != 5000 {
			t.Fatalf("unexpected rejection payload: %+v", rejection)
		}
	})

	t.Run("currency mismatch emits FundsReservationRejected", func(t *testing.T) {
		fundedWallet := mustCreateFundedWallet(t, 5000)
		fundedWallet.ClearUncommitted()

		err := fundedWallet.Reserve("pay_usd", events.Money{Amount: 1000, Currency: "USD"}, events.Metadata{CorrelationID: "pay_usd"})

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		uncommitted := fundedWallet.UncommittedEvents()
		if len(uncommitted) != 1 || uncommitted[0].Type != events.EventFundsReservationRejected {
			t.Fatalf("expected FundsReservationRejected, got %+v", uncommitted)
		}
		rejection, err := events.DecodePayload[events.FundsReservationRejectedPayload](uncommitted[0])
		if err != nil {
			t.Fatalf("DecodePayload failed: %v", err)
		}
		if rejection.Reason != events.ReasonCurrencyMismatch {
			t.Fatalf("got reason=%q, want %q", rejection.Reason, events.ReasonCurrencyMismatch)
		}
	})
}

func TestWalletReleaseAndIdempotency(t *testing.T) {
	fundedWallet := mustCreateFundedWallet(t, 10000)
	meta := events.Metadata{CorrelationID: "pay_rel"}
	if err := fundedWallet.Reserve("pay_rel", events.Money{Amount: 3000, Currency: "ARS"}, meta); err != nil {
		t.Fatalf("Reserve failed: %v", err)
	}

	if err := fundedWallet.Release("pay_rel", events.ReasonCircuitOpen, meta); err != nil {
		t.Fatalf("Release failed: %v", err)
	}
	if fundedWallet.Balance() != 10000 || fundedWallet.Held() != 0 || fundedWallet.Available() != 10000 {
		t.Fatalf("unexpected balances after release: balance=%d held=%d available=%d",
			fundedWallet.Balance(), fundedWallet.Held(), fundedWallet.Available())
	}

	versionAfterRelease := fundedWallet.Version()
	if err := fundedWallet.Release("pay_rel", events.ReasonCircuitOpen, meta); err != nil {
		t.Fatalf("duplicate Release failed: %v", err)
	}
	if fundedWallet.Version() != versionAfterRelease {
		t.Fatalf("duplicate Release emitted extra event: got version=%d want %d", fundedWallet.Version(), versionAfterRelease)
	}

	if err := fundedWallet.Release("pay_unknown", "test", meta); !errors.Is(err, wallet.ErrReservationNotFound) {
		t.Fatalf("got err=%v, want ErrReservationNotFound", err)
	}
}

func TestWalletCaptureErrorsAndCannotReleaseCaptured(t *testing.T) {
	fundedWallet := mustCreateFundedWallet(t, 10000)
	meta := events.Metadata{CorrelationID: "pay_1"}

	if err := fundedWallet.Capture("pay_missing", events.Money{Amount: 100, Currency: "ARS"}, meta); !errors.Is(err, wallet.ErrReservationNotFound) {
		t.Fatalf("got %v, want ErrReservationNotFound", err)
	}
	if err := fundedWallet.Reserve("pay_1", events.Money{Amount: 2000, Currency: "ARS"}, meta); err != nil {
		t.Fatalf("Reserve failed: %v", err)
	}
	if err := fundedWallet.Capture("pay_1", events.Money{Amount: 1500, Currency: "ARS"}, meta); !errors.Is(err, wallet.ErrCaptureAmountMismatch) {
		t.Fatalf("got %v, want ErrCaptureAmountMismatch", err)
	}
	if err := fundedWallet.Capture("pay_1", events.Money{Amount: 2000, Currency: "USD"}, meta); !errors.Is(err, wallet.ErrCurrencyMismatch) {
		t.Fatalf("got %v, want ErrCurrencyMismatch", err)
	}
	if err := fundedWallet.Capture("pay_1", events.Money{Amount: 2000, Currency: "ARS"}, meta); err != nil {
		t.Fatalf("Capture failed: %v", err)
	}
	if err := fundedWallet.Release("pay_1", "too_late", meta); !errors.Is(err, wallet.ErrReservationAlreadyCaptured) {
		t.Fatalf("got %v, want ErrReservationAlreadyCaptured", err)
	}
}

func TestWalletRefundAndDepositIdempotency(t *testing.T) {
	fundedWallet := mustCreateFundedWallet(t, 10000)
	initialVersion := fundedWallet.Version()

	if err := fundedWallet.Deposit("dep_1", events.Money{Amount: 10000, Currency: "ARS"}, events.Metadata{}); err != nil {
		t.Fatalf("duplicate Deposit failed: %v", err)
	}
	if fundedWallet.Balance() != 10000 || fundedWallet.Version() != initialVersion {
		t.Fatalf("duplicate Deposit altered state: balance=%d version=%d", fundedWallet.Balance(), fundedWallet.Version())
	}

	if err := fundedWallet.Refund("pay_1", "ref_1", events.Money{Amount: 2500, Currency: "ARS"}, events.Metadata{CorrelationID: "pay_1"}); err != nil {
		t.Fatalf("Refund failed: %v", err)
	}
	if fundedWallet.Balance() != 12500 {
		t.Fatalf("expected balance=12500, got %d", fundedWallet.Balance())
	}

	versionAfterRefund := fundedWallet.Version()
	if err := fundedWallet.Refund("pay_1", "ref_1", events.Money{Amount: 2500, Currency: "ARS"}, events.Metadata{CorrelationID: "pay_1"}); err != nil {
		t.Fatalf("duplicate Refund failed: %v", err)
	}
	if fundedWallet.Balance() != 12500 || fundedWallet.Version() != versionAfterRefund {
		t.Fatalf("duplicate Refund altered state: balance=%d version=%d", fundedWallet.Balance(), fundedWallet.Version())
	}
}

func TestWalletRehydrateFromHistoryAndSnapshot(t *testing.T) {
	fundedWallet := mustCreateFundedWallet(t, 10000)
	if err := fundedWallet.Reserve("pay_1", events.Money{Amount: 3000, Currency: "ARS"}, events.Metadata{CorrelationID: "pay_1"}); err != nil {
		t.Fatalf("Reserve pay_1 failed: %v", err)
	}
	snapshot := fundedWallet.TakeSnapshot()

	// Mutating the snapshot maps must never leak back into the live aggregate.
	snapshot.Reservations["pay_fake"] = 999
	if _, leaked := fundedWallet.HasReservation("pay_fake"); leaked {
		t.Fatal("snapshot mutation leaked into aggregate")
	}
	delete(snapshot.Reservations, "pay_fake")

	if err := fundedWallet.Capture("pay_1", events.Money{Amount: 3000, Currency: "ARS"}, events.Metadata{CorrelationID: "pay_1"}); err != nil {
		t.Fatalf("Capture failed: %v", err)
	}
	if err := fundedWallet.Reserve("pay_2", events.Money{Amount: 2000, Currency: "ARS"}, events.Metadata{CorrelationID: "pay_2"}); err != nil {
		t.Fatalf("Reserve pay_2 failed: %v", err)
	}
	if err := fundedWallet.Refund("pay_1", "ref_1", events.Money{Amount: 500, Currency: "ARS"}, events.Metadata{CorrelationID: "pay_1"}); err != nil {
		t.Fatalf("Refund failed: %v", err)
	}
	history := fundedWallet.UncommittedEvents()

	fromHistory, err := wallet.Rehydrate(nil, history)
	if err != nil {
		t.Fatalf("Rehydrate from full history failed: %v", err)
	}
	if fromHistory.Balance() != 7500 || fromHistory.Held() != 2000 || fromHistory.Available() != 5500 || fromHistory.Version() != fundedWallet.Version() {
		t.Fatalf("mismatch from history: balance=%d held=%d available=%d version=%d",
			fromHistory.Balance(), fromHistory.Held(), fromHistory.Available(), fromHistory.Version())
	}

	fromSnapshot, err := wallet.Rehydrate(&snapshot, history)
	if err != nil {
		t.Fatalf("Rehydrate from snapshot failed: %v", err)
	}
	if fromSnapshot.Balance() != fromHistory.Balance() || fromSnapshot.Held() != fromHistory.Held() || fromSnapshot.Version() != fromHistory.Version() {
		t.Fatalf("snapshot rehydration diverged: got=(%d, %d, v%d) want=(%d, %d, v%d)",
			fromSnapshot.Balance(), fromSnapshot.Held(), fromSnapshot.Version(),
			fromHistory.Balance(), fromHistory.Held(), fromHistory.Version())
	}
	if reservedAmount, ok := fromSnapshot.HasReservation("pay_2"); !ok || reservedAmount != 2000 {
		t.Fatalf("expected active reservation pay_2=2000, got (%d, %v)", reservedAmount, ok)
	}
}

func TestWalletApplyOutOfSequenceVersion(t *testing.T) {
	fundedWallet := mustCreateFundedWallet(t, 1000)
	outOfOrderEvent, err := events.NewEnvelope(
		events.EventFundsDeposited,
		events.AggregateTypeWallet,
		fundedWallet.ID(),
		99,
		events.FundsDepositedPayload{WalletID: fundedWallet.ID(), Amount: events.Money{Amount: 100, Currency: "ARS"}},
		events.Metadata{},
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("NewEnvelope failed: %v", err)
	}

	err = fundedWallet.Apply(outOfOrderEvent)

	if !errors.Is(err, wallet.ErrEventVersionMismatch) {
		t.Fatalf("got %v, want ErrEventVersionMismatch", err)
	}
}
