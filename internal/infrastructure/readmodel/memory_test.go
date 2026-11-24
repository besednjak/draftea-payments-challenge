package readmodel_test

import (
	"context"
	"testing"

	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/payment"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/wallet"
	"github.com/besednjak/draftea-payments-challenge/internal/infrastructure/eventstore"
	"github.com/besednjak/draftea-payments-challenge/internal/infrastructure/readmodel"
)

func TestProjectorRebuildAllFromEventStore(t *testing.T) {
	ctx := context.Background()
	store := eventstore.NewInMemoryEventStore()

	// Create wallet, deposit 10_000, reserve 4_000, capture 4_000.
	w, err := wallet.Create("wal_1", "usr_1", "ARS", events.Metadata{})
	if err != nil {
		t.Fatalf("wallet.Create failed: %v", err)
	}
	_ = w.Deposit("dep_1", events.Money{Amount: 10000, Currency: "ARS"}, events.Metadata{})
	_ = w.Reserve("pay_1", events.Money{Amount: 4000, Currency: "ARS"}, events.Metadata{CorrelationID: "pay_1"})
	_ = w.Capture("pay_1", events.Money{Amount: 4000, Currency: "ARS"}, events.Metadata{CorrelationID: "pay_1"})
	if err := store.Append(ctx, w.StreamID(), 0, w.UncommittedEvents()...); err != nil {
		t.Fatalf("store.Append wallet failed: %v", err)
	}

	// Create two payments: one COMPLETED and one active (SUBMITTED).
	p1, _ := payment.Initiate(payment.InitiateParams{
		PaymentID: "pay_1", WalletID: "wal_1", UserID: "usr_1", ServiceID: "svc_1", Provider: "mockpay",
		Amount: events.Money{Amount: 4000, Currency: "ARS"},
	}, events.Metadata{})
	_ = p1.MarkFundsReserved(6000, events.Metadata{})
	_ = p1.MarkCompleted("prov_1", events.Metadata{})
	if err := store.Append(ctx, p1.StreamID(), 0, p1.UncommittedEvents()...); err != nil {
		t.Fatalf("store.Append p1 failed: %v", err)
	}

	p2, _ := payment.Initiate(payment.InitiateParams{
		PaymentID: "pay_2", WalletID: "wal_1", UserID: "usr_1", ServiceID: "svc_2", Provider: "mockpay",
		Amount: events.Money{Amount: 1000, Currency: "ARS"},
	}, events.Metadata{})
	_ = p2.MarkFundsReserved(5000, events.Metadata{})
	_ = p2.MarkSubmitted("prov_2", events.Metadata{})
	if err := store.Append(ctx, p2.StreamID(), 0, p2.UncommittedEvents()...); err != nil {
		t.Fatalf("store.Append p2 failed: %v", err)
	}

	balStore := readmodel.NewWalletBalanceStore()
	txStore := readmodel.NewWalletTransactionStore()
	payStore := readmodel.NewPaymentViewStore()
	projector := readmodel.NewProjector(balStore, txStore, payStore)

	replayed, err := projector.RebuildAll(ctx, store)
	if err != nil {
		t.Fatalf("RebuildAll failed: %v", err)
	}
	if replayed != 10 {
		t.Fatalf("expected 10 replayed events, got %d", replayed)
	}

	bal, err := balStore.Get(ctx, "wal_1")
	if err != nil {
		t.Fatalf("Get balance failed: %v", err)
	}
	if bal.Balance != 6000 || bal.Held != 0 || bal.Available != 6000 || bal.Version != 4 {
		t.Fatalf("unexpected rebuilt balance: %+v", bal)
	}

	txs, err := txStore.ListByWallet(ctx, "wal_1")
	if err != nil || len(txs) != 3 {
		t.Fatalf("expected 3 wallet transactions (deposit, reserve, capture), got %d err=%v", len(txs), err)
	}

	pv1, err := payStore.Get(ctx, "pay_1")
	if err != nil || pv1.Status != string(payment.StatusCompleted) {
		t.Fatalf("unexpected pay_1 view: %+v err=%v", pv1, err)
	}

	nonTerminal, err := payStore.ListNonTerminal(ctx)
	if err != nil || len(nonTerminal) != 1 || nonTerminal[0].PaymentID != "pay_2" {
		t.Fatalf("expected only pay_2 as active saga, got %+v err=%v", nonTerminal, err)
	}
}
