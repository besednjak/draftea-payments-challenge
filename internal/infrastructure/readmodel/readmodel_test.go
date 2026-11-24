package readmodel_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/besednjak/draftea-payments-challenge/internal/application/ports"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/payment"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/wallet"
	"github.com/besednjak/draftea-payments-challenge/internal/infrastructure/eventstore"
	"github.com/besednjak/draftea-payments-challenge/internal/infrastructure/readmodel"
)

func TestProjectorRebuildAllFromEventStore(t *testing.T) {
	ctx := context.Background()
	store := eventstore.NewInMemoryEventStore()

	userWallet, err := wallet.Create("wal_1", "usr_1", "ARS", events.Metadata{})
	if err != nil {
		t.Fatalf("wallet.Create failed: %v", err)
	}
	if err := userWallet.Deposit("dep_1", events.Money{Amount: 10000, Currency: "ARS"}, events.Metadata{}); err != nil {
		t.Fatalf("Deposit failed: %v", err)
	}
	if err := userWallet.Reserve("pay_1", events.Money{Amount: 4000, Currency: "ARS"}, events.Metadata{CorrelationID: "pay_1"}); err != nil {
		t.Fatalf("Reserve failed: %v", err)
	}
	if err := userWallet.Capture("pay_1", events.Money{Amount: 4000, Currency: "ARS"}, events.Metadata{CorrelationID: "pay_1"}); err != nil {
		t.Fatalf("Capture failed: %v", err)
	}
	if err := store.Append(ctx, userWallet.StreamID(), 0, userWallet.UncommittedEvents()...); err != nil {
		t.Fatalf("store.Append wallet failed: %v", err)
	}

	completedPayment, err := payment.Initiate(payment.InitiateParams{
		PaymentID: "pay_1", WalletID: "wal_1", UserID: "usr_1", ServiceID: "svc_1", Provider: "mockpay",
		Amount: events.Money{Amount: 4000, Currency: "ARS"},
	}, events.Metadata{})
	if err != nil {
		t.Fatalf("Initiate pay_1 failed: %v", err)
	}
	if err := completedPayment.MarkFundsReserved(6000, events.Metadata{}); err != nil {
		t.Fatalf("MarkFundsReserved pay_1 failed: %v", err)
	}
	if err := completedPayment.MarkCompleted("prov_1", events.Metadata{}); err != nil {
		t.Fatalf("MarkCompleted pay_1 failed: %v", err)
	}
	if err := store.Append(ctx, completedPayment.StreamID(), 0, completedPayment.UncommittedEvents()...); err != nil {
		t.Fatalf("store.Append pay_1 failed: %v", err)
	}

	activePayment, err := payment.Initiate(payment.InitiateParams{
		PaymentID: "pay_2", WalletID: "wal_1", UserID: "usr_1", ServiceID: "svc_2", Provider: "mockpay",
		Amount: events.Money{Amount: 1000, Currency: "ARS"},
	}, events.Metadata{})
	if err != nil {
		t.Fatalf("Initiate pay_2 failed: %v", err)
	}
	if err := activePayment.MarkFundsReserved(5000, events.Metadata{}); err != nil {
		t.Fatalf("MarkFundsReserved pay_2 failed: %v", err)
	}
	if err := activePayment.MarkSubmitted("prov_2", events.Metadata{}); err != nil {
		t.Fatalf("MarkSubmitted pay_2 failed: %v", err)
	}
	if err := store.Append(ctx, activePayment.StreamID(), 0, activePayment.UncommittedEvents()...); err != nil {
		t.Fatalf("store.Append pay_2 failed: %v", err)
	}

	balanceStore := readmodel.NewWalletBalanceStore()
	transactionStore := readmodel.NewWalletTransactionStore()
	paymentStore := readmodel.NewPaymentViewStore()
	projector := readmodel.NewProjector(balanceStore, transactionStore, paymentStore)

	replayedCount, err := projector.RebuildAll(ctx, store)
	if err != nil {
		t.Fatalf("RebuildAll failed: %v", err)
	}
	if replayedCount != 10 {
		t.Fatalf("expected 10 replayed events, got %d", replayedCount)
	}

	balance, err := balanceStore.Get(ctx, "wal_1")
	if err != nil {
		t.Fatalf("Get balance failed: %v", err)
	}
	if balance.Balance != 6000 || balance.Held != 0 || balance.Available != 6000 || balance.Version != 4 {
		t.Fatalf("unexpected rebuilt balance: %+v", balance)
	}

	transactions, err := transactionStore.ListByWallet(ctx, "wal_1")
	if err != nil || len(transactions) != 3 {
		t.Fatalf("expected 3 wallet transactions, got %d err=%v", len(transactions), err)
	}

	paymentView, err := paymentStore.Get(ctx, "pay_1")
	if err != nil || paymentView.Status != string(payment.StatusCompleted) || paymentView.CreatedAt.IsZero() {
		t.Fatalf("unexpected pay_1 view: %+v err=%v", paymentView, err)
	}

	nonTerminal, err := paymentStore.ListNonTerminal(ctx)
	if err != nil || len(nonTerminal) != 1 || nonTerminal[0].PaymentID != "pay_2" {
		t.Fatalf("expected only pay_2 as active saga, got %+v err=%v", nonTerminal, err)
	}
}

func TestProjectorCorruptPayloadIsNonRetryable(t *testing.T) {
	projector := readmodel.NewProjector(
		readmodel.NewWalletBalanceStore(),
		readmodel.NewWalletTransactionStore(),
		readmodel.NewPaymentViewStore(),
	)
	corruptEnvelope := events.Envelope{
		EventID:       "evt-bad",
		Type:          events.EventFundsReserved,
		AggregateType: events.AggregateTypeWallet,
		AggregateID:   "wal_1",
		Payload:       json.RawMessage(`{invalid-json`),
	}

	err := projector.HandleEvent(context.Background(), corruptEnvelope)

	if err == nil || !ports.IsNonRetryable(err) {
		t.Fatalf("expected non-retryable decode error, got %v", err)
	}
}
