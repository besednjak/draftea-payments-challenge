package saga_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/besednjak/draftea-payments-challenge/internal/application/ports"
	"github.com/besednjak/draftea-payments-challenge/internal/application/saga"
	appwallet "github.com/besednjak/draftea-payments-challenge/internal/application/wallet"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/payment"
	"github.com/besednjak/draftea-payments-challenge/internal/infrastructure/eventbus"
	"github.com/besednjak/draftea-payments-challenge/internal/infrastructure/eventstore"
	"github.com/besednjak/draftea-payments-challenge/internal/infrastructure/gateway"
	"github.com/besednjak/draftea-payments-challenge/internal/infrastructure/readmodel"
	"github.com/besednjak/draftea-payments-challenge/internal/observability"
)

type testSystem struct {
	store            *eventstore.InMemoryEventStore
	snapshotStore    *eventstore.InMemorySnapshotStore
	inbox            *eventstore.InMemoryInboxStore
	idempotencyStore *eventstore.InMemoryIdempotencyStore
	bus              *eventbus.PartitionedBus
	balanceStore     *readmodel.WalletBalanceStore
	transactionStore *readmodel.WalletTransactionStore
	paymentStore     *readmodel.PaymentViewStore
	projector        *readmodel.Projector
	mockGateway      *gateway.MockGateway
	breaker          *gateway.CircuitBreaker
	gatewayAdapter   *gateway.Adapter
	walletService    *appwallet.Service
	orchestrator     *saga.PaymentOrchestrator
	metrics          *observability.Collector
	health           *observability.HealthRegistry
	webhookSecret    string
}

func newTestSystem(
	t *testing.T,
	defaultBehavior gateway.MockBehavior,
	store *eventstore.InMemoryEventStore,
	snapshotStore *eventstore.InMemorySnapshotStore,
) *testSystem {
	t.Helper()

	if store == nil {
		store = eventstore.NewInMemoryEventStore()
	}
	if snapshotStore == nil {
		snapshotStore = eventstore.NewInMemorySnapshotStore()
	}

	metrics := observability.NewCollector()
	inbox := eventstore.NewInMemoryInboxStore()
	idempotencyStore := eventstore.NewInMemoryIdempotencyStore(nil)
	bus := eventbus.NewPartitionedBus(eventbus.Config{
		Partitions:     8,
		MaxAttempts:    3,
		RetryBaseDelay: time.Millisecond,
		RetryMaxDelay:  10 * time.Millisecond,
		Metrics:        metrics,
	})

	balanceStore := readmodel.NewWalletBalanceStore()
	transactionStore := readmodel.NewWalletTransactionStore()
	paymentStore := readmodel.NewPaymentViewStore()
	projector := readmodel.NewProjector(balanceStore, transactionStore, paymentStore)

	mockGateway := gateway.NewMockGateway(defaultBehavior)
	breaker := gateway.NewCircuitBreaker(gateway.CircuitBreakerConfig{
		Provider:            "mockpay",
		ConsecutiveTimeouts: 5,
		OpenTimeout:         30 * time.Second,
		HalfOpenMaxProbes:   3,
		OnStateChange: func(provider string, _, to gateway.CircuitState) {
			metrics.RecordCircuitBreakerState(provider, string(to))
		},
	})

	webhookSecret := "test-webhook-secret"
	gatewayAdapter := gateway.NewAdapter(gateway.AdapterConfig{
		Gateway:       mockGateway,
		Publisher:     bus,
		Inbox:         inbox,
		Breaker:       breaker,
		WebhookSecret: webhookSecret,
		Metrics:       metrics,
		Retry: gateway.RetryConfig{
			MaxAttempts:    3,
			BaseDelay:      time.Millisecond,
			MaxDelay:       5 * time.Millisecond,
			AttemptTimeout: time.Second,
			Sleep:          func(context.Context, time.Duration) error { return nil },
		},
	})

	walletService := appwallet.NewService(appwallet.Config{
		EventStore:       store,
		SnapshotStore:    snapshotStore,
		SnapshotInterval: 3,
		BalanceReadModel: balanceStore,
		Publisher:        bus,
		Inbox:            inbox,
		Metrics:          metrics,
		MaxLockRetries:   10,
		LockRetryMin:     time.Millisecond,
		LockRetryMax:     5 * time.Millisecond,
	})

	orchestrator := saga.NewPaymentOrchestrator(saga.Config{
		EventStore:      store,
		PaymentViews:    paymentStore,
		Publisher:       bus,
		Inbox:           inbox,
		Idempotency:     idempotencyStore,
		Metrics:         metrics,
		SyncReserveWait: 2 * time.Second,
		MaxLockRetries:  5,
	})

	subscriptions := []struct {
		topic   string
		group   string
		handler ports.EventHandler
	}{
		{events.TopicWalletsCommands, "wallet-service", walletService.HandleCommand},
		{events.TopicGatewayCommands, "gateway-adapter", gatewayAdapter.HandleCommand},
		{events.TopicWalletsEvents, "payment-saga", orchestrator.HandleWalletEvent},
		{events.TopicGatewayEvents, "payment-saga", orchestrator.HandleGatewayEvent},
		{events.TopicWalletsEvents, "readmodel-projector", projector.HandleEvent},
		{events.TopicPaymentsEvents, "readmodel-projector", projector.HandleEvent},
		{events.TopicPaymentsEvents, "metrics-collector", metrics.HandleEvent},
		{events.TopicWalletsEvents, "metrics-collector", metrics.HandleEvent},
		{events.TopicGatewayEvents, "metrics-collector", metrics.HandleEvent},
	}
	for _, sub := range subscriptions {
		if err := bus.Subscribe(sub.topic, sub.group, sub.handler); err != nil {
			t.Fatalf("Subscribe(%s, %s) failed: %v", sub.topic, sub.group, err)
		}
	}

	health := observability.NewHealthRegistry(nil)
	health.Register(store)
	health.Register(bus)
	health.Register(mockGateway)

	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = bus.Close(closeCtx)
	})

	return &testSystem{
		store:            store,
		snapshotStore:    snapshotStore,
		inbox:            inbox,
		idempotencyStore: idempotencyStore,
		bus:              bus,
		balanceStore:     balanceStore,
		transactionStore: transactionStore,
		paymentStore:     paymentStore,
		projector:        projector,
		mockGateway:      mockGateway,
		breaker:          breaker,
		gatewayAdapter:   gatewayAdapter,
		walletService:    walletService,
		orchestrator:     orchestrator,
		metrics:          metrics,
		health:           health,
		webhookSecret:    webhookSecret,
	}
}

func (s *testSystem) drain(t *testing.T) {
	t.Helper()
	drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.bus.Drain(drainCtx); err != nil {
		t.Fatalf("bus.Drain failed: %v", err)
	}
}

func (s *testSystem) mustCreateFundedWallet(t *testing.T, walletID, userID string, amount int64) {
	t.Helper()
	ctx := context.Background()
	if _, err := s.walletService.CreateWallet(ctx, walletID, userID, "ARS"); err != nil {
		t.Fatalf("CreateWallet(%s) failed: %v", walletID, err)
	}
	if _, err := s.walletService.Deposit(ctx, walletID, "dep_"+walletID, events.Money{Amount: amount, Currency: "ARS"}); err != nil {
		t.Fatalf("Deposit(%s) failed: %v", walletID, err)
	}
}

func (s *testSystem) mustInitiatePayment(t *testing.T, req saga.InitiatePaymentRequest) saga.InitiatePaymentResult {
	t.Helper()
	result, err := s.orchestrator.InitiatePayment(context.Background(), req)
	if err != nil {
		t.Fatalf("InitiatePayment(%s) failed: %v", req.PaymentID, err)
	}
	return result
}

func (s *testSystem) mustDeliverApprovedWebhook(t *testing.T, eventID, paymentID, providerRef string) {
	t.Helper()
	webhookBody, err := json.Marshal(gateway.WebhookPayload{
		ProviderEventID: eventID,
		PaymentID:       paymentID,
		Provider:        "mockpay",
		ProviderRef:     providerRef,
		Status:          string(ports.GatewayOutcomeApproved),
	})
	if err != nil {
		t.Fatalf("marshal webhook failed: %v", err)
	}
	signature := gateway.SignWebhook(s.webhookSecret, webhookBody)
	if err := s.gatewayAdapter.HandleWebhook(context.Background(), signature, webhookBody); err != nil {
		t.Fatalf("HandleWebhook failed: %v", err)
	}
}

func TestScenario1_HappyPathWebhookAndRefunds(t *testing.T) {
	ctx := context.Background()

	t.Run("async approval via HMAC webhook preserves CreatedAt and idempotency", func(t *testing.T) {
		sys := newTestSystem(t, gateway.MockBehavior{SubmitOutcome: ports.GatewayOutcomeAccepted}, nil, nil)
		sys.mustCreateFundedWallet(t, "wal_1", "usr_1", 50000)

		request := saga.InitiatePaymentRequest{
			IdempotencyKey: "idem_happy_1",
			PaymentID:      "pay_happy_1",
			WalletID:       "wal_1",
			UserID:         "usr_1",
			ServiceID:      "svc_internet",
			Provider:       "mockpay",
			Amount:         events.Money{Amount: 15000, Currency: "ARS"},
		}

		initialResult := sys.mustInitiatePayment(t, request)
		if initialResult.HTTPStatus != http.StatusAccepted || initialResult.Status != payment.StatusFundsReserved || initialResult.AvailableBalance != 35000 {
			t.Fatalf("unexpected initial response: %+v", initialResult)
		}

		duplicateResult := sys.mustInitiatePayment(t, request)
		if !duplicateResult.FromIdempotencyCache || duplicateResult.PaymentID != initialResult.PaymentID {
			t.Fatalf("expected cached idempotent response, got %+v", duplicateResult)
		}

		sys.drain(t)

		submittedView, err := sys.orchestrator.GetPayment(ctx, "pay_happy_1")
		if err != nil || submittedView.Status != string(payment.StatusSubmitted) || submittedView.CreatedAt.IsZero() {
			t.Fatalf("expected SUBMITTED with CreatedAt set, got %+v err=%v", submittedView, err)
		}
		initialCreatedAt := submittedView.CreatedAt

		heldBalance, err := sys.walletService.GetBalance(ctx, "wal_1")
		if err != nil || heldBalance.Balance != 50000 || heldBalance.Held != 15000 || heldBalance.Available != 35000 {
			t.Fatalf("expected funds held while SUBMITTED, got %+v err=%v", heldBalance, err)
		}

		sys.mustDeliverApprovedWebhook(t, "wh_evt_001", "pay_happy_1", "prov_pay_happy_1")
		sys.drain(t)

		completedView, err := sys.orchestrator.GetPayment(ctx, "pay_happy_1")
		if err != nil || completedView.Status != string(payment.StatusCompleted) {
			t.Fatalf("expected COMPLETED, got %+v err=%v", completedView, err)
		}
		if !completedView.CreatedAt.Equal(initialCreatedAt) {
			t.Fatalf("CreatedAt was overwritten on transition: got %v want %v", completedView.CreatedAt, initialCreatedAt)
		}

		capturedBalance, err := sys.walletService.GetBalance(ctx, "wal_1")
		if err != nil || capturedBalance.Balance != 35000 || capturedBalance.Held != 0 || capturedBalance.Available != 35000 {
			t.Fatalf("expected captured balance 35000, got %+v err=%v", capturedBalance, err)
		}

		report := sys.metrics.Snapshot()
		if report.PaymentsCompleted != 1 || report.SuccessRate != 1.0 {
			t.Fatalf("unexpected metrics report: %+v", report)
		}
	})

	t.Run("partial and full refunds credit wallet and transition to REFUNDED", func(t *testing.T) {
		sys := newTestSystem(t, gateway.MockBehavior{SubmitOutcome: ports.GatewayOutcomeApproved}, nil, nil)
		sys.mustCreateFundedWallet(t, "wal_ref", "usr_ref", 50000)

		sys.mustInitiatePayment(t, saga.InitiatePaymentRequest{
			PaymentID: "pay_ref_1",
			WalletID:  "wal_ref",
			UserID:    "usr_ref",
			ServiceID: "svc_internet",
			Provider:  "mockpay",
			Amount:    events.Money{Amount: 15000, Currency: "ARS"},
		})
		sys.drain(t)

		if err := sys.orchestrator.RequestRefund(ctx, "pay_ref_1", "ref_1", events.Money{Amount: 5000, Currency: "ARS"}); err != nil {
			t.Fatalf("partial refund failed: %v", err)
		}
		sys.drain(t)

		partialView, err := sys.orchestrator.GetPayment(ctx, "pay_ref_1")
		if err != nil {
			t.Fatalf("GetPayment failed: %v", err)
		}
		partialBalance, err := sys.walletService.GetBalance(ctx, "wal_ref")
		if err != nil {
			t.Fatalf("GetBalance failed: %v", err)
		}
		if partialView.Status != string(payment.StatusCompleted) || partialView.RefundedTotal != 5000 || partialBalance.Balance != 40000 {
			t.Fatalf("unexpected state after partial refund: view=%+v balance=%+v", partialView, partialBalance)
		}

		if err := sys.orchestrator.RequestRefund(ctx, "pay_ref_1", "ref_2", events.Money{Amount: 10000, Currency: "ARS"}); err != nil {
			t.Fatalf("final refund failed: %v", err)
		}
		sys.drain(t)

		refundedView, err := sys.orchestrator.GetPayment(ctx, "pay_ref_1")
		if err != nil {
			t.Fatalf("GetPayment failed: %v", err)
		}
		refundedBalance, err := sys.walletService.GetBalance(ctx, "wal_ref")
		if err != nil {
			t.Fatalf("GetBalance failed: %v", err)
		}
		if refundedView.Status != string(payment.StatusRefunded) || refundedView.RefundedTotal != 15000 || refundedBalance.Balance != 50000 {
			t.Fatalf("unexpected state after full refund: view=%+v balance=%+v", refundedView, refundedBalance)
		}

		history, err := sys.transactionStore.ListByWallet(ctx, "wal_ref")
		if err != nil || len(history) != 5 {
			t.Fatalf("expected 5 wallet transactions, got %d err=%v", len(history), err)
		}
	})

	t.Run("concurrent partial refunds cannot exceed captured amount", func(t *testing.T) {
		sys := newTestSystem(t, gateway.MockBehavior{SubmitOutcome: ports.GatewayOutcomeApproved}, nil, nil)
		sys.mustCreateFundedWallet(t, "wal_cref", "usr_cref", 50000)

		sys.mustInitiatePayment(t, saga.InitiatePaymentRequest{
			PaymentID: "pay_cref_1",
			WalletID:  "wal_cref",
			UserID:    "usr_cref",
			ServiceID: "svc_internet",
			Provider:  "mockpay",
			Amount:    events.Money{Amount: 10000, Currency: "ARS"},
		})
		sys.drain(t)

		var (
			wg        sync.WaitGroup
			startCh   = make(chan struct{})
			succeeded atomic.Int32
			exceeded  atomic.Int32
		)
		for idx := 0; idx < 2; idx++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				<-startCh
				err := sys.orchestrator.RequestRefund(
					ctx,
					"pay_cref_1",
					fmt.Sprintf("ref_conc_%d", index),
					events.Money{Amount: 7000, Currency: "ARS"},
				)
				if err == nil {
					succeeded.Add(1)
					return
				}
				if errors.Is(err, payment.ErrRefundExceedsCaptured) {
					exceeded.Add(1)
					return
				}
				t.Errorf("unexpected RequestRefund[%d] error: %v", index, err)
			}(idx)
		}
		close(startCh)
		wg.Wait()
		sys.drain(t)

		if succeeded.Load() != 1 || exceeded.Load() != 1 {
			t.Fatalf("expected 1 succeeded and 1 ErrRefundExceedsCaptured, got succeeded=%d exceeded=%d",
				succeeded.Load(), exceeded.Load())
		}

		paymentView, err := sys.orchestrator.GetPayment(ctx, "pay_cref_1")
		if err != nil {
			t.Fatalf("GetPayment failed: %v", err)
		}
		balance, err := sys.walletService.GetBalance(ctx, "wal_cref")
		if err != nil {
			t.Fatalf("GetBalance failed: %v", err)
		}
		if paymentView.RefundedTotal != 7000 || balance.Balance != 47000 {
			t.Fatalf("unexpected state after concurrent partial refunds: view=%+v balance=%+v", paymentView, balance)
		}
	})

	t.Run("gateway refund decline releases pending reservation so retry succeeds", func(t *testing.T) {
		sys := newTestSystem(t, gateway.MockBehavior{
			SubmitOutcome:  ports.GatewayOutcomeApproved,
			RefundDeclined: true,
		}, nil, nil)
		sys.mustCreateFundedWallet(t, "wal_rdec", "usr_rdec", 50000)

		sys.mustInitiatePayment(t, saga.InitiatePaymentRequest{
			PaymentID: "pay_rdec_1", WalletID: "wal_rdec", UserID: "usr_rdec", ServiceID: "svc_1", Provider: "mockpay",
			Amount: events.Money{Amount: 10000, Currency: "ARS"},
		})
		sys.drain(t)

		if err := sys.orchestrator.RequestRefund(ctx, "pay_rdec_1", "ref_declined_1", events.Money{Amount: 10000, Currency: "ARS"}); err != nil {
			t.Fatalf("RequestRefund failed: %v", err)
		}
		sys.drain(t)

		loaded, err := sys.orchestrator.Load(ctx, "pay_rdec_1")
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if loaded.PendingRefundTotal() != 0 || loaded.RefundableAmount() != 10000 {
			t.Fatalf("expected declined refund reservation released, pending=%d refundable=%d",
				loaded.PendingRefundTotal(), loaded.RefundableAmount())
		}

		// Provider now approves a new refund request for the full amount.
		sys.mockGateway.SetRule("pay_rdec_1", gateway.MockBehavior{SubmitOutcome: ports.GatewayOutcomeApproved, RefundDeclined: false})
		if err := sys.orchestrator.RequestRefund(ctx, "pay_rdec_1", "ref_ok_2", events.Money{Amount: 10000, Currency: "ARS"}); err != nil {
			t.Fatalf("retry RequestRefund failed: %v", err)
		}
		sys.drain(t)

		finalView, err := sys.orchestrator.GetPayment(ctx, "pay_rdec_1")
		if err != nil || finalView.Status != string(payment.StatusRefunded) {
			t.Fatalf("expected REFUNDED after retry, got %+v err=%v", finalView, err)
		}
	})
}

func TestScenario2_InsufficientFundsImmediateRejection(t *testing.T) {
	ctx := context.Background()
	sys := newTestSystem(t, gateway.MockBehavior{SubmitOutcome: ports.GatewayOutcomeApproved}, nil, nil)
	sys.mustCreateFundedWallet(t, "wal_low", "usr_low", 5000)

	result := sys.mustInitiatePayment(t, saga.InitiatePaymentRequest{
		PaymentID: "pay_low_1",
		WalletID:  "wal_low",
		UserID:    "usr_low",
		ServiceID: "svc_gas",
		Provider:  "mockpay",
		Amount:    events.Money{Amount: 12000, Currency: "ARS"},
	})
	sys.drain(t)

	if result.HTTPStatus != http.StatusUnprocessableEntity || result.Status != payment.StatusRejected || result.Reason != events.ReasonInsufficientFunds {
		t.Fatalf("expected immediate 422 REJECTED INSUFFICIENT_FUNDS, got %+v", result)
	}
	if sys.mockGateway.SubmitCalls("pay_low_1") != 0 {
		t.Fatalf("gateway must NOT be called on insufficient funds, got %d calls", sys.mockGateway.SubmitCalls("pay_low_1"))
	}
	balance, err := sys.walletService.GetBalance(ctx, "wal_low")
	if err != nil || balance.Balance != 5000 || balance.Held != 0 || balance.Available != 5000 {
		t.Fatalf("wallet balance must remain intact, got %+v err=%v", balance, err)
	}
	if report := sys.metrics.Snapshot(); report.InsufficientFundsCount != 1 || report.PaymentsRejected != 1 {
		t.Fatalf("unexpected metrics: %+v", report)
	}
}

func TestScenario3_GatewayTimeoutReconciliationAndCircuitBreaker(t *testing.T) {
	ctx := context.Background()

	t.Run("timeout -> GATEWAY_UNKNOWN -> reconciliation approves -> COMPLETED", func(t *testing.T) {
		sys := newTestSystem(t, gateway.MockBehavior{
			AlwaysTimeout: true,
			StatusOutcome: ports.GatewayOutcomeApproved,
		}, nil, nil)
		sys.mustCreateFundedWallet(t, "wal_t1", "usr_1", 10000)

		sys.mustInitiatePayment(t, saga.InitiatePaymentRequest{
			PaymentID: "pay_to_ok", WalletID: "wal_t1", UserID: "usr_1", ServiceID: "svc_1", Provider: "mockpay",
			Amount: events.Money{Amount: 8000, Currency: "ARS"},
		})
		sys.drain(t)

		if sys.mockGateway.SubmitCalls("pay_to_ok") != 3 {
			t.Fatalf("expected 3 retry attempts, got %d", sys.mockGateway.SubmitCalls("pay_to_ok"))
		}

		unknownView, err := sys.orchestrator.GetPayment(ctx, "pay_to_ok")
		if err != nil {
			t.Fatalf("GetPayment failed: %v", err)
		}
		heldBalance, err := sys.walletService.GetBalance(ctx, "wal_t1")
		if err != nil {
			t.Fatalf("GetBalance failed: %v", err)
		}
		if unknownView.Status != string(payment.StatusGatewayUnknown) || heldBalance.Held != 8000 || heldBalance.Available != 2000 {
			t.Fatalf("expected GATEWAY_UNKNOWN with held=8000, got view=%+v balance=%+v", unknownView, heldBalance)
		}

		if err := sys.orchestrator.TriggerReconciliation(ctx, "pay_to_ok"); err != nil {
			t.Fatalf("TriggerReconciliation failed: %v", err)
		}
		sys.drain(t)

		completedView, err := sys.orchestrator.GetPayment(ctx, "pay_to_ok")
		if err != nil {
			t.Fatalf("GetPayment failed: %v", err)
		}
		finalBalance, err := sys.walletService.GetBalance(ctx, "wal_t1")
		if err != nil {
			t.Fatalf("GetBalance failed: %v", err)
		}
		if completedView.Status != string(payment.StatusCompleted) || finalBalance.Balance != 2000 || finalBalance.Held != 0 {
			t.Fatalf("expected COMPLETED with balance=2000 held=0, got view=%+v balance=%+v", completedView, finalBalance)
		}
	})

	t.Run("timeout -> GATEWAY_UNKNOWN -> reconciliation NOT_FOUND -> compensation FAILED", func(t *testing.T) {
		sys := newTestSystem(t, gateway.MockBehavior{
			AlwaysTimeout: true,
			StatusOutcome: ports.GatewayOutcomeNotFound,
		}, nil, nil)
		sys.mustCreateFundedWallet(t, "wal_t2", "usr_2", 10000)

		sys.mustInitiatePayment(t, saga.InitiatePaymentRequest{
			PaymentID: "pay_to_nf", WalletID: "wal_t2", UserID: "usr_2", ServiceID: "svc_1", Provider: "mockpay",
			Amount: events.Money{Amount: 6000, Currency: "ARS"},
		})
		sys.drain(t)

		if err := sys.orchestrator.TriggerReconciliation(ctx, "pay_to_nf"); err != nil {
			t.Fatalf("TriggerReconciliation failed: %v", err)
		}
		sys.drain(t)

		failedView, err := sys.orchestrator.GetPayment(ctx, "pay_to_nf")
		if err != nil {
			t.Fatalf("GetPayment failed: %v", err)
		}
		releasedBalance, err := sys.walletService.GetBalance(ctx, "wal_t2")
		if err != nil {
			t.Fatalf("GetBalance failed: %v", err)
		}
		if failedView.Status != string(payment.StatusFailed) || releasedBalance.Balance != 10000 || releasedBalance.Held != 0 || releasedBalance.Available != 10000 {
			t.Fatalf("expected FAILED with full funds released, got view=%+v balance=%+v", failedView, releasedBalance)
		}
	})

	t.Run("timeout -> window expires -> MANUAL_REVIEW -> manual decline releases funds", func(t *testing.T) {
		sys := newTestSystem(t, gateway.MockBehavior{AlwaysTimeout: true, StatusOutcome: ports.GatewayOutcomePending}, nil, nil)
		sys.mustCreateFundedWallet(t, "wal_t3", "usr_3", 10000)

		sys.mustInitiatePayment(t, saga.InitiatePaymentRequest{
			PaymentID: "pay_mr", WalletID: "wal_t3", UserID: "usr_3", ServiceID: "svc_1", Provider: "mockpay",
			Amount: events.Money{Amount: 4000, Currency: "ARS"},
		})
		sys.drain(t)

		if err := sys.orchestrator.ExpireReconciliationWindow(ctx, "pay_mr"); err != nil {
			t.Fatalf("ExpireReconciliationWindow failed: %v", err)
		}
		sys.drain(t)

		reviewView, err := sys.orchestrator.GetPayment(ctx, "pay_mr")
		if err != nil {
			t.Fatalf("GetPayment failed: %v", err)
		}
		heldBalance, err := sys.walletService.GetBalance(ctx, "wal_t3")
		if err != nil {
			t.Fatalf("GetBalance failed: %v", err)
		}
		if reviewView.Status != string(payment.StatusManualReview) || heldBalance.Held != 4000 {
			t.Fatalf("expected MANUAL_REVIEW with held=4000, got view=%+v balance=%+v", reviewView, heldBalance)
		}

		if err := sys.orchestrator.ResolveManualReview(ctx, "pay_mr", false, "", "T+1_NOT_SETTLED"); err != nil {
			t.Fatalf("ResolveManualReview failed: %v", err)
		}
		sys.drain(t)

		failedView, err := sys.orchestrator.GetPayment(ctx, "pay_mr")
		if err != nil {
			t.Fatalf("GetPayment failed: %v", err)
		}
		finalBalance, err := sys.walletService.GetBalance(ctx, "wal_t3")
		if err != nil {
			t.Fatalf("GetBalance failed: %v", err)
		}
		if failedView.Status != string(payment.StatusFailed) || finalBalance.Held != 0 || finalBalance.Available != 10000 {
			t.Fatalf("expected FAILED after manual decline, got view=%+v balance=%+v", failedView, finalBalance)
		}
	})

	t.Run("circuit breaker open -> immediate safe release without contacting gateway", func(t *testing.T) {
		sys := newTestSystem(t, gateway.MockBehavior{SubmitOutcome: ports.GatewayOutcomeApproved}, nil, nil)
		for i := 0; i < 5; i++ {
			sys.breaker.RecordFailure(true)
		}
		if sys.breaker.State() != gateway.StateOpen {
			t.Fatalf("expected circuit OPEN, got %s", sys.breaker.State())
		}

		sys.mustCreateFundedWallet(t, "wal_cb", "usr_cb", 10000)

		sys.mustInitiatePayment(t, saga.InitiatePaymentRequest{
			PaymentID: "pay_cb", WalletID: "wal_cb", UserID: "usr_cb", ServiceID: "svc_1", Provider: "mockpay",
			Amount: events.Money{Amount: 7000, Currency: "ARS"},
		})
		sys.drain(t)

		if sys.mockGateway.SubmitCalls("pay_cb") != 0 {
			t.Fatalf("expected 0 outbound calls with open circuit, got %d", sys.mockGateway.SubmitCalls("pay_cb"))
		}
		failedView, err := sys.orchestrator.GetPayment(ctx, "pay_cb")
		if err != nil {
			t.Fatalf("GetPayment failed: %v", err)
		}
		releasedBalance, err := sys.walletService.GetBalance(ctx, "wal_cb")
		if err != nil {
			t.Fatalf("GetBalance failed: %v", err)
		}
		if failedView.Status != string(payment.StatusFailed) || failedView.FailureReason != events.ReasonCircuitOpen || releasedBalance.Held != 0 || releasedBalance.Available != 10000 {
			t.Fatalf("expected safe immediate release on CIRCUIT_OPEN, got view=%+v balance=%+v", failedView, releasedBalance)
		}
	})
}

func TestScenario4_ConcurrentPaymentsNoDoubleSpend(t *testing.T) {
	ctx := context.Background()

	t.Run("balance 10000 vs two simultaneous payments of 8000", func(t *testing.T) {
		sys := newTestSystem(t, gateway.MockBehavior{SubmitOutcome: ports.GatewayOutcomeApproved}, nil, nil)
		sys.mustCreateFundedWallet(t, "wal_race", "usr_race", 10000)

		var (
			wg      sync.WaitGroup
			startCh = make(chan struct{})
		)
		for idx := 0; idx < 2; idx++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				<-startCh
				_, err := sys.orchestrator.InitiatePayment(ctx, saga.InitiatePaymentRequest{
					PaymentID: fmt.Sprintf("pay_race_%d", index),
					WalletID:  "wal_race",
					UserID:    "usr_race",
					ServiceID: "svc_1",
					Provider:  "mockpay",
					Amount:    events.Money{Amount: 8000, Currency: "ARS"},
				})
				if err != nil {
					t.Errorf("InitiatePayment[%d] error: %v", index, err)
				}
			}(idx)
		}
		close(startCh)
		wg.Wait()
		sys.drain(t)

		completed, rejected := 0, 0
		for idx := 0; idx < 2; idx++ {
			paymentView, err := sys.orchestrator.GetPayment(ctx, fmt.Sprintf("pay_race_%d", idx))
			if err != nil {
				t.Fatalf("GetPayment failed: %v", err)
			}
			if paymentView.Status == string(payment.StatusCompleted) {
				completed++
			} else if paymentView.Status == string(payment.StatusRejected) {
				rejected++
			} else {
				t.Fatalf("unexpected final status: %s", paymentView.Status)
			}
		}

		if completed != 1 || rejected != 1 {
			t.Fatalf("expected 1 COMPLETED and 1 REJECTED, got completed=%d rejected=%d", completed, rejected)
		}

		balance, err := sys.walletService.GetBalance(ctx, "wal_race")
		if err != nil || balance.Balance != 2000 || balance.Held != 0 || balance.Available != 2000 {
			t.Fatalf("unexpected final wallet balance: %+v err=%v", balance, err)
		}
	})

	t.Run("direct concurrent WalletService contention triggers optimistic locking without negative balance", func(t *testing.T) {
		sys := newTestSystem(t, gateway.MockBehavior{}, nil, nil)
		sys.mustCreateFundedWallet(t, "wal_ol", "usr_ol", 5000)

		const concurrentCommands = 12
		var (
			wg      sync.WaitGroup
			startCh = make(chan struct{})
			errCnt  atomic.Int32
		)
		for idx := 0; idx < concurrentCommands; idx++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				<-startCh
				commandEnv, err := events.NewEnvelope(
					events.CommandReserveFunds,
					events.AggregateTypeWallet,
					"wal_ol",
					0,
					events.ReserveFundsCommand{
						PaymentID: fmt.Sprintf("pay_ol_%d", index),
						WalletID:  "wal_ol",
						Amount:    events.Money{Amount: 1000, Currency: "ARS"},
					},
					events.Metadata{CorrelationID: fmt.Sprintf("pay_ol_%d", index)},
					nil,
					nil,
				)
				if err != nil {
					errCnt.Add(1)
					t.Errorf("NewEnvelope failed: %v", err)
					return
				}
				if err := sys.walletService.HandleCommand(ctx, commandEnv); err != nil {
					errCnt.Add(1)
					t.Errorf("HandleCommand[%d] failed: %v", index, err)
				}
			}(idx)
		}
		close(startCh)
		wg.Wait()
		sys.drain(t)

		if errCnt.Load() != 0 {
			t.Fatalf("expected 0 command errors under optimistic locking retries, got %d", errCnt.Load())
		}
		loadedWallet, err := sys.walletService.Load(ctx, "wal_ol")
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if loadedWallet.Balance() != 5000 || loadedWallet.Held() != 5000 || loadedWallet.Available() != 0 {
			t.Fatalf("double spend detected: balance=%d held=%d available=%d",
				loadedWallet.Balance(), loadedWallet.Held(), loadedWallet.Available())
		}
	})
}

func TestScenario5_SystemRecoveryRebuildAndResumeActiveSagas(t *testing.T) {
	ctx := context.Background()
	durableStore := eventstore.NewInMemoryEventStore()
	durableSnapshots := eventstore.NewInMemorySnapshotStore()

	// Boot #1: complete one payment and persist two in-flight payments simulating an abrupt crash.
	firstInstance := newTestSystem(t, gateway.MockBehavior{SubmitOutcome: ports.GatewayOutcomeApproved}, durableStore, durableSnapshots)
	firstInstance.mustCreateFundedWallet(t, "wal_rec", "usr_rec", 50000)

	firstInstance.mustInitiatePayment(t, saga.InitiatePaymentRequest{
		PaymentID: "pay_done", WalletID: "wal_rec", UserID: "usr_rec", ServiceID: "svc_1", Provider: "mockpay",
		Amount: events.Money{Amount: 10000, Currency: "ARS"},
	})
	firstInstance.drain(t)

	loadedWallet, err := firstInstance.walletService.Load(ctx, "wal_rec")
	if err != nil {
		t.Fatalf("load wallet failed: %v", err)
	}
	if err := loadedWallet.Reserve("pay_stuck_pending", events.Money{Amount: 5000, Currency: "ARS"}, events.Metadata{CorrelationID: "pay_stuck_pending"}); err != nil {
		t.Fatalf("seed reserve pay_stuck_pending failed: %v", err)
	}
	if err := loadedWallet.Reserve("pay_stuck_reserved", events.Money{Amount: 15000, Currency: "ARS"}, events.Metadata{CorrelationID: "pay_stuck_reserved"}); err != nil {
		t.Fatalf("seed reserve pay_stuck_reserved failed: %v", err)
	}
	if err := loadedWallet.Reserve("pay_stuck_unknown", events.Money{Amount: 10000, Currency: "ARS"}, events.Metadata{CorrelationID: "pay_stuck_unknown"}); err != nil {
		t.Fatalf("seed reserve pay_stuck_unknown failed: %v", err)
	}
	if err := durableStore.Append(ctx, loadedWallet.StreamID(), loadedWallet.BaseVersion(), loadedWallet.UncommittedEvents()...); err != nil {
		t.Fatalf("append seeded wallet reservations failed: %v", err)
	}

	// Payment still PENDING even though Wallet already persisted FundsReserved before crash.
	stuckPending, err := payment.Initiate(payment.InitiateParams{
		PaymentID: "pay_stuck_pending", WalletID: "wal_rec", UserID: "usr_rec", ServiceID: "svc_0", Provider: "mockpay",
		Amount: events.Money{Amount: 5000, Currency: "ARS"},
	}, events.Metadata{})
	if err != nil {
		t.Fatalf("initiate stuckPending failed: %v", err)
	}
	if err := durableStore.Append(ctx, stuckPending.StreamID(), 0, stuckPending.UncommittedEvents()...); err != nil {
		t.Fatalf("append stuckPending failed: %v", err)
	}

	stuckReserved, err := payment.Initiate(payment.InitiateParams{
		PaymentID: "pay_stuck_reserved", WalletID: "wal_rec", UserID: "usr_rec", ServiceID: "svc_2", Provider: "mockpay",
		Amount: events.Money{Amount: 15000, Currency: "ARS"},
	}, events.Metadata{})
	if err != nil {
		t.Fatalf("initiate stuckReserved failed: %v", err)
	}
	if err := stuckReserved.MarkFundsReserved(20000, events.Metadata{}); err != nil {
		t.Fatalf("MarkFundsReserved stuckReserved failed: %v", err)
	}
	if err := durableStore.Append(ctx, stuckReserved.StreamID(), 0, stuckReserved.UncommittedEvents()...); err != nil {
		t.Fatalf("append stuckReserved failed: %v", err)
	}

	stuckUnknown, err := payment.Initiate(payment.InitiateParams{
		PaymentID: "pay_stuck_unknown", WalletID: "wal_rec", UserID: "usr_rec", ServiceID: "svc_3", Provider: "mockpay",
		Amount: events.Money{Amount: 10000, Currency: "ARS"},
	}, events.Metadata{})
	if err != nil {
		t.Fatalf("initiate stuckUnknown failed: %v", err)
	}
	if err := stuckUnknown.MarkFundsReserved(10000, events.Metadata{}); err != nil {
		t.Fatalf("MarkFundsReserved stuckUnknown failed: %v", err)
	}
	if err := stuckUnknown.MarkGatewayUnknown(events.ReasonGatewayTimeout, events.Metadata{}); err != nil {
		t.Fatalf("MarkGatewayUnknown stuckUnknown failed: %v", err)
	}
	if err := durableStore.Append(ctx, stuckUnknown.StreamID(), 0, stuckUnknown.UncommittedEvents()...); err != nil {
		t.Fatalf("append stuckUnknown failed: %v", err)
	}

	if err := firstInstance.bus.Close(ctx); err != nil {
		t.Fatalf("close firstInstance bus failed: %v", err)
	}

	// Boot #2: fresh process with wiped read models reading only durable stores.
	recoveredInstance := newTestSystem(t, gateway.MockBehavior{
		SubmitOutcome: ports.GatewayOutcomeApproved,
		StatusOutcome: ports.GatewayOutcomeDeclined,
	}, durableStore, durableSnapshots)

	ready, statuses := recoveredInstance.health.Readiness(ctx)
	if !ready || len(statuses) != 3 {
		t.Fatalf("expected healthy readiness on reboot, got ready=%v statuses=%+v", ready, statuses)
	}

	replayedCount, err := recoveredInstance.projector.RebuildAll(ctx, durableStore)
	if err != nil || replayedCount == 0 {
		t.Fatalf("RebuildAll failed: replayed=%d err=%v", replayedCount, err)
	}

	rebuiltBalance, err := recoveredInstance.balanceStore.Get(ctx, "wal_rec")
	if err != nil || rebuiltBalance.Balance != 40000 || rebuiltBalance.Held != 30000 || rebuiltBalance.Available != 10000 {
		t.Fatalf("unexpected rebuilt balance before recovery: %+v err=%v", rebuiltBalance, err)
	}

	recoverySummary, err := recoveredInstance.orchestrator.RecoverActiveSagas(ctx)
	if err != nil {
		t.Fatalf("RecoverActiveSagas failed: %v", err)
	}
	if recoverySummary.Scanned != 4 || recoverySummary.Resumed != 3 {
		t.Fatalf("expected 4 scanned and 3 resumed sagas, got %+v", recoverySummary)
	}
	recoveredInstance.drain(t)

	pendingView, err := recoveredInstance.orchestrator.GetPayment(ctx, "pay_stuck_pending")
	if err != nil || pendingView.Status != string(payment.StatusCompleted) {
		t.Fatalf("expected pay_stuck_pending COMPLETED after Wallet republished FundsReserved, got %+v err=%v", pendingView, err)
	}
	reservedView, err := recoveredInstance.orchestrator.GetPayment(ctx, "pay_stuck_reserved")
	if err != nil || reservedView.Status != string(payment.StatusCompleted) {
		t.Fatalf("expected pay_stuck_reserved COMPLETED, got %+v err=%v", reservedView, err)
	}
	unknownView, err := recoveredInstance.orchestrator.GetPayment(ctx, "pay_stuck_unknown")
	if err != nil || unknownView.Status != string(payment.StatusFailed) {
		t.Fatalf("expected pay_stuck_unknown FAILED, got %+v err=%v", unknownView, err)
	}
	finalBalance, err := recoveredInstance.walletService.GetBalance(ctx, "wal_rec")
	if err != nil || finalBalance.Balance != 20000 || finalBalance.Held != 0 || finalBalance.Available != 20000 {
		t.Fatalf("unexpected final balance after recovery: %+v err=%v", finalBalance, err)
	}

	snapshot, found, err := durableSnapshots.LoadSnapshot(ctx, events.StreamID(events.AggregateTypeWallet, "wal_rec"))
	if err != nil || !found || snapshot.Version == 0 {
		t.Fatalf("expected wallet snapshot to be persisted, found=%v snapshot=%+v err=%v", found, snapshot, err)
	}
}

type failingAppendStore struct {
	ports.EventStore
	failNextAppends atomic.Int32
}

func (f *failingAppendStore) Append(ctx context.Context, streamID string, expectedVersion int64, evts ...events.Envelope) error {
	if f.failNextAppends.Add(-1) >= 0 {
		return errors.New("transient eventstore write failure")
	}
	return f.EventStore.Append(ctx, streamID, expectedVersion, evts...)
}

func TestSagaInboxRollbackAllowsBusRetry(t *testing.T) {
	ctx := context.Background()
	baseStore := eventstore.NewInMemoryEventStore()
	flakyStore := &failingAppendStore{EventStore: baseStore}
	inbox := eventstore.NewInMemoryInboxStore()
	bus := eventbus.NewPartitionedBus(eventbus.Config{
		Partitions:     2,
		MaxAttempts:    3,
		RetryBaseDelay: time.Millisecond,
	})
	defer func() { _ = bus.Close(ctx) }()

	// Seed a payment in PENDING state so HandleWalletEvent(FundsReserved) transitions it and emits SubmitPayment.
	initiated, err := payment.Initiate(payment.InitiateParams{
		PaymentID: "pay_inbox_saga", WalletID: "wal_1", UserID: "usr_1", ServiceID: "svc_1", Provider: "mockpay",
		Amount: events.Money{Amount: 3000, Currency: "ARS"},
	}, events.Metadata{})
	if err != nil {
		t.Fatalf("Initiate failed: %v", err)
	}
	if err := baseStore.Append(ctx, initiated.StreamID(), 0, initiated.UncommittedEvents()...); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Inject a transient storage error on the first saga transition append.
	flakyStore.failNextAppends.Store(1)

	orchestrator := saga.NewPaymentOrchestrator(saga.Config{
		EventStore: flakyStore,
		Publisher:  bus,
		Inbox:      inbox,
	})
	if err := bus.Subscribe(events.TopicWalletsEvents, "payment-saga", orchestrator.HandleWalletEvent); err != nil {
		t.Fatalf("Subscribe wallet events failed: %v", err)
	}

	var (
		submitMu       sync.Mutex
		submitCommands []events.Envelope
	)
	if err := bus.Subscribe(events.TopicGatewayCommands, "gateway-probe", func(_ context.Context, env events.Envelope) error {
		submitMu.Lock()
		submitCommands = append(submitCommands, env)
		submitMu.Unlock()
		return nil
	}); err != nil {
		t.Fatalf("Subscribe gateway commands failed: %v", err)
	}

	reservedEnv, err := events.NewEnvelope(
		events.EventFundsReserved,
		events.AggregateTypeWallet,
		"wal_1",
		2,
		events.FundsReservedPayload{
			PaymentID:      "pay_inbox_saga",
			Amount:         events.Money{Amount: 3000, Currency: "ARS"},
			BalanceAfter:   10000,
			HeldAfter:      3000,
			AvailableAfter: 7000,
		},
		events.Metadata{CorrelationID: "pay_inbox_saga"},
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("NewEnvelope failed: %v", err)
	}

	if err := bus.Publish(ctx, events.TopicWalletsEvents, "wal_1", reservedEnv); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	drainCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := bus.Drain(drainCtx); err != nil {
		t.Fatalf("Drain failed: %v", err)
	}

	if dlqs := bus.DeadLetters(events.TopicWalletsEvents); len(dlqs) != 0 {
		t.Fatalf("expected bus retry to succeed after inbox rollback, got DLQs: %+v", dlqs)
	}
	loaded, err := orchestrator.Load(ctx, "pay_inbox_saga")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if loaded.Status() != payment.StatusFundsReserved {
		t.Fatalf("expected FUNDS_RESERVED after retry, got %s", loaded.Status())
	}

	submitMu.Lock()
	defer submitMu.Unlock()
	if len(submitCommands) != 1 || submitCommands[0].Type != events.CommandSubmitPayment {
		t.Fatalf("expected 1 SubmitPayment command emitted on retry, got %+v", submitCommands)
	}
}

func TestSagaPoisonPillRoutesDirectlyToDLQ(t *testing.T) {
	ctx := context.Background()
	bus := eventbus.NewPartitionedBus(eventbus.Config{
		Partitions:     2,
		MaxAttempts:    3,
		RetryBaseDelay: time.Millisecond,
	})
	defer func() { _ = bus.Close(ctx) }()

	orchestrator := saga.NewPaymentOrchestrator(saga.Config{
		EventStore: eventstore.NewInMemoryEventStore(),
		Publisher:  bus,
		Inbox:      eventstore.NewInMemoryInboxStore(),
	})
	if err := bus.Subscribe(events.TopicGatewayEvents, "payment-saga", orchestrator.HandleGatewayEvent); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	corruptEvent := events.Envelope{
		EventID:       "evt-poison-saga",
		Type:          events.EventGatewayPaymentApproved,
		AggregateType: events.AggregateTypeGateway,
		AggregateID:   "pay_bad",
		Payload:       json.RawMessage(`{invalid-json`),
	}
	if err := bus.Publish(ctx, events.TopicGatewayEvents, "pay_bad", corruptEvent); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	drainCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := bus.Drain(drainCtx); err != nil {
		t.Fatalf("Drain failed: %v", err)
	}

	dlqs := bus.DeadLetters(events.TopicGatewayEvents)
	if len(dlqs) != 1 || dlqs[0].Attempts != 1 {
		t.Fatalf("expected poison pill in DLQ after 1 attempt, got %+v", dlqs)
	}
}

type failOnceOnTopicPublisher struct {
	delegate    ports.EventPublisher
	targetTopic string
	failedOnce  atomic.Bool
}

func (p *failOnceOnTopicPublisher) Publish(ctx context.Context, topic, partitionKey string, env events.Envelope) error {
	if topic == p.targetTopic && p.failedOnce.CompareAndSwap(false, true) {
		return errors.New("transient publish failure after append")
	}
	return p.delegate.Publish(ctx, topic, partitionKey, env)
}

func TestSagaRepublishesCommandAfterAppendThenPublishFailure(t *testing.T) {
	ctx := context.Background()
	store := eventstore.NewInMemoryEventStore()
	inbox := eventstore.NewInMemoryInboxStore()
	bus := eventbus.NewPartitionedBus(eventbus.Config{
		Partitions:     2,
		MaxAttempts:    3,
		RetryBaseDelay: time.Millisecond,
	})
	defer func() { _ = bus.Close(ctx) }()

	initiated, err := payment.Initiate(payment.InitiateParams{
		PaymentID: "pay_pubfail_saga", WalletID: "wal_1", UserID: "usr_1", ServiceID: "svc_1", Provider: "mockpay",
		Amount: events.Money{Amount: 3000, Currency: "ARS"},
	}, events.Metadata{})
	if err != nil {
		t.Fatalf("Initiate failed: %v", err)
	}
	if err := store.Append(ctx, initiated.StreamID(), 0, initiated.UncommittedEvents()...); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Attempt 1 appends PaymentFundsReserved, then fails publishing SubmitPayment to gateway.commands.v1.
	flakyPub := &failOnceOnTopicPublisher{delegate: bus, targetTopic: events.TopicGatewayCommands}
	orchestrator := saga.NewPaymentOrchestrator(saga.Config{
		EventStore: store,
		Publisher:  flakyPub,
		Inbox:      inbox,
	})
	if err := bus.Subscribe(events.TopicWalletsEvents, "payment-saga", orchestrator.HandleWalletEvent); err != nil {
		t.Fatalf("Subscribe wallet events failed: %v", err)
	}

	var (
		cmdMu          sync.Mutex
		submitCommands []events.Envelope
	)
	if err := bus.Subscribe(events.TopicGatewayCommands, "gateway-probe", func(_ context.Context, env events.Envelope) error {
		cmdMu.Lock()
		submitCommands = append(submitCommands, env)
		cmdMu.Unlock()
		return nil
	}); err != nil {
		t.Fatalf("Subscribe gateway commands failed: %v", err)
	}

	reservedEnv, err := events.NewEnvelope(
		events.EventFundsReserved,
		events.AggregateTypeWallet,
		"wal_1",
		2,
		events.FundsReservedPayload{
			PaymentID:      "pay_pubfail_saga",
			Amount:         events.Money{Amount: 3000, Currency: "ARS"},
			BalanceAfter:   10000,
			HeldAfter:      3000,
			AvailableAfter: 7000,
		},
		events.Metadata{CorrelationID: "pay_pubfail_saga"},
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("NewEnvelope failed: %v", err)
	}

	if err := bus.Publish(ctx, events.TopicWalletsEvents, "wal_1", reservedEnv); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	drainCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := bus.Drain(drainCtx); err != nil {
		t.Fatalf("Drain failed: %v", err)
	}

	if dlqs := bus.DeadLetters(events.TopicWalletsEvents); len(dlqs) != 0 {
		t.Fatalf("expected retry to recover after SubmitPayment publish failure, got DLQs: %+v", dlqs)
	}
	cmdMu.Lock()
	defer cmdMu.Unlock()
	if len(submitCommands) != 1 || submitCommands[0].Type != events.CommandSubmitPayment {
		t.Fatalf("expected SubmitPayment emitted on retry, got %+v", submitCommands)
	}
}

func TestInitiatePaymentSyncWaitTimeoutAndContextCancellation(t *testing.T) {
	t.Run("SyncReserveWait expiration returns 202 PENDING while saga completes asynchronously", func(t *testing.T) {
		ctx := context.Background()
		store := eventstore.NewInMemoryEventStore()
		bus := eventbus.NewPartitionedBus(eventbus.Config{Partitions: 2})
		defer func() { _ = bus.Close(ctx) }()

		balanceStore := readmodel.NewWalletBalanceStore()
		paymentStore := readmodel.NewPaymentViewStore()
		walletService := appwallet.NewService(appwallet.Config{
			EventStore:       store,
			BalanceReadModel: balanceStore,
			Publisher:        bus,
		})
		if _, err := walletService.CreateWallet(ctx, "wal_slow", "usr_slow", "ARS"); err != nil {
			t.Fatalf("CreateWallet failed: %v", err)
		}
		if _, err := walletService.Deposit(ctx, "wal_slow", "dep_1", events.Money{Amount: 10000, Currency: "ARS"}); err != nil {
			t.Fatalf("Deposit failed: %v", err)
		}

		releaseWallet := make(chan struct{})
		if err := bus.Subscribe(events.TopicWalletsCommands, "slow-wallet", func(hCtx context.Context, env events.Envelope) error {
			<-releaseWallet
			return walletService.HandleCommand(hCtx, env)
		}); err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}

		orchestrator := saga.NewPaymentOrchestrator(saga.Config{
			EventStore:      store,
			PaymentViews:    paymentStore,
			Publisher:       bus,
			SyncReserveWait: 15 * time.Millisecond,
		})
		if err := bus.Subscribe(events.TopicWalletsEvents, "payment-saga", orchestrator.HandleWalletEvent); err != nil {
			t.Fatalf("Subscribe wallet events failed: %v", err)
		}

		res, err := orchestrator.InitiatePayment(ctx, saga.InitiatePaymentRequest{
			PaymentID: "pay_slow_1", WalletID: "wal_slow", UserID: "usr_slow", ServiceID: "svc_1", Provider: "mockpay",
			Amount: events.Money{Amount: 4000, Currency: "ARS"},
		})
		if err != nil {
			t.Fatalf("InitiatePayment failed: %v", err)
		}
		if res.HTTPStatus != http.StatusAccepted || res.Status != payment.StatusPending {
			t.Fatalf("expected 202 PENDING on wait expiry, got %+v", res)
		}

		close(releaseWallet)
		drainCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		if err := bus.Drain(drainCtx); err != nil {
			t.Fatalf("Drain failed: %v", err)
		}

		view, err := orchestrator.GetPayment(ctx, "pay_slow_1")
		if err != nil || view.Status != string(payment.StatusFundsReserved) {
			t.Fatalf("expected FUNDS_RESERVED asynchronously after unblock, got %+v err=%v", view, err)
		}
	})

	t.Run("caller context cancellation during wait still completes Idempotency-Key", func(t *testing.T) {
		baseCtx := context.Background()
		store := eventstore.NewInMemoryEventStore()
		idempotencyStore := eventstore.NewInMemoryIdempotencyStore(nil)
		bus := eventbus.NewPartitionedBus(eventbus.Config{Partitions: 2})
		defer func() { _ = bus.Close(baseCtx) }()

		walletReached := make(chan struct{})
		releaseWallet := make(chan struct{})
		defer close(releaseWallet)

		if err := bus.Subscribe(events.TopicWalletsCommands, "delayed-wallet", func(_ context.Context, _ events.Envelope) error {
			close(walletReached)
			<-releaseWallet
			return nil
		}); err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}

		orchestrator := saga.NewPaymentOrchestrator(saga.Config{
			EventStore:      store,
			Publisher:       bus,
			Idempotency:     idempotencyStore,
			SyncReserveWait: 2 * time.Second,
		})

		reqCtx, cancelReq := context.WithCancel(baseCtx)
		go func() {
			<-walletReached
			cancelReq()
		}()

		req := saga.InitiatePaymentRequest{
			IdempotencyKey: "idem_cancel_1",
			PaymentID:      "pay_cancel_1",
			WalletID:       "wal_cancel",
			UserID:         "usr_cancel",
			ServiceID:      "svc_1",
			Provider:       "mockpay",
			Amount:         events.Money{Amount: 5000, Currency: "ARS"},
		}
		firstRes, err := orchestrator.InitiatePayment(reqCtx, req)
		if err != nil {
			t.Fatalf("InitiatePayment failed: %v", err)
		}
		if firstRes.HTTPStatus != http.StatusAccepted || firstRes.Status != payment.StatusPending {
			t.Fatalf("expected 202 PENDING on caller cancel, got %+v", firstRes)
		}

		// Retrying with the same Idempotency-Key must return the cached result, NOT ErrIdempotencyInProgress.
		retryRes, err := orchestrator.InitiatePayment(baseCtx, req)
		if err != nil {
			t.Fatalf("retry after caller cancellation failed (key may be stuck IN_PROGRESS): %v", err)
		}
		if !retryRes.FromIdempotencyCache || retryRes.PaymentID != firstRes.PaymentID {
			t.Fatalf("expected cached response on retry, got %+v", retryRes)
		}
	})
}

func mustCommandEnvelope(t *testing.T, commandType, aggregateType, aggregateID, correlationID string, payload any) events.Envelope {
	t.Helper()
	env, err := events.NewEnvelope(commandType, aggregateType, aggregateID, 0, payload,
		events.Metadata{CorrelationID: correlationID}, nil, nil)
	if err != nil {
		t.Fatalf("NewEnvelope(%s) failed: %v", commandType, err)
	}
	return env
}

func TestRegressionDuplicateSubmitWithOpenBreakerDoesNotReleaseSubmittedFunds(t *testing.T) {
	ctx := context.Background()
	sys := newTestSystem(t, gateway.MockBehavior{SubmitOutcome: ports.GatewayOutcomeAccepted}, nil, nil)
	sys.mustCreateFundedWallet(t, "wal_c1", "usr_c1", 10000)
	sys.mustInitiatePayment(t, saga.InitiatePaymentRequest{
		PaymentID: "pay_c1", WalletID: "wal_c1", UserID: "usr_c1", ServiceID: "svc_1", Provider: "mockpay",
		Amount: events.Money{Amount: 6000, Currency: "ARS"},
	})
	sys.drain(t)
	if view, err := sys.orchestrator.GetPayment(ctx, "pay_c1"); err != nil || view.Status != string(payment.StatusSubmitted) {
		t.Fatalf("expected SUBMITTED before duplicate submit, got %+v err=%v", view, err)
	}
	for i := 0; i < 5; i++ {
		sys.breaker.RecordFailure(true)
	}

	// Duplicate SubmitPayment hits an open breaker, followed by a late NotSent on the same partition.
	duplicateSubmit := mustCommandEnvelope(t, events.CommandSubmitPayment, events.AggregateTypeGateway, "pay_c1", "pay_c1",
		events.SubmitPaymentCommand{PaymentID: "pay_c1", Provider: "mockpay", ServiceID: "svc_1", Amount: events.Money{Amount: 6000, Currency: "ARS"}})
	lateNotSent := mustCommandEnvelope(t, events.EventGatewaySubmissionNotSent, events.AggregateTypeGateway, "pay_c1", "pay_c1",
		events.GatewaySubmissionNotSentPayload{PaymentID: "pay_c1", Provider: "mockpay", Reason: events.ReasonCircuitOpen})
	if err := sys.bus.Publish(ctx, events.TopicGatewayCommands, "pay_c1", duplicateSubmit); err != nil {
		t.Fatalf("Publish duplicate submit failed: %v", err)
	}
	if err := sys.bus.Publish(ctx, events.TopicGatewayEvents, "pay_c1", lateNotSent); err != nil {
		t.Fatalf("Publish late NotSent failed: %v", err)
	}
	sys.drain(t)

	if calls := sys.mockGateway.SubmitCalls("pay_c1"); calls != 1 {
		t.Fatalf("expected exactly 1 outbound submit, got %d", calls)
	}
	view, err := sys.orchestrator.GetPayment(ctx, "pay_c1")
	if err != nil || view.Status != string(payment.StatusGatewayUnknown) {
		t.Fatalf("expected GATEWAY_UNKNOWN (never FAILED), got %+v err=%v", view, err)
	}
	balance, err := sys.walletService.GetBalance(ctx, "wal_c1")
	if err != nil || balance.Held != 6000 || balance.Available != 4000 {
		t.Fatalf("funds must remain held for an already-submitted payment, got %+v err=%v", balance, err)
	}
}

func TestRegressionRefundTimeoutThenOpenBreakerKeepsPendingRefund(t *testing.T) {
	ctx := context.Background()
	sys := newTestSystem(t, gateway.MockBehavior{
		SubmitOutcome:       ports.GatewayOutcomeApproved,
		RefundAlwaysTimeout: true,
	}, nil, nil)
	sys.mustCreateFundedWallet(t, "wal_c2", "usr_c2", 20000)
	sys.mustInitiatePayment(t, saga.InitiatePaymentRequest{
		PaymentID: "pay_c2", WalletID: "wal_c2", UserID: "usr_c2", ServiceID: "svc_1", Provider: "mockpay",
		Amount: events.Money{Amount: 10000, Currency: "ARS"},
	})
	sys.drain(t)

	var refundDeclined atomic.Int32
	if err := sys.bus.Subscribe(events.TopicGatewayEvents, "refund-decline-probe", func(_ context.Context, env events.Envelope) error {
		if env.Type == events.EventGatewayRefundDeclined {
			refundDeclined.Add(1)
		}
		return nil
	}); err != nil {
		t.Fatalf("Subscribe probe failed: %v", err)
	}

	if err := sys.orchestrator.RequestRefund(ctx, "pay_c2", "ref_c2", events.Money{Amount: 4000, Currency: "ARS"}); err != nil {
		t.Fatalf("RequestRefund failed: %v", err)
	}
	sys.drain(t)

	if sys.breaker.State() != gateway.StateOpen {
		t.Fatalf("expected breaker OPEN after repeated refund timeouts, got %s", sys.breaker.State())
	}
	if refundDeclined.Load() != 0 {
		t.Fatalf("timeouts or open breaker must never emit GatewayRefundDeclined, got %d", refundDeclined.Load())
	}
	loaded, err := sys.orchestrator.Load(ctx, "pay_c2")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if !loaded.HasPendingRefund("ref_c2") || loaded.PendingRefundTotal() != 4000 || loaded.RefundedTotal() != 0 {
		t.Fatalf("expected pending refund kept reserved, pending=%d refunded=%d", loaded.PendingRefundTotal(), loaded.RefundedTotal())
	}
	if balance, err := sys.walletService.GetBalance(ctx, "wal_c2"); err != nil || balance.Balance != 10000 {
		t.Fatalf("wallet must not be credited on uncertain refund, got %+v err=%v", balance, err)
	}
	if dlqs := sys.bus.DeadLetters(events.TopicGatewayCommands); len(dlqs) != 1 {
		t.Fatalf("expected uncertain refund parked in DLQ for redrive, got %d entries", len(dlqs))
	}
}

func TestRegressionLateReserveAfterRejectionLeavesNoOrphanHold(t *testing.T) {
	ctx := context.Background()

	t.Run("duplicate ReserveFunds after rejection does not hold funds even after a top-up", func(t *testing.T) {
		sys := newTestSystem(t, gateway.MockBehavior{SubmitOutcome: ports.GatewayOutcomeApproved}, nil, nil)
		sys.mustCreateFundedWallet(t, "wal_m1", "usr_m1", 5000)
		result := sys.mustInitiatePayment(t, saga.InitiatePaymentRequest{
			PaymentID: "pay_m1", WalletID: "wal_m1", UserID: "usr_m1", ServiceID: "svc_1", Provider: "mockpay",
			Amount: events.Money{Amount: 8000, Currency: "ARS"},
		})
		sys.drain(t)
		if result.Status != payment.StatusRejected {
			t.Fatalf("expected REJECTED, got %+v", result)
		}
		if _, err := sys.walletService.Deposit(ctx, "wal_m1", "dep_topup", events.Money{Amount: 10000, Currency: "ARS"}); err != nil {
			t.Fatalf("top-up Deposit failed: %v", err)
		}

		duplicateReserve := mustCommandEnvelope(t, events.CommandReserveFunds, events.AggregateTypeWallet, "wal_m1", "pay_m1",
			events.ReserveFundsCommand{PaymentID: "pay_m1", WalletID: "wal_m1", Amount: events.Money{Amount: 8000, Currency: "ARS"}})
		if err := sys.bus.Publish(ctx, events.TopicWalletsCommands, "wal_m1", duplicateReserve); err != nil {
			t.Fatalf("Publish duplicate reserve failed: %v", err)
		}
		sys.drain(t)

		balance, err := sys.walletService.GetBalance(ctx, "wal_m1")
		if err != nil || balance.Balance != 15000 || balance.Held != 0 || balance.Available != 15000 {
			t.Fatalf("expected no orphaned hold, got %+v err=%v", balance, err)
		}
		if view, err := sys.orchestrator.GetPayment(ctx, "pay_m1"); err != nil || view.Status != string(payment.StatusRejected) {
			t.Fatalf("expected payment to stay REJECTED, got %+v err=%v", view, err)
		}
		if calls := sys.mockGateway.SubmitCalls("pay_m1"); calls != 0 {
			t.Fatalf("rejected payment must never reach the gateway, got %d calls", calls)
		}
	})

	t.Run("late FundsReserved for a REJECTED payment triggers defensive ReleaseFunds", func(t *testing.T) {
		sys := newTestSystem(t, gateway.MockBehavior{SubmitOutcome: ports.GatewayOutcomeApproved}, nil, nil)
		sys.mustCreateFundedWallet(t, "wal_m1b", "usr_m1b", 10000)

		rejectedPayment, err := payment.Initiate(payment.InitiateParams{
			PaymentID: "pay_m1b", WalletID: "wal_m1b", UserID: "usr_m1b", ServiceID: "svc_1", Provider: "mockpay",
			Amount: events.Money{Amount: 3000, Currency: "ARS"},
		}, events.Metadata{})
		if err != nil {
			t.Fatalf("Initiate failed: %v", err)
		}
		if err := rejectedPayment.MarkRejected(events.ReasonInsufficientFunds, events.Metadata{}); err != nil {
			t.Fatalf("MarkRejected failed: %v", err)
		}
		if err := sys.store.Append(ctx, rejectedPayment.StreamID(), 0, rejectedPayment.UncommittedEvents()...); err != nil {
			t.Fatalf("append rejected payment failed: %v", err)
		}

		orphanWallet, err := sys.walletService.Load(ctx, "wal_m1b")
		if err != nil {
			t.Fatalf("Load wallet failed: %v", err)
		}
		if err := orphanWallet.Reserve("pay_m1b", events.Money{Amount: 3000, Currency: "ARS"}, events.Metadata{CorrelationID: "pay_m1b"}); err != nil {
			t.Fatalf("seed orphan Reserve failed: %v", err)
		}
		orphanEvents := orphanWallet.UncommittedEvents()
		if err := sys.store.Append(ctx, orphanWallet.StreamID(), orphanWallet.BaseVersion(), orphanEvents...); err != nil {
			t.Fatalf("append orphan reservation failed: %v", err)
		}
		if err := sys.bus.Publish(ctx, events.TopicWalletsEvents, "wal_m1b", orphanEvents[0]); err != nil {
			t.Fatalf("Publish late FundsReserved failed: %v", err)
		}
		sys.drain(t)

		reloadedWallet, err := sys.walletService.Load(ctx, "wal_m1b")
		if err != nil {
			t.Fatalf("reload wallet failed: %v", err)
		}
		if reloadedWallet.Held() != 0 || reloadedWallet.Available() != 10000 {
			t.Fatalf("expected orphaned hold to be compensated, held=%d available=%d", reloadedWallet.Held(), reloadedWallet.Available())
		}
		if loaded, err := sys.orchestrator.Load(ctx, "pay_m1b"); err != nil || loaded.Status() != payment.StatusRejected {
			t.Fatalf("expected payment to stay REJECTED, got err=%v", err)
		}
	})
}
