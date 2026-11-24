package saga_test

import (
	"context"
	"encoding/json"
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
	store      *eventstore.InMemoryEventStore
	snapshots  *eventstore.InMemorySnapshotStore
	inbox      *eventstore.InMemoryInboxStore
	idemp      *eventstore.InMemoryIdempotencyStore
	bus        *eventbus.PartitionedBus
	balances   *readmodel.WalletBalanceStore
	txs        *readmodel.WalletTransactionStore
	payViews   *readmodel.PaymentViewStore
	projector  *readmodel.Projector
	mockGW     *gateway.MockGateway
	breaker    *gateway.CircuitBreaker
	gwAdapter  *gateway.Adapter
	walletSvc  *appwallet.Service
	orch       *saga.PaymentOrchestrator
	metrics    *observability.Collector
	health     *observability.HealthRegistry
	webhookSec string
}

func newTestSystem(t *testing.T, defaultBehavior gateway.MockBehavior, store *eventstore.InMemoryEventStore, snaps *eventstore.InMemorySnapshotStore) *testSystem {
	t.Helper()

	if store == nil {
		store = eventstore.NewInMemoryEventStore()
	}
	if snaps == nil {
		snaps = eventstore.NewInMemorySnapshotStore()
	}

	metrics := observability.NewCollector()
	inbox := eventstore.NewInMemoryInboxStore()
	idemp := eventstore.NewInMemoryIdempotencyStore(nil)
	bus := eventbus.NewPartitionedBus(eventbus.Config{
		Partitions:     8,
		MaxAttempts:    3,
		RetryBaseDelay: time.Millisecond,
		RetryMaxDelay:  10 * time.Millisecond,
		Metrics:        metrics,
	})

	balances := readmodel.NewWalletBalanceStore()
	txs := readmodel.NewWalletTransactionStore()
	payViews := readmodel.NewPaymentViewStore()
	projector := readmodel.NewProjector(balances, txs, payViews)

	mockGW := gateway.NewMockGateway(defaultBehavior)
	breaker := gateway.NewCircuitBreaker(gateway.CircuitBreakerConfig{
		Provider:            "mockpay",
		ConsecutiveTimeouts: 5,
		OpenTimeout:         30 * time.Second,
		HalfOpenMaxProbes:   3,
		OnStateChange: func(provider string, _, to gateway.CircuitState) {
			metrics.RecordCircuitBreakerState(provider, string(to))
		},
	})

	webhookSec := "test-webhook-secret"
	gwAdapter := gateway.NewAdapter(gateway.AdapterConfig{
		Gateway:       mockGW,
		Publisher:     bus,
		Inbox:         inbox,
		Breaker:       breaker,
		WebhookSecret: webhookSec,
		Metrics:       metrics,
		Retry: gateway.RetryConfig{
			MaxAttempts:    3,
			BaseDelay:      time.Millisecond,
			MaxDelay:       5 * time.Millisecond,
			AttemptTimeout: time.Second,
			Sleep:          func(context.Context, time.Duration) error { return nil },
		},
	})

	walletSvc := appwallet.NewService(appwallet.Config{
		EventStore:       store,
		SnapshotStore:    snaps,
		SnapshotInterval: 3,
		BalanceReadModel: balances,
		Publisher:        bus,
		Inbox:            inbox,
		Metrics:          metrics,
		MaxLockRetries:   10,
		LockRetryMin:     time.Millisecond,
		LockRetryMax:     5 * time.Millisecond,
	})

	orch := saga.NewPaymentOrchestrator(saga.Config{
		EventStore:      store,
		PaymentViews:    payViews,
		Publisher:       bus,
		Inbox:           inbox,
		Idempotency:     idemp,
		Metrics:         metrics,
		SyncReserveWait: 2 * time.Second,
		MaxLockRetries:  5,
	})

	// Wire topic subscriptions matching docs/03-event-specification.md.
	_ = bus.Subscribe(events.TopicWalletsCommands, "wallet-service", walletSvc.HandleCommand)
	_ = bus.Subscribe(events.TopicGatewayCommands, "gateway-adapter", gwAdapter.HandleCommand)
	_ = bus.Subscribe(events.TopicWalletsEvents, "payment-saga", orch.HandleWalletEvent)
	_ = bus.Subscribe(events.TopicGatewayEvents, "payment-saga", orch.HandleGatewayEvent)
	_ = bus.Subscribe(events.TopicWalletsEvents, "readmodel-projector", projector.HandleEvent)
	_ = bus.Subscribe(events.TopicPaymentsEvents, "readmodel-projector", projector.HandleEvent)
	_ = bus.Subscribe(events.TopicPaymentsEvents, "metrics-collector", metrics.HandleEvent)
	_ = bus.Subscribe(events.TopicWalletsEvents, "metrics-collector", metrics.HandleEvent)
	_ = bus.Subscribe(events.TopicGatewayEvents, "metrics-collector", metrics.HandleEvent)

	health := observability.NewHealthRegistry(nil)
	health.Register(store)
	health.Register(bus)
	health.Register(mockGW)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = bus.Close(ctx)
	})

	return &testSystem{
		store:      store,
		snapshots:  snaps,
		inbox:      inbox,
		idemp:      idemp,
		bus:        bus,
		balances:   balances,
		txs:        txs,
		payViews:   payViews,
		projector:  projector,
		mockGW:     mockGW,
		breaker:    breaker,
		gwAdapter:  gwAdapter,
		walletSvc:  walletSvc,
		orch:       orch,
		metrics:    metrics,
		health:     health,
		webhookSec: webhookSec,
	}
}

func (s *testSystem) drain(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.bus.Drain(ctx); err != nil {
		t.Fatalf("bus.Drain failed: %v", err)
	}
}

// Scenario 1: Happy path (async gateway acceptance + HMAC webhook approval) + Idempotency + Refunds.
func TestScenario1_HappyPathWebhookAndRefunds(t *testing.T) {
	ctx := context.Background()
	sys := newTestSystem(t, gateway.MockBehavior{
		SubmitOutcome: ports.GatewayOutcomeAccepted, // Async flow requiring webhook
	}, nil, nil)

	_, err := sys.walletSvc.CreateWallet(ctx, "wal_1", "usr_1", "ARS")
	if err != nil {
		t.Fatalf("CreateWallet failed: %v", err)
	}
	_, err = sys.walletSvc.Deposit(ctx, "wal_1", "dep_1", events.Money{Amount: 50000, Currency: "ARS"})
	if err != nil {
		t.Fatalf("Deposit failed: %v", err)
	}

	req := saga.InitiatePaymentRequest{
		IdempotencyKey: "idem_happy_1",
		PaymentID:      "pay_happy_1",
		WalletID:       "wal_1",
		UserID:         "usr_1",
		ServiceID:      "svc_internet",
		Provider:       "mockpay",
		Amount:         events.Money{Amount: 15000, Currency: "ARS"},
	}

	res, err := sys.orch.InitiatePayment(ctx, req)
	if err != nil {
		t.Fatalf("InitiatePayment failed: %v", err)
	}
	if res.HTTPStatus != http.StatusAccepted || res.Status != payment.StatusFundsReserved || res.AvailableBalance != 35000 {
		t.Fatalf("unexpected initial response: %+v", res)
	}

	// Duplicate API request with same Idempotency-Key returns cached response without charging again.
	dupRes, err := sys.orch.InitiatePayment(ctx, req)
	if err != nil {
		t.Fatalf("duplicate InitiatePayment failed: %v", err)
	}
	if !dupRes.FromIdempotencyCache || dupRes.PaymentID != res.PaymentID {
		t.Fatalf("expected cached idempotent response, got %+v", dupRes)
	}

	sys.drain(t)

	// Payment must be SUBMITTED waiting for webhook, with funds held.
	pv, err := sys.orch.GetPayment(ctx, "pay_happy_1")
	if err != nil || pv.Status != string(payment.StatusSubmitted) {
		t.Fatalf("expected SUBMITTED, got %+v err=%v", pv, err)
	}
	bal, _ := sys.walletSvc.GetBalance(ctx, "wal_1")
	if bal.Balance != 50000 || bal.Held != 15000 || bal.Available != 35000 {
		t.Fatalf("expected funds held while SUBMITTED, got %+v", bal)
	}

	// External gateway delivers signed HMAC approval webhook.
	whBody, _ := json.Marshal(gateway.WebhookPayload{
		ProviderEventID: "wh_evt_001",
		PaymentID:       "pay_happy_1",
		Provider:        "mockpay",
		ProviderRef:     "prov_pay_happy_1",
		Status:          string(ports.GatewayOutcomeApproved),
	})
	sig := gateway.SignWebhook(sys.webhookSec, whBody)
	if err := sys.gwAdapter.HandleWebhook(ctx, sig, whBody); err != nil {
		t.Fatalf("HandleWebhook failed: %v", err)
	}
	sys.drain(t)

	// Payment is COMPLETED and funds are permanently captured.
	pv, _ = sys.orch.GetPayment(ctx, "pay_happy_1")
	if pv.Status != string(payment.StatusCompleted) {
		t.Fatalf("expected COMPLETED, got %+v", pv)
	}
	bal, _ = sys.walletSvc.GetBalance(ctx, "wal_1")
	if bal.Balance != 35000 || bal.Held != 0 || bal.Available != 35000 {
		t.Fatalf("expected captured balance 35000, got %+v", bal)
	}

	// Partial refund (5000) keeps payment COMPLETED; remaining refund (10000) transitions to REFUNDED.
	if err := sys.orch.RequestRefund(ctx, "pay_happy_1", "ref_1", events.Money{Amount: 5000, Currency: "ARS"}); err != nil {
		t.Fatalf("partial refund failed: %v", err)
	}
	sys.drain(t)

	pv, _ = sys.orch.GetPayment(ctx, "pay_happy_1")
	bal, _ = sys.walletSvc.GetBalance(ctx, "wal_1")
	if pv.Status != string(payment.StatusCompleted) || pv.RefundedTotal != 5000 || bal.Balance != 40000 {
		t.Fatalf("unexpected state after partial refund: pv=%+v bal=%+v", pv, bal)
	}

	if err := sys.orch.RequestRefund(ctx, "pay_happy_1", "ref_2", events.Money{Amount: 10000, Currency: "ARS"}); err != nil {
		t.Fatalf("final refund failed: %v", err)
	}
	sys.drain(t)

	pv, _ = sys.orch.GetPayment(ctx, "pay_happy_1")
	bal, _ = sys.walletSvc.GetBalance(ctx, "wal_1")
	if pv.Status != string(payment.StatusRefunded) || pv.RefundedTotal != 15000 || bal.Balance != 50000 {
		t.Fatalf("unexpected state after full refund: pv=%+v bal=%+v", pv, bal)
	}

	txHistory, _ := sys.txs.ListByWallet(ctx, "wal_1")
	if len(txHistory) != 5 { // deposit, reserve, capture, refund1, refund2
		t.Fatalf("expected 5 wallet transactions, got %d", len(txHistory))
	}

	rep := sys.metrics.Snapshot()
	if rep.PaymentsCompleted != 1 || rep.PaymentsRefunded != 2 || rep.SuccessRate != 1.0 {
		t.Fatalf("unexpected metrics report: %+v", rep)
	}
}

// Scenario 2: Insufficient funds immediate 422 rejection within 2s bounded wait.
func TestScenario2_InsufficientFundsImmediateRejection(t *testing.T) {
	ctx := context.Background()
	sys := newTestSystem(t, gateway.MockBehavior{SubmitOutcome: ports.GatewayOutcomeApproved}, nil, nil)

	_, _ = sys.walletSvc.CreateWallet(ctx, "wal_low", "usr_low", "ARS")
	_, _ = sys.walletSvc.Deposit(ctx, "wal_low", "dep_1", events.Money{Amount: 5000, Currency: "ARS"})

	res, err := sys.orch.InitiatePayment(ctx, saga.InitiatePaymentRequest{
		PaymentID: "pay_low_1",
		WalletID:  "wal_low",
		UserID:    "usr_low",
		ServiceID: "svc_gas",
		Provider:  "mockpay",
		Amount:    events.Money{Amount: 12000, Currency: "ARS"},
	})
	if err != nil {
		t.Fatalf("InitiatePayment failed: %v", err)
	}
	if res.HTTPStatus != http.StatusUnprocessableEntity || res.Status != payment.StatusRejected || res.Reason != events.ReasonInsufficientFunds {
		t.Fatalf("expected immediate 422 REJECTED INSUFFICIENT_FUNDS, got %+v", res)
	}

	sys.drain(t)

	if sys.mockGW.SubmitCalls("pay_low_1") != 0 {
		t.Fatalf("gateway must NOT be called on insufficient funds, got %d calls", sys.mockGW.SubmitCalls("pay_low_1"))
	}
	bal, _ := sys.walletSvc.GetBalance(ctx, "wal_low")
	if bal.Balance != 5000 || bal.Held != 0 || bal.Available != 5000 {
		t.Fatalf("wallet balance must remain intact, got %+v", bal)
	}
	if rep := sys.metrics.Snapshot(); rep.InsufficientFundsCount != 1 || rep.PaymentsRejected != 1 {
		t.Fatalf("unexpected metrics: %+v", rep)
	}
}

// Scenario 3: Gateway timeout -> retries -> GATEWAY_UNKNOWN (funds held!) -> reconciliation / manual review / circuit breaker.
func TestScenario3_GatewayTimeoutReconciliationAndCircuitBreaker(t *testing.T) {
	ctx := context.Background()

	t.Run("timeout -> GATEWAY_UNKNOWN -> reconciliation approves -> COMPLETED", func(t *testing.T) {
		sys := newTestSystem(t, gateway.MockBehavior{
			AlwaysTimeout: true,
			StatusOutcome: ports.GatewayOutcomeApproved,
		}, nil, nil)

		_, _ = sys.walletSvc.CreateWallet(ctx, "wal_t1", "usr_1", "ARS")
		_, _ = sys.walletSvc.Deposit(ctx, "wal_t1", "dep_1", events.Money{Amount: 10000, Currency: "ARS"})

		_, err := sys.orch.InitiatePayment(ctx, saga.InitiatePaymentRequest{
			PaymentID: "pay_to_ok", WalletID: "wal_t1", UserID: "usr_1", ServiceID: "svc_1", Provider: "mockpay",
			Amount: events.Money{Amount: 8000, Currency: "ARS"},
		})
		if err != nil {
			t.Fatalf("InitiatePayment failed: %v", err)
		}
		sys.drain(t)

		if sys.mockGW.SubmitCalls("pay_to_ok") != 3 {
			t.Fatalf("expected 3 retry attempts, got %d", sys.mockGW.SubmitCalls("pay_to_ok"))
		}

		// Golden rule: payment is GATEWAY_UNKNOWN and funds remain HELD (NOT released!).
		pv, _ := sys.orch.GetPayment(ctx, "pay_to_ok")
		bal, _ := sys.walletSvc.GetBalance(ctx, "wal_t1")
		if pv.Status != string(payment.StatusGatewayUnknown) || bal.Held != 8000 || bal.Available != 2000 {
			t.Fatalf("expected GATEWAY_UNKNOWN with held=8000, got pv=%+v bal=%+v", pv, bal)
		}

		// Active reconciliation confirms provider approved -> captures funds -> COMPLETED.
		if err := sys.orch.TriggerReconciliation(ctx, "pay_to_ok"); err != nil {
			t.Fatalf("TriggerReconciliation failed: %v", err)
		}
		sys.drain(t)

		pv, _ = sys.orch.GetPayment(ctx, "pay_to_ok")
		bal, _ = sys.walletSvc.GetBalance(ctx, "wal_t1")
		if pv.Status != string(payment.StatusCompleted) || bal.Balance != 2000 || bal.Held != 0 {
			t.Fatalf("expected COMPLETED with balance=2000 held=0, got pv=%+v bal=%+v", pv, bal)
		}
	})

	t.Run("timeout -> GATEWAY_UNKNOWN -> reconciliation NOT_FOUND -> compensation FAILED", func(t *testing.T) {
		sys := newTestSystem(t, gateway.MockBehavior{
			AlwaysTimeout: true,
			StatusOutcome: ports.GatewayOutcomeNotFound,
		}, nil, nil)

		_, _ = sys.walletSvc.CreateWallet(ctx, "wal_t2", "usr_2", "ARS")
		_, _ = sys.walletSvc.Deposit(ctx, "wal_t2", "dep_1", events.Money{Amount: 10000, Currency: "ARS"})

		_, _ = sys.orch.InitiatePayment(ctx, saga.InitiatePaymentRequest{
			PaymentID: "pay_to_nf", WalletID: "wal_t2", UserID: "usr_2", ServiceID: "svc_1", Provider: "mockpay",
			Amount: events.Money{Amount: 6000, Currency: "ARS"},
		})
		sys.drain(t)

		if err := sys.orch.TriggerReconciliation(ctx, "pay_to_nf"); err != nil {
			t.Fatalf("TriggerReconciliation failed: %v", err)
		}
		sys.drain(t)

		pv, _ := sys.orch.GetPayment(ctx, "pay_to_nf")
		bal, _ := sys.walletSvc.GetBalance(ctx, "wal_t2")
		if pv.Status != string(payment.StatusFailed) || bal.Balance != 10000 || bal.Held != 0 || bal.Available != 10000 {
			t.Fatalf("expected FAILED with full funds released, got pv=%+v bal=%+v", pv, bal)
		}
	})

	t.Run("timeout -> window expires -> MANUAL_REVIEW -> manual decline releases funds", func(t *testing.T) {
		sys := newTestSystem(t, gateway.MockBehavior{AlwaysTimeout: true, StatusOutcome: ports.GatewayOutcomePending}, nil, nil)

		_, _ = sys.walletSvc.CreateWallet(ctx, "wal_t3", "usr_3", "ARS")
		_, _ = sys.walletSvc.Deposit(ctx, "wal_t3", "dep_1", events.Money{Amount: 10000, Currency: "ARS"})

		_, _ = sys.orch.InitiatePayment(ctx, saga.InitiatePaymentRequest{
			PaymentID: "pay_mr", WalletID: "wal_t3", UserID: "usr_3", ServiceID: "svc_1", Provider: "mockpay",
			Amount: events.Money{Amount: 4000, Currency: "ARS"},
		})
		sys.drain(t)

		if err := sys.orch.ExpireReconciliationWindow(ctx, "pay_mr"); err != nil {
			t.Fatalf("ExpireReconciliationWindow failed: %v", err)
		}
		sys.drain(t)

		pv, _ := sys.orch.GetPayment(ctx, "pay_mr")
		bal, _ := sys.walletSvc.GetBalance(ctx, "wal_t3")
		if pv.Status != string(payment.StatusManualReview) || bal.Held != 4000 {
			t.Fatalf("expected MANUAL_REVIEW with held=4000, got pv=%+v bal=%+v", pv, bal)
		}

		if err := sys.orch.ResolveManualReview(ctx, "pay_mr", false, "", "T+1_NOT_SETTLED"); err != nil {
			t.Fatalf("ResolveManualReview failed: %v", err)
		}
		sys.drain(t)

		pv, _ = sys.orch.GetPayment(ctx, "pay_mr")
		bal, _ = sys.walletSvc.GetBalance(ctx, "wal_t3")
		if pv.Status != string(payment.StatusFailed) || bal.Held != 0 || bal.Available != 10000 {
			t.Fatalf("expected FAILED after manual decline, got pv=%+v bal=%+v", pv, bal)
		}
	})

	t.Run("circuit breaker open -> immediate safe release without contacting gateway", func(t *testing.T) {
		sys := newTestSystem(t, gateway.MockBehavior{SubmitOutcome: ports.GatewayOutcomeApproved}, nil, nil)

		// Trip breaker open via 5 consecutive timeouts.
		for i := 0; i < 5; i++ {
			sys.breaker.RecordFailure(true)
		}
		if sys.breaker.State() != gateway.StateOpen {
			t.Fatalf("expected circuit OPEN, got %s", sys.breaker.State())
		}

		_, _ = sys.walletSvc.CreateWallet(ctx, "wal_cb", "usr_cb", "ARS")
		_, _ = sys.walletSvc.Deposit(ctx, "wal_cb", "dep_1", events.Money{Amount: 10000, Currency: "ARS"})

		_, _ = sys.orch.InitiatePayment(ctx, saga.InitiatePaymentRequest{
			PaymentID: "pay_cb", WalletID: "wal_cb", UserID: "usr_cb", ServiceID: "svc_1", Provider: "mockpay",
			Amount: events.Money{Amount: 7000, Currency: "ARS"},
		})
		sys.drain(t)

		if sys.mockGW.SubmitCalls("pay_cb") != 0 {
			t.Fatalf("expected 0 outbound calls with open circuit, got %d", sys.mockGW.SubmitCalls("pay_cb"))
		}
		pv, _ := sys.orch.GetPayment(ctx, "pay_cb")
		bal, _ := sys.walletSvc.GetBalance(ctx, "wal_cb")
		if pv.Status != string(payment.StatusFailed) || pv.FailureReason != events.ReasonCircuitOpen || bal.Held != 0 || bal.Available != 10000 {
			t.Fatalf("expected safe immediate release on CIRCUIT_OPEN, got pv=%+v bal=%+v", pv, bal)
		}
	})
}

// Scenario 4: Concurrent payments — race condition protection via partitioning AND optimistic locking.
func TestScenario4_ConcurrentPaymentsNoDoubleSpend(t *testing.T) {
	ctx := context.Background()

	t.Run("balance 10000 vs two simultaneous payments of 8000", func(t *testing.T) {
		sys := newTestSystem(t, gateway.MockBehavior{SubmitOutcome: ports.GatewayOutcomeApproved}, nil, nil)
		_, _ = sys.walletSvc.CreateWallet(ctx, "wal_race", "usr_race", "ARS")
		_, _ = sys.walletSvc.Deposit(ctx, "wal_race", "dep_1", events.Money{Amount: 10000, Currency: "ARS"})

		var (
			wg      sync.WaitGroup
			startCh = make(chan struct{})
			results = make([]saga.InitiatePaymentResult, 2)
		)

		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				<-startCh
				res, err := sys.orch.InitiatePayment(ctx, saga.InitiatePaymentRequest{
					PaymentID: fmt.Sprintf("pay_race_%d", idx),
					WalletID:  "wal_race",
					UserID:    "usr_race",
					ServiceID: "svc_1",
					Provider:  "mockpay",
					Amount:    events.Money{Amount: 8000, Currency: "ARS"},
				})
				if err != nil {
					t.Errorf("InitiatePayment[%d] error: %v", idx, err)
					return
				}
				results[idx] = res
			}(i)
		}
		close(startCh)
		wg.Wait()
		sys.drain(t)

		completed, rejected := 0, 0
		for i := 0; i < 2; i++ {
			pv, err := sys.orch.GetPayment(ctx, fmt.Sprintf("pay_race_%d", i))
			if err != nil {
				t.Fatalf("GetPayment failed: %v", err)
			}
			switch pv.Status {
			case string(payment.StatusCompleted):
				completed++
			case string(payment.StatusRejected):
				rejected++
			default:
				t.Fatalf("unexpected final status: %s", pv.Status)
			}
		}

		if completed != 1 || rejected != 1 {
			t.Fatalf("expected exactly 1 COMPLETED and 1 REJECTED, got completed=%d rejected=%d", completed, rejected)
		}

		bal, _ := sys.walletSvc.GetBalance(ctx, "wal_race")
		if bal.Balance != 2000 || bal.Held != 0 || bal.Available != 2000 {
			t.Fatalf("unexpected final wallet balance: %+v", bal)
		}
	})

	t.Run("direct concurrent WalletService contention triggers optimistic locking without negative balance", func(t *testing.T) {
		sys := newTestSystem(t, gateway.MockBehavior{}, nil, nil)
		_, _ = sys.walletSvc.CreateWallet(ctx, "wal_ol", "usr_ol", "ARS")
		_, _ = sys.walletSvc.Deposit(ctx, "wal_ol", "dep_1", events.Money{Amount: 5000, Currency: "ARS"})

		const concurrentCmds = 12
		var (
			wg      sync.WaitGroup
			startCh = make(chan struct{})
			errCnt  atomic.Int32
		)

		for i := 0; i < concurrentCmds; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				<-startCh
				env, _ := events.NewEnvelope(
					events.CommandReserveFunds,
					events.AggregateTypeWallet,
					"wal_ol",
					0,
					events.ReserveFundsCommand{
						PaymentID: fmt.Sprintf("pay_ol_%d", idx),
						WalletID:  "wal_ol",
						Amount:    events.Money{Amount: 1000, Currency: "ARS"},
					},
					events.Metadata{CorrelationID: fmt.Sprintf("pay_ol_%d", idx)},
					nil,
					nil,
				)
				if err := sys.walletSvc.HandleCommand(ctx, env); err != nil {
					errCnt.Add(1)
					t.Errorf("HandleCommand[%d] failed: %v", idx, err)
				}
			}(i)
		}
		close(startCh)
		wg.Wait()
		sys.drain(t)

		if errCnt.Load() != 0 {
			t.Fatalf("expected 0 command errors under optimistic locking retries, got %d", errCnt.Load())
		}

		w, err := sys.walletSvc.Load(ctx, "wal_ol")
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if w.Balance() != 5000 || w.Held() != 5000 || w.Available() != 0 {
			t.Fatalf("double spend detected! balance=%d held=%d available=%d", w.Balance(), w.Held(), w.Available())
		}
	})
}

// Scenario 5: System crash recovery — rebuild read models from Event Store & resume active sagas.
func TestScenario5_SystemRecoveryRebuildAndResumeActiveSagas(t *testing.T) {
	ctx := context.Background()

	// Durable stores that survive a process crash (PostgreSQL in production).
	durableStore := eventstore.NewInMemoryEventStore()
	durableSnaps := eventstore.NewInMemorySnapshotStore()

	// --- Boot #1: create state and simulate an abrupt crash mid-saga ---
	sys1 := newTestSystem(t, gateway.MockBehavior{SubmitOutcome: ports.GatewayOutcomeApproved}, durableStore, durableSnaps)

	_, _ = sys1.walletSvc.CreateWallet(ctx, "wal_rec", "usr_rec", "ARS")
	_, _ = sys1.walletSvc.Deposit(ctx, "wal_rec", "dep_1", events.Money{Amount: 50000, Currency: "ARS"})

	// 1. A completed payment before the crash.
	_, _ = sys1.orch.InitiatePayment(ctx, saga.InitiatePaymentRequest{
		PaymentID: "pay_done", WalletID: "wal_rec", UserID: "usr_rec", ServiceID: "svc_1", Provider: "mockpay",
		Amount: events.Money{Amount: 10000, Currency: "ARS"},
	})
	sys1.drain(t)

	// 2. Persist a payment stuck in FUNDS_RESERVED (crash happened before SubmitPayment reached gateway).
	w, err := sys1.walletSvc.Load(ctx, "wal_rec")
	if err != nil {
		t.Fatalf("load wallet failed: %v", err)
	}
	_ = w.Reserve("pay_stuck_reserved", events.Money{Amount: 15000, Currency: "ARS"}, events.Metadata{CorrelationID: "pay_stuck_reserved"})
	_ = w.Reserve("pay_stuck_unknown", events.Money{Amount: 10000, Currency: "ARS"}, events.Metadata{CorrelationID: "pay_stuck_unknown"})
	if err := durableStore.Append(ctx, w.StreamID(), w.BaseVersion(), w.UncommittedEvents()...); err != nil {
		t.Fatalf("seed wallet reservations failed: %v", err)
	}

	pReserved, _ := payment.Initiate(payment.InitiateParams{
		PaymentID: "pay_stuck_reserved", WalletID: "wal_rec", UserID: "usr_rec", ServiceID: "svc_2", Provider: "mockpay",
		Amount: events.Money{Amount: 15000, Currency: "ARS"},
	}, events.Metadata{})
	_ = pReserved.MarkFundsReserved(25000, events.Metadata{})
	if err := durableStore.Append(ctx, pReserved.StreamID(), 0, pReserved.UncommittedEvents()...); err != nil {
		t.Fatalf("seed pay_stuck_reserved failed: %v", err)
	}

	pUnknown, _ := payment.Initiate(payment.InitiateParams{
		PaymentID: "pay_stuck_unknown", WalletID: "wal_rec", UserID: "usr_rec", ServiceID: "svc_3", Provider: "mockpay",
		Amount: events.Money{Amount: 10000, Currency: "ARS"},
	}, events.Metadata{})
	_ = pUnknown.MarkFundsReserved(15000, events.Metadata{})
	_ = pUnknown.MarkGatewayUnknown(events.ReasonGatewayTimeout, events.Metadata{})
	if err := durableStore.Append(ctx, pUnknown.StreamID(), 0, pUnknown.UncommittedEvents()...); err != nil {
		t.Fatalf("seed pay_stuck_unknown failed: %v", err)
	}

	// Simulate abrupt crash of Boot #1: shut down bus and discard all in-memory read models/state.
	_ = sys1.bus.Close(ctx)

	// --- Boot #2: fresh process reading ONLY from durableStore + durableSnaps ---
	sys2 := newTestSystem(t, gateway.MockBehavior{
		SubmitOutcome: ports.GatewayOutcomeApproved, // pay_stuck_reserved succeeds on retry
		StatusOutcome: ports.GatewayOutcomeDeclined, // pay_stuck_unknown was declined at provider -> must release!
	}, durableStore, durableSnaps)

	// Verify health checks pass on fresh instance.
	ready, statuses := sys2.health.Readiness(ctx)
	if !ready || len(statuses) != 3 {
		t.Fatalf("expected healthy readiness on boot #2, got ready=%v statuses=%+v", ready, statuses)
	}

	// Step 1: Rebuild wiped read models from Event Store.
	replayed, err := sys2.projector.RebuildAll(ctx, durableStore)
	if err != nil {
		t.Fatalf("RebuildAll failed: %v", err)
	}
	if replayed == 0 {
		t.Fatal("expected replayed events > 0")
	}

	rebuiltBal, err := sys2.balances.Get(ctx, "wal_rec")
	if err != nil {
		t.Fatalf("Get rebuilt balance failed: %v", err)
	}
	if rebuiltBal.Balance != 40000 || rebuiltBal.Held != 25000 || rebuiltBal.Available != 15000 {
		t.Fatalf("unexpected rebuilt balance before saga recovery: %+v", rebuiltBal)
	}

	// Step 2: Recover active sagas from Event Store.
	recSummary, err := sys2.orch.RecoverActiveSagas(ctx)
	if err != nil {
		t.Fatalf("RecoverActiveSagas failed: %v", err)
	}
	if recSummary.Scanned != 3 || recSummary.Resumed != 2 {
		t.Fatalf("expected 3 scanned and 2 resumed sagas, got %+v", recSummary)
	}
	sys2.drain(t)

	// Verify final states after recovery:
	// - pay_done: COMPLETED (10000 captured earlier)
	// - pay_stuck_reserved: COMPLETED (15000 captured after resume)
	// - pay_stuck_unknown: FAILED (10000 released after reconciliation confirmed decline)
	// Final wallet balance = 50000 - 10000 - 15000 = 25000, held = 0, available = 25000.
	pvRes, _ := sys2.orch.GetPayment(ctx, "pay_stuck_reserved")
	pvUnk, _ := sys2.orch.GetPayment(ctx, "pay_stuck_unknown")
	finalBal, _ := sys2.walletSvc.GetBalance(ctx, "wal_rec")

	if pvRes.Status != string(payment.StatusCompleted) {
		t.Fatalf("expected pay_stuck_reserved COMPLETED, got %s", pvRes.Status)
	}
	if pvUnk.Status != string(payment.StatusFailed) {
		t.Fatalf("expected pay_stuck_unknown FAILED, got %s", pvUnk.Status)
	}
	if finalBal.Balance != 25000 || finalBal.Held != 0 || finalBal.Available != 25000 {
		t.Fatalf("unexpected final balance after recovery: %+v", finalBal)
	}

	// Ensure Wallet snapshot was persisted during operations.
	snap, ok, err := durableSnaps.LoadSnapshot(ctx, events.StreamID(events.AggregateTypeWallet, "wal_rec"))
	if err != nil || !ok || snap.Version == 0 {
		t.Fatalf("expected wallet snapshot to be persisted, ok=%v snap=%+v err=%v", ok, snap, err)
	}
}
