package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
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

type platform struct {
	store         *eventstore.InMemoryEventStore
	snapshots     *eventstore.InMemorySnapshotStore
	inbox         *eventstore.InMemoryInboxStore
	idemp         *eventstore.InMemoryIdempotencyStore
	bus           *eventbus.PartitionedBus
	balances      *readmodel.WalletBalanceStore
	txs           *readmodel.WalletTransactionStore
	payViews      *readmodel.PaymentViewStore
	projector     *readmodel.Projector
	mockGW        *gateway.MockGateway
	breaker       *gateway.CircuitBreaker
	gwAdapter     *gateway.Adapter
	walletSvc     *appwallet.Service
	orchestrator  *saga.PaymentOrchestrator
	metrics       *observability.Collector
	health        *observability.HealthRegistry
	webhookSecret string
}

func newPlatform(
	store *eventstore.InMemoryEventStore,
	snapshots *eventstore.InMemorySnapshotStore,
	logger *slog.Logger,
) *platform {
	if store == nil {
		store = eventstore.NewInMemoryEventStore()
	}
	if snapshots == nil {
		snapshots = eventstore.NewInMemorySnapshotStore()
	}

	metrics := observability.NewCollector()
	inbox := eventstore.NewInMemoryInboxStore()
	idemp := eventstore.NewInMemoryIdempotencyStore(nil)
	bus := eventbus.NewPartitionedBus(eventbus.Config{
		Partitions:     8,
		MaxAttempts:    3,
		RetryBaseDelay: 5 * time.Millisecond,
		RetryMaxDelay:  50 * time.Millisecond,
		Logger:         logger,
		Metrics:        metrics,
	})

	balances := readmodel.NewWalletBalanceStore()
	txs := readmodel.NewWalletTransactionStore()
	payViews := readmodel.NewPaymentViewStore()
	projector := readmodel.NewProjector(balances, txs, payViews)

	mockGW := gateway.NewMockGateway(gateway.MockBehavior{
		SubmitOutcome: ports.GatewayOutcomeApproved,
	})
	breaker := gateway.NewCircuitBreaker(gateway.CircuitBreakerConfig{
		Provider:            "mockpay",
		ConsecutiveTimeouts: 5,
		OpenTimeout:         30 * time.Second,
		HalfOpenMaxProbes:   3,
		OnStateChange: func(provider string, from, to gateway.CircuitState) {
			metrics.RecordCircuitBreakerState(provider, string(to))
			logger.Warn("circuit breaker state changed", "provider", provider, "from", from, "to", to)
		},
	})
	metrics.RecordCircuitBreakerState("mockpay", string(breaker.State()))

	webhookSecret := "demo-hmac-secret"
	gwAdapter := gateway.NewAdapter(gateway.AdapterConfig{
		Gateway:       mockGW,
		Publisher:     bus,
		Inbox:         inbox,
		Breaker:       breaker,
		WebhookSecret: webhookSecret,
		Logger:        logger,
		Metrics:       metrics,
		Retry: gateway.RetryConfig{
			MaxAttempts:    3,
			BaseDelay:      10 * time.Millisecond,
			MaxDelay:       40 * time.Millisecond,
			AttemptTimeout: 500 * time.Millisecond,
		},
	})

	walletSvc := appwallet.NewService(appwallet.Config{
		EventStore:       store,
		SnapshotStore:    snapshots,
		SnapshotInterval: 5,
		BalanceReadModel: balances,
		Publisher:        bus,
		Inbox:            inbox,
		Metrics:          metrics,
		Logger:           logger,
	})

	orchestrator := saga.NewPaymentOrchestrator(saga.Config{
		EventStore:      store,
		PaymentViews:    payViews,
		Publisher:       bus,
		Inbox:           inbox,
		Idempotency:     idemp,
		Metrics:         metrics,
		Logger:          logger,
		SyncReserveWait: 2 * time.Second,
	})

	// Event-driven wiring (4 modular monolith services communicating only through topics).
	mustSub(bus.Subscribe(events.TopicWalletsCommands, "wallet-service", walletSvc.HandleCommand))
	mustSub(bus.Subscribe(events.TopicGatewayCommands, "gateway-adapter", gwAdapter.HandleCommand))
	mustSub(bus.Subscribe(events.TopicWalletsEvents, "payment-saga", orchestrator.HandleWalletEvent))
	mustSub(bus.Subscribe(events.TopicGatewayEvents, "payment-saga", orchestrator.HandleGatewayEvent))
	mustSub(bus.Subscribe(events.TopicWalletsEvents, "readmodel-projector", projector.HandleEvent))
	mustSub(bus.Subscribe(events.TopicPaymentsEvents, "readmodel-projector", projector.HandleEvent))
	mustSub(bus.Subscribe(events.TopicPaymentsEvents, "metrics-service", metrics.HandleEvent))
	mustSub(bus.Subscribe(events.TopicWalletsEvents, "metrics-service", metrics.HandleEvent))
	mustSub(bus.Subscribe(events.TopicGatewayEvents, "metrics-service", metrics.HandleEvent))

	health := observability.NewHealthRegistry(nil)
	health.Register(store)
	health.Register(bus)
	health.Register(mockGW)

	return &platform{
		store:         store,
		snapshots:     snapshots,
		inbox:         inbox,
		idemp:         idemp,
		bus:           bus,
		balances:      balances,
		txs:           txs,
		payViews:      payViews,
		projector:     projector,
		mockGW:        mockGW,
		breaker:       breaker,
		gwAdapter:     gwAdapter,
		walletSvc:     walletSvc,
		orchestrator:  orchestrator,
		metrics:       metrics,
		health:        health,
		webhookSecret: webhookSecret,
	}
}

func (p *platform) drain(ctx context.Context) {
	dCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := p.bus.Drain(dCtx); err != nil {
		panic(fmt.Sprintf("bus drain failed: %v", err))
	}
}

func mustSub(err error) {
	if err != nil {
		panic(err)
	}
}

func fmtCents(amount int64, currency string) string {
	sign := ""
	if amount < 0 {
		sign = "-"
		amount = -amount
	}
	return fmt.Sprintf("%s$%d.%02d %s", sign, amount/100, amount%100, currency)
}

func main() {
	ctx := context.Background()

	// Set DEMO_LOGS=1 to emit structured JSON slog output alongside the summary.
	var logOut io.Writer = io.Discard
	if os.Getenv("DEMO_LOGS") == "1" {
		logOut = os.Stderr
	}
	logger := observability.NewLogger(logOut, slog.LevelInfo)

	p := newPlatform(nil, nil, logger)
	defer func() { _ = p.bus.Close(ctx) }()

	fmt.Println("======================================================================")
	fmt.Println("  Draftea Payments Challenge — Event-Driven Modular Monolith Demo")
	fmt.Println("======================================================================")

	// ------------------------------------------------------------------
	// Scenario 1: Happy path (async gateway + HMAC webhook) + partial refund
	// ------------------------------------------------------------------
	fmt.Println("\n[1] Ruta Feliz (asincrónica con webhook HMAC) + Reembolso Parcial")
	_, _ = p.walletSvc.CreateWallet(ctx, "wal_alice", "usr_alice", "ARS")
	_, _ = p.walletSvc.Deposit(ctx, "wal_alice", "dep_alice_1", events.Money{Amount: 50000, Currency: "ARS"})
	p.mockGW.SetRule("pay_happy_1", gateway.MockBehavior{SubmitOutcome: ports.GatewayOutcomeAccepted})

	res1, _ := p.orchestrator.InitiatePayment(ctx, saga.InitiatePaymentRequest{
		IdempotencyKey: "idem-alice-1",
		PaymentID:      "pay_happy_1",
		WalletID:       "wal_alice",
		UserID:         "usr_alice",
		ServiceID:      "svc_electricity",
		Provider:       "mockpay",
		Amount:         events.Money{Amount: 15000, Currency: "ARS"},
	})
	p.drain(ctx)
	fmt.Printf("    POST /v1/payments -> HTTP %d (status=%s, available=%s)\n",
		res1.HTTPStatus, res1.Status, fmtCents(res1.AvailableBalance, "ARS"))

	whBody, _ := json.Marshal(gateway.WebhookPayload{
		ProviderEventID: "wh_alice_001",
		PaymentID:       "pay_happy_1",
		Provider:        "mockpay",
		ProviderRef:     "prov_alice_001",
		Status:          string(ports.GatewayOutcomeApproved),
	})
	_ = p.gwAdapter.HandleWebhook(ctx, gateway.SignWebhook(p.webhookSecret, whBody), whBody)
	p.drain(ctx)

	pv1, _ := p.orchestrator.GetPayment(ctx, "pay_happy_1")
	bal1, _ := p.walletSvc.GetBalance(ctx, "wal_alice")
	fmt.Printf("    Webhook HMAC APPROVED -> payment=%s | balance=%s held=%s\n",
		pv1.Status, fmtCents(bal1.Balance, "ARS"), fmtCents(bal1.Held, "ARS"))

	_ = p.orchestrator.RequestRefund(ctx, "pay_happy_1", "ref_alice_1", events.Money{Amount: 5000, Currency: "ARS"})
	p.drain(ctx)
	pv1, _ = p.orchestrator.GetPayment(ctx, "pay_happy_1")
	bal1, _ = p.walletSvc.GetBalance(ctx, "wal_alice")
	fmt.Printf("    Reembolso parcial $50.00 -> payment=%s refunded=%s | balance=%s\n",
		pv1.Status, fmtCents(pv1.RefundedTotal, "ARS"), fmtCents(bal1.Balance, "ARS"))

	// ------------------------------------------------------------------
	// Scenario 2: Insufficient funds immediate rejection (422)
	// ------------------------------------------------------------------
	fmt.Println("\n[2] Saldo Insuficiente (rechazo inmediato con espera acotada de 2s)")
	_, _ = p.walletSvc.CreateWallet(ctx, "wal_bob", "usr_bob", "ARS")
	_, _ = p.walletSvc.Deposit(ctx, "wal_bob", "dep_bob_1", events.Money{Amount: 5000, Currency: "ARS"})

	res2, _ := p.orchestrator.InitiatePayment(ctx, saga.InitiatePaymentRequest{
		PaymentID: "pay_bob_1",
		WalletID:  "wal_bob",
		UserID:    "usr_bob",
		ServiceID: "svc_fiber",
		Provider:  "mockpay",
		Amount:    events.Money{Amount: 12000, Currency: "ARS"},
	})
	p.drain(ctx)
	fmt.Printf("    POST /v1/payments ($120.00 vs saldo $50.00) -> HTTP %d (status=%s, reason=%s, gateway_calls=%d)\n",
		res2.HTTPStatus, res2.Status, res2.Reason, p.mockGW.SubmitCalls("pay_bob_1"))

	// ------------------------------------------------------------------
	// Scenario 3: Gateway timeout -> GATEWAY_UNKNOWN -> reconciliation
	// ------------------------------------------------------------------
	fmt.Println("\n[3] Timeout de Pasarela Externa (reintentos + backoff/jitter + conciliación)")
	_, _ = p.walletSvc.CreateWallet(ctx, "wal_carol", "usr_carol", "ARS")
	_, _ = p.walletSvc.Deposit(ctx, "wal_carol", "dep_carol_1", events.Money{Amount: 20000, Currency: "ARS"})
	p.mockGW.SetRule("pay_timeout_1", gateway.MockBehavior{
		AlwaysTimeout: true,
		StatusOutcome: ports.GatewayOutcomeApproved,
	})

	_, _ = p.orchestrator.InitiatePayment(ctx, saga.InitiatePaymentRequest{
		PaymentID: "pay_timeout_1",
		WalletID:  "wal_carol",
		UserID:    "usr_carol",
		ServiceID: "svc_water",
		Provider:  "mockpay",
		Amount:    events.Money{Amount: 8000, Currency: "ARS"},
	})
	p.drain(ctx)
	pv3, _ := p.orchestrator.GetPayment(ctx, "pay_timeout_1")
	bal3, _ := p.walletSvc.GetBalance(ctx, "wal_carol")
	fmt.Printf("    Tras %d intentos con timeout -> status=%s | fondos retenidos=%s (regla de oro: no liberar sin evidencia)\n",
		p.mockGW.SubmitCalls("pay_timeout_1"), pv3.Status, fmtCents(bal3.Held, "ARS"))

	_ = p.orchestrator.TriggerReconciliation(ctx, "pay_timeout_1")
	p.drain(ctx)
	pv3, _ = p.orchestrator.GetPayment(ctx, "pay_timeout_1")
	bal3, _ = p.walletSvc.GetBalance(ctx, "wal_carol")
	fmt.Printf("    Conciliación GET /payments/{id} -> APPROVED -> status=%s | balance=%s held=%s\n",
		pv3.Status, fmtCents(bal3.Balance, "ARS"), fmtCents(bal3.Held, "ARS"))

	// ------------------------------------------------------------------
	// Scenario 4: Concurrent payments ($100 balance vs two $80 payments)
	// ------------------------------------------------------------------
	fmt.Println("\n[4] Pagos Concurrentes ($100.00 disponibles vs 2 pagos simultáneos de $80.00)")
	_, _ = p.walletSvc.CreateWallet(ctx, "wal_dave", "usr_dave", "ARS")
	_, _ = p.walletSvc.Deposit(ctx, "wal_dave", "dep_dave_1", events.Money{Amount: 10000, Currency: "ARS"})

	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, pid := range []string{"pay_concurrent_A", "pay_concurrent_B"} {
		wg.Add(1)
		go func(paymentID string) {
			defer wg.Done()
			<-start
			_, _ = p.orchestrator.InitiatePayment(ctx, saga.InitiatePaymentRequest{
				PaymentID: paymentID,
				WalletID:  "wal_dave",
				UserID:    "usr_dave",
				ServiceID: "svc_mobile",
				Provider:  "mockpay",
				Amount:    events.Money{Amount: 8000, Currency: "ARS"},
			})
		}(pid)
	}
	close(start)
	wg.Wait()
	p.drain(ctx)

	pvA, _ := p.orchestrator.GetPayment(ctx, "pay_concurrent_A")
	pvB, _ := p.orchestrator.GetPayment(ctx, "pay_concurrent_B")
	balDave, _ := p.walletSvc.GetBalance(ctx, "wal_dave")
	fmt.Printf("    Pago A=%s | Pago B=%s | Saldo final=%s (sin doble gasto)\n",
		pvA.Status, pvB.Status, fmtCents(balDave.Balance, "ARS"))

	// ------------------------------------------------------------------
	// Scenario 5: System crash & recovery (rebuild read models + resume sagas)
	// ------------------------------------------------------------------
	fmt.Println("\n[5] Recuperación del Sistema tras Reinicio (Event Store Replay + Saga Recovery)")
	// Persist an in-flight payment in FUNDS_RESERVED simulating a crash right before gateway dispatch.
	wCarol, _ := p.walletSvc.Load(ctx, "wal_carol")
	_ = wCarol.Reserve("pay_recovery_1", events.Money{Amount: 5000, Currency: "ARS"}, events.Metadata{CorrelationID: "pay_recovery_1"})
	_ = p.store.Append(ctx, wCarol.StreamID(), wCarol.BaseVersion(), wCarol.UncommittedEvents()...)

	pStuck, _ := payment.Initiate(payment.InitiateParams{
		PaymentID: "pay_recovery_1", WalletID: "wal_carol", UserID: "usr_carol", ServiceID: "svc_cloud", Provider: "mockpay",
		Amount: events.Money{Amount: 5000, Currency: "ARS"},
	}, events.Metadata{})
	_ = pStuck.MarkFundsReserved(7000, events.Metadata{})
	_ = p.store.Append(ctx, pStuck.StreamID(), 0, pStuck.UncommittedEvents()...)

	// Snapshot metrics before shutting down instance #1, then reboot instance #2 using ONLY durable stores.
	reportBoot1 := p.metrics.Snapshot()
	_ = p.bus.Close(ctx)

	p2 := newPlatform(p.store, p.snapshots, logger)
	defer func() { _ = p2.bus.Close(ctx) }()

	replayed, _ := p2.projector.RebuildAll(ctx, p.store)
	rec, _ := p2.orchestrator.RecoverActiveSagas(ctx)
	p2.drain(ctx)

	pvRec, _ := p2.orchestrator.GetPayment(ctx, "pay_recovery_1")
	balCarolAfter, _ := p2.walletSvc.GetBalance(ctx, "wal_carol")
	fmt.Printf("    Read Models reconstruidos (%d eventos reproducidos) | Sagas escaneadas=%d retomadas=%d\n",
		replayed, rec.Scanned, rec.Resumed)
	fmt.Printf("    pay_recovery_1 tras recovery -> status=%s | saldo Carol=%s held=%s\n",
		pvRec.Status, fmtCents(balCarolAfter.Balance, "ARS"), fmtCents(balCarolAfter.Held, "ARS"))

	// ------------------------------------------------------------------
	// Observability: Health Checks & Metrics
	// ------------------------------------------------------------------
	fmt.Println("\n[Observabilidad] Health Checks (/healthz, /readyz) y Métricas")
	live := p2.health.Liveness()
	ready, checks := p2.health.Readiness(ctx)
	fmt.Printf("    /healthz: healthy=%v | /readyz: ready=%v (", live.Healthy, ready)
	for i, c := range checks {
		if i > 0 {
			fmt.Print(", ")
		}
		fmt.Printf("%s=%v", c.Name, c.Healthy)
	}
	fmt.Println(")")

	fmt.Printf("    Métricas (instancia 1): initiated=%d completed=%d rejected=%d failed=%d refunded=%d success_rate=%.0f%% volume=%s\n",
		reportBoot1.PaymentsInitiated,
		reportBoot1.PaymentsCompleted,
		reportBoot1.PaymentsRejected,
		reportBoot1.PaymentsFailed,
		reportBoot1.PaymentsRefunded,
		reportBoot1.SuccessRate*100,
		fmtCents(reportBoot1.VolumeByCurrency["ARS"], "ARS"),
	)
	fmt.Printf("    Operativas: insufficient_funds=%d circuit_breaker=%s dlq_msgs=%d\n",
		reportBoot1.InsufficientFundsCount,
		reportBoot1.CircuitBreakerStates["mockpay"],
		len(p2.bus.DeadLetters(events.TopicWalletsCommands))+len(p2.bus.DeadLetters(events.TopicGatewayCommands)),
	)
	fmt.Println("======================================================================")
}
