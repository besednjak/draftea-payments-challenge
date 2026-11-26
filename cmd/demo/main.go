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

type demoPlatform struct {
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

func newDemoPlatform(
	store *eventstore.InMemoryEventStore,
	snapshotStore *eventstore.InMemorySnapshotStore,
	logger *slog.Logger,
) *demoPlatform {
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
		RetryBaseDelay: 5 * time.Millisecond,
		RetryMaxDelay:  50 * time.Millisecond,
		Logger:         logger,
		Metrics:        metrics,
	})

	balanceStore := readmodel.NewWalletBalanceStore()
	transactionStore := readmodel.NewWalletTransactionStore()
	paymentStore := readmodel.NewPaymentViewStore()
	projector := readmodel.NewProjector(balanceStore, transactionStore, paymentStore)

	mockGateway := gateway.NewMockGateway(gateway.MockBehavior{
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
	gatewayAdapter := gateway.NewAdapter(gateway.AdapterConfig{
		Gateway:       mockGateway,
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

	walletService := appwallet.NewService(appwallet.Config{
		EventStore:       store,
		SnapshotStore:    snapshotStore,
		SnapshotInterval: 5,
		BalanceReadModel: balanceStore,
		Publisher:        bus,
		Inbox:            inbox,
		Metrics:          metrics,
		Logger:           logger,
	})

	orchestrator := saga.NewPaymentOrchestrator(saga.Config{
		EventStore:      store,
		PaymentViews:    paymentStore,
		Publisher:       bus,
		Inbox:           inbox,
		Idempotency:     idempotencyStore,
		Metrics:         metrics,
		Logger:          logger,
		SyncReserveWait: 2 * time.Second,
	})

	must(bus.Subscribe(events.TopicWalletsCommands, "wallet-service", walletService.HandleCommand))
	must(bus.Subscribe(events.TopicGatewayCommands, "gateway-adapter", gatewayAdapter.HandleCommand))
	must(bus.Subscribe(events.TopicWalletsEvents, "payment-saga", orchestrator.HandleWalletEvent))
	must(bus.Subscribe(events.TopicGatewayEvents, "payment-saga", orchestrator.HandleGatewayEvent))
	must(bus.Subscribe(events.TopicWalletsEvents, "readmodel-projector", projector.HandleEvent))
	must(bus.Subscribe(events.TopicPaymentsEvents, "readmodel-projector", projector.HandleEvent))
	must(bus.Subscribe(events.TopicPaymentsEvents, "metrics-service", metrics.HandleEvent))
	must(bus.Subscribe(events.TopicWalletsEvents, "metrics-service", metrics.HandleEvent))
	must(bus.Subscribe(events.TopicGatewayEvents, "metrics-service", metrics.HandleEvent))

	health := observability.NewHealthRegistry(nil)
	health.Register(store)
	health.Register(bus)
	health.Register(mockGateway)

	return &demoPlatform{
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

func (p *demoPlatform) drain(ctx context.Context) {
	drainCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	must(p.bus.Drain(drainCtx))
}

func must(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "demo error: %v\n", err)
		os.Exit(1)
	}
}

func mustValue[T any](value T, err error) T {
	must(err)
	return value
}

func formatCents(amount int64, currency string) string {
	sign := ""
	if amount < 0 {
		sign = "-"
		amount = -amount
	}
	return fmt.Sprintf("%s$%d.%02d %s", sign, amount/100, amount%100, currency)
}

func runScenario1HappyPath(ctx context.Context, demo *demoPlatform) {
	fmt.Println("\n[1] Ruta Feliz (asincrónica con webhook HMAC) + Reembolso Parcial")

	mustValue(demo.walletService.CreateWallet(ctx, "wal_alice", "usr_alice", "ARS"))
	mustValue(demo.walletService.Deposit(ctx, "wal_alice", "dep_alice_1", events.Money{Amount: 50000, Currency: "ARS"}))
	demo.mockGateway.SetRule("pay_happy_1", gateway.MockBehavior{SubmitOutcome: ports.GatewayOutcomeAccepted})

	initialResult := mustValue(demo.orchestrator.InitiatePayment(ctx, saga.InitiatePaymentRequest{
		IdempotencyKey: "idem-alice-1",
		PaymentID:      "pay_happy_1",
		WalletID:       "wal_alice",
		UserID:         "usr_alice",
		ServiceID:      "svc_electricity",
		Provider:       "mockpay",
		Amount:         events.Money{Amount: 15000, Currency: "ARS"},
	}))
	demo.drain(ctx)
	fmt.Printf("    POST /v1/payments -> HTTP %d (status=%s, available=%s)\n",
		initialResult.HTTPStatus, initialResult.Status, formatCents(initialResult.AvailableBalance, "ARS"))

	webhookBody := mustValue(json.Marshal(gateway.WebhookPayload{
		ProviderEventID: "wh_alice_001",
		PaymentID:       "pay_happy_1",
		Provider:        "mockpay",
		ProviderRef:     "prov_alice_001",
		Status:          string(ports.GatewayOutcomeApproved),
	}))
	must(demo.gatewayAdapter.HandleWebhook(ctx, gateway.SignWebhook(demo.webhookSecret, webhookBody), webhookBody))
	demo.drain(ctx)

	paymentView := mustValue(demo.orchestrator.GetPayment(ctx, "pay_happy_1"))
	aliceBalance := mustValue(demo.walletService.GetBalance(ctx, "wal_alice"))
	fmt.Printf("    Webhook HMAC APPROVED -> payment=%s | balance=%s held=%s\n",
		paymentView.Status, formatCents(aliceBalance.Balance, "ARS"), formatCents(aliceBalance.Held, "ARS"))

	must(demo.orchestrator.RequestRefund(ctx, "pay_happy_1", "ref_alice_1", events.Money{Amount: 5000, Currency: "ARS"}))
	demo.drain(ctx)

	paymentView = mustValue(demo.orchestrator.GetPayment(ctx, "pay_happy_1"))
	aliceBalance = mustValue(demo.walletService.GetBalance(ctx, "wal_alice"))
	fmt.Printf("    Reembolso parcial $50.00 -> payment=%s refunded=%s | balance=%s\n",
		paymentView.Status, formatCents(paymentView.RefundedTotal, "ARS"), formatCents(aliceBalance.Balance, "ARS"))
}

func runScenario2InsufficientFunds(ctx context.Context, demo *demoPlatform) {
	fmt.Println("\n[2] Saldo Insuficiente (rechazo inmediato con espera acotada de 2s)")

	mustValue(demo.walletService.CreateWallet(ctx, "wal_bob", "usr_bob", "ARS"))
	mustValue(demo.walletService.Deposit(ctx, "wal_bob", "dep_bob_1", events.Money{Amount: 5000, Currency: "ARS"}))

	result := mustValue(demo.orchestrator.InitiatePayment(ctx, saga.InitiatePaymentRequest{
		PaymentID: "pay_bob_1",
		WalletID:  "wal_bob",
		UserID:    "usr_bob",
		ServiceID: "svc_fiber",
		Provider:  "mockpay",
		Amount:    events.Money{Amount: 12000, Currency: "ARS"},
	}))
	demo.drain(ctx)
	fmt.Printf("    POST /v1/payments ($120.00 vs saldo $50.00) -> HTTP %d (status=%s, reason=%s, gateway_calls=%d)\n",
		result.HTTPStatus, result.Status, result.Reason, demo.mockGateway.SubmitCalls("pay_bob_1"))
}

func runScenario3GatewayTimeout(ctx context.Context, demo *demoPlatform) {
	fmt.Println("\n[3] Timeout de Pasarela Externa (reintentos + backoff/jitter + conciliación)")

	mustValue(demo.walletService.CreateWallet(ctx, "wal_carol", "usr_carol", "ARS"))
	mustValue(demo.walletService.Deposit(ctx, "wal_carol", "dep_carol_1", events.Money{Amount: 20000, Currency: "ARS"}))
	demo.mockGateway.SetRule("pay_timeout_1", gateway.MockBehavior{
		AlwaysTimeout: true,
		StatusOutcome: ports.GatewayOutcomeApproved,
	})

	mustValue(demo.orchestrator.InitiatePayment(ctx, saga.InitiatePaymentRequest{
		PaymentID: "pay_timeout_1",
		WalletID:  "wal_carol",
		UserID:    "usr_carol",
		ServiceID: "svc_water",
		Provider:  "mockpay",
		Amount:    events.Money{Amount: 8000, Currency: "ARS"},
	}))
	demo.drain(ctx)

	paymentView := mustValue(demo.orchestrator.GetPayment(ctx, "pay_timeout_1"))
	carolBalance := mustValue(demo.walletService.GetBalance(ctx, "wal_carol"))
	fmt.Printf("    Tras %d intentos con timeout -> status=%s | fondos retenidos=%s (regla de oro: no liberar sin evidencia)\n",
		demo.mockGateway.SubmitCalls("pay_timeout_1"), paymentView.Status, formatCents(carolBalance.Held, "ARS"))

	must(demo.orchestrator.TriggerReconciliation(ctx, "pay_timeout_1"))
	demo.drain(ctx)

	paymentView = mustValue(demo.orchestrator.GetPayment(ctx, "pay_timeout_1"))
	carolBalance = mustValue(demo.walletService.GetBalance(ctx, "wal_carol"))
	fmt.Printf("    Conciliación GET /payments/{id} -> APPROVED -> status=%s | balance=%s held=%s\n",
		paymentView.Status, formatCents(carolBalance.Balance, "ARS"), formatCents(carolBalance.Held, "ARS"))
}

func runScenario4ConcurrentPayments(ctx context.Context, demo *demoPlatform) {
	fmt.Println("\n[4] Pagos Concurrentes ($100.00 disponibles vs 2 pagos simultáneos de $80.00)")

	mustValue(demo.walletService.CreateWallet(ctx, "wal_dave", "usr_dave", "ARS"))
	mustValue(demo.walletService.Deposit(ctx, "wal_dave", "dep_dave_1", events.Money{Amount: 10000, Currency: "ARS"}))

	var (
		wg      sync.WaitGroup
		startCh = make(chan struct{})
		errs    = make(chan error, 2)
	)
	for _, paymentID := range []string{"pay_concurrent_A", "pay_concurrent_B"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			<-startCh
			if _, err := demo.orchestrator.InitiatePayment(ctx, saga.InitiatePaymentRequest{
				PaymentID: id,
				WalletID:  "wal_dave",
				UserID:    "usr_dave",
				ServiceID: "svc_mobile",
				Provider:  "mockpay",
				Amount:    events.Money{Amount: 8000, Currency: "ARS"},
			}); err != nil {
				errs <- err
			}
		}(paymentID)
	}
	close(startCh)
	wg.Wait()
	close(errs)
	for err := range errs {
		must(err)
	}
	demo.drain(ctx)

	paymentViewA := mustValue(demo.orchestrator.GetPayment(ctx, "pay_concurrent_A"))
	paymentViewB := mustValue(demo.orchestrator.GetPayment(ctx, "pay_concurrent_B"))
	daveBalance := mustValue(demo.walletService.GetBalance(ctx, "wal_dave"))
	fmt.Printf("    Pago A=%s | Pago B=%s | Saldo final=%s (sin doble gasto)\n",
		paymentViewA.Status, paymentViewB.Status, formatCents(daveBalance.Balance, "ARS"))
}

func runScenario5SystemRecovery(
	ctx context.Context,
	firstInstance *demoPlatform,
	logger *slog.Logger,
) (*demoPlatform, observability.MetricsReport) {
	fmt.Println("\n[5] Recuperación del Sistema tras Reinicio (Event Store Replay + Saga Recovery)")

	carolWallet := mustValue(firstInstance.walletService.Load(ctx, "wal_carol"))
	must(carolWallet.Reserve("pay_recovery_1", "usr_carol", events.Money{Amount: 5000, Currency: "ARS"}, events.Metadata{CorrelationID: "pay_recovery_1"}))
	must(firstInstance.store.Append(ctx, carolWallet.StreamID(), carolWallet.BaseVersion(), carolWallet.UncommittedEvents()...))

	stuckPayment := mustValue(payment.Initiate(payment.InitiateParams{
		PaymentID: "pay_recovery_1", WalletID: "wal_carol", UserID: "usr_carol", ServiceID: "svc_cloud", Provider: "mockpay",
		Amount: events.Money{Amount: 5000, Currency: "ARS"},
	}, events.Metadata{}))
	must(stuckPayment.MarkFundsReserved(7000, events.Metadata{}))
	must(firstInstance.store.Append(ctx, stuckPayment.StreamID(), 0, stuckPayment.UncommittedEvents()...))

	firstInstanceReport := firstInstance.metrics.Snapshot()
	must(firstInstance.bus.Close(ctx))

	recoveredInstance := newDemoPlatform(firstInstance.store, firstInstance.snapshotStore, logger)
	replayedEvents := mustValue(recoveredInstance.projector.RebuildAll(ctx, firstInstance.store))
	recoverySummary := mustValue(recoveredInstance.orchestrator.RecoverActiveSagas(ctx))
	recoveredInstance.drain(ctx)

	recoveredView := mustValue(recoveredInstance.orchestrator.GetPayment(ctx, "pay_recovery_1"))
	carolBalance := mustValue(recoveredInstance.walletService.GetBalance(ctx, "wal_carol"))
	fmt.Printf("    Read Models reconstruidos (%d eventos reproducidos) | Sagas escaneadas=%d retomadas=%d\n",
		replayedEvents, recoverySummary.Scanned, recoverySummary.Resumed)
	fmt.Printf("    pay_recovery_1 tras recovery -> status=%s | saldo Carol=%s held=%s\n",
		recoveredView.Status, formatCents(carolBalance.Balance, "ARS"), formatCents(carolBalance.Held, "ARS"))

	return recoveredInstance, firstInstanceReport
}

func printObservabilitySummary(ctx context.Context, demo *demoPlatform, report observability.MetricsReport) {
	fmt.Println("\n[Observabilidad] Health Checks (/healthz, /readyz) y Métricas")

	liveness := demo.health.Liveness()
	ready, checks := demo.health.Readiness(ctx)
	fmt.Printf("    /healthz: healthy=%v | /readyz: ready=%v (", liveness.Healthy, ready)
	for index, check := range checks {
		if index > 0 {
			fmt.Print(", ")
		}
		fmt.Printf("%s=%v", check.Name, check.Healthy)
	}
	fmt.Println(")")

	dlqCount := len(demo.bus.DeadLetters(events.TopicWalletsCommands)) + len(demo.bus.DeadLetters(events.TopicGatewayCommands))
	fmt.Printf("    Métricas (instancia 1): initiated=%d completed=%d rejected=%d failed=%d refunded=%d success_rate=%.0f%% volume=%s\n",
		report.PaymentsInitiated,
		report.PaymentsCompleted,
		report.PaymentsRejected,
		report.PaymentsFailed,
		report.PaymentsRefunded,
		report.SuccessRate*100,
		formatCents(report.VolumeByCurrency["ARS"], "ARS"),
	)
	fmt.Printf("    Operativas: insufficient_funds=%d circuit_breaker=%s dlq_msgs=%d\n",
		report.InsufficientFundsCount,
		report.CircuitBreakerStates["mockpay"],
		dlqCount,
	)
}

func main() {
	ctx := context.Background()

	var logOutput io.Writer = io.Discard
	if os.Getenv("DEMO_LOGS") == "1" {
		logOutput = os.Stderr
	}
	logger := observability.NewLogger(logOutput, slog.LevelInfo)

	firstInstance := newDemoPlatform(nil, nil, logger)

	fmt.Println("======================================================================")
	fmt.Println("  Draftea Payments Challenge — Event-Driven Modular Monolith Demo")
	fmt.Println("======================================================================")

	runScenario1HappyPath(ctx, firstInstance)
	runScenario2InsufficientFunds(ctx, firstInstance)
	runScenario3GatewayTimeout(ctx, firstInstance)
	runScenario4ConcurrentPayments(ctx, firstInstance)
	recoveredInstance, firstInstanceReport := runScenario5SystemRecovery(ctx, firstInstance, logger)
	defer func() { _ = recoveredInstance.bus.Close(ctx) }()

	printObservabilitySummary(ctx, recoveredInstance, firstInstanceReport)
	fmt.Println("======================================================================")
}
