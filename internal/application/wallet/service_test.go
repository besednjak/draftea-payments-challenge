package wallet_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/besednjak/draftea-payments-challenge/internal/application/ports"
	appwallet "github.com/besednjak/draftea-payments-challenge/internal/application/wallet"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
	"github.com/besednjak/draftea-payments-challenge/internal/infrastructure/eventbus"
	"github.com/besednjak/draftea-payments-challenge/internal/infrastructure/eventstore"
	"github.com/besednjak/draftea-payments-challenge/internal/infrastructure/readmodel"
	"github.com/besednjak/draftea-payments-challenge/internal/observability"
)

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

type recordingPublisher struct {
	mu        sync.Mutex
	published []events.Envelope
}

func (r *recordingPublisher) Publish(_ context.Context, _, _ string, env events.Envelope) error {
	r.mu.Lock()
	r.published = append(r.published, env)
	r.mu.Unlock()
	return nil
}

func (r *recordingPublisher) list() []events.Envelope {
	r.mu.Lock()
	defer r.mu.Unlock()
	copied := make([]events.Envelope, len(r.published))
	copy(copied, r.published)
	return copied
}

func TestWalletServiceReadYourWritesSnapshotsAndOptimisticRetry(t *testing.T) {
	ctx := context.Background()
	store := eventstore.NewInMemoryEventStore()
	snapshotStore := eventstore.NewInMemorySnapshotStore()
	balanceStore := readmodel.NewWalletBalanceStore()
	inbox := eventstore.NewInMemoryInboxStore()
	metrics := observability.NewCollector()

	walletService := appwallet.NewService(appwallet.Config{
		EventStore:       store,
		SnapshotStore:    snapshotStore,
		SnapshotInterval: 3,
		BalanceReadModel: balanceStore,
		Inbox:            inbox,
		Metrics:          metrics,
		MaxLockRetries:   10,
		LockRetryMin:     time.Millisecond,
		LockRetryMax:     5 * time.Millisecond,
	})

	if _, err := walletService.CreateWallet(ctx, "wal_svc", "usr_1", "ARS"); err != nil {
		t.Fatalf("CreateWallet failed: %v", err)
	}
	if _, err := walletService.Deposit(ctx, "wal_svc", "dep_1", events.Money{Amount: 5000, Currency: "ARS"}); err != nil {
		t.Fatalf("Deposit failed: %v", err)
	}

	initialBalance, err := walletService.GetBalance(ctx, "wal_svc")
	if err != nil || initialBalance.Balance != 5000 || initialBalance.Available != 5000 || initialBalance.Version != 2 {
		t.Fatalf("unexpected read-your-writes balance: %+v err=%v", initialBalance, err)
	}

	const workers = 10
	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
	)
	for workerIndex := 0; workerIndex < workers; workerIndex++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			cmd, err := events.NewEnvelope(
				events.CommandReserveFunds,
				events.AggregateTypeWallet,
				"wal_svc",
				0,
				events.ReserveFundsCommand{
					PaymentID: fmt.Sprintf("pay_w_%d", idx),
					WalletID:  "wal_svc",
					Amount:    events.Money{Amount: 1000, Currency: "ARS"},
				},
				events.Metadata{CorrelationID: fmt.Sprintf("pay_w_%d", idx)},
				nil,
				nil,
			)
			if err != nil {
				t.Errorf("NewEnvelope failed: %v", err)
				return
			}
			if err := walletService.HandleCommand(ctx, cmd); err != nil {
				t.Errorf("HandleCommand[%d] failed: %v", idx, err)
			}
		}(workerIndex)
	}
	close(start)
	wg.Wait()

	finalBalance, err := walletService.GetBalance(ctx, "wal_svc")
	if err != nil {
		t.Fatalf("GetBalance failed: %v", err)
	}
	if finalBalance.Balance != 5000 || finalBalance.Held != 5000 || finalBalance.Available != 0 {
		t.Fatalf("invariant violation after concurrent reserves: %+v", finalBalance)
	}

	snapshot, found, err := snapshotStore.LoadSnapshot(ctx, events.StreamID(events.AggregateTypeWallet, "wal_svc"))
	if err != nil || !found || snapshot.Version < 3 {
		t.Fatalf("expected snapshot >= v3, found=%v snapshot=%+v err=%v", found, snapshot, err)
	}

	reloaded, err := walletService.Load(ctx, "wal_svc")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if reloaded.Balance() != finalBalance.Balance || reloaded.Held() != finalBalance.Held || reloaded.Version() != finalBalance.Version {
		t.Fatalf("reloaded mismatch: agg=(%d, %d, v%d) view=%+v",
			reloaded.Balance(), reloaded.Held(), reloaded.Version(), finalBalance)
	}
}

func TestWalletServiceInboxRollbackAllowsBusRetry(t *testing.T) {
	ctx := context.Background()
	baseStore := eventstore.NewInMemoryEventStore()
	flakyStore := &failingAppendStore{EventStore: baseStore}
	recorder := &recordingPublisher{}
	inbox := eventstore.NewInMemoryInboxStore()
	bus := eventbus.NewPartitionedBus(eventbus.Config{
		Partitions:     2,
		MaxAttempts:    3,
		RetryBaseDelay: time.Millisecond,
	})
	defer func() { _ = bus.Close(ctx) }()

	walletService := appwallet.NewService(appwallet.Config{
		EventStore: flakyStore,
		Publisher:  recorder,
		Inbox:      inbox,
	})
	if _, err := walletService.CreateWallet(ctx, "wal_retry", "usr_1", "ARS"); err != nil {
		t.Fatalf("CreateWallet failed: %v", err)
	}
	if _, err := walletService.Deposit(ctx, "wal_retry", "dep_1", events.Money{Amount: 5000, Currency: "ARS"}); err != nil {
		t.Fatalf("Deposit failed: %v", err)
	}

	// Inject a transient storage error on the first ReserveFunds append.
	flakyStore.failNextAppends.Store(1)
	publishedBeforeReserve := len(recorder.list())

	if err := bus.Subscribe(events.TopicWalletsCommands, "wallet-service", walletService.HandleCommand); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	reserveCmd, err := events.NewEnvelope(
		events.CommandReserveFunds,
		events.AggregateTypeWallet,
		"wal_retry",
		0,
		events.ReserveFundsCommand{
			PaymentID: "pay_retry",
			WalletID:  "wal_retry",
			Amount:    events.Money{Amount: 2000, Currency: "ARS"},
		},
		events.Metadata{CorrelationID: "pay_retry"},
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("NewEnvelope failed: %v", err)
	}

	if err := bus.Publish(ctx, events.TopicWalletsCommands, "wal_retry", reserveCmd); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	drainCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := bus.Drain(drainCtx); err != nil {
		t.Fatalf("Drain failed: %v", err)
	}

	if dlqs := bus.DeadLetters(events.TopicWalletsCommands); len(dlqs) != 0 {
		t.Fatalf("expected retry to succeed after inbox rollback, got DLQs: %+v", dlqs)
	}
	loaded, err := walletService.Load(ctx, "wal_retry")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if loaded.Held() != 2000 || loaded.Available() != 3000 {
		t.Fatalf("expected held=2000 available=3000, got held=%d available=%d", loaded.Held(), loaded.Available())
	}

	emitted := recorder.list()[publishedBeforeReserve:]
	if len(emitted) != 1 || emitted[0].Type != events.EventFundsReserved {
		t.Fatalf("expected 1 FundsReserved event published on retry, got %+v", emitted)
	}
}

func TestWalletServicePoisonPillRoutesDirectlyToDLQ(t *testing.T) {
	ctx := context.Background()
	bus := eventbus.NewPartitionedBus(eventbus.Config{
		Partitions:     2,
		MaxAttempts:    3,
		RetryBaseDelay: time.Millisecond,
	})
	defer func() { _ = bus.Close(ctx) }()

	walletService := appwallet.NewService(appwallet.Config{
		EventStore: eventstore.NewInMemoryEventStore(),
		Inbox:      eventstore.NewInMemoryInboxStore(),
	})
	if err := bus.Subscribe(events.TopicWalletsCommands, "wallet-service", walletService.HandleCommand); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	corruptCommand := events.Envelope{
		EventID:       "evt-poison-wallet",
		Type:          events.CommandReserveFunds,
		AggregateType: events.AggregateTypeWallet,
		AggregateID:   "wal_bad",
		Payload:       json.RawMessage(`{invalid-json`),
	}
	if err := bus.Publish(ctx, events.TopicWalletsCommands, "wal_bad", corruptCommand); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	drainCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := bus.Drain(drainCtx); err != nil {
		t.Fatalf("Drain failed: %v", err)
	}

	dlqs := bus.DeadLetters(events.TopicWalletsCommands)
	if len(dlqs) != 1 || dlqs[0].Attempts != 1 {
		t.Fatalf("expected poison pill in DLQ after 1 attempt, got %+v", dlqs)
	}
}
