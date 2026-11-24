package wallet_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	appwallet "github.com/besednjak/draftea-payments-challenge/internal/application/wallet"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
	"github.com/besednjak/draftea-payments-challenge/internal/infrastructure/eventstore"
	"github.com/besednjak/draftea-payments-challenge/internal/infrastructure/readmodel"
	"github.com/besednjak/draftea-payments-challenge/internal/observability"
)

func TestWalletServiceReadYourWritesSnapshotsAndOptimisticRetry(t *testing.T) {
	ctx := context.Background()
	store := eventstore.NewInMemoryEventStore()
	snaps := eventstore.NewInMemorySnapshotStore()
	balances := readmodel.NewWalletBalanceStore()
	inbox := eventstore.NewInMemoryInboxStore()
	metrics := observability.NewCollector()

	svc := appwallet.NewService(appwallet.Config{
		EventStore:       store,
		SnapshotStore:    snaps,
		SnapshotInterval: 3,
		BalanceReadModel: balances,
		Inbox:            inbox,
		Metrics:          metrics,
		MaxLockRetries:   10,
		LockRetryMin:     time.Millisecond,
		LockRetryMax:     5 * time.Millisecond,
	})

	if _, err := svc.CreateWallet(ctx, "wal_svc", "usr_1", "ARS"); err != nil {
		t.Fatalf("CreateWallet failed: %v", err)
	}
	if _, err := svc.Deposit(ctx, "wal_svc", "dep_1", events.Money{Amount: 5000, Currency: "ARS"}); err != nil {
		t.Fatalf("Deposit failed: %v", err)
	}

	// Read-your-writes: balance view is immediately up to date without waiting for async consumers.
	bal, err := svc.GetBalance(ctx, "wal_svc")
	if err != nil || bal.Balance != 5000 || bal.Available != 5000 || bal.Version != 2 {
		t.Fatalf("unexpected read-your-writes balance: %+v err=%v", bal, err)
	}

	// Concurrent ReserveFunds commands hitting the same wallet trigger optimistic locking retries
	// while preserving Available >= 0 and creating periodic snapshots.
	const workers = 10
	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
	)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			cmd, _ := events.NewEnvelope(
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
			if err := svc.HandleCommand(ctx, cmd); err != nil {
				t.Errorf("HandleCommand[%d] failed: %v", idx, err)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	finalBal, err := svc.GetBalance(ctx, "wal_svc")
	if err != nil {
		t.Fatalf("GetBalance failed: %v", err)
	}
	if finalBal.Balance != 5000 || finalBal.Held != 5000 || finalBal.Available != 0 {
		t.Fatalf("invariant violation after concurrent reserves: %+v", finalBal)
	}

	// Snapshot should have been saved at multiples of SnapshotInterval=3.
	snap, ok, err := snaps.LoadSnapshot(ctx, events.StreamID(events.AggregateTypeWallet, "wal_svc"))
	if err != nil || !ok || snap.Version < 3 {
		t.Fatalf("expected snapshot >= v3, ok=%v snap=%+v err=%v", ok, snap, err)
	}

	// Reloading through Load uses snapshot + delta and matches read model.
	reloaded, err := svc.Load(ctx, "wal_svc")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if reloaded.Balance() != finalBal.Balance || reloaded.Held() != finalBal.Held || reloaded.Version() != finalBal.Version {
		t.Fatalf("reloaded mismatch: agg=(b=%d h=%d v=%d) view=%+v",
			reloaded.Balance(), reloaded.Held(), reloaded.Version(), finalBal)
	}
}
