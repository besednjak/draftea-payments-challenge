package eventstore_test

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
	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
	"github.com/besednjak/draftea-payments-challenge/internal/infrastructure/eventstore"
)

func makeTestEvent(t *testing.T, streamAggID string, version int64) events.Envelope {
	t.Helper()
	env, err := events.NewEnvelope(
		events.EventFundsDeposited,
		events.AggregateTypeWallet,
		streamAggID,
		version,
		events.FundsDepositedPayload{
			WalletID: streamAggID,
			Amount:   events.Money{Amount: 100, Currency: "ARS"},
		},
		events.Metadata{CorrelationID: fmt.Sprintf("corr_%d", version)},
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("NewEnvelope failed: %v", err)
	}
	return env
}

func TestEventStoreConcurrentOptimisticLockingSingleWinner(t *testing.T) {
	store := eventstore.NewInMemoryEventStore()
	ctx := context.Background()
	streamID := "wallet-wal_race"

	const workers = 20
	var (
		wg        sync.WaitGroup
		succeeded atomic.Int32
		conflicts atomic.Int32
		startCh   = make(chan struct{})
	)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startCh
			env := makeTestEvent(t, "wal_race", 1)
			err := store.Append(ctx, streamID, 0, env)
			if err == nil {
				succeeded.Add(1)
				return
			}
			if errors.Is(err, ports.ErrVersionConflict) {
				conflicts.Add(1)
				return
			}
			t.Errorf("unexpected error: %v", err)
		}()
	}

	close(startCh)
	wg.Wait()

	if succeeded.Load() != 1 {
		t.Fatalf("expected exactly 1 winner, got %d", succeeded.Load())
	}
	if conflicts.Load() != workers-1 {
		t.Fatalf("expected %d version conflicts, got %d", workers-1, conflicts.Load())
	}

	stream, err := store.LoadStream(ctx, streamID, 0)
	if err != nil {
		t.Fatalf("LoadStream failed: %v", err)
	}
	if len(stream) != 1 || stream[0].AggregateVersion != 1 {
		t.Fatalf("unexpected stream state: len=%d", len(stream))
	}
}

func TestEventStoreConcurrentOptimisticLockingWithRetry(t *testing.T) {
	store := eventstore.NewInMemoryEventStore()
	ctx := context.Background()
	streamID := "wallet-wal_retry"

	// Seed version 1 so stream exists.
	if err := store.Append(ctx, streamID, 0, makeTestEvent(t, "wal_retry", 1)); err != nil {
		t.Fatalf("seed append failed: %v", err)
	}

	const writers = 25
	var wg sync.WaitGroup
	var totalConflicts atomic.Int32

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				history, err := store.LoadStream(ctx, streamID, 0)
				if err != nil {
					t.Errorf("LoadStream failed: %v", err)
					return
				}
				expected := int64(len(history))
				err = store.Append(ctx, streamID, expected, makeTestEvent(t, "wal_retry", expected+1))
				if err == nil {
					return
				}
				if errors.Is(err, ports.ErrVersionConflict) {
					totalConflicts.Add(1)
					continue
				}
				t.Errorf("unexpected error: %v", err)
				return
			}
		}()
	}
	wg.Wait()

	history, err := store.LoadStream(ctx, streamID, 0)
	if err != nil {
		t.Fatalf("LoadStream failed: %v", err)
	}
	if len(history) != writers+1 {
		t.Fatalf("expected %d events, got %d", writers+1, len(history))
	}
	for idx, env := range history {
		if env.AggregateVersion != int64(idx+1) {
			t.Fatalf("gap at index %d: got version %d", idx, env.AggregateVersion)
		}
	}

	all, err := store.LoadAll(ctx, 0)
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}
	if len(all) != writers+1 {
		t.Fatalf("expected %d global events, got %d", writers+1, len(all))
	}
}

func TestEventStoreQueriesAndAuxStores(t *testing.T) {
	ctx := context.Background()
	store := eventstore.NewInMemoryEventStore()

	if _, err := store.LoadStream(ctx, "wallet-missing", 0); !errors.Is(err, ports.ErrStreamNotFound) {
		t.Fatalf("expected ErrStreamNotFound, got %v", err)
	}

	_ = store.Append(ctx, "wallet-a", 0, makeTestEvent(t, "a", 1), makeTestEvent(t, "a", 2))
	_ = store.Append(ctx, "payment-1", 0, makeTestEvent(t, "1", 1))

	delta, err := store.LoadStream(ctx, "wallet-a", 1)
	if err != nil || len(delta) != 1 || delta[0].AggregateVersion != 2 {
		t.Fatalf("unexpected delta load: len=%d err=%v", len(delta), err)
	}

	prefixes, err := store.ListStreamsByPrefix(ctx, "wallet-")
	if err != nil || len(prefixes) != 1 || prefixes[0] != "wallet-a" {
		t.Fatalf("unexpected ListStreamsByPrefix: %v err=%v", prefixes, err)
	}

	// SnapshotStore
	snaps := eventstore.NewInMemorySnapshotStore()
	if _, ok, err := snaps.LoadSnapshot(ctx, "wallet-a"); err != nil || ok {
		t.Fatalf("expected empty snapshot, ok=%v err=%v", ok, err)
	}
	_ = snaps.SaveSnapshot(ctx, ports.Snapshot{StreamID: "wallet-a", Version: 2, State: json.RawMessage(`{"balance":200}`)})
	snap, ok, err := snaps.LoadSnapshot(ctx, "wallet-a")
	if err != nil || !ok || snap.Version != 2 {
		t.Fatalf("unexpected snapshot: %+v ok=%v err=%v", snap, ok, err)
	}

	// InboxStore
	inbox := eventstore.NewInMemoryInboxStore()
	dup, err := inbox.TryMarkProcessed(ctx, "wallet-svc", "evt-1")
	if err != nil || dup {
		t.Fatalf("first mark should not be dup: dup=%v err=%v", dup, err)
	}
	dup, err = inbox.TryMarkProcessed(ctx, "wallet-svc", "evt-1")
	if err != nil || !dup {
		t.Fatalf("second mark should be dup: dup=%v err=%v", dup, err)
	}
	dup, err = inbox.TryMarkProcessed(ctx, "metrics-svc", "evt-1")
	if err != nil || dup {
		t.Fatalf("different consumer should not be dup: dup=%v err=%v", dup, err)
	}

	// IdempotencyStore
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	idemp := eventstore.NewInMemoryIdempotencyStore(func() time.Time { return now })

	rec, reserved, err := idemp.Reserve(ctx, "usr_1", "key_1", "hash_A", time.Hour)
	if err != nil || !reserved || rec != nil {
		t.Fatalf("expected initial reserve: rec=%v reserved=%v err=%v", rec, reserved, err)
	}
	if _, _, err := idemp.Reserve(ctx, "usr_1", "key_1", "hash_A", time.Hour); !errors.Is(err, ports.ErrIdempotencyInProgress) {
		t.Fatalf("expected ErrIdempotencyInProgress, got %v", err)
	}
	if _, _, err := idemp.Reserve(ctx, "usr_1", "key_1", "hash_B", time.Hour); !errors.Is(err, ports.ErrIdempotencyConflict) {
		t.Fatalf("expected ErrIdempotencyConflict, got %v", err)
	}

	if err := idemp.Complete(ctx, "usr_1", "key_1", "pay_1", json.RawMessage(`{"status":"ACCEPTED"}`)); err != nil {
		t.Fatalf("Complete failed: %v", err)
	}
	rec, reserved, err = idemp.Reserve(ctx, "usr_1", "key_1", "hash_A", time.Hour)
	if err != nil || reserved || rec == nil || rec.PaymentID != "pay_1" {
		t.Fatalf("expected cached completed record: rec=%+v reserved=%v err=%v", rec, reserved, err)
	}

	// Expiry allows reuse.
	now = now.Add(2 * time.Hour)
	_, reserved, err = idemp.Reserve(ctx, "usr_1", "key_1", "hash_NEW", time.Hour)
	if err != nil || !reserved {
		t.Fatalf("expected expired key to be re-reservable: reserved=%v err=%v", reserved, err)
	}
}
