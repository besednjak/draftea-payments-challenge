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

func makeDepositEnvelope(t *testing.T, walletID string, version int64) events.Envelope {
	t.Helper()
	env, err := events.NewEnvelope(
		events.EventFundsDeposited,
		events.AggregateTypeWallet,
		walletID,
		version,
		events.FundsDepositedPayload{
			WalletID: walletID,
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
			err := store.Append(ctx, streamID, 0, makeDepositEnvelope(t, "wal_race", 1))
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
	const writers = 25

	if err := store.Append(ctx, streamID, 0, makeDepositEnvelope(t, "wal_retry", 1)); err != nil {
		t.Fatalf("seed append failed: %v", err)
	}

	var wg sync.WaitGroup
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
				err = store.Append(ctx, streamID, expected, makeDepositEnvelope(t, "wal_retry", expected+1))
				if err == nil {
					return
				}
				if errors.Is(err, ports.ErrVersionConflict) {
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
	for index, env := range history {
		if env.AggregateVersion != int64(index+1) {
			t.Fatalf("version gap at index %d: got %d", index, env.AggregateVersion)
		}
	}

	allEvents, err := store.LoadAll(ctx, 0)
	if err != nil {
		t.Fatalf("LoadAll failed: %v", err)
	}
	if len(allEvents) != writers+1 {
		t.Fatalf("expected %d global events, got %d", writers+1, len(allEvents))
	}
}

func TestEventStoreLoadAndPrefixQueries(t *testing.T) {
	ctx := context.Background()
	store := eventstore.NewInMemoryEventStore()

	if err := store.Append(ctx, "wallet-a", 0, makeDepositEnvelope(t, "a", 1), makeDepositEnvelope(t, "a", 2)); err != nil {
		t.Fatalf("append wallet-a failed: %v", err)
	}
	if err := store.Append(ctx, "payment-1", 0, makeDepositEnvelope(t, "1", 1)); err != nil {
		t.Fatalf("append payment-1 failed: %v", err)
	}

	t.Run("returns ErrStreamNotFound for missing stream", func(t *testing.T) {
		_, err := store.LoadStream(ctx, "wallet-missing", 0)

		if !errors.Is(err, ports.ErrStreamNotFound) {
			t.Fatalf("expected ErrStreamNotFound, got %v", err)
		}
	})

	t.Run("loads only events after fromVersion", func(t *testing.T) {
		delta, err := store.LoadStream(ctx, "wallet-a", 1)

		if err != nil {
			t.Fatalf("LoadStream failed: %v", err)
		}
		if len(delta) != 1 || delta[0].AggregateVersion != 2 {
			t.Fatalf("unexpected delta load: len=%d", len(delta))
		}
	})

	t.Run("filters stream IDs by prefix", func(t *testing.T) {
		matching, err := store.ListStreamsByPrefix(ctx, "wallet-")

		if err != nil {
			t.Fatalf("ListStreamsByPrefix failed: %v", err)
		}
		if len(matching) != 1 || matching[0] != "wallet-a" {
			t.Fatalf("unexpected matching streams: %v", matching)
		}
	})
}

func TestSnapshotStoreSaveAndLoad(t *testing.T) {
	ctx := context.Background()
	snapshotStore := eventstore.NewInMemorySnapshotStore()

	if _, found, err := snapshotStore.LoadSnapshot(ctx, "wallet-a"); err != nil || found {
		t.Fatalf("expected missing snapshot, found=%v err=%v", found, err)
	}

	err := snapshotStore.SaveSnapshot(ctx, ports.Snapshot{
		StreamID: "wallet-a",
		Version:  2,
		State:    json.RawMessage(`{"balance":200}`),
	})
	if err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	loaded, found, err := snapshotStore.LoadSnapshot(ctx, "wallet-a")
	if err != nil || !found || loaded.Version != 2 {
		t.Fatalf("unexpected snapshot: %+v found=%v err=%v", loaded, found, err)
	}
}

func TestInboxStoreDeduplicationAndRollback(t *testing.T) {
	ctx := context.Background()
	inbox := eventstore.NewInMemoryInboxStore()

	alreadyProcessed, err := inbox.TryMarkProcessed(ctx, "wallet-svc", "evt-1")
	if err != nil || alreadyProcessed {
		t.Fatalf("first mark should succeed: alreadyProcessed=%v err=%v", alreadyProcessed, err)
	}

	alreadyProcessed, err = inbox.TryMarkProcessed(ctx, "wallet-svc", "evt-1")
	if err != nil || !alreadyProcessed {
		t.Fatalf("second mark should be deduplicated: alreadyProcessed=%v err=%v", alreadyProcessed, err)
	}

	// Unmarking reverts a failed attempt so a retry can process the event again.
	if err := inbox.UnmarkProcessed(ctx, "wallet-svc", "evt-1"); err != nil {
		t.Fatalf("UnmarkProcessed failed: %v", err)
	}
	alreadyProcessed, err = inbox.TryMarkProcessed(ctx, "wallet-svc", "evt-1")
	if err != nil || alreadyProcessed {
		t.Fatalf("mark after UnmarkProcessed should not be duplicate: alreadyProcessed=%v err=%v", alreadyProcessed, err)
	}

	// Independent consumers maintain isolated inbox records.
	alreadyProcessed, err = inbox.TryMarkProcessed(ctx, "metrics-svc", "evt-1")
	if err != nil || alreadyProcessed {
		t.Fatalf("different consumer should not be deduplicated: alreadyProcessed=%v err=%v", alreadyProcessed, err)
	}
}

func TestIdempotencyStoreLifecycle(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	idempotencyStore := eventstore.NewInMemoryIdempotencyStore(func() time.Time { return now })

	record, reserved, err := idempotencyStore.Reserve(ctx, "usr_1", "key_1", "hash_A", time.Hour)
	if err != nil || !reserved || record != nil {
		t.Fatalf("expected initial reserve: record=%v reserved=%v err=%v", record, reserved, err)
	}

	t.Run("rejects concurrent request while in progress", func(t *testing.T) {
		_, _, err := idempotencyStore.Reserve(ctx, "usr_1", "key_1", "hash_A", time.Hour)

		if !errors.Is(err, ports.ErrIdempotencyInProgress) {
			t.Fatalf("expected ErrIdempotencyInProgress, got %v", err)
		}
	})

	t.Run("rejects key reuse with different request hash", func(t *testing.T) {
		_, _, err := idempotencyStore.Reserve(ctx, "usr_1", "key_1", "hash_B", time.Hour)

		if !errors.Is(err, ports.ErrIdempotencyConflict) {
			t.Fatalf("expected ErrIdempotencyConflict, got %v", err)
		}
	})

	t.Run("returns cached response after completion", func(t *testing.T) {
		if err := idempotencyStore.Complete(ctx, "usr_1", "key_1", "pay_1", json.RawMessage(`{"status":"ACCEPTED"}`)); err != nil {
			t.Fatalf("Complete failed: %v", err)
		}

		cached, wasReserved, err := idempotencyStore.Reserve(ctx, "usr_1", "key_1", "hash_A", time.Hour)

		if err != nil || wasReserved || cached == nil || cached.PaymentID != "pay_1" {
			t.Fatalf("expected cached completed record: cached=%+v reserved=%v err=%v", cached, wasReserved, err)
		}
	})

	t.Run("allows reuse after TTL expiration", func(t *testing.T) {
		now = now.Add(2 * time.Hour)

		_, wasReserved, err := idempotencyStore.Reserve(ctx, "usr_1", "key_1", "hash_NEW", time.Hour)

		if err != nil || !wasReserved {
			t.Fatalf("expected expired key to be re-reservable: reserved=%v err=%v", wasReserved, err)
		}
	})
}
