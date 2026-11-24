package eventbus_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
	"github.com/besednjak/draftea-payments-challenge/internal/infrastructure/eventbus"
	"github.com/besednjak/draftea-payments-challenge/internal/observability"
)

func TestPartitionedBusPreservesFIFOOrderPerKey(t *testing.T) {
	bus := eventbus.NewPartitionedBus(eventbus.Config{
		Partitions:     8,
		MaxAttempts:    2,
		RetryBaseDelay: time.Millisecond,
	})
	defer func() { _ = bus.Close(context.Background()) }()

	var (
		mu       sync.Mutex
		received = make(map[string][]int64)
	)

	err := bus.Subscribe(events.TopicWalletsCommands, "wallet-svc", func(_ context.Context, env events.Envelope) error {
		mu.Lock()
		received[env.AggregateID] = append(received[env.AggregateID], env.AggregateVersion)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	const numKeys = 10
	const msgsPerKey = 50

	var wg sync.WaitGroup
	for k := 0; k < numKeys; k++ {
		key := fmt.Sprintf("wal_%d", k)
		wg.Add(1)
		go func(walletID string) {
			defer wg.Done()
			for seq := int64(1); seq <= msgsPerKey; seq++ {
				env, _ := events.NewEnvelope(
					events.CommandReserveFunds,
					events.AggregateTypeWallet,
					walletID,
					seq,
					map[string]int64{"seq": seq},
					events.Metadata{},
					nil,
					nil,
				)
				if err := bus.Publish(context.Background(), events.TopicWalletsCommands, walletID, env); err != nil {
					t.Errorf("Publish failed: %v", err)
				}
			}
		}(key)
	}
	wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := bus.Drain(ctx); err != nil {
		t.Fatalf("Drain failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	for k := 0; k < numKeys; k++ {
		key := fmt.Sprintf("wal_%d", k)
		got := received[key]
		if len(got) != msgsPerKey {
			t.Fatalf("key %s: got %d messages, want %d", key, len(got), msgsPerKey)
		}
		for i, v := range got {
			if v != int64(i+1) {
				t.Fatalf("key %s out of order at idx %d: got %d want %d", key, i, v, i+1)
			}
		}
	}
}

func TestPartitionedBusRetriesAndDLQ(t *testing.T) {
	metrics := observability.NewCollector()
	bus := eventbus.NewPartitionedBus(eventbus.Config{
		Partitions:     4,
		MaxAttempts:    3,
		RetryBaseDelay: time.Millisecond,
		RetryMaxDelay:  5 * time.Millisecond,
		Metrics:        metrics,
	})
	defer func() { _ = bus.Close(context.Background()) }()

	var (
		transientAttempts atomic.Int32
		alwaysFailCount   atomic.Int32
		poisonCount       atomic.Int32
	)

	// Consumer 1: recovers on 2nd attempt (no DLQ).
	_ = bus.Subscribe(events.TopicGatewayCommands, "recovering-group", func(_ context.Context, env events.Envelope) error {
		if env.AggregateID != "pay_recover" {
			return nil
		}
		if transientAttempts.Add(1) < 2 {
			return errors.New("temporary db hiccup")
		}
		return nil
	})

	// Consumer 2: always fails -> DLQ after 3 attempts; poison pill -> DLQ on attempt 1.
	_ = bus.Subscribe(events.TopicGatewayCommands, "failing-group", func(_ context.Context, env events.Envelope) error {
		switch env.AggregateID {
		case "pay_fail":
			alwaysFailCount.Add(1)
			return errors.New("downstream unreachable")
		case "pay_poison":
			poisonCount.Add(1)
			return eventbus.NonRetryable(errors.New("corrupt json payload"))
		default:
			return nil
		}
	})

	for _, id := range []string{"pay_recover", "pay_fail", "pay_poison"} {
		env, _ := events.NewEnvelope(events.CommandSubmitPayment, events.AggregateTypePayment, id, 1, map[string]string{"id": id}, events.Metadata{CorrelationID: id}, nil, nil)
		if err := bus.Publish(context.Background(), events.TopicGatewayCommands, id, env); err != nil {
			t.Fatalf("Publish %s failed: %v", id, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := bus.Drain(ctx); err != nil {
		t.Fatalf("Drain failed: %v", err)
	}

	if transientAttempts.Load() != 2 {
		t.Fatalf("expected 2 attempts for recovering consumer, got %d", transientAttempts.Load())
	}
	if alwaysFailCount.Load() != 3 {
		t.Fatalf("expected 3 attempts for failing consumer, got %d", alwaysFailCount.Load())
	}
	if poisonCount.Load() != 1 {
		t.Fatalf("expected 1 attempt for poison pill, got %d", poisonCount.Load())
	}

	dlqs := bus.DeadLetters(events.TopicGatewayCommands)
	if len(dlqs) != 2 {
		t.Fatalf("expected 2 DLQ entries, got %d", len(dlqs))
	}

	byID := make(map[string]int)
	for _, d := range dlqs {
		byID[d.Envelope.AggregateID] = d.Attempts
		if d.OriginalTopic != events.TopicGatewayCommands || d.ConsumerGroup != "failing-group" || d.FirstFailedAt.IsZero() {
			t.Fatalf("incomplete DLQ metadata: %+v", d)
		}
	}
	if byID["pay_fail"] != 3 || byID["pay_poison"] != 1 {
		t.Fatalf("unexpected DLQ attempts map: %+v", byID)
	}

	report := metrics.Snapshot()
	if report.DLQMessagesByTopic[events.TopicGatewayCommands] != 2 {
		t.Fatalf("expected 2 DLQ metrics recorded, got %d", report.DLQMessagesByTopic[events.TopicGatewayCommands])
	}
}

func TestPartitionedBusRecursivePublishSamePartitionNoDeadlock(t *testing.T) {
	bus := eventbus.NewPartitionedBus(eventbus.Config{Partitions: 1}) // single partition forces same-partition recursion
	defer func() { _ = bus.Close(context.Background()) }()

	var finalCount atomic.Int32
	_ = bus.Subscribe(events.TopicWalletsCommands, "step1", func(ctx context.Context, env events.Envelope) error {
		next, _ := events.NewEnvelope(events.EventFundsReserved, events.AggregateTypeWallet, env.AggregateID, 2, map[string]string{"ok": "1"}, events.Metadata{}, nil, nil)
		return bus.Publish(ctx, events.TopicWalletsEvents, env.AggregateID, next)
	})
	_ = bus.Subscribe(events.TopicWalletsEvents, "step2", func(_ context.Context, _ events.Envelope) error {
		finalCount.Add(1)
		return nil
	})

	env, _ := events.NewEnvelope(events.CommandReserveFunds, events.AggregateTypeWallet, "wal_1", 1, map[string]string{"ok": "1"}, events.Metadata{}, nil, nil)
	if err := bus.Publish(context.Background(), events.TopicWalletsCommands, "wal_1", env); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := bus.Drain(ctx); err != nil {
		t.Fatalf("Drain timed out (possible deadlock): %v", err)
	}
	if finalCount.Load() != 1 {
		t.Fatalf("expected 1 cascaded delivery, got %d", finalCount.Load())
	}
}
