package eventbus_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/besednjak/draftea-payments-challenge/internal/application/ports"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
	"github.com/besednjak/draftea-payments-challenge/internal/infrastructure/eventbus"
	"github.com/besednjak/draftea-payments-challenge/internal/observability"
)

func mustMakeTestEnvelope(t *testing.T, eventType, aggregateType, aggregateID string, version int64) events.Envelope {
	t.Helper()
	env, err := events.NewEnvelope(
		eventType,
		aggregateType,
		aggregateID,
		version,
		map[string]int64{"version": version},
		events.Metadata{CorrelationID: aggregateID},
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("NewEnvelope failed: %v", err)
	}
	return env
}

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
	if err := bus.Subscribe(events.TopicWalletsCommands, "wallet-svc", func(_ context.Context, env events.Envelope) error {
		mu.Lock()
		received[env.AggregateID] = append(received[env.AggregateID], env.AggregateVersion)
		mu.Unlock()
		return nil
	}); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	const numWallets = 10
	const messagesPerWallet = 50

	var wg sync.WaitGroup
	for walletIndex := 0; walletIndex < numWallets; walletIndex++ {
		walletID := fmt.Sprintf("wal_%d", walletIndex)
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for seq := int64(1); seq <= messagesPerWallet; seq++ {
				env := mustMakeTestEnvelope(t, events.CommandReserveFunds, events.AggregateTypeWallet, id, seq)
				if err := bus.Publish(context.Background(), events.TopicWalletsCommands, id, env); err != nil {
					t.Errorf("Publish failed: %v", err)
				}
			}
		}(walletID)
	}
	wg.Wait()

	drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := bus.Drain(drainCtx); err != nil {
		t.Fatalf("Drain failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	for walletIndex := 0; walletIndex < numWallets; walletIndex++ {
		walletID := fmt.Sprintf("wal_%d", walletIndex)
		versions := received[walletID]
		if len(versions) != messagesPerWallet {
			t.Fatalf("key %s: got %d messages, want %d", walletID, len(versions), messagesPerWallet)
		}
		for idx, version := range versions {
			if version != int64(idx+1) {
				t.Fatalf("key %s out of order at idx %d: got %d want %d", walletID, idx, version, idx+1)
			}
		}
	}
}

func TestPartitionedBusRetriesAndDLQ(t *testing.T) {
	t.Run("recovers after transient failure without routing to DLQ", func(t *testing.T) {
		bus := eventbus.NewPartitionedBus(eventbus.Config{
			Partitions:     2,
			MaxAttempts:    3,
			RetryBaseDelay: time.Millisecond,
			RetryMaxDelay:  5 * time.Millisecond,
		})
		defer func() { _ = bus.Close(context.Background()) }()

		var attempts atomic.Int32
		if err := bus.Subscribe(events.TopicGatewayCommands, "recovering-group", func(_ context.Context, _ events.Envelope) error {
			if attempts.Add(1) < 2 {
				return errors.New("temporary db hiccup")
			}
			return nil
		}); err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}

		env := mustMakeTestEnvelope(t, events.CommandSubmitPayment, events.AggregateTypePayment, "pay_recover", 1)
		if err := bus.Publish(context.Background(), events.TopicGatewayCommands, "pay_recover", env); err != nil {
			t.Fatalf("Publish failed: %v", err)
		}

		drainCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := bus.Drain(drainCtx); err != nil {
			t.Fatalf("Drain failed: %v", err)
		}

		if attempts.Load() != 2 {
			t.Fatalf("expected 2 attempts, got %d", attempts.Load())
		}
		if dlqs := bus.DeadLetters(events.TopicGatewayCommands); len(dlqs) != 0 {
			t.Fatalf("expected 0 DLQ entries, got %d", len(dlqs))
		}
	})

	t.Run("routes message to DLQ after exhausting retries", func(t *testing.T) {
		metrics := observability.NewCollector()
		bus := eventbus.NewPartitionedBus(eventbus.Config{
			Partitions:     2,
			MaxAttempts:    3,
			RetryBaseDelay: time.Millisecond,
			RetryMaxDelay:  5 * time.Millisecond,
			Metrics:        metrics,
		})
		defer func() { _ = bus.Close(context.Background()) }()

		var attempts atomic.Int32
		if err := bus.Subscribe(events.TopicGatewayCommands, "failing-group", func(_ context.Context, _ events.Envelope) error {
			attempts.Add(1)
			return errors.New("downstream unreachable")
		}); err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}

		env := mustMakeTestEnvelope(t, events.CommandSubmitPayment, events.AggregateTypePayment, "pay_fail", 1)
		if err := bus.Publish(context.Background(), events.TopicGatewayCommands, "pay_fail", env); err != nil {
			t.Fatalf("Publish failed: %v", err)
		}

		drainCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := bus.Drain(drainCtx); err != nil {
			t.Fatalf("Drain failed: %v", err)
		}

		if attempts.Load() != 3 {
			t.Fatalf("expected 3 attempts, got %d", attempts.Load())
		}
		dlqs := bus.DeadLetters(events.TopicGatewayCommands)
		if len(dlqs) != 1 {
			t.Fatalf("expected 1 DLQ entry, got %d", len(dlqs))
		}
		if dlqs[0].Attempts != 3 || dlqs[0].ConsumerGroup != "failing-group" || dlqs[0].FirstFailedAt.IsZero() {
			t.Fatalf("unexpected DLQ metadata: %+v", dlqs[0])
		}
		if report := metrics.Snapshot(); report.DLQMessagesByTopic[events.TopicGatewayCommands] != 1 {
			t.Fatalf("expected 1 DLQ metric, got %d", report.DLQMessagesByTopic[events.TopicGatewayCommands])
		}
	})

	t.Run("routes non-retryable poison pill directly to DLQ on attempt 1", func(t *testing.T) {
		bus := eventbus.NewPartitionedBus(eventbus.Config{
			Partitions:     2,
			MaxAttempts:    3,
			RetryBaseDelay: time.Millisecond,
		})
		defer func() { _ = bus.Close(context.Background()) }()

		var attempts atomic.Int32
		if err := bus.Subscribe(events.TopicGatewayCommands, "poison-group", func(_ context.Context, _ events.Envelope) error {
			attempts.Add(1)
			return ports.NonRetryable(errors.New("corrupt json payload"))
		}); err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}

		env := mustMakeTestEnvelope(t, events.CommandSubmitPayment, events.AggregateTypePayment, "pay_poison", 1)
		if err := bus.Publish(context.Background(), events.TopicGatewayCommands, "pay_poison", env); err != nil {
			t.Fatalf("Publish failed: %v", err)
		}

		drainCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := bus.Drain(drainCtx); err != nil {
			t.Fatalf("Drain failed: %v", err)
		}

		if attempts.Load() != 1 {
			t.Fatalf("expected exactly 1 attempt for poison pill, got %d", attempts.Load())
		}
		dlqs := bus.DeadLetters(events.TopicGatewayCommands)
		if len(dlqs) != 1 || dlqs[0].Attempts != 1 {
			t.Fatalf("expected 1 DLQ entry with Attempts=1, got %+v", dlqs)
		}
	})
}

func TestPartitionedBusCloseCancelsPendingRetryImmediately(t *testing.T) {
	bus := eventbus.NewPartitionedBus(eventbus.Config{
		Partitions:     1,
		MaxAttempts:    5,
		RetryBaseDelay: 5 * time.Second,
		RetryMaxDelay:  10 * time.Second,
	})

	firstAttemptStarted := make(chan struct{})
	var attempts atomic.Int32
	if err := bus.Subscribe(events.TopicGatewayCommands, "slow-retry-group", func(_ context.Context, _ events.Envelope) error {
		if attempts.Add(1) == 1 {
			close(firstAttemptStarted)
		}
		return errors.New("always fails")
	}); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	env := mustMakeTestEnvelope(t, events.CommandSubmitPayment, events.AggregateTypePayment, "pay_cancel", 1)
	if err := bus.Publish(context.Background(), events.TopicGatewayCommands, "pay_cancel", env); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	<-firstAttemptStarted

	closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	_ = bus.Close(closeCtx)
	elapsed := time.Since(start)

	if elapsed > time.Second {
		t.Fatalf("Close did not abort pending retry backoff promptly: elapsed=%v", elapsed)
	}
	// Give the cancelled worker a brief moment to finish unwinding before asserting.
	time.Sleep(10 * time.Millisecond)
	if attempts.Load() != 1 {
		t.Fatalf("expected retry loop to exit after cancellation on attempt 1, got %d attempts", attempts.Load())
	}
	if dlqs := bus.DeadLetters(events.TopicGatewayCommands); len(dlqs) != 0 {
		t.Fatalf("expected shutdown-interrupted message not to enter DLQ, got %+v", dlqs)
	}
}

func TestPartitionedBusCloseCancelsInFlightHandlerAndRespectsDeadline(t *testing.T) {
	t.Run("cancels handler context on Close deadline", func(t *testing.T) {
		bus := eventbus.NewPartitionedBus(eventbus.Config{Partitions: 1})
		handlerStarted := make(chan struct{})
		handlerCancelled := make(chan struct{})

		if err := bus.Subscribe(events.TopicWalletsCommands, "ctx-aware", func(ctx context.Context, _ events.Envelope) error {
			close(handlerStarted)
			<-ctx.Done()
			close(handlerCancelled)
			return ctx.Err()
		}); err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}

		env := mustMakeTestEnvelope(t, events.CommandReserveFunds, events.AggregateTypeWallet, "wal_ctx", 1)
		if err := bus.Publish(context.Background(), events.TopicWalletsCommands, "wal_ctx", env); err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
		<-handlerStarted

		closeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		err := bus.Close(closeCtx)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context.DeadlineExceeded, got %v", err)
		}

		select {
		case <-handlerCancelled:
		case <-time.After(time.Second):
			t.Fatal("expected in-flight handler context to be cancelled on Close")
		}
	})

	t.Run("does not block indefinitely if handler ignores cancellation", func(t *testing.T) {
		bus := eventbus.NewPartitionedBus(eventbus.Config{Partitions: 1})
		handlerStarted := make(chan struct{})
		releaseHandler := make(chan struct{})
		defer close(releaseHandler)

		if err := bus.Subscribe(events.TopicWalletsCommands, "stuck-handler", func(_ context.Context, _ events.Envelope) error {
			close(handlerStarted)
			<-releaseHandler
			return nil
		}); err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}

		env := mustMakeTestEnvelope(t, events.CommandReserveFunds, events.AggregateTypeWallet, "wal_stuck", 1)
		if err := bus.Publish(context.Background(), events.TopicWalletsCommands, "wal_stuck", env); err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
		<-handlerStarted

		closeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		start := time.Now()
		err := bus.Close(closeCtx)
		elapsed := time.Since(start)

		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context.DeadlineExceeded, got %v", err)
		}
		if elapsed > 500*time.Millisecond {
			t.Fatalf("Close blocked beyond deadline: elapsed=%v", elapsed)
		}
	})
}

func TestPartitionedBusDrainRespectsContextTimeout(t *testing.T) {
	bus := eventbus.NewPartitionedBus(eventbus.Config{Partitions: 1})
	defer func() { _ = bus.Close(context.Background()) }()

	blockHandler := make(chan struct{})
	if err := bus.Subscribe(events.TopicWalletsCommands, "blocking-group", func(_ context.Context, _ events.Envelope) error {
		<-blockHandler
		return nil
	}); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	env := mustMakeTestEnvelope(t, events.CommandReserveFunds, events.AggregateTypeWallet, "wal_block", 1)
	if err := bus.Publish(context.Background(), events.TopicWalletsCommands, "wal_block", env); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	shortCtx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	if err := bus.Drain(shortCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded from Drain, got %v", err)
	}

	close(blockHandler)
	okCtx, okCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer okCancel()
	if err := bus.Drain(okCtx); err != nil {
		t.Fatalf("expected Drain to succeed after handler unblocks, got %v", err)
	}
}

func TestPartitionedBusRecursivePublishSamePartitionNoDeadlock(t *testing.T) {
	// A single partition forces recursive publications onto the exact same worker queue.
	bus := eventbus.NewPartitionedBus(eventbus.Config{Partitions: 1})
	defer func() { _ = bus.Close(context.Background()) }()

	var finalCount atomic.Int32
	if err := bus.Subscribe(events.TopicWalletsCommands, "step1", func(ctx context.Context, env events.Envelope) error {
		next := mustMakeTestEnvelope(t, events.EventFundsReserved, events.AggregateTypeWallet, env.AggregateID, 2)
		return bus.Publish(ctx, events.TopicWalletsEvents, env.AggregateID, next)
	}); err != nil {
		t.Fatalf("Subscribe step1 failed: %v", err)
	}
	if err := bus.Subscribe(events.TopicWalletsEvents, "step2", func(_ context.Context, _ events.Envelope) error {
		finalCount.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("Subscribe step2 failed: %v", err)
	}

	env := mustMakeTestEnvelope(t, events.CommandReserveFunds, events.AggregateTypeWallet, "wal_1", 1)
	if err := bus.Publish(context.Background(), events.TopicWalletsCommands, "wal_1", env); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	drainCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := bus.Drain(drainCtx); err != nil {
		t.Fatalf("Drain timed out (possible deadlock): %v", err)
	}
	if finalCount.Load() != 1 {
		t.Fatalf("expected 1 cascaded delivery, got %d", finalCount.Load())
	}
}
