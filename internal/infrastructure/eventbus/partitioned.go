package eventbus

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/besednjak/draftea-payments-challenge/internal/application/ports"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
	"github.com/besednjak/draftea-payments-challenge/internal/observability"
)

var ErrBusClosed = errors.New("event bus is closed")

// NonRetryable marks an error as permanent so the bus routes it directly to the DLQ.
func NonRetryable(err error) error {
	return ports.NonRetryable(err)
}

// IsNonRetryable reports whether err is marked as a permanent/poison-pill error.
func IsNonRetryable(err error) bool {
	return ports.IsNonRetryable(err)
}

// Config tunes the in-memory partitioned EventBus.
type Config struct {
	Partitions     int
	MaxAttempts    int
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
	Logger         *slog.Logger
	Metrics        ports.MetricsRecorder
}

func (c Config) withDefaults() Config {
	if c.Partitions <= 0 {
		c.Partitions = 16
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 3
	}
	if c.RetryBaseDelay <= 0 {
		c.RetryBaseDelay = 5 * time.Millisecond
	}
	if c.RetryMaxDelay <= 0 {
		c.RetryMaxDelay = 100 * time.Millisecond
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	return c
}

type subscription struct {
	group   string
	handler ports.EventHandler
}

type busMessage struct {
	ctx          context.Context
	topic        string
	partitionKey string
	envelope     events.Envelope
}

// partitionQueue is a mutex+cond FIFO queue so recursive publishes from handlers
// onto the same partition never deadlock.
type partitionQueue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	items  []busMessage
	closed bool
}

func newPartitionQueue() *partitionQueue {
	queue := &partitionQueue{}
	queue.cond = sync.NewCond(&queue.mu)
	return queue
}

func (q *partitionQueue) push(msg busMessage) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	q.items = append(q.items, msg)
	q.cond.Signal()
	return true
}

func (q *partitionQueue) pop() (busMessage, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 && !q.closed {
		q.cond.Wait()
	}
	if len(q.items) == 0 {
		return busMessage{}, false
	}
	msg := q.items[0]
	q.items[0] = busMessage{}
	q.items = q.items[1:]
	return msg, true
}

func (q *partitionQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.cond.Broadcast()
	q.mu.Unlock()
}

// PartitionedBus is an in-memory Kafka-like EventBus that hashes partitionKey onto N
// dedicated worker goroutines, preserving strict FIFO order per key and capturing
// failed messages into topic DLQs.
type PartitionedBus struct {
	cfg        Config
	partitions []*partitionQueue
	workersWG  sync.WaitGroup

	subMu sync.RWMutex
	subs  map[string][]subscription

	dlqMu sync.RWMutex
	dlqs  map[string][]ports.DeadLetter

	// idleCh is closed whenever inFlight reaches 0 and replaced when inFlight becomes > 0,
	// allowing Drain(ctx) to wait without spawning goroutines that leak on timeout.
	inFlightMu sync.Mutex
	inFlight   int64
	idleCh     chan struct{}

	closed    atomic.Bool
	closeOnce sync.Once
	runCtx    context.Context
	cancelRun context.CancelFunc
}

// NewPartitionedBus creates and starts the partition workers.
func NewPartitionedBus(cfg Config) *PartitionedBus {
	cfg = cfg.withDefaults()
	runCtx, cancelRun := context.WithCancel(context.Background())

	initialIdle := make(chan struct{})
	close(initialIdle)

	bus := &PartitionedBus{
		cfg:        cfg,
		partitions: make([]*partitionQueue, cfg.Partitions),
		subs:       make(map[string][]subscription),
		dlqs:       make(map[string][]ports.DeadLetter),
		idleCh:     initialIdle,
		runCtx:     runCtx,
		cancelRun:  cancelRun,
	}

	for index := 0; index < cfg.Partitions; index++ {
		queue := newPartitionQueue()
		bus.partitions[index] = queue
		bus.workersWG.Add(1)
		go bus.runPartition(queue)
	}
	return bus
}

// Subscribe registers a consumer-group handler for topic.
func (b *PartitionedBus) Subscribe(topic, consumerGroup string, handler ports.EventHandler) error {
	if topic == "" || consumerGroup == "" || handler == nil {
		return fmt.Errorf("topic, consumerGroup, and handler are required")
	}

	b.subMu.Lock()
	b.subs[topic] = append(b.subs[topic], subscription{
		group:   consumerGroup,
		handler: handler,
	})
	b.subMu.Unlock()
	return nil
}

// Publish enqueues env onto the partition determined by partitionKey.
func (b *PartitionedBus) Publish(ctx context.Context, topic, partitionKey string, env events.Envelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.closed.Load() {
		return ErrBusClosed
	}

	partitionIndex := b.partitionFor(partitionKey)
	msgCtx := context.WithoutCancel(ctx)

	b.addInFlight(1)
	if !b.partitions[partitionIndex].push(busMessage{
		ctx:          msgCtx,
		topic:        topic,
		partitionKey: partitionKey,
		envelope:     env,
	}) {
		b.addInFlight(-1)
		return ErrBusClosed
	}
	return nil
}

// Drain blocks until all enqueued and cascaded messages finish processing or ctx expires.
func (b *PartitionedBus) Drain(ctx context.Context) error {
	for {
		b.inFlightMu.Lock()
		if b.inFlight == 0 {
			b.inFlightMu.Unlock()
			return nil
		}
		waitCh := b.idleCh
		b.inFlightMu.Unlock()

		select {
		case <-waitCh:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Close stops accepting new publications and waits for in-flight messages to drain
// until ctx expires, cancelling any active retry backoffs immediately on timeout.
func (b *PartitionedBus) Close(ctx context.Context) error {
	b.closeOnce.Do(func() {
		b.closed.Store(true)
		for _, queue := range b.partitions {
			queue.close()
		}
	})

	done := make(chan struct{})
	go func() {
		b.workersWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		b.cancelRun()
		return nil
	case <-ctx.Done():
		b.cancelRun()
		<-done
		return ctx.Err()
	}
}

// DeadLetters returns a copy of dead-lettered messages for topic (accepts either
// the source topic name or its ".dlq" name).
func (b *PartitionedBus) DeadLetters(topic string) []ports.DeadLetter {
	dlqName := topic
	if !strings.HasSuffix(dlqName, ".dlq") {
		dlqName = events.DLQTopic(topic)
	}

	b.dlqMu.RLock()
	defer b.dlqMu.RUnlock()

	stored := b.dlqs[dlqName]
	copied := make([]ports.DeadLetter, len(stored))
	copy(copied, stored)
	return copied
}

// Name implements ports.HealthChecker.
func (b *PartitionedBus) Name() string { return "eventbus" }

// Check implements ports.HealthChecker.
func (b *PartitionedBus) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.closed.Load() {
		return ErrBusClosed
	}
	return nil
}

func (b *PartitionedBus) partitionFor(key string) int {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))
	return int(hasher.Sum32()) % len(b.partitions)
}

func (b *PartitionedBus) addInFlight(delta int64) {
	b.inFlightMu.Lock()
	previous := b.inFlight
	b.inFlight += delta
	if previous == 0 && b.inFlight > 0 {
		b.idleCh = make(chan struct{})
	} else if b.inFlight <= 0 {
		b.inFlight = 0
		if previous > 0 {
			close(b.idleCh)
		}
	}
	b.inFlightMu.Unlock()
}

func (b *PartitionedBus) runPartition(queue *partitionQueue) {
	defer b.workersWG.Done()

	for {
		msg, ok := queue.pop()
		if !ok {
			return
		}
		b.dispatch(msg)
		b.addInFlight(-1)
	}
}

func (b *PartitionedBus) dispatch(msg busMessage) {
	b.subMu.RLock()
	subscribers := make([]subscription, len(b.subs[msg.topic]))
	copy(subscribers, b.subs[msg.topic])
	b.subMu.RUnlock()

	for _, sub := range subscribers {
		b.deliverWithRetry(msg, sub)
	}
}

func (b *PartitionedBus) deliverWithRetry(msg busMessage, sub subscription) {
	var (
		lastErr       error
		firstFailedAt time.Time
		attempts      int
	)

	delay := b.cfg.RetryBaseDelay
retryLoop:
	for attempt := 1; attempt <= b.cfg.MaxAttempts; attempt++ {
		attempts = attempt
		err := sub.handler(msg.ctx, msg.envelope)
		if err == nil {
			return
		}

		lastErr = err
		if firstFailedAt.IsZero() {
			firstFailedAt = time.Now().UTC()
		}

		if ports.IsNonRetryable(err) {
			break retryLoop
		}

		if attempt < b.cfg.MaxAttempts {
			timer := time.NewTimer(delay)
			select {
			case <-b.runCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			delay *= 2
			if delay > b.cfg.RetryMaxDelay {
				delay = b.cfg.RetryMaxDelay
			}
		}
	}

	b.sendToDLQ(msg, sub.group, lastErr, attempts, firstFailedAt)
}

func (b *PartitionedBus) sendToDLQ(
	msg busMessage,
	group string,
	err error,
	attempts int,
	firstFailedAt time.Time,
) {
	dlqName := events.DLQTopic(msg.topic)
	entry := ports.DeadLetter{
		OriginalTopic: msg.topic,
		PartitionKey:  msg.partitionKey,
		ConsumerGroup: group,
		Error:         err.Error(),
		Attempts:      attempts,
		FirstFailedAt: firstFailedAt,
		Envelope:      msg.envelope,
	}

	b.dlqMu.Lock()
	b.dlqs[dlqName] = append(b.dlqs[dlqName], entry)
	b.dlqMu.Unlock()

	if b.cfg.Metrics != nil {
		b.cfg.Metrics.RecordDLQMessage(msg.topic)
	}

	logCtx := observability.WithCorrelation(msg.ctx, msg.envelope.CorrelationID, "", "")
	observability.LoggerFromContext(logCtx, b.cfg.Logger).Error("message routed to DLQ",
		"dlq_topic", dlqName,
		"original_topic", msg.topic,
		"consumer_group", group,
		"partition_key", msg.partitionKey,
		"event_id", msg.envelope.EventID,
		"event_type", msg.envelope.Type,
		"attempts", attempts,
		"error", err.Error(),
	)
}
