package eventstore

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/besednjak/draftea-payments-challenge/internal/application/ports"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
)

// InMemoryEventStore is a thread-safe, append-only Event Store enforcing optimistic
// concurrency control on (streamID, version) and maintaining a global sequence log.
type InMemoryEventStore struct {
	mu      sync.RWMutex
	streams map[string][]events.Envelope
	global  []ports.StoredEvent
}

// NewInMemoryEventStore creates an empty InMemoryEventStore.
func NewInMemoryEventStore() *InMemoryEventStore {
	return &InMemoryEventStore{
		streams: make(map[string][]events.Envelope),
	}
}

func cloneEnvelope(env events.Envelope) events.Envelope {
	cp := env
	if env.Payload != nil {
		cp.Payload = make(json.RawMessage, len(env.Payload))
		copy(cp.Payload, env.Payload)
	}
	return cp
}

// Append atomically appends evts if the current stream version matches expectedVersion.
func (s *InMemoryEventStore) Append(ctx context.Context, streamID string, expectedVersion int64, evts ...events.Envelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(evts) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	existing := s.streams[streamID]
	currentVersion := int64(len(existing))
	if currentVersion != expectedVersion {
		return fmt.Errorf("%w: stream=%s expected=%d actual=%d",
			ports.ErrVersionConflict, streamID, expectedVersion, currentVersion)
	}

	for i, env := range evts {
		wantVersion := expectedVersion + int64(i) + 1
		if env.AggregateVersion != wantVersion {
			return fmt.Errorf("%w: stream=%s event[%d] version=%d want=%d",
				ports.ErrVersionConflict, streamID, i, env.AggregateVersion, wantVersion)
		}
	}

	for _, env := range evts {
		cloned := cloneEnvelope(env)
		existing = append(existing, cloned)
		s.global = append(s.global, ports.StoredEvent{
			GlobalSeq: int64(len(s.global) + 1),
			Envelope:  cloned,
		})
	}
	s.streams[streamID] = existing
	return nil
}

// LoadStream returns events for streamID with AggregateVersion > fromVersion.
func (s *InMemoryEventStore) LoadStream(ctx context.Context, streamID string, fromVersion int64) ([]events.Envelope, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	stream, ok := s.streams[streamID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ports.ErrStreamNotFound, streamID)
	}

	out := make([]events.Envelope, 0, len(stream))
	for _, env := range stream {
		if env.AggregateVersion > fromVersion {
			out = append(out, cloneEnvelope(env))
		}
	}
	return out, nil
}

// LoadAll returns all persisted events across all streams with GlobalSeq > fromSeq.
func (s *InMemoryEventStore) LoadAll(ctx context.Context, fromSeq int64) ([]ports.StoredEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if fromSeq < 0 {
		fromSeq = 0
	}
	if fromSeq >= int64(len(s.global)) {
		return nil, nil
	}

	out := make([]ports.StoredEvent, 0, int64(len(s.global))-fromSeq)
	for _, se := range s.global[fromSeq:] {
		out = append(out, ports.StoredEvent{
			GlobalSeq: se.GlobalSeq,
			Envelope:  cloneEnvelope(se.Envelope),
		})
	}
	return out, nil
}

// ListStreamsByPrefix returns sorted stream IDs matching prefix.
func (s *InMemoryEventStore) ListStreamsByPrefix(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	var ids []string
	for id := range s.streams {
		if strings.HasPrefix(id, prefix) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids, nil
}

// Name implements ports.HealthChecker.
func (s *InMemoryEventStore) Name() string { return "eventstore" }

// Check implements ports.HealthChecker.
func (s *InMemoryEventStore) Check(ctx context.Context) error { return ctx.Err() }

// --- SnapshotStore ---

// InMemorySnapshotStore persists the latest snapshot per stream.
type InMemorySnapshotStore struct {
	mu        sync.RWMutex
	snapshots map[string]ports.Snapshot
}

// NewInMemorySnapshotStore creates an empty InMemorySnapshotStore.
func NewInMemorySnapshotStore() *InMemorySnapshotStore {
	return &InMemorySnapshotStore{
		snapshots: make(map[string]ports.Snapshot),
	}
}

func (s *InMemorySnapshotStore) SaveSnapshot(ctx context.Context, snap ports.Snapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	cp := snap
	if snap.State != nil {
		cp.State = make(json.RawMessage, len(snap.State))
		copy(cp.State, snap.State)
	}
	if cp.CreatedAt.IsZero() {
		cp.CreatedAt = time.Now().UTC()
	}
	s.snapshots[snap.StreamID] = cp
	return nil
}

func (s *InMemorySnapshotStore) LoadSnapshot(ctx context.Context, streamID string) (ports.Snapshot, bool, error) {
	if err := ctx.Err(); err != nil {
		return ports.Snapshot{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	snap, ok := s.snapshots[streamID]
	if !ok {
		return ports.Snapshot{}, false, nil
	}
	cp := snap
	if snap.State != nil {
		cp.State = make(json.RawMessage, len(snap.State))
		copy(cp.State, snap.State)
	}
	return cp, true, nil
}

// --- InboxStore ---

// InMemoryInboxStore tracks processed (consumer, eventID) pairs for consumer deduplication.
type InMemoryInboxStore struct {
	mu        sync.Mutex
	processed map[string]struct{}
}

// NewInMemoryInboxStore creates an empty InMemoryInboxStore.
func NewInMemoryInboxStore() *InMemoryInboxStore {
	return &InMemoryInboxStore{
		processed: make(map[string]struct{}),
	}
}

func (s *InMemoryInboxStore) TryMarkProcessed(ctx context.Context, consumer, eventID string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	key := consumer + "\x00" + eventID

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.processed[key]; exists {
		return true, nil
	}
	s.processed[key] = struct{}{}
	return false, nil
}

// --- IdempotencyStore ---

// InMemoryIdempotencyStore deduplicates API requests by (userID, Idempotency-Key).
type InMemoryIdempotencyStore struct {
	mu      sync.Mutex
	records map[string]ports.IdempotencyRecord
	clock   events.Clock
}

// NewInMemoryIdempotencyStore creates an InMemoryIdempotencyStore with an optional clock.
func NewInMemoryIdempotencyStore(clock events.Clock) *InMemoryIdempotencyStore {
	if clock == nil {
		clock = events.DefaultClock
	}
	return &InMemoryIdempotencyStore{
		records: make(map[string]ports.IdempotencyRecord),
		clock:   clock,
	}
}

func idempKey(userID, key string) string {
	return userID + "\x00" + key
}

func (s *InMemoryIdempotencyStore) Reserve(
	ctx context.Context,
	userID, key, requestHash string,
	ttl time.Duration,
) (*ports.IdempotencyRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock().UTC()
	k := idempKey(userID, key)

	if rec, exists := s.records[k]; exists && (rec.ExpiresAt.IsZero() || now.Before(rec.ExpiresAt)) {
		if rec.RequestHash != requestHash {
			return nil, false, ports.ErrIdempotencyConflict
		}
		if rec.Status == ports.IdempotencyInProgress {
			return nil, false, ports.ErrIdempotencyInProgress
		}
		cp := rec
		if rec.Response != nil {
			cp.Response = make(json.RawMessage, len(rec.Response))
			copy(cp.Response, rec.Response)
		}
		return &cp, false, nil
	}

	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = now.Add(ttl)
	}
	s.records[k] = ports.IdempotencyRecord{
		UserID:      userID,
		Key:         key,
		RequestHash: requestHash,
		Status:      ports.IdempotencyInProgress,
		ExpiresAt:   expiresAt,
	}
	return nil, true, nil
}

func (s *InMemoryIdempotencyStore) Complete(
	ctx context.Context,
	userID, key, paymentID string,
	response json.RawMessage,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	k := idempKey(userID, key)
	rec, exists := s.records[k]
	if !exists {
		return ports.ErrNotFound
	}
	rec.Status = ports.IdempotencyCompleted
	rec.PaymentID = paymentID
	if response != nil {
		rec.Response = make(json.RawMessage, len(response))
		copy(rec.Response, response)
	}
	s.records[k] = rec
	return nil
}

func (s *InMemoryIdempotencyStore) Delete(ctx context.Context, userID, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.records, idempKey(userID, key))
	return nil
}
