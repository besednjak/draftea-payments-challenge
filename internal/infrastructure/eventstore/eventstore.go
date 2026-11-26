package eventstore

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"

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
	cloned := env
	if env.Payload != nil {
		cloned.Payload = make(json.RawMessage, len(env.Payload))
		copy(cloned.Payload, env.Payload)
	}
	return cloned
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

	for index, env := range evts {
		wantVersion := expectedVersion + int64(index) + 1
		if env.AggregateVersion != wantVersion {
			return fmt.Errorf("%w: stream=%s event[%d] version=%d want=%d",
				ports.ErrVersionConflict, streamID, index, env.AggregateVersion, wantVersion)
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

	stream, exists := s.streams[streamID]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ports.ErrStreamNotFound, streamID)
	}

	loaded := make([]events.Envelope, 0, len(stream))
	for _, env := range stream {
		if env.AggregateVersion > fromVersion {
			loaded = append(loaded, cloneEnvelope(env))
		}
	}
	return loaded, nil
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

	loaded := make([]ports.StoredEvent, 0, int64(len(s.global))-fromSeq)
	for _, storedEvent := range s.global[fromSeq:] {
		loaded = append(loaded, ports.StoredEvent{
			GlobalSeq: storedEvent.GlobalSeq,
			Envelope:  cloneEnvelope(storedEvent.Envelope),
		})
	}
	return loaded, nil
}

// ListStreamsByPrefix returns sorted stream IDs matching prefix.
func (s *InMemoryEventStore) ListStreamsByPrefix(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	var matching []string
	for streamID := range s.streams {
		if strings.HasPrefix(streamID, prefix) {
			matching = append(matching, streamID)
		}
	}
	slices.Sort(matching)
	return matching, nil
}

// Name implements ports.HealthChecker.
func (s *InMemoryEventStore) Name() string { return "eventstore" }

// Check implements ports.HealthChecker.
func (s *InMemoryEventStore) Check(ctx context.Context) error { return ctx.Err() }
