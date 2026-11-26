package eventstore

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/besednjak/draftea-payments-challenge/internal/application/ports"
)

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

// SaveSnapshot stores a defensive copy of snap keyed by StreamID.
func (s *InMemorySnapshotStore) SaveSnapshot(ctx context.Context, snap ports.Snapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	cloned := snap
	if snap.State != nil {
		cloned.State = make(json.RawMessage, len(snap.State))
		copy(cloned.State, snap.State)
	}
	if cloned.CreatedAt.IsZero() {
		cloned.CreatedAt = time.Now().UTC()
	}
	s.snapshots[snap.StreamID] = cloned
	return nil
}

// LoadSnapshot retrieves a defensive copy of the latest snapshot for streamID.
func (s *InMemorySnapshotStore) LoadSnapshot(ctx context.Context, streamID string) (ports.Snapshot, bool, error) {
	if err := ctx.Err(); err != nil {
		return ports.Snapshot{}, false, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	snap, exists := s.snapshots[streamID]
	if !exists {
		return ports.Snapshot{}, false, nil
	}
	cloned := snap
	if snap.State != nil {
		cloned.State = make(json.RawMessage, len(snap.State))
		copy(cloned.State, snap.State)
	}
	return cloned, true, nil
}
