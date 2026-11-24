package eventstore

import (
	"context"
	"sync"
)

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

func inboxKey(consumer, eventID string) string {
	return consumer + "\x00" + eventID
}

// TryMarkProcessed claims (consumer, eventID) if unseen.
func (s *InMemoryInboxStore) TryMarkProcessed(ctx context.Context, consumer, eventID string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := inboxKey(consumer, eventID)
	if _, exists := s.processed[key]; exists {
		return true, nil
	}
	s.processed[key] = struct{}{}
	return false, nil
}

// UnmarkProcessed releases a claimed (consumer, eventID) entry when processing fails.
func (s *InMemoryInboxStore) UnmarkProcessed(ctx context.Context, consumer, eventID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	delete(s.processed, inboxKey(consumer, eventID))
	s.mu.Unlock()
	return nil
}
