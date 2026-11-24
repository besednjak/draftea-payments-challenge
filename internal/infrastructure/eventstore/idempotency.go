package eventstore

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/besednjak/draftea-payments-challenge/internal/application/ports"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
)

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

func idempotencyMapKey(userID, key string) string {
	return userID + "\x00" + key
}

// Reserve atomically claims (userID, key) or returns the existing completed record.
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
	mapKey := idempotencyMapKey(userID, key)

	if record, exists := s.records[mapKey]; exists && (record.ExpiresAt.IsZero() || now.Before(record.ExpiresAt)) {
		if record.RequestHash != requestHash {
			return nil, false, ports.ErrIdempotencyConflict
		}
		if record.Status == ports.IdempotencyInProgress {
			return nil, false, ports.ErrIdempotencyInProgress
		}
		cloned := record
		if record.Response != nil {
			cloned.Response = make(json.RawMessage, len(record.Response))
			copy(cloned.Response, record.Response)
		}
		return &cloned, false, nil
	}

	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = now.Add(ttl)
	}
	s.records[mapKey] = ports.IdempotencyRecord{
		UserID:      userID,
		Key:         key,
		RequestHash: requestHash,
		Status:      ports.IdempotencyInProgress,
		ExpiresAt:   expiresAt,
	}
	return nil, true, nil
}

// Complete marks a reserved key as COMPLETED with its serialized response.
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

	mapKey := idempotencyMapKey(userID, key)
	record, exists := s.records[mapKey]
	if !exists {
		return ports.ErrNotFound
	}
	record.Status = ports.IdempotencyCompleted
	record.PaymentID = paymentID
	if response != nil {
		record.Response = make(json.RawMessage, len(response))
		copy(record.Response, response)
	}
	s.records[mapKey] = record
	return nil
}

// Delete removes a reserved idempotency record.
func (s *InMemoryIdempotencyStore) Delete(ctx context.Context, userID, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	delete(s.records, idempotencyMapKey(userID, key))
	s.mu.Unlock()
	return nil
}
