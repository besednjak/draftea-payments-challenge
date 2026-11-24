package ports

import (
	"context"
	"encoding/json"
	"time"
)

// InboxStore tracks processed message IDs per consumer to guarantee idempotent
// processing under at-least-once delivery.
type InboxStore interface {
	// TryMarkProcessed records (consumer, eventID) if not seen before.
	// Returns alreadyProcessed=true when the message was already handled.
	TryMarkProcessed(ctx context.Context, consumer, eventID string) (alreadyProcessed bool, err error)
}

// IdempotencyStatus tracks the execution state of an API-level idempotent request.
type IdempotencyStatus string

const (
	IdempotencyInProgress IdempotencyStatus = "IN_PROGRESS"
	IdempotencyCompleted  IdempotencyStatus = "COMPLETED"
)

// IdempotencyRecord holds the persisted outcome of an API-level idempotent request.
type IdempotencyRecord struct {
	UserID      string
	Key         string
	RequestHash string
	Status      IdempotencyStatus
	PaymentID   string
	Response    json.RawMessage
	ExpiresAt   time.Time
}

// IdempotencyStore deduplicates client requests using (userID, Idempotency-Key).
type IdempotencyStore interface {
	// Reserve atomically claims (userID, key).
	//   - If unused (or expired), inserts an IN_PROGRESS record and returns (nil, true, nil).
	//   - If already present with a different requestHash, returns ErrIdempotencyConflict.
	//   - If IN_PROGRESS with the same requestHash, returns ErrIdempotencyInProgress.
	//   - If COMPLETED with the same requestHash, returns (record, false, nil).
	Reserve(ctx context.Context, userID, key, requestHash string, ttl time.Duration) (existing *IdempotencyRecord, reserved bool, err error)

	// Complete marks a reserved key as COMPLETED with the serialized response.
	Complete(ctx context.Context, userID, key, paymentID string, response json.RawMessage) error

	// Delete removes a reserved key (e.g., if the initial validation failed before persisting).
	Delete(ctx context.Context, userID, key string) error
}
