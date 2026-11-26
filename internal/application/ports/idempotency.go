package ports

import (
	"context"
	"encoding/json"
	"time"
)

// InboxStore tracks processed message IDs per consumer to guarantee idempotent
// processing under at-least-once delivery.
type InboxStore interface {
	// TryMarkProcessed claims (consumer, eventID) if unseen.
	// Returns alreadyProcessed=true when the message was already processed.
	TryMarkProcessed(ctx context.Context, consumer, eventID string) (alreadyProcessed bool, err error)

	// UnmarkProcessed reverts a claimed (consumer, eventID) entry when handler
	// execution fails so subsequent retry attempts are not discarded as duplicates.
	UnmarkProcessed(ctx context.Context, consumer, eventID string) error
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
	Reserve(ctx context.Context, userID, key, requestHash string, ttl time.Duration) (existing *IdempotencyRecord, reserved bool, err error)
	Complete(ctx context.Context, userID, key, paymentID string, response json.RawMessage) error
	Delete(ctx context.Context, userID, key string) error
}
