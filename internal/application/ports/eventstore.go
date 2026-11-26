package ports

import (
	"context"
	"encoding/json"
	"time"

	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
)

// StoredEvent pairs a persisted Envelope with its monotonically increasing global sequence number.
type StoredEvent struct {
	GlobalSeq int64
	Envelope  events.Envelope
}

// EventStore is the append-only persistence port for Event-Sourced aggregates.
// Implementations must enforce optimistic concurrency control on (streamID, expectedVersion).
type EventStore interface {
	// Append atomically appends evts to streamID only if the current stream version
	// equals expectedVersion. Returns ErrVersionConflict otherwise.
	Append(ctx context.Context, streamID string, expectedVersion int64, evts ...events.Envelope) error

	// LoadStream returns all events for streamID with AggregateVersion > fromVersion,
	// ordered ascending by AggregateVersion.
	LoadStream(ctx context.Context, streamID string, fromVersion int64) ([]events.Envelope, error)

	// LoadAll returns all persisted events across streams with GlobalSeq > fromSeq,
	// ordered ascending by GlobalSeq (used for read model rebuilds and recovery).
	LoadAll(ctx context.Context, fromSeq int64) ([]StoredEvent, error)

	// ListStreamsByPrefix returns all stream IDs starting with prefix (e.g., "payment-").
	ListStreamsByPrefix(ctx context.Context, prefix string) ([]string, error)
}

// Snapshot stores serialized aggregate state at a specific version to accelerate rehydration.
type Snapshot struct {
	StreamID  string
	Version   int64
	State     json.RawMessage
	CreatedAt time.Time
}

// SnapshotStore is segregated from EventStore (ISP) because only long-lived streams
// like Wallet require periodic snapshots.
type SnapshotStore interface {
	SaveSnapshot(ctx context.Context, snap Snapshot) error
	LoadSnapshot(ctx context.Context, streamID string) (Snapshot, bool, error)
}
