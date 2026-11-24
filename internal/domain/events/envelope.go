package events

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const (
	// DefaultSchemaVersion is the initial schema version for all envelopes.
	DefaultSchemaVersion = 1
)

var (
	// ErrInvalidEnvelope is returned when required envelope fields are missing.
	ErrInvalidEnvelope = errors.New("invalid event envelope")
)

// Clock returns the current time. Injectable in tests for deterministic timestamps.
type Clock func() time.Time

// IDGenerator produces unique identifiers. Injectable in tests for deterministic IDs.
type IDGenerator func() string

// DefaultClock returns the current UTC time.
func DefaultClock() time.Time {
	return time.Now().UTC()
}

// NewUUID generates a random RFC 4122 version 4 UUID using crypto/rand (stdlib only).
func NewUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant RFC 4122

	var out [36]byte
	hex.Encode(out[0:8], b[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], b[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], b[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], b[8:10])
	out[23] = '-'
	hex.Encode(out[24:36], b[10:16])
	return string(out[:])
}

// Envelope is the canonical message wrapper shared by all domain events and commands.
type Envelope struct {
	EventID          string          `json:"event_id"`
	Type             string          `json:"type"`
	SchemaVersion    int             `json:"schema_version"`
	AggregateType    string          `json:"aggregate_type"`
	AggregateID      string          `json:"aggregate_id"`
	AggregateVersion int64           `json:"aggregate_version"`
	OccurredAt       time.Time       `json:"occurred_at"`
	CorrelationID    string          `json:"correlation_id"`
	CausationID      string          `json:"causation_id,omitempty"`
	Payload          json.RawMessage `json:"payload"`
}

// Metadata carries tracing and optional deterministic overrides when creating envelopes.
type Metadata struct {
	CorrelationID string
	CausationID   string
	EventID       string
	OccurredAt    time.Time
}

// NewEnvelope marshals payload into a standard Envelope.
func NewEnvelope(
	eventType string,
	aggregateType string,
	aggregateID string,
	aggregateVersion int64,
	payload any,
	meta Metadata,
	clock Clock,
	idGen IDGenerator,
) (Envelope, error) {
	if eventType == "" || aggregateType == "" || aggregateID == "" {
		return Envelope{}, fmt.Errorf("%w: eventType, aggregateType, and aggregateID are required", ErrInvalidEnvelope)
	}
	if aggregateVersion < 0 {
		return Envelope{}, fmt.Errorf("%w: aggregateVersion must be >= 0", ErrInvalidEnvelope)
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("marshal payload for %s: %w", eventType, err)
	}

	eventID := meta.EventID
	if eventID == "" {
		if idGen != nil {
			eventID = idGen()
		} else {
			eventID = NewUUID()
		}
	}

	occurredAt := meta.OccurredAt
	if occurredAt.IsZero() {
		if clock != nil {
			occurredAt = clock().UTC()
		} else {
			occurredAt = DefaultClock()
		}
	} else {
		occurredAt = occurredAt.UTC()
	}

	return Envelope{
		EventID:          eventID,
		Type:             eventType,
		SchemaVersion:    DefaultSchemaVersion,
		AggregateType:    aggregateType,
		AggregateID:      aggregateID,
		AggregateVersion: aggregateVersion,
		OccurredAt:       occurredAt,
		CorrelationID:    meta.CorrelationID,
		CausationID:      meta.CausationID,
		Payload:          raw,
	}, nil
}

// DecodePayload unmarshals the JSON payload of an Envelope into type T.
func DecodePayload[T any](env Envelope) (T, error) {
	var out T
	if len(env.Payload) == 0 {
		return out, fmt.Errorf("%w: empty payload for %s", ErrInvalidEnvelope, env.Type)
	}
	if err := json.Unmarshal(env.Payload, &out); err != nil {
		return out, fmt.Errorf("decode payload for %s: %w", env.Type, err)
	}
	return out, nil
}

// StreamID returns the canonical Event Store stream identifier (<aggregate>-<id>).
func StreamID(aggregateType, aggregateID string) string {
	return fmt.Sprintf("%s-%s", aggregateType, aggregateID)
}
