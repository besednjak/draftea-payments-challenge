package events

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const DefaultSchemaVersion = 1

var ErrInvalidEnvelope = errors.New("invalid event envelope")

// Clock returns the current time and is injectable for deterministic tests.
type Clock func() time.Time

// IDGenerator produces unique identifiers and is injectable for deterministic tests.
type IDGenerator func() string

// DefaultClock returns the current UTC time.
func DefaultClock() time.Time {
	return time.Now().UTC()
}

// NewUUID generates a random RFC 4122 version 4 UUID using crypto/rand.
func NewUUID() string {
	var raw [16]byte
	_, _ = rand.Read(raw[:])
	// Set version (4) and RFC 4122 variant bits.
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80

	var formatted [36]byte
	hex.Encode(formatted[0:8], raw[0:4])
	formatted[8] = '-'
	hex.Encode(formatted[9:13], raw[4:6])
	formatted[13] = '-'
	hex.Encode(formatted[14:18], raw[6:8])
	formatted[18] = '-'
	hex.Encode(formatted[19:23], raw[8:10])
	formatted[23] = '-'
	hex.Encode(formatted[24:36], raw[10:16])
	return string(formatted[:])
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

// Metadata carries tracing identifiers and optional deterministic overrides.
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

	encoded, err := json.Marshal(payload)
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
		Payload:          encoded,
	}, nil
}

// DecodePayload unmarshals the JSON payload of an Envelope into type T.
func DecodePayload[T any](env Envelope) (T, error) {
	var decoded T
	if len(env.Payload) == 0 {
		return decoded, fmt.Errorf("%w: empty payload for %s", ErrInvalidEnvelope, env.Type)
	}
	if err := json.Unmarshal(env.Payload, &decoded); err != nil {
		return decoded, fmt.Errorf("decode payload for %s: %w", env.Type, err)
	}
	return decoded, nil
}

// StreamID returns the canonical Event Store stream identifier (<aggregate>-<id>).
func StreamID(aggregateType, aggregateID string) string {
	return fmt.Sprintf("%s-%s", aggregateType, aggregateID)
}
