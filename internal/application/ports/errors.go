package ports

import "errors"

var (
	// ErrVersionConflict indicates an optimistic locking violation on (stream_id, version).
	ErrVersionConflict = errors.New("event store version conflict")
	// ErrStreamNotFound indicates the requested aggregate stream does not exist.
	ErrStreamNotFound = errors.New("event stream not found")
	// ErrNotFound indicates a read model or state record was not found.
	ErrNotFound = errors.New("record not found")
	// ErrIdempotencyConflict indicates an Idempotency-Key was reused with a different request payload.
	ErrIdempotencyConflict = errors.New("idempotency key reused with different request payload")
	// ErrIdempotencyInProgress indicates a request with the same Idempotency-Key is currently executing.
	ErrIdempotencyInProgress = errors.New("idempotency key request already in progress")
	// ErrGatewayTimeout indicates a call to the external payment gateway timed out (uncertain outcome).
	ErrGatewayTimeout = errors.New("external gateway timeout")
	// ErrGatewayUnavailable indicates a transient gateway infrastructure failure (e.g., 5xx/429).
	ErrGatewayUnavailable = errors.New("external gateway temporarily unavailable")
	// ErrCircuitOpen indicates the gateway circuit breaker is open and the request was NOT sent.
	ErrCircuitOpen = errors.New("gateway circuit breaker is open")
)
