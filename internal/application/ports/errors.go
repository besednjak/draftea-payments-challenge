package ports

import "errors"

var (
	ErrVersionConflict       = errors.New("event store version conflict")
	ErrStreamNotFound        = errors.New("event stream not found")
	ErrNotFound              = errors.New("record not found")
	ErrIdempotencyConflict   = errors.New("idempotency key reused with different request payload")
	ErrIdempotencyInProgress = errors.New("idempotency key request already in progress")
	ErrGatewayTimeout        = errors.New("external gateway timeout")
	ErrGatewayUnavailable    = errors.New("external gateway temporarily unavailable")
	ErrCircuitOpen           = errors.New("gateway circuit breaker is open")
)

type nonRetryableError struct {
	err error
}

func (e nonRetryableError) Error() string { return e.err.Error() }
func (e nonRetryableError) Unwrap() error { return e.err }

// NonRetryable marks an error (such as a corrupt payload / poison pill) as permanent
// so the EventBus routes the message directly to the DLQ without retrying.
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return nonRetryableError{err: err}
}

// IsNonRetryable reports whether err is marked as a non-retryable poison-pill error.
func IsNonRetryable(err error) bool {
	var target nonRetryableError
	return errors.As(err, &target)
}
