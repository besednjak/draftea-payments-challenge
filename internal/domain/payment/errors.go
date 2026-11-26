package payment

import "errors"

var (
	ErrInvalidPaymentID        = errors.New("payment id is required")
	ErrInvalidWalletID         = errors.New("wallet id is required")
	ErrInvalidUserID           = errors.New("user id is required")
	ErrInvalidServiceID        = errors.New("service id is required")
	ErrInvalidProvider         = errors.New("provider is required")
	ErrInvalidRefundID         = errors.New("refund id is required")
	ErrPaymentAlreadyInitiated = errors.New("payment already initiated")
	ErrPaymentNotInitiated     = errors.New("payment not initiated")
	ErrInvalidTransition       = errors.New("invalid payment status transition")
	ErrCurrencyMismatch        = errors.New("payment currency mismatch")
	ErrRefundExceedsCaptured   = errors.New("refund amount exceeds remaining captured amount")
	ErrEventVersionMismatch    = errors.New("event aggregate version out of sequence")
	ErrUnknownEventType        = errors.New("unknown payment event type")
)
