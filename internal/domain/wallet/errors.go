package wallet

import "errors"

var (
	ErrInvalidWalletID            = errors.New("wallet id is required")
	ErrInvalidUserID              = errors.New("user id is required")
	ErrInvalidPaymentID           = errors.New("payment id is required")
	ErrInvalidRefundID            = errors.New("refund id is required")
	ErrWalletAlreadyCreated       = errors.New("wallet already created")
	ErrWalletNotCreated           = errors.New("wallet not created")
	ErrCurrencyMismatch           = errors.New("wallet currency mismatch")
	ErrReservationNotFound        = errors.New("active reservation not found")
	ErrReservationAlreadyCaptured = errors.New("reservation already captured")
	ErrCaptureAmountMismatch      = errors.New("capture amount does not match reserved amount")
	ErrEventVersionMismatch       = errors.New("event aggregate version out of sequence")
	ErrUnknownEventType           = errors.New("unknown wallet event type")
)
