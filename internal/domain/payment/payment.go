package payment

import (
	"fmt"
	"strings"
	"time"

	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
)

// Status represents the explicit lifecycle state of a Payment saga.
type Status string

const (
	StatusPending        Status = "PENDING"
	StatusFundsReserved  Status = "FUNDS_RESERVED"
	StatusRejected       Status = "REJECTED"
	StatusSubmitted      Status = "SUBMITTED"
	StatusGatewayUnknown Status = "GATEWAY_UNKNOWN"
	StatusManualReview   Status = "MANUAL_REVIEW"
	StatusCompleted      Status = "COMPLETED"
	StatusFailed         Status = "FAILED"
	StatusRefunded       Status = "REFUNDED"
)

var allowedTransitions = map[Status]map[Status]bool{
	StatusPending: {
		StatusRejected:      true,
		StatusFundsReserved: true,
	},
	StatusFundsReserved: {
		StatusSubmitted:      true,
		StatusGatewayUnknown: true,
		StatusCompleted:      true,
		StatusFailed:         true,
	},
	StatusSubmitted: {
		StatusCompleted:      true,
		StatusFailed:         true,
		StatusGatewayUnknown: true,
	},
	StatusGatewayUnknown: {
		StatusCompleted:    true,
		StatusFailed:       true,
		StatusManualReview: true,
	},
	StatusManualReview: {
		StatusCompleted: true,
		StatusFailed:    true,
	},
	StatusCompleted: {
		StatusRefunded: true,
	},
}

// CanTransitionTo reports whether transitioning from s to next is valid.
func (s Status) CanTransitionTo(next Status) bool {
	targets, ok := allowedTransitions[s]
	if !ok {
		return false
	}
	return targets[next]
}

// IsTerminal reports whether no further state transitions are possible.
func (s Status) IsTerminal() bool {
	return s == StatusRejected || s == StatusFailed || s == StatusRefunded
}

// IsActiveSaga reports whether the payment is in an in-flight state that should
// be resumed automatically after a process restart.
func (s Status) IsActiveSaga() bool {
	switch s {
	case StatusPending, StatusFundsReserved, StatusSubmitted, StatusGatewayUnknown:
		return true
	default:
		return false
	}
}

// Option configures non-persistent dependencies (clock, ID generator) for deterministic tests.
type Option func(*Payment)

// WithClock overrides the clock used to timestamp newly emitted events.
func WithClock(clock events.Clock) Option {
	return func(p *Payment) {
		if clock != nil {
			p.clock = clock
		}
	}
}

// WithIDGenerator overrides the ID generator used for newly emitted events.
func WithIDGenerator(gen events.IDGenerator) Option {
	return func(p *Payment) {
		if gen != nil {
			p.idGen = gen
		}
	}
}

// InitiateParams carries the required input to create a new Payment aggregate.
type InitiateParams struct {
	PaymentID string
	WalletID  string
	UserID    string
	ServiceID string
	Provider  string
	Amount    events.Money
}

// Payment is the Event-Sourced aggregate owning the payment lifecycle state machine
// and refund invariants (refundedTotal + pendingRefundTotal <= capturedAmount).
type Payment struct {
	id                 string
	walletID           string
	userID             string
	serviceID          string
	provider           string
	amount             events.Money
	status             Status
	providerRef        string
	failureReason      string
	capturedAmount     int64
	refundedTotal      int64
	pendingRefundTotal int64
	refunds            map[string]int64
	pendingRefunds     map[string]int64
	createdAt          time.Time
	version            int64
	initiated          bool

	uncommitted []events.Envelope
	clock       events.Clock
	idGen       events.IDGenerator
}

func newEmpty(opts ...Option) *Payment {
	p := &Payment{
		refunds:        make(map[string]int64),
		pendingRefunds: make(map[string]int64),
		clock:          events.DefaultClock,
		idGen:          events.NewUUID,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(p)
		}
	}
	return p
}

// Initiate creates a new Payment aggregate in PENDING state and emits PaymentInitiated.
func Initiate(params InitiateParams, meta events.Metadata, opts ...Option) (*Payment, error) {
	params.PaymentID = strings.TrimSpace(params.PaymentID)
	params.WalletID = strings.TrimSpace(params.WalletID)
	params.UserID = strings.TrimSpace(params.UserID)
	params.ServiceID = strings.TrimSpace(params.ServiceID)
	params.Provider = strings.TrimSpace(params.Provider)

	if params.PaymentID == "" {
		return nil, ErrInvalidPaymentID
	}
	if params.WalletID == "" {
		return nil, ErrInvalidWalletID
	}
	if params.UserID == "" {
		return nil, ErrInvalidUserID
	}
	if params.ServiceID == "" {
		return nil, ErrInvalidServiceID
	}
	if params.Provider == "" {
		return nil, ErrInvalidProvider
	}
	money, err := events.NewMoney(params.Amount.Amount, params.Amount.Currency)
	if err != nil {
		return nil, err
	}

	p := newEmpty(opts...)
	if meta.CorrelationID == "" {
		meta.CorrelationID = params.PaymentID
	}

	payload := events.PaymentInitiatedPayload{
		PaymentID: params.PaymentID,
		WalletID:  params.WalletID,
		UserID:    params.UserID,
		ServiceID: params.ServiceID,
		Provider:  params.Provider,
		Amount:    money,
	}
	if err := p.record(events.EventPaymentInitiated, params.PaymentID, payload, meta); err != nil {
		return nil, err
	}
	return p, nil
}

// Rehydrate reconstructs a Payment aggregate by replaying its event history in order.
func Rehydrate(history []events.Envelope, opts ...Option) (*Payment, error) {
	p := newEmpty(opts...)
	for _, env := range history {
		if err := p.Apply(env); err != nil {
			return nil, err
		}
	}
	if !p.initiated {
		return nil, ErrPaymentNotInitiated
	}
	return p, nil
}

// MarkFundsReserved transitions PENDING -> FUNDS_RESERVED.
func (p *Payment) MarkFundsReserved(availableAfter int64, meta events.Metadata) error {
	if err := p.ensureTransition(StatusFundsReserved); err != nil {
		return err
	}
	payload := events.PaymentFundsReservedPayload{
		PaymentID:      p.id,
		WalletID:       p.walletID,
		Amount:         p.amount,
		AvailableAfter: availableAfter,
	}
	return p.record(events.EventPaymentFundsReserved, p.id, payload, meta)
}

// MarkRejected transitions PENDING -> REJECTED.
func (p *Payment) MarkRejected(reason string, meta events.Metadata) error {
	if err := p.ensureTransition(StatusRejected); err != nil {
		return err
	}
	payload := events.PaymentRejectedPayload{
		PaymentID: p.id,
		Reason:    reason,
	}
	return p.record(events.EventPaymentRejected, p.id, payload, meta)
}

// MarkSubmitted transitions FUNDS_RESERVED -> SUBMITTED.
func (p *Payment) MarkSubmitted(providerRef string, meta events.Metadata) error {
	if err := p.ensureTransition(StatusSubmitted); err != nil {
		return err
	}
	payload := events.PaymentSubmittedPayload{
		PaymentID:   p.id,
		Provider:    p.provider,
		ProviderRef: providerRef,
	}
	return p.record(events.EventPaymentSubmitted, p.id, payload, meta)
}

// MarkGatewayUnknown transitions FUNDS_RESERVED or SUBMITTED -> GATEWAY_UNKNOWN.
func (p *Payment) MarkGatewayUnknown(reason string, meta events.Metadata) error {
	if err := p.ensureTransition(StatusGatewayUnknown); err != nil {
		return err
	}
	payload := events.PaymentGatewayUnknownPayload{
		PaymentID: p.id,
		Provider:  p.provider,
		Reason:    reason,
	}
	return p.record(events.EventPaymentGatewayUnknown, p.id, payload, meta)
}

// MarkManualReview transitions GATEWAY_UNKNOWN -> MANUAL_REVIEW.
func (p *Payment) MarkManualReview(reason string, meta events.Metadata) error {
	if err := p.ensureTransition(StatusManualReview); err != nil {
		return err
	}
	payload := events.PaymentManualReviewRequiredPayload{
		PaymentID: p.id,
		Reason:    reason,
	}
	return p.record(events.EventPaymentManualReviewRequired, p.id, payload, meta)
}

// MarkCompleted transitions an active or manual-review payment to COMPLETED.
func (p *Payment) MarkCompleted(providerRef string, meta events.Metadata) error {
	if err := p.ensureTransition(StatusCompleted); err != nil {
		return err
	}
	effectiveRef := providerRef
	if effectiveRef == "" {
		effectiveRef = p.providerRef
	}
	payload := events.PaymentCompletedPayload{
		PaymentID:      p.id,
		ProviderRef:    effectiveRef,
		CapturedAmount: p.amount.Amount,
		Currency:       p.amount.Currency,
	}
	return p.record(events.EventPaymentCompleted, p.id, payload, meta)
}

// MarkFailed transitions an active or manual-review payment to FAILED.
func (p *Payment) MarkFailed(reason string, meta events.Metadata) error {
	if err := p.ensureTransition(StatusFailed); err != nil {
		return err
	}
	payload := events.PaymentFailedPayload{
		PaymentID: p.id,
		Reason:    reason,
	}
	return p.record(events.EventPaymentFailed, p.id, payload, meta)
}

// RequestRefund reserves an in-flight refund amount on a COMPLETED payment so
// concurrent refund requests cannot collectively exceed capturedAmount.
func (p *Payment) RequestRefund(refundID string, amount events.Money, meta events.Metadata) error {
	if !p.initiated {
		return ErrPaymentNotInitiated
	}
	if refundID == "" {
		return ErrInvalidRefundID
	}
	money, err := events.NewMoney(amount.Amount, amount.Currency)
	if err != nil {
		return err
	}
	if _, completed := p.refunds[refundID]; completed {
		return nil
	}
	if _, pending := p.pendingRefunds[refundID]; pending {
		return nil
	}
	if p.status != StatusCompleted {
		return fmt.Errorf("%w: cannot refund payment in status %s", ErrInvalidTransition, p.status)
	}
	if money.Currency != p.amount.Currency {
		return fmt.Errorf("%w: payment=%s refund=%s", ErrCurrencyMismatch, p.amount.Currency, money.Currency)
	}
	if money.Amount > p.RefundableAmount() {
		return fmt.Errorf("%w: captured=%d refunded=%d pending=%d requested=%d",
			ErrRefundExceedsCaptured, p.capturedAmount, p.refundedTotal, p.pendingRefundTotal, money.Amount)
	}

	payload := events.PaymentRefundRequestedPayload{
		PaymentID: p.id,
		RefundID:  refundID,
		Amount:    money,
	}
	return p.record(events.EventPaymentRefundRequested, p.id, payload, meta)
}

// DeclineRefund releases an in-flight refund reservation after an explicit gateway decline.
func (p *Payment) DeclineRefund(refundID, reason string, meta events.Metadata) error {
	if !p.initiated {
		return ErrPaymentNotInitiated
	}
	if refundID == "" {
		return ErrInvalidRefundID
	}
	if _, pending := p.pendingRefunds[refundID]; !pending {
		return nil
	}

	payload := events.PaymentRefundDeclinedPayload{
		PaymentID: p.id,
		RefundID:  refundID,
		Reason:    reason,
	}
	return p.record(events.EventPaymentRefundDeclined, p.id, payload, meta)
}

// RecordRefund applies a settled partial or full refund on a COMPLETED payment.
// It is idempotent per refundID and enforces refundedTotal + amount <= capturedAmount.
func (p *Payment) RecordRefund(refundID string, amount events.Money, meta events.Metadata) error {
	if !p.initiated {
		return ErrPaymentNotInitiated
	}
	if refundID == "" {
		return ErrInvalidRefundID
	}
	money, err := events.NewMoney(amount.Amount, amount.Currency)
	if err != nil {
		return err
	}
	if _, exists := p.refunds[refundID]; exists {
		return nil
	}
	if p.status != StatusCompleted {
		return fmt.Errorf("%w: cannot refund payment in status %s", ErrInvalidTransition, p.status)
	}
	if money.Currency != p.amount.Currency {
		return fmt.Errorf("%w: payment=%s refund=%s", ErrCurrencyMismatch, p.amount.Currency, money.Currency)
	}

	availableLimit := p.RefundableAmount()
	if reserved, wasPending := p.pendingRefunds[refundID]; wasPending {
		availableLimit += reserved
	}
	if money.Amount > availableLimit {
		return fmt.Errorf("%w: captured=%d refunded=%d requested=%d",
			ErrRefundExceedsCaptured, p.capturedAmount, p.refundedTotal, money.Amount)
	}

	newTotal := p.refundedTotal + money.Amount
	payload := events.PaymentRefundedPayload{
		PaymentID:     p.id,
		RefundID:      refundID,
		Amount:        money,
		RefundedTotal: newTotal,
		FullyRefunded: newTotal == p.capturedAmount,
	}
	return p.record(events.EventPaymentRefunded, p.id, payload, meta)
}

// Apply advances the aggregate state using a persisted or newly recorded event.
func (p *Payment) Apply(env events.Envelope) error {
	expectedVersion := p.version + 1
	if env.AggregateVersion != expectedVersion {
		return fmt.Errorf("%w: expected %d, got %d", ErrEventVersionMismatch, expectedVersion, env.AggregateVersion)
	}

	switch env.Type {
	case events.EventPaymentInitiated:
		if p.initiated {
			return ErrPaymentAlreadyInitiated
		}
		payload, err := events.DecodePayload[events.PaymentInitiatedPayload](env)
		if err != nil {
			return err
		}
		p.id = payload.PaymentID
		p.walletID = payload.WalletID
		p.userID = payload.UserID
		p.serviceID = payload.ServiceID
		p.provider = payload.Provider
		p.amount = payload.Amount
		p.status = StatusPending
		p.createdAt = env.OccurredAt
		p.initiated = true

	case events.EventPaymentFundsReserved:
		if _, err := events.DecodePayload[events.PaymentFundsReservedPayload](env); err != nil {
			return err
		}
		p.status = StatusFundsReserved

	case events.EventPaymentRejected:
		payload, err := events.DecodePayload[events.PaymentRejectedPayload](env)
		if err != nil {
			return err
		}
		p.status = StatusRejected
		p.failureReason = payload.Reason

	case events.EventPaymentSubmitted:
		payload, err := events.DecodePayload[events.PaymentSubmittedPayload](env)
		if err != nil {
			return err
		}
		p.status = StatusSubmitted
		if payload.ProviderRef != "" {
			p.providerRef = payload.ProviderRef
		}

	case events.EventPaymentGatewayUnknown:
		payload, err := events.DecodePayload[events.PaymentGatewayUnknownPayload](env)
		if err != nil {
			return err
		}
		p.status = StatusGatewayUnknown
		p.failureReason = payload.Reason

	case events.EventPaymentManualReviewRequired:
		payload, err := events.DecodePayload[events.PaymentManualReviewRequiredPayload](env)
		if err != nil {
			return err
		}
		p.status = StatusManualReview
		p.failureReason = payload.Reason

	case events.EventPaymentCompleted:
		payload, err := events.DecodePayload[events.PaymentCompletedPayload](env)
		if err != nil {
			return err
		}
		p.status = StatusCompleted
		p.capturedAmount = payload.CapturedAmount
		if payload.ProviderRef != "" {
			p.providerRef = payload.ProviderRef
		}
		p.failureReason = ""

	case events.EventPaymentFailed:
		payload, err := events.DecodePayload[events.PaymentFailedPayload](env)
		if err != nil {
			return err
		}
		p.status = StatusFailed
		p.failureReason = payload.Reason

	case events.EventPaymentRefundRequested:
		payload, err := events.DecodePayload[events.PaymentRefundRequestedPayload](env)
		if err != nil {
			return err
		}
		p.pendingRefunds[payload.RefundID] = payload.Amount.Amount
		p.pendingRefundTotal += payload.Amount.Amount

	case events.EventPaymentRefundDeclined:
		payload, err := events.DecodePayload[events.PaymentRefundDeclinedPayload](env)
		if err != nil {
			return err
		}
		if reserved, exists := p.pendingRefunds[payload.RefundID]; exists {
			delete(p.pendingRefunds, payload.RefundID)
			p.pendingRefundTotal -= reserved
		}

	case events.EventPaymentRefunded:
		payload, err := events.DecodePayload[events.PaymentRefundedPayload](env)
		if err != nil {
			return err
		}
		if reserved, exists := p.pendingRefunds[payload.RefundID]; exists {
			delete(p.pendingRefunds, payload.RefundID)
			p.pendingRefundTotal -= reserved
		}
		p.refundedTotal = payload.RefundedTotal
		p.refunds[payload.RefundID] = payload.Amount.Amount
		if payload.FullyRefunded {
			p.status = StatusRefunded
		}

	default:
		return fmt.Errorf("%w: %s", ErrUnknownEventType, env.Type)
	}

	p.version = env.AggregateVersion
	return nil
}

func (p *Payment) ensureTransition(next Status) error {
	if !p.initiated {
		return ErrPaymentNotInitiated
	}
	if !p.status.CanTransitionTo(next) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, p.status, next)
	}
	return nil
}

func (p *Payment) record(eventType, aggregateID string, payload any, meta events.Metadata) error {
	if meta.CorrelationID == "" {
		meta.CorrelationID = aggregateID
	}
	env, err := events.NewEnvelope(
		eventType,
		events.AggregateTypePayment,
		aggregateID,
		p.version+1,
		payload,
		meta,
		p.clock,
		p.idGen,
	)
	if err != nil {
		return err
	}
	if err := p.Apply(env); err != nil {
		return err
	}
	p.uncommitted = append(p.uncommitted, env)
	return nil
}

func (p *Payment) ID() string                { return p.id }
func (p *Payment) WalletID() string          { return p.walletID }
func (p *Payment) UserID() string            { return p.userID }
func (p *Payment) ServiceID() string         { return p.serviceID }
func (p *Payment) Provider() string          { return p.provider }
func (p *Payment) Amount() events.Money      { return p.amount }
func (p *Payment) Status() Status            { return p.status }
func (p *Payment) ProviderRef() string       { return p.providerRef }
func (p *Payment) FailureReason() string     { return p.failureReason }
func (p *Payment) CapturedAmount() int64     { return p.capturedAmount }
func (p *Payment) RefundedTotal() int64      { return p.refundedTotal }
func (p *Payment) PendingRefundTotal() int64 { return p.pendingRefundTotal }
func (p *Payment) RefundableAmount() int64 {
	return p.capturedAmount - p.refundedTotal - p.pendingRefundTotal
}
func (p *Payment) CreatedAt() time.Time { return p.createdAt }
func (p *Payment) Version() int64       { return p.version }
func (p *Payment) StreamID() string     { return events.StreamID(events.AggregateTypePayment, p.id) }

// HasPendingRefund reports whether refundID is currently reserved in flight.
func (p *Payment) HasPendingRefund(refundID string) bool {
	_, exists := p.pendingRefunds[refundID]
	return exists
}

// BaseVersion returns the aggregate version prior to any uncommitted events.
func (p *Payment) BaseVersion() int64 {
	return p.version - int64(len(p.uncommitted))
}

// UncommittedEvents returns a copy of events recorded since the last ClearUncommitted.
func (p *Payment) UncommittedEvents() []events.Envelope {
	copied := make([]events.Envelope, len(p.uncommitted))
	copy(copied, p.uncommitted)
	return copied
}

// ClearUncommitted empties the pending event buffer after persistence.
func (p *Payment) ClearUncommitted() {
	p.uncommitted = nil
}
