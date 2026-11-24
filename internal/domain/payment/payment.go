package payment

import (
	"fmt"

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

// CanTransitionTo reports whether transitioning from s to next is valid in the state machine.
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

// IsActiveSaga reports whether the payment is still in an in-flight saga state
// that should be resumed automatically after a process restart.
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
// and refund invariants (refundedTotal <= capturedAmount).
type Payment struct {
	id             string
	walletID       string
	userID         string
	serviceID      string
	provider       string
	amount         events.Money
	status         Status
	providerRef    string
	failureReason  string
	capturedAmount int64
	refundedTotal  int64
	refunds        map[string]int64
	version        int64
	initiated      bool

	uncommitted []events.Envelope
	clock       events.Clock
	idGen       events.IDGenerator
}

func newEmpty(opts ...Option) *Payment {
	p := &Payment{
		refunds: make(map[string]int64),
		clock:   events.DefaultClock,
		idGen:   events.NewUUID,
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

// MarkCompleted transitions FUNDS_RESERVED, SUBMITTED, GATEWAY_UNKNOWN, or MANUAL_REVIEW -> COMPLETED.
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

// MarkFailed transitions FUNDS_RESERVED, SUBMITTED, GATEWAY_UNKNOWN, or MANUAL_REVIEW -> FAILED.
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

// RecordRefund applies a partial or full refund on a COMPLETED payment.
// Idempotent per refundID. Enforces refundedTotal + amount <= capturedAmount.
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
	if p.refundedTotal+money.Amount > p.capturedAmount {
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
		d, err := events.DecodePayload[events.PaymentInitiatedPayload](env)
		if err != nil {
			return err
		}
		p.id = d.PaymentID
		p.walletID = d.WalletID
		p.userID = d.UserID
		p.serviceID = d.ServiceID
		p.provider = d.Provider
		p.amount = d.Amount
		p.status = StatusPending
		p.initiated = true

	case events.EventPaymentFundsReserved:
		if _, err := events.DecodePayload[events.PaymentFundsReservedPayload](env); err != nil {
			return err
		}
		p.status = StatusFundsReserved

	case events.EventPaymentRejected:
		d, err := events.DecodePayload[events.PaymentRejectedPayload](env)
		if err != nil {
			return err
		}
		p.status = StatusRejected
		p.failureReason = d.Reason

	case events.EventPaymentSubmitted:
		d, err := events.DecodePayload[events.PaymentSubmittedPayload](env)
		if err != nil {
			return err
		}
		p.status = StatusSubmitted
		if d.ProviderRef != "" {
			p.providerRef = d.ProviderRef
		}

	case events.EventPaymentGatewayUnknown:
		d, err := events.DecodePayload[events.PaymentGatewayUnknownPayload](env)
		if err != nil {
			return err
		}
		p.status = StatusGatewayUnknown
		p.failureReason = d.Reason

	case events.EventPaymentManualReviewRequired:
		d, err := events.DecodePayload[events.PaymentManualReviewRequiredPayload](env)
		if err != nil {
			return err
		}
		p.status = StatusManualReview
		p.failureReason = d.Reason

	case events.EventPaymentCompleted:
		d, err := events.DecodePayload[events.PaymentCompletedPayload](env)
		if err != nil {
			return err
		}
		p.status = StatusCompleted
		p.capturedAmount = d.CapturedAmount
		if d.ProviderRef != "" {
			p.providerRef = d.ProviderRef
		}
		p.failureReason = ""

	case events.EventPaymentFailed:
		d, err := events.DecodePayload[events.PaymentFailedPayload](env)
		if err != nil {
			return err
		}
		p.status = StatusFailed
		p.failureReason = d.Reason

	case events.EventPaymentRefunded:
		d, err := events.DecodePayload[events.PaymentRefundedPayload](env)
		if err != nil {
			return err
		}
		p.refundedTotal = d.RefundedTotal
		p.refunds[d.RefundID] = d.Amount.Amount
		if d.FullyRefunded {
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

// --- Accessors ---

func (p *Payment) ID() string              { return p.id }
func (p *Payment) WalletID() string        { return p.walletID }
func (p *Payment) UserID() string          { return p.userID }
func (p *Payment) ServiceID() string       { return p.serviceID }
func (p *Payment) Provider() string        { return p.provider }
func (p *Payment) Amount() events.Money    { return p.amount }
func (p *Payment) Status() Status          { return p.status }
func (p *Payment) ProviderRef() string     { return p.providerRef }
func (p *Payment) FailureReason() string   { return p.failureReason }
func (p *Payment) CapturedAmount() int64   { return p.capturedAmount }
func (p *Payment) RefundedTotal() int64    { return p.refundedTotal }
func (p *Payment) RefundableAmount() int64 { return p.capturedAmount - p.refundedTotal }
func (p *Payment) Version() int64          { return p.version }
func (p *Payment) StreamID() string        { return events.StreamID(events.AggregateTypePayment, p.id) }

// BaseVersion returns the aggregate version prior to any uncommitted events.
func (p *Payment) BaseVersion() int64 {
	return p.version - int64(len(p.uncommitted))
}

// UncommittedEvents returns a copy of the events recorded since the last ClearUncommitted.
func (p *Payment) UncommittedEvents() []events.Envelope {
	out := make([]events.Envelope, len(p.uncommitted))
	copy(out, p.uncommitted)
	return out
}

// ClearUncommitted empties the pending event buffer after persistence.
func (p *Payment) ClearUncommitted() {
	p.uncommitted = nil
}
