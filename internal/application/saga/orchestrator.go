package saga

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/besednjak/draftea-payments-challenge/internal/application/ports"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/payment"
)

const (
	walletConsumerGroup  = "payment-saga-wallet"
	gatewayConsumerGroup = "payment-saga-gateway"
)

// Config wires dependencies for the Payment Saga Orchestrator.
type Config struct {
	EventStore      ports.EventStore
	PaymentViews    ports.PaymentViewReadModel
	Publisher       ports.EventPublisher
	Inbox           ports.InboxStore
	Idempotency     ports.IdempotencyStore
	Metrics         ports.MetricsRecorder
	Logger          *slog.Logger
	SyncReserveWait time.Duration
	MaxLockRetries  int
	Clock           events.Clock
	IDGenerator     events.IDGenerator
}

// InitiatePaymentRequest is the application input for POST /v1/payments.
type InitiatePaymentRequest struct {
	IdempotencyKey string
	PaymentID      string
	WalletID       string
	UserID         string
	ServiceID      string
	Provider       string
	Amount         events.Money
}

// InitiatePaymentResult mirrors the bounded-wait API response (202 Accepted or 422 Rejected).
type InitiatePaymentResult struct {
	PaymentID            string         `json:"payment_id"`
	Status               payment.Status `json:"status"`
	AvailableBalance     int64          `json:"available_balance,omitempty"`
	Reason               string         `json:"reason,omitempty"`
	HTTPStatus           int            `json:"http_status"`
	FromIdempotencyCache bool           `json:"from_idempotency_cache,omitempty"`
}

// RecoverySummary reports the outcome of RecoverActiveSagas after a restart.
type RecoverySummary struct {
	Scanned  int
	Resumed  int
	ByStatus map[payment.Status]int
}

type reserveOutcome struct {
	reserved       bool
	availableAfter int64
	reason         string
}

// PaymentOrchestrator owns the Payment Event-Sourced aggregate and coordinates
// the distributed payment saga across Wallet and Gateway Adapter via commands/events.
type PaymentOrchestrator struct {
	store           ports.EventStore
	views           ports.PaymentViewReadModel
	publisher       ports.EventPublisher
	inbox           ports.InboxStore
	idempotency     ports.IdempotencyStore
	metrics         ports.MetricsRecorder
	logger          *slog.Logger
	syncReserveWait time.Duration
	maxLockRetries  int
	clock           events.Clock
	idGen           events.IDGenerator

	waitMu  sync.Mutex
	waiters map[string]chan reserveOutcome

	refMu        sync.Mutex
	approvedRefs map[string]string
}

// NewPaymentOrchestrator constructs a PaymentOrchestrator.
func NewPaymentOrchestrator(cfg Config) *PaymentOrchestrator {
	if cfg.SyncReserveWait <= 0 {
		cfg.SyncReserveWait = 2 * time.Second
	}
	if cfg.MaxLockRetries <= 0 {
		cfg.MaxLockRetries = 5
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	if cfg.Clock == nil {
		cfg.Clock = events.DefaultClock
	}
	if cfg.IDGenerator == nil {
		cfg.IDGenerator = events.NewUUID
	}

	return &PaymentOrchestrator{
		store:           cfg.EventStore,
		views:           cfg.PaymentViews,
		publisher:       cfg.Publisher,
		inbox:           cfg.Inbox,
		idempotency:     cfg.Idempotency,
		metrics:         cfg.Metrics,
		logger:          cfg.Logger,
		syncReserveWait: cfg.SyncReserveWait,
		maxLockRetries:  cfg.MaxLockRetries,
		clock:           cfg.Clock,
		idGen:           cfg.IDGenerator,
		waiters:         make(map[string]chan reserveOutcome),
		approvedRefs:    make(map[string]string),
	}
}

// InitiatePayment validates idempotency, persists PaymentInitiated, dispatches ReserveFunds,
// and waits up to SyncReserveWait (default 2s) to return an immediate 422 on insufficient funds
// or 202 Accepted with the updated available balance.
func (o *PaymentOrchestrator) InitiatePayment(ctx context.Context, req InitiatePaymentRequest) (InitiatePaymentResult, error) {
	reqHash := hashInitiateRequest(req)
	idempReserved := false

	if o.idempotency != nil && req.IdempotencyKey != "" {
		existing, reserved, err := o.idempotency.Reserve(ctx, req.UserID, req.IdempotencyKey, reqHash, 24*time.Hour)
		if err != nil {
			return InitiatePaymentResult{}, err
		}
		if !reserved && existing != nil {
			var cached InitiatePaymentResult
			if err := json.Unmarshal(existing.Response, &cached); err != nil {
				return InitiatePaymentResult{}, fmt.Errorf("decode cached idempotency response: %w", err)
			}
			cached.FromIdempotencyCache = true
			return cached, nil
		}
		idempReserved = true
	}

	paymentID := req.PaymentID
	if paymentID == "" {
		paymentID = "pay_" + o.idGen()
	}

	p, err := payment.Initiate(
		payment.InitiateParams{
			PaymentID: paymentID,
			WalletID:  req.WalletID,
			UserID:    req.UserID,
			ServiceID: req.ServiceID,
			Provider:  req.Provider,
			Amount:    req.Amount,
		},
		events.Metadata{CorrelationID: paymentID},
		payment.WithClock(o.clock),
		payment.WithIDGenerator(o.idGen),
	)
	if err != nil {
		if idempReserved {
			_ = o.idempotency.Delete(ctx, req.UserID, req.IdempotencyKey)
		}
		return InitiatePaymentResult{}, err
	}

	uncommitted := p.UncommittedEvents()
	if err := o.store.Append(ctx, p.StreamID(), 0, uncommitted...); err != nil {
		if idempReserved {
			_ = o.idempotency.Delete(ctx, req.UserID, req.IdempotencyKey)
		}
		return InitiatePaymentResult{}, err
	}
	p.ClearUncommitted()
	_ = o.syncPaymentView(ctx, p, uncommitted[0].OccurredAt)

	if err := o.publishPaymentEvents(ctx, p.ID(), uncommitted); err != nil {
		return InitiatePaymentResult{}, err
	}

	// Register waiter BEFORE dispatching ReserveFunds to eliminate missed signals.
	waitCh := o.registerWaiter(p.ID())
	defer o.unregisterWaiter(p.ID())

	if err := o.emitReserveFunds(ctx, p, uncommitted[0].EventID); err != nil {
		return InitiatePaymentResult{}, err
	}

	result := InitiatePaymentResult{
		PaymentID:  p.ID(),
		Status:     payment.StatusPending,
		HTTPStatus: http.StatusAccepted,
	}

	timer := time.NewTimer(o.syncReserveWait)
	defer timer.Stop()

	select {
	case outcome := <-waitCh:
		if outcome.reserved {
			result.Status = payment.StatusFundsReserved
			result.AvailableBalance = outcome.availableAfter
			result.HTTPStatus = http.StatusAccepted
		} else {
			result.Status = payment.StatusRejected
			result.Reason = outcome.reason
			result.HTTPStatus = http.StatusUnprocessableEntity
		}
	case <-timer.C:
		// Bounded 2s wait elapsed: return 202 Accepted (PENDING) so client polls GET /v1/payments/{id}.
	case <-ctx.Done():
		// Caller cancelled/timed out: payment continues asynchronously.
	}

	if idempReserved {
		if raw, err := json.Marshal(result); err == nil {
			_ = o.idempotency.Complete(ctx, req.UserID, req.IdempotencyKey, p.ID(), raw)
		}
	}

	return result, nil
}

// GetPayment queries the payment read model (falling back to Event Store rehydration).
func (o *PaymentOrchestrator) GetPayment(ctx context.Context, paymentID string) (ports.PaymentView, error) {
	if o.views != nil {
		return o.views.Get(ctx, paymentID)
	}
	p, err := o.Load(ctx, paymentID)
	if err != nil {
		return ports.PaymentView{}, err
	}
	return toPaymentView(p, o.clock().UTC()), nil
}

// Load rehydrates a Payment aggregate directly from the Event Store.
func (o *PaymentOrchestrator) Load(ctx context.Context, paymentID string) (*payment.Payment, error) {
	streamID := events.StreamID(events.AggregateTypePayment, paymentID)
	history, err := o.store.LoadStream(ctx, streamID, 0)
	if err != nil {
		return nil, err
	}
	return payment.Rehydrate(
		history,
		payment.WithClock(o.clock),
		payment.WithIDGenerator(o.idGen),
	)
}

// HandleWalletEvent consumes wallets.events.v1 to drive saga steps.
func (o *PaymentOrchestrator) HandleWalletEvent(ctx context.Context, env events.Envelope) error {
	if o.inbox != nil && env.EventID != "" {
		dup, err := o.inbox.TryMarkProcessed(ctx, walletConsumerGroup, env.EventID)
		if err != nil {
			return err
		}
		if dup {
			return nil
		}
	}

	switch env.Type {
	case events.EventFundsReserved:
		d, err := events.DecodePayload[events.FundsReservedPayload](env)
		if err != nil {
			return err
		}
		meta := events.Metadata{CorrelationID: d.PaymentID, CausationID: env.EventID}
		p, transitioned, err := o.mutatePayment(ctx, d.PaymentID, func(p *payment.Payment) error {
			if p.Status() != payment.StatusPending {
				return nil
			}
			return p.MarkFundsReserved(d.AvailableAfter, meta)
		})
		if err != nil {
			return err
		}
		o.notifyWaiter(d.PaymentID, reserveOutcome{reserved: true, availableAfter: d.AvailableAfter})
		if transitioned {
			return o.emitSubmitPayment(ctx, p, env.EventID)
		}
		return nil

	case events.EventFundsReservationRejected:
		d, err := events.DecodePayload[events.FundsReservationRejectedPayload](env)
		if err != nil {
			return err
		}
		meta := events.Metadata{CorrelationID: d.PaymentID, CausationID: env.EventID}
		_, _, err = o.mutatePayment(ctx, d.PaymentID, func(p *payment.Payment) error {
			if p.Status() != payment.StatusPending {
				return nil
			}
			return p.MarkRejected(d.Reason, meta)
		})
		if err != nil {
			return err
		}
		o.notifyWaiter(d.PaymentID, reserveOutcome{reserved: false, reason: d.Reason})
		return nil

	case events.EventFundsCaptured:
		d, err := events.DecodePayload[events.FundsCapturedPayload](env)
		if err != nil {
			return err
		}
		providerRef := o.popApprovedRef(d.PaymentID)
		meta := events.Metadata{CorrelationID: d.PaymentID, CausationID: env.EventID}
		_, _, err = o.mutatePayment(ctx, d.PaymentID, func(p *payment.Payment) error {
			if p.Status() == payment.StatusCompleted || p.Status() == payment.StatusRefunded {
				return nil
			}
			return p.MarkCompleted(providerRef, meta)
		})
		return err

	case events.EventFundsReleased:
		d, err := events.DecodePayload[events.FundsReleasedPayload](env)
		if err != nil {
			return err
		}
		meta := events.Metadata{CorrelationID: d.PaymentID, CausationID: env.EventID}
		_, _, err = o.mutatePayment(ctx, d.PaymentID, func(p *payment.Payment) error {
			if p.Status() == payment.StatusFailed || p.Status() == payment.StatusRejected {
				return nil
			}
			return p.MarkFailed(d.Reason, meta)
		})
		return err

	case events.EventFundsRefunded:
		d, err := events.DecodePayload[events.FundsRefundedPayload](env)
		if err != nil {
			return err
		}
		meta := events.Metadata{CorrelationID: d.PaymentID, CausationID: env.EventID}
		_, _, err = o.mutatePayment(ctx, d.PaymentID, func(p *payment.Payment) error {
			return p.RecordRefund(d.RefundID, d.Amount, meta)
		})
		return err

	default:
		return nil
	}
}

// HandleGatewayEvent consumes gateway.events.v1 to react to provider outcomes.
func (o *PaymentOrchestrator) HandleGatewayEvent(ctx context.Context, env events.Envelope) error {
	if o.inbox != nil && env.EventID != "" {
		dup, err := o.inbox.TryMarkProcessed(ctx, gatewayConsumerGroup, env.EventID)
		if err != nil {
			return err
		}
		if dup {
			return nil
		}
	}

	switch env.Type {
	case events.EventGatewaySubmissionAccepted:
		d, err := events.DecodePayload[events.GatewaySubmissionAcceptedPayload](env)
		if err != nil {
			return err
		}
		meta := events.Metadata{CorrelationID: d.PaymentID, CausationID: env.EventID}
		_, _, err = o.mutatePayment(ctx, d.PaymentID, func(p *payment.Payment) error {
			if p.Status() != payment.StatusFundsReserved {
				return nil
			}
			return p.MarkSubmitted(d.ProviderRef, meta)
		})
		return err

	case events.EventGatewaySubmissionNotSent:
		d, err := events.DecodePayload[events.GatewaySubmissionNotSentPayload](env)
		if err != nil {
			return err
		}
		p, err := o.Load(ctx, d.PaymentID)
		if err != nil {
			return err
		}
		if p.Status().IsTerminal() || p.Status() == payment.StatusCompleted {
			return nil
		}
		// Certainty of non-delivery -> safe to compensate by releasing held funds.
		return o.emitReleaseFunds(ctx, p, d.Reason, env.EventID)

	case events.EventGatewayOutcomeUnknown:
		d, err := events.DecodePayload[events.GatewayOutcomeUnknownPayload](env)
		if err != nil {
			return err
		}
		meta := events.Metadata{CorrelationID: d.PaymentID, CausationID: env.EventID}
		_, _, err = o.mutatePayment(ctx, d.PaymentID, func(p *payment.Payment) error {
			if p.Status() == payment.StatusGatewayUnknown || p.Status() == payment.StatusCompleted || p.Status().IsTerminal() {
				return nil
			}
			return p.MarkGatewayUnknown(d.Reason, meta)
		})
		return err

	case events.EventGatewayPaymentApproved:
		d, err := events.DecodePayload[events.GatewayPaymentApprovedPayload](env)
		if err != nil {
			return err
		}
		p, err := o.Load(ctx, d.PaymentID)
		if err != nil {
			return err
		}
		if p.Status() == payment.StatusCompleted || p.Status() == payment.StatusRefunded {
			return nil
		}
		o.storeApprovedRef(d.PaymentID, d.ProviderRef)
		// Point of no return reached -> advance forward by capturing held funds.
		return o.emitCaptureFunds(ctx, p, env.EventID)

	case events.EventGatewayPaymentDeclined:
		d, err := events.DecodePayload[events.GatewayPaymentDeclinedPayload](env)
		if err != nil {
			return err
		}
		p, err := o.Load(ctx, d.PaymentID)
		if err != nil {
			return err
		}
		if p.Status().IsTerminal() || p.Status() == payment.StatusCompleted {
			return nil
		}
		// Negative evidence from provider -> compensate by releasing held funds.
		return o.emitReleaseFunds(ctx, p, d.Reason, env.EventID)

	case events.EventGatewayStatusPending:
		d, err := events.DecodePayload[events.GatewayStatusPendingPayload](env)
		if err != nil {
			return err
		}
		o.logger.Info("reconciliation check still pending at provider",
			"correlation_id", d.PaymentID,
			"provider", d.Provider,
		)
		return nil

	case events.EventGatewayRefundApproved:
		d, err := events.DecodePayload[events.GatewayRefundApprovedPayload](env)
		if err != nil {
			return err
		}
		p, err := o.Load(ctx, d.PaymentID)
		if err != nil {
			return err
		}
		return o.emitRefundFunds(ctx, p, d.RefundID, d.Amount, env.EventID)

	case events.EventGatewayRefundDeclined:
		d, err := events.DecodePayload[events.GatewayRefundDeclinedPayload](env)
		if err != nil {
			return err
		}
		o.logger.Warn("gateway declined refund request",
			"correlation_id", d.PaymentID,
			"refund_id", d.RefundID,
			"reason", d.Reason,
		)
		return nil

	default:
		return nil
	}
}

// TriggerReconciliation dispatches CheckPaymentStatus for an uncertain or stalled payment.
func (o *PaymentOrchestrator) TriggerReconciliation(ctx context.Context, paymentID string) error {
	p, err := o.Load(ctx, paymentID)
	if err != nil {
		return err
	}
	if p.Status() == payment.StatusSubmitted {
		meta := events.Metadata{CorrelationID: paymentID}
		p, _, err = o.mutatePayment(ctx, paymentID, func(cur *payment.Payment) error {
			if cur.Status() != payment.StatusSubmitted {
				return nil
			}
			return cur.MarkGatewayUnknown("WEBHOOK_TIMEOUT", meta)
		})
		if err != nil {
			return err
		}
	}
	if p.Status() != payment.StatusGatewayUnknown {
		return fmt.Errorf("%w: cannot reconcile payment in status %s", payment.ErrInvalidTransition, p.Status())
	}
	return o.emitCheckPaymentStatus(ctx, p, "")
}

// ExpireReconciliationWindow moves a GATEWAY_UNKNOWN payment into MANUAL_REVIEW while
// keeping funds safely held until operations resolves it.
func (o *PaymentOrchestrator) ExpireReconciliationWindow(ctx context.Context, paymentID string) error {
	meta := events.Metadata{CorrelationID: paymentID}
	_, _, err := o.mutatePayment(ctx, paymentID, func(p *payment.Payment) error {
		return p.MarkManualReview(events.ReasonReconcileExpired, meta)
	})
	return err
}

// ResolveManualReview resolves a MANUAL_REVIEW payment based on provider settlement evidence.
func (o *PaymentOrchestrator) ResolveManualReview(ctx context.Context, paymentID string, approved bool, providerRef, reason string) error {
	p, err := o.Load(ctx, paymentID)
	if err != nil {
		return err
	}
	if p.Status() != payment.StatusManualReview {
		return fmt.Errorf("%w: payment is in status %s, expected MANUAL_REVIEW", payment.ErrInvalidTransition, p.Status())
	}
	if approved {
		o.storeApprovedRef(paymentID, providerRef)
		return o.emitCaptureFunds(ctx, p, "")
	}
	if reason == "" {
		reason = "MANUAL_REVIEW_DECLINED"
	}
	return o.emitReleaseFunds(ctx, p, reason, "")
}

// RequestRefund validates refund invariants on a COMPLETED payment and emits RefundPayment.
func (o *PaymentOrchestrator) RequestRefund(ctx context.Context, paymentID, refundID string, amount events.Money) error {
	if refundID == "" {
		return payment.ErrInvalidRefundID
	}
	money, err := events.NewMoney(amount.Amount, amount.Currency)
	if err != nil {
		return err
	}

	p, err := o.Load(ctx, paymentID)
	if err != nil {
		return err
	}
	if p.Status() != payment.StatusCompleted {
		return fmt.Errorf("%w: cannot refund payment in status %s", payment.ErrInvalidTransition, p.Status())
	}
	if money.Currency != p.Amount().Currency {
		return fmt.Errorf("%w: payment=%s refund=%s", payment.ErrCurrencyMismatch, p.Amount().Currency, money.Currency)
	}
	if money.Amount > p.RefundableAmount() {
		return fmt.Errorf("%w: refundable=%d requested=%d", payment.ErrRefundExceedsCaptured, p.RefundableAmount(), money.Amount)
	}

	return o.emitRefundPayment(ctx, p, refundID, money)
}

// RecoverActiveSagas scans all persisted Payment streams in the Event Store,
// rehydrates each aggregate, and re-emits the pending idempotent command for
// every active saga state (PENDING, FUNDS_RESERVED, SUBMITTED, GATEWAY_UNKNOWN).
func (o *PaymentOrchestrator) RecoverActiveSagas(ctx context.Context) (RecoverySummary, error) {
	summary := RecoverySummary{ByStatus: make(map[payment.Status]int)}

	prefix := events.AggregateTypePayment + "-"
	streamIDs, err := o.store.ListStreamsByPrefix(ctx, prefix)
	if err != nil {
		return summary, err
	}

	for _, streamID := range streamIDs {
		paymentID := strings.TrimPrefix(streamID, prefix)
		p, err := o.Load(ctx, paymentID)
		if err != nil {
			return summary, fmt.Errorf("rehydrate %s during recovery: %w", streamID, err)
		}
		summary.Scanned++

		if !p.Status().IsActiveSaga() {
			continue
		}

		switch p.Status() {
		case payment.StatusPending:
			if err := o.emitReserveFunds(ctx, p, "recovery"); err != nil {
				return summary, err
			}
		case payment.StatusFundsReserved:
			if err := o.emitSubmitPayment(ctx, p, "recovery"); err != nil {
				return summary, err
			}
		case payment.StatusSubmitted, payment.StatusGatewayUnknown:
			if err := o.emitCheckPaymentStatus(ctx, p, "recovery"); err != nil {
				return summary, err
			}
		}

		summary.Resumed++
		summary.ByStatus[p.Status()]++
		o.logger.Info("recovered active payment saga",
			"payment_id", p.ID(),
			"status", string(p.Status()),
		)
	}

	return summary, nil
}

// --- Internal helpers ---

func (o *PaymentOrchestrator) mutatePayment(
	ctx context.Context,
	paymentID string,
	mutate func(*payment.Payment) error,
) (*payment.Payment, bool, error) {
	var lastErr error
	for attempt := 0; attempt < o.maxLockRetries; attempt++ {
		p, err := o.Load(ctx, paymentID)
		if err != nil {
			return nil, false, err
		}
		if err := mutate(p); err != nil {
			return nil, false, err
		}

		uncommitted := p.UncommittedEvents()
		if len(uncommitted) == 0 {
			return p, false, nil
		}

		err = o.store.Append(ctx, p.StreamID(), p.BaseVersion(), uncommitted...)
		if err == nil {
			p.ClearUncommitted()
			_ = o.syncPaymentView(ctx, p, uncommitted[len(uncommitted)-1].OccurredAt)
			if err := o.publishPaymentEvents(ctx, p.ID(), uncommitted); err != nil {
				return nil, false, err
			}
			return p, true, nil
		}
		if !errors.Is(err, ports.ErrVersionConflict) {
			return nil, false, err
		}

		lastErr = err
		if o.metrics != nil {
			o.metrics.RecordOptimisticLockRetry(events.AggregateTypePayment)
		}
	}
	return nil, false, fmt.Errorf("payment %s failed after %d lock retries: %w", paymentID, o.maxLockRetries, lastErr)
}

func (o *PaymentOrchestrator) syncPaymentView(ctx context.Context, p *payment.Payment, at time.Time) error {
	if o.views == nil {
		return nil
	}
	return o.views.Upsert(ctx, toPaymentView(p, at))
}

func toPaymentView(p *payment.Payment, at time.Time) ports.PaymentView {
	return ports.PaymentView{
		PaymentID:     p.ID(),
		WalletID:      p.WalletID(),
		UserID:        p.UserID(),
		ServiceID:     p.ServiceID(),
		Provider:      p.Provider(),
		Status:        string(p.Status()),
		ProviderRef:   p.ProviderRef(),
		FailureReason: p.FailureReason(),
		Amount:        p.Amount().Amount,
		RefundedTotal: p.RefundedTotal(),
		Currency:      p.Amount().Currency,
		Version:       p.Version(),
		UpdatedAt:     at,
	}
}

func (o *PaymentOrchestrator) publishPaymentEvents(ctx context.Context, paymentID string, evts []events.Envelope) error {
	if o.publisher == nil {
		return nil
	}
	for _, env := range evts {
		if err := o.publisher.Publish(ctx, events.TopicPaymentsEvents, paymentID, env); err != nil {
			return err
		}
	}
	return nil
}

func (o *PaymentOrchestrator) emitReserveFunds(ctx context.Context, p *payment.Payment, causationID string) error {
	env, err := events.NewEnvelope(
		events.CommandReserveFunds,
		events.AggregateTypeWallet,
		p.WalletID(),
		0,
		events.ReserveFundsCommand{
			PaymentID: p.ID(),
			WalletID:  p.WalletID(),
			Amount:    p.Amount(),
		},
		events.Metadata{CorrelationID: p.ID(), CausationID: causationID},
		o.clock,
		o.idGen,
	)
	if err != nil {
		return err
	}
	return o.publisher.Publish(ctx, events.TopicWalletsCommands, p.WalletID(), env)
}

func (o *PaymentOrchestrator) emitCaptureFunds(ctx context.Context, p *payment.Payment, causationID string) error {
	env, err := events.NewEnvelope(
		events.CommandCaptureFunds,
		events.AggregateTypeWallet,
		p.WalletID(),
		0,
		events.CaptureFundsCommand{
			PaymentID: p.ID(),
			WalletID:  p.WalletID(),
			Amount:    p.Amount(),
		},
		events.Metadata{CorrelationID: p.ID(), CausationID: causationID},
		o.clock,
		o.idGen,
	)
	if err != nil {
		return err
	}
	return o.publisher.Publish(ctx, events.TopicWalletsCommands, p.WalletID(), env)
}

func (o *PaymentOrchestrator) emitReleaseFunds(ctx context.Context, p *payment.Payment, reason, causationID string) error {
	env, err := events.NewEnvelope(
		events.CommandReleaseFunds,
		events.AggregateTypeWallet,
		p.WalletID(),
		0,
		events.ReleaseFundsCommand{
			PaymentID: p.ID(),
			WalletID:  p.WalletID(),
			Reason:    reason,
		},
		events.Metadata{CorrelationID: p.ID(), CausationID: causationID},
		o.clock,
		o.idGen,
	)
	if err != nil {
		return err
	}
	return o.publisher.Publish(ctx, events.TopicWalletsCommands, p.WalletID(), env)
}

func (o *PaymentOrchestrator) emitRefundFunds(ctx context.Context, p *payment.Payment, refundID string, amount events.Money, causationID string) error {
	env, err := events.NewEnvelope(
		events.CommandRefundFunds,
		events.AggregateTypeWallet,
		p.WalletID(),
		0,
		events.RefundFundsCommand{
			PaymentID: p.ID(),
			RefundID:  refundID,
			WalletID:  p.WalletID(),
			Amount:    amount,
		},
		events.Metadata{CorrelationID: p.ID(), CausationID: causationID},
		o.clock,
		o.idGen,
	)
	if err != nil {
		return err
	}
	return o.publisher.Publish(ctx, events.TopicWalletsCommands, p.WalletID(), env)
}

func (o *PaymentOrchestrator) emitSubmitPayment(ctx context.Context, p *payment.Payment, causationID string) error {
	env, err := events.NewEnvelope(
		events.CommandSubmitPayment,
		events.AggregateTypeGateway,
		p.ID(),
		0,
		events.SubmitPaymentCommand{
			PaymentID: p.ID(),
			Provider:  p.Provider(),
			ServiceID: p.ServiceID(),
			Amount:    p.Amount(),
		},
		events.Metadata{CorrelationID: p.ID(), CausationID: causationID},
		o.clock,
		o.idGen,
	)
	if err != nil {
		return err
	}
	return o.publisher.Publish(ctx, events.TopicGatewayCommands, p.ID(), env)
}

func (o *PaymentOrchestrator) emitCheckPaymentStatus(ctx context.Context, p *payment.Payment, causationID string) error {
	env, err := events.NewEnvelope(
		events.CommandCheckPaymentStatus,
		events.AggregateTypeGateway,
		p.ID(),
		0,
		events.CheckPaymentStatusCommand{
			PaymentID: p.ID(),
			Provider:  p.Provider(),
		},
		events.Metadata{CorrelationID: p.ID(), CausationID: causationID},
		o.clock,
		o.idGen,
	)
	if err != nil {
		return err
	}
	return o.publisher.Publish(ctx, events.TopicGatewayCommands, p.ID(), env)
}

func (o *PaymentOrchestrator) emitRefundPayment(ctx context.Context, p *payment.Payment, refundID string, amount events.Money) error {
	env, err := events.NewEnvelope(
		events.CommandRefundPayment,
		events.AggregateTypeGateway,
		p.ID(),
		0,
		events.RefundPaymentCommand{
			PaymentID:   p.ID(),
			RefundID:    refundID,
			Provider:    p.Provider(),
			ProviderRef: p.ProviderRef(),
			Amount:      amount,
		},
		events.Metadata{CorrelationID: p.ID()},
		o.clock,
		o.idGen,
	)
	if err != nil {
		return err
	}
	return o.publisher.Publish(ctx, events.TopicGatewayCommands, p.ID(), env)
}

func (o *PaymentOrchestrator) registerWaiter(paymentID string) chan reserveOutcome {
	ch := make(chan reserveOutcome, 1)
	o.waitMu.Lock()
	o.waiters[paymentID] = ch
	o.waitMu.Unlock()
	return ch
}

func (o *PaymentOrchestrator) unregisterWaiter(paymentID string) {
	o.waitMu.Lock()
	delete(o.waiters, paymentID)
	o.waitMu.Unlock()
}

func (o *PaymentOrchestrator) notifyWaiter(paymentID string, outcome reserveOutcome) {
	o.waitMu.Lock()
	ch, ok := o.waiters[paymentID]
	o.waitMu.Unlock()
	if ok {
		select {
		case ch <- outcome:
		default:
		}
	}
}

func (o *PaymentOrchestrator) storeApprovedRef(paymentID, providerRef string) {
	if providerRef == "" {
		return
	}
	o.refMu.Lock()
	o.approvedRefs[paymentID] = providerRef
	o.refMu.Unlock()
}

func (o *PaymentOrchestrator) popApprovedRef(paymentID string) string {
	o.refMu.Lock()
	ref := o.approvedRefs[paymentID]
	delete(o.approvedRefs, paymentID)
	o.refMu.Unlock()
	return ref
}

func hashInitiateRequest(req InitiatePaymentRequest) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%s|%d|%s",
		req.WalletID, req.ServiceID, req.Provider, req.Amount.Amount, strings.ToUpper(req.Amount.Currency))))
	return hex.EncodeToString(sum[:])
}
