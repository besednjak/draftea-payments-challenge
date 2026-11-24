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
	"math/rand/v2"
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
	LockRetryMin    time.Duration
	LockRetryMax    time.Duration
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
	lockRetryMin    time.Duration
	lockRetryMax    time.Duration
	clock           events.Clock
	idGen           events.IDGenerator

	waitMu  sync.Mutex
	waiters map[string]chan reserveOutcome
}

// NewPaymentOrchestrator constructs a PaymentOrchestrator.
func NewPaymentOrchestrator(cfg Config) *PaymentOrchestrator {
	if cfg.SyncReserveWait <= 0 {
		cfg.SyncReserveWait = 2 * time.Second
	}
	if cfg.MaxLockRetries <= 0 {
		cfg.MaxLockRetries = 5
	}
	if cfg.LockRetryMin <= 0 {
		cfg.LockRetryMin = 2 * time.Millisecond
	}
	if cfg.LockRetryMax <= 0 {
		cfg.LockRetryMax = 15 * time.Millisecond
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
		lockRetryMin:    cfg.LockRetryMin,
		lockRetryMax:    cfg.LockRetryMax,
		clock:           cfg.Clock,
		idGen:           cfg.IDGenerator,
		waiters:         make(map[string]chan reserveOutcome),
	}
}

// InitiatePayment validates idempotency, persists PaymentInitiated, dispatches ReserveFunds,
// and waits up to SyncReserveWait (default 2s) to return an immediate 422 on insufficient funds
// or 202 Accepted with the updated available balance.
func (o *PaymentOrchestrator) InitiatePayment(ctx context.Context, req InitiatePaymentRequest) (InitiatePaymentResult, error) {
	requestHash := hashInitiateRequest(req)
	idempotencyReserved := false

	if o.idempotency != nil && req.IdempotencyKey != "" {
		existing, reserved, err := o.idempotency.Reserve(ctx, req.UserID, req.IdempotencyKey, requestHash, 24*time.Hour)
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
		idempotencyReserved = true
	}

	paymentID := req.PaymentID
	if paymentID == "" {
		paymentID = "pay_" + o.idGen()
	}
	ctx = ports.WithCorrelation(ctx, paymentID, paymentID, req.WalletID)
	cleanupCtx := context.WithoutCancel(ctx)

	initiated, err := payment.Initiate(
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
		if idempotencyReserved {
			if delErr := o.idempotency.Delete(cleanupCtx, req.UserID, req.IdempotencyKey); delErr != nil {
				ports.LoggerFromContext(ctx, o.logger).Warn("delete idempotency key failed", "error", delErr.Error())
			}
		}
		return InitiatePaymentResult{}, err
	}

	uncommitted := initiated.UncommittedEvents()
	if err := o.store.Append(ctx, initiated.StreamID(), 0, uncommitted...); err != nil {
		if idempotencyReserved {
			if delErr := o.idempotency.Delete(cleanupCtx, req.UserID, req.IdempotencyKey); delErr != nil {
				ports.LoggerFromContext(ctx, o.logger).Warn("delete idempotency key failed", "error", delErr.Error())
			}
		}
		return InitiatePaymentResult{}, err
	}
	initiated.ClearUncommitted()
	o.syncPaymentViewLogged(ctx, initiated, uncommitted[0].OccurredAt)

	result := InitiatePaymentResult{
		PaymentID:  initiated.ID(),
		Status:     payment.StatusPending,
		HTTPStatus: http.StatusAccepted,
	}

	// PaymentInitiated is durable from here on, so the Idempotency-Key is always completed
	// (never deleted) using WithoutCancel; it can never stay stuck IN_PROGRESS for 24 hours.
	completeIdempotency := func(response InitiatePaymentResult) {
		if !idempotencyReserved {
			return
		}
		encoded, err := json.Marshal(response)
		if err != nil {
			ports.LoggerFromContext(ctx, o.logger).Warn("marshal idempotency response failed", "error", err.Error())
			return
		}
		if err := o.idempotency.Complete(cleanupCtx, req.UserID, req.IdempotencyKey, initiated.ID(), encoded); err != nil {
			ports.LoggerFromContext(ctx, o.logger).Warn("complete idempotency key failed", "error", err.Error())
		}
	}

	if err := o.publishPaymentEvents(ctx, initiated.ID(), uncommitted); err != nil {
		completeIdempotency(result)
		return InitiatePaymentResult{}, err
	}

	// Register waiter before dispatching ReserveFunds to eliminate missed signals.
	waitCh := o.registerWaiter(initiated.ID())
	defer o.unregisterWaiter(initiated.ID())

	if err := o.publishCommand(
		ctx,
		events.TopicWalletsCommands,
		initiated.WalletID(),
		events.CommandReserveFunds,
		events.AggregateTypeWallet,
		initiated.WalletID(),
		initiated.ID(),
		uncommitted[0].EventID,
		events.ReserveFundsCommand{
			PaymentID: initiated.ID(),
			WalletID:  initiated.WalletID(),
			Amount:    initiated.Amount(),
		},
	); err != nil {
		completeIdempotency(result)
		return InitiatePaymentResult{}, err
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
	case <-ctx.Done():
	}

	completeIdempotency(result)

	return result, nil
}

// GetPayment queries the payment read model (falling back to Event Store rehydration).
func (o *PaymentOrchestrator) GetPayment(ctx context.Context, paymentID string) (ports.PaymentView, error) {
	if o.views != nil {
		return o.views.Get(ctx, paymentID)
	}
	loaded, err := o.Load(ctx, paymentID)
	if err != nil {
		return ports.PaymentView{}, err
	}
	return toPaymentView(loaded, o.clock().UTC()), nil
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
func (o *PaymentOrchestrator) HandleWalletEvent(ctx context.Context, env events.Envelope) (retErr error) {
	if o.inbox != nil && env.EventID != "" {
		alreadyProcessed, err := o.inbox.TryMarkProcessed(ctx, walletConsumerGroup, env.EventID)
		if err != nil {
			return err
		}
		if alreadyProcessed {
			return nil
		}
		defer func() {
			if retErr != nil {
				_ = o.inbox.UnmarkProcessed(ctx, walletConsumerGroup, env.EventID)
			}
		}()
	}

	switch env.Type {
	case events.EventFundsReserved:
		payload, err := events.DecodePayload[events.FundsReservedPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		ctx = ports.WithCorrelation(ctx, payload.PaymentID, payload.PaymentID, env.AggregateID)
		meta := events.Metadata{CorrelationID: payload.PaymentID, CausationID: env.EventID}
		updated, transitioned, err := o.mutatePayment(ctx, payload.PaymentID, func(p *payment.Payment) error {
			if p.Status() != payment.StatusPending {
				return nil
			}
			return p.MarkFundsReserved(payload.AvailableAfter, meta)
		})
		if err != nil {
			return wrapPermanentDomainError(err)
		}
		if updated.Status() == payment.StatusRejected || updated.Status() == payment.StatusFailed {
			// Defensive compensation: a late reservation for a closed payment must not leave orphaned held funds.
			return o.publishCommand(
				ctx,
				events.TopicWalletsCommands,
				updated.WalletID(),
				events.CommandReleaseFunds,
				events.AggregateTypeWallet,
				updated.WalletID(),
				updated.ID(),
				env.EventID,
				events.ReleaseFundsCommand{
					PaymentID: updated.ID(),
					WalletID:  updated.WalletID(),
					Reason:    "ORPHAN_RESERVATION_COMPENSATION",
				},
			)
		}
		o.notifyWaiter(payload.PaymentID, reserveOutcome{reserved: true, availableAfter: payload.AvailableAfter})
		if !transitioned && updated.Status() == payment.StatusFundsReserved {
			if err := o.republishPersistedPaymentEvent(ctx, updated, matchEventType(events.EventPaymentFundsReserved)); err != nil {
				return err
			}
		}
		if transitioned || updated.Status() == payment.StatusFundsReserved {
			return o.publishCommand(
				ctx,
				events.TopicGatewayCommands,
				updated.ID(),
				events.CommandSubmitPayment,
				events.AggregateTypeGateway,
				updated.ID(),
				updated.ID(),
				env.EventID,
				events.SubmitPaymentCommand{
					PaymentID: updated.ID(),
					Provider:  updated.Provider(),
					ServiceID: updated.ServiceID(),
					Amount:    updated.Amount(),
				},
			)
		}
		return nil

	case events.EventFundsReservationRejected:
		payload, err := events.DecodePayload[events.FundsReservationRejectedPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		ctx = ports.WithCorrelation(ctx, payload.PaymentID, payload.PaymentID, env.AggregateID)
		meta := events.Metadata{CorrelationID: payload.PaymentID, CausationID: env.EventID}
		updated, transitioned, err := o.mutatePayment(ctx, payload.PaymentID, func(p *payment.Payment) error {
			if p.Status() != payment.StatusPending {
				return nil
			}
			return p.MarkRejected(payload.Reason, meta)
		})
		if err != nil {
			return wrapPermanentDomainError(err)
		}
		o.notifyWaiter(payload.PaymentID, reserveOutcome{reserved: false, reason: payload.Reason})
		if !transitioned && updated.Status() == payment.StatusRejected {
			return o.republishPersistedPaymentEvent(ctx, updated, matchEventType(events.EventPaymentRejected))
		}
		return nil

	case events.EventFundsCaptured:
		payload, err := events.DecodePayload[events.FundsCapturedPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		ctx = ports.WithCorrelation(ctx, payload.PaymentID, payload.PaymentID, env.AggregateID)
		meta := events.Metadata{CorrelationID: payload.PaymentID, CausationID: env.EventID}
		updated, transitioned, err := o.mutatePayment(ctx, payload.PaymentID, func(p *payment.Payment) error {
			if p.Status() == payment.StatusCompleted || p.Status() == payment.StatusRefunded {
				return nil
			}
			return p.MarkCompleted(payload.ProviderRef, meta)
		})
		if err != nil {
			return wrapPermanentDomainError(err)
		}
		if !transitioned && (updated.Status() == payment.StatusCompleted || updated.Status() == payment.StatusRefunded) {
			return o.republishPersistedPaymentEvent(ctx, updated, matchEventType(events.EventPaymentCompleted))
		}
		return nil

	case events.EventFundsReleased:
		payload, err := events.DecodePayload[events.FundsReleasedPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		ctx = ports.WithCorrelation(ctx, payload.PaymentID, payload.PaymentID, env.AggregateID)
		meta := events.Metadata{CorrelationID: payload.PaymentID, CausationID: env.EventID}
		updated, transitioned, err := o.mutatePayment(ctx, payload.PaymentID, func(p *payment.Payment) error {
			if p.Status() == payment.StatusFailed || p.Status() == payment.StatusRejected {
				return nil
			}
			return p.MarkFailed(payload.Reason, meta)
		})
		if err != nil {
			return wrapPermanentDomainError(err)
		}
		if !transitioned && updated.Status() == payment.StatusFailed {
			return o.republishPersistedPaymentEvent(ctx, updated, matchEventType(events.EventPaymentFailed))
		}
		return nil

	case events.EventFundsRefunded:
		payload, err := events.DecodePayload[events.FundsRefundedPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		ctx = ports.WithCorrelation(ctx, payload.PaymentID, payload.PaymentID, env.AggregateID)
		meta := events.Metadata{CorrelationID: payload.PaymentID, CausationID: env.EventID}
		updated, transitioned, err := o.mutatePayment(ctx, payload.PaymentID, func(p *payment.Payment) error {
			return p.RecordRefund(payload.RefundID, payload.Amount, meta)
		})
		if err != nil {
			return wrapPermanentDomainError(err)
		}
		if !transitioned {
			return o.republishPersistedPaymentEvent(ctx, updated, matchRefundEvent(payload.RefundID))
		}
		return nil

	default:
		return nil
	}
}

// HandleGatewayEvent consumes gateway.events.v1 to react to provider outcomes.
func (o *PaymentOrchestrator) HandleGatewayEvent(ctx context.Context, env events.Envelope) (retErr error) {
	if o.inbox != nil && env.EventID != "" {
		alreadyProcessed, err := o.inbox.TryMarkProcessed(ctx, gatewayConsumerGroup, env.EventID)
		if err != nil {
			return err
		}
		if alreadyProcessed {
			return nil
		}
		defer func() {
			if retErr != nil {
				_ = o.inbox.UnmarkProcessed(ctx, gatewayConsumerGroup, env.EventID)
			}
		}()
	}

	switch env.Type {
	case events.EventGatewaySubmissionAccepted:
		payload, err := events.DecodePayload[events.GatewaySubmissionAcceptedPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		ctx = ports.WithCorrelation(ctx, payload.PaymentID, payload.PaymentID, "")
		meta := events.Metadata{CorrelationID: payload.PaymentID, CausationID: env.EventID}
		updated, transitioned, err := o.mutatePayment(ctx, payload.PaymentID, func(p *payment.Payment) error {
			if p.Status() != payment.StatusFundsReserved {
				return nil
			}
			return p.MarkSubmitted(payload.ProviderRef, meta)
		})
		if err != nil {
			return wrapPermanentDomainError(err)
		}
		if !transitioned && updated.Status() == payment.StatusSubmitted {
			return o.republishPersistedPaymentEvent(ctx, updated, matchEventType(events.EventPaymentSubmitted))
		}
		return nil

	case events.EventGatewaySubmissionNotSent:
		payload, err := events.DecodePayload[events.GatewaySubmissionNotSentPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		loaded, err := o.Load(ctx, payload.PaymentID)
		if err != nil {
			return err
		}
		// A late or duplicate NotSent must never release funds once the payment progressed past
		// FUNDS_RESERVED (SUBMITTED / GATEWAY_UNKNOWN may already be processed by the provider).
		if loaded.Status() != payment.StatusFundsReserved {
			return nil
		}
		ctx = ports.WithCorrelation(ctx, loaded.ID(), loaded.ID(), loaded.WalletID())
		return o.publishCommand(
			ctx,
			events.TopicWalletsCommands,
			loaded.WalletID(),
			events.CommandReleaseFunds,
			events.AggregateTypeWallet,
			loaded.WalletID(),
			loaded.ID(),
			env.EventID,
			events.ReleaseFundsCommand{
				PaymentID: loaded.ID(),
				WalletID:  loaded.WalletID(),
				Reason:    payload.Reason,
			},
		)

	case events.EventGatewayOutcomeUnknown:
		payload, err := events.DecodePayload[events.GatewayOutcomeUnknownPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		ctx = ports.WithCorrelation(ctx, payload.PaymentID, payload.PaymentID, "")
		meta := events.Metadata{CorrelationID: payload.PaymentID, CausationID: env.EventID}
		updated, transitioned, err := o.mutatePayment(ctx, payload.PaymentID, func(p *payment.Payment) error {
			if p.Status() == payment.StatusGatewayUnknown || p.Status() == payment.StatusCompleted || p.Status().IsTerminal() {
				return nil
			}
			return p.MarkGatewayUnknown(payload.Reason, meta)
		})
		if err != nil {
			return wrapPermanentDomainError(err)
		}
		if !transitioned && updated.Status() == payment.StatusGatewayUnknown {
			return o.republishPersistedPaymentEvent(ctx, updated, matchEventType(events.EventPaymentGatewayUnknown))
		}
		return nil

	case events.EventGatewayPaymentApproved:
		payload, err := events.DecodePayload[events.GatewayPaymentApprovedPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		loaded, err := o.Load(ctx, payload.PaymentID)
		if err != nil {
			return err
		}
		if loaded.Status().IsTerminal() || loaded.Status() == payment.StatusCompleted {
			return nil
		}
		providerRef := payload.ProviderRef
		if providerRef == "" {
			providerRef = loaded.ProviderRef()
		}
		ctx = ports.WithCorrelation(ctx, loaded.ID(), loaded.ID(), loaded.WalletID())
		return o.publishCommand(
			ctx,
			events.TopicWalletsCommands,
			loaded.WalletID(),
			events.CommandCaptureFunds,
			events.AggregateTypeWallet,
			loaded.WalletID(),
			loaded.ID(),
			env.EventID,
			events.CaptureFundsCommand{
				PaymentID:   loaded.ID(),
				WalletID:    loaded.WalletID(),
				ProviderRef: providerRef,
				Amount:      loaded.Amount(),
			},
		)

	case events.EventGatewayPaymentDeclined:
		payload, err := events.DecodePayload[events.GatewayPaymentDeclinedPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		loaded, err := o.Load(ctx, payload.PaymentID)
		if err != nil {
			return err
		}
		if loaded.Status().IsTerminal() || loaded.Status() == payment.StatusCompleted {
			return nil
		}
		ctx = ports.WithCorrelation(ctx, loaded.ID(), loaded.ID(), loaded.WalletID())
		return o.publishCommand(
			ctx,
			events.TopicWalletsCommands,
			loaded.WalletID(),
			events.CommandReleaseFunds,
			events.AggregateTypeWallet,
			loaded.WalletID(),
			loaded.ID(),
			env.EventID,
			events.ReleaseFundsCommand{
				PaymentID: loaded.ID(),
				WalletID:  loaded.WalletID(),
				Reason:    payload.Reason,
			},
		)

	case events.EventGatewayStatusPending:
		payload, err := events.DecodePayload[events.GatewayStatusPendingPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		ctx = ports.WithCorrelation(ctx, payload.PaymentID, payload.PaymentID, "")
		ports.LoggerFromContext(ctx, o.logger).Info("reconciliation check still pending at provider",
			"provider", payload.Provider,
		)
		return nil

	case events.EventGatewayRefundApproved:
		payload, err := events.DecodePayload[events.GatewayRefundApprovedPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		loaded, err := o.Load(ctx, payload.PaymentID)
		if err != nil {
			return err
		}
		ctx = ports.WithCorrelation(ctx, loaded.ID(), loaded.ID(), loaded.WalletID())
		return o.publishCommand(
			ctx,
			events.TopicWalletsCommands,
			loaded.WalletID(),
			events.CommandRefundFunds,
			events.AggregateTypeWallet,
			loaded.WalletID(),
			loaded.ID(),
			env.EventID,
			events.RefundFundsCommand{
				PaymentID: loaded.ID(),
				RefundID:  payload.RefundID,
				WalletID:  loaded.WalletID(),
				Amount:    payload.Amount,
			},
		)

	case events.EventGatewayRefundDeclined:
		payload, err := events.DecodePayload[events.GatewayRefundDeclinedPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		ctx = ports.WithCorrelation(ctx, payload.PaymentID, payload.PaymentID, "")
		meta := events.Metadata{CorrelationID: payload.PaymentID, CausationID: env.EventID}
		if _, _, err := o.mutatePayment(ctx, payload.PaymentID, func(p *payment.Payment) error {
			return p.DeclineRefund(payload.RefundID, payload.Reason, meta)
		}); err != nil {
			return wrapPermanentDomainError(err)
		}
		ports.LoggerFromContext(ctx, o.logger).Warn("gateway declined refund request, released reservation",
			"refund_id", payload.RefundID,
			"reason", payload.Reason,
		)
		return nil

	default:
		return nil
	}
}

// TriggerReconciliation dispatches CheckPaymentStatus for an uncertain or stalled payment.
func (o *PaymentOrchestrator) TriggerReconciliation(ctx context.Context, paymentID string) error {
	loaded, err := o.Load(ctx, paymentID)
	if err != nil {
		return err
	}
	ctx = ports.WithCorrelation(ctx, loaded.ID(), loaded.ID(), loaded.WalletID())

	if loaded.Status() == payment.StatusSubmitted {
		meta := events.Metadata{CorrelationID: paymentID}
		loaded, _, err = o.mutatePayment(ctx, paymentID, func(current *payment.Payment) error {
			if current.Status() != payment.StatusSubmitted {
				return nil
			}
			return current.MarkGatewayUnknown("WEBHOOK_TIMEOUT", meta)
		})
		if err != nil {
			return err
		}
	}
	if loaded.Status() != payment.StatusGatewayUnknown {
		return fmt.Errorf("%w: cannot reconcile payment in status %s", payment.ErrInvalidTransition, loaded.Status())
	}
	return o.publishCommand(
		ctx,
		events.TopicGatewayCommands,
		loaded.ID(),
		events.CommandCheckPaymentStatus,
		events.AggregateTypeGateway,
		loaded.ID(),
		loaded.ID(),
		"",
		events.CheckPaymentStatusCommand{
			PaymentID: loaded.ID(),
			Provider:  loaded.Provider(),
		},
	)
}

// ExpireReconciliationWindow moves a GATEWAY_UNKNOWN payment into MANUAL_REVIEW while
// keeping funds safely held until operations resolves it.
func (o *PaymentOrchestrator) ExpireReconciliationWindow(ctx context.Context, paymentID string) error {
	ctx = ports.WithCorrelation(ctx, paymentID, paymentID, "")
	meta := events.Metadata{CorrelationID: paymentID}
	_, _, err := o.mutatePayment(ctx, paymentID, func(p *payment.Payment) error {
		return p.MarkManualReview(events.ReasonReconcileExpired, meta)
	})
	return err
}

// ResolveManualReview resolves a MANUAL_REVIEW payment based on provider settlement evidence.
func (o *PaymentOrchestrator) ResolveManualReview(ctx context.Context, paymentID string, approved bool, providerRef, reason string) error {
	loaded, err := o.Load(ctx, paymentID)
	if err != nil {
		return err
	}
	if loaded.Status() != payment.StatusManualReview {
		return fmt.Errorf("%w: payment is in status %s, expected MANUAL_REVIEW", payment.ErrInvalidTransition, loaded.Status())
	}
	ctx = ports.WithCorrelation(ctx, loaded.ID(), loaded.ID(), loaded.WalletID())

	if approved {
		effectiveRef := providerRef
		if effectiveRef == "" {
			effectiveRef = loaded.ProviderRef()
		}
		return o.publishCommand(
			ctx,
			events.TopicWalletsCommands,
			loaded.WalletID(),
			events.CommandCaptureFunds,
			events.AggregateTypeWallet,
			loaded.WalletID(),
			loaded.ID(),
			"",
			events.CaptureFundsCommand{
				PaymentID:   loaded.ID(),
				WalletID:    loaded.WalletID(),
				ProviderRef: effectiveRef,
				Amount:      loaded.Amount(),
			},
		)
	}
	if reason == "" {
		reason = "MANUAL_REVIEW_DECLINED"
	}
	return o.publishCommand(
		ctx,
		events.TopicWalletsCommands,
		loaded.WalletID(),
		events.CommandReleaseFunds,
		events.AggregateTypeWallet,
		loaded.WalletID(),
		loaded.ID(),
		"",
		events.ReleaseFundsCommand{
			PaymentID: loaded.ID(),
			WalletID:  loaded.WalletID(),
			Reason:    reason,
		},
	)
}

// RequestRefund reserves the refund amount on the Payment aggregate via optimistic locking
// before emitting RefundPayment so concurrent partial refunds cannot exceed capturedAmount.
func (o *PaymentOrchestrator) RequestRefund(ctx context.Context, paymentID, refundID string, amount events.Money) error {
	if refundID == "" {
		return payment.ErrInvalidRefundID
	}
	money, err := events.NewMoney(amount.Amount, amount.Currency)
	if err != nil {
		return err
	}

	ctx = ports.WithCorrelation(ctx, paymentID, paymentID, "")
	meta := events.Metadata{CorrelationID: paymentID}

	updated, transitioned, err := o.mutatePayment(ctx, paymentID, func(p *payment.Payment) error {
		return p.RequestRefund(refundID, money, meta)
	})
	if err != nil {
		return err
	}
	if !transitioned && !updated.HasPendingRefund(refundID) {
		return nil // Refund already settled.
	}

	return o.publishCommand(
		ctx,
		events.TopicGatewayCommands,
		updated.ID(),
		events.CommandRefundPayment,
		events.AggregateTypeGateway,
		updated.ID(),
		updated.ID(),
		"",
		events.RefundPaymentCommand{
			PaymentID:   updated.ID(),
			RefundID:    refundID,
			Provider:    updated.Provider(),
			ProviderRef: updated.ProviderRef(),
			Amount:      money,
		},
	)
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
		loaded, err := o.Load(ctx, paymentID)
		if err != nil {
			return summary, fmt.Errorf("rehydrate %s during recovery: %w", streamID, err)
		}
		summary.Scanned++

		if !loaded.Status().IsActiveSaga() {
			continue
		}

		sagaCtx := ports.WithCorrelation(ctx, loaded.ID(), loaded.ID(), loaded.WalletID())

		switch loaded.Status() {
		case payment.StatusPending:
			err = o.publishCommand(
				sagaCtx,
				events.TopicWalletsCommands,
				loaded.WalletID(),
				events.CommandReserveFunds,
				events.AggregateTypeWallet,
				loaded.WalletID(),
				loaded.ID(),
				"recovery",
				events.ReserveFundsCommand{
					PaymentID: loaded.ID(),
					WalletID:  loaded.WalletID(),
					Amount:    loaded.Amount(),
				},
			)
		case payment.StatusFundsReserved:
			err = o.publishCommand(
				sagaCtx,
				events.TopicGatewayCommands,
				loaded.ID(),
				events.CommandSubmitPayment,
				events.AggregateTypeGateway,
				loaded.ID(),
				loaded.ID(),
				"recovery",
				events.SubmitPaymentCommand{
					PaymentID: loaded.ID(),
					Provider:  loaded.Provider(),
					ServiceID: loaded.ServiceID(),
					Amount:    loaded.Amount(),
				},
			)
		case payment.StatusSubmitted, payment.StatusGatewayUnknown:
			err = o.publishCommand(
				sagaCtx,
				events.TopicGatewayCommands,
				loaded.ID(),
				events.CommandCheckPaymentStatus,
				events.AggregateTypeGateway,
				loaded.ID(),
				loaded.ID(),
				"recovery",
				events.CheckPaymentStatusCommand{
					PaymentID: loaded.ID(),
					Provider:  loaded.Provider(),
				},
			)
		}
		if err != nil {
			return summary, err
		}

		summary.Resumed++
		summary.ByStatus[loaded.Status()]++
		ports.LoggerFromContext(sagaCtx, o.logger).Info("recovered active payment saga",
			"status", string(loaded.Status()),
		)
	}

	return summary, nil
}

func (o *PaymentOrchestrator) mutatePayment(
	ctx context.Context,
	paymentID string,
	mutate func(*payment.Payment) error,
) (*payment.Payment, bool, error) {
	var lastErr error
	for attempt := 0; attempt < o.maxLockRetries; attempt++ {
		loaded, err := o.Load(ctx, paymentID)
		if err != nil {
			return nil, false, err
		}
		if err := mutate(loaded); err != nil {
			return nil, false, err
		}

		uncommitted := loaded.UncommittedEvents()
		if len(uncommitted) == 0 {
			o.syncPaymentViewLogged(ctx, loaded, o.clock().UTC())
			return loaded, false, nil
		}

		err = o.store.Append(ctx, loaded.StreamID(), loaded.BaseVersion(), uncommitted...)
		if err == nil {
			loaded.ClearUncommitted()
			o.syncPaymentViewLogged(ctx, loaded, uncommitted[len(uncommitted)-1].OccurredAt)
			if err := o.publishPaymentEvents(ctx, loaded.ID(), uncommitted); err != nil {
				return nil, false, err
			}
			return loaded, true, nil
		}
		if !errors.Is(err, ports.ErrVersionConflict) {
			return nil, false, err
		}

		lastErr = err
		if o.metrics != nil {
			o.metrics.RecordOptimisticLockRetry(events.AggregateTypePayment)
		}
		ports.LoggerFromContext(ctx, o.logger).Warn("payment optimistic lock conflict, reloading aggregate",
			"attempt", attempt+1,
			"max_retries", o.maxLockRetries,
		)

		if attempt+1 < o.maxLockRetries {
			jitter := o.lockRetryMin
			if o.lockRetryMax > o.lockRetryMin {
				jitter += time.Duration(rand.Int64N(int64(o.lockRetryMax - o.lockRetryMin)))
			}
			timer := time.NewTimer(jitter)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, false, ctx.Err()
			case <-timer.C:
			}
		}
	}
	return nil, false, fmt.Errorf("payment %s failed after %d lock retries: %w", paymentID, o.maxLockRetries, lastErr)
}

func (o *PaymentOrchestrator) republishPersistedPaymentEvent(
	ctx context.Context,
	p *payment.Payment,
	match func(events.Envelope) bool,
) error {
	if o.publisher == nil {
		return nil
	}
	history, err := o.store.LoadStream(ctx, p.StreamID(), 0)
	if err != nil {
		return err
	}
	for index := len(history) - 1; index >= 0; index-- {
		if match(history[index]) {
			return o.publisher.Publish(ctx, events.TopicPaymentsEvents, p.ID(), history[index])
		}
	}
	return nil
}

func (o *PaymentOrchestrator) syncPaymentViewLogged(ctx context.Context, p *payment.Payment, updatedAt time.Time) {
	if o.views == nil {
		return
	}
	if err := o.views.Upsert(ctx, toPaymentView(p, updatedAt)); err != nil {
		ports.LoggerFromContext(ctx, o.logger).Warn("sync payment view failed", "error", err.Error())
	}
}

func toPaymentView(p *payment.Payment, updatedAt time.Time) ports.PaymentView {
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
		CreatedAt:     p.CreatedAt(),
		UpdatedAt:     updatedAt,
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

func (o *PaymentOrchestrator) publishCommand(
	ctx context.Context,
	topic string,
	partitionKey string,
	commandType string,
	aggregateType string,
	aggregateID string,
	correlationID string,
	causationID string,
	payload any,
) error {
	env, err := events.NewEnvelope(
		commandType,
		aggregateType,
		aggregateID,
		0,
		payload,
		events.Metadata{CorrelationID: correlationID, CausationID: causationID},
		o.clock,
		o.idGen,
	)
	if err != nil {
		return err
	}
	return o.publisher.Publish(ctx, topic, partitionKey, env)
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
	waitCh, exists := o.waiters[paymentID]
	o.waitMu.Unlock()
	if exists {
		select {
		case waitCh <- outcome:
		default:
		}
	}
}

func hashInitiateRequest(req InitiatePaymentRequest) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%s|%d|%s",
		req.WalletID, req.ServiceID, req.Provider, req.Amount.Amount, strings.ToUpper(req.Amount.Currency))))
	return hex.EncodeToString(sum[:])
}

func wrapPermanentDomainError(err error) error {
	if err == nil {
		return nil
	}
	permanentErrors := []error{
		payment.ErrInvalidPaymentID,
		payment.ErrInvalidWalletID,
		payment.ErrInvalidUserID,
		payment.ErrInvalidServiceID,
		payment.ErrInvalidProvider,
		payment.ErrInvalidRefundID,
		payment.ErrPaymentAlreadyInitiated,
		payment.ErrPaymentNotInitiated,
		payment.ErrInvalidTransition,
		payment.ErrCurrencyMismatch,
		payment.ErrRefundExceedsCaptured,
		events.ErrInvalidAmount,
		events.ErrInvalidCurrency,
	}
	for _, target := range permanentErrors {
		if errors.Is(err, target) {
			return ports.NonRetryable(err)
		}
	}
	return err
}

func matchEventType(eventType string) func(events.Envelope) bool {
	return func(env events.Envelope) bool {
		return env.Type == eventType
	}
}

func matchRefundEvent(refundID string) func(events.Envelope) bool {
	return func(env events.Envelope) bool {
		if env.Type != events.EventPaymentRefunded {
			return false
		}
		payload, err := events.DecodePayload[events.PaymentRefundedPayload](env)
		return err == nil && payload.RefundID == refundID
	}
}
