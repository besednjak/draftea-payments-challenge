package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/besednjak/draftea-payments-challenge/internal/application/ports"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
)

// WebhookPayload represents a normalized provider callback body authenticated with HMAC.
type WebhookPayload struct {
	ProviderEventID string `json:"provider_event_id"`
	PaymentID       string `json:"payment_id"`
	Provider        string `json:"provider"`
	ProviderRef     string `json:"provider_ref,omitempty"`
	Status          string `json:"status"` // "APPROVED" or "DECLINED"
	Reason          string `json:"reason,omitempty"`
}

// AdapterConfig wires the Anti-Corruption Layer around an external PaymentGateway.
type AdapterConfig struct {
	Gateway       ports.PaymentGateway
	Publisher     ports.EventPublisher
	Inbox         ports.InboxStore
	Breaker       *CircuitBreaker
	Retry         RetryConfig
	BulkheadLimit int
	WebhookSecret string
	Logger        *slog.Logger
	Metrics       ports.MetricsRecorder
	Clock         events.Clock
	IDGenerator   events.IDGenerator
}

// Adapter translates gateway.commands.v1 into external provider calls protected by
// bulkhead, circuit breaker, and exponential backoff + jitter, and emits normalized
// events on gateway.events.v1.
type Adapter struct {
	gw            ports.PaymentGateway
	publisher     ports.EventPublisher
	inbox         ports.InboxStore
	breaker       *CircuitBreaker
	retry         RetryConfig
	bulkhead      chan struct{}
	webhookSecret string
	logger        *slog.Logger
	metrics       ports.MetricsRecorder
	clock         events.Clock
	idGen         events.IDGenerator

	webhookMu    sync.Mutex
	seenWebhooks map[string]struct{}
}

// NewAdapter creates a Gateway Adapter ACL.
func NewAdapter(cfg AdapterConfig) *Adapter {
	limit := cfg.BulkheadLimit
	if limit <= 0 {
		limit = 50
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	breaker := cfg.Breaker
	if breaker == nil {
		breaker = NewCircuitBreaker(CircuitBreakerConfig{})
	}
	clock := cfg.Clock
	if clock == nil {
		clock = events.DefaultClock
	}
	idGen := cfg.IDGenerator
	if idGen == nil {
		idGen = events.NewUUID
	}

	return &Adapter{
		gw:            cfg.Gateway,
		publisher:     cfg.Publisher,
		inbox:         cfg.Inbox,
		breaker:       breaker,
		retry:         cfg.Retry.withDefaults(),
		bulkhead:      make(chan struct{}, limit),
		webhookSecret: cfg.WebhookSecret,
		logger:        logger,
		metrics:       cfg.Metrics,
		clock:         clock,
		idGen:         idGen,
		seenWebhooks:  make(map[string]struct{}),
	}
}

// HandleCommand consumes commands from gateway.commands.v1.
func (a *Adapter) HandleCommand(ctx context.Context, env events.Envelope) error {
	if a.inbox != nil {
		dup, err := a.inbox.TryMarkProcessed(ctx, "gateway-adapter", env.EventID)
		if err != nil {
			return err
		}
		if dup {
			return nil
		}
	}

	switch env.Type {
	case events.CommandSubmitPayment:
		return a.handleSubmitPayment(ctx, env)
	case events.CommandCheckPaymentStatus:
		return a.handleCheckStatus(ctx, env)
	case events.CommandRefundPayment:
		return a.handleRefundPayment(ctx, env)
	default:
		return nil
	}
}

// HandleWebhook validates HMAC signature, deduplicates by provider_event_id, and
// publishes the normalized gateway outcome event.
func (a *Adapter) HandleWebhook(ctx context.Context, signature string, body []byte) error {
	if a.webhookSecret != "" && !VerifyWebhook(a.webhookSecret, body, signature) {
		return ErrInvalidWebhookSignature
	}

	var wh WebhookPayload
	if err := json.Unmarshal(body, &wh); err != nil {
		return fmt.Errorf("unmarshal webhook payload: %w", err)
	}
	if wh.ProviderEventID == "" || wh.PaymentID == "" {
		return fmt.Errorf("provider_event_id and payment_id are required")
	}

	a.webhookMu.Lock()
	if _, dup := a.seenWebhooks[wh.ProviderEventID]; dup {
		a.webhookMu.Unlock()
		return nil
	}
	a.seenWebhooks[wh.ProviderEventID] = struct{}{}
	a.webhookMu.Unlock()

	meta := events.Metadata{
		CorrelationID: wh.PaymentID,
		CausationID:   wh.ProviderEventID,
	}

	switch wh.Status {
	case string(ports.GatewayOutcomeApproved):
		return a.emit(ctx, events.EventGatewayPaymentApproved, wh.PaymentID, events.GatewayPaymentApprovedPayload{
			PaymentID:   wh.PaymentID,
			Provider:    wh.Provider,
			ProviderRef: wh.ProviderRef,
		}, meta)

	case string(ports.GatewayOutcomeDeclined):
		reason := wh.Reason
		if reason == "" {
			reason = events.ReasonGatewayDeclined
		}
		return a.emit(ctx, events.EventGatewayPaymentDeclined, wh.PaymentID, events.GatewayPaymentDeclinedPayload{
			PaymentID: wh.PaymentID,
			Provider:  wh.Provider,
			Reason:    reason,
		}, meta)

	default:
		return fmt.Errorf("unsupported webhook status: %s", wh.Status)
	}
}

func (a *Adapter) handleSubmitPayment(ctx context.Context, env events.Envelope) error {
	cmd, err := events.DecodePayload[events.SubmitPaymentCommand](env)
	if err != nil {
		return err
	}
	meta := events.Metadata{
		CorrelationID: cmd.PaymentID,
		CausationID:   env.EventID,
	}

	// Non-blocking bulkhead acquisition: if saturated, we know the request was NOT sent.
	select {
	case a.bulkhead <- struct{}{}:
		defer func() { <-a.bulkhead }()
	default:
		return a.emit(ctx, events.EventGatewaySubmissionNotSent, cmd.PaymentID, events.GatewaySubmissionNotSentPayload{
			PaymentID: cmd.PaymentID,
			Provider:  cmd.Provider,
			Reason:    "BULKHEAD_SATURATED",
		}, meta)
	}

	sentAnyAttempt := false
	var lastErr error

	for attempt := 0; attempt < a.retry.MaxAttempts; attempt++ {
		if err := a.breaker.Allow(); err != nil {
			if !sentAnyAttempt {
				// Certainty of non-delivery: safe for saga to release funds.
				return a.emit(ctx, events.EventGatewaySubmissionNotSent, cmd.PaymentID, events.GatewaySubmissionNotSentPayload{
					PaymentID: cmd.PaymentID,
					Provider:  cmd.Provider,
					Reason:    events.ReasonCircuitOpen,
				}, meta)
			}
			// A prior attempt already went over the wire: outcome is uncertain!
			return a.emit(ctx, events.EventGatewayOutcomeUnknown, cmd.PaymentID, events.GatewayOutcomeUnknownPayload{
				PaymentID: cmd.PaymentID,
				Provider:  cmd.Provider,
				Reason:    events.ReasonGatewayTimeout,
			}, meta)
		}

		sentAnyAttempt = true
		start := a.clock()
		attemptCtx, cancel := context.WithTimeout(ctx, a.retry.AttemptTimeout)
		res, callErr := a.gw.SubmitPayment(attemptCtx, ports.GatewaySubmitRequest{
			PaymentID: cmd.PaymentID,
			Provider:  cmd.Provider,
			ServiceID: cmd.ServiceID,
			Amount:    cmd.Amount,
		})
		cancel()
		elapsed := a.clock().Sub(start)

		if callErr == nil {
			a.breaker.RecordSuccess()
			a.recordAttempt(cmd.Provider, string(res.Outcome), elapsed)
			return a.emitSubmitOutcome(ctx, cmd, res, meta)
		}

		lastErr = callErr
		isTimeout := errors.Is(callErr, ports.ErrGatewayTimeout) || errors.Is(callErr, context.DeadlineExceeded)
		a.breaker.RecordFailure(isTimeout)
		a.recordAttempt(cmd.Provider, "ERROR", elapsed)

		a.logger.Warn("gateway submit attempt failed",
			"correlation_id", cmd.PaymentID,
			"provider", cmd.Provider,
			"attempt", attempt+1,
			"max_attempts", a.retry.MaxAttempts,
			"error", callErr.Error(),
		)

		if attempt+1 < a.retry.MaxAttempts {
			delay := FullJitterDelay(attempt, a.retry.BaseDelay, a.retry.MaxDelay, a.retry.RandFloat)
			if sleepErr := a.retry.Sleep(ctx, delay); sleepErr != nil {
				break
			}
		}
	}

	reason := events.ReasonGatewayTimeout
	if lastErr != nil && !errors.Is(lastErr, ports.ErrGatewayTimeout) && !errors.Is(lastErr, context.DeadlineExceeded) {
		reason = lastErr.Error()
	}
	return a.emit(ctx, events.EventGatewayOutcomeUnknown, cmd.PaymentID, events.GatewayOutcomeUnknownPayload{
		PaymentID: cmd.PaymentID,
		Provider:  cmd.Provider,
		Reason:    reason,
	}, meta)
}

func (a *Adapter) emitSubmitOutcome(
	ctx context.Context,
	cmd events.SubmitPaymentCommand,
	res ports.GatewaySubmitResult,
	meta events.Metadata,
) error {
	switch res.Outcome {
	case ports.GatewayOutcomeAccepted:
		return a.emit(ctx, events.EventGatewaySubmissionAccepted, cmd.PaymentID, events.GatewaySubmissionAcceptedPayload{
			PaymentID:   cmd.PaymentID,
			Provider:    cmd.Provider,
			ProviderRef: res.ProviderRef,
		}, meta)

	case ports.GatewayOutcomeApproved:
		return a.emit(ctx, events.EventGatewayPaymentApproved, cmd.PaymentID, events.GatewayPaymentApprovedPayload{
			PaymentID:   cmd.PaymentID,
			Provider:    cmd.Provider,
			ProviderRef: res.ProviderRef,
		}, meta)

	case ports.GatewayOutcomeDeclined:
		reason := res.Reason
		if reason == "" {
			reason = events.ReasonGatewayDeclined
		}
		return a.emit(ctx, events.EventGatewayPaymentDeclined, cmd.PaymentID, events.GatewayPaymentDeclinedPayload{
			PaymentID: cmd.PaymentID,
			Provider:  cmd.Provider,
			Reason:    reason,
		}, meta)

	default:
		return a.emit(ctx, events.EventGatewayOutcomeUnknown, cmd.PaymentID, events.GatewayOutcomeUnknownPayload{
			PaymentID: cmd.PaymentID,
			Provider:  cmd.Provider,
			Reason:    fmt.Sprintf("unexpected gateway outcome: %s", res.Outcome),
		}, meta)
	}
}

func (a *Adapter) handleCheckStatus(ctx context.Context, env events.Envelope) error {
	cmd, err := events.DecodePayload[events.CheckPaymentStatusCommand](env)
	if err != nil {
		return err
	}
	meta := events.Metadata{
		CorrelationID: cmd.PaymentID,
		CausationID:   env.EventID,
	}

	attemptCtx, cancel := context.WithTimeout(ctx, a.retry.AttemptTimeout)
	res, err := a.gw.CheckStatus(attemptCtx, cmd.PaymentID, cmd.Provider)
	cancel()

	if err != nil {
		return a.emit(ctx, events.EventGatewayStatusPending, cmd.PaymentID, events.GatewayStatusPendingPayload{
			PaymentID: cmd.PaymentID,
			Provider:  cmd.Provider,
		}, meta)
	}

	switch res.Outcome {
	case ports.GatewayOutcomeApproved:
		return a.emit(ctx, events.EventGatewayPaymentApproved, cmd.PaymentID, events.GatewayPaymentApprovedPayload{
			PaymentID:   cmd.PaymentID,
			Provider:    cmd.Provider,
			ProviderRef: res.ProviderRef,
		}, meta)

	case ports.GatewayOutcomeDeclined, ports.GatewayOutcomeNotFound:
		reason := res.Reason
		if reason == "" {
			reason = string(res.Outcome)
		}
		return a.emit(ctx, events.EventGatewayPaymentDeclined, cmd.PaymentID, events.GatewayPaymentDeclinedPayload{
			PaymentID: cmd.PaymentID,
			Provider:  cmd.Provider,
			Reason:    reason,
		}, meta)

	default:
		return a.emit(ctx, events.EventGatewayStatusPending, cmd.PaymentID, events.GatewayStatusPendingPayload{
			PaymentID: cmd.PaymentID,
			Provider:  cmd.Provider,
		}, meta)
	}
}

func (a *Adapter) handleRefundPayment(ctx context.Context, env events.Envelope) error {
	cmd, err := events.DecodePayload[events.RefundPaymentCommand](env)
	if err != nil {
		return err
	}
	meta := events.Metadata{
		CorrelationID: cmd.PaymentID,
		CausationID:   env.EventID,
	}

	attemptCtx, cancel := context.WithTimeout(ctx, a.retry.AttemptTimeout)
	res, err := a.gw.RefundPayment(attemptCtx, ports.GatewayRefundRequest{
		PaymentID:   cmd.PaymentID,
		RefundID:    cmd.RefundID,
		Provider:    cmd.Provider,
		ProviderRef: cmd.ProviderRef,
		Amount:      cmd.Amount,
	})
	cancel()

	if err != nil || !res.Approved {
		reason := res.Reason
		if err != nil {
			reason = err.Error()
		}
		return a.emit(ctx, events.EventGatewayRefundDeclined, cmd.PaymentID, events.GatewayRefundDeclinedPayload{
			PaymentID: cmd.PaymentID,
			RefundID:  cmd.RefundID,
			Provider:  cmd.Provider,
			Reason:    reason,
		}, meta)
	}

	return a.emit(ctx, events.EventGatewayRefundApproved, cmd.PaymentID, events.GatewayRefundApprovedPayload{
		PaymentID:   cmd.PaymentID,
		RefundID:    cmd.RefundID,
		Provider:    cmd.Provider,
		ProviderRef: res.ProviderRef,
		Amount:      cmd.Amount,
	}, meta)
}

func (a *Adapter) emit(ctx context.Context, eventType, paymentID string, payload any, meta events.Metadata) error {
	env, err := events.NewEnvelope(
		eventType,
		events.AggregateTypeGateway,
		paymentID,
		0,
		payload,
		meta,
		a.clock,
		a.idGen,
	)
	if err != nil {
		return err
	}
	return a.publisher.Publish(ctx, events.TopicGatewayEvents, paymentID, env)
}

func (a *Adapter) recordAttempt(provider, outcome string, elapsed time.Duration) {
	if a.metrics != nil {
		a.metrics.RecordGatewayAttempt(provider, outcome, elapsed)
	}
}
