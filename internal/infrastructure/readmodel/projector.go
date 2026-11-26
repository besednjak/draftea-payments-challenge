package readmodel

import (
	"context"

	"github.com/besednjak/draftea-payments-challenge/internal/application/ports"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/payment"
)

// Projector updates CQRS read models from domain events and supports full rebuilds
// from the Event Store during disaster recovery.
type Projector struct {
	balances     ports.WalletBalanceReadModel
	transactions ports.WalletTransactionReadModel
	payments     ports.PaymentViewReadModel
}

// NewProjector creates a Projector wired to the given read models.
func NewProjector(
	balances ports.WalletBalanceReadModel,
	transactions ports.WalletTransactionReadModel,
	payments ports.PaymentViewReadModel,
) *Projector {
	return &Projector{
		balances:     balances,
		transactions: transactions,
		payments:     payments,
	}
}

// HandleEvent projects a single wallet or payment domain event into the read models.
func (p *Projector) HandleEvent(ctx context.Context, env events.Envelope) error {
	switch env.AggregateType {
	case events.AggregateTypeWallet:
		return p.projectWalletEvent(ctx, env)
	case events.AggregateTypePayment:
		return p.projectPaymentEvent(ctx, env)
	default:
		return nil
	}
}

// RebuildAll resets all read models and replays every persisted event from the Event Store.
func (p *Projector) RebuildAll(ctx context.Context, store ports.EventStore) (int, error) {
	if p.balances != nil {
		if err := p.balances.Reset(ctx); err != nil {
			return 0, err
		}
	}
	if p.transactions != nil {
		if err := p.transactions.Reset(ctx); err != nil {
			return 0, err
		}
	}
	if p.payments != nil {
		if err := p.payments.Reset(ctx); err != nil {
			return 0, err
		}
	}

	storedEvents, err := store.LoadAll(ctx, 0)
	if err != nil {
		return 0, err
	}
	for _, storedEvent := range storedEvents {
		if err := p.HandleEvent(ctx, storedEvent.Envelope); err != nil {
			return 0, err
		}
	}
	return len(storedEvents), nil
}

func (p *Projector) projectWalletEvent(ctx context.Context, env events.Envelope) error {
	switch env.Type {
	case events.EventWalletCreated:
		payload, err := events.DecodePayload[events.WalletCreatedPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		if p.balances == nil {
			return nil
		}
		return p.balances.Upsert(ctx, ports.WalletBalanceView{
			WalletID:  payload.WalletID,
			UserID:    payload.UserID,
			Currency:  payload.Currency,
			Version:   env.AggregateVersion,
			UpdatedAt: env.OccurredAt,
		})

	case events.EventFundsDeposited:
		payload, err := events.DecodePayload[events.FundsDepositedPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		return p.applyWalletMovement(ctx, env, "", payload.Amount, payload.BalanceAfter, payload.HeldAfter, payload.AvailableAfter)

	case events.EventFundsReserved:
		payload, err := events.DecodePayload[events.FundsReservedPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		return p.applyWalletMovement(ctx, env, payload.PaymentID, payload.Amount, payload.BalanceAfter, payload.HeldAfter, payload.AvailableAfter)

	case events.EventFundsCaptured:
		payload, err := events.DecodePayload[events.FundsCapturedPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		return p.applyWalletMovement(ctx, env, payload.PaymentID, payload.Amount, payload.BalanceAfter, payload.HeldAfter, payload.AvailableAfter)

	case events.EventFundsReleased:
		payload, err := events.DecodePayload[events.FundsReleasedPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		return p.applyWalletMovement(ctx, env, payload.PaymentID, payload.Amount, payload.BalanceAfter, payload.HeldAfter, payload.AvailableAfter)

	case events.EventFundsRefunded:
		payload, err := events.DecodePayload[events.FundsRefundedPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		return p.applyWalletMovement(ctx, env, payload.PaymentID, payload.Amount, payload.BalanceAfter, payload.HeldAfter, payload.AvailableAfter)
	}
	return nil
}

func (p *Projector) applyWalletMovement(
	ctx context.Context,
	env events.Envelope,
	paymentID string,
	amount events.Money,
	balanceAfter int64,
	heldAfter int64,
	availableAfter int64,
) error {
	if p.balances != nil {
		current, err := p.balances.Get(ctx, env.AggregateID)
		if err != nil {
			current = ports.WalletBalanceView{WalletID: env.AggregateID}
		}
		current.Balance = balanceAfter
		current.Held = heldAfter
		current.Available = availableAfter
		current.Version = env.AggregateVersion
		current.UpdatedAt = env.OccurredAt
		if err := p.balances.Upsert(ctx, current); err != nil {
			return err
		}
	}

	if p.transactions != nil {
		return p.transactions.Append(ctx, ports.WalletTransactionView{
			EventID:      env.EventID,
			WalletID:     env.AggregateID,
			PaymentID:    paymentID,
			Type:         env.Type,
			Amount:       amount.Amount,
			Currency:     amount.Currency,
			BalanceAfter: balanceAfter,
			HeldAfter:    heldAfter,
			OccurredAt:   env.OccurredAt,
		})
	}
	return nil
}

func (p *Projector) projectPaymentEvent(ctx context.Context, env events.Envelope) error {
	if p.payments == nil {
		return nil
	}

	if env.Type == events.EventPaymentInitiated {
		payload, err := events.DecodePayload[events.PaymentInitiatedPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		return p.payments.Upsert(ctx, ports.PaymentView{
			PaymentID: payload.PaymentID,
			WalletID:  payload.WalletID,
			UserID:    payload.UserID,
			ServiceID: payload.ServiceID,
			Provider:  payload.Provider,
			Status:    string(payment.StatusPending),
			Amount:    payload.Amount.Amount,
			Currency:  payload.Amount.Currency,
			Version:   env.AggregateVersion,
			CreatedAt: env.OccurredAt,
			UpdatedAt: env.OccurredAt,
		})
	}

	current, err := p.payments.Get(ctx, env.AggregateID)
	if err != nil {
		current = ports.PaymentView{PaymentID: env.AggregateID}
	}

	switch env.Type {
	case events.EventPaymentFundsReserved:
		current.Status = string(payment.StatusFundsReserved)

	case events.EventPaymentRejected:
		payload, err := events.DecodePayload[events.PaymentRejectedPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		current.Status = string(payment.StatusRejected)
		current.FailureReason = payload.Reason

	case events.EventPaymentSubmitted:
		payload, err := events.DecodePayload[events.PaymentSubmittedPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		current.Status = string(payment.StatusSubmitted)
		if payload.ProviderRef != "" {
			current.ProviderRef = payload.ProviderRef
		}

	case events.EventPaymentGatewayUnknown:
		payload, err := events.DecodePayload[events.PaymentGatewayUnknownPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		current.Status = string(payment.StatusGatewayUnknown)
		current.FailureReason = payload.Reason

	case events.EventPaymentManualReviewRequired:
		payload, err := events.DecodePayload[events.PaymentManualReviewRequiredPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		current.Status = string(payment.StatusManualReview)
		current.FailureReason = payload.Reason

	case events.EventPaymentCompleted:
		payload, err := events.DecodePayload[events.PaymentCompletedPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		current.Status = string(payment.StatusCompleted)
		if payload.ProviderRef != "" {
			current.ProviderRef = payload.ProviderRef
		}
		current.FailureReason = ""

	case events.EventPaymentFailed:
		payload, err := events.DecodePayload[events.PaymentFailedPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		current.Status = string(payment.StatusFailed)
		current.FailureReason = payload.Reason

	case events.EventPaymentRefunded:
		payload, err := events.DecodePayload[events.PaymentRefundedPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		current.RefundedTotal = payload.RefundedTotal
		if payload.FullyRefunded {
			current.Status = string(payment.StatusRefunded)
		}

	default:
		return nil
	}

	current.Version = env.AggregateVersion
	current.UpdatedAt = env.OccurredAt
	return p.payments.Upsert(ctx, current)
}
