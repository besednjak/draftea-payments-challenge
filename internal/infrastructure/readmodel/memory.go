package readmodel

import (
	"context"
	"fmt"
	"sync"

	"github.com/besednjak/draftea-payments-challenge/internal/application/ports"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/payment"
)

// WalletBalanceStore is the in-memory synchronous CQRS read model for wallet balances.
type WalletBalanceStore struct {
	mu    sync.RWMutex
	views map[string]ports.WalletBalanceView
}

func NewWalletBalanceStore() *WalletBalanceStore {
	return &WalletBalanceStore{views: make(map[string]ports.WalletBalanceView)}
}

func (s *WalletBalanceStore) Upsert(ctx context.Context, view ports.WalletBalanceView) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.views[view.WalletID]; ok && existing.Version > view.Version {
		return nil // Ignore stale out-of-order updates.
	}
	s.views[view.WalletID] = view
	return nil
}

func (s *WalletBalanceStore) Get(ctx context.Context, walletID string) (ports.WalletBalanceView, error) {
	if err := ctx.Err(); err != nil {
		return ports.WalletBalanceView{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	view, ok := s.views[walletID]
	if !ok {
		return ports.WalletBalanceView{}, fmt.Errorf("%w: wallet %s", ports.ErrNotFound, walletID)
	}
	return view, nil
}

func (s *WalletBalanceStore) Reset(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.views = make(map[string]ports.WalletBalanceView)
	s.mu.Unlock()
	return nil
}

// WalletTransactionStore is the in-memory asynchronous CQRS read model for wallet history.
type WalletTransactionStore struct {
	mu   sync.RWMutex
	txs  map[string][]ports.WalletTransactionView
	seen map[string]struct{}
}

func NewWalletTransactionStore() *WalletTransactionStore {
	return &WalletTransactionStore{
		txs:  make(map[string][]ports.WalletTransactionView),
		seen: make(map[string]struct{}),
	}
}

func (s *WalletTransactionStore) Append(ctx context.Context, tx ports.WalletTransactionView) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if tx.EventID != "" {
		if _, dup := s.seen[tx.EventID]; dup {
			return nil
		}
		s.seen[tx.EventID] = struct{}{}
	}
	s.txs[tx.WalletID] = append(s.txs[tx.WalletID], tx)
	return nil
}

func (s *WalletTransactionStore) ListByWallet(ctx context.Context, walletID string) ([]ports.WalletTransactionView, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	src := s.txs[walletID]
	out := make([]ports.WalletTransactionView, len(src))
	copy(out, src)
	return out, nil
}

func (s *WalletTransactionStore) Reset(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.txs = make(map[string][]ports.WalletTransactionView)
	s.seen = make(map[string]struct{})
	s.mu.Unlock()
	return nil
}

// PaymentViewStore is the in-memory CQRS read model for payment lifecycle queries.
type PaymentViewStore struct {
	mu    sync.RWMutex
	views map[string]ports.PaymentView
}

func NewPaymentViewStore() *PaymentViewStore {
	return &PaymentViewStore{views: make(map[string]ports.PaymentView)}
}

func (s *PaymentViewStore) Upsert(ctx context.Context, view ports.PaymentView) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.views[view.PaymentID]; ok && existing.Version > view.Version {
		return nil
	}
	s.views[view.PaymentID] = view
	return nil
}

func (s *PaymentViewStore) Get(ctx context.Context, paymentID string) (ports.PaymentView, error) {
	if err := ctx.Err(); err != nil {
		return ports.PaymentView{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	view, ok := s.views[paymentID]
	if !ok {
		return ports.PaymentView{}, fmt.Errorf("%w: payment %s", ports.ErrNotFound, paymentID)
	}
	return view, nil
}

func (s *PaymentViewStore) ListNonTerminal(ctx context.Context) ([]ports.PaymentView, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []ports.PaymentView
	for _, v := range s.views {
		if payment.Status(v.Status).IsActiveSaga() {
			out = append(out, v)
		}
	}
	return out, nil
}

func (s *PaymentViewStore) Reset(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.views = make(map[string]ports.PaymentView)
	s.mu.Unlock()
	return nil
}

// Projector updates CQRS read models from domain events and supports full rebuilds
// from the Event Store during disaster recovery.
type Projector struct {
	balances ports.WalletBalanceReadModel
	txs      ports.WalletTransactionReadModel
	payments ports.PaymentViewReadModel
}

func NewProjector(
	balances ports.WalletBalanceReadModel,
	txs ports.WalletTransactionReadModel,
	payments ports.PaymentViewReadModel,
) *Projector {
	return &Projector{
		balances: balances,
		txs:      txs,
		payments: payments,
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
	if p.txs != nil {
		if err := p.txs.Reset(ctx); err != nil {
			return 0, err
		}
	}
	if p.payments != nil {
		if err := p.payments.Reset(ctx); err != nil {
			return 0, err
		}
	}

	all, err := store.LoadAll(ctx, 0)
	if err != nil {
		return 0, err
	}
	for _, se := range all {
		if err := p.HandleEvent(ctx, se.Envelope); err != nil {
			return 0, err
		}
	}
	return len(all), nil
}

func (p *Projector) projectWalletEvent(ctx context.Context, env events.Envelope) error {
	switch env.Type {
	case events.EventWalletCreated:
		d, err := events.DecodePayload[events.WalletCreatedPayload](env)
		if err != nil {
			return err
		}
		if p.balances != nil {
			return p.balances.Upsert(ctx, ports.WalletBalanceView{
				WalletID:  d.WalletID,
				UserID:    d.UserID,
				Currency:  d.Currency,
				Version:   env.AggregateVersion,
				UpdatedAt: env.OccurredAt,
			})
		}

	case events.EventFundsDeposited:
		d, err := events.DecodePayload[events.FundsDepositedPayload](env)
		if err != nil {
			return err
		}
		if err := p.updateBalance(ctx, env, d.BalanceAfter, d.HeldAfter, d.AvailableAfter); err != nil {
			return err
		}
		if p.txs != nil {
			return p.txs.Append(ctx, ports.WalletTransactionView{
				EventID:      env.EventID,
				WalletID:     env.AggregateID,
				Type:         env.Type,
				Amount:       d.Amount.Amount,
				Currency:     d.Amount.Currency,
				BalanceAfter: d.BalanceAfter,
				HeldAfter:    d.HeldAfter,
				OccurredAt:   env.OccurredAt,
			})
		}

	case events.EventFundsReserved:
		d, err := events.DecodePayload[events.FundsReservedPayload](env)
		if err != nil {
			return err
		}
		if err := p.updateBalance(ctx, env, d.BalanceAfter, d.HeldAfter, d.AvailableAfter); err != nil {
			return err
		}
		if p.txs != nil {
			return p.txs.Append(ctx, ports.WalletTransactionView{
				EventID:      env.EventID,
				WalletID:     env.AggregateID,
				PaymentID:    d.PaymentID,
				Type:         env.Type,
				Amount:       d.Amount.Amount,
				Currency:     d.Amount.Currency,
				BalanceAfter: d.BalanceAfter,
				HeldAfter:    d.HeldAfter,
				OccurredAt:   env.OccurredAt,
			})
		}

	case events.EventFundsCaptured:
		d, err := events.DecodePayload[events.FundsCapturedPayload](env)
		if err != nil {
			return err
		}
		if err := p.updateBalance(ctx, env, d.BalanceAfter, d.HeldAfter, d.AvailableAfter); err != nil {
			return err
		}
		if p.txs != nil {
			return p.txs.Append(ctx, ports.WalletTransactionView{
				EventID:      env.EventID,
				WalletID:     env.AggregateID,
				PaymentID:    d.PaymentID,
				Type:         env.Type,
				Amount:       d.Amount.Amount,
				Currency:     d.Amount.Currency,
				BalanceAfter: d.BalanceAfter,
				HeldAfter:    d.HeldAfter,
				OccurredAt:   env.OccurredAt,
			})
		}

	case events.EventFundsReleased:
		d, err := events.DecodePayload[events.FundsReleasedPayload](env)
		if err != nil {
			return err
		}
		if err := p.updateBalance(ctx, env, d.BalanceAfter, d.HeldAfter, d.AvailableAfter); err != nil {
			return err
		}
		if p.txs != nil {
			return p.txs.Append(ctx, ports.WalletTransactionView{
				EventID:      env.EventID,
				WalletID:     env.AggregateID,
				PaymentID:    d.PaymentID,
				Type:         env.Type,
				Amount:       d.Amount.Amount,
				Currency:     d.Amount.Currency,
				BalanceAfter: d.BalanceAfter,
				HeldAfter:    d.HeldAfter,
				OccurredAt:   env.OccurredAt,
			})
		}

	case events.EventFundsRefunded:
		d, err := events.DecodePayload[events.FundsRefundedPayload](env)
		if err != nil {
			return err
		}
		if err := p.updateBalance(ctx, env, d.BalanceAfter, d.HeldAfter, d.AvailableAfter); err != nil {
			return err
		}
		if p.txs != nil {
			return p.txs.Append(ctx, ports.WalletTransactionView{
				EventID:      env.EventID,
				WalletID:     env.AggregateID,
				PaymentID:    d.PaymentID,
				Type:         env.Type,
				Amount:       d.Amount.Amount,
				Currency:     d.Amount.Currency,
				BalanceAfter: d.BalanceAfter,
				HeldAfter:    d.HeldAfter,
				OccurredAt:   env.OccurredAt,
			})
		}
	}
	return nil
}

func (p *Projector) updateBalance(ctx context.Context, env events.Envelope, balance, held, available int64) error {
	if p.balances == nil {
		return nil
	}
	cur, err := p.balances.Get(ctx, env.AggregateID)
	if err != nil {
		cur = ports.WalletBalanceView{WalletID: env.AggregateID}
	}
	cur.Balance = balance
	cur.Held = held
	cur.Available = available
	cur.Version = env.AggregateVersion
	cur.UpdatedAt = env.OccurredAt
	return p.balances.Upsert(ctx, cur)
}

func (p *Projector) projectPaymentEvent(ctx context.Context, env events.Envelope) error {
	if p.payments == nil {
		return nil
	}

	if env.Type == events.EventPaymentInitiated {
		d, err := events.DecodePayload[events.PaymentInitiatedPayload](env)
		if err != nil {
			return err
		}
		return p.payments.Upsert(ctx, ports.PaymentView{
			PaymentID: d.PaymentID,
			WalletID:  d.WalletID,
			UserID:    d.UserID,
			ServiceID: d.ServiceID,
			Provider:  d.Provider,
			Status:    string(payment.StatusPending),
			Amount:    d.Amount.Amount,
			Currency:  d.Amount.Currency,
			Version:   env.AggregateVersion,
			CreatedAt: env.OccurredAt,
			UpdatedAt: env.OccurredAt,
		})
	}

	cur, err := p.payments.Get(ctx, env.AggregateID)
	if err != nil {
		cur = ports.PaymentView{PaymentID: env.AggregateID}
	}

	switch env.Type {
	case events.EventPaymentFundsReserved:
		cur.Status = string(payment.StatusFundsReserved)
	case events.EventPaymentRejected:
		d, err := events.DecodePayload[events.PaymentRejectedPayload](env)
		if err != nil {
			return err
		}
		cur.Status = string(payment.StatusRejected)
		cur.FailureReason = d.Reason
	case events.EventPaymentSubmitted:
		d, err := events.DecodePayload[events.PaymentSubmittedPayload](env)
		if err != nil {
			return err
		}
		cur.Status = string(payment.StatusSubmitted)
		if d.ProviderRef != "" {
			cur.ProviderRef = d.ProviderRef
		}
	case events.EventPaymentGatewayUnknown:
		d, err := events.DecodePayload[events.PaymentGatewayUnknownPayload](env)
		if err != nil {
			return err
		}
		cur.Status = string(payment.StatusGatewayUnknown)
		cur.FailureReason = d.Reason
	case events.EventPaymentManualReviewRequired:
		d, err := events.DecodePayload[events.PaymentManualReviewRequiredPayload](env)
		if err != nil {
			return err
		}
		cur.Status = string(payment.StatusManualReview)
		cur.FailureReason = d.Reason
	case events.EventPaymentCompleted:
		d, err := events.DecodePayload[events.PaymentCompletedPayload](env)
		if err != nil {
			return err
		}
		cur.Status = string(payment.StatusCompleted)
		if d.ProviderRef != "" {
			cur.ProviderRef = d.ProviderRef
		}
		cur.FailureReason = ""
	case events.EventPaymentFailed:
		d, err := events.DecodePayload[events.PaymentFailedPayload](env)
		if err != nil {
			return err
		}
		cur.Status = string(payment.StatusFailed)
		cur.FailureReason = d.Reason
	case events.EventPaymentRefunded:
		d, err := events.DecodePayload[events.PaymentRefundedPayload](env)
		if err != nil {
			return err
		}
		cur.RefundedTotal = d.RefundedTotal
		if d.FullyRefunded {
			cur.Status = string(payment.StatusRefunded)
		}
	default:
		return nil
	}

	cur.Version = env.AggregateVersion
	cur.UpdatedAt = env.OccurredAt
	return p.payments.Upsert(ctx, cur)
}
