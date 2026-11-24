package readmodel

import (
	"context"
	"fmt"
	"sync"

	"github.com/besednjak/draftea-payments-challenge/internal/application/ports"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/payment"
)

// PaymentViewStore is the in-memory CQRS read model for payment lifecycle queries.
type PaymentViewStore struct {
	mu    sync.RWMutex
	views map[string]ports.PaymentView
}

// NewPaymentViewStore creates an empty PaymentViewStore.
func NewPaymentViewStore() *PaymentViewStore {
	return &PaymentViewStore{views: make(map[string]ports.PaymentView)}
}

// Upsert stores view unless a newer aggregate version is already persisted.
func (s *PaymentViewStore) Upsert(ctx context.Context, view ports.PaymentView) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, exists := s.views[view.PaymentID]; exists && existing.Version > view.Version {
		return nil
	}
	s.views[view.PaymentID] = view
	return nil
}

// Get returns the projected payment view for paymentID.
func (s *PaymentViewStore) Get(ctx context.Context, paymentID string) (ports.PaymentView, error) {
	if err := ctx.Err(); err != nil {
		return ports.PaymentView{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	view, exists := s.views[paymentID]
	if !exists {
		return ports.PaymentView{}, fmt.Errorf("%w: payment %s", ports.ErrNotFound, paymentID)
	}
	return view, nil
}

// ListNonTerminal returns all payments currently in active in-flight saga states.
func (s *PaymentViewStore) ListNonTerminal(ctx context.Context) ([]ports.PaymentView, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	var active []ports.PaymentView
	for _, view := range s.views {
		if payment.Status(view.Status).IsActiveSaga() {
			active = append(active, view)
		}
	}
	return active, nil
}

// Reset clears all projected payment views prior to a full replay.
func (s *PaymentViewStore) Reset(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.views = make(map[string]ports.PaymentView)
	s.mu.Unlock()
	return nil
}
