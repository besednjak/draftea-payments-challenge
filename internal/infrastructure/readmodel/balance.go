package readmodel

import (
	"context"
	"fmt"
	"sync"

	"github.com/besednjak/draftea-payments-challenge/internal/application/ports"
)

// WalletBalanceStore is the in-memory synchronous CQRS read model for wallet balances.
type WalletBalanceStore struct {
	mu    sync.RWMutex
	views map[string]ports.WalletBalanceView
}

// NewWalletBalanceStore creates an empty WalletBalanceStore.
func NewWalletBalanceStore() *WalletBalanceStore {
	return &WalletBalanceStore{views: make(map[string]ports.WalletBalanceView)}
}

// Upsert stores view unless a newer aggregate version is already persisted.
func (s *WalletBalanceStore) Upsert(ctx context.Context, view ports.WalletBalanceView) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// Ignore stale out-of-order updates to preserve monotonic projection state.
	if existing, exists := s.views[view.WalletID]; exists && existing.Version > view.Version {
		return nil
	}
	s.views[view.WalletID] = view
	return nil
}

// Get returns the current balance view for walletID.
func (s *WalletBalanceStore) Get(ctx context.Context, walletID string) (ports.WalletBalanceView, error) {
	if err := ctx.Err(); err != nil {
		return ports.WalletBalanceView{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	view, exists := s.views[walletID]
	if !exists {
		return ports.WalletBalanceView{}, fmt.Errorf("%w: wallet %s", ports.ErrNotFound, walletID)
	}
	return view, nil
}

// Reset clears all projected wallet balances prior to a full replay.
func (s *WalletBalanceStore) Reset(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.views = make(map[string]ports.WalletBalanceView)
	s.mu.Unlock()
	return nil
}
