package readmodel

import (
	"context"
	"sync"

	"github.com/besednjak/draftea-payments-challenge/internal/application/ports"
)

// WalletTransactionStore is the in-memory asynchronous CQRS read model for wallet history.
type WalletTransactionStore struct {
	mu           sync.RWMutex
	transactions map[string][]ports.WalletTransactionView
	seenEventIDs map[string]struct{}
}

// NewWalletTransactionStore creates an empty WalletTransactionStore.
func NewWalletTransactionStore() *WalletTransactionStore {
	return &WalletTransactionStore{
		transactions: make(map[string][]ports.WalletTransactionView),
		seenEventIDs: make(map[string]struct{}),
	}
}

// Append records a wallet movement, deduplicating by EventID for idempotent replay.
func (s *WalletTransactionStore) Append(ctx context.Context, tx ports.WalletTransactionView) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if tx.EventID != "" {
		if _, duplicate := s.seenEventIDs[tx.EventID]; duplicate {
			return nil
		}
		s.seenEventIDs[tx.EventID] = struct{}{}
	}
	s.transactions[tx.WalletID] = append(s.transactions[tx.WalletID], tx)
	return nil
}

// ListByWallet returns a copy of all recorded movements for walletID.
func (s *WalletTransactionStore) ListByWallet(ctx context.Context, walletID string) ([]ports.WalletTransactionView, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	stored := s.transactions[walletID]
	copied := make([]ports.WalletTransactionView, len(stored))
	copy(copied, stored)
	return copied, nil
}

// Reset clears all projected wallet transactions prior to a full replay.
func (s *WalletTransactionStore) Reset(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.transactions = make(map[string][]ports.WalletTransactionView)
	s.seenEventIDs = make(map[string]struct{})
	s.mu.Unlock()
	return nil
}
