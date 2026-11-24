package ports

import (
	"context"
	"time"
)

// WalletBalanceView is the synchronous CQRS read model for immediate read-your-writes balance queries.
type WalletBalanceView struct {
	WalletID  string
	UserID    string
	Currency  string
	Balance   int64
	Held      int64
	Available int64
	Version   int64
	UpdatedAt time.Time
}

// WalletTransactionView is an append-only ledger entry projected asynchronously for wallet history.
type WalletTransactionView struct {
	EventID      string
	WalletID     string
	PaymentID    string
	Type         string
	Amount       int64
	Currency     string
	BalanceAfter int64
	HeldAfter    int64
	OccurredAt   time.Time
}

// PaymentView is the CQRS read model exposing the current state of a payment.
type PaymentView struct {
	PaymentID     string
	WalletID      string
	UserID        string
	ServiceID     string
	Provider      string
	Status        string
	ProviderRef   string
	FailureReason string
	Amount        int64
	RefundedTotal int64
	Currency      string
	Version       int64
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// WalletBalanceReadModel serves current wallet balances.
type WalletBalanceReadModel interface {
	Upsert(ctx context.Context, view WalletBalanceView) error
	Get(ctx context.Context, walletID string) (WalletBalanceView, error)
	Reset(ctx context.Context) error
}

// WalletTransactionReadModel serves historical wallet movements.
type WalletTransactionReadModel interface {
	Append(ctx context.Context, tx WalletTransactionView) error
	ListByWallet(ctx context.Context, walletID string) ([]WalletTransactionView, error)
	Reset(ctx context.Context) error
}

// PaymentViewReadModel serves payment status queries and active saga discovery.
type PaymentViewReadModel interface {
	Upsert(ctx context.Context, view PaymentView) error
	Get(ctx context.Context, paymentID string) (PaymentView, error)
	ListNonTerminal(ctx context.Context) ([]PaymentView, error)
	Reset(ctx context.Context) error
}
