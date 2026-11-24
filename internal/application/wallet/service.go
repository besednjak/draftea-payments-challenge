package wallet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/besednjak/draftea-payments-challenge/internal/application/ports"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
	domainwallet "github.com/besednjak/draftea-payments-challenge/internal/domain/wallet"
)

const consumerName = "wallet-service"

// Config wires dependencies and resilience settings for the Wallet application service.
type Config struct {
	EventStore       ports.EventStore
	SnapshotStore    ports.SnapshotStore
	SnapshotInterval int64
	BalanceReadModel ports.WalletBalanceReadModel
	Publisher        ports.EventPublisher
	Inbox            ports.InboxStore
	Metrics          ports.MetricsRecorder
	Logger           *slog.Logger
	MaxLockRetries   int
	LockRetryMin     time.Duration
	LockRetryMax     time.Duration
	Clock            events.Clock
	IDGenerator      events.IDGenerator
}

// Service orchestrates commands targeting the Wallet Event-Sourced aggregate.
// It enforces optimistic locking with jittered retries, periodic snapshots,
// and synchronous read-your-writes updates to WalletBalanceReadModel.
type Service struct {
	store            ports.EventStore
	snapshots        ports.SnapshotStore
	snapshotInterval int64
	balances         ports.WalletBalanceReadModel
	publisher        ports.EventPublisher
	inbox            ports.InboxStore
	metrics          ports.MetricsRecorder
	logger           *slog.Logger
	maxLockRetries   int
	lockRetryMin     time.Duration
	lockRetryMax     time.Duration
	clock            events.Clock
	idGen            events.IDGenerator
}

// NewService creates a Wallet application service.
func NewService(cfg Config) *Service {
	if cfg.SnapshotInterval <= 0 {
		cfg.SnapshotInterval = 100
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

	return &Service{
		store:            cfg.EventStore,
		snapshots:        cfg.SnapshotStore,
		snapshotInterval: cfg.SnapshotInterval,
		balances:         cfg.BalanceReadModel,
		publisher:        cfg.Publisher,
		inbox:            cfg.Inbox,
		metrics:          cfg.Metrics,
		logger:           cfg.Logger,
		maxLockRetries:   cfg.MaxLockRetries,
		lockRetryMin:     cfg.LockRetryMin,
		lockRetryMax:     cfg.LockRetryMax,
		clock:            cfg.Clock,
		idGen:            cfg.IDGenerator,
	}
}

// CreateWallet initializes a new wallet stream, updates the balance read model,
// and publishes WalletCreated.
func (s *Service) CreateWallet(ctx context.Context, walletID, userID, currency string) (*domainwallet.Wallet, error) {
	w, err := domainwallet.Create(
		walletID,
		userID,
		currency,
		events.Metadata{CorrelationID: walletID},
		domainwallet.WithClock(s.clock),
		domainwallet.WithIDGenerator(s.idGen),
	)
	if err != nil {
		return nil, err
	}

	uncommitted := w.UncommittedEvents()
	if err := s.store.Append(ctx, w.StreamID(), 0, uncommitted...); err != nil {
		return nil, err
	}
	w.ClearUncommitted()

	if err := s.syncBalanceView(ctx, w); err != nil {
		return nil, err
	}
	if err := s.publishEvents(ctx, w.ID(), uncommitted); err != nil {
		return nil, err
	}
	return w, nil
}

// Deposit credits funds to walletID with optimistic locking retry.
func (s *Service) Deposit(ctx context.Context, walletID, referenceID string, amount events.Money) (*domainwallet.Wallet, error) {
	meta := events.Metadata{CorrelationID: referenceID}
	return s.executeWithRetry(ctx, walletID, func(w *domainwallet.Wallet) error {
		return w.Deposit(referenceID, amount, meta)
	})
}

// GetBalance queries the synchronous read-your-writes wallet balance view.
func (s *Service) GetBalance(ctx context.Context, walletID string) (ports.WalletBalanceView, error) {
	if s.balances == nil {
		w, err := s.Load(ctx, walletID)
		if err != nil {
			return ports.WalletBalanceView{}, err
		}
		return ports.WalletBalanceView{
			WalletID:  w.ID(),
			UserID:    w.UserID(),
			Currency:  w.Currency(),
			Balance:   w.Balance(),
			Held:      w.Held(),
			Available: w.Available(),
			Version:   w.Version(),
			UpdatedAt: s.clock().UTC(),
		}, nil
	}
	return s.balances.Get(ctx, walletID)
}

// Load rehydrates a Wallet aggregate using the latest snapshot (if available) plus delta events.
func (s *Service) Load(ctx context.Context, walletID string) (*domainwallet.Wallet, error) {
	streamID := events.StreamID(events.AggregateTypeWallet, walletID)

	var (
		domSnap     *domainwallet.Snapshot
		fromVersion int64
	)

	if s.snapshots != nil {
		snap, ok, err := s.snapshots.LoadSnapshot(ctx, streamID)
		if err != nil {
			return nil, fmt.Errorf("load wallet snapshot %s: %w", streamID, err)
		}
		if ok {
			var decoded domainwallet.Snapshot
			if err := json.Unmarshal(snap.State, &decoded); err != nil {
				return nil, fmt.Errorf("decode wallet snapshot %s: %w", streamID, err)
			}
			domSnap = &decoded
			fromVersion = snap.Version
		}
	}

	history, err := s.store.LoadStream(ctx, streamID, fromVersion)
	if err != nil {
		if domSnap != nil && errors.Is(err, ports.ErrStreamNotFound) {
			history = nil
		} else {
			return nil, err
		}
	}

	return domainwallet.Rehydrate(
		domSnap,
		history,
		domainwallet.WithClock(s.clock),
		domainwallet.WithIDGenerator(s.idGen),
	)
}

// HandleCommand consumes commands from wallets.commands.v1.
func (s *Service) HandleCommand(ctx context.Context, env events.Envelope) error {
	if s.inbox != nil && env.EventID != "" {
		dup, err := s.inbox.TryMarkProcessed(ctx, consumerName, env.EventID)
		if err != nil {
			return err
		}
		if dup {
			return nil
		}
	}

	switch env.Type {
	case events.CommandReserveFunds:
		cmd, err := events.DecodePayload[events.ReserveFundsCommand](env)
		if err != nil {
			return err
		}
		meta := events.Metadata{CorrelationID: cmd.PaymentID, CausationID: env.EventID}
		_, err = s.executeWithRetry(ctx, cmd.WalletID, func(w *domainwallet.Wallet) error {
			return w.Reserve(cmd.PaymentID, cmd.Amount, meta)
		})
		return err

	case events.CommandCaptureFunds:
		cmd, err := events.DecodePayload[events.CaptureFundsCommand](env)
		if err != nil {
			return err
		}
		meta := events.Metadata{CorrelationID: cmd.PaymentID, CausationID: env.EventID}
		_, err = s.executeWithRetry(ctx, cmd.WalletID, func(w *domainwallet.Wallet) error {
			return w.Capture(cmd.PaymentID, cmd.Amount, meta)
		})
		return err

	case events.CommandReleaseFunds:
		cmd, err := events.DecodePayload[events.ReleaseFundsCommand](env)
		if err != nil {
			return err
		}
		meta := events.Metadata{CorrelationID: cmd.PaymentID, CausationID: env.EventID}
		_, err = s.executeWithRetry(ctx, cmd.WalletID, func(w *domainwallet.Wallet) error {
			return w.Release(cmd.PaymentID, cmd.Reason, meta)
		})
		return err

	case events.CommandRefundFunds:
		cmd, err := events.DecodePayload[events.RefundFundsCommand](env)
		if err != nil {
			return err
		}
		meta := events.Metadata{CorrelationID: cmd.PaymentID, CausationID: env.EventID}
		_, err = s.executeWithRetry(ctx, cmd.WalletID, func(w *domainwallet.Wallet) error {
			return w.Refund(cmd.PaymentID, cmd.RefundID, cmd.Amount, meta)
		})
		return err

	default:
		return nil
	}
}

func (s *Service) executeWithRetry(
	ctx context.Context,
	walletID string,
	mutate func(*domainwallet.Wallet) error,
) (*domainwallet.Wallet, error) {
	var lastErr error

	for attempt := 0; attempt < s.maxLockRetries; attempt++ {
		w, err := s.Load(ctx, walletID)
		if err != nil {
			return nil, err
		}
		baseVersion := w.Version()

		if err := mutate(w); err != nil {
			return nil, err
		}

		uncommitted := w.UncommittedEvents()
		if len(uncommitted) == 0 {
			return w, nil // Idempotent no-op.
		}

		err = s.store.Append(ctx, w.StreamID(), w.BaseVersion(), uncommitted...)
		if err == nil {
			w.ClearUncommitted()
			if err := s.syncBalanceView(ctx, w); err != nil {
				return nil, err
			}
			s.maybeSaveSnapshot(ctx, w, baseVersion)
			if err := s.publishEvents(ctx, w.ID(), uncommitted); err != nil {
				return nil, err
			}
			return w, nil
		}

		if !errors.Is(err, ports.ErrVersionConflict) {
			return nil, err
		}

		lastErr = err
		if s.metrics != nil {
			s.metrics.RecordOptimisticLockRetry(events.AggregateTypeWallet)
		}
		s.logger.Warn("wallet optimistic lock conflict, reloading aggregate",
			"wallet_id", walletID,
			"attempt", attempt+1,
			"max_retries", s.maxLockRetries,
		)

		if attempt+1 < s.maxLockRetries {
			jitter := s.lockRetryMin
			if s.lockRetryMax > s.lockRetryMin {
				jitter += time.Duration(rand.Int64N(int64(s.lockRetryMax - s.lockRetryMin)))
			}
			timer := time.NewTimer(jitter)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
	}

	return nil, fmt.Errorf("wallet %s failed after %d optimistic lock retries: %w", walletID, s.maxLockRetries, lastErr)
}

func (s *Service) syncBalanceView(ctx context.Context, w *domainwallet.Wallet) error {
	if s.balances == nil {
		return nil
	}
	return s.balances.Upsert(ctx, ports.WalletBalanceView{
		WalletID:  w.ID(),
		UserID:    w.UserID(),
		Currency:  w.Currency(),
		Balance:   w.Balance(),
		Held:      w.Held(),
		Available: w.Available(),
		Version:   w.Version(),
		UpdatedAt: s.clock().UTC(),
	})
}

func (s *Service) maybeSaveSnapshot(ctx context.Context, w *domainwallet.Wallet, prevVersion int64) {
	if s.snapshots == nil || s.snapshotInterval <= 0 {
		return
	}
	if w.Version()/s.snapshotInterval <= prevVersion/s.snapshotInterval {
		return
	}
	raw, err := json.Marshal(w.TakeSnapshot())
	if err != nil {
		s.logger.Error("marshal wallet snapshot failed", "wallet_id", w.ID(), "error", err.Error())
		return
	}
	if err := s.snapshots.SaveSnapshot(ctx, ports.Snapshot{
		StreamID:  w.StreamID(),
		Version:   w.Version(),
		State:     raw,
		CreatedAt: s.clock().UTC(),
	}); err != nil {
		s.logger.Error("save wallet snapshot failed", "wallet_id", w.ID(), "error", err.Error())
	}
}

func (s *Service) publishEvents(ctx context.Context, walletID string, evts []events.Envelope) error {
	if s.publisher == nil {
		return nil
	}
	for _, env := range evts {
		if err := s.publisher.Publish(ctx, events.TopicWalletsEvents, walletID, env); err != nil {
			return err
		}
	}
	return nil
}
