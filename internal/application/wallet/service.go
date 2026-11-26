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

const walletConsumerName = "wallet-service"

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
	ctx = ports.WithCorrelation(ctx, walletID, "", walletID)
	createdWallet, err := domainwallet.Create(
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

	uncommitted := createdWallet.UncommittedEvents()
	if err := s.store.Append(ctx, createdWallet.StreamID(), 0, uncommitted...); err != nil {
		return nil, err
	}
	createdWallet.ClearUncommitted()

	if err := s.syncBalanceView(ctx, createdWallet); err != nil {
		return nil, err
	}
	if err := s.publishEvents(ctx, createdWallet.ID(), uncommitted); err != nil {
		return nil, err
	}
	return createdWallet, nil
}

// Deposit credits funds to walletID with optimistic locking retry.
func (s *Service) Deposit(ctx context.Context, walletID, referenceID string, amount events.Money) (*domainwallet.Wallet, error) {
	ctx = ports.WithCorrelation(ctx, referenceID, "", walletID)
	meta := events.Metadata{CorrelationID: referenceID}
	return s.executeWithRetry(ctx, walletID, func(w *domainwallet.Wallet) error {
		return w.Deposit(referenceID, amount, meta)
	}, nil)
}

// GetBalance queries the synchronous read-your-writes wallet balance view.
func (s *Service) GetBalance(ctx context.Context, walletID string) (ports.WalletBalanceView, error) {
	if s.balances == nil {
		loaded, err := s.Load(ctx, walletID)
		if err != nil {
			return ports.WalletBalanceView{}, err
		}
		return ports.WalletBalanceView{
			WalletID:  loaded.ID(),
			UserID:    loaded.UserID(),
			Currency:  loaded.Currency(),
			Balance:   loaded.Balance(),
			Held:      loaded.Held(),
			Available: loaded.Available(),
			Version:   loaded.Version(),
			UpdatedAt: s.clock().UTC(),
		}, nil
	}
	return s.balances.Get(ctx, walletID)
}

// Load rehydrates a Wallet aggregate using the latest snapshot (if available) plus delta events.
func (s *Service) Load(ctx context.Context, walletID string) (*domainwallet.Wallet, error) {
	streamID := events.StreamID(events.AggregateTypeWallet, walletID)

	var (
		domainSnap  *domainwallet.Snapshot
		fromVersion int64
	)

	if s.snapshots != nil {
		snap, found, err := s.snapshots.LoadSnapshot(ctx, streamID)
		if err != nil {
			return nil, fmt.Errorf("load wallet snapshot %s: %w", streamID, err)
		}
		if found {
			var decoded domainwallet.Snapshot
			if err := json.Unmarshal(snap.State, &decoded); err != nil {
				return nil, fmt.Errorf("decode wallet snapshot %s: %w", streamID, err)
			}
			domainSnap = &decoded
			fromVersion = snap.Version
		}
	}

	history, err := s.store.LoadStream(ctx, streamID, fromVersion)
	if err != nil {
		if domainSnap != nil && errors.Is(err, ports.ErrStreamNotFound) {
			history = nil
		} else {
			return nil, err
		}
	}

	return domainwallet.Rehydrate(
		domainSnap,
		history,
		domainwallet.WithClock(s.clock),
		domainwallet.WithIDGenerator(s.idGen),
	)
}

// HandleCommand consumes commands from wallets.commands.v1.
func (s *Service) HandleCommand(ctx context.Context, env events.Envelope) (retErr error) {
	if s.inbox != nil && env.EventID != "" {
		alreadyProcessed, err := s.inbox.TryMarkProcessed(ctx, walletConsumerName, env.EventID)
		if err != nil {
			return err
		}
		if alreadyProcessed {
			return nil
		}
		defer func() {
			if retErr != nil {
				_ = s.inbox.UnmarkProcessed(ctx, walletConsumerName, env.EventID)
			}
		}()
	}

	switch env.Type {
	case events.CommandReserveFunds:
		cmd, err := events.DecodePayload[events.ReserveFundsCommand](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		ctx = ports.WithCorrelation(ctx, cmd.PaymentID, cmd.PaymentID, cmd.WalletID)
		meta := events.Metadata{CorrelationID: cmd.PaymentID, CausationID: env.EventID}
		_, err = s.executeWithRetry(ctx, cmd.WalletID, func(w *domainwallet.Wallet) error {
			return w.Reserve(cmd.PaymentID, cmd.UserID, cmd.Amount, meta)
		}, matchReservationOutcomeEvent(cmd.PaymentID))
		return wrapPermanentDomainError(err)

	case events.CommandCaptureFunds:
		cmd, err := events.DecodePayload[events.CaptureFundsCommand](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		ctx = ports.WithCorrelation(ctx, cmd.PaymentID, cmd.PaymentID, cmd.WalletID)
		meta := events.Metadata{CorrelationID: cmd.PaymentID, CausationID: env.EventID}
		_, err = s.executeWithRetry(ctx, cmd.WalletID, func(w *domainwallet.Wallet) error {
			return w.Capture(cmd.PaymentID, cmd.ProviderRef, cmd.Amount, meta)
		}, matchCapturedEvent(cmd.PaymentID))
		return wrapPermanentDomainError(err)

	case events.CommandReleaseFunds:
		cmd, err := events.DecodePayload[events.ReleaseFundsCommand](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		ctx = ports.WithCorrelation(ctx, cmd.PaymentID, cmd.PaymentID, cmd.WalletID)
		meta := events.Metadata{CorrelationID: cmd.PaymentID, CausationID: env.EventID}
		_, err = s.executeWithRetry(ctx, cmd.WalletID, func(w *domainwallet.Wallet) error {
			return w.Release(cmd.PaymentID, cmd.Reason, meta)
		}, matchReleasedEvent(cmd.PaymentID))
		return wrapPermanentDomainError(err)

	case events.CommandRefundFunds:
		cmd, err := events.DecodePayload[events.RefundFundsCommand](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		ctx = ports.WithCorrelation(ctx, cmd.PaymentID, cmd.PaymentID, cmd.WalletID)
		meta := events.Metadata{CorrelationID: cmd.PaymentID, CausationID: env.EventID}
		_, err = s.executeWithRetry(ctx, cmd.WalletID, func(w *domainwallet.Wallet) error {
			return w.Refund(cmd.PaymentID, cmd.RefundID, cmd.Amount, meta)
		}, matchRefundedEvent(cmd.RefundID))
		return wrapPermanentDomainError(err)

	default:
		return nil
	}
}

func (s *Service) executeWithRetry(
	ctx context.Context,
	walletID string,
	mutate func(*domainwallet.Wallet) error,
	republishMatch func(events.Envelope) bool,
) (*domainwallet.Wallet, error) {
	var lastErr error

	for attempt := 0; attempt < s.maxLockRetries; attempt++ {
		loaded, err := s.Load(ctx, walletID)
		if err != nil {
			return nil, err
		}
		previousVersion := loaded.Version()

		if err := mutate(loaded); err != nil {
			return nil, err
		}

		uncommitted := loaded.UncommittedEvents()
		if len(uncommitted) == 0 {
			// Command effect was already persisted (e.g., retry after Publish failure or recovery).
			// Re-sync the read model and republish the persisted event so the saga never stalls.
			if err := s.syncBalanceView(ctx, loaded); err != nil {
				return nil, err
			}
			if republishMatch != nil {
				if err := s.republishPersistedEvent(ctx, loaded, republishMatch); err != nil {
					return nil, err
				}
			}
			return loaded, nil
		}

		err = s.store.Append(ctx, loaded.StreamID(), loaded.BaseVersion(), uncommitted...)
		if err == nil {
			loaded.ClearUncommitted()
			if err := s.syncBalanceView(ctx, loaded); err != nil {
				return nil, err
			}
			s.maybeSaveSnapshot(ctx, loaded, previousVersion)
			if err := s.publishEvents(ctx, loaded.ID(), uncommitted); err != nil {
				return nil, err
			}
			return loaded, nil
		}

		if !errors.Is(err, ports.ErrVersionConflict) {
			return nil, err
		}

		lastErr = err
		if s.metrics != nil {
			s.metrics.RecordOptimisticLockRetry(events.AggregateTypeWallet)
		}
		ports.LoggerFromContext(ctx, s.logger).Warn("wallet optimistic lock conflict, reloading aggregate",
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

func (s *Service) republishPersistedEvent(
	ctx context.Context,
	w *domainwallet.Wallet,
	match func(events.Envelope) bool,
) error {
	if s.publisher == nil {
		return nil
	}
	history, err := s.store.LoadStream(ctx, w.StreamID(), 0)
	if err != nil {
		return err
	}
	for index := len(history) - 1; index >= 0; index-- {
		if match(history[index]) {
			return s.publisher.Publish(ctx, events.TopicWalletsEvents, w.ID(), history[index])
		}
	}
	return nil
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

func (s *Service) maybeSaveSnapshot(ctx context.Context, w *domainwallet.Wallet, previousVersion int64) {
	if s.snapshots == nil || s.snapshotInterval <= 0 {
		return
	}
	if w.Version()/s.snapshotInterval <= previousVersion/s.snapshotInterval {
		return
	}
	encoded, err := json.Marshal(w.TakeSnapshot())
	if err != nil {
		ports.LoggerFromContext(ctx, s.logger).Error("marshal wallet snapshot failed", "error", err.Error())
		return
	}
	if err := s.snapshots.SaveSnapshot(ctx, ports.Snapshot{
		StreamID:  w.StreamID(),
		Version:   w.Version(),
		State:     encoded,
		CreatedAt: s.clock().UTC(),
	}); err != nil {
		ports.LoggerFromContext(ctx, s.logger).Error("save wallet snapshot failed", "error", err.Error())
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

func wrapPermanentDomainError(err error) error {
	if err == nil {
		return nil
	}
	permanentErrors := []error{
		domainwallet.ErrInvalidWalletID,
		domainwallet.ErrInvalidUserID,
		domainwallet.ErrInvalidPaymentID,
		domainwallet.ErrInvalidRefundID,
		domainwallet.ErrWalletAlreadyCreated,
		domainwallet.ErrWalletNotCreated,
		domainwallet.ErrCurrencyMismatch,
		domainwallet.ErrReservationNotFound,
		domainwallet.ErrReservationAlreadyCaptured,
		domainwallet.ErrCaptureAmountMismatch,
		domainwallet.ErrPaymentNotCaptured,
		domainwallet.ErrRefundExceedsCaptured,
		domainwallet.ErrAmountOverflow,
		ports.ErrStreamNotFound,
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

// matchReservationOutcomeEvent matches the persisted outcome (reserved or rejected) of a
// ReserveFunds command so a duplicate command republishes the original decision.
func matchReservationOutcomeEvent(paymentID string) func(events.Envelope) bool {
	return func(env events.Envelope) bool {
		switch env.Type {
		case events.EventFundsReserved:
			payload, err := events.DecodePayload[events.FundsReservedPayload](env)
			return err == nil && payload.PaymentID == paymentID
		case events.EventFundsReservationRejected:
			payload, err := events.DecodePayload[events.FundsReservationRejectedPayload](env)
			return err == nil && payload.PaymentID == paymentID
		default:
			return false
		}
	}
}

func matchCapturedEvent(paymentID string) func(events.Envelope) bool {
	return func(env events.Envelope) bool {
		if env.Type != events.EventFundsCaptured {
			return false
		}
		payload, err := events.DecodePayload[events.FundsCapturedPayload](env)
		return err == nil && payload.PaymentID == paymentID
	}
}

func matchReleasedEvent(paymentID string) func(events.Envelope) bool {
	return func(env events.Envelope) bool {
		if env.Type != events.EventFundsReleased {
			return false
		}
		payload, err := events.DecodePayload[events.FundsReleasedPayload](env)
		return err == nil && payload.PaymentID == paymentID
	}
}

func matchRefundedEvent(refundID string) func(events.Envelope) bool {
	return func(env events.Envelope) bool {
		if env.Type != events.EventFundsRefunded {
			return false
		}
		payload, err := events.DecodePayload[events.FundsRefundedPayload](env)
		return err == nil && payload.RefundID == refundID
	}
}
