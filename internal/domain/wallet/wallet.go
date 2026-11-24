package wallet

import (
	"fmt"
	"maps"

	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
)

// Option configures non-persistent dependencies (clock, ID generator) for deterministic tests.
type Option func(*Wallet)

// WithClock overrides the clock used to timestamp newly emitted events.
func WithClock(clock events.Clock) Option {
	return func(w *Wallet) {
		if clock != nil {
			w.clock = clock
		}
	}
}

// WithIDGenerator overrides the ID generator used for newly emitted events.
func WithIDGenerator(gen events.IDGenerator) Option {
	return func(w *Wallet) {
		if gen != nil {
			w.idGen = gen
		}
	}
}

// Snapshot captures the full Wallet aggregate state at a given version so long
// event streams do not need to be replayed from version 1.
type Snapshot struct {
	WalletID     string           `json:"wallet_id"`
	UserID       string           `json:"user_id"`
	Currency     string           `json:"currency"`
	Balance      int64            `json:"balance"`
	Held         int64            `json:"held"`
	Version      int64            `json:"version"`
	Deposits     map[string]bool  `json:"deposits,omitempty"`
	Reservations map[string]int64 `json:"reservations,omitempty"`
	Captured     map[string]int64 `json:"captured,omitempty"`
	Released     map[string]bool  `json:"released,omitempty"`
	Refunds      map[string]int64 `json:"refunds,omitempty"`
}

// Wallet is the Event-Sourced aggregate that acts as the source of truth for a
// user's funds in a single currency. Invariant: Available() == Balance - Held >= 0.
type Wallet struct {
	id       string
	userID   string
	currency string
	balance  int64
	held     int64
	version  int64
	created  bool

	deposits     map[string]bool
	reservations map[string]int64
	captured     map[string]int64
	released     map[string]bool
	refunds      map[string]int64

	uncommitted []events.Envelope
	clock       events.Clock
	idGen       events.IDGenerator
}

func newEmpty(opts ...Option) *Wallet {
	w := &Wallet{
		deposits:     make(map[string]bool),
		reservations: make(map[string]int64),
		captured:     make(map[string]int64),
		released:     make(map[string]bool),
		refunds:      make(map[string]int64),
		clock:        events.DefaultClock,
		idGen:        events.NewUUID,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(w)
		}
	}
	return w
}

// Create initializes a new Wallet aggregate and emits WalletCreated.
func Create(walletID, userID, currency string, meta events.Metadata, opts ...Option) (*Wallet, error) {
	if walletID == "" {
		return nil, ErrInvalidWalletID
	}
	if userID == "" {
		return nil, ErrInvalidUserID
	}
	normCurrency, err := events.NormalizeCurrency(currency)
	if err != nil {
		return nil, err
	}

	w := newEmpty(opts...)
	payload := events.WalletCreatedPayload{
		WalletID: walletID,
		UserID:   userID,
		Currency: normCurrency,
	}
	if err := w.record(events.EventWalletCreated, walletID, payload, meta); err != nil {
		return nil, err
	}
	return w, nil
}

// Rehydrate rebuilds a Wallet from an optional snapshot plus subsequent events.
func Rehydrate(snap *Snapshot, history []events.Envelope, opts ...Option) (*Wallet, error) {
	w := newEmpty(opts...)
	if snap != nil {
		if snap.WalletID == "" {
			return nil, ErrInvalidWalletID
		}
		w.id = snap.WalletID
		w.userID = snap.UserID
		w.currency = snap.Currency
		w.balance = snap.Balance
		w.held = snap.Held
		w.version = snap.Version
		w.created = true
		maps.Copy(w.deposits, snap.Deposits)
		maps.Copy(w.reservations, snap.Reservations)
		maps.Copy(w.captured, snap.Captured)
		maps.Copy(w.released, snap.Released)
		maps.Copy(w.refunds, snap.Refunds)
	}

	for _, env := range history {
		if snap != nil && env.AggregateVersion <= snap.Version {
			continue
		}
		if err := w.Apply(env); err != nil {
			return nil, err
		}
	}

	if !w.created {
		return nil, ErrWalletNotCreated
	}
	return w, nil
}

// Deposit credits funds to the wallet. If referenceID is non-empty and was already
// applied, the call is an idempotent no-op.
func (w *Wallet) Deposit(referenceID string, amount events.Money, meta events.Metadata) error {
	if !w.created {
		return ErrWalletNotCreated
	}
	money, err := events.NewMoney(amount.Amount, amount.Currency)
	if err != nil {
		return err
	}
	if money.Currency != w.currency {
		return fmt.Errorf("%w: wallet=%s deposit=%s", ErrCurrencyMismatch, w.currency, money.Currency)
	}
	if referenceID != "" && w.deposits[referenceID] {
		return nil
	}

	balanceAfter := w.balance + money.Amount
	payload := events.FundsDepositedPayload{
		WalletID:       w.id,
		ReferenceID:    referenceID,
		Amount:         money,
		BalanceAfter:   balanceAfter,
		HeldAfter:      w.held,
		AvailableAfter: balanceAfter - w.held,
	}
	return w.record(events.EventFundsDeposited, w.id, payload, meta)
}

// Reserve places a hold for paymentID.
//   - Idempotent if paymentID was already reserved, captured, or released.
//   - Emits FundsReservationRejected if currency mismatches or available funds are insufficient.
//   - Emits FundsReserved otherwise.
func (w *Wallet) Reserve(paymentID string, amount events.Money, meta events.Metadata) error {
	if !w.created {
		return ErrWalletNotCreated
	}
	if paymentID == "" {
		return ErrInvalidPaymentID
	}
	money, err := events.NewMoney(amount.Amount, amount.Currency)
	if err != nil {
		return err
	}

	if _, ok := w.reservations[paymentID]; ok {
		return nil
	}
	if _, ok := w.captured[paymentID]; ok {
		return nil
	}
	if w.released[paymentID] {
		return nil
	}

	if money.Currency != w.currency {
		payload := events.FundsReservationRejectedPayload{
			PaymentID:        paymentID,
			Amount:           money,
			Reason:           events.ReasonCurrencyMismatch,
			AvailableBalance: w.Available(),
		}
		return w.record(events.EventFundsReservationRejected, w.id, payload, meta)
	}

	if w.Available() < money.Amount {
		payload := events.FundsReservationRejectedPayload{
			PaymentID:        paymentID,
			Amount:           money,
			Reason:           events.ReasonInsufficientFunds,
			AvailableBalance: w.Available(),
		}
		return w.record(events.EventFundsReservationRejected, w.id, payload, meta)
	}

	heldAfter := w.held + money.Amount
	payload := events.FundsReservedPayload{
		PaymentID:      paymentID,
		Amount:         money,
		BalanceAfter:   w.balance,
		HeldAfter:      heldAfter,
		AvailableAfter: w.balance - heldAfter,
	}
	return w.record(events.EventFundsReserved, w.id, payload, meta)
}

// Capture permanently deducts an active reservation for paymentID.
// Idempotent if paymentID was already captured.
func (w *Wallet) Capture(paymentID string, amount events.Money, meta events.Metadata) error {
	if !w.created {
		return ErrWalletNotCreated
	}
	if paymentID == "" {
		return ErrInvalidPaymentID
	}
	money, err := events.NewMoney(amount.Amount, amount.Currency)
	if err != nil {
		return err
	}
	if _, ok := w.captured[paymentID]; ok {
		return nil
	}
	if money.Currency != w.currency {
		return fmt.Errorf("%w: wallet=%s capture=%s", ErrCurrencyMismatch, w.currency, money.Currency)
	}

	reserved, ok := w.reservations[paymentID]
	if !ok {
		return fmt.Errorf("%w: payment_id=%s", ErrReservationNotFound, paymentID)
	}
	if reserved != money.Amount {
		return fmt.Errorf("%w: reserved=%d requested=%d", ErrCaptureAmountMismatch, reserved, money.Amount)
	}

	balanceAfter := w.balance - money.Amount
	heldAfter := w.held - money.Amount
	payload := events.FundsCapturedPayload{
		PaymentID:      paymentID,
		Amount:         money,
		BalanceAfter:   balanceAfter,
		HeldAfter:      heldAfter,
		AvailableAfter: balanceAfter - heldAfter,
	}
	return w.record(events.EventFundsCaptured, w.id, payload, meta)
}

// Release frees an active reservation (compensating action).
// Idempotent if paymentID was already released.
func (w *Wallet) Release(paymentID, reason string, meta events.Metadata) error {
	if !w.created {
		return ErrWalletNotCreated
	}
	if paymentID == "" {
		return ErrInvalidPaymentID
	}
	if w.released[paymentID] {
		return nil
	}
	if _, ok := w.captured[paymentID]; ok {
		return fmt.Errorf("%w: payment_id=%s", ErrReservationAlreadyCaptured, paymentID)
	}

	reserved, ok := w.reservations[paymentID]
	if !ok {
		return fmt.Errorf("%w: payment_id=%s", ErrReservationNotFound, paymentID)
	}

	heldAfter := w.held - reserved
	payload := events.FundsReleasedPayload{
		PaymentID:      paymentID,
		Amount:         events.Money{Amount: reserved, Currency: w.currency},
		Reason:         reason,
		BalanceAfter:   w.balance,
		HeldAfter:      heldAfter,
		AvailableAfter: w.balance - heldAfter,
	}
	return w.record(events.EventFundsReleased, w.id, payload, meta)
}

// Refund credits a refund amount back into the wallet balance.
// Idempotent per refundID.
func (w *Wallet) Refund(paymentID, refundID string, amount events.Money, meta events.Metadata) error {
	if !w.created {
		return ErrWalletNotCreated
	}
	if paymentID == "" {
		return ErrInvalidPaymentID
	}
	if refundID == "" {
		return ErrInvalidRefundID
	}
	money, err := events.NewMoney(amount.Amount, amount.Currency)
	if err != nil {
		return err
	}
	if money.Currency != w.currency {
		return fmt.Errorf("%w: wallet=%s refund=%s", ErrCurrencyMismatch, w.currency, money.Currency)
	}
	if _, ok := w.refunds[refundID]; ok {
		return nil
	}

	balanceAfter := w.balance + money.Amount
	payload := events.FundsRefundedPayload{
		PaymentID:      paymentID,
		RefundID:       refundID,
		Amount:         money,
		BalanceAfter:   balanceAfter,
		HeldAfter:      w.held,
		AvailableAfter: balanceAfter - w.held,
	}
	return w.record(events.EventFundsRefunded, w.id, payload, meta)
}

// Apply transitions the aggregate state by applying a persisted or newly recorded event.
func (w *Wallet) Apply(env events.Envelope) error {
	expectedVersion := w.version + 1
	if env.AggregateVersion != expectedVersion {
		return fmt.Errorf("%w: expected %d, got %d", ErrEventVersionMismatch, expectedVersion, env.AggregateVersion)
	}

	switch env.Type {
	case events.EventWalletCreated:
		if w.created {
			return ErrWalletAlreadyCreated
		}
		p, err := events.DecodePayload[events.WalletCreatedPayload](env)
		if err != nil {
			return err
		}
		w.id = p.WalletID
		w.userID = p.UserID
		w.currency = p.Currency
		w.balance = 0
		w.held = 0
		w.created = true

	case events.EventFundsDeposited:
		p, err := events.DecodePayload[events.FundsDepositedPayload](env)
		if err != nil {
			return err
		}
		w.balance += p.Amount.Amount
		if p.ReferenceID != "" {
			w.deposits[p.ReferenceID] = true
		}

	case events.EventFundsReserved:
		p, err := events.DecodePayload[events.FundsReservedPayload](env)
		if err != nil {
			return err
		}
		w.held += p.Amount.Amount
		w.reservations[p.PaymentID] = p.Amount.Amount

	case events.EventFundsReservationRejected:
		if _, err := events.DecodePayload[events.FundsReservationRejectedPayload](env); err != nil {
			return err
		}
		// Balance and held remain unchanged.

	case events.EventFundsCaptured:
		p, err := events.DecodePayload[events.FundsCapturedPayload](env)
		if err != nil {
			return err
		}
		w.balance -= p.Amount.Amount
		w.held -= p.Amount.Amount
		delete(w.reservations, p.PaymentID)
		w.captured[p.PaymentID] = p.Amount.Amount

	case events.EventFundsReleased:
		p, err := events.DecodePayload[events.FundsReleasedPayload](env)
		if err != nil {
			return err
		}
		w.held -= p.Amount.Amount
		delete(w.reservations, p.PaymentID)
		w.released[p.PaymentID] = true

	case events.EventFundsRefunded:
		p, err := events.DecodePayload[events.FundsRefundedPayload](env)
		if err != nil {
			return err
		}
		w.balance += p.Amount.Amount
		w.refunds[p.RefundID] = p.Amount.Amount

	default:
		return fmt.Errorf("%w: %s", ErrUnknownEventType, env.Type)
	}

	w.version = env.AggregateVersion
	return nil
}

func (w *Wallet) record(eventType, aggregateID string, payload any, meta events.Metadata) error {
	env, err := events.NewEnvelope(
		eventType,
		events.AggregateTypeWallet,
		aggregateID,
		w.version+1,
		payload,
		meta,
		w.clock,
		w.idGen,
	)
	if err != nil {
		return err
	}
	if err := w.Apply(env); err != nil {
		return err
	}
	w.uncommitted = append(w.uncommitted, env)
	return nil
}

// TakeSnapshot returns a deep copy of the current aggregate state for snapshot persistence.
func (w *Wallet) TakeSnapshot() Snapshot {
	snap := Snapshot{
		WalletID:     w.id,
		UserID:       w.userID,
		Currency:     w.currency,
		Balance:      w.balance,
		Held:         w.held,
		Version:      w.version,
		Deposits:     make(map[string]bool, len(w.deposits)),
		Reservations: make(map[string]int64, len(w.reservations)),
		Captured:     make(map[string]int64, len(w.captured)),
		Released:     make(map[string]bool, len(w.released)),
		Refunds:      make(map[string]int64, len(w.refunds)),
	}
	maps.Copy(snap.Deposits, w.deposits)
	maps.Copy(snap.Reservations, w.reservations)
	maps.Copy(snap.Captured, w.captured)
	maps.Copy(snap.Released, w.released)
	maps.Copy(snap.Refunds, w.refunds)
	return snap
}

// --- Accessors ---

func (w *Wallet) ID() string        { return w.id }
func (w *Wallet) UserID() string    { return w.userID }
func (w *Wallet) Currency() string  { return w.currency }
func (w *Wallet) Balance() int64    { return w.balance }
func (w *Wallet) Held() int64       { return w.held }
func (w *Wallet) Available() int64  { return w.balance - w.held }
func (w *Wallet) Version() int64    { return w.version }
func (w *Wallet) StreamID() string  { return events.StreamID(events.AggregateTypeWallet, w.id) }

// BaseVersion returns the aggregate version prior to any uncommitted events.
// Pass this value as expectedVersion to EventStore.Append for optimistic locking.
func (w *Wallet) BaseVersion() int64 {
	return w.version - int64(len(w.uncommitted))
}

// HasReservation reports whether paymentID currently has an active hold.
func (w *Wallet) HasReservation(paymentID string) (int64, bool) {
	amt, ok := w.reservations[paymentID]
	return amt, ok
}

// UncommittedEvents returns a copy of the events recorded since the last ClearUncommitted.
func (w *Wallet) UncommittedEvents() []events.Envelope {
	out := make([]events.Envelope, len(w.uncommitted))
	copy(out, w.uncommitted)
	return out
}

// ClearUncommitted empties the pending event buffer after persistence.
func (w *Wallet) ClearUncommitted() {
	w.uncommitted = nil
}
