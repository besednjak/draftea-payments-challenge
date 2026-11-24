package events

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrInvalidAmount    = errors.New("amount must be strictly positive")
	ErrInvalidCurrency  = errors.New("currency must be a 3-letter ISO-4217 code")
	ErrCurrencyMismatch = errors.New("currency mismatch")
)

// Money represents an immutable monetary value in minor units (cents) paired with
// its ISO-4217 currency code. Floating-point types are never used for money.
type Money struct {
	Amount   int64  `json:"amount"`
	Currency string `json:"currency"`
}

// NewMoney validates and constructs a strictly positive Money value.
func NewMoney(amount int64, currency string) (Money, error) {
	if amount <= 0 {
		return Money{}, fmt.Errorf("%w: got %d", ErrInvalidAmount, amount)
	}
	normalized, err := NormalizeCurrency(currency)
	if err != nil {
		return Money{}, err
	}
	return Money{
		Amount:   amount,
		Currency: normalized,
	}, nil
}

// NormalizeCurrency trims, uppercases, and validates an ISO-4217 3-letter currency code.
func NormalizeCurrency(currency string) (string, error) {
	normalized := strings.ToUpper(strings.TrimSpace(currency))
	if len(normalized) != 3 {
		return "", fmt.Errorf("%w: got %q", ErrInvalidCurrency, currency)
	}
	for i := 0; i < 3; i++ {
		if normalized[i] < 'A' || normalized[i] > 'Z' {
			return "", fmt.Errorf("%w: got %q", ErrInvalidCurrency, currency)
		}
	}
	return normalized, nil
}

// Validate checks that the Money value has a strictly positive amount and valid currency.
func (m Money) Validate() error {
	_, err := NewMoney(m.Amount, m.Currency)
	return err
}

// SameCurrency reports whether m and other share the same normalized currency code.
func (m Money) SameCurrency(other Money) bool {
	return strings.EqualFold(m.Currency, other.Currency)
}

// String returns a human-readable representation of the value in minor units.
func (m Money) String() string {
	return fmt.Sprintf("%d %s", m.Amount, m.Currency)
}
