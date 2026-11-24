package events

import (
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrInvalidAmount is returned when a monetary amount is not strictly positive.
	ErrInvalidAmount = errors.New("amount must be strictly positive")
	// ErrInvalidCurrency is returned when a currency code is not a valid 3-letter ISO-4217 code.
	ErrInvalidCurrency = errors.New("currency must be a 3-letter ISO-4217 code")
	// ErrCurrencyMismatch is returned when an operation mixes two different currencies.
	ErrCurrencyMismatch = errors.New("currency mismatch")
)

// Money represents an immutable monetary value in minor units (e.g., cents)
// paired with its ISO-4217 currency code. Floating-point types are never used.
type Money struct {
	Amount   int64  `json:"amount"`
	Currency string `json:"currency"`
}

// NewMoney validates and constructs a positive Money value.
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
	c := strings.ToUpper(strings.TrimSpace(currency))
	if len(c) != 3 {
		return "", fmt.Errorf("%w: got %q", ErrInvalidCurrency, currency)
	}
	for i := 0; i < 3; i++ {
		if c[i] < 'A' || c[i] > 'Z' {
			return "", fmt.Errorf("%w: got %q", ErrInvalidCurrency, currency)
		}
	}
	return c, nil
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

// String returns a human-readable representation of the monetary value in minor units.
func (m Money) String() string {
	return fmt.Sprintf("%d %s", m.Amount, m.Currency)
}
