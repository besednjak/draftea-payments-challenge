package events_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
)

func TestNewMoney(t *testing.T) {
	t.Run("valid and normalizes currency", func(t *testing.T) {
		m, err := events.NewMoney(15000, " ars ")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if m.Amount != 15000 || m.Currency != "ARS" {
			t.Fatalf("got %+v, want {15000 ARS}", m)
		}
		if m.String() != "15000 ARS" {
			t.Fatalf("got string %q, want %q", m.String(), "15000 ARS")
		}
	})

	cases := []struct {
		name     string
		amount   int64
		currency string
		wantErr  error
	}{
		{"zero amount", 0, "ARS", events.ErrInvalidAmount},
		{"negative amount", -100, "USD", events.ErrInvalidAmount},
		{"empty currency", 100, "", events.ErrInvalidCurrency},
		{"short currency", 100, "US", events.ErrInvalidCurrency},
		{"long currency", 100, "USDT", events.ErrInvalidCurrency},
		{"numeric currency", 100, "123", events.ErrInvalidCurrency},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := events.NewMoney(tc.amount, tc.currency)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("got err=%v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestMoneySameCurrency(t *testing.T) {
	a := events.Money{Amount: 100, Currency: "ARS"}
	b := events.Money{Amount: 500, Currency: "ars"}
	c := events.Money{Amount: 100, Currency: "USD"}

	if !a.SameCurrency(b) {
		t.Fatal("expected ARS and ars to match")
	}
	if a.SameCurrency(c) {
		t.Fatal("expected ARS and USD not to match")
	}
}

func TestNewUUID(t *testing.T) {
	id1 := events.NewUUID()
	id2 := events.NewUUID()

	if len(id1) != 36 {
		t.Fatalf("expected UUID length 36, got %d (%q)", len(id1), id1)
	}
	if id1 == id2 {
		t.Fatalf("expected unique UUIDs, got duplicate %q", id1)
	}
	parts := strings.Split(id1, "-")
	if len(parts) != 5 {
		t.Fatalf("expected 5 hyphen-separated parts, got %q", id1)
	}
	if parts[2][0] != '4' {
		t.Fatalf("expected UUIDv4 nibble '4', got %q", id1)
	}
}

func TestEnvelopeRoundtripAndDeterministicHooks(t *testing.T) {
	fixedTime := time.Date(2026, 11, 24, 15, 0, 0, 0, time.UTC)
	fixedClock := func() time.Time { return fixedTime }
	fixedID := func() string { return "evt-fixed-001" }

	payload := events.FundsReservedPayload{
		PaymentID:      "pay_1",
		Amount:         events.Money{Amount: 2500, Currency: "ARS"},
		BalanceAfter:   10000,
		HeldAfter:      2500,
		AvailableAfter: 7500,
	}

	env, err := events.NewEnvelope(
		events.EventFundsReserved,
		events.AggregateTypeWallet,
		"wal_1",
		3,
		payload,
		events.Metadata{CorrelationID: "pay_1", CausationID: "cmd_1"},
		fixedClock,
		fixedID,
	)
	if err != nil {
		t.Fatalf("NewEnvelope error: %v", err)
	}

	if env.EventID != "evt-fixed-001" {
		t.Fatalf("got EventID=%q, want evt-fixed-001", env.EventID)
	}
	if !env.OccurredAt.Equal(fixedTime) {
		t.Fatalf("got OccurredAt=%v, want %v", env.OccurredAt, fixedTime)
	}
	if env.SchemaVersion != events.DefaultSchemaVersion {
		t.Fatalf("got SchemaVersion=%d, want %d", env.SchemaVersion, events.DefaultSchemaVersion)
	}

	decoded, err := events.DecodePayload[events.FundsReservedPayload](env)
	if err != nil {
		t.Fatalf("DecodePayload error: %v", err)
	}
	if decoded != payload {
		t.Fatalf("decoded=%+v, want %+v", decoded, payload)
	}
}

func TestEnvelopeValidationAndHelpers(t *testing.T) {
	if _, err := events.NewEnvelope("", "wallet", "wal_1", 1, struct{}{}, events.Metadata{}, nil, nil); !errors.Is(err, events.ErrInvalidEnvelope) {
		t.Fatalf("expected ErrInvalidEnvelope for empty eventType, got %v", err)
	}
	if _, err := events.NewEnvelope("wallet.Created", "wallet", "wal_1", -1, struct{}{}, events.Metadata{}, nil, nil); !errors.Is(err, events.ErrInvalidEnvelope) {
		t.Fatalf("expected ErrInvalidEnvelope for negative version, got %v", err)
	}
	if got := events.StreamID(events.AggregateTypeWallet, "wal_123"); got != "wallet-wal_123" {
		t.Fatalf("got StreamID=%q, want wallet-wal_123", got)
	}
	if got := events.DLQTopic(events.TopicWalletsCommands); got != "wallets.commands.v1.dlq" {
		t.Fatalf("got DLQTopic=%q, want wallets.commands.v1.dlq", got)
	}
}
