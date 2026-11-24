package ports

import (
	"context"

	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
)

// GatewayOutcome classifies the normalized technical outcome returned by an external provider.
type GatewayOutcome string

const (
	GatewayOutcomeAccepted GatewayOutcome = "ACCEPTED"
	GatewayOutcomeApproved GatewayOutcome = "APPROVED"
	GatewayOutcomeDeclined GatewayOutcome = "DECLINED"
	GatewayOutcomePending  GatewayOutcome = "PENDING"
	GatewayOutcomeNotFound GatewayOutcome = "NOT_FOUND"
)

type GatewaySubmitRequest struct {
	PaymentID string
	Provider  string
	ServiceID string
	Amount    events.Money
}

type GatewaySubmitResult struct {
	Outcome     GatewayOutcome
	ProviderRef string
	Reason      string
}

type GatewayStatusResult struct {
	Outcome     GatewayOutcome
	ProviderRef string
	Reason      string
}

type GatewayRefundRequest struct {
	PaymentID   string
	RefundID    string
	Provider    string
	ProviderRef string
	Amount      events.Money
}

type GatewayRefundResult struct {
	Approved    bool
	ProviderRef string
	Reason      string
}

// PaymentGateway is the driven port abstracting the external payment provider.
// Implementations use PaymentID / RefundID as idempotency keys toward the provider.
type PaymentGateway interface {
	SubmitPayment(ctx context.Context, req GatewaySubmitRequest) (GatewaySubmitResult, error)
	CheckStatus(ctx context.Context, paymentID, provider string) (GatewayStatusResult, error)
	RefundPayment(ctx context.Context, req GatewayRefundRequest) (GatewayRefundResult, error)
}
