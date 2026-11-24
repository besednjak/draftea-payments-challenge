package events

const (
	TopicPaymentsEvents  = "payments.events.v1"
	TopicWalletsCommands = "wallets.commands.v1"
	TopicWalletsEvents   = "wallets.events.v1"
	TopicGatewayCommands = "gateway.commands.v1"
	TopicGatewayEvents   = "gateway.events.v1"
)

// DLQTopic returns the dead-letter queue topic name for a source topic.
func DLQTopic(topic string) string {
	return topic + ".dlq"
}

const (
	AggregateTypeWallet  = "wallet"
	AggregateTypePayment = "payment"
	AggregateTypeGateway = "gateway"
)

const (
	ReasonInsufficientFunds = "INSUFFICIENT_FUNDS"
	ReasonCurrencyMismatch  = "CURRENCY_MISMATCH"
	ReasonCircuitOpen       = "CIRCUIT_OPEN"
	ReasonGatewayTimeout    = "GATEWAY_TIMEOUT"
	ReasonGatewayDeclined   = "GATEWAY_DECLINED"
	ReasonReconcileExpired  = "RECONCILIATION_WINDOW_EXPIRED"
)

const (
	CommandReserveFunds       = "wallet.ReserveFunds"
	CommandCaptureFunds       = "wallet.CaptureFunds"
	CommandReleaseFunds       = "wallet.ReleaseFunds"
	CommandRefundFunds        = "wallet.RefundFunds"
	CommandSubmitPayment      = "gateway.SubmitPayment"
	CommandCheckPaymentStatus = "gateway.CheckPaymentStatus"
	CommandRefundPayment      = "gateway.RefundPayment"
)

const (
	EventWalletCreated            = "wallet.WalletCreated"
	EventFundsDeposited           = "wallet.FundsDeposited"
	EventFundsReserved            = "wallet.FundsReserved"
	EventFundsReservationRejected = "wallet.FundsReservationRejected"
	EventFundsCaptured            = "wallet.FundsCaptured"
	EventFundsReleased            = "wallet.FundsReleased"
	EventFundsRefunded            = "wallet.FundsRefunded"
)

const (
	EventPaymentInitiated            = "payment.PaymentInitiated"
	EventPaymentFundsReserved        = "payment.PaymentFundsReserved"
	EventPaymentRejected             = "payment.PaymentRejected"
	EventPaymentSubmitted            = "payment.PaymentSubmitted"
	EventPaymentGatewayUnknown       = "payment.PaymentGatewayUnknown"
	EventPaymentManualReviewRequired = "payment.PaymentManualReviewRequired"
	EventPaymentCompleted            = "payment.PaymentCompleted"
	EventPaymentFailed               = "payment.PaymentFailed"
	EventPaymentRefundRequested      = "payment.PaymentRefundRequested"
	EventPaymentRefundDeclined       = "payment.PaymentRefundDeclined"
	EventPaymentRefunded             = "payment.PaymentRefunded"
)

const (
	EventGatewaySubmissionAccepted = "gateway.GatewaySubmissionAccepted"
	EventGatewaySubmissionNotSent  = "gateway.GatewaySubmissionNotSent"
	EventGatewayOutcomeUnknown     = "gateway.GatewayOutcomeUnknown"
	EventGatewayPaymentApproved    = "gateway.GatewayPaymentApproved"
	EventGatewayPaymentDeclined    = "gateway.GatewayPaymentDeclined"
	EventGatewayStatusPending      = "gateway.GatewayStatusPending"
	EventGatewayRefundApproved     = "gateway.GatewayRefundApproved"
	EventGatewayRefundDeclined     = "gateway.GatewayRefundDeclined"
)

type ReserveFundsCommand struct {
	PaymentID string `json:"payment_id"`
	WalletID  string `json:"wallet_id"`
	Amount    Money  `json:"amount"`
}

type CaptureFundsCommand struct {
	PaymentID   string `json:"payment_id"`
	WalletID    string `json:"wallet_id"`
	ProviderRef string `json:"provider_ref,omitempty"`
	Amount      Money  `json:"amount"`
}

type ReleaseFundsCommand struct {
	PaymentID string `json:"payment_id"`
	WalletID  string `json:"wallet_id"`
	Reason    string `json:"reason"`
}

type RefundFundsCommand struct {
	PaymentID string `json:"payment_id"`
	RefundID  string `json:"refund_id"`
	WalletID  string `json:"wallet_id"`
	Amount    Money  `json:"amount"`
}

type SubmitPaymentCommand struct {
	PaymentID string `json:"payment_id"`
	Provider  string `json:"provider"`
	ServiceID string `json:"service_id"`
	Amount    Money  `json:"amount"`
}

type CheckPaymentStatusCommand struct {
	PaymentID string `json:"payment_id"`
	Provider  string `json:"provider"`
}

type RefundPaymentCommand struct {
	PaymentID   string `json:"payment_id"`
	RefundID    string `json:"refund_id"`
	Provider    string `json:"provider"`
	ProviderRef string `json:"provider_ref"`
	Amount      Money  `json:"amount"`
}

type WalletCreatedPayload struct {
	WalletID string `json:"wallet_id"`
	UserID   string `json:"user_id"`
	Currency string `json:"currency"`
}

type FundsDepositedPayload struct {
	WalletID       string `json:"wallet_id"`
	ReferenceID    string `json:"reference_id,omitempty"`
	Amount         Money  `json:"amount"`
	BalanceAfter   int64  `json:"balance_after"`
	HeldAfter      int64  `json:"held_after"`
	AvailableAfter int64  `json:"available_after"`
}

type FundsReservedPayload struct {
	PaymentID      string `json:"payment_id"`
	Amount         Money  `json:"amount"`
	BalanceAfter   int64  `json:"balance_after"`
	HeldAfter      int64  `json:"held_after"`
	AvailableAfter int64  `json:"available_after"`
}

type FundsReservationRejectedPayload struct {
	PaymentID        string `json:"payment_id"`
	Amount           Money  `json:"amount"`
	Reason           string `json:"reason"`
	AvailableBalance int64  `json:"available_balance"`
}

type FundsCapturedPayload struct {
	PaymentID      string `json:"payment_id"`
	ProviderRef    string `json:"provider_ref,omitempty"`
	Amount         Money  `json:"amount"`
	BalanceAfter   int64  `json:"balance_after"`
	HeldAfter      int64  `json:"held_after"`
	AvailableAfter int64  `json:"available_after"`
}

type FundsReleasedPayload struct {
	PaymentID      string `json:"payment_id"`
	Amount         Money  `json:"amount"`
	Reason         string `json:"reason"`
	BalanceAfter   int64  `json:"balance_after"`
	HeldAfter      int64  `json:"held_after"`
	AvailableAfter int64  `json:"available_after"`
}

type FundsRefundedPayload struct {
	PaymentID      string `json:"payment_id"`
	RefundID       string `json:"refund_id"`
	Amount         Money  `json:"amount"`
	BalanceAfter   int64  `json:"balance_after"`
	HeldAfter      int64  `json:"held_after"`
	AvailableAfter int64  `json:"available_after"`
}

type PaymentInitiatedPayload struct {
	PaymentID string `json:"payment_id"`
	WalletID  string `json:"wallet_id"`
	UserID    string `json:"user_id"`
	ServiceID string `json:"service_id"`
	Provider  string `json:"provider"`
	Amount    Money  `json:"amount"`
}

type PaymentFundsReservedPayload struct {
	PaymentID      string `json:"payment_id"`
	WalletID       string `json:"wallet_id"`
	Amount         Money  `json:"amount"`
	AvailableAfter int64  `json:"available_after"`
}

type PaymentRejectedPayload struct {
	PaymentID string `json:"payment_id"`
	Reason    string `json:"reason"`
}

type PaymentSubmittedPayload struct {
	PaymentID   string `json:"payment_id"`
	Provider    string `json:"provider"`
	ProviderRef string `json:"provider_ref"`
}

type PaymentGatewayUnknownPayload struct {
	PaymentID string `json:"payment_id"`
	Provider  string `json:"provider"`
	Reason    string `json:"reason"`
}

type PaymentManualReviewRequiredPayload struct {
	PaymentID string `json:"payment_id"`
	Reason    string `json:"reason"`
}

type PaymentCompletedPayload struct {
	PaymentID      string `json:"payment_id"`
	ProviderRef    string `json:"provider_ref"`
	CapturedAmount int64  `json:"captured_amount"`
	Currency       string `json:"currency"`
}

type PaymentFailedPayload struct {
	PaymentID string `json:"payment_id"`
	Reason    string `json:"reason"`
}

type PaymentRefundRequestedPayload struct {
	PaymentID string `json:"payment_id"`
	RefundID  string `json:"refund_id"`
	Amount    Money  `json:"amount"`
}

type PaymentRefundDeclinedPayload struct {
	PaymentID string `json:"payment_id"`
	RefundID  string `json:"refund_id"`
	Reason    string `json:"reason"`
}

type PaymentRefundedPayload struct {
	PaymentID     string `json:"payment_id"`
	RefundID      string `json:"refund_id"`
	Amount        Money  `json:"amount"`
	RefundedTotal int64  `json:"refunded_total"`
	FullyRefunded bool   `json:"fully_refunded"`
}

type GatewaySubmissionAcceptedPayload struct {
	PaymentID   string `json:"payment_id"`
	Provider    string `json:"provider"`
	ProviderRef string `json:"provider_ref"`
}

type GatewaySubmissionNotSentPayload struct {
	PaymentID string `json:"payment_id"`
	Provider  string `json:"provider"`
	Reason    string `json:"reason"`
}

type GatewayOutcomeUnknownPayload struct {
	PaymentID string `json:"payment_id"`
	Provider  string `json:"provider"`
	Reason    string `json:"reason"`
}

type GatewayPaymentApprovedPayload struct {
	PaymentID   string `json:"payment_id"`
	Provider    string `json:"provider"`
	ProviderRef string `json:"provider_ref"`
}

type GatewayPaymentDeclinedPayload struct {
	PaymentID string `json:"payment_id"`
	Provider  string `json:"provider"`
	Reason    string `json:"reason"`
}

type GatewayStatusPendingPayload struct {
	PaymentID string `json:"payment_id"`
	Provider  string `json:"provider"`
}

type GatewayRefundApprovedPayload struct {
	PaymentID   string `json:"payment_id"`
	RefundID    string `json:"refund_id"`
	Provider    string `json:"provider"`
	ProviderRef string `json:"provider_ref"`
	Amount      Money  `json:"amount"`
}

type GatewayRefundDeclinedPayload struct {
	PaymentID string `json:"payment_id"`
	RefundID  string `json:"refund_id"`
	Provider  string `json:"provider"`
	Reason    string `json:"reason"`
}
