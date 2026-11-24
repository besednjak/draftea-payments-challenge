package observability

import (
	"context"
	"maps"
	"sync"
	"time"

	"github.com/besednjak/draftea-payments-challenge/internal/application/ports"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/payment"
)

// MetricsReport is an immutable snapshot of business and operational metrics.
type MetricsReport struct {
	PaymentsInitiated      int64             `json:"payments_initiated"`
	PaymentsCompleted      int64             `json:"payments_completed"`
	PaymentsFailed         int64             `json:"payments_failed"`
	PaymentsRejected       int64             `json:"payments_rejected"`
	PaymentsGatewayUnknown int64             `json:"payments_gateway_unknown"`
	PaymentsManualReview   int64             `json:"payments_manual_review"`
	PaymentsRefunded       int64             `json:"payments_refunded"`
	InsufficientFundsCount int64             `json:"insufficient_funds_count"`
	SuccessRate            float64           `json:"success_rate"`
	VolumeByCurrency       map[string]int64  `json:"volume_by_currency"`
	RefundedByCurrency     map[string]int64  `json:"refunded_by_currency"`
	GatewayAttempts        map[string]int64  `json:"gateway_attempts"`
	CircuitBreakerStates   map[string]string `json:"circuit_breaker_states"`
	DLQMessagesByTopic     map[string]int64  `json:"dlq_messages_by_topic"`
	OptimisticLockRetries  map[string]int64  `json:"optimistic_lock_retries"`
	LatencySamples         int64             `json:"latency_samples"`
	AvgPaymentLatency      time.Duration     `json:"avg_payment_latency"`
	MaxPaymentLatency      time.Duration     `json:"max_payment_latency"`
}

// Collector implements ports.MetricsRecorder and acts as the choreographed Metrics
// Service consumer reacting to domain events without blocking the payment path.
type Collector struct {
	mu                     sync.RWMutex
	paymentsInitiated      int64
	paymentsCompleted      int64
	paymentsFailed         int64
	paymentsRejected       int64
	paymentsGatewayUnknown int64
	paymentsManualReview   int64
	paymentsRefunded       int64
	insufficientFundsCount int64
	volumeByCurrency       map[string]int64
	refundedByCurrency     map[string]int64
	gatewayAttempts        map[string]int64
	circuitBreakerStates   map[string]string
	dlqMessagesByTopic     map[string]int64
	optimisticLockRetries  map[string]int64
	latencySamples         int64
	totalLatency           time.Duration
	maxLatency             time.Duration
	paymentStartedAt       map[string]time.Time
}

// NewCollector initializes a thread-safe Metrics Collector.
func NewCollector() *Collector {
	return &Collector{
		volumeByCurrency:      make(map[string]int64),
		refundedByCurrency:    make(map[string]int64),
		gatewayAttempts:       make(map[string]int64),
		circuitBreakerStates:  make(map[string]string),
		dlqMessagesByTopic:    make(map[string]int64),
		optimisticLockRetries: make(map[string]int64),
		paymentStartedAt:      make(map[string]time.Time),
	}
}

// HandleEvent consumes domain events from payments.events.v1, wallets.events.v1,
// and gateway.events.v1 in a choreographed fashion.
func (c *Collector) HandleEvent(_ context.Context, env events.Envelope) error {
	switch env.Type {
	case events.EventPaymentInitiated:
		c.mu.Lock()
		c.paymentsInitiated++
		c.paymentStartedAt[env.AggregateID] = env.OccurredAt
		c.mu.Unlock()

	case events.EventPaymentCompleted:
		payload, err := events.DecodePayload[events.PaymentCompletedPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		c.RecordPaymentOutcome(string(payment.StatusCompleted), payload.CapturedAmount, payload.Currency)
		c.observeEndToEndLatency(env.AggregateID, env.OccurredAt)

	case events.EventPaymentFailed:
		c.RecordPaymentOutcome(string(payment.StatusFailed), 0, "")
		c.observeEndToEndLatency(env.AggregateID, env.OccurredAt)

	case events.EventPaymentRejected:
		c.RecordPaymentOutcome(string(payment.StatusRejected), 0, "")
		c.observeEndToEndLatency(env.AggregateID, env.OccurredAt)

	case events.EventPaymentGatewayUnknown:
		c.mu.Lock()
		c.paymentsGatewayUnknown++
		c.mu.Unlock()

	case events.EventPaymentManualReviewRequired:
		c.mu.Lock()
		c.paymentsManualReview++
		c.mu.Unlock()

	case events.EventPaymentRefunded:
		payload, err := events.DecodePayload[events.PaymentRefundedPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		c.RecordRefund(payload.Amount.Amount, payload.Amount.Currency)

	case events.EventFundsReservationRejected:
		payload, err := events.DecodePayload[events.FundsReservationRejectedPayload](env)
		if err != nil {
			return ports.NonRetryable(err)
		}
		if payload.Reason == events.ReasonInsufficientFunds {
			c.RecordInsufficientFunds(env.AggregateID)
		}
	}
	return nil
}

func (c *Collector) observeEndToEndLatency(paymentID string, finishedAt time.Time) {
	c.mu.Lock()
	startedAt, exists := c.paymentStartedAt[paymentID]
	if exists {
		delete(c.paymentStartedAt, paymentID)
	}
	c.mu.Unlock()

	if exists && !finishedAt.Before(startedAt) {
		c.RecordPaymentLatency(finishedAt.Sub(startedAt))
	}
}

func (c *Collector) RecordPaymentOutcome(status string, amount int64, currency string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch payment.Status(status) {
	case payment.StatusCompleted:
		c.paymentsCompleted++
		if amount > 0 && currency != "" {
			c.volumeByCurrency[currency] += amount
		}
	case payment.StatusFailed:
		c.paymentsFailed++
	case payment.StatusRejected:
		c.paymentsRejected++
	}
}

func (c *Collector) RecordPaymentLatency(elapsed time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.latencySamples++
	c.totalLatency += elapsed
	if elapsed > c.maxLatency {
		c.maxLatency = elapsed
	}
}

func (c *Collector) RecordInsufficientFunds(_ string) {
	c.mu.Lock()
	c.insufficientFundsCount++
	c.mu.Unlock()
}

func (c *Collector) RecordRefund(amount int64, currency string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.paymentsRefunded++
	if amount > 0 && currency != "" {
		c.refundedByCurrency[currency] += amount
	}
}

func (c *Collector) RecordGatewayAttempt(provider, outcome string, _ time.Duration) {
	c.mu.Lock()
	c.gatewayAttempts[provider+":"+outcome]++
	c.mu.Unlock()
}

func (c *Collector) RecordCircuitBreakerState(provider, state string) {
	c.mu.Lock()
	c.circuitBreakerStates[provider] = state
	c.mu.Unlock()
}

func (c *Collector) RecordDLQMessage(topic string) {
	c.mu.Lock()
	c.dlqMessagesByTopic[topic]++
	c.mu.Unlock()
}

func (c *Collector) RecordOptimisticLockRetry(streamType string) {
	c.mu.Lock()
	c.optimisticLockRetries[streamType]++
	c.mu.Unlock()
}

// Snapshot returns a deep-copied MetricsReport safe for concurrent reading.
func (c *Collector) Snapshot() MetricsReport {
	c.mu.RLock()
	defer c.mu.RUnlock()

	settled := c.paymentsCompleted + c.paymentsFailed + c.paymentsRejected
	var successRate float64
	if settled > 0 {
		successRate = float64(c.paymentsCompleted) / float64(settled)
	}
	var avgLatency time.Duration
	if c.latencySamples > 0 {
		avgLatency = c.totalLatency / time.Duration(c.latencySamples)
	}

	report := MetricsReport{
		PaymentsInitiated:      c.paymentsInitiated,
		PaymentsCompleted:      c.paymentsCompleted,
		PaymentsFailed:         c.paymentsFailed,
		PaymentsRejected:       c.paymentsRejected,
		PaymentsGatewayUnknown: c.paymentsGatewayUnknown,
		PaymentsManualReview:   c.paymentsManualReview,
		PaymentsRefunded:       c.paymentsRefunded,
		InsufficientFundsCount: c.insufficientFundsCount,
		SuccessRate:            successRate,
		VolumeByCurrency:       make(map[string]int64, len(c.volumeByCurrency)),
		RefundedByCurrency:     make(map[string]int64, len(c.refundedByCurrency)),
		GatewayAttempts:        make(map[string]int64, len(c.gatewayAttempts)),
		CircuitBreakerStates:   make(map[string]string, len(c.circuitBreakerStates)),
		DLQMessagesByTopic:     make(map[string]int64, len(c.dlqMessagesByTopic)),
		OptimisticLockRetries:  make(map[string]int64, len(c.optimisticLockRetries)),
		LatencySamples:         c.latencySamples,
		AvgPaymentLatency:      avgLatency,
		MaxPaymentLatency:      c.maxLatency,
	}
	maps.Copy(report.VolumeByCurrency, c.volumeByCurrency)
	maps.Copy(report.RefundedByCurrency, c.refundedByCurrency)
	maps.Copy(report.GatewayAttempts, c.gatewayAttempts)
	maps.Copy(report.CircuitBreakerStates, c.circuitBreakerStates)
	maps.Copy(report.DLQMessagesByTopic, c.dlqMessagesByTopic)
	maps.Copy(report.OptimisticLockRetries, c.optimisticLockRetries)
	return report
}
