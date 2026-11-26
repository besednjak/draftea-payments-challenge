package gateway

import (
	"context"
	"math/rand/v2"
	"time"
)

// RetryConfig configures exponential backoff with full jitter for outbound gateway calls.
type RetryConfig struct {
	MaxAttempts    int
	BaseDelay      time.Duration
	MaxDelay       time.Duration
	AttemptTimeout time.Duration
	Sleep          func(ctx context.Context, d time.Duration) error
	RandFloat      func() float64
}

func (c RetryConfig) withDefaults() RetryConfig {
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 3
	}
	if c.BaseDelay <= 0 {
		c.BaseDelay = 500 * time.Millisecond
	}
	if c.MaxDelay <= 0 {
		c.MaxDelay = 10 * time.Second
	}
	if c.AttemptTimeout <= 0 {
		c.AttemptTimeout = 10 * time.Second
	}
	if c.Sleep == nil {
		c.Sleep = defaultSleep
	}
	if c.RandFloat == nil {
		c.RandFloat = rand.Float64
	}
	return c
}

// FullJitterDelay computes random(0, min(cap, base * 2^attempt)) where attempt starts at 0.
func FullJitterDelay(attempt int, base, capDelay time.Duration, randFloat func() float64) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	if randFloat == nil {
		randFloat = rand.Float64
	}

	limit := base
	for i := 0; i < attempt; i++ {
		if limit >= capDelay/2 {
			limit = capDelay
			break
		}
		limit *= 2
	}
	if limit > capDelay {
		limit = capDelay
	}
	if limit <= 0 {
		return 0
	}

	factor := randFloat()
	if factor < 0 {
		factor = 0
	}
	if factor > 1 {
		factor = 1
	}
	return time.Duration(factor * float64(limit))
}

func defaultSleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
