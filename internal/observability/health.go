package observability

import (
	"context"
	"sync"

	"github.com/besednjak/draftea-payments-challenge/internal/application/ports"
	"github.com/besednjak/draftea-payments-challenge/internal/domain/events"
)

// HealthRegistry coordinates liveness (/healthz) and dependency readiness (/readyz) probes.
type HealthRegistry struct {
	mu       sync.RWMutex
	checkers []ports.HealthChecker
	clock    events.Clock
}

// NewHealthRegistry creates a HealthRegistry with an optional clock.
func NewHealthRegistry(clock events.Clock) *HealthRegistry {
	if clock == nil {
		clock = events.DefaultClock
	}
	return &HealthRegistry{clock: clock}
}

// Register adds a dependency checker to readiness probes.
func (r *HealthRegistry) Register(checker ports.HealthChecker) {
	if checker == nil {
		return
	}
	r.mu.Lock()
	r.checkers = append(r.checkers, checker)
	r.mu.Unlock()
}

// Liveness reports whether the process itself is alive (/healthz).
func (r *HealthRegistry) Liveness() ports.HealthStatus {
	return ports.HealthStatus{
		Name:      "process",
		Healthy:   true,
		Message:   "ok",
		CheckedAt: r.clock().UTC(),
	}
}

// Readiness evaluates all registered dependency health checks (/readyz).
func (r *HealthRegistry) Readiness(ctx context.Context) (bool, []ports.HealthStatus) {
	r.mu.RLock()
	checkers := make([]ports.HealthChecker, len(r.checkers))
	copy(checkers, r.checkers)
	r.mu.RUnlock()

	allHealthy := true
	results := make([]ports.HealthStatus, 0, len(checkers))
	now := r.clock().UTC()

	for _, c := range checkers {
		status := ports.HealthStatus{
			Name:      c.Name(),
			Healthy:   true,
			Message:   "ok",
			CheckedAt: now,
		}
		if err := c.Check(ctx); err != nil {
			status.Healthy = false
			status.Message = err.Error()
			allHealthy = false
		}
		results = append(results, status)
	}
	return allHealthy, results
}
