package metrics

import (
	"context"
	"log/slog"
	"time"
)

// StatsStore supplies the values for gauge metrics sampled from Postgres.
type StatsStore interface {
	CountActiveTasksByState(ctx context.Context) (map[string]int, error)
	CountAliveWorkers(ctx context.Context) (int, error)
	CountPendingOutbox(ctx context.Context) (int, error)
}

// activeStates are always reported (0 when absent) so a gauge doesn't go stale.
var activeStates = []string{"queued", "dispatching", "running", "retrying"}

// Collector periodically samples Postgres to update gauge metrics.
type Collector struct {
	store    StatsStore
	interval time.Duration
	logger   *slog.Logger
}

// NewCollector builds a gauge collector.
func NewCollector(st StatsStore, interval time.Duration, logger *slog.Logger) *Collector {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	return &Collector{store: st, interval: interval, logger: logger}
}

// Run samples until ctx is cancelled.
func (c *Collector) Run(ctx context.Context) {
	c.logger.Info("metrics collector started", "interval", c.interval.String())
	c.sample(ctx) // sample once at startup
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("metrics collector stopped")
			return
		case <-ticker.C:
			c.sample(ctx)
		}
	}
}

func (c *Collector) sample(ctx context.Context) {
	if byState, err := c.store.CountActiveTasksByState(ctx); err != nil {
		if ctx.Err() == nil {
			c.logger.Warn("metrics: count tasks failed", "error", err)
		}
	} else {
		for _, s := range activeStates {
			TasksActive.WithLabelValues(s).Set(float64(byState[s]))
		}
	}
	if n, err := c.store.CountAliveWorkers(ctx); err == nil {
		WorkersAlive.Set(float64(n))
	}
	if n, err := c.store.CountPendingOutbox(ctx); err == nil {
		OutboxPending.Set(float64(n))
	}
}
