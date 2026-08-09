// Package reaper runs the periodic maintenance scans that make TaskFloww
// self-healing:
//   - expired-lease scan: re-queue tasks whose worker crashed (missed heartbeats),
//   - schedule firing: materialize recurring (cron) task runs when due,
//   - stale-worker scan: mark workers dead when their heartbeat lapses.
//
// All scans are safe to run on every orchestrator instance concurrently (the
// store uses FOR UPDATE SKIP LOCKED).
package reaper

import (
	"context"
	"log/slog"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/Tejas-67/taskfloww/orchestrator/internal/backoff"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/metrics"
	"github.com/Tejas-67/taskfloww/orchestrator/internal/store"
)

// Store is the persistence surface the reaper needs.
type Store interface {
	ReapExpiredLeases(ctx context.Context, batch int, backoffFor func(attempt int) time.Duration) (int, int, error)
	FireDueSchedules(ctx context.Context, batch int, nextFire store.NextFireFunc) (int, error)
	MarkStaleWorkers(ctx context.Context, timeout time.Duration) (int64, error)
}

// Reaper periodically re-queues crashed tasks, fires due schedules, and marks
// stale workers dead.
type Reaper struct {
	store         Store
	backoff       *backoff.Policy
	logger        *slog.Logger
	interval      time.Duration
	batch         int
	workerTimeout time.Duration
}

// New builds a Reaper. workerTimeout is how long since the last heartbeat before
// a worker is considered dead (typically the lease TTL).
func New(st Store, bo *backoff.Policy, interval time.Duration, batch int, workerTimeout time.Duration, logger *slog.Logger) *Reaper {
	return &Reaper{store: st, backoff: bo, logger: logger, interval: interval, batch: batch, workerTimeout: workerTimeout}
}

// Run scans until ctx is cancelled.
func (r *Reaper) Run(ctx context.Context) {
	r.logger.Info("reaper started", "interval", r.interval.String(), "worker_timeout", r.workerTimeout.String())
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			r.logger.Info("reaper stopped")
			return
		case <-ticker.C:
			r.tick(ctx)
		}
	}
}

func (r *Reaper) tick(ctx context.Context) {
	if requeued, dead, err := r.store.ReapExpiredLeases(ctx, r.batch, r.backoff.Next); err != nil {
		if ctx.Err() == nil {
			r.logger.Error("lease reap failed", "error", err)
		}
	} else if requeued > 0 || dead > 0 {
		metrics.LeasesReaped.WithLabelValues("requeued").Add(float64(requeued))
		metrics.LeasesReaped.WithLabelValues("dead").Add(float64(dead))
		r.logger.Info("reaped expired leases", "requeued", requeued, "dead", dead)
	}

	if fired, err := r.store.FireDueSchedules(ctx, r.batch, NextFire); err != nil {
		if ctx.Err() == nil {
			r.logger.Error("schedule firing failed", "error", err)
		}
	} else if fired > 0 {
		metrics.SchedulesFired.Add(float64(fired))
		r.logger.Info("fired due schedules", "count", fired)
	}

	if marked, err := r.store.MarkStaleWorkers(ctx, r.workerTimeout); err != nil {
		if ctx.Err() == nil {
			r.logger.Error("stale-worker scan failed", "error", err)
		}
	} else if marked > 0 {
		metrics.WorkersMarkedDead.Add(float64(marked))
		r.logger.Info("marked stale workers dead", "count", marked)
	}
}

// NextFire computes the next cron fire time after `after`, in the schedule's
// timezone, returned in UTC. Satisfies store.NextFireFunc.
func NextFire(cronExpr, timezone string, after time.Time) (time.Time, error) {
	loc := time.UTC
	if timezone != "" {
		if l, err := time.LoadLocation(timezone); err == nil {
			loc = l
		}
	}
	sched, err := cron.ParseStandard(cronExpr)
	if err != nil {
		return time.Time{}, err
	}
	return sched.Next(after.In(loc)).UTC(), nil
}
