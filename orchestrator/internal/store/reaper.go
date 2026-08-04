package store

import (
	"context"
	"time"

	"github.com/google/uuid"
)

const leaseExpiredError = "lease expired (worker lost)"

// ReapExpiredLeases finds in-flight tasks whose lease expired (the worker
// stopped heartbeating — i.e. crashed) and re-queues them: retrying with backoff
// while attempts remain, else dead + a dead_letters row. The stale execution is
// marked failed. Concurrency-safe across instances via FOR UPDATE SKIP LOCKED.
// Returns (requeued, dead) counts.
func (p *Postgres) ReapExpiredLeases(ctx context.Context, batch int, backoffFor func(attempt int) time.Duration) (int, int, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	const q = `
		SELECT id::text, attempt, max_retries FROM tasks
		WHERE state IN ('dispatching', 'running') AND lease_expires_at < now()
		ORDER BY lease_expires_at
		FOR UPDATE SKIP LOCKED
		LIMIT $1`
	rows, err := tx.Query(ctx, q, batch)
	if err != nil {
		return 0, 0, err
	}
	type expired struct {
		id                  string
		attempt, maxRetries int
	}
	var items []expired
	for rows.Next() {
		var e expired
		if err := rows.Scan(&e.id, &e.attempt, &e.maxRetries); err != nil {
			rows.Close()
			return 0, 0, err
		}
		items = append(items, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, 0, err
	}

	requeued, dead := 0, 0
	for _, e := range items {
		// Mark the in-flight execution failed (timed out), if not already terminal.
		if _, err := tx.Exec(ctx,
			`UPDATE task_executions SET state = 'failed', finished_at = now(), error = $3
			 WHERE task_id = $1::uuid AND attempt_number = $2 AND state NOT IN ('succeeded', 'failed')`,
			e.id, e.attempt, leaseExpiredError,
		); err != nil {
			return requeued, dead, err
		}

		if e.attempt <= e.maxRetries {
			delay := backoffFor(e.attempt)
			if _, err := tx.Exec(ctx,
				`UPDATE tasks SET state = 'retrying',
					next_run_at = now() + make_interval(secs => $2),
					locked_by = NULL, lease_expires_at = NULL, last_error = $3
				 WHERE id = $1::uuid`,
				e.id, int(delay.Seconds()), leaseExpiredError,
			); err != nil {
				return requeued, dead, err
			}
			requeued++
		} else {
			if _, err := tx.Exec(ctx,
				`UPDATE tasks SET state = 'dead', completed_at = now(),
					locked_by = NULL, lease_expires_at = NULL, last_error = $2
				 WHERE id = $1::uuid`,
				e.id, leaseExpiredError,
			); err != nil {
				return requeued, dead, err
			}
			if _, err := tx.Exec(ctx,
				`INSERT INTO dead_letters (task_id, task_name, payload, attempts, last_error)
				 SELECT id, task_name, payload, attempt, $2 FROM tasks WHERE id = $1::uuid`,
				e.id, leaseExpiredError,
			); err != nil {
				return requeued, dead, err
			}
			dead++
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, 0, err
	}
	return requeued, dead, nil
}

// NextFireFunc computes a schedule's next fire time after t.
type NextFireFunc func(cronExpr, timezone string, after time.Time) (time.Time, error)

// FireDueSchedules materializes a task run for each enabled schedule that is due
// and advances the schedule's next_fire_at. Concurrency-safe across instances.
// Returns the number of runs materialized.
func (p *Postgres) FireDueSchedules(ctx context.Context, batch int, nextFire NextFireFunc) (int, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	const q = `
		SELECT id::text, name, task_name, payload, cron_expr, timezone, priority, max_retries, queue
		FROM schedules
		WHERE enabled AND next_fire_at <= now()
		ORDER BY next_fire_at
		FOR UPDATE SKIP LOCKED
		LIMIT $1`
	rows, err := tx.Query(ctx, q, batch)
	if err != nil {
		return 0, err
	}
	type sched struct {
		id, name, taskName string
		payload            []byte
		cron, tz           string
		priority           int16
		maxRetries         int
		queue              *string
	}
	var due []sched
	for rows.Next() {
		var s sched
		if err := rows.Scan(&s.id, &s.name, &s.taskName, &s.payload, &s.cron, &s.tz,
			&s.priority, &s.maxRetries, &s.queue); err != nil {
			rows.Close()
			return 0, err
		}
		due = append(due, s)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	now := time.Now().UTC()
	fired := 0
	for _, s := range due {
		// Materialize a concrete queued run (execution_type=recurring, schedule_id set).
		if _, err := tx.Exec(ctx,
			`INSERT INTO tasks (idempotency_key, task_name, payload, state, execution_type,
				priority, max_retries, next_run_at, queue, schedule_id)
			 VALUES ($1, $2, $3::jsonb, 'queued', 'recurring', $4, $5, now(), $6, $7::uuid)`,
			s.name+"-"+uuid.NewString(), s.taskName, string(payloadOrEmpty(s.payload)),
			s.priority, s.maxRetries, s.queue, s.id,
		); err != nil {
			return fired, err
		}
		next, err := nextFire(s.cron, s.tz, now)
		if err != nil {
			return fired, err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE schedules SET last_fired_at = now(), next_fire_at = $2 WHERE id = $1::uuid`,
			s.id, next,
		); err != nil {
			return fired, err
		}
		fired++
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return fired, nil
}

// MarkStaleWorkers flags alive workers whose heartbeat is older than timeout as
// dead (observability; the lease scan is what actually re-queues their tasks).
func (p *Postgres) MarkStaleWorkers(ctx context.Context, timeout time.Duration) (int64, error) {
	const q = `
		UPDATE workers SET status = 'dead'
		WHERE status = 'alive' AND last_heartbeat_at < now() - make_interval(secs => $1)`
	tag, err := p.pool.Exec(ctx, q, int(timeout.Seconds()))
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
