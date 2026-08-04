package store

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/Tejas-67/taskfloww/orchestrator/internal/domain"
)

// taskColumnsT is taskColumns qualified with the "t" alias, for UPDATE ... FROM
// RETURNING where a joined CTE also exposes an "id" column. Order MUST match
// scanTask.
const taskColumnsT = `t.id::text, t.idempotency_key, t.task_name, t.payload, t.state, t.execution_type,
	t.priority, t.max_retries, t.attempt, t.next_run_at, t.queue, t.locked_by, t.lease_expires_at,
	t.last_heartbeat_at, t.dispatched_at, t.started_at, t.completed_at, t.last_error,
	t.schedule_id::text, t.created_at, t.updated_at`

// Claimed is a task the dispatcher has leased for dispatch, paired with the
// execution id that travels in its message.
type Claimed struct {
	Task        *domain.Task
	ExecutionID string
}

// BuildOutboxFunc resolves a claimed task (and its execution id) into the
// outbox message to enqueue. Supplied by the dispatcher so routing/serialization
// stays out of the store while remaining inside the claim transaction.
type BuildOutboxFunc func(t *domain.Task, executionID string) (domain.OutboxMessage, error)

// ClaimDueTasks atomically claims up to batch due tasks and, for each, records
// an execution attempt and an outbox row — all in ONE transaction. Concurrency
// across orchestrator instances is safe via FOR UPDATE SKIP LOCKED, so a task
// is claimed by exactly one instance.
//
// Each claimed task is transitioned queued/retrying → dispatching, its attempt
// counter incremented, and a lease set (lease from now).
func (p *Postgres) ClaimDueTasks(
	ctx context.Context, batch int, lease time.Duration, lockedBy string, build BuildOutboxFunc,
) ([]Claimed, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	const claimQ = `
		WITH due AS (
			SELECT id FROM tasks
			WHERE state IN ('queued', 'retrying') AND next_run_at <= now()
			ORDER BY priority DESC, next_run_at
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		UPDATE tasks t
		SET state = 'dispatching', locked_by = $2,
			lease_expires_at = now() + make_interval(secs => $3),
			dispatched_at = now(), attempt = attempt + 1
		FROM due WHERE t.id = due.id
		RETURNING ` + taskColumnsT

	rows, err := tx.Query(ctx, claimQ, batch, lockedBy, int(lease.Seconds()))
	if err != nil {
		return nil, err
	}
	var tasks []*domain.Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		tasks = append(tasks, t)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	claimed := make([]Claimed, 0, len(tasks))
	for _, t := range tasks {
		execID := uuid.NewString()
		if _, err := tx.Exec(ctx,
			`INSERT INTO task_executions (id, task_id, attempt_number, state)
			 VALUES ($1::uuid, $2::uuid, $3, 'dispatched')`,
			execID, t.ID, t.Attempt,
		); err != nil {
			return nil, err
		}
		msg, err := build(t, execID)
		if err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO outbox (task_id, exchange, routing_key, payload, headers, priority)
			 VALUES ($1::uuid, $2, $3, $4::jsonb, $5::jsonb, $6)`,
			t.ID, msg.Exchange, msg.RoutingKey, string(payloadOrEmpty(msg.Payload)),
			string(headersOrEmpty(msg.Headers)), msg.Priority,
		); err != nil {
			return nil, err
		}
		claimed = append(claimed, Claimed{Task: t, ExecutionID: execID})
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return claimed, nil
}

// PublishFunc publishes one outbox message to the broker.
type PublishFunc func(domain.OutboxMessage) error

// PublishOutbox claims up to batch unpublished outbox rows (FOR UPDATE SKIP
// LOCKED, in id order) and publishes each. Rows that publish successfully are
// stamped published_at; rows that fail have publish_attempts incremented and
// are left for a later tick. Returns the number published.
func (p *Postgres) PublishOutbox(ctx context.Context, batch int, publish PublishFunc) (int, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	const selectQ = `
		SELECT id, task_id::text, exchange, routing_key, payload, headers, priority,
			created_at, published_at, publish_attempts
		FROM outbox
		WHERE published_at IS NULL
		ORDER BY id
		FOR UPDATE SKIP LOCKED
		LIMIT $1`

	rows, err := tx.Query(ctx, selectQ, batch)
	if err != nil {
		return 0, err
	}
	var msgs []domain.OutboxMessage
	for rows.Next() {
		var (
			m                domain.OutboxMessage
			payload, headers []byte
		)
		if err := rows.Scan(&m.ID, &m.TaskID, &m.Exchange, &m.RoutingKey, &payload, &headers,
			&m.Priority, &m.CreatedAt, &m.PublishedAt, &m.PublishAttempts); err != nil {
			rows.Close()
			return 0, err
		}
		m.Payload = payload
		m.Headers = headers
		msgs = append(msgs, m)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	published := 0
	for _, m := range msgs {
		if perr := publish(m); perr != nil {
			if _, err := tx.Exec(ctx,
				`UPDATE outbox SET publish_attempts = publish_attempts + 1 WHERE id = $1`, m.ID,
			); err != nil {
				return published, err
			}
			continue
		}
		if _, err := tx.Exec(ctx,
			`UPDATE outbox SET published_at = now() WHERE id = $1`, m.ID,
		); err != nil {
			return published, err
		}
		published++
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return published, nil
}

func headersOrEmpty(h []byte) []byte {
	if len(h) == 0 {
		return []byte("{}")
	}
	return h
}
