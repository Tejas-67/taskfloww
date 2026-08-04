package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Tejas-67/taskfloww/orchestrator/internal/domain"
)

// WorkerInput is the subset of worker fields upserted from a heartbeat.
type WorkerInput struct {
	ID       string
	Hostname string
	PID      int
	Queues   []string
	Status   domain.WorkerStatus
}

// UpsertWorker inserts or refreshes a worker's registry/liveness row from a
// heartbeat (the orchestrator is the only writer — ADR-0002/B1).
func (p *Postgres) UpsertWorker(ctx context.Context, w WorkerInput) error {
	const q = `
		INSERT INTO workers (id, hostname, pid, queues, status, last_heartbeat_at)
		VALUES ($1, $2, $3, $4, $5::worker_status, now())
		ON CONFLICT (id) DO UPDATE
		SET hostname = EXCLUDED.hostname, pid = EXCLUDED.pid, queues = EXCLUDED.queues,
			status = EXCLUDED.status, last_heartbeat_at = now()`
	status := w.Status
	if status == "" {
		status = domain.WorkerAlive
	}
	queues := w.Queues
	if queues == nil {
		queues = []string{} // column is NOT NULL DEFAULT '{}'
	}
	_, err := p.pool.Exec(ctx, q, w.ID, nz(w.Hostname), w.PID, queues, status)
	return err
}

// RenewLeases advances in-flight tasks to 'running' and extends their lease,
// stamping last_heartbeat_at. The state guard means a late heartbeat cannot
// resurrect a task that already reached a terminal state. Returns rows renewed.
func (p *Postgres) RenewLeases(ctx context.Context, taskIDs []string, lease time.Duration) (int64, error) {
	if len(taskIDs) == 0 {
		return 0, nil
	}
	const q = `
		UPDATE tasks
		SET state = 'running', lease_expires_at = now() + make_interval(secs => $2),
			last_heartbeat_at = now()
		WHERE id = ANY($1::uuid[]) AND state IN ('dispatching', 'running')`
	tag, err := p.pool.Exec(ctx, q, taskIDs, int(lease.Seconds()))
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// ApplyResult applies a worker's result to an execution and its task, exactly
// once. On a duplicate (execution already terminal) it returns applied=false.
//
//   - success            → task 'completed'
//   - failure, retries   → task 'retrying', next_run_at = now + backoffFor(attempt)
//   - failure, exhausted → task 'dead' + a dead_letters row
//
// backoffFor computes the retry delay for the just-failed attempt.
func (p *Postgres) ApplyResult(
	ctx context.Context, executionID, workerID string, success bool,
	result json.RawMessage, errMsg string, backoffFor func(attempt int) time.Duration,
) (applied bool, taskState domain.TaskState, err error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return false, "", err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Lock the execution and dedupe.
	var execState, taskID string
	err = tx.QueryRow(ctx,
		`SELECT state, task_id::text FROM task_executions WHERE id = $1::uuid FOR UPDATE`,
		executionID,
	).Scan(&execState, &taskID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, "", ErrNotFound
	}
	if err != nil {
		return false, "", err
	}
	if execState == string(domain.ExecutionSucceeded) || execState == string(domain.ExecutionFailed) {
		return false, "", nil // already applied — idempotent no-op
	}

	newExecState := domain.ExecutionSucceeded
	if !success {
		newExecState = domain.ExecutionFailed
	}
	if _, err = tx.Exec(ctx,
		`UPDATE task_executions
		 SET state = $2::execution_state, finished_at = now(), error = $3,
			 result = $4::jsonb,
			 worker_id = COALESCE(worker_id, (SELECT id FROM workers WHERE id = $5))
		 WHERE id = $1::uuid`,
		executionID, newExecState, nz(errMsg), nzJSON(result), nz(workerID),
	); err != nil {
		return false, "", err
	}

	// Lock the task to read attempt accounting.
	var attempt, maxRetries int
	if err = tx.QueryRow(ctx,
		`SELECT attempt, max_retries FROM tasks WHERE id = $1::uuid FOR UPDATE`, taskID,
	).Scan(&attempt, &maxRetries); err != nil {
		return false, "", err
	}

	switch {
	case success:
		taskState = domain.TaskCompleted
		_, err = tx.Exec(ctx,
			`UPDATE tasks SET state = 'completed', completed_at = now(),
				locked_by = NULL, lease_expires_at = NULL, last_error = NULL
			 WHERE id = $1::uuid`, taskID)
	case attempt <= maxRetries:
		// max_retries counts retries beyond the first attempt: attempt N may
		// retry while N <= max_retries (total attempts = max_retries + 1).
		taskState = domain.TaskRetrying
		delay := backoffFor(attempt)
		_, err = tx.Exec(ctx,
			`UPDATE tasks SET state = 'retrying',
				next_run_at = now() + make_interval(secs => $2),
				locked_by = NULL, lease_expires_at = NULL, last_error = $3
			 WHERE id = $1::uuid`, taskID, int(delay.Seconds()), nz(errMsg))
	default:
		taskState = domain.TaskDead
		if _, err = tx.Exec(ctx,
			`UPDATE tasks SET state = 'dead', completed_at = now(),
				locked_by = NULL, lease_expires_at = NULL, last_error = $2
			 WHERE id = $1::uuid`, taskID, nz(errMsg)); err != nil {
			return false, "", err
		}
		// Record for DLQ introspection / replay.
		_, err = tx.Exec(ctx,
			`INSERT INTO dead_letters (task_id, task_name, payload, attempts, last_error, original_execution_id)
			 SELECT id, task_name, payload, attempt, $2, $3::uuid FROM tasks WHERE id = $1::uuid`,
			taskID, nz(errMsg), executionID)
	}
	if err != nil {
		return false, "", err
	}

	if err = tx.Commit(ctx); err != nil {
		return false, "", err
	}
	return true, taskState, nil
}

// nz returns nil for an empty string (→ SQL NULL), else a pointer to it.
func nz(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// nzJSON returns nil for empty JSON (→ SQL NULL), else the string form.
func nzJSON(b json.RawMessage) *string {
	if len(b) == 0 {
		return nil
	}
	s := string(b)
	return &s
}
