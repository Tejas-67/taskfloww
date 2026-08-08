package store

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/Tejas-67/taskfloww/orchestrator/internal/domain"
)

const deadLetterColumns = `id::text, task_id::text, task_name, payload, attempts, last_error,
	original_execution_id::text, died_at, replayed_at`

func scanDeadLetter(row scannable) (*domain.DeadLetter, error) {
	var (
		dl      domain.DeadLetter
		payload []byte
	)
	if err := row.Scan(&dl.ID, &dl.TaskID, &dl.TaskName, &payload, &dl.Attempts, &dl.LastError,
		&dl.OriginalExecutionID, &dl.DiedAt, &dl.ReplayedAt); err != nil {
		return nil, err
	}
	dl.Payload = payload
	return &dl, nil
}

// ListDeadLetters returns dead letters newest-first. When includeReplayed is
// false only un-replayed entries are returned.
func (p *Postgres) ListDeadLetters(ctx context.Context, limit, offset int, includeReplayed bool) ([]domain.DeadLetter, error) {
	where := "WHERE replayed_at IS NULL"
	if includeReplayed {
		where = ""
	}
	q := `SELECT ` + deadLetterColumns + ` FROM dead_letters ` + where + `
		ORDER BY died_at DESC LIMIT $1 OFFSET $2`
	rows, err := p.pool.Query(ctx, q, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]domain.DeadLetter, 0, limit)
	for rows.Next() {
		dl, err := scanDeadLetter(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *dl)
	}
	return out, rows.Err()
}

// GetDeadLetter returns a dead letter by id, or ErrNotFound.
func (p *Postgres) GetDeadLetter(ctx context.Context, id string) (*domain.DeadLetter, error) {
	const q = `SELECT ` + deadLetterColumns + ` FROM dead_letters WHERE id = $1::uuid`
	dl, err := scanDeadLetter(p.pool.QueryRow(ctx, q, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return dl, err
}

// ReplayDeadLetter re-queues a dead task for a fresh run: it resets the task to
// 'queued' with attempt=0 and stamps the dead letter replayed_at, in one tx.
// Returns the re-queued task. ErrNotFound if the dead letter is missing;
// ErrConflict if it was already replayed or its task is no longer 'dead'.
func (p *Postgres) ReplayDeadLetter(ctx context.Context, id string) (*domain.Task, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var (
		taskID     *string
		replayedAt *string
	)
	err = tx.QueryRow(ctx,
		`SELECT task_id::text, replayed_at::text FROM dead_letters WHERE id = $1::uuid FOR UPDATE`, id,
	).Scan(&taskID, &replayedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if replayedAt != nil {
		return nil, ErrConflict // already replayed
	}
	if taskID == nil {
		return nil, ErrConflict // original task record was removed
	}

	task, err := scanTask(tx.QueryRow(ctx,
		`UPDATE tasks SET state = 'queued', attempt = 0, next_run_at = now(),
			completed_at = NULL, last_error = NULL, locked_by = NULL, lease_expires_at = NULL
		 WHERE id = $1::uuid AND state = 'dead'
		 RETURNING `+taskColumns, *taskID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrConflict // task is not in 'dead' state
	}
	if err != nil {
		return nil, err
	}

	// A replay is a fresh run: clear the prior run's execution ledger so the
	// dispatcher can re-create attempt 1 without colliding on uq_task_attempt.
	// The dead run's history is preserved in the dead_letters row.
	if _, err := tx.Exec(ctx,
		`DELETE FROM task_executions WHERE task_id = $1::uuid`, *taskID); err != nil {
		return nil, err
	}

	if _, err := tx.Exec(ctx,
		`UPDATE dead_letters SET replayed_at = now() WHERE id = $1::uuid`, id); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return task, nil
}
