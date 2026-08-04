package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Tejas-67/taskfloww/orchestrator/internal/domain"
)

// Postgres is the PostgreSQL-backed store.
type Postgres struct {
	pool *pgxpool.Pool
}

// NewPostgres wraps a pgx pool.
func NewPostgres(pool *pgxpool.Pool) *Postgres { return &Postgres{pool: pool} }

// Connect builds a pgx pool from a DSN and pool sizing, verifying connectivity.
func Connect(ctx context.Context, uri string, maxConns, minConns, connMaxIdleSeconds int) (*Postgres, error) {
	cfg, err := pgxpool.ParseConfig(uri)
	if err != nil {
		return nil, fmt.Errorf("parsing database uri: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = int32(maxConns)
	}
	if minConns >= 0 {
		cfg.MinConns = int32(minConns)
	}
	if connMaxIdleSeconds > 0 {
		cfg.MaxConnIdleTime = time.Duration(connMaxIdleSeconds) * time.Second
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("creating pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
	}
	return &Postgres{pool: pool}, nil
}

// Close releases the underlying pool.
func (p *Postgres) Close() { p.pool.Close() }

// Ping verifies connectivity.
func (p *Postgres) Ping(ctx context.Context) error { return p.pool.Ping(ctx) }

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// taskColumns is the canonical SELECT list; keep in sync with scanTask.
const taskColumns = `id::text, idempotency_key, task_name, payload, state, execution_type,
	priority, max_retries, attempt, next_run_at, queue, locked_by, lease_expires_at,
	last_heartbeat_at, dispatched_at, started_at, completed_at, last_error,
	schedule_id::text, created_at, updated_at`

// scannable is satisfied by both pgx.Row and pgx.Rows.
type scannable interface {
	Scan(dest ...any) error
}

func scanTask(row scannable) (*domain.Task, error) {
	var (
		t            domain.Task
		payload      []byte
		state, etype string
	)
	err := row.Scan(
		&t.ID, &t.IdempotencyKey, &t.TaskName, &payload, &state, &etype,
		&t.Priority, &t.MaxRetries, &t.Attempt, &t.NextRunAt, &t.Queue, &t.LockedBy, &t.LeaseExpiresAt,
		&t.LastHeartbeatAt, &t.DispatchedAt, &t.StartedAt, &t.CompletedAt, &t.LastError,
		&t.ScheduleID, &t.CreatedAt, &t.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	t.Payload = payload
	t.State = domain.TaskState(state)
	t.ExecutionType = domain.ExecutionType(etype)
	return &t, nil
}

// InsertTask inserts a new task, populating generated fields (ID, State,
// CreatedAt, UpdatedAt) on t. Returns ErrDuplicate on idempotency_key collision.
func (p *Postgres) InsertTask(ctx context.Context, t *domain.Task) error {
	const q = `
		INSERT INTO tasks (idempotency_key, task_name, payload, state, execution_type,
			priority, max_retries, next_run_at, queue, schedule_id)
		VALUES ($1, $2, $3::jsonb, $4::task_state, $5::execution_type, $6, $7, $8, $9, $10::uuid)
		RETURNING id::text, state, created_at, updated_at`
	var state string
	err := p.pool.QueryRow(ctx, q,
		t.IdempotencyKey, t.TaskName, string(payloadOrEmpty(t.Payload)), t.State, t.ExecutionType,
		t.Priority, t.MaxRetries, t.NextRunAt, t.Queue, t.ScheduleID,
	).Scan(&t.ID, &state, &t.CreatedAt, &t.UpdatedAt)
	if isUniqueViolation(err) {
		return ErrDuplicate
	}
	if err != nil {
		return err
	}
	t.State = domain.TaskState(state)
	return nil
}

// GetTask returns a task by id, or ErrNotFound.
func (p *Postgres) GetTask(ctx context.Context, id string) (*domain.Task, error) {
	const q = `SELECT ` + taskColumns + ` FROM tasks WHERE id = $1::uuid`
	t, err := scanTask(p.pool.QueryRow(ctx, q, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return t, err
}

// GetTaskByIdempotencyKey returns a task by its client idempotency key, or
// ErrNotFound. Used to make submissions idempotent.
func (p *Postgres) GetTaskByIdempotencyKey(ctx context.Context, key string) (*domain.Task, error) {
	const q = `SELECT ` + taskColumns + ` FROM tasks WHERE idempotency_key = $1`
	t, err := scanTask(p.pool.QueryRow(ctx, q, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return t, err
}

// CancelTask transitions a queued/retrying task to cancelled. If the task
// exists but is already running or terminal it returns ErrConflict; if it does
// not exist, ErrNotFound.
func (p *Postgres) CancelTask(ctx context.Context, id string) (*domain.Task, error) {
	const q = `
		UPDATE tasks SET state = 'cancelled', completed_at = now()
		WHERE id = $1::uuid AND state IN ('queued', 'retrying')
		RETURNING ` + taskColumns
	t, err := scanTask(p.pool.QueryRow(ctx, q, id))
	if errors.Is(err, pgx.ErrNoRows) {
		// Distinguish "missing" from "not cancellable".
		if existing, gerr := p.GetTask(ctx, id); gerr == nil {
			return existing, ErrConflict
		} else if errors.Is(gerr, ErrNotFound) {
			return nil, ErrNotFound
		} else {
			return nil, gerr
		}
	}
	return t, err
}

// InsertSchedule inserts a recurring schedule, populating generated fields.
// Returns ErrDuplicate on name collision.
func (p *Postgres) InsertSchedule(ctx context.Context, s *domain.Schedule) error {
	const q = `
		INSERT INTO schedules (name, task_name, payload, cron_expr, timezone,
			priority, max_retries, queue, enabled, next_fire_at)
		VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7, $8, $9, $10)
		RETURNING id::text, created_at, updated_at`
	err := p.pool.QueryRow(ctx, q,
		s.Name, s.TaskName, string(payloadOrEmpty(s.Payload)), s.CronExpr, s.Timezone,
		s.Priority, s.MaxRetries, s.Queue, s.Enabled, s.NextFireAt,
	).Scan(&s.ID, &s.CreatedAt, &s.UpdatedAt)
	if isUniqueViolation(err) {
		return ErrDuplicate
	}
	return err
}

func payloadOrEmpty(p []byte) []byte {
	if len(p) == 0 {
		return []byte("{}")
	}
	return p
}
