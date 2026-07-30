-- +goose Up
-- Domain enums mirror the Go domain package (orchestrator/internal/domain).
-- Native enums keep hot-path indexes (state, execution_type) compact and give
-- sqlc strong typing later. To add a value in a future migration, use
-- "ALTER TYPE task_state ADD VALUE 'x';" in a migration marked NO TRANSACTION.

CREATE TYPE task_state AS ENUM (
  'queued',       -- ready; dispatched when next_run_at <= now()
  'dispatching',  -- claimed by an orchestrator, being published (leased)
  'running',      -- a worker is executing it (leased, heartbeating)
  'retrying',     -- failed; waiting for backoff (next_run_at in the future)
  'completed',    -- succeeded (terminal)
  'failed',       -- terminal, non-retryable failure (terminal)
  'dead',         -- retries exhausted; moved to the DLQ (terminal)
  'cancelled'     -- cancelled before completion (terminal)
);

CREATE TYPE execution_type AS ENUM ('immediate', 'delayed', 'recurring');

CREATE TYPE execution_state AS ENUM ('dispatched', 'running', 'succeeded', 'failed');

CREATE TYPE worker_status AS ENUM ('alive', 'draining', 'dead');

-- Sets updated_at on every UPDATE; attached per-table via BEFORE UPDATE triggers.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose StatementEnd

-- +goose Down
DROP FUNCTION IF EXISTS set_updated_at();
DROP TYPE IF EXISTS worker_status;
DROP TYPE IF EXISTS execution_state;
DROP TYPE IF EXISTS execution_type;
DROP TYPE IF EXISTS task_state;
