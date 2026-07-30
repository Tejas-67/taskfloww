-- +goose Up
-- The central state machine and scheduler unit. One row = one runnable task
-- (immediate, delayed, or a materialized recurring run). PostgreSQL is the
-- source of truth (ADR-0001): dispatch, leasing, retry accounting and cron all
-- read/write here.
CREATE TABLE tasks (
  id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),   -- internal surrogate id
  idempotency_key   TEXT NOT NULL UNIQUE,                         -- client "unique id"; dedupes submissions
  task_name         TEXT NOT NULL,                                -- maps to a worker function (plug-and-play)
  payload           JSONB NOT NULL DEFAULT '{}',
  state             task_state NOT NULL DEFAULT 'queued',
  execution_type    execution_type NOT NULL DEFAULT 'immediate',
  priority          SMALLINT NOT NULL DEFAULT 0 CHECK (priority BETWEEN 0 AND 255),  -- higher = more urgent
  max_retries       INTEGER NOT NULL DEFAULT 5 CHECK (max_retries >= 0),
  attempt           INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),

  -- Scheduler clock: when the task becomes eligible to dispatch.
  --   immediate → now()   delayed → future   retrying → now()+backoff   recurring → fire time
  next_run_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  queue             TEXT,                                         -- resolved from config at dispatch if NULL

  -- Lease / execution tracking (fault tolerance). Set on dispatch, cleared on
  -- terminal state. The reaper re-queues rows whose lease has expired.
  locked_by         TEXT,                                         -- worker/orchestrator holding the lease
  lease_expires_at  TIMESTAMPTZ,                                  -- visibility timeout
  last_heartbeat_at TIMESTAMPTZ,
  dispatched_at     TIMESTAMPTZ,
  started_at        TIMESTAMPTZ,
  completed_at      TIMESTAMPTZ,
  last_error        TEXT,

  -- Provenance: set when this run was materialized from a recurring schedule.
  schedule_id       UUID REFERENCES schedules(id) ON DELETE SET NULL,

  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

  -- A 'recurring' task must carry its schedule provenance.
  CONSTRAINT chk_recurring_has_schedule
    CHECK (execution_type <> 'recurring' OR schedule_id IS NOT NULL)
) WITH (fillfactor = 85);  -- frequent state UPDATEs → leave in-page room (tune down under heavy load)

-- HOT PATH — dispatcher due-scan:
--   SELECT ... WHERE state IN ('queued','retrying') AND next_run_at <= now()
--   ORDER BY priority DESC, next_run_at FOR UPDATE SKIP LOCKED
-- Partial index holds only dispatchable rows and matches the ORDER BY exactly.
CREATE INDEX idx_tasks_due ON tasks (priority DESC, next_run_at)
  WHERE state IN ('queued', 'retrying');

-- Reaper: in-flight tasks whose lease expired (worker vanished mid-execution).
CREATE INDEX idx_tasks_lease_expiry ON tasks (lease_expires_at)
  WHERE state IN ('dispatching', 'running');

-- Lookups of all runs spawned by a given schedule.
CREATE INDEX idx_tasks_schedule ON tasks (schedule_id) WHERE schedule_id IS NOT NULL;

CREATE TRIGGER trg_tasks_updated_at BEFORE UPDATE ON tasks
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- +goose Down
DROP TABLE IF EXISTS tasks;
