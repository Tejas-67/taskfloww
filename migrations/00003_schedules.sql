-- +goose Up
-- Recurring (cron) DEFINITIONS. A schedule is a template, not a run: the
-- scheduler scans for due schedules and MATERIALIZES a concrete row in `tasks`
-- for each firing. This keeps full run history (vs. mutating one row forever)
-- and cleanly separates "the recurring rule" from "individual executions".
CREATE TABLE schedules (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name          TEXT NOT NULL UNIQUE,                       -- human identifier
  task_name     TEXT NOT NULL,                              -- handler to run (plug-and-play mapping)
  payload       JSONB NOT NULL DEFAULT '{}',               -- template payload for each run
  cron_expr     TEXT NOT NULL,
  timezone      TEXT NOT NULL DEFAULT 'UTC',
  priority      SMALLINT NOT NULL DEFAULT 0 CHECK (priority BETWEEN 0 AND 255),
  max_retries   INTEGER NOT NULL DEFAULT 5 CHECK (max_retries >= 0),
  queue         TEXT,
  enabled       BOOLEAN NOT NULL DEFAULT TRUE,
  next_fire_at  TIMESTAMPTZ NOT NULL,                       -- computed next cron fire time
  last_fired_at TIMESTAMPTZ,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Scheduler due-scan: only enabled schedules, ordered by when they fire.
CREATE INDEX idx_schedules_due ON schedules (next_fire_at) WHERE enabled;

CREATE TRIGGER trg_schedules_updated_at BEFORE UPDATE ON schedules
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- +goose Down
DROP TABLE IF EXISTS schedules;
