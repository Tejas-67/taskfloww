-- +goose Up
-- Idempotency ledger + execution history. Every dispatch of a task creates one
-- execution row; its id travels in the RabbitMQ message as `execution_id`. The
-- result/heartbeat consumer uses this to dedupe at-least-once redeliveries: a
-- duplicate result for an already-finished execution is ignored.
CREATE TABLE task_executions (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),          -- execution_id (in messages)
  task_id        UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  attempt_number INTEGER NOT NULL CHECK (attempt_number >= 1),
  worker_id      TEXT REFERENCES workers(id) ON DELETE SET NULL,      -- who ran it (NULL until claimed)
  state          execution_state NOT NULL DEFAULT 'dispatched',
  dispatched_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at     TIMESTAMPTZ,
  finished_at    TIMESTAMPTZ,
  error          TEXT,
  result         JSONB,

  -- Idempotency: exactly one execution record per (task, attempt).
  CONSTRAINT uq_task_attempt UNIQUE (task_id, attempt_number)
);

CREATE INDEX idx_executions_task ON task_executions (task_id);
CREATE INDEX idx_executions_worker ON task_executions (worker_id) WHERE worker_id IS NOT NULL;

-- +goose Down
DROP TABLE IF EXISTS task_executions;
