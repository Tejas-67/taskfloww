-- +goose Up
-- Dead Letter Queue introspection table. RabbitMQ's native DLX physically
-- dead-letters the message; this table records it for querying and REPLAY.
-- A task lands here once attempt > max_retries (state → 'dead').
CREATE TABLE dead_letters (
  id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  task_id               UUID REFERENCES tasks(id) ON DELETE SET NULL,
  task_name             TEXT NOT NULL,
  payload               JSONB NOT NULL,
  attempts              INTEGER NOT NULL,                 -- attempts made before dying
  last_error            TEXT,
  original_execution_id UUID,                             -- last task_executions.id
  died_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  replayed_at           TIMESTAMPTZ                       -- set when an operator replays it
);

-- Operators list un-replayed dead letters, newest first.
CREATE INDEX idx_dead_letters_unreplayed ON dead_letters (died_at DESC) WHERE replayed_at IS NULL;

-- +goose Down
DROP TABLE IF EXISTS dead_letters;
