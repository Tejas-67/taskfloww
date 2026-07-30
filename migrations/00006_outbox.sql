-- +goose Up
-- Transactional outbox (ADR-0001). The dispatcher writes the task state
-- transition (→ 'dispatching') AND an outbox row in the SAME transaction; a
-- relay then publishes unpublished rows to RabbitMQ and stamps published_at.
-- This guarantees exactly-one publish per committed state change even if the
-- process crashes between the DB commit and the broker publish.
CREATE TABLE outbox (
  id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,  -- monotonic → preserves publish order
  task_id          UUID REFERENCES tasks(id) ON DELETE CASCADE,
  exchange         TEXT NOT NULL,
  routing_key      TEXT NOT NULL,
  payload          JSONB NOT NULL,                                   -- message body (task_id, execution_id, ...)
  headers          JSONB NOT NULL DEFAULT '{}',
  priority         SMALLINT NOT NULL DEFAULT 0 CHECK (priority BETWEEN 0 AND 255),
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at     TIMESTAMPTZ,                                      -- NULL = pending
  publish_attempts INTEGER NOT NULL DEFAULT 0
);

-- Relay claims pending rows in insertion order via FOR UPDATE SKIP LOCKED.
-- Partial index stays tiny (only unpublished rows). A retention job prunes old
-- published rows.
CREATE INDEX idx_outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

-- +goose Down
DROP TABLE IF EXISTS outbox;
