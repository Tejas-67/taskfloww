-- +goose Up
-- Worker fleet registry. Workers self-identify with a stable id and the
-- orchestrator upserts this row from registration/heartbeat MESSAGES (ADR-0002,
-- B1: only the orchestrator writes the database). Heartbeats are worker-level
-- (one row per worker) so the reaper detects a crashed node with a single cheap
-- scan instead of per-task heartbeat writes.
CREATE TABLE workers (
  id                TEXT PRIMARY KEY,                        -- e.g. "worker-<host>-<pid>-<rand>"
  hostname          TEXT,
  pid               INTEGER,
  queues            TEXT[] NOT NULL DEFAULT '{}',            -- queues this worker consumes
  status            worker_status NOT NULL DEFAULT 'alive',
  metadata          JSONB NOT NULL DEFAULT '{}',            -- version, capacity, labels
  registered_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
) WITH (fillfactor = 70);  -- frequent heartbeat UPDATEs → leave room for in-page tuple versions

-- Reaper scans alive workers whose heartbeat has gone stale.
CREATE INDEX idx_workers_liveness ON workers (last_heartbeat_at) WHERE status = 'alive';

CREATE TRIGGER trg_workers_updated_at BEFORE UPDATE ON workers
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- +goose Down
DROP TABLE IF EXISTS workers;
