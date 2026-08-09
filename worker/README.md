# worker (Python)

Thin, **plug-and-play** worker SDK for TaskFloww. Write an ordinary Python function, map it to a
task name in the YAML config, and the worker runs it — no engine code changes. Workers talk **only
to RabbitMQ** (never PostgreSQL) — ADR-0002/B1 — so the SDK stays tiny and no DB credentials live
on the worker fleet.

## How it works

1. Consumes task messages from the configured priority queues (prefetch = backpressure).
2. Dedupes redeliveries on `execution_id` (at-least-once safety).
3. Runs the mapped handler in a thread pool (a slow task never blocks heartbeats).
4. Publishes a **result** message (success/failure) back to the control exchange.
5. Emits periodic **heartbeats** listing in-flight executions (renews leases + registers the worker).

The orchestrator applies results (→ `completed`, or `retrying`/`dead`) and renews leases from
heartbeats. Failed handlers (exceptions) are reported as failures and retried with backoff.

## Writing a task

A handler takes the task `payload` (a dict) and returns a JSON-serializable result (or `None`):

```python
# examples/tasks.py
def send_email(payload: dict) -> dict:
    ...
    return {"sent": True, "to": payload["to"]}
```

Map it in the config (`handler` is an importable `module:function`):

```yaml
tasks:
  - name: send_email
    handler: examples.tasks:send_email
    queue: tasks.default
    max_retries: 3
```

## Run

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e '.[dev]'
# run from a dir where your handler modules import (CWD is added to sys.path):
python -m taskfloww_worker -c ../config/config.example.yaml
```

Requires a running RabbitMQ (see `../deploy`). Set `TASKFLOWW_WORKER_ID` to pin a stable id.

## Layout

| Module | Responsibility |
|---|---|
| `config.py` | plug-and-play config loader (pydantic) |
| `registry.py` | task registry — imports `module:function` handlers; `@task` decorator |
| `messages.py` | wire contract (mirrors `orchestrator/internal/message`) |
| `worker.py` | consume loop, thread-pool execution, heartbeats, dedupe, graceful shutdown |
| `metrics.py` | Prometheus metrics (`prometheus_client`) — tasks processed, in-flight gauge, duration histogram, duplicates, heartbeats |
| `examples/tasks.py` | example handlers (`send_email`, `generate_report`, `always_fails`) |

## Metrics

The worker exposes Prometheus metrics at `http://<host>:<metrics.worker_port>/metrics`
(default `9100`). Series are namespaced `taskfloww_worker_*` (e.g. `tasks_processed_total`,
`tasks_in_flight`, `task_duration_seconds`, `duplicate_deliveries_total`, `heartbeats_sent_total`).

## Test

```bash
python -m pytest -q     # or: make -C .. test-worker
```
