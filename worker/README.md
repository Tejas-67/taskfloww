# worker (Python)

Thin, **plug-and-play** worker SDK for TaskFloww. A developer writes an ordinary Python function,
maps it to a task name in the YAML config, and the worker runs it — no engine code changes.

Workers talk **only to RabbitMQ** (never directly to PostgreSQL) — see ADR-0002 (B1) in
[`../docs/DECISIONS.md`](../docs/DECISIONS.md). This keeps the SDK tiny and keeps database
credentials off the worker fleet.

**Phase 0 scaffold** — currently just structured JSON logging and a startup banner. Added in
Phase 4 (see [`../docs/ROADMAP.md`](../docs/ROADMAP.md)): task registry, RabbitMQ consume loop
with prefetch backpressure, heartbeat + result publishers, idempotency guard, graceful shutdown.

## Run

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .          # dev extras: pip install -e '.[dev]'
python -m taskfloww_worker # or: taskfloww-worker
```
