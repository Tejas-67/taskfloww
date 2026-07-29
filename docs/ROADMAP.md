# Roadmap

Phased build order. Dependencies mirror the session DB (`todos` / `todo_deps`).
Legend: `[ ]` pending · `[~]` in progress · `[x]` done · `[!]` blocked.

---

## Phase 0 — Bootstrap  `[x]`  (`phase0-bootstrap`)
Init monorepo; set **local** git identity (`Tejas-67` / `tejasjha54@gmail.com`) and disable the
work signing key; `.gitignore` / `README` / `LICENSE`; create the **Tejas-67** GitHub repo and
first push over SSH; `docker-compose` with Postgres + RabbitMQ + Prometheus.
**Deliverable:** `docker-compose up` gives a clean Postgres + RabbitMQ + Prometheus; repo on GitHub.
**Depends on:** —

## Phase 1 — Schema & migrations  `[ ]`  (`phase1-schema`)
DDL for `tasks`, `task_executions` (idempotency ledger), `outbox`, `workers`, `schedules`
(recurring). Partial indexes tuned for state updates + due-scan. `goose` wiring.
**Deliverable:** `migrate up/down` runs clean; ERD in docs.
**Depends on:** Phase 0

## Phase 2 — Plug-and-play config  `[ ]`  (`phase2-config`)
YAML schema + loaders/validation for Go (`koanf`) and Python (`pydantic-settings`): broker DSN,
DB URI, queue/routing defs, retry policy, heartbeat/timeout, **task→function map**, env overrides.
**Deliverable:** one `config.yaml` drives both sides; invalid config fails fast with clear errors.
**Depends on:** Phase 0

## Phase 3a — Submission API  `[ ]`  (`phase3a-api`)
REST-first (`chi`) behind a `SchedulerService` interface. Validate + insert task **and** outbox row
in one tx. Supports immediate / delayed / recurring + priority + `max_retries`.
**Deliverable:** `POST /tasks` persists a task and its outbox row atomically.
**Depends on:** Phases 1, 2

## Phase 3b — Dispatcher + outbox relay  `[ ]`  (`phase3b-dispatcher`)
Due-task claim via `SELECT … FOR UPDATE SKIP LOCKED`; publish to RabbitMQ priority queues;
outbox relay for atomic publish; mark dispatched.
**Deliverable:** queued task appears on the right RabbitMQ queue exactly once.
**Depends on:** Phases 1, 2

## Phase 3c — Result/heartbeat consumer  `[ ]`  (`phase3c-consumer`)
Consume worker result + heartbeat messages; update state, renew `lease_expires_at`, record
executions idempotently. **The only component that writes terminal task state.**
**Deliverable:** completion + heartbeat messages correctly mutate state.
**Depends on:** Phases 1, 2

## Phase 3d — Reaper  `[ ]`  (`phase3d-reaper`)
Worker-liveness scan + task-lease-expiry scan → re-queue (retry, exp backoff + jitter) or DLQ when
`attempt > max`; compute cron `next_run_at`. Guarded across instances via `SKIP LOCKED`/advisory locks.
**Deliverable:** killing a worker mid-task re-queues to a healthy worker.
**Depends on:** Phase 3c

## Phase 3e — DLQ wiring  `[ ]`  (`phase3e-dlq`)
Native dead-letter exchange topology + `dead_letters` table for introspection; move exhausted tasks;
replay path.
**Deliverable:** tasks past `max_retries` land in the DLQ and are inspectable/replayable.
**Depends on:** Phase 3d

## Phase 4 — Python worker SDK  `[ ]`  (`phase4-worker-sdk`)
Rabbit-only worker: config load, task registry (decorator + config-path mapping), prefetch loop,
ack/nack, idempotency guard, periodic heartbeat + result publishers, structured logging,
graceful shutdown.
**Deliverable:** `worker.run(config)` executes a registered function end-to-end.
**Depends on:** Phases 2, 3b

## Phase 5 — Observability  `[ ]`  (`phase5-observability`)
Go `slog`(JSON)+`client_golang`; Python `structlog`+`prometheus_client`; `/metrics` endpoints.
Metrics: queue depth, worker count, success/failure/retry counters, latency histograms.
**Deliverable:** Prometheus scrapes both; a starter dashboard.
**Depends on:** Phases 3b, 4

## Phase 6 — Fault-tolerance testing  `[ ]`  (`phase6-testing`)
Unit (state machine, backoff, cron), integration (submit→dispatch→execute→complete), chaos (kill
worker mid-task → re-queue), idempotency (duplicate delivery), DLQ overflow, multi-orchestrator
no-double-dispatch.
**Deliverable:** green test suite proving the guarantees.
**Depends on:** Phases 3d, 3e, 4

## Phase 7 — Docs & examples  `[ ]`  (`phase7-docs`)
README + architecture doc + example task functions + example config + quickstart (compose up +
submit a task) + API reference.
**Deliverable:** a new developer onboards a task in <10 minutes without editing the engine.
**Depends on:** Phase 6

---

## Dependency graph

```
phase0 ─┬─ phase1 ─┬─ phase3a
        │          ├─ phase3b ─┬─ phase4 ─┐
        │          └─ phase3c ─┴─ phase3d ─ phase3e ─┐
        └─ phase2 ──(feeds 3a/3b/3c/4)               │
                                                     ├─ phase6 ─ phase7
   phase3b + phase4 ── phase5                        │
   phase3d + phase3e + phase4 ──────────────────────┘
```

## Suggested delivery milestones
- **M1 (walking skeleton):** Phases 0→1→2→3a→3b→4 — submit a task, worker runs it.
- **M2 (fault tolerance):** Phases 3c→3d→3e — retries, re-queue, DLQ.
- **M3 (production-ish):** Phases 5→6→7 — metrics, tests, docs.
