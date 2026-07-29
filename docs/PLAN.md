# TaskFloww — Master Plan

> Distributed Task Scheduler & Workflow Engine.
> Go orchestrator · Python workers · PostgreSQL state · RabbitMQ transport.
> **Status:** Planning complete — all four core decisions locked. Repo name: **`taskfloww`** (see DECISIONS.md).

---

## 1. Vision (one-liner)

A **stateless**, **config-driven** ("plug-and-play") task scheduler where a developer just
writes a Python function, maps it to a task name in a YAML file, and the engine handles
scheduling (immediate / delayed / cron), at-least-once execution, retries with backoff,
heartbeat-based crash recovery, a Dead Letter Queue, and Prometheus/JSON observability —
**without touching the core engine code**.

---

## 2. Tech stack

| Layer | Choice | Notes |
|---|---|---|
| Orchestrator | **Go** | Stateless; N instances behind a load balancer |
| Worker nodes | **Python** | Thin SDK; user code + config only |
| State / source of truth | **PostgreSQL** | State machine, scheduler, leases, retry accounting |
| Message broker | **RabbitMQ** | Transport for ready work + native DLX for the DLQ |
| Metrics | **Prometheus** | `/metrics` on orchestrator + workers |
| Logging | **Structured JSON** | Go `slog` · Python `structlog` |
| Local dev | **docker-compose** | Postgres + RabbitMQ + Prometheus |

**Provisional stack defaults (locked unless changed):** Go `pgx` + `sqlc` + `goose` migrations;
Go config `koanf` (or `viper`); Python `pydantic-settings` + `pika`/`aio-pika`;
retry = exponential backoff **+ jitter**; idempotency via a `task_executions` ledger (no Redis).

---

## 3. Architecture overview

```
                         ┌──────────────────────────────────────────┐
  Client ──submit──▶ REST/gRPC API │  GO ORCHESTRATOR (stateless, xN) │
                         │                                          │
                         │  ┌────────────┐   ┌────────────────────┐ │
                         │  │ Dispatcher │   │ Result/HB Consumer │ │
                         │  │(SKIP LOCKED│   │ (single PG writer) │ │
                         │  │  poller)   │   └─────────▲──────────┘ │
                         │  └─────┬──────┘             │            │
                         │  ┌─────┴──────┐   ┌─────────┴──────────┐ │
                         │  │  Reaper    │   │  Outbox Relay      │ │
                         │  │(lease+cron)│   │ (atomic publish)   │ │
                         │  └────────────┘   └────────────────────┘ │
                         └──────┬───────────────────────▲───────────┘
                                │ publish ready work     │ result/heartbeat msgs
                    ┌───────────▼─────────┐   ┌──────────┴───────────┐
   state ◀────────▶ │     PostgreSQL      │   │      RabbitMQ        │
   (source of truth)│ tasks / executions  │   │ priority queues +   │
                    │ outbox / workers    │   │ DLX → DLQ           │
                    └─────────────────────┘   └──────────┬──────────┘
                                                         │ consume
                                              ┌──────────▼──────────┐
                                              │  PYTHON WORKERS (xN) │
                                              │  registry + run loop │
                                              │  heartbeat + result  │
                                              └─────────────────────┘
```

### Orchestrator components (all stateless; coordinate via Postgres)
- **API** — submit / cancel / query tasks. Writes task row **and** outbox row in one tx.
- **Dispatcher** — polls due tasks with `SELECT … FOR UPDATE SKIP LOCKED`, leases them,
  hands to the outbox relay for publishing.
- **Outbox relay** — publishes committed outbox rows to RabbitMQ (atomic state↔publish).
- **Result/Heartbeat consumer** — the **only** writer of terminal state; renews leases,
  records executions idempotently.
- **Reaper** — expired-lease re-queue (retry/backoff → DLQ), worker liveness, cron `next_run_at`.

### Worker (thin, Rabbit-only — Decision B1)
Connect to RabbitMQ → consume by routing key → dedupe via execution id → run registered
Python fn → publish result + periodic heartbeat. **No DB credentials on workers.**

---

## 4. Locked design decisions (see DECISIONS.md for full ADRs)

- **A — Scheduling engine:** *Hybrid.* Postgres is source of truth (SKIP LOCKED poller,
  lease reaper, cron, retry accounting). RabbitMQ transports ready work and provides the
  **native DLX** for the DLQ. A **transactional outbox** keeps state-commit and publish atomic.
- **B — Worker↔DB boundary:** *B1.* Workers talk **only** to RabbitMQ. The orchestrator
  consumes result/heartbeat messages and owns **all** Postgres writes.

- **C — API surface:** *REST-first* via `chi` behind a `SchedulerService` interface; gRPC later.
- **D — Repo:** monorepo **`taskfloww`** under GitHub `Tejas-67`; local git identity override.

---

## 5. Data flow — happy path (submission → completion)

1. **Submit** → API validates, inserts `tasks(state=queued, next_run_at=…)` + `outbox` row (one tx).
2. **Dispatch** → dispatcher claims due rows (`SKIP LOCKED`), sets `state=dispatching`,
   `locked_by`, `lease_expires_at`.
3. **Publish** → outbox relay publishes to the priority queue for the task's routing key.
4. **Consume** → a worker pulls the message (prefetch = backpressure), sets local run context.
5. **Heartbeat** → worker emits heartbeats every `heartbeat_interval`; consumer renews `lease_expires_at`.
6. **Execute** → worker runs the mapped Python function (idempotency-guarded by execution id).
7. **Result** → worker publishes success/failure; consumer writes terminal state
   (`completed`/`failed`) and appends to `task_executions`.
8. **Recurring** → on completion of a cron task, reaper computes the next `next_run_at`
   and resets `state=queued`.

---

## 6. Failure scenarios (this is the core value)

| Scenario | Detection | Recovery |
|---|---|---|
| **Worker crashes mid-task** | Heartbeats stop → `lease_expires_at < now()` | Reaper: `attempt++`, backoff, `state=retrying`, re-dispatch to a healthy worker |
| **Orchestrator instance dies** | Nothing — it's stateless | Other instances keep polling via `SKIP LOCKED`; in-flight lease simply expires and is reaped |
| **Duplicate delivery** (at-least-once) | Worker checks execution id | Idempotency guard: second delivery is a no-op / returns prior result |
| **Poison / repeatedly failing task** | `attempt > max_retries` | Move to **DLQ** (RabbitMQ DLX + `dead_letters` table) for inspection/replay |
| **Publish succeeds, state write lost (or vice-versa)** | — | **Outbox** guarantees exactly-one publish per committed state transition |
| **Broker backlog / slow workers** | Prefetch + queue-depth metric | Backpressure; autoscale workers; alert on queue depth |

---

## 7. Non-functional requirements

- **Stateless orchestrator** → horizontal scale behind a load balancer; all state in PG/Rabbit.
- **Async high-throughput** → batched `SKIP LOCKED` claims, priority queues, prefetch.
- **Observability-first** → Prometheus (queue depth, worker count, success/failure/retry rates,
  latency histograms) + structured JSON logs with correlation ids (`task_id`, `execution_id`).
- **Plug-and-play** → everything (DSNs, queues, routing, retry policy, heartbeat/timeout,
  task→function map) lives in one config file. No core-engine edits to onboard a task.

---

## 8. Key tunables (defaults — all config-driven)

| Parameter | Default | Rationale |
|---|---|---|
| `heartbeat_interval` | 10s | Frequent enough to detect crashes quickly |
| `lease_ttl` | 60s | > heartbeat + broker/consumer lag to avoid false re-queue |
| `reaper_interval` | 15s | Bounds detection latency without hammering PG |
| `dispatch_batch_size` | 100 | Amortizes poll cost |
| `poll_interval` | 200ms–1s | Latency vs. PG load tradeoff |
| `max_retries` (default) | 5 | Per-task override allowed |
| `backoff` | exponential + jitter | Avoids thundering-herd retries |

---

## 9. Explicitly out of scope (v1)

DAG / multi-step workflow dependencies (single tasks first), a UI dashboard, multi-tenant
auth/RBAC, and exactly-once side effects (we provide **at-least-once + idempotency**).
Revisit after the core engine is proven.
