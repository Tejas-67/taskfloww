# Decision Log (ADRs)

Lightweight Architecture Decision Records. Status: **Accepted** = locked, **Proposed** = default we'll
build on unless changed. Mirrored in the session DB (`decisions` table).

---

## ADR-0001 — Scheduling & fault-tolerance engine — **Accepted**

**Context:** The engine must support delayed, recurring (cron), durable state tracking,
heartbeat-based re-queue (at-least-once), a DLQ, and a stateless orchestrator.

**Options considered:**
1. **Postgres as source of truth** — `SKIP LOCKED` due-task poller + lease/visibility-timeout reaper.
2. **RabbitMQ-centric** — delayed-message plugin + per-message TTL/DLX.
3. **Hybrid** — Postgres backbone + RabbitMQ transport + native DLX + transactional outbox.

**Decision:** **Option 3 (Hybrid).** Postgres owns the state machine, scheduler (`next_run_at`),
cron, lease-based re-queue, and retry accounting. RabbitMQ transports *ready* work and provides
the **native DLX** for the DLQ. A **transactional outbox** keeps state-commit and publish atomic.

**Why:** Pure RabbitMQ still needs a poller (no cron), fights `consumer_timeout` for long tasks,
and can't introspect/cancel delayed messages → dual-writes. Pure Postgres rebuilds a DLQ that
Rabbit gives for free. Hybrid satisfies every requirement with the fewest custom mechanisms and
keeps state queryable/cancellable. `SKIP LOCKED` + partial indexes scale well past a side project.

**Consequences:** We build a poller, reaper, and outbox relay. Polling adds ≤1s latency (acceptable).
Hot-row update pressure mitigated with partial indexes / batching / fillfactor.

---

## ADR-0002 — Worker ↔ Database boundary — **Accepted**

**Context:** Where do workers read/write state, and who renews the lease/heartbeat?

**Options considered:**
- **B1** — Workers talk **only** to RabbitMQ; orchestrator consumes result/heartbeat messages
  and owns **all** Postgres writes.
- **B2** — Workers write state & heartbeats directly to Postgres.
- **B3** — Data over RabbitMQ, but a thin orchestrator control API for heartbeat/lease renewal.

**Decision:** **B1.**

**Why:** Best fit for the plug-and-play thin SDK; **no DB credentials on the worker fleet**;
single writer to Postgres (cleaner concurrency + schema evolution); no PgBouncer needed for
worker connection sprawl. Cost = one message hop of heartbeat latency, absorbed by sizing
`lease_ttl` (10s heartbeat, 60s lease, 15s reaper).

**Consequences:** Need a result/heartbeat consumer component and a heartbeat message channel.
Lease TTL must always exceed heartbeat interval + broker/consumer lag.

---

## ADR-0003 — API surface — **Accepted**

**Context:** Task submission must expose REST and/or gRPC.

**Decision:** **REST-first** using Go `chi`, behind a `SchedulerService` interface so a gRPC
implementation can be added later without touching business logic. Optionally add
`grpc-gateway` to serve both from one definition when gRPC lands.

**Why:** Fastest to build and test now; the interface keeps business logic transport-agnostic
so gRPC is additive, not a rewrite.

---

## ADR-0004 — Repository name & structure — **Accepted**

**Context:** Personal project; must be pushed to **GitHub `Tejas-67`** (NOT the work GitLab).

**Decision:**
- **Monorepo**, name **`taskfloww`** (double-w, confirmed intentional).
- Layout:
  ```
  taskfloww/
    orchestrator/     # Go: api, dispatcher, consumer, reaper, outbox
    worker/           # Python SDK + example tasks
    migrations/       # SQL (goose/golang-migrate)
    config/           # example plug-and-play YAML
    deploy/           # docker-compose, prometheus, dashboards
    docs/             # this planning set
  ```
- **Git identity (critical):** set **local** repo config to
  `user.name=Tejas-67`, `user.email=tejasjha54@gmail.com`, and **unset the work signing key**
  locally — matching the existing `ExpenseTracker` repo. Push over SSH via `~/.ssh/id_ed25519_github2`.

**Open question for the user:** confirm/rename the repo, and confirm monorepo vs. split repos.
_Resolved: `taskfloww`, monorepo._

---

## Stack defaults (bundled, low-risk — change any on request)

| Area | Default |
|---|---|
| Go DB access | `pgx` + `sqlc` (typed queries) |
| Migrations | `goose` (or `golang-migrate`) |
| Go config | `koanf` (or `viper`) |
| Go router | `chi` |
| Go logging | `slog` (JSON handler) |
| Go metrics | `prometheus/client_golang` |
| Python broker | `pika` (sync) or `aio-pika` (async) |
| Python config | `pydantic-settings` (YAML + env) |
| Python logging | `structlog` |
| Python metrics | `prometheus_client` |
| Idempotency store | Postgres `task_executions` ledger (no Redis) |
| Local dev | `docker-compose` |

---

## Decision status board

| ID | Topic | Status |
|---|---|---|
| ADR-0001 | Scheduling engine | ✅ Accepted (Hybrid) |
| ADR-0002 | Worker↔DB boundary | ✅ Accepted (B1) |
| ADR-0003 | API surface | ✅ Accepted (REST-first) |
| ADR-0004 | Repo name & structure | ✅ Accepted (monorepo `taskfloww`) |
