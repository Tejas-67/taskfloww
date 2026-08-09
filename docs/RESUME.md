# Resume Here 👋

Single source of truth for **where we are** and **what to do next**. Read this first when
returning to the project.

_Last updated: 2026-08-09 (Phase 6 — fault-tolerance tests)._

---

## TL;DR

We finished **discovery + planning** and shipped **Phase 0 → 6**: bootstrap, schema, config,
submission API, dispatcher+relay, result/heartbeat consumer, Python worker SDK, reaper, DLQ
replay, observability (Prometheus + Grafana), and now a **fault-tolerance test suite** —
multi-instance concurrency (no double dispatch/publish/reap), worker failure/idempotency tests, and
a **live crash-recovery E2E** (kill a worker mid-task → another finishes it). Next = **Phase 7
(docs + quickstart)** — the last one.

> ⚠️ Phase 6 is implemented and validated but **not yet committed** — the user commits manually.

---

## Current status

- ✅ Environment verified: right machine (`/Users/tejashwadeep.jha/personal`), personal GitHub SSH
  key present (`~/.ssh/id_ed25519_github2` → `Tejas-67`).
- ✅ **Git identity handled:** repo uses a **local** override → `Tejas-67` /
  `tejasjha54@gmail.com`, `commit.gpgsign=false` (work signing key NOT used). Verified on the
  Phase 0 commit (author+committer personal, signature `N`). A global `includeIf gitdir:~/personal/`
  supplies the personal name/email automatically; we still pin it locally for robustness.
- ✅ Plan persisted here in `taskfloww/docs/`.
- ✅ Decisions **A, B, C, D** all locked (see DECISIONS.md).
- ✅ **Phase 0 shipped** — monorepo scaffold builds (`go build` ✅, worker runs ✅), infra
  compose + Makefile + docs, pushed to `github.com/Tejas-67/taskfloww` (`main`).
- ✅ **Phase 1 implemented (uncommitted)** — 7 goose migrations (`tasks`, `schedules`,
  `task_executions`, `outbox`, `workers`, `dead_letters` + enums/triggers), Go domain models in
  `orchestrator/internal/domain` (+ tests), ERD in `docs/SCHEMA.md`, `migrations/README.md`,
  Makefile `migrate-*` targets. Validated `up`→v7 and `reset`→v0 clean against a throwaway
  Postgres 16; partial indexes/constraints/trigger + dispatcher EXPLAIN all confirmed.
- ✅ **Phase 2 implemented (uncommitted)** — plug-and-play config: Go loader `koanf`
  (`orchestrator/internal/config`) + Python loader `pydantic` (`worker/taskfloww_worker/config.py`),
  both with `${VAR:-default}` interpolation, `TASKFLOWW_*__*` env overrides, and fail-fast
  validation (heartbeat lease invariant, handler shape, queue refs, etc.). Wired into both mains
  (config-driven logging + redacted startup summary). `docs/CONFIG.md` reference. Tests: Go
  `internal/config` + Python `worker/tests` all green; smoke-tested both binaries on the example.
- ✅ **Phase 3a implemented (uncommitted)** — REST submission API. New Go packages: `internal/store`
  (pgx; the only task-state writer), `internal/service` (`SchedulerService`: validation, next_run_at,
  cron via robfig/cron), `internal/api` (chi: POST/GET/cancel, error mapping, request logging).
  Wired into `cmd/orchestrator` (connect Postgres → serve API). Supports immediate/delayed/recurring
  + priority + max_retries; idempotent on `id`; unknown task names rejected. Tests: service (fake
  store) + api (fake service) unit tests + a `-tags=integration` store test. **Validated end-to-end**
  against a throwaway Postgres: submit/get/cancel/idempotency/404/409/400 all correct; rows persisted.
- ✅ **Phase 3b implemented (uncommitted)** — dispatcher + outbox relay + RabbitMQ transport. New Go
  packages: `internal/broker` (amqp091: topology = priority queues + DLX, confirmed publishes),
  `internal/dispatcher` (claims due tasks via `FOR UPDATE SKIP LOCKED`, transition→dispatching+lease,
  execution-ledger row + outbox row in ONE tx), `internal/relay` (drains outbox→broker, marks
  published), `internal/message` (wire contract). Store gained `ClaimDueTasks` + `PublishOutbox`.
- ✅ **Phase 3c + 4 implemented (uncommitted)** — the execution loop closes. Go: `internal/consumer`
  (applies results idempotently via execution-state dedup → completed/retrying/dead+dead_letters;
  heartbeats renew leases + upsert workers), `internal/backoff` (exp/fixed + jitter), store
  `ApplyResult`/`RenewLeases`/`UpsertWorker`, control topology + `Consume` on the broker, message
  `control.go`. Python worker SDK: `messages`, `registry` (imports `module:function`), `worker`
  (pika consume + thread-pool exec + threadsafe result/ack + heartbeats + dedupe + graceful stop),
  `examples/tasks.py`, wired `__main__`. Dep: pika.
- ✅ **Phase 3d implemented (uncommitted)** — the reaper makes it self-healing. Go: `internal/reaper`
  (expired-lease re-queue → retry/backoff or dead+dead_letters; cron schedule firing → materialize
  recurring task runs; stale-worker marking), store `ReapExpiredLeases`/`FireDueSchedules`/
  `MarkStaleWorkers` + `NextFireFunc`, wired as a 4th engine loop. Robustness fixes found via E2E:
  worker now **self-declares** its work queues (can start before the orchestrator). Validated:
  reaper unit + 4 store integration tests, plus a **crash E2E** (kill -9 a worker mid-task →
  reaper re-queues → healthy worker completes; stale worker marked dead) on throwaway PG + RabbitMQ.
- ✅ **Phase 3e implemented (uncommitted)** — DLQ introspection + replay. Store
  `ListDeadLetters`/`GetDeadLetter`/`ReplayDeadLetter` (`internal/store/deadletters.go`); service +
  API routes `GET /v1/dead-letters`, `GET /v1/dead-letters/{id}`, `POST …/{id}/replay`. Fixed a real
  **replay bug** (reset attempt=0 collided with old executions on re-dispatch → now clears the prior
  run's execution ledger on replay). Validated: store integration (list/get/replay/conflict) + api
  unit tests, plus a **live DLQ E2E** (reaper drove a task to `dead` → list/inspect/replay via REST →
  task re-dispatched cleanly with a fresh execution ledger, second replay → 409).
- ✅ **Phase 5 implemented (uncommitted)** — observability. Go `internal/metrics` (promauto counters/
  gauges/histogram + a Postgres-backed gauge collector) instrumented across api/dispatcher/consumer/
  reaper/relay; `/metrics` served on the orchestrator. Python `taskfloww_worker/metrics.py`
  (prometheus_client) instrumented in the worker; `/metrics` on `metrics.worker_port`. Enabled the
  Prometheus scrape jobs + added a provisioned **Grafana** dashboard (`deploy/grafana`). Fixed a real
  bug found live: worker `_execute` used `time` without importing it → silent pool `NameError` left
  tasks stuck in-flight; added `import time` + a future-exception guard. Metrics unit tests (Go +
  Python) + a **live scrape E2E** confirmed both endpoints reflect real activity.
- ✅ **Phase 6 implemented (uncommitted)** — fault-tolerance test suite. Added
  `orchestrator/internal/store/concurrency_integration_test.go` proving the stateless-orchestrator
  guarantee under real contention: **no-double-dispatch** (K claimers), **no-double-publish** (K
  relays), **no-double-reap** (K reapers), each keyed on per-task ledger assertions and run under
  `-race`. Added `worker/tests/test_faults.py` — handler-failure publishes a *failed* Result and
  still acks (orchestrator owns retries), success path, unknown-task, and redelivery-after-completion
  idempotency (dedupe via `_processed`, not just in-flight). Sealed with a **live crash-recovery
  E2E**: SIGKILLed a worker mid-`slow`-task → reaper detected the expired lease → re-queued → a
  replacement worker completed it (ledger showed the crashed attempt `failed`/"lease expired (worker
  lost)" then a later attempt `succeeded`). Full suites green: Go units + 13 store integration tests
  (clean schema) + 29 Python tests.

## Locked decisions
- **A — Hybrid scheduling:** Postgres source of truth (SKIP LOCKED poller + lease reaper + cron +
  retry) · RabbitMQ transport + native DLX for DLQ · transactional outbox.
- **B — Worker↔DB (B1):** workers talk only to RabbitMQ; orchestrator owns all Postgres writes.
- **C — API surface:** REST-first via `chi` behind a `SchedulerService` interface; gRPC later.
- **D — Repo:** monorepo **`taskfloww`** (double-w, confirmed) under GitHub `Tejas-67`.
- **E — Languages:** Go orchestrator + Python workers (Java considered, rejected — see ADR/decisions).

## ⬅️ Next step
**Phase 6 done** (pending your manual commit). Next: **Phase 7 — docs & examples** (the last phase):
consolidate/polish README + architecture doc, example task functions + example config, a
compose-up quickstart (submit a task in <10 min without editing the engine), and an API reference.
Most of this content already exists across `docs/` — Phase 7 is mainly consolidation and a
top-to-bottom quickstart pass.

### Fault-tolerance test notes (Phase 6)
- Concurrency tests fire K goroutines through a start barrier (simulating K instances hammering the
  same rows) and assert **per-task** ledger integrity keyed on the test's own ids — so they're
  independent of other rows in the shared DB. Run with `-race`.
- Live crash-recovery E2E requires **consistent timings**: worker `heartbeat.interval_seconds` MUST
  be < orchestrator `lease_ttl_seconds` (renewal happens on each heartbeat via `RenewLeases`). A
  mismatch (e.g. lease 6s but worker interval 10s) makes the reaper reap live tasks — this is a
  config error, not a bug (the config comment already warns "lease_ttl must exceed interval + lag").
  For the E2E use e.g. interval=2s / lease=8s / reaper=2s, and set the interval on BOTH sides.

### Metrics notes (Phase 5)
- Go metrics use `promauto` on the default registry (package-level vars in `internal/metrics`), so
  components just call `metrics.X.Inc()`; `/metrics` = `promhttp.Handler()`. Gauges (active tasks by
  state, workers alive, outbox pending) are sampled from Postgres by a `Collector` loop.
- Worker metrics use `prometheus_client`; `start_http_server(worker_port)` exposes them.
- **Integration-test note carried over:** run store integration tests against a clean schema
  (`goose reset && goose up`) — greedy `ClaimDueTasks(100)` + shared DB causes false failures otherwise.

### DLQ notes (Phase 3e)
- Two DLQ layers: the **`dead_letters` table** is the authoritative, queryable/replayable DLQ
  (Postgres = source of truth); RabbitMQ's native **DLX → `tasks.dlq`** is a broker-level safety net
  for nacked/TTL-expired messages.
- **Replay** = reset task to `queued`, `attempt=0`, **delete the prior run's `task_executions`** (so
  re-dispatch doesn't collide on `uq_task_attempt`), stamp `dead_letters.replayed_at`. Second replay → 409.
- Integration tests share a DB and use greedy `ClaimDueTasks(100)`; run them against a **clean
  schema** (`goose reset && goose up`) — accumulated cruft from reruns causes false failures.

### Reaper notes (Phase 3d)
- The reaper runs three scans every `heartbeat.reaper_interval_seconds`, all `SKIP LOCKED` guarded:
  `ReapExpiredLeases` (state IN dispatching/running AND lease_expires_at < now → retry/dead),
  `FireDueSchedules` (enabled schedules due → materialize a `recurring` task + advance next_fire_at
  via `reaper.NextFire` cron), `MarkStaleWorkers`.
- Crash recovery has **two** layers: RabbitMQ redelivers a worker's *unacked* message when its
  connection drops (fast), and the reaper re-queues via the DB lease (backstop for hung/stuck
  workers or lost messages). Idempotency (execution-state dedup) keeps duplicates safe.
- The worker does a **graceful drain** on SIGTERM (finishes in-flight work); a real crash is SIGKILL.
- E2E used short timings via env: `TASKFLOWW_HEARTBEAT__LEASE_TTL_SECONDS=6`, `__INTERVAL_SECONDS=2`,
  `__REAPER_INTERVAL_SECONDS=2`.

---

## The immediate next step

> Execute **Phase 4 — Python worker SDK** (see ROADMAP.md): a thin Rabbit-only worker that consumes
> from the priority queues, dedupes on `execution_id`, runs the mapped `module:function`, and
> publishes a result message + periodic heartbeats. Pairs with **Phase 3c** (orchestrator consumer
> that applies results and renews leases). The wire contract is `internal/message.Task` (mirror it
> in Python).

**Tooling installed this session:** Go 1.26.5, `goose` (`~/go/bin`), PostgreSQL 16
(`/opt/homebrew/opt/postgresql@16`, keg-only), **RabbitMQ 4.3.4** (`/opt/homebrew/opt/rabbitmq`),
Python venv at `worker/.venv` (pydantic + pyyaml + pytest). **Docker is still not installed** —
install Docker Desktop (or `colima`) to run `make up`. Phases 3a/3b were validated against
throwaway local Postgres + RabbitMQ nodes (initdb/goose/curl + rabbitmq-server).

### Engine notes (Phase 3a/3b)
- Layering: `api` (chi) → `service` (`SchedulerService`, validation/cron) → `store` (pgx). The
  `dispatcher` and `relay` are background loops; `broker` owns AMQP. The store is the ONLY task-state
  writer (ADR-0002/B1).
- Dispatch is one tx: claim (`FOR UPDATE SKIP LOCKED`) → `dispatching` + lease + `attempt++` →
  `task_executions` row (attempt_number) → `outbox` row. The relay publishes the outbox with
  publisher confirms and stamps `published_at` (at-least-once publish; workers dedupe on execution_id).
- Message routing: task's `queue` (from config mapping) → `QueueDef.routing_key` on
  `queues.default_exchange`; AMQP priority = task priority clamped to the queue's `max_priority`.
- Recurring submissions create a `schedules` row (a cron *definition*); firing/materialization into
  `tasks` runs is Phase 3d. Immediate/delayed create a `tasks` row directly.
- Store integration tests are build-tagged `integration` and need `TASKFLOWW_TEST_DB_URI`.

### Config loader notes (Phase 2)
- Go loader lives in `orchestrator/internal/config` (koanf); Python in
  `worker/taskfloww_worker/config.py` (pydantic BaseModel + explicit env overlay — chosen over
  `pydantic-settings` because the primary source is a YAML file with `${}` interpolation).
- Precedence: defaults → YAML file → `TASKFLOWW_*__*` env. `${VAR:-default}` interpolation on both.
- Both fail-fast and aggregate every problem. Passwords are redacted in startup logs.

---

## Where everything lives

| Artifact | Location |
|---|---|
| Master plan (architecture, data flow, failure modes) | `docs/PLAN.md` |
| Decision log (ADRs) | `docs/DECISIONS.md` |
| Phased roadmap + dependency graph | `docs/ROADMAP.md` |
| DB schema, ERD, indexing rationale | `docs/SCHEMA.md` |
| Config reference (plug-and-play) | `docs/CONFIG.md` |
| REST API reference | `docs/API.md` |
| This resume file | `docs/RESUME.md` |
| Live task tracking | session DB `todos` / `todo_deps` |
| Decision record (machine-readable) | session DB `decisions` |

## How to resume in a new session
1. Open `docs/RESUME.md` (this file), then `docs/ROADMAP.md`.
2. Re-hydrate tracking if needed (the session DB may not carry over):
   the phase list in ROADMAP.md is the canonical backlog.
3. Pull the repo (`git@github.com:Tejas-67/taskfloww.git`) and start the first `[ ]` phase (Phase 3a).

## Progress log
- **2026-08-09 (Phase 6)** — Fault-tolerance test suite. Added multi-instance concurrency
  integration tests (`concurrency_integration_test.go`): no-double-dispatch / -publish / -reap under
  a start-barrier of K goroutines, per-task ledger assertions, run under `-race`. Added
  `worker/tests/test_faults.py` (handler-failure→failed-result-acked, success, unknown-task,
  redelivery-after-completion idempotency). Live crash-recovery E2E: SIGKILL worker mid-`slow` →
  reaper re-queued via expired lease → replacement worker completed (attempt failed "lease expired
  (worker lost)" → later attempt succeeded). Suites green: Go units + 13 integration + 29 Python.
  Caught a self-inflicted timing pitfall (worker interval must be < lease TTL) — documented. **Not
  committed**.
- **2026-08-08 (Phase 5)** — Observability. Go `internal/metrics` (promauto counters/gauges/histogram
  + Postgres gauge collector) instrumented across api/dispatcher/consumer/reaper/relay; `/metrics`
  served. Python `taskfloww_worker/metrics.py` (prometheus_client) in the worker; `/metrics` on
  `worker_port`. Enabled Prometheus scrape jobs + provisioned Grafana dashboard (`deploy/grafana`).
  Fixed a live-found bug (worker `_execute` missing `import time` → silent pool crash, tasks stuck) +
  a future-exception guard. Unit tests (Go+Python) + live scrape E2E green. **Not committed**.
- **2026-08-08 (Phase 3e)** — DLQ introspection + replay. Store `deadletters.go`
  (`ListDeadLetters`/`GetDeadLetter`/`ReplayDeadLetter`), service + API routes under
  `/v1/dead-letters`. Fixed a real replay bug (clear prior executions on replay to avoid
  `uq_task_attempt` collision on re-dispatch). Store integration + api unit tests + a live REST E2E
  (task driven to `dead` by the reaper → list/inspect/replay → clean re-dispatch, 409 on 2nd replay).
  **Not committed** (user commits manually).
- **2026-08-04 (Phase 3d)** — Reaper (self-healing). Go: `internal/reaper` (expired-lease re-queue,
  cron schedule firing via robfig/cron, stale-worker marking), store `ReapExpiredLeases`/
  `FireDueSchedules`/`MarkStaleWorkers`, wired as a 4th engine loop. Worker now self-declares work
  queues (start-order independent) + quieter pika logging; added `slow` example task. Fixed via
  E2E: worker 404 when starting before orchestrator. Reaper unit + 4 store integration tests green;
  **crash E2E** (kill -9 mid-task → reaper re-queue → healthy worker completes) proven on throwaway
  PG + RabbitMQ. **Not committed** (user commits manually).
- **2026-08-04 (Phase 3c + 4)** — Closed the execution loop. Go: `internal/consumer` (idempotent
  result apply via execution-state dedup → completed/retrying/dead+dead_letters; heartbeat lease
  renew + worker upsert), `internal/backoff` (exp/fixed+jitter), store `ApplyResult`/`RenewLeases`/
  `UpsertWorker`, broker control topology + `Consume`, `message/control.go`, config `control`
  section. Python worker SDK: `messages`/`registry`/`worker`/`examples/tasks.py`, wired `__main__`,
  pika dep. Fixed 3 real bugs found by tests/E2E: worker-FK ordering (set worker_id only if the
  worker row exists), retry off-by-one (`attempt <= max_retries`), nil `queues` on upsert; plus a
  broker **channel-recovery** fix (a poison publish no longer wedges the relay). Full E2E (PG +
  RabbitMQ) proved happy + failure paths; store integration + both unit suites green.
  **Not committed** (user commits manually).
- **2026-08-04 (Phase 3b)** — Dispatcher + outbox relay + RabbitMQ. New Go packages: `internal/broker`
  (amqp091: topology priority queues + DLX, confirmed publishes), `internal/dispatcher` (claim due via
  `FOR UPDATE SKIP LOCKED` → transition+lease+attempt++ → execution row + outbox row in one tx),
  `internal/relay` (drain outbox → broker, mark published/attempts), `internal/message` (wire
  contract). Store: `ClaimDueTasks` (aliased RETURNING to avoid ambiguous `id`) + `PublishOutbox`.
  Wired both loops into `cmd/orchestrator` with WaitGroup shutdown. Dep: rabbitmq/amqp091-go. Tests:
  dispatcher/relay unit (fakes) + store integration. Validated E2E on throwaway Postgres + RabbitMQ
  4.3.4 (installed via brew): messages routed to correct priority queues with correct body/priority.
  **Not committed** (user commits manually).
- **2026-07-31 (Phase 3a)** — Submission API. New Go packages: `internal/store` (pgx; only
  task-state writer, sentinel errors, jsonb/enum/uuid casts), `internal/service`
  (`SchedulerService`: validate task_name against config, next_run_at for immediate/delayed, cron
  via robfig/cron for recurring→schedule, idempotent submit), `internal/api` (chi router: POST
  `/v1/tasks`, GET/cancel, error→status mapping, request logging). Wired `cmd/orchestrator` to
  connect Postgres + serve. Tests: service (fake store), api (fake service), store integration
  (`-tags=integration`). Deps: chi, pgx, robfig/cron, google/uuid. Validated end-to-end on a
  throwaway Postgres (submit immediate/delayed/recurring, get, cancel, idempotency, 400/404/409,
  rows persisted). Added `docs/API.md`. **Not committed** (user commits manually).
- **2026-07-31 (Phase 2)** — Plug-and-play config on both sides. Go: `internal/config` (koanf) with
  `${VAR:-default}` interpolation, `TASKFLOWW_*__*` env overrides, aggregated fail-fast validation,
  `RedactURI`, + tests; wired into `cmd/orchestrator`. Python: `taskfloww_worker/config.py`
  (pydantic) mirroring the schema + `worker/tests` (pytest, 8 passing); wired into `__main__`.
  Formalized `config/config.example.yaml`, added `docs/CONFIG.md`, Makefile `test-worker` +
  fixed `run-worker` path. Both binaries smoke-tested on the example; bad configs fail fast.
  **Not committed** (user commits manually).
- **2026-07-30 (Phase 1)** — Designed the schema: 7 goose migrations (enums+triggers, workers,
  schedules, tasks, task_executions, outbox, dead_letters) with partial indexes tuned for the
  dispatcher due-scan and reaper lease-scan, fillfactor tuning, idempotency ledger, transactional
  outbox, and DLQ table. Added Go domain models + tests (`orchestrator/internal/domain`), ERD
  (`docs/SCHEMA.md`), `migrations/README.md`, and Makefile `migrate-*` targets. Installed goose +
  Postgres 16; validated `up`→v7 and `reset`→v0 clean, plus functional constraint/trigger/EXPLAIN
  checks. **Not committed** (user commits manually).
- **2026-07-29 (Phase 0)** — Scaffolded monorepo (orchestrator Go skeleton, worker Python SDK
  skeleton, docker-compose infra, config preview, Makefile, MIT license, .gitignore). Set local
  git identity (`Tejas-67`/`tejasjha54@gmail.com`, signing off), branch `main`, SSH remote.
  Verified `go build`/`go vet`/`/healthz`, worker run, YAML parse, `make help`. Committed
  (`cc6fea0`) and pushed to `github.com/Tejas-67/taskfloww`. Installed Go 1.26.5 via brew.
- **2026-07-29 (Planning)** — Discovery + environment check; locked ADR-0001 (hybrid), ADR-0002 (B1),
  ADR-0003 (REST-first), ADR-0004 (monorepo `taskfloww`); authored PLAN/DECISIONS/ROADMAP/RESUME;
  created `taskfloww/` folder. All decisions locked; Phase 0 next. No code yet.
