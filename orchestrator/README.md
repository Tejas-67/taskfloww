# orchestrator (Go)

Stateless orchestrator for TaskFloww. All state lives in PostgreSQL and RabbitMQ, so any number
of instances can run behind a load balancer.

**Phase 3d** — the orchestrator runs the full self-healing engine: config-driven boot; PostgreSQL +
RabbitMQ; the REST **submission API**; **dispatcher** (SKIP LOCKED → outbox), **outbox relay**
(→ RabbitMQ, with channel recovery), **result/heartbeat consumer** (← workers), and the **reaper**
(expired-lease re-queue for crashed workers, cron schedule firing, stale-worker marking).

## Packages

| Package | Responsibility |
|---|---|
| `cmd/orchestrator` | entrypoint: config → DB + broker + topology → API + engine loops → serve |
| `internal/config` | plug-and-play config loader (koanf) |
| `internal/domain` | task/worker state machines + models (mirrors the SQL schema) |
| `internal/store` | PostgreSQL persistence (pgx); the only writer of task state |
| `internal/service` | `SchedulerService` — validation, scheduling, cron (transport-agnostic) |
| `internal/api` | REST layer (chi): decode → service → encode; error mapping |
| `internal/broker` | RabbitMQ transport: topology (priority queues + DLX + control), confirmed publishes |
| `internal/dispatcher` | claims due tasks (`FOR UPDATE SKIP LOCKED`) → execution ledger + outbox |
| `internal/relay` | drains the outbox → broker (exactly-one-publish per committed transition) |
| `internal/consumer` | applies worker results (idempotent) + heartbeats (lease renew, worker upsert) |
| `internal/reaper` | self-healing scans: expired-lease re-queue, cron schedule firing, stale-worker marking |
| `internal/backoff` | retry-delay policy (exponential/fixed + jitter) |
| `internal/metrics` | Prometheus metrics + a Postgres-backed gauge collector; serves `/metrics` |
| `internal/message` | wire contract for task/result/heartbeat messages (mirrored by the Python worker) |

Coming next: Prometheus metrics (5), fault-tolerance test suite (6), docs (7) — see [`../docs/ROADMAP.md`](../docs/ROADMAP.md).

## Run

```bash
# needs Postgres (see ../deploy) migrated with goose
go build ./...
./orchestrator -config ../config/config.example.yaml     # serves :8080
curl -s localhost:8080/healthz                            # {"status":"ok"}
```

## Test

```bash
go test ./...                                   # unit tests (no DB needed)
TASKFLOWW_TEST_DB_URI="postgres://…?sslmode=disable" \
  go test -tags=integration ./internal/store/   # store integration tests (needs Postgres)
```

Module path: `github.com/Tejas-67/taskfloww/orchestrator`. API reference: [`../docs/API.md`](../docs/API.md).
