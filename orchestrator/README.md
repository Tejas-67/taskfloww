# orchestrator (Go)

Stateless orchestrator for TaskFloww. All state lives in PostgreSQL and RabbitMQ, so any number
of instances can run behind a load balancer.

**Phase 3a** — config-driven boot, PostgreSQL connection (source of truth), and the REST
**submission API** (chi) behind the `SchedulerService` interface, with structured JSON logging and
graceful shutdown.

## Packages

| Package | Responsibility |
|---|---|
| `cmd/orchestrator` | entrypoint: load config → connect DB → build API → serve |
| `internal/config` | plug-and-play config loader (koanf) |
| `internal/domain` | task/worker state machines + models (mirrors the SQL schema) |
| `internal/store` | PostgreSQL persistence (pgx); the only writer of task state |
| `internal/service` | `SchedulerService` — validation, scheduling, cron (transport-agnostic) |
| `internal/api` | REST layer (chi): decode → service → encode; error mapping |

Coming next: dispatcher (SKIP LOCKED) + outbox relay (3b), result/heartbeat consumer (3c),
reaper (3d) — see [`../docs/ROADMAP.md`](../docs/ROADMAP.md).

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
