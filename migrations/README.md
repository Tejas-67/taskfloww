# Migrations

PostgreSQL schema migrations, managed with [goose](https://github.com/pressly/goose).
Each file has `-- +goose Up` / `-- +goose Down` sections and is applied in filename order.

TaskFloww uses **PostgreSQL as its source of truth** — see [`../docs/SCHEMA.md`](../docs/SCHEMA.md)
for the ERD, table purposes, indexing strategy, and design rationale.

## Files

| # | File | Adds |
|---|---|---|
| 1 | `00001_enums_and_functions.sql` | enums (`task_state`, `execution_type`, `execution_state`, `worker_status`) + `set_updated_at()` |
| 2 | `00002_workers.sql` | `workers` registry + liveness index |
| 3 | `00003_schedules.sql` | `schedules` (cron definitions) + due index |
| 4 | `00004_tasks.sql` | `tasks` state machine + dispatcher/reaper partial indexes |
| 5 | `00005_task_executions.sql` | `task_executions` idempotency ledger |
| 6 | `00006_outbox.sql` | transactional `outbox` |
| 7 | `00007_dead_letters.sql` | `dead_letters` (DLQ introspection) |

## Prerequisites

```bash
# goose CLI
go install github.com/pressly/goose/v3/cmd/goose@latest   # -> $(go env GOPATH)/bin/goose

# a running Postgres (via the dev stack)
make up                                                   # deploy/docker-compose.yml
```

## Usage

Set the connection string (matches `deploy/.env.example`):

```bash
export DATABASE_URI="postgres://taskfloww:taskfloww@localhost:5432/taskfloww?sslmode=disable"
```

Then, from the repo root:

```bash
make migrate-up        # apply all pending migrations
make migrate-status    # show applied / pending
make migrate-down      # roll back the most recent migration
make migrate-reset     # roll back everything (dev only)
```

…or invoke goose directly:

```bash
goose -dir migrations postgres "$DATABASE_URI" up
```

## Conventions

- **Reversible:** every migration has a working `Down`. `up` then `reset` must leave a clean
  database (verified in Phase 1).
- **Transactional:** goose wraps each file in a transaction. All Phase 1 DDL is transaction-safe.
  A migration that needs `CREATE INDEX CONCURRENTLY` or `ALTER TYPE ... ADD VALUE` must start with
  `-- +goose NO TRANSACTION`.
- **plpgsql bodies** (functions/DO blocks) must be wrapped in
  `-- +goose StatementBegin` / `-- +goose StatementEnd` so goose doesn't split on inner `;`.
- **Enum values must stay in sync** with `orchestrator/internal/domain` (guarded by `enums_test.go`).
