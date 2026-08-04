# TaskFloww

> A **stateless**, **config-driven** distributed **Task Scheduler & Workflow Engine**.
> Go orchestrator · Python workers · PostgreSQL (source of truth) · RabbitMQ (transport + DLQ).

[![status](https://img.shields.io/badge/status-phase%203a%20submission%20api-orange)]()
[![license](https://img.shields.io/badge/license-MIT-blue)]()

TaskFloww lets a developer **write a Python function, map it to a task name in a YAML file, and
submit work** — the engine handles scheduling (immediate / delayed / cron), at-least-once
execution, retries with exponential backoff, heartbeat-based crash recovery, a Dead Letter
Queue, and Prometheus/JSON observability — **without editing the core engine**.

---

## Status

🚧 **Phase 3a — submission API.** The orchestrator now serves a REST API (chi) to submit
(immediate / delayed / recurring), fetch, and cancel tasks — persisted to PostgreSQL behind a
`SchedulerService` interface. Config (Phase 2), schema (Phase 1), and scaffold (Phase 0) are in
place. The SKIP LOCKED dispatcher, consumer, and reaper come next — see [`docs/ROADMAP.md`](docs/ROADMAP.md).

## Architecture at a glance

```
Client ──▶ REST API ─┐         ┌────────── Go Orchestrator (stateless, xN) ──────────┐
                     │         │  API · Dispatcher(SKIP LOCKED) · Outbox relay        │
                     └────────▶│  Result/Heartbeat consumer · Reaper(lease+cron)      │
                               └───────┬───────────────────────────▲──────────────────┘
                          publish ready│ work            result/heartbeat│ messages
                          ┌────────────▼─────────┐        ┌─────────────┴──────────┐
       source of truth ──▶│      PostgreSQL      │        │        RabbitMQ         │
                          │ tasks/executions/... │        │ priority queues + DLX  │
                          └──────────────────────┘        └────────────┬───────────┘
                                                              consume   │
                                                        ┌───────────────▼──────────┐
                                                        │  Python workers (xN)     │
                                                        │  registry + run loop     │
                                                        └──────────────────────────┘
```

Full design, data flow, and failure scenarios: [`docs/PLAN.md`](docs/PLAN.md).
Locked architecture decisions (ADRs): [`docs/DECISIONS.md`](docs/DECISIONS.md).

## Repository layout

```
taskfloww/
├── orchestrator/     # Go — API, dispatcher, consumer, reaper, outbox (stateless)
│   └── cmd/orchestrator/   # main entrypoint
├── worker/           # Python — thin plug-and-play worker SDK + example tasks
│   └── taskfloww_worker/
├── migrations/       # SQL schema migrations — goose (Phase 1)
├── config/           # plug-and-play example config (config.example.yaml)
├── deploy/           # docker-compose (Postgres + RabbitMQ + Prometheus), prometheus/
└── docs/             # PLAN, DECISIONS, ROADMAP, RESUME, SCHEMA
```

## Quickstart (local infra)

> Requires Docker. Starts PostgreSQL, RabbitMQ (with management UI), and Prometheus.

```bash
cd deploy
cp .env.example .env          # tweak credentials/ports if you like
docker compose up -d          # or: make -C .. up
```

| Service | URL / port | Notes |
|---|---|---|
| PostgreSQL | `localhost:5432` | user/pass/db from `.env` |
| RabbitMQ | `localhost:5672` | AMQP |
| RabbitMQ UI | http://localhost:15672 | management console |
| Prometheus | http://localhost:9090 | metrics (targets wired in later phases) |

Tear down with `docker compose down` (add `-v` to also drop data volumes).

## Database migrations

Once Postgres is up, apply the schema with [goose](https://github.com/pressly/goose):

```bash
go install github.com/pressly/goose/v3/cmd/goose@latest   # one-time
export DATABASE_URI="postgres://taskfloww:taskfloww@localhost:5432/taskfloww?sslmode=disable"
make migrate-up          # apply · make migrate-status · make migrate-down
```

Schema design, ERD, and indexing strategy: [`docs/SCHEMA.md`](docs/SCHEMA.md).
Migration authoring guide: [`migrations/README.md`](migrations/README.md).

## Configuration (plug-and-play)

One YAML file drives **both** the orchestrator and the workers — add a task by writing a function
and mapping it here; no engine code changes. See [`config/config.example.yaml`](config/config.example.yaml)
and the full reference in [`docs/CONFIG.md`](docs/CONFIG.md).

- **Interpolation:** `${VAR:-default}` pulls values from the environment.
- **Overrides:** `TASKFLOWW_<SECTION>__<KEY>` (e.g. `TASKFLOWW_BROKER__PREFETCH=64`).
- **Fail-fast:** invalid config aborts startup with a list of every problem.

```bash
# point either component at a config file (default: config/config.example.yaml)
./orchestrator -config config/config.example.yaml           # Go
python -m taskfloww_worker -c config/config.example.yaml    # Python
```

## REST API

Once Postgres is up and migrated, the orchestrator serves a task API (full reference:
[`docs/API.md`](docs/API.md)):

```bash
# submit an immediate task
curl -X POST localhost:8080/v1/tasks -H 'Content-Type: application/json' \
  -d '{"task_name":"send_email","payload":{"to":"a@b.com"},"priority":5}'
# fetch / cancel
curl localhost:8080/v1/tasks/<uuid>
curl -X POST localhost:8080/v1/tasks/<uuid>/cancel
```

Supports `immediate` / `delayed` (`delay_seconds`|`run_at`) / `recurring` (`cron`); submissions are
idempotent on `id`; unknown task names are rejected (only configured tasks are accepted).

## Building the components

```bash
# Orchestrator (Go) — requires Postgres (make up && make migrate-up)
cd orchestrator && go build ./... && ./orchestrator -config ../config/config.example.yaml

# Worker (Python)
cd worker && python -m venv .venv && source .venv/bin/activate
pip install -e '.[dev]' && python -m taskfloww_worker -c ../config/config.example.yaml
```

## Roadmap

Phased plan with dependencies and milestones: [`docs/ROADMAP.md`](docs/ROADMAP.md).
Returning to the project? Start with [`docs/RESUME.md`](docs/RESUME.md).

## License

[MIT](LICENSE) © 2026 Tejashwadeep Jha (Tejas-67)
