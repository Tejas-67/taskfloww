# TaskFloww

> A **stateless**, **config-driven** distributed **Task Scheduler & Workflow Engine**.
> Go orchestrator · Python workers · PostgreSQL (source of truth) · RabbitMQ (transport + DLQ).

[![status](https://img.shields.io/badge/status-phase%200%20bootstrap-orange)]()
[![license](https://img.shields.io/badge/license-MIT-blue)]()

TaskFloww lets a developer **write a Python function, map it to a task name in a YAML file, and
submit work** — the engine handles scheduling (immediate / delayed / cron), at-least-once
execution, retries with exponential backoff, heartbeat-based crash recovery, a Dead Letter
Queue, and Prometheus/JSON observability — **without editing the core engine**.

---

## Status

🚧 **Phase 0 — Bootstrap.** Monorepo scaffold, local dev infra, and repo setup only.
The scheduling engine is built in later phases — see [`docs/ROADMAP.md`](docs/ROADMAP.md).

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
├── migrations/       # SQL schema migrations (Phase 1)
├── config/           # plug-and-play example config (config.example.yaml)
├── deploy/           # docker-compose (Postgres + RabbitMQ + Prometheus), prometheus/
└── docs/             # PLAN, DECISIONS, ROADMAP, RESUME
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

## Building the components

```bash
# Orchestrator (Go)
cd orchestrator && go build ./... && ./orchestrator   # serves GET /healthz on :8080

# Worker (Python)
cd worker && python -m venv .venv && source .venv/bin/activate
pip install -e . && python -m taskfloww_worker
```

## Roadmap

Phased plan with dependencies and milestones: [`docs/ROADMAP.md`](docs/ROADMAP.md).
Returning to the project? Start with [`docs/RESUME.md`](docs/RESUME.md).

## License

[MIT](LICENSE) © 2026 Tejashwadeep Jha (Tejas-67)
