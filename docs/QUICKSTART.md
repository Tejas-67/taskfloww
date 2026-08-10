# TaskFloww — Quickstart

**Goal: from clone to a completed task in under 10 minutes — without editing the engine.**

You'll start the infra, run the orchestrator + a worker, submit a task, then **add your own task**
(a Python function + one YAML entry). No engine code changes required.

---

## Prerequisites

- **Go** ≥ 1.25 (orchestrator) and **Python** ≥ 3.11 (worker)
- **PostgreSQL** and **RabbitMQ** — either via Docker (Option A) or already installed (Option B)
- [`goose`](https://github.com/pressly/goose) for migrations:
  `go install github.com/pressly/goose/v3/cmd/goose@latest`

---

## 1. Start the infra

### Option A — Docker (recommended)

```bash
cd deploy
cp .env.example .env            # tweak credentials/ports if you like
docker compose up -d            # Postgres + RabbitMQ (+ UI) + Prometheus + Grafana
cd ..
export DATABASE_URI="postgres://taskfloww:taskfloww@localhost:5432/taskfloww?sslmode=disable"
export BROKER_URI="amqp://taskfloww:taskfloww@localhost:5672/"
```

### Option B — Local (no Docker)

Point at any local Postgres + RabbitMQ. Create the database once, then export the two URIs the
config interpolates:

```bash
createdb taskfloww     # or: psql -c 'CREATE DATABASE taskfloww;'
export DATABASE_URI="postgres://<user>:<pass>@localhost:5432/taskfloww?sslmode=disable"
export BROKER_URI="amqp://guest:guest@localhost:5672/"
```

> The example config reads `${DATABASE_URI}` and `${BROKER_URI}` — exporting them is all the wiring
> you need.

---

## 2. Apply the database schema

```bash
goose -dir ./migrations postgres "$DATABASE_URI" up
# or: make migrate-up
```

---

## 3. Run the orchestrator

```bash
cd orchestrator
go run ./cmd/orchestrator -config ../config/config.example.yaml
# serves the API on :8080, plus dispatcher + outbox relay + result/heartbeat consumer + reaper
```

Check it's alive (in another shell):

```bash
curl -s localhost:8080/healthz     # {"status":"ok"}
```

---

## 4. Run a worker

Run it **from the `worker/` directory** so the bundled `examples.*` handlers are importable.

```bash
cd worker
python -m venv .venv && source .venv/bin/activate
pip install -e '.[dev]'
python -m taskfloww_worker -c ../config/config.example.yaml
```

You should see: `worker <id> started; tasks=[...] queues=[...]`.

---

## 5. Submit a task and watch it complete

```bash
# the bundled `send_email` handler just echoes a result
curl -s -X POST localhost:8080/v1/tasks -H 'Content-Type: application/json' \
  -d '{"task_name":"send_email","payload":{"to":"a@b.com"}}' | jq .

# copy the returned task.id, then poll it:
curl -s localhost:8080/v1/tasks/<id> | jq '.state'
# queued → dispatching → running → completed
```

That's the full loop. 🎉

---

## 6. Add your OWN task (the plug-and-play payoff)

Three steps, **zero engine changes**:

**1) Write a plain function** (payload in, JSON-serializable value out). Add it to
`worker/examples/tasks.py` — or your own importable module:

```python
# worker/examples/tasks.py
def resize_image(payload: dict) -> dict:
    width = payload["width"]
    # ... your logic ...
    return {"resized_to": width}
```

**2) Map it in the config** under `tasks:` (`handler` is an importable `module:function` path):

```yaml
# config/config.example.yaml
tasks:
  - name: resize_image
    handler: examples.tasks:resize_image
    queue: tasks.default
```

**3) Restart the worker** and submit it:

```bash
curl -s -X POST localhost:8080/v1/tasks -H 'Content-Type: application/json' \
  -d '{"task_name":"resize_image","payload":{"width":800}}' | jq .
```

Delayed and recurring variants use the same endpoint:

```bash
# run in 5 minutes
-d '{"task_name":"resize_image","execution_type":"delayed","delay_seconds":300}'
# every 5 minutes (cron)
-d '{"task_name":"resize_image","execution_type":"recurring","cron":"*/5 * * * *"}'
```

Full API: [`docs/API.md`](API.md). Full config reference: [`docs/CONFIG.md`](CONFIG.md).

---

## 7. See it recover from a crash (optional, ~90s)

Prove at-least-once fault tolerance. Use the bundled `slow` handler and **tightened timings** so the
reaper acts quickly (heartbeat interval **must** be < lease TTL — see Troubleshooting):

```bash
# run the orchestrator with a short lease + fast reaper
TASKFLOWW_HEARTBEAT__INTERVAL_SECONDS=2 \
TASKFLOWW_HEARTBEAT__LEASE_TTL_SECONDS=8 \
TASKFLOWW_HEARTBEAT__REAPER_INTERVAL_SECONDS=2 \
  go run ./cmd/orchestrator -config ../config/config.example.yaml

# run the worker with a matching heartbeat interval
TASKFLOWW_HEARTBEAT__INTERVAL_SECONDS=2 \
  python -m taskfloww_worker -c ../config/config.example.yaml

# submit a 60-second task, confirm it's `running`, then kill the worker (Ctrl-C won't crash it —
# it drains gracefully; use `kill -9 <pid>` to simulate a real crash), and start a new worker.
curl -s -X POST localhost:8080/v1/tasks -H 'Content-Type: application/json' \
  -d '{"task_name":"slow","payload":{"seconds":60}}' | jq .
```

The orchestrator detects the missed heartbeats (expired lease), re-queues the task, and the new
worker finishes it. The task's execution ledger shows the crashed attempt as
`failed` / "lease expired (worker lost)" followed by a later `succeeded` attempt.

---

## 8. Observe

Both sides expose Prometheus metrics and structured JSON logs:

```bash
curl -s localhost:8080/metrics | grep taskfloww_orchestrator_   # orchestrator
curl -s localhost:9100/metrics | grep taskfloww_worker_         # worker
```

With Option A, **Prometheus** (http://localhost:9090) scrapes both and **Grafana**
(http://localhost:3000, admin/admin) ships a provisioned **TaskFloww Overview** dashboard.

---

## Troubleshooting

| Symptom | Cause & fix |
|---|---|
| Task submitted but never runs | No worker is consuming its `queue`, or the worker wasn't started from `worker/` (so `examples.*` fails to import). Check the worker's startup log. |
| `unknown task` / `400` on submit | The `task_name` isn't in the config `tasks:` map. Only configured tasks are accepted (that's the safety of plug-and-play). |
| Tasks get re-queued while a worker is clearly alive | **Heartbeat interval ≥ lease TTL.** Leases are renewed on each heartbeat, so `heartbeat.interval_seconds` must be comfortably **less** than `heartbeat.lease_ttl_seconds` (defaults 10s / 60s). Set the interval on **both** the orchestrator and the worker. |
| Worker won't connect to RabbitMQ | Check `BROKER_URI`; prefer `127.0.0.1` over `localhost` to avoid an IPv6-first resolve delay. |
| Orchestrator exits on boot | It fails fast if Postgres is unreachable or migrations haven't been applied — run step 2. |

---

## Where to next

- **Architecture, data flow & failure scenarios:** [`docs/PLAN.md`](PLAN.md)
- **Config reference (every knob):** [`docs/CONFIG.md`](CONFIG.md)
- **REST API reference:** [`docs/API.md`](API.md)
- **Schema & indexing:** [`docs/SCHEMA.md`](SCHEMA.md)
