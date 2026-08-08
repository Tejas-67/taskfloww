# REST API (Phase 3a)

The orchestrator exposes a REST API (chi) for task submission and lifecycle, behind the
transport-agnostic `SchedulerService` interface (a gRPC layer can wrap the same service later —
ADR-0003). Base URL defaults to `http://localhost:8080` (`server.http_addr`).

All responses are JSON. Errors use `{"error":{"code","message"}}`.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/healthz` | Liveness — `{"status":"ok"}` |
| `POST` | `/v1/tasks` | Submit an immediate / delayed / recurring task |
| `GET` | `/v1/tasks/{id}` | Fetch a task by UUID |
| `POST` | `/v1/tasks/{id}/cancel` | Cancel a `queued`/`retrying` task |
| `GET` | `/v1/dead-letters` | List dead-lettered tasks (`?limit=&offset=&include_replayed=true`) |
| `GET` | `/v1/dead-letters/{id}` | Fetch one dead letter |
| `POST` | `/v1/dead-letters/{id}/replay` | Re-queue a dead task for a fresh run |

## Submit a task — `POST /v1/tasks`

| Field | Type | Notes |
|---|---|---|
| `id` | string | Client "unique id" → `idempotency_key`. **Optional**; generated if omitted. Resubmitting the same `id` returns the existing task (idempotent, HTTP 200). |
| `task_name` | string | **Required.** Must exist in the config `tasks` map (plug-and-play). |
| `payload` | object | Arbitrary JSON. Default `{}`. |
| `priority` | int | 0–255, higher = more urgent. Default 0. |
| `max_retries` | int | Overrides the config default for this task. |
| `execution_type` | string | `immediate` (default) · `delayed` · `recurring`. |
| `run_at` | RFC3339 | *(delayed)* absolute time; must be future. |
| `delay_seconds` | int | *(delayed)* relative delay; alternative to `run_at`. |
| `cron` | string | *(recurring)* 5-field cron; **required** for recurring. |
| `timezone` | string | *(recurring)* IANA tz; default `UTC`. |
| `schedule_name` | string | *(recurring)* unique name; default `<task_name>-<uuid>`. |

**Responses:** `201 Created` (new), `200 OK` (idempotent replay), `400` (validation),
`409` (duplicate schedule name). Body is an envelope:

```json
{ "kind": "task", "task": { "id": "…", "state": "queued", "next_run_at": "…", … } }
```
Recurring submissions create a **schedule** (a cron definition; each firing later materializes a
task run — Phase 3d):
```json
{ "kind": "schedule", "schedule": { "id": "…", "cron_expr": "*/5 * * * *", "next_fire_at": "…", "enabled": true } }
```

### Examples

```bash
# immediate
curl -X POST localhost:8080/v1/tasks -H 'Content-Type: application/json' \
  -d '{"id":"welcome-42","task_name":"send_email","payload":{"to":"a@b.com"},"priority":5}'

# delayed 5 minutes
curl -X POST localhost:8080/v1/tasks -H 'Content-Type: application/json' \
  -d '{"task_name":"send_email","execution_type":"delayed","delay_seconds":300}'

# recurring every 5 minutes
curl -X POST localhost:8080/v1/tasks -H 'Content-Type: application/json' \
  -d '{"task_name":"generate_report","execution_type":"recurring","cron":"*/5 * * * *"}'
```

## Get / Cancel

```bash
curl localhost:8080/v1/tasks/<uuid>                 # 200 task | 404 not found | 400 bad uuid
curl -X POST localhost:8080/v1/tasks/<uuid>/cancel  # 200 cancelled | 409 not cancellable | 404
```

`cancel` only succeeds while the task is `queued` or `retrying`; a `running`/terminal task
returns `409` (a running task can't be pulled back from a worker in this phase).

## Dead Letter Queue (DLQ)

A task that exhausts its retries transitions to `dead` and gets a `dead_letters` row (populated by
the result consumer and the reaper). The DLQ is **queryable and replayable**:

```bash
# list un-replayed dead letters (newest first)
curl 'localhost:8080/v1/dead-letters?limit=50'
# inspect one
curl localhost:8080/v1/dead-letters/<uuid>
# replay: reset the task to queued (fresh attempt budget) and re-dispatch
curl -X POST localhost:8080/v1/dead-letters/<uuid>/replay
```

- **Replay** resets the task to `queued` with `attempt=0`, clears the prior run's execution ledger
  (so it re-dispatches cleanly), and stamps the dead letter `replayed_at`. A second replay returns
  `409`.
- Two DLQ layers exist: the **`dead_letters` table** (the authoritative, queryable/replayable DLQ,
  since Postgres is the source of truth) and RabbitMQ's native **DLX → `tasks.dlq`** (a broker-level
  safety net for nacked/TTL-expired messages).

## Status codes

| Code | Meaning |
|---|---|
| 200 | OK (get, cancel, idempotent resubmit) |
| 201 | Created (new task/schedule) |
| 400 | Validation error (`invalid_request`) — unknown task, bad cron, past `run_at`, bad UUID, malformed JSON |
| 404 | Task not found |
| 409 | Conflict — not cancellable (`conflict`) or duplicate schedule name (`duplicate`) |
| 500 | Internal error |

## Notes

- **Idempotency:** submission dedups on `id` (`idempotency_key` UNIQUE). At-least-once *execution*
  idempotency (duplicate deliveries) is handled later via the `task_executions` ledger.
- The API only **persists** tasks (state `queued`). Dispatching to RabbitMQ is Phase 3b; execution
  is Phase 4.
- Requires PostgreSQL (`make up` + `make migrate-up`); the orchestrator fails fast if the DB is
  unreachable.
