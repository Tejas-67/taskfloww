# Configuration Reference (Phase 2)

TaskFloww is **config-driven**: a single YAML file configures both the Go orchestrator and the
Python workers, so a developer onboards a task by writing a function and mapping it here — no
engine code changes. Example: [`../config/config.example.yaml`](../config/config.example.yaml).

## How values are resolved

Precedence, low → high:

1. **Built-in defaults** (in code) — every optional field has a sane default.
2. **The YAML file** — overrides defaults.
3. **Environment variables** — override the file.

Plus two conveniences:

- **Interpolation** inside YAML values: `${VAR}` or `${VAR:-default}`. If `VAR` is unset/empty the
  default is used (or empty string if no default). Great for secrets/DSNs:
  `uri: ${DATABASE_URI:-postgres://taskfloww:taskfloww@localhost:5432/taskfloww?sslmode=disable}`
- **Overrides** via `TASKFLOWW_` env vars, nested keys joined by `__`:
  | Env var | Overrides |
  |---|---|
  | `TASKFLOWW_BROKER__PREFETCH=64` | `broker.prefetch` |
  | `TASKFLOWW_DATABASE__URI=...` | `database.uri` |
  | `TASKFLOWW_LOGGING__LEVEL=debug` | `logging.level` |
  | `TASKFLOWW_SERVER__HTTP_ADDR=:9000` | `server.http_addr` |

Both loaders **fail fast**: an invalid file aborts startup and prints *every* problem at once
(e.g. `lease_ttl_seconds must be > interval_seconds`, `handler must be "module:function"`).

## Who reads what

| Section | Orchestrator (Go) | Worker (Python) |
|---|:--:|:--:|
| `app`, `logging`, `metrics` | ✓ | ✓ |
| `broker`, `queues`, `retry`, `heartbeat`, `tasks` | ✓ | ✓ |
| `database` | ✓ | ignored |
| `server`, `scheduler` | ✓ | ignored |

Workers never receive database credentials (ADR-0002 / B1); orchestrator-only sections in the
shared file are simply ignored by the worker loader.

## Sections

### `app` / `logging` / `server` / `metrics`
```yaml
app:      { name: taskfloww, environment: development }   # development|staging|production
logging:  { level: info, format: json }                  # level: debug|info|warn|error · format: json|text
server:   { http_addr: ":8080" }                          # orchestrator HTTP (health, metrics, API)
metrics:  { enabled: true, path: /metrics, worker_port: 9100 }
```

### `database` (orchestrator only)
```yaml
database:
  uri: ${DATABASE_URI:-postgres://...}
  max_open_conns: 20     # > 0
  max_idle_conns: 10     # 0..max_open_conns
  conn_max_idle_seconds: 300
```

### `broker` + `queues`
```yaml
broker: { uri: ${BROKER_URI:-amqp://...}, prefetch: 32, connection_name: taskfloww }
queues:
  default_exchange: taskfloww.direct
  dead_letter_exchange: taskfloww.dlx
  definitions:                       # >= 1 required; names unique
    - { name: tasks.high,    routing_key: priority.high,    max_priority: 10 }
    - { name: tasks.default, routing_key: priority.default, max_priority: 10 }
  dead_letter: { name: tasks.dlq, routing_key: dead }
```

### `retry` (default policy; per-task overridable)
```yaml
retry:
  max_retries: 5
  backoff:
    strategy: exponential   # exponential|fixed
    base_seconds: 2         # > 0
    multiplier: 2.0         # >= 1 for exponential
    max_seconds: 300        # >= base_seconds
    jitter: true            # avoid thundering-herd retries
```

### `heartbeat` (fault-tolerance timings)
```yaml
heartbeat:
  interval_seconds: 10          # worker → orchestrator heartbeat cadence
  lease_ttl_seconds: 60         # MUST be > interval_seconds (else false re-queues)
  reaper_interval_seconds: 15   # MUST be <= lease_ttl_seconds (detect expiry in time)
```
These invariants are validated — they are the crux of at-least-once crash recovery.

### `scheduler` (orchestrator only)
```yaml
scheduler: { dispatch_batch_size: 100, poll_interval_ms: 500 }
```

### `tasks` — the plug-and-play core
Maps a task name to a worker function. `handler` is an importable `module:function` path in the
worker environment. `queue` (optional) must reference a `queues.definitions[].name`;
`max_retries` (optional) overrides `retry.max_retries`.
```yaml
tasks:
  - { name: send_email,     handler: myapp.tasks:send_email,     queue: tasks.default, max_retries: 3 }
  - { name: generate_report, handler: myapp.tasks:generate_report, queue: tasks.high }
```

## Using it in code

```bash
./orchestrator -config config/config.example.yaml        # Go  (or TASKFLOWW_CONFIG=...)
python -m taskfloww_worker -c config/config.example.yaml  # Python (or TASKFLOWW_CONFIG=...)
```

- Go: `config.Load(path) (*config.Config, error)` — see `orchestrator/internal/config`.
- Python: `taskfloww_worker.config.load_config(path) -> Config` — pydantic-validated.
  `Config.handler_map()` returns `name → handler` for building the worker registry.
