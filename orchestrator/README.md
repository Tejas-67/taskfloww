# orchestrator (Go)

Stateless orchestrator for TaskFloww. All state lives in PostgreSQL and RabbitMQ, so any number
of instances can run behind a load balancer.

**Phase 0 scaffold** — currently just a health server with structured JSON logging and graceful
shutdown. Components added in later phases (see [`../docs/ROADMAP.md`](../docs/ROADMAP.md)):

- **API** (`chi`) — submit / cancel / query tasks; writes task + outbox row in one tx.
- **Dispatcher** — `SELECT … FOR UPDATE SKIP LOCKED` due-task poller.
- **Outbox relay** — atomic state-commit ↔ RabbitMQ publish.
- **Result/Heartbeat consumer** — the single writer of terminal task state; renews leases.
- **Reaper** — lease-expiry re-queue (retry/backoff → DLQ), worker liveness, cron `next_run_at`.

## Run

```bash
go build ./...
TASKFLOWW_HTTP_ADDR=":8080" ./orchestrator
curl -s localhost:8080/healthz    # {"status":"ok"}
```

Module path: `github.com/Tejas-67/taskfloww/orchestrator`.
