# Resume Here 👋

Single source of truth for **where we are** and **what to do next**. Read this first when
returning to the project.

_Last updated: 2026-07-29 (planning session)._

---

## TL;DR

We finished **discovery + planning** and shipped **Phase 0 (bootstrap)** and **Phase 1
(PostgreSQL schema & migrations)**. All architecture decisions are locked. The schema is designed,
migratable via goose, and validated up/down against a real Postgres. Next action = **Phase 2
(plug-and-play config)** or **Phase 3a (submission API)**.

> ⚠️ Phase 1 is implemented and validated but **not yet committed** — the user commits manually.

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

## Locked decisions
- **A — Hybrid scheduling:** Postgres source of truth (SKIP LOCKED poller + lease reaper + cron +
  retry) · RabbitMQ transport + native DLX for DLQ · transactional outbox.
- **B — Worker↔DB (B1):** workers talk only to RabbitMQ; orchestrator owns all Postgres writes.
- **C — API surface:** REST-first via `chi` behind a `SchedulerService` interface; gRPC later.
- **D — Repo:** monorepo **`taskfloww`** (double-w, confirmed) under GitHub `Tejas-67`.
- **E — Languages:** Go orchestrator + Python workers (Java considered, rejected — see ADR/decisions).

## ⬅️ Next step
**Phase 1 is done** (pending your manual commit). Next: **Phase 2 — plug-and-play config**
(YAML loader for Go `koanf` + Python `pydantic-settings`) or **Phase 3a — submission API**.

---

## The immediate next step

> Execute **Phase 2 — plug-and-play config** or **Phase 3a — submission API** (see ROADMAP.md).
> Phase 2: one YAML schema driving Go (`koanf`) + Python (`pydantic-settings`) — broker/DB DSNs,
> queue/routing defs, retry policy, heartbeat/timeout, and the task→function map.

**Tooling installed this session:** Go 1.26.5, `goose` (`~/go/bin`), PostgreSQL 16
(`/opt/homebrew/opt/postgresql@16`, keg-only). **Docker is still not installed** — install Docker
Desktop (or `colima`) to run `make up`; Phase 1 was validated using a throwaway local Postgres
cluster instead.

---

## Where everything lives

| Artifact | Location |
|---|---|
| Master plan (architecture, data flow, failure modes) | `docs/PLAN.md` |
| Decision log (ADRs) | `docs/DECISIONS.md` |
| Phased roadmap + dependency graph | `docs/ROADMAP.md` |
| DB schema, ERD, indexing rationale | `docs/SCHEMA.md` |
| This resume file | `docs/RESUME.md` |
| Live task tracking | session DB `todos` / `todo_deps` |
| Decision record (machine-readable) | session DB `decisions` |

## How to resume in a new session
1. Open `docs/RESUME.md` (this file), then `docs/ROADMAP.md`.
2. Re-hydrate tracking if needed (the session DB may not carry over):
   the phase list in ROADMAP.md is the canonical backlog.
3. Pull the repo (`git@github.com:Tejas-67/taskfloww.git`) and start the first `[ ]` phase (Phase 2).

## Progress log
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
