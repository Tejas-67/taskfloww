# Resume Here 👋

Single source of truth for **where we are** and **what to do next**. Read this first when
returning to the project.

_Last updated: 2026-07-29 (planning session)._

---

## TL;DR

We finished **discovery + planning** and shipped **Phase 0 (bootstrap)**, **Phase 1 (schema &
migrations)**, and **Phase 2 (plug-and-play config)**. All architecture decisions are locked. One
YAML file drives both sides with env interpolation/overrides + fail-fast validation. Next action =
**Phase 3a (submission API)** or **Phase 3b (dispatcher)**.

> ⚠️ Phase 2 is implemented and validated but **not yet committed** — the user commits manually.

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
- ✅ **Phase 2 implemented (uncommitted)** — plug-and-play config: Go loader `koanf`
  (`orchestrator/internal/config`) + Python loader `pydantic` (`worker/taskfloww_worker/config.py`),
  both with `${VAR:-default}` interpolation, `TASKFLOWW_*__*` env overrides, and fail-fast
  validation (heartbeat lease invariant, handler shape, queue refs, etc.). Wired into both mains
  (config-driven logging + redacted startup summary). `docs/CONFIG.md` reference. Tests: Go
  `internal/config` + Python `worker/tests` all green; smoke-tested both binaries on the example.

## Locked decisions
- **A — Hybrid scheduling:** Postgres source of truth (SKIP LOCKED poller + lease reaper + cron +
  retry) · RabbitMQ transport + native DLX for DLQ · transactional outbox.
- **B — Worker↔DB (B1):** workers talk only to RabbitMQ; orchestrator owns all Postgres writes.
- **C — API surface:** REST-first via `chi` behind a `SchedulerService` interface; gRPC later.
- **D — Repo:** monorepo **`taskfloww`** (double-w, confirmed) under GitHub `Tejas-67`.
- **E — Languages:** Go orchestrator + Python workers (Java considered, rejected — see ADR/decisions).

## ⬅️ Next step
**Phase 2 is done** (pending your manual commit). Next: **Phase 3a — submission API** (REST via
`chi` behind a `SchedulerService` interface; validate + insert task in one tx) or **Phase 3b —
dispatcher** (SKIP LOCKED due-scan + outbox relay).

---

## The immediate next step

> Execute **Phase 3a — submission API** or **Phase 3b — dispatcher** (see ROADMAP.md).
> Phase 3a: REST via `chi` behind a `SchedulerService` interface; validate + insert task (+ compute
> next_run_at for immediate/delayed) in one tx. Phase 3b: SKIP LOCKED due-scan + outbox relay.

**Tooling installed this session:** Go 1.26.5, `goose` (`~/go/bin`), PostgreSQL 16
(`/opt/homebrew/opt/postgresql@16`, keg-only), Python venv at `worker/.venv` (pydantic + pyyaml +
pytest). **Docker is still not installed** — install Docker Desktop (or `colima`) to run `make up`.

### Config loader notes (Phase 2)
- Go loader lives in `orchestrator/internal/config` (koanf); Python in
  `worker/taskfloww_worker/config.py` (pydantic BaseModel + explicit env overlay — chosen over
  `pydantic-settings` because the primary source is a YAML file with `${}` interpolation).
- Precedence: defaults → YAML file → `TASKFLOWW_*__*` env. `${VAR:-default}` interpolation on both.
- Both fail-fast and aggregate every problem. Passwords are redacted in startup logs.

---

## Where everything lives

| Artifact | Location |
|---|---|
| Master plan (architecture, data flow, failure modes) | `docs/PLAN.md` |
| Decision log (ADRs) | `docs/DECISIONS.md` |
| Phased roadmap + dependency graph | `docs/ROADMAP.md` |
| DB schema, ERD, indexing rationale | `docs/SCHEMA.md` |
| Config reference (plug-and-play) | `docs/CONFIG.md` |
| This resume file | `docs/RESUME.md` |
| Live task tracking | session DB `todos` / `todo_deps` |
| Decision record (machine-readable) | session DB `decisions` |

## How to resume in a new session
1. Open `docs/RESUME.md` (this file), then `docs/ROADMAP.md`.
2. Re-hydrate tracking if needed (the session DB may not carry over):
   the phase list in ROADMAP.md is the canonical backlog.
3. Pull the repo (`git@github.com:Tejas-67/taskfloww.git`) and start the first `[ ]` phase (Phase 3a).

## Progress log
- **2026-07-31 (Phase 2)** — Plug-and-play config on both sides. Go: `internal/config` (koanf) with
  `${VAR:-default}` interpolation, `TASKFLOWW_*__*` env overrides, aggregated fail-fast validation,
  `RedactURI`, + tests; wired into `cmd/orchestrator`. Python: `taskfloww_worker/config.py`
  (pydantic) mirroring the schema + `worker/tests` (pytest, 8 passing); wired into `__main__`.
  Formalized `config/config.example.yaml`, added `docs/CONFIG.md`, Makefile `test-worker` +
  fixed `run-worker` path. Both binaries smoke-tested on the example; bad configs fail fast.
  **Not committed** (user commits manually).
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
