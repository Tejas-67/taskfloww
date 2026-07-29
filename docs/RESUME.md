# Resume Here 👋

Single source of truth for **where we are** and **what to do next**. Read this first when
returning to the project.

_Last updated: 2026-07-29 (planning session)._

---

## TL;DR

We finished **discovery + planning**. **All four core architecture decisions are locked**
(A, B, C, D). **No application code written yet.** Next action = execute **Phase 0 (bootstrap)**.

---

## Current status

- ✅ Environment verified: right machine (`/Users/tejashwadeep.jha/personal`), personal GitHub SSH
  key present (`~/.ssh/id_ed25519_github2` → `Tejas-67`).
- ⚠️ **Git identity risk:** global git = **work** identity (`tejashwadeep.jha@phonepe.com` + work
  signing key). New repo **must** override locally to `Tejas-67` / `tejasjha54@gmail.com` and
  disable the signing key (done for `ExpenseTracker` already). Handled in Phase 0.
- ✅ Plan persisted here in `taskfloww/docs/`.
- ✅ Decisions **A, B, C, D** all locked (see DECISIONS.md).
- ⛔ No code yet — Phase 0 is next.

## Locked decisions
- **A — Hybrid scheduling:** Postgres source of truth (SKIP LOCKED poller + lease reaper + cron +
  retry) · RabbitMQ transport + native DLX for DLQ · transactional outbox.
- **B — Worker↔DB (B1):** workers talk only to RabbitMQ; orchestrator owns all Postgres writes.
- **C — API surface:** REST-first via `chi` behind a `SchedulerService` interface; gRPC later.
- **D — Repo:** monorepo **`taskfloww`** (double-w, confirmed) under GitHub `Tejas-67`.

## ⬅️ Next step
Start **Phase 0 — Bootstrap** (all decisions resolved). No blocking questions remain.

---

## The immediate next step

> Execute **Phase 0 — Bootstrap** (see ROADMAP.md):
> monorepo scaffold → local git identity override → `.gitignore`/`README`/`LICENSE` →
> create `Tejas-67/taskfloww` on GitHub → first push over SSH → `docker-compose` (Postgres +
> RabbitMQ + Prometheus).

Note: `gh` CLI is **not installed**. To create the GitHub repo either
`brew install gh`, or create it in the browser and we push over the existing SSH key.

---

## Where everything lives

| Artifact | Location |
|---|---|
| Master plan (architecture, data flow, failure modes) | `docs/PLAN.md` |
| Decision log (ADRs) | `docs/DECISIONS.md` |
| Phased roadmap + dependency graph | `docs/ROADMAP.md` |
| This resume file | `docs/RESUME.md` |
| Live task tracking | session DB `todos` / `todo_deps` |
| Decision record (machine-readable) | session DB `decisions` |

## How to resume in a new session
1. Open `docs/RESUME.md` (this file), then `docs/ROADMAP.md`.
2. Re-hydrate tracking if needed (the session DB may not carry over):
   the phase list in ROADMAP.md is the canonical backlog.
3. Answer the two open decisions, then start the first `[ ]` phase (Phase 0).

## Progress log
- **2026-07-29** — Discovery + environment check; locked ADR-0001 (hybrid), ADR-0002 (B1),
  ADR-0003 (REST-first), ADR-0004 (monorepo `taskfloww`); authored PLAN/DECISIONS/ROADMAP/RESUME;
  created `taskfloww/` folder. All decisions locked; Phase 0 next. No code yet.
