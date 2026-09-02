## Working agreements

Search symbols first. Never read main_window.py or cloud_sync.py wholesale unless there is a demonstrated need.

- Do not run `npm test` after modifying JavaScript files.
- Ask for confirmation before adding new production dependencies.
- Do not run heavy build steps, including Capacitor syncs, PyInstaller, Docker builds, full app builds, packaging commands, or dependency installation, unless explicitly requested.
- Keep patches narrow. If a task touches multiple workflows or large UI files, propose staged patches and stop after the current stage.
- Do not rewrite or refactor unrelated code while fixing a bug. Preserve existing behavior unless the prompt explicitly asks for a behavior change.
- Do not commit or push unless the user explicitly asks. Never rewrite published history or force-push without explicit authorization.
- For sporely-py, always use the project virtual environment:
  `/Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/python` and
  `/Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/pytest`.
  Ask for confirmation before installing or upgrading packages in .venv.
  For syntax checks, use `./.venv/bin/python -m py_compile <touched files>`.

## Subsystem rules (read before touching these areas)

Detailed invariants live in `.claude/rules/` (Claude loads them automatically for matching paths; other agents must read them explicitly):

- **Cloud sync** (`utils/cloud_sync*`, sync tests, sync contracts): read `.claude/rules/cloud-sync.md` before any sync, cursor, media-flag, or cloud-write change. These invariants encode real incidents; do not improvise.
- **Localization** (`tr(...)` strings, `i18n/`): read `.claude/rules/localization.md`.
- **UI screenshot evidence** (`tools/review_ui/`): read `.claude/rules/ui-screenshots.md`.

## Agent routing and token budget

Use one top-level agent for one architectural slice. Subagents are optional and should be used only when they save context or provide genuinely independent review. Do not automatically chain planner -> implementer -> reviewer.

### Codex roles

- `act` — **Luna, low effort**. Use for small, direct, low-risk work that is already clear: targeted symbol lookup, documentation, narrow mechanical edits, focused test repairs, or one-file/tightly bounded changes. If the task reveals a contract or subsystem question, stop and hand off rather than expanding.
- `explore-review` — **Luna, medium effort, read-only**. Use as the cheap scout: locate symbols, map a call/data path, triage a diff, or perform a first-pass low-risk review. Return a compact symbol map or concrete findings; do not independently rediscover the whole subsystem.
- `planner` — **Terra, medium effort, read-only**. Use before ambiguous, cross-repository, persistence/schema, sync, or architecture-changing work. Produce small independently verifiable stages. Do not use for an already-clear local patch.
- `implementer` — **Terra, low effort**. Use for an approved bounded plan stage that needs nontrivial edits and focused tests. Stop at the stage boundary.
- `reviewer` — **Terra, medium effort, read-only**. Use once at a meaningful stage/landing boundary or when explicitly requested. Do not spawn it after every small patch.
- `security_reviewer` — **Terra, high effort, read-only**. Use only when a change materially touches auth/session handling, RLS/authorization, SECURITY DEFINER/public RPCs, storage access, secrets/service-role use, account binding, privacy/visibility, moderation/blocking, deletion, or another authoritative security boundary.
- **Sol is escalation-only**, not the default. Use it only when the user explicitly requests it, Terra reports unresolved high-risk ambiguity, or a release/production gate has unusual architectural or security risk.

Claude role agents under `.claude/agents/` follow the same boundaries with Claude models (shared routing/escalation policy: `../CLAUDE.md`). Do not invoke a role agent merely because it exists; delegate only when the role boundary above is met.

### Context discipline

- The active plan is durable project memory; the current agent context is disposable working memory. At an architectural/subsystem boundary, update the plan/handoff and prefer a fresh agent. Keep the same agent only for tightly related follow-up work where its recent context is directly useful.
- Read the active plan's **current stage/handoff first**. Do not read completed-stage history unless a concrete compatibility question requires it. Use `docs/technical-overview.md` for orientation instead of rediscovering the repository.
- Search before reading. Use `rg`/symbol search, then inspect bounded ranges around relevant definitions/callers. Never dump a large file to context just to understand it. In particular, do not read `ui/main_window.py`, `utils/cloud_sync.py`, or other multi-thousand-line modules wholesale.
- Start reviews with `git diff --stat`, `git diff --name-only`, and the actual targeted diff. Expand into surrounding code only for touched symbols or a concrete suspected failure mode.
- Do not have multiple agents perform the same repository archaeology. If a scout already returned the relevant symbols/call path, later agents should use that handoff and verify only where necessary.
- When delegating a scout/review subtask, give a narrow question and ask for a compact result (normally <=20 lines plus file/symbol references). Do not ask for broad repository summaries.
- Keep command output small: prefer focused tests and quiet output during iteration. The implementer owns repository-required validation; reviewers should not rerun already-passed broad suites unless a finding requires it.
- Compaction restores context-window room but does not make prior work free. Do not use compaction as a reason to broaden scope or carry a finished subsystem into the next stage.

## Documentation roles

- `README.md` is the human-facing project introduction, installation guide, and documentation index.
- `AGENTS.md` is the repository-wide source of instructions for coding agents; `.claude/rules/` holds path-scoped subsystem invariants.
- `docs/technical-overview.md` is a concise description of the current architecture. It is orientation material, not a requirements document.
- `docs/development/gui-conventions.md` contains the detailed PySide6 layout, sizing, state, threading, and image-handling conventions.
- `docs/plans/INBOX.md` holds rough ideas; `docs/plans/active/` holds scoped unfinished plans; `docs/plans/completed/` preserves finished plans.
- For active work, read the current stage/handoff and referenced invariants first. Historical completed-stage detail is reference-only unless the current task depends on it.
- Detailed contracts, architecture maps, runbooks, audits, and user guides remain beside the subsystem they document.

When documents disagree, current code and tests establish implementation reality, while the most specific applicable contract governs intended behavior. Plans describe intent and Git history records what actually landed.

## Review and implementation conventions

- In reviews, distinguish correctness defects from cleanup or style opinions. Check especially for duplicate logic, competing sources of truth, database consistency, state-flow errors, UI inconsistency, dead code, unclear boundaries, naming, and error handling. Keep review reports factual and concise.
- Use Python 3.10+ and PySide6; do not introduce PyQt.
- Follow `docs/development/gui-conventions.md` for GUI-specific work.
- Use Qt layouts instead of absolute positioning with `setGeometry()` or `move()`. Avoid fixed width/height where stretch policies work. Use `QSplitter` for resizable panes and persist user-adjustable splitter state through `QSettings`.
- Guard programmatic widget updates against signal recursion with loading flags or `blockSignals()`.
- Keep network and other expensive work off the UI thread, normally with `QThread`, and clean up worker threads when their owning UI closes. Use `QTimer.singleShot` when UI initialization genuinely needs to be deferred.
- Respect EXIF orientation with `QImageReader.setAutoTransform(True)` when loading previews.
- Use native type annotations for signatures and class variables. Prefix private UI builders and event slots with one underscore.
- Prefer dataclasses for structured data passed between UI and internal logic; do not broaden a focused patch solely to retrofit existing structures.

## Database and generated artifacts

- Inspect the existing schema and migration path before changing persistence. Local SQLite currently uses the Python helpers documented in `database/sqlite_migrations/README.md`; do not place Supabase/Postgres SQL in that directory.
- Preserve existing data and add focused migration/round-trip coverage for schema changes.
- Do not edit or commit generated databases, caches, local credentials, downloaded source archives, or ad hoc screenshot output. Commit generated artifacts only when the owning documented workflow explicitly requires them (for example, translated `.qm` files alongside their `.ts` sources).

## Plugins

Superpowers skills are optional techniques, not the project workflow. Do not invoke Superpowers brainstorming, writing-plans, executing-plans, subagent-driven-development, or finishing-a-development-branch unless explicitly requested. The repository's docs/plans structure, phase contracts, and project-specific agent/review instructions are authoritative. Superpowers systematic-debugging and verification-before-completion may be used when useful, provided they do not replace or modify the project workflow.
