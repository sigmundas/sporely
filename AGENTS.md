## Working agreements

These instructions apply to this Git worktree and override conflicting shared
Sporely workflow defaults (including the parent's and the user's global Claude
instructions). Explicit user instructions take precedence over repository
defaults. A generated stage prompt does not by itself authorize overriding a
repository Git restriction.

- Preserve unrelated edits already in the working tree.
- Ask for confirmation before adding new production dependencies, and before
  installing or upgrading packages in `.venv`.
- Do not run heavy build steps, including Capacitor syncs, PyInstaller, Docker builds, full app builds, packaging commands, or dependency installation, unless explicitly requested.
- Keep patches narrow. Do not refactor unrelated code while fixing a bug, and
  preserve existing behavior unless the task asks for a change. If a task spans
  several workflows or large UI files, propose stages and stop after the current one.
- Prefer extending an existing widget, dialog, state structure, or code path over adding a parallel one. If the task assumes a reuse target and you cannot find it, stop and report rather than writing a second implementation.
- Always use the project virtual environment, run from this worktree root:
  `/Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/python` and
  `/Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/pytest`.
  For syntax checks, use the same Python with `-m py_compile <touched files>`.

## Workflow

- The top-level session implements. Use subagents only for parallel read-only
  scouting, or for an independent review when Codex is unavailable. Never
  delegate concurrent implementation writes.
- At each stopping point (the end of a stage or step, before reporting), run
  a Codex review (`/codex:review`; `/codex:challenge` for risky
  changes such as migrations, deletion or sync identity). Verify each finding
  against the code, then fix it or state why it does not apply.
- Always ask the user before: product or UX direction choices, SQLite
  schema/persistence changes, sync-contract changes, security-sensitive changes
  (auth, RLS, storage privacy, deletion, moderation, secrets), production
  writes, and releases. Other implementation decisions are the agent's call;
  name the notable ones in the report.

## Verification and commits

Commit only work whose verification has actually passed. If verification fails
partway, do not commit a partial stage — the failure is the report. When a task
defines numbered stages, commit each verified stage as its own commit and report
the hash. Push completed work; see **Git policy** below.

Verification has three tiers:

1. **Agent-verified — commit without waiting for the user.**
   - Unit and integration tests, syntax checks.
   - Interactive UI behavior, proven by tests that drive the real production
     widgets (pytest-qt `qtbot` is available): signal loops, focus changes,
     scroll retention across rebuilds, resize and splitter behavior, and state
     surviving a restart (persist, destroy, recreate from settings or the database).
   - macOS-specific focus, popup and keyboard behavior: additionally a cocoa
     real-input run, as described in `.claude/rules/ui-screenshots.md`.
   - Layout, proven by renderer screenshots that the agent has opened and
     inspected, as described in `.claude/rules/ui-screenshots.md`.

   Local test scope: run only the stage's tests and the tests for the modules
   it touches. CI runs the full suite on every push. When a change touches
   shared fixtures or harnesses that many tests import (such as
   `tests/conftest.py` or `tools/review_ui/live_lab_harness.py`), run the full
   suite locally as well.

   A screenshot proves layout, never behavior. A claim about what happens when
   the user interacts needs a test or a real-input run that exercises it.
2. **The user's layout opinion — does not block commits.** For visible UI
   changes, include before/after screenshots in the report and ask whether the
   layout is what the user wants. Requested changes are new commits.
3. **Human-gated — do not commit until the user confirms.** Camera/microscope
   hardware, live Supabase writes, RLS behavior, cross-client sync with real
   accounts, performance on real data, and judgment about whether output reads
   correctly to a mycologist. Leave the work uncommitted and report a numbered
   checklist of exactly what the user must do. The commit happens after the user
   confirms, in the next task.
   - **Exception — engine-gated verification in an Agent Sparring plan run.** When an approved Agent Sparring plan holds the human check in an engine gate (`gates_before` / `completion_gates`, such as a live-canary gate), the stage may be committed, pushed and accepted on its automated evidence. The gate is then the human check, not an uncommitted working tree: it holds the next stage (or plan completion) until the user records a pass, and a failed check is fixed in a new stage or reverted. Agents may execute a live Supabase write only after explicit approval under **Production access** below; that approval does not satisfy a human verification gate.

Agents must not claim to have performed a human check themselves. Automated
real-input runs are agent checks. Before stopping, record progress in the
destination given under **Context discipline**: verification, commit or
manual-test status, and deferred work.

## Production access — read-only by default

- Agents may inspect production databases, schema/SQL definitions, logs, and
  files/storage objects without separate approval. Use read-only credentials,
  endpoints, and transactions where available; verify the production target.
- Production writes require asking the user first and receiving explicit approval
  for the specific operation in the current conversation. Present the target,
  exact SQL/command or file changes, expected effects, and relevant validation
  before asking. Task assignment, a plan, or tool access alone is not approval.
- After approval, agents may execute that operation through the approved
  repository/runbook path, with all prechecks and deployment guards intact.
  Approval persists for that operation; ask again if its target or scope changes.
- Writes include data/schema/permission changes, migrations, mutating RPCs,
  imports, uploads, overwrites, moves/deletions, deployments, and scripts/tests
  or app sync that change production state. A SELECT that calls a mutating
  function is a write. If side effects are uncertain, treat the operation as a
  write and ask first.
- Do not bypass a tool approval prompt, weaken access controls, or expose secrets
  or private production data in logs/reports. Local development edits and isolated
  local/test databases are outside this production approval rule.

## Git policy

Commit each completed step and push it to the current branch. Do not open pull
requests unless the user asks, or unless the work spans several worktrees. Never
merge into or push to `main`; the user does that. Agents may create branches and
merge `main` into the working branch when `main` has changed. Keep history
understandable.

Do not:
- force-push unless the user explicitly asks for it;
- rewrite published history unnecessarily;
- push secrets or credentials;
- deploy, publish a release, or modify production systems without explicit user approval for the specific operation under **Production access** above.

These rules do not weaken **Verification and commits** above.

## Subsystem rules (read before touching these areas)

Read only the rules for the area the task touches:

- **Cloud sync** (`utils/cloud_sync*`, sync tests/contracts, cursors, media flags,
  cloud writes): `.claude/rules/cloud-sync.md`; the tracked contract is
  `docs/supabase-sync-contract.md`, with navigation in `docs/cloud-sync-architecture.md`.
- **Localization** (`tr(...)` strings, `i18n/`): `.claude/rules/localization.md`.
- **UI verification / screenshot tooling**: `.claude/rules/ui-screenshots.md`;
  the renderer entry point is `tools/render_review_screenshots.py`.
- **GUI behavior/layout**: relevant sections of `docs/development/gui-conventions.md`.
- **SQLite persistence**: `database/sqlite_migrations/README.md` before schema edits.

The three `.claude/rules/` files are version-controlled project invariants.
Claude can load matching path rules automatically; other agents must read the
applicable rule explicitly. Do not load unrelated subsystem rules.

## Context discipline

- Where progress goes depends on the workflow of the current task, not merely
  the presence of `.sparring/`:
  - **Agent Sparring managed run:** the input plan is immutable. Record progress
    in the current stage's `notes.md`. The engine owns generated handoffs,
    verdicts, state, session lifecycle and stage advancement; do not edit those.
    Managed acceptance requires a pushed candidate.
  - **Other staged implementation:** update the active plan's current
    stage/handoff record before stopping. Use the task's plan reference; do not
    pick a plan by guessing from a directory listing.
  - **Ordinary bounded task:** no plan or staged report is required unless the
    task requests one.
- Never read `ui/main_window.py`, `ui/observations_tab.py`, `utils/cloud_sync.py`,
  or other multi-thousand-line modules wholesale; search, then read bounded ranges.
- Scope searches to this worktree. Never search the parent `sporely/` directory
  or substitute sibling worktrees: they can contain plausible stale code.
- Start change reviews from status and diff stat for the actual base and
  candidate, including staged and untracked work.
- Do not load completed-stage history unless a concrete compatibility question
  requires it. Use relevant headings of `docs/technical-overview.md` for
  orientation, not as requirements.

## Document authority

`AGENTS.md` owns agent workflow; subsystem rules/contracts own specific intended
behavior. Code/tests establish implementation reality, not permission to ignore
a contract. Report disagreements. `README.md` is the human-facing index and
`docs/technical-overview.md` is orientation, not requirements. Plans under
`docs/plans/active/` describe unfinished work; `INBOX.md` and completed plans are
not current task authorization. Verification tiers recorded in older plan entries
reflect the rules in force when they were written.

## Review and implementation conventions

- In reviews, distinguish correctness defects from cleanup or style opinions. Check especially for duplicate logic, competing sources of truth, database consistency, state-flow errors, UI inconsistency, dead code, unclear boundaries, naming, and error handling. Keep review reports factual and concise.
- Use Python 3.10+ and PySide6; do not introduce PyQt.
- Preserve Qt layouts, flexible sizing, persisted splitters, signal-recursion
  guards, worker cleanup and EXIF-aware loading. Do not retrofit unrelated code
  to these rules.

### Stage reports

In managed runs, follow the generated role-specific response format. Reviewers
return the required structured verdict, not an implementation checklist. Keep
implementation evidence in current-stage notes; do not duplicate whole diffs or
completed-stage history.

Outside managed runs, report touched files/symbols, reused code paths,
validation/results, Codex review outcome, what the inspected screenshots show,
deviations with reasons, and verification tier (commit hash, layout question for
the user, or numbered human checks). Cite symbols or lines for behavior claims.

## Database and generated artifacts

- Inspect the existing schema and the SQLite migration guide before changing
  persistence; do not put Supabase/Postgres SQL in the SQLite migration directory.
- Preserve existing data and add focused migration/round-trip coverage for schema changes.
- Do not edit or commit generated databases, caches, local credentials, downloaded source archives, or ad hoc screenshot output. Commit generated artifacts only when the owning documented workflow explicitly requires them (for example, translated `.qm` files alongside their `.ts` sources).

## Plugins

Superpowers skills are optional techniques, not the project workflow. Do not invoke Superpowers brainstorming, writing-plans, executing-plans, subagent-driven-development, or finishing-a-development-branch unless explicitly requested. The repository's docs/plans structure, phase contracts, and project-specific agent/review instructions are authoritative. Superpowers systematic-debugging and verification-before-completion may be used when useful, provided they do not replace or modify the project workflow.
