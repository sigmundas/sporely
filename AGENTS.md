## Working agreements

These instructions apply to this Git worktree and override conflicting shared
Sporely workflow defaults (including the parent's Claude commit/push policy).
Explicit user instructions take precedence over repository defaults. A generated
stage prompt does not by itself authorize overriding a repository Git restriction.

Start with `git status --short` to identify existing work. Preserve unrelated edits.
Read this file once per session; follow the task-specific reading routes below.

- Do not run `npm test` after modifying JavaScript files.
- Ask for confirmation before adding new production dependencies.
- Do not run heavy build steps, including Capacitor syncs, PyInstaller, Docker builds, full app builds, packaging commands, or dependency installation, unless explicitly requested.
- Keep patches narrow. If a task touches multiple workflows or large UI files, propose staged patches and stop after the current stage.
- Do not rewrite or refactor unrelated code while fixing a bug. Preserve existing behavior unless the prompt explicitly asks for a behavior change.
- Agents may commit, but only work whose verification has actually passed. When a task defines numbered stages, commit each verified stage as its own commit and report the hash.
  - Record progress before stopping using the workflow-specific destination under **Context discipline** below. Include verification, commit or manual-test status, and deferred work.
  - **Self-verifiable stages** — the checks are ones you can run: unit tests, syntax checks, renderer screenshots for static layout. Run them, then commit.
  - **Human-gated stages** — verification needs the user: interactive behavior (signal loops, focus, scroll retention, drag/resize), state surviving an app restart, camera/microscope hardware, live Supabase writes, RLS, cross-client sync, performance on real data, or judgment about whether output reads correctly to a mycologist. Do not commit. Leave the work uncommitted, and report a numbered checklist of exactly what the user must do to verify. The commit happens after the user confirms, in the next task.
  - A renderer screenshot proves layout, not behavior. A change to what happens when the user interacts is human-gated even when every screenshot is clean.
  - If verification fails partway, do not commit a partial stage — the failure is the report.
  - Push completed stage work; see **Git policy** below.
- For sporely-py, always use the project virtual environment:
  `/Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/python` and
  `/Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/pytest`.
  Ask for confirmation before installing or upgrading packages in .venv.
  Run from this worktree root. For syntax checks, use the same absolute Python
  path above with `-m py_compile <touched files>`; this worktree has no local `.venv`.

## Git policy

Agents may create branches, commit, push, merge, and delete branches as needed to complete the task.

Use normal Git workflows and keep history understandable.

Do not:
- force-push unless the user explicitly asks for it;
- rewrite published history unnecessarily;
- push secrets or credentials;
- merge obviously unrelated work;
- deploy, publish a release, or modify production systems unless the task explicitly includes that.

For staged/agent-sparring work:
- commit and push completed stage work;
- merge when the stage or plan calls for it;
- leave a clear handoff describing what changed, what was tested, and any unresolved issues.

This permission does not weaken the verification rules under **Working agreements**:
only work whose verification has actually passed may be committed, and a
human-gated stage still waits for the user's confirmation before its commit.

## Subsystem rules (read before touching these areas)

Read only the rules for the area the task touches:

- **Cloud sync** (`utils/cloud_sync*`, sync tests/contracts, cursors, media flags,
  cloud writes): `.claude/rules/cloud-sync.md`; the tracked contract is
  `docs/supabase-sync-contract.md`, with navigation in `docs/cloud-sync-architecture.md`.
- **Localization** (`tr(...)` strings, `i18n/`): `.claude/rules/localization.md`;
  also see the Localization & Text section in `docs/development/gui-conventions.md`.
- **Visual UI changes / screenshot tooling**: `.claude/rules/ui-screenshots.md`;
  the renderer entry point is `tools/render_review_screenshots.py`.
- **GUI behavior/layout**: relevant sections of `docs/development/gui-conventions.md`.
- **SQLite persistence**: `database/sqlite_migrations/README.md` before schema edits.

The three `.claude/rules/` files are version-controlled project invariants.
Claude can load matching path rules automatically; other agents must read the
applicable rule explicitly. Do not load unrelated subsystem rules.

## Agent routing and token budget

Use one top-level agent for one architectural slice. Subagents are optional and should be used only when they save context or provide genuinely independent review. Do not automatically chain planner -> implementer -> reviewer.

For optional role/model selection, read `docs/development/agent-routing.md`
only when delegating. Use bounded read-only exploration, test/log investigation,
or specialist review; do not delegate concurrent implementation writes. A
subagent review does not replace the independent top-level sparring session.

### Context discipline

- Identify the workflow from the current task, not merely the presence of
  `.sparring/`. Read only the assigned stage and current feedback/handoff.
  - **Agent Sparring managed run:** the input plan is immutable. Record progress
    in the current stage's `notes.md`; the engine owns generated handoffs,
    verdicts, state and plan-run bookkeeping. Do not manually edit those files.
    The engine owns top-level session lifecycle and stage advancement.
  - **Other staged implementation:** update the canonical active plan's current
    stage/handoff record before stopping. Do not select a plan by guessing from
    a directory listing; use the task's plan reference.
  - **Ordinary bounded task:** no plan or staged report is required unless the
    task requests one.
- Managed acceptance requires a pushed candidate. Push the candidate as part of
  finalization, under **Git policy** above.
- At a subsystem boundary, write a compact handoff and stop at the assigned
  stage. Outside managed runs, prefer a fresh session for the next slice.
- Do not load completed-stage history unless a concrete compatibility question
  requires it. Use relevant headings of `docs/technical-overview.md` only when
  orientation is needed, rather than reading the whole overview by default.
- Search before reading. Use `rg`/symbol search, then inspect bounded ranges around relevant definitions/callers. Never dump a large file to context just to understand it. In particular, do not read `ui/main_window.py`, `ui/observations_tab.py`, `utils/cloud_sync.py`, or other multi-thousand-line modules wholesale.
- Scope searches to this worktree. Never search the parent `sporely/` directory
  or substitute sibling worktrees: they can contain plausible stale code.
- Start change reviews with status and diff stat/name lists for the actual review
  base and candidate (include staged/untracked work for a working-tree review).
  Then read targeted diffs. An empty unstaged diff does not prove there is no
  change. Expand only for touched symbols or a concrete suspected failure mode.
- Do not have multiple agents perform the same repository archaeology. If a scout already returned the relevant symbols/call path, later agents should use that handoff and verify only where necessary.
- When delegating a scout/review subtask, give a narrow question and ask for a compact result (normally <=20 lines plus file/symbol references). Do not ask for broad repository summaries.
- Keep command output small: scoped `rg -n`, bounded `sed` ranges, focused tests.
  If output truncates, narrow the query rather than rereading the same dump.
  The implementer owns required validation. Reviewers verify evidence against
  the candidate and rerun checks when a finding, changed code/environment, or
  missing/stale evidence warrants it; a prior agent's report is not proof.
- Compaction restores context-window room but does not make prior work free. Do not use compaction as a reason to broaden scope or carry a finished subsystem into the next stage.

## Document authority

`AGENTS.md` owns agent workflow; subsystem rules/contracts own specific intended
behavior. Code/tests establish implementation reality, not permission to ignore
a contract. Report disagreements. `README.md` is the human-facing index and
`docs/technical-overview.md` is orientation, not requirements. Plans under
`docs/plans/active/` describe unfinished work; `INBOX.md` and completed plans are
not current task authorization.

## Review and implementation conventions

- In reviews, distinguish correctness defects from cleanup or style opinions. Check especially for duplicate logic, competing sources of truth, database consistency, state-flow errors, UI inconsistency, dead code, unclear boundaries, naming, and error handling. Keep review reports factual and concise.
- Prefer extending an existing widget, dialog, state structure, or code path over adding a parallel one. If the task assumes a reuse target and you cannot find it, stop and report rather than writing a second implementation.
- Use Python 3.10+ and PySide6; do not introduce PyQt.
- Read applicable GUI conventions via the subsystem routes above. Preserve Qt
  layouts, flexible sizing, persisted splitters, signal-recursion guards, worker
  cleanup and EXIF-aware loading. Do not retrofit unrelated code to these rules.

### Stage reports

In managed runs, follow the generated role-specific response format. Reviewers
return the required structured verdict, not an implementation checklist. Keep
implementation evidence in current-stage notes; do not duplicate whole diffs or
completed-stage history.

For implementation stages outside managed runs, report touched files/symbols,
reused code paths, validation/results, visual evidence, deviations with reasons,
and verification tier (commit hash or numbered human checks). Cite symbols or
lines for behavior claims. Describe what screenshots show; rendering without
errors is neither layout inspection nor proof of interactive behavior.

## Database and generated artifacts

- Inspect the existing schema and the SQLite migration guide before changing
  persistence; do not put Supabase/Postgres SQL in the SQLite migration directory.
- Preserve existing data and add focused migration/round-trip coverage for schema changes.
- Do not edit or commit generated databases, caches, local credentials, downloaded source archives, or ad hoc screenshot output. Commit generated artifacts only when the owning documented workflow explicitly requires them (for example, translated `.qm` files alongside their `.ts` sources).

## Plugins

Superpowers skills are optional techniques, not the project workflow. Do not invoke Superpowers brainstorming, writing-plans, executing-plans, subagent-driven-development, or finishing-a-development-branch unless explicitly requested. The repository's docs/plans structure, phase contracts, and project-specific agent/review instructions are authoritative. Superpowers systematic-debugging and verification-before-completion may be used when useful, provided they do not replace or modify the project workflow.
