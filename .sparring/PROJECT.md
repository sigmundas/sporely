# sporely-py — project knowledge

Durable knowledge for an implementation or sparring agent working on this
repository. Verified against the working tree on 2026-09-11 at commit
`8097bc8` (branch `feature/reference-save-and-plot`, the base of the current
statistics work).

This file is injected verbatim into both the stage-agent and the sparring-agent
prompt. Keep it factual and short; stage-specific scope belongs in the stage
brief, not here. `AGENTS.md` at the repository root is the authoritative agent
instruction file and is loaded by Claude Code automatically; this file adds the
context the sparring workflow needs and does not replace it.

## What this repository is

`sporely-py` (GitHub repo name `sporely`) is Sporely's Python/PySide6 desktop
application: field observations, microscopy calibration, spore measurement,
a reference library of literature spore data, and optional bidirectional sync
with Sporely Cloud (Supabase). The local SQLite databases are the source of
truth; cloud sync is an add-on.

Siblings under `~/Documents/Code/sporely/`: `sporely-web/` (mobile web app and
the **owner of all Supabase migrations**), `sporely-landing/`, `sporely-admin/`.
A stage in this repository may read `sporely-web/supabase/migrations/` to learn
the deployed cloud contract, but must never edit a sibling repository.

**Stale-worktree hazard.** The parent directory holds orphaned worktree copies
(`sporely-web-*`, `sporely-landing-stage6*`, `sporely-admin-*`,
`sporely-recovery-*`) whose git metadata is dead. They are readable, plausible,
stale source. Never search the parent root; scope every search to this
repository's own directory. `sporely-py-reported-statistics/` is a live
worktree of this repository, not one of the orphans.

**Worktree layout.** The canonical checkout is
`/Users/sigmundas/Documents/Code/sporely/sporely-py` (branch
`feature/reference-save-and-plot`, carrying human-gated uncommitted parser
work that must not be touched). Stage work for the reported-statistics plan
runs in the linked worktree
`/Users/sigmundas/Documents/Code/sporely/sporely-py-reported-statistics` on
branch `feature/reported-statistics-contract`. Run every command from the
worktree you were started in; do not `cd` into the canonical checkout.

## Stack

- Python 3.10+ (the project venv runs 3.14), PySide6. Never introduce PyQt.
- SQLite via the standard library; schema owners are plain Python modules
  (`database/*_schema.py`), migrations are Python helpers documented in
  `database/sqlite_migrations/README.md`. No Postgres SQL in that directory.
- Supabase (PostgREST + RPCs) for cloud sync; Cloudflare R2 for media.
- Tests: pytest, in `tests/` (flat, ~220 modules) plus
  `database/taxonomy/tests/`. `tests/conftest.py` inserts the repo root on
  `sys.path`.

## Important directories

- `ui/` — PySide6 widgets. `ui/main_window.py` (22.7k lines),
  `ui/observations_tab.py` (20.8k) are huge: search symbols, read bounded
  ranges, never read wholesale.
- `utils/` — integrations; `utils/cloud_sync.py` (24.8k lines) is the
  observation sync engine and is governed by `.claude/rules/cloud-sync.md`.
- `database/` — SQLite schemas, repositories, sync-state and reconciliation
  modules. Reference library: `database/reference_library.py` (repository and
  `MeasurementSet` model), `database/reference_library_schema.py` (schema
  owner, `init_reference_library_schema`), `database/reference_sync_state.py`,
  `database/reference_sync_reconciliation.py`, `database/reference_citation.py`
  (snapshots), `database/reference_use_sync_reconciliation.py`,
  `database/curated_reference_forks.py`.
- `references/` — pure domain code such as `references/measurement_parser.py`
  and `references/reference_plotting.py`.
- `utils/reference_cloud_adapter.py`, `utils/reference_cloud_sync.py`,
  `utils/db_share.py` (bundle import), `utils/archive/portable_import.py`.
- `docs/plans/active/` — canonical active plans; `docs/plans/completed/` —
  finished ones. `docs/technical-overview.md` is orientation only.
- `docs/architecture/decisions/` — numbered ADRs (0001–0005 exist).
- `.claude/rules/` — path-scoped invariants: `cloud-sync.md`,
  `localization.md`, `ui-screenshots.md`. Read the matching one before touching
  sync, `tr(...)` strings, or screenshot tooling.

## Test / build commands

Always use the project virtual environment by absolute path; the linked
worktree has no `.venv` of its own:

```bash
PY=/Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/python
PT=/Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/pytest

$PT -q -p no:cacheprovider tests/test_reference_library_schema.py   # focused
QT_QPA_PLATFORM=offscreen $PT -q -p no:cacheprovider                # full suite
$PY -m py_compile <touched files>                                    # syntax
git diff --check                                                     # whitespace
```

Prefer focused runs during iteration. Run the full suite once at a stage
boundary and report the counts you observe. Do not install or upgrade packages
in the venv without asking. Do not run PyInstaller, Docker, packaging or
dependency installation.

### Known baseline — do not attribute these to your change

Full suite at baseline `8097bc8`, run in the linked worktree
`sporely-py-reported-statistics` with the command above on 2026-09-11:
**31 failed, 3936 passed, 10 skipped, 55 errors, exit 1** (about 2 min 45 s).
All of it pre-exists and none of it touches the reference library:

1. `fixture 'qapp' not found` — 35 errors in `tests/test_image_gallery_widget.py`
   (17), `tests/test_observations_tab_gallery_move.py` (12),
   `tests/test_live_lab_raw_controls.py` (6). `pytest-qt` is not installed in
   the venv. Reproduces in the canonical checkout. Do not install it.
2. `tests/taxonomy/test_w2d_reconciliation.py` (19 errors, 1 failure) and
   `tests/taxonomy/test_supplement_loader.py` (12 failures) — they need the
   gitignored generated taxonomy release directory
   `database/reference_data/generated/taxonomy_v2/global_macrofungi_tax-2026.08.01-01/`,
   which exists only in the canonical checkout, not in the linked worktree.
3. `tests/test_cloud_media_recovery.py` — collection error
   `No module named 'scripts.recover_cloud_media'`: the top-level `scripts/`
   directory is shadowed by `database/taxonomy/scripts` during full
   collection. The module collects fine on its own.
4. Single pre-existing failures: `tests/test_observation_geography_sync.py`
   (9, `_RecordingClient` lacks `_sync_observation_selected_taxon`),
   `tests/test_render_review_screenshots.py` (4), `tests/test_taxon_lookup.py`,
   `tests/test_sample_source_ui_presence.py`, `tests/test_image_gallery_widget.py`
   (1 failure beyond the fixture errors), `tests/test_archive_inventory.py`,
   `tests/test_ai_id_parity.py` (1 each; the last two also need gitignored
   local data such as `database/reference_data/sources/artsorakel_3images.txt`).

A stage is clean when the failing set is exactly this and nothing else. Report
the counts you observe, not "tests pass". The reference-library modules
(`tests/test_reference_*.py`, `tests/test_curated_reference_*.py`,
`tests/test_legacy_reference_*.py`, `tests/test_observation_reference_use_*.py`)
all pass at baseline.

## Coding conventions

- Native type annotations; dataclasses for structured data between UI and
  logic; private UI builders and slots prefixed with one underscore.
- Qt layouts, not absolute positioning; `QSplitter` state via `QSettings`;
  guard programmatic widget updates against signal recursion; network and
  heavy work off the UI thread. Details: `docs/development/gui-conventions.md`.
- Prefer extending an existing widget, dialog, repository method or code path
  over adding a parallel one. If a reuse target the task assumes cannot be
  found, stop and report rather than writing a second implementation.
- Keep patches narrow. Do not refactor unrelated code while fixing a bug.
- Ask before adding a production dependency.
- User-visible strings go through `tr(...)`; see `.claude/rules/localization.md`.

## Working-tree hygiene

- Inspect `git status` before staging; stage only files belonging to the
  current stage.
- Never commit generated databases, caches, credentials, downloaded archives,
  or ad hoc screenshots.
- Never rewrite published history (no `--force`, no `--amend` on a pushed
  commit). A candidate SHA handed to review is frozen; corrections are new
  commits.
- Do not merge to `main`. That is a separate human decision.

## Verification tiers (from AGENTS.md)

- **Self-verifiable**: unit tests, syntax checks, fixture round trips,
  renderer screenshots for static layout. Run them, commit, push.
- **Human-gated**: interactive behavior, state surviving an app restart,
  camera/microscope hardware, live Supabase writes, RLS, cross-client sync,
  running actual older desktop binaries, judgment about whether output reads
  correctly to a mycologist. Do not commit human-gated work; leave it local
  and report a numbered checklist. A renderer screenshot proves layout, not
  behavior.

If a stage's real proof needs something you cannot run, say so and stop.
That is a legitimate NEEDS_YOU, not a failure.

## Product constraints and invariants

- **Cloud sync** invariants in `.claude/rules/cloud-sync.md` encode real
  incidents (echo loops from no-op writes, cursor regressions). Do not
  improvise in `utils/cloud_sync.py`; find the canonical owning function.
- **Supabase migrations live in `sporely-web` and are production history.**
  Never author, edit, or apply a migration from this repository. Cloud contract
  changes are proposed in a plan and implemented in `sporely-web` as a separate
  stage. Never connect to production data from a stage.
- **Reference library**: one measurement set is the atomic persistence and
  sync unit. Scalar means live only in `length_mean`/`width_mean`/`q_mean`.
  Legacy `p05`/`p95`-named columns are not statistical evidence of percentiles.
  No inferred statistics, midpoints, CV, or Parmasto eligibility from tags.
- **Cross-repository consistency**: `docs/supabase-sync-contract.md` exists in
  both this repository and `sporely-web`; sync behavior changes update both.
- **Localization**: `.ts`/`.qm` files are regenerated by the documented
  workflow only.

## Data handling

Local databases and the production cloud hold personal data (accounts,
emails, observation locations). Under SINTEF policy this tooling is approved
for green and yellow data only. Work against schemas, contracts, fixtures and
synthetic data; never load a user's real `sporely.db` or production rows into
a prompt. Never put a service-role key or access token in a prompt or a
commit.

## Reading discipline

Search symbols first (`rg "symbol" path/`), then read bounded ranges. A
wholesale read of `ui/main_window.py`, `ui/observations_tab.py` or
`utils/cloud_sync.py` ends a session. The active plan's **current
stage/handoff section** is the first thing to read; completed-stage history
is reference-only.

## Subagent policy

Default: do the stage directly in the top-level implementation session. The
stage agent is the single implementation owner.

Subagents may be used only for read-only exploration of a genuinely large or
unfamiliar area (`Explore`, Haiku) or a clearly independent work package the
brief explicitly permits (`sporely-implementer`). Do not use subagents for
ordinary search, tightly coupled edits, splitting one feature into pieces, or
"independent review" (agent-sparring already provides that). No nested
delegation; no concurrent writers in the same worktree.

If a subagent is used, the handoff must state which one, its exact task,
whether it edited files, the model if actually known (else "unknown"), and
what was incorporated. The stage agent remains responsible for integrating,
verifying, and committing.
