# Database package layout and SQLite migrations

Source: external review (its R4, R5, R3 and R6), verified on 2026-11-18 against
`main` at `a2cc84f`. Line numbers are as of that commit.

## Verified findings

- **Reverse dependency (R4):** `ui/observations_tab.py:11046` lazy-imports
  `parse_picker_results` from the operational script
  `database/fetch_artportalen_taxon_ids.py:197`. Confirmed.
- **Taxonomy tooling inside the runtime package (R4):** `database/taxonomy/` is
  168 files / 63k lines, 56 of them test files with their own `conftest.py`;
  a root `pytest` run collects them. It does **not** bloat the app bundle
  (PyInstaller ships only statically reachable modules), so the payoff of moving
  it is navigation and import hygiene only. The review's "~50 evidence scripts"
  did not reproduce (6 by filename).
- **`models.py` (R5):** 9 classes, 106 methods, 100 `@staticmethod`; lazy
  exports in `database/__init__.py:31`. Review's "106 of 117" was off.
- **Migrations (R5):** `schema.py` has 98 `except sqlite3.OperationalError`
  handlers and no `PRAGMA user_version`; `database/sqlite_migrations/` holds
  only a README. `database/migrate.py` duplicates several `schema.py` helpers
  (`schema.py:715-946` vs `migrate.py:64-261`) and production never calls
  `migrate_database()`, but three test files do
  (`test_sample_source_split.py`, `test_image_provenance_columns.py`,
  `test_cloud_ai_selection_sync.py`) and `test_migrate_backup_wal.py` uses its
  snapshot helper. So those tests partly cover a path the app does not run.
- **Big-file split (R3):** `main_window.py` is 23k lines and
  `observations_tab.py` 22.6k (review said 20k/14k). The desktop UI redesign
  plan already extracts the monolith panels it touches as mechanical PRs. No
  separate plan; extractions from the other two plans shrink these files too.
- **Broad `except Exception` (R6):** current counts are higher than the review
  said: `observations_tab.py` 318, `main_window.py` 234, `cloud_sync.py` 135,
  `image_import_dialog.py` 74 (~270 followed directly by `pass`). No cleanup
  project; narrow them only in code a stage is already changing.

## Stage 1 — Fix the reverse import (small)

Move `parse_picker_results` into the Artsobs integration module the UI already
uses, and import it from there in both `observations_tab.py` and
`fetch_artportalen_taxon_ids.py`. Agent-verified by tests.

## Stage 2 — Decide what `migrate.py` is (owner decision, then small)

Either retarget its three migration tests at the `schema.py` code the app
actually runs, move `snapshot_sqlite_database` next to the backup code, and
delete `migrate.py`, or record why it must stay. Touches SQLite persistence
tests, so it needs the owner's go-ahead.

## Stage 3 — Ordered SQLite migrations (owner decision; high risk; own plan)

Introduce `PRAGMA user_version` and ordered migrations in
`database/sqlite_migrations/`, with fixture upgrades from real legacy DB
shapes. This is a schema/persistence change: write a dedicated plan and get
approval first. Scope is the owner's own desktop DB only
(single desktop-sync user), so no fleet compatibility layer.

## Optional, only if wanted

- Split `models.py` into per-class modules behind the existing lazy exports.
  Low risk, but it conflicts with every open branch that touches `models.py`;
  do it at a quiet moment, as one mechanical PR.
- Move `database/taxonomy/` tooling and tests out of the runtime package.
  Large mechanical churn for little gain; not recommended now.
