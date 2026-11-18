# Stale code sweep

Source: external performance/stale-code review, verified on 2026-11-18 against
`main` at `a2cc84f`. Line numbers are as of that commit; re-grep before editing.

Packaging note: the build scripts (`build_mac.sh`, `build_linux.sh`,
`build.ps1`) use no `collect_submodules`/`--collect-*`, so PyInstaller ships
only modules reachable by static import. None of the files below ships in the
app; the payoff is clarity and removing foot-guns, not bundle size.

## Verified findings

| Item | Verdict | Evidence |
|---|---|---|
| `database/userreset.py` | Dead, dangerous | No references. `requests.put` to the Supabase admin users endpoint runs at **module scope** on import. Key is a truncated placeholder, not a real secret. |
| `database/add_point_columns.py` | Dead, superseded | No importers; `schema.py:1918-1925` creates `p1_x`…`p4_y`. Only `database/sqlite_migrations/README.md:8` mentions it (wrongly, as in use). |
| `update_translations_final.py` (root) | Dead, dangerous | No references. Rewrites `i18n/Sporely_{nb_NO,sv_SE,de_DE}.ts` at module scope (lines 117-119), no main guard. Strings it adds that are missing from `i18n/` (“Sync content”, “Client Secret (optional…)”) no longer exist in source, so nothing is lost. Real workflow: `tools/update_translations.sh`. |
| `ui/measurement_tool.py` | Dead (**not in the review**) | `MeasurementTool` is imported nowhere; only listed as a source file in `tools/update_translations.sh` / `.ps1`. |
| `MainWindow.load_image` (`ui/main_window.py:14380`), `MainWindow.load_image_for_observation` (`:22818`) | Dead (**not in the review**) | No callers or signal connections. The review cited the first as the live RAW import path; it is not. |
| `CalibrationDialog.set_calibration_distance` / `set_calibration_preview` (`ui/calibration_dialog.py:6468`, `:6472`) | Dead | `pass`-only. The callers in `image_import_dialog.py` (2247, 6566) act on `ScaleBarImportDialog` (`enter_calibration_mode` is only called with one at `:824`). The review's line numbers (6185-6195) were wrong. |
| `one_time_exif_sync.py` (root) | Unreferenced one-off | Has a main guard; imports private `utils/cloud_sync` helpers. |
| `reset_database.py` (root) | Unreferenced operator CLI | Destructive, gated by `input()`. Depends on `database.migrate.snapshot_sqlite_database`. |
| `utils/raw_presets.py`, `utils/generated_artifact_provenance.py` | Test-only | No non-test importers; no active plan names them. |
| `database/import_taxa_to_supabase.py` | Legacy one-off | `database/README.md:79` lists it as legacy. Calls `sys.exit` at import time if the admin key is missing. |
| `database/migrate.py` | **Not dead** — see `2026-11-18-database-package-layout.md` | Production never calls `migrate_database()`, but three test files do, and its helpers duplicate `schema.py`. Belongs with the migration-pipeline decision, not this sweep. |
| `database/migrate_observations_sporely_id.py`, `database/audit_observation_identity.py`, `utils/cloud_spore_mosaic_backfill.py` | **Not stale** — keep | Imported by tests; documented operator tools (`docs/taxonomy-identity-repair-runbook.md`, `docs/cloud-sync-architecture.md:58`, a subprocess `--help` test). Moving them is churn with no payoff. |

## Stage 1 — Delete dead code (one PR)

- Delete `database/userreset.py`, `database/add_point_columns.py`,
  `update_translations_final.py`, `ui/measurement_tool.py`.
- Delete `MainWindow.load_image`, `MainWindow.load_image_for_observation`,
  `CalibrationDialog.set_calibration_distance`, `set_calibration_preview`.
  Grep for `getattr`/string uses of each name first.
- Remove `ui/measurement_tool.py` from `tools/update_translations.sh` and `.ps1`.
- Fix `database/sqlite_migrations/README.md:8` (it should not name
  `add_point_columns.py`; leave the `migrate.py` wording to the database plan).
- Verification (agent tier): `py_compile` on touched files, full `pytest`.
  Do not run `lupdate` in this PR; obsolete strings drop out on the next
  normal translation update.

## Stage 2 — Move or retire operator scripts (one PR, after owner decisions)

- `reset_database.py`, `one_time_exif_sync.py`, `database/import_taxa_to_supabase.py`
  → `tools/` (update `database/README.md:79` and
  `database/taxonomy/docs/w3a-compatibility-audit.md:49`), or delete.
- `utils/raw_presets.py`, `utils/generated_artifact_provenance.py`: wire into a
  feature or delete together with their tests.

### Open decisions for the owner

1. Has `one_time_exif_sync.py` done its job (delete) or is it still needed (move)?
2. Are `raw_presets` / `generated_artifact_provenance` planned features (keep,
   and add them to a plan) or abandoned scaffolding (delete with tests)?
3. `import_taxa_to_supabase.py`: delete as superseded, or keep under `tools/`?
