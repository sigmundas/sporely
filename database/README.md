# Desktop Database

This folder contains the desktop app's SQLite runtime schema, migration helpers, and database-adjacent code.

## Important boundaries

- The desktop SQLite database is not intended to match Supabase/Postgres DDL exactly.
- `schema.py` and `migrate.py` own the local schema/versioning behavior.
- `sqlite_migrations/` is reserved for standalone local SQL migrations if we add them later.
- Shared field names should be validated by contract or code review, not by copying Postgres DDL into SQLite.

## Runtime SQLite files

- The local app database lives outside the repo in the user's app-data directory.
- The bundled reference database and taxonomy lookup files now live under `reference_data/`.

## Reference data pipeline

- `reference_data/sources/` holds source inputs such as `taxon.txt`, `vernacularname.txt`, and Parmasto tables.
- `reference_data/generated/` holds build artifacts such as `reference_values.db`, `vernacular_multilanguage*.sqlite3`, and the Artportalen lookup exports.
- Rebuild instructions live in `reference_data/README.md`.

## Schema helpers

- `schema.py` initializes and upgrades the local app database.
- `migrate.py` and `add_point_columns.py` are app-maintenance scripts, not Supabase migrations.


## ongoing
Stage A — sporely-py local calibration UUID
  Add calibration_uuid to local SQLite calibrations.
  Backfill local rows.
  Generate UUIDs for new calibrations.
  Preserve UUIDs in export/import.

Stage B — sporely-web / Supabase calibration UUID
  Add calibration_uuid to public.calibrations in Supabase.
  Use native uuid.
  Backfill existing cloud rows.
  Set default gen_random_uuid().
  Set NOT NULL.
  Add uniqueness on (user_id, calibration_uuid).
  Do not add desktop_id yet.

Stage C — metadata-only calibration push/pull
Stage D — calibration photo/reference-image sync
Stage E — image-calibration linkage/reconciliation
  