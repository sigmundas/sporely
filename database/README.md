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
Stage A — DONE
Stage B — DONE
Stage C — DONE
Stage D1 — DONE
Stage D2 — DONE

Stage E1 — Tombstone deletion model — DONE
  Local + cloud tombstones exist and sync both ways.
  Deleted images do not reappear from cloud/other desktop.
  Local originals/measurements are not automatically destroyed by cloud tombstones.

Stage E2 — Image provenance/source tags — NEXT
  Add/define DB concepts for:
  imported source, converted local working image, local canonical image,
  cloud derivative, cloud recovery/cache, generated thumbnail/plot/artifact.


Stage E — image provenance tags and tombstone deletion model
  Add clear DB concepts for image roles/source types before deeper file sync.
  Distinguish imported source, local canonical image, cloud derivative, cloud recovery/cache, generated artifacts.
  Add deletion/tombstone behavior so deleted images do not reappear from cloud or another device.
  Cover local deletion, cloud deletion/soft-delete, dependent measurements, and cross-device refresh.

Stage F — calibration photo recovery/download cache
  Download cloud calibration derivative to a cache/recovery location when local photo is missing.
  Mark it cloud-derived.
  Do not overwrite local originals.
  Do not write recovery paths into canonical local provenance fields unless explicitly designed.

Stage G — image-calibration linkage/reconciliation
  Link synced calibration records to images/calibration_id safely.
  Reconcile scale fields, objective names, and calibration_uuid.
  Avoid automatic rescaling unless conflicts are clear.

Stage H — multi-asset calibration provenance
  Add a dedicated calibration_assets-style model/table if needed.
  Support multiple calibration photos, crops, overlays, role labels, hashes, derived artifacts, and provenance.
  Do not overload public.calibrations with many path columns.

Stage I — optional full-resolution original sync
  Only after provenance, quotas, and user settings are clear.
  Never replace better local originals with cloud copies.
  