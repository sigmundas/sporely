# Cloud-media incident audit

`scripts/audit_cloud_media.py` creates a read-only evidence inventory for the
August 2026 cloud-media incident. It does not recover or mutate data.

Examples:

```bash
./.venv/bin/python scripts/audit_cloud_media.py --no-storage-check
./.venv/bin/python scripts/audit_cloud_media.py --observation-id 123 --output audit.json
./.venv/bin/python scripts/audit_cloud_media.py --name "Mycena haematopus" --output audit.csv
./.venv/bin/python scripts/audit_cloud_media.py --since 2026-08-03 --max-observations 100
```

Local SQLite is opened with `mode=ro`. Cloud reads are scoped to the currently
authenticated user. REST reads use a fixed-token, explicitly non-refreshing GET
path. An expired token aborts with a reauthentication message; the audit cannot
refresh, save, or clear credentials.

Cloud IDs are queried in bounded batches and every query is paginated. The
report records requested IDs, returned rows, batch/page counts, and completeness
for observations, images, and measurements. Any page failure aborts the audit
before missing-row classification.

Storage verification uses an authenticated one-byte range request and
distinguishes missing, unauthorized, unavailable, and unsupported results.
`--no-storage-check` records `not_checked` without changing metadata-based
classification. Suggested fallback matches are never treated as authoritative,
and duplicate stable IDs or ambiguous candidates are surfaced for review.

`--output` writes a diagnostic JSON or CSV report. Existing files are not
overwritten unless `--force` is supplied; writes use a temporary file followed
by an atomic rename and request restrictive permissions where supported.

Do not run the audit against production until its implementation and intended
output location have been reviewed. Reports contain local file paths and cloud
object keys and should be handled as private diagnostic data.

Metadata-only anchors are considered legitimate only for active microscope
rows on observations with public spore data and at least one production-
eligible length/width measurement. Field rows and nonqualifying microscope rows
with no `storage_path` are reported as active rows missing a storage path unless
stronger incident evidence makes them suspicious. Incident timing uses sync and
tombstone timestamps, never the biological observation date.

## Separate deletion-safety follow-up

This phase records but does not change `delete_cloud_observation()`. That method
currently starts storage deletion before database deletion completes,
hard-deletes image and observation rows, and gathers `storage_path` but
apparently not `original_storage_path`. It requires a separate design and fix.
