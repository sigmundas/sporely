---
paths:
  - "utils/cloud_sync*.py"
  - "utils/cloud_sync/**"
  - "tests/test_cloud_*.py"
  - "tests/test_child_change_probe.py"
  - "tests/test_image_tombstones.py"
  - "docs/cloud-sync-*.md"
  - "docs/supabase-sync-contract.md"
---

# Cloud sync invariants

- Preserve account binding through `linked_cloud_user_id`; never sync a local database with a different account unless the user explicitly resets the cloud link.
- Keep `is_draft` (workflow), `sharing_scope`/cloud `visibility` (audience), and `location_precision` (location disclosure) as independent concepts.
- Auto-merge only non-overlapping changes covered by the sync contract. True overlapping edits require an explicit conflict plan; identity disagreement must fail closed.
- Treat `sync_images`, `materialize_remote_images`, and `full_pull` as independent controls. Never turn all three on as a generic "full sync" fix.
- Observations-tab refresh uses `sync_images=False`, `materialize_remote_images=True`, and `full_pull=False`: push metadata, fast-pull only new/changed remote observations, and download their missing media without scanning the local media backlog.
- Profile & Cloud "Sync now" uses `sync_images=True`, `materialize_remote_images=True`, and `full_pull=False`: upload genuinely pending selected local media and materialize new/changed remote media, while preserving unchanged-observation pruning.
- `full_pull=True` is a deep reconciliation/recovery control. Pair it with `sync_images=False` unless a separately named, user-confirmed migration explicitly requires scanning and uploading all pending local media.
- `sync_images=True` activates the global pending-image dirty scan. It may re-dirty synced observations containing eligible `cloud_id IS NULL` rows and must never be enabled for background/startup/ordinary refresh paths.
- `materialize_remote_images=True` controls remote byte download; it does not require `sync_images=True` or `full_pull=True`.
- Rows with `source_role=cloud_recovery_cache` or `file_purpose=cache` are remote-owned recovery copies. They may receive metadata/link repairs but their bytes must never be prepared or uploaded back to cloud.
- Preserve the no-op fast-path contract: unchanged observations must not trigger bulk image/measurement fetches, WebP preparation, measurement pushes, or mosaic rebuilds.
- Never issue no-op cloud writes on sync paths. `observation_images.updated_at` is trigger-bumped on every UPDATE for every role, so a value-identical PATCH advances the child-change cursor and forces re-pulls (2026-08-24 echo-loop incident). Check the current remote value and skip the request when it already matches (e.g. `_remote_image_desktop_id_current` for `desktop_id` relinks).
- The child-change cursor commits the true `MAX(updated_at, id)` tuple over every inspected row, ids compared numerically via `_child_change_cursor_id_key` (identical ordering in filter and advancement), and advances only after `pull_all` succeeds. Cursor/probe changes must run `tests/test_child_change_probe.py`.
- Any sync flag or media-selection wiring change must include focused tests for the exact caller mode and run `tests/test_cloud_sync_fast_path.py`, `tests/test_cloud_sync_dirty_loop_steady_state.py`, and the affected media pull/upload policy tests.
- Do not add new sync behavior by finding a convenient location in `utils/cloud_sync.py`. Identify the canonical owning subsystem/function first (see the ownership table in `docs/cloud-sync-architecture.md`). If ownership is unclear, document or establish the boundary before adding another implementation path.
- Do not bypass canonical policy functions: `cloud_image_bytes_desired` for byte-storage decisions, `_reconcile_local_image_cloud_id` for image identity repair, `_resolve_existing_observation_for_push` for observation push identity (verified local `cloud_id` is primary; remote `desktop_id` is recovery-only; a reverse-link miss is never a create signal), `_resolve_existing_image_for_push` for image push identity (verified `images.cloud_id` is primary; `desktop_id` scoped to observation is recovery; disagreement raises `ImageIdentityConflictError`), the tombstone helpers for deletion intent, and the snapshot store/load helpers for baselines.
- Never interpret a filtered, batched, bounded, or partial remote collection as deletion. Absence is meaningful only after a complete, successful paginated read.
- Bulk remote readers must use `SporelyCloudClient._get_paginated` with a deterministic `order=` clause ending in `id.asc`; PostgREST silently caps unpaginated responses at the server row limit.
- Download from Cloud (`sync_all(pull_only=True)`) is a strict zero-cloud-write mode. Any new `SporelyCloudClient` writer method must be added to `_PULL_ONLY_BLOCKED_CLIENT_METHODS`; new read methods join `_PULL_ONLY_ALLOWED_READ_METHODS` only as an explicit, reviewed choice. Pull-side writes must be gated at the source — a `blocked_write_attempts` entry is a bug, not a handled event.
- Sync behavior changes must update `docs/supabase-sync-contract.md` in this worktree and, when navigation or ownership changes, `docs/cloud-sync-architecture.md`, plus the relevant safety tests (`tests/test_cloud_download_only.py`, `tests/test_image_tombstones.py`, `tests/test_cloud_image_bytes_desired.py`).

If the task also changes another repository's sync contract, update its named
contract within the authorized scope after reading that repository's instructions.
Do not search or edit sibling worktree copies just to keep them in sync.
