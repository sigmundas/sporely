# Cloud media garbage collection plan

Status: Deferred; implementation not found.

## Agent handoff

- Status: Deferred; implementation not found.
- Last completed stage: None verified.
- Current/next stage: Define retention/recovery policy and schema before any purge implementation.
- Important decisions: Keep tombstone rows as sync identity and distinguish retained, missing, and intentionally purged media.
- Do not: Delete local files or full-resolution originals without explicit policy and user intent.
- Remaining acceptance criteria: The policy, constraints, and rollout requirements below.

Purpose: clean up R2 objects for tombstoned image rows after sync identity and provenance are stable.

Planned policy:

- Single-image delete immediately sets `observation_images.deleted_at`.
- R2 objects are retained during a recovery/undo/sync-safety window.
- A later cleanup job purges R2 `storage_path` and generated variants for tombstoned rows older than the retention period.
- Add `storage_purged_at` before automatic purging so missing media can be distinguished from intentionally purged media.
- Do not delete `observation_images` rows when purging bucket objects; keep tombstone identity for sync/reupload blocking.
- Do not purge full-resolution originals unless full-original sync is explicitly implemented and the user chose permanent deletion.
