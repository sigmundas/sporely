# Sporely Desktop — History & Debugging Notes

### Optional full-resolution original sync policy

Stage I is still helper-first. The desktop now preserves optional original-object metadata in
stored cloud snapshots, and the cloud schema now carries nullable
`public.observation_images.original_storage_path`, but the actual upload/download engine remains
deferred.

Covered changes:
- Added `utils/original_sync_policy.py` with candidate and safe-download helpers.
- Identified `sync_full_resolution_originals` as the opt-in gate and kept it default-off.
- Added nullable cloud contract support for `original_storage_path` and preserved it as passive
  snapshot metadata only.
- Added focused tests for canonical local eligibility, HEIC lineage handling, converted-local
  opt-in, snapshot preservation, and non-overwrite recovery.
- No runtime upload/download or local replacement behavior was added.

### Multi-asset calibration provenance

Stage H is now closed out. The desktop keeps calibration-side asset provenance in a local
`calibration_assets` table, preserves original source paths when existing calibrations are reopened
and resaved, and keeps the bundle export/import path portable for calibration assets without adding
cloud path columns.

Covered changes:
- Added a local `calibration_assets` table/model with deterministic `asset_uuid` values.
- Stored source photos, working photos, calibration crops, overlays, debug artifacts, and
  reference-cache rows with accepted role and purpose vocabulary.
- Preserved `source_path` and `working_path` through calibration save/load so old records do not
  lose source provenance on resave.
- Backfilled calibration assets from existing calibrations and kept missing-file rows safe.
- Exported/imported calibration assets in desktop bundle archives without touching the cloud
  contract.

### Image provenance/source tags

Stage E2 is now closed out. The desktop preserves HEIC source paths in `original_filepath` when a
converted working copy is created, keeps the local import provenance vocabulary aligned with the
existing image schema, and tags cloud-recovered media rows as cache-backed rather than canonical.

Covered changes:
- Audited every image-row creation path and confirmed the runtime paths either use
  `build_local_image_provenance(...)` or intentionally special-case cloud recovery/cache rows.
- Preserved the original source path for converted HEIC imports in the shared import flow and direct
  import entry points.
- Tagged cloud-recovered local rows with `source_role=cloud_recovery_cache` and
  `file_purpose=cache`.
- Normalized generated-artifact vocabulary to the accepted purposes used by the current code and
  tests.
- Confirmed the `_UNSET` cloud materialization fix only patches calibration ids when a matching
  local calibration exists.
- Added focused coverage for HEIC original-path preservation, cloud recovery provenance, and the
  generated-artifact vocabulary.
- Kept deferred items explicit: cloud provenance fields, full-resolution original sync, generated
  artifact tables, and multi-asset calibration provenance.

### Image-calibration linkage and reconciliation

The desktop now carries portable `calibration_uuid` values through image cloud payloads and snapshots, resolves them back to local `calibration_id` on import/materialization when the matching calibration exists, and reconciles imported cloud images from stored snapshots after calibration sync. This keeps the cloud link stable without treating the local numeric calibration id as the portable identity.

Covered changes:
- Threaded `calibration_uuid` through image push/pull snapshot payloads in `utils/cloud_sync.py`.
- Added snapshot-based reconciliation so images that arrived before calibrations can be linked once the calibration sync completes.
- Added focused tests for push, pull/materialization, reconciliation, and conflict matching.
- Added the cloud-side `observation_images.calibration_uuid` migration.

### Calibration reference recovery

The desktop now downloads cloud calibration reference images into a local recovery cache when the original local photo is missing. The recovery cache is keyed by `calibration_uuid`, the calibration dialog marks recovered previews as cloud-derived, and the canonical local original is never overwritten.

Covered changes:
- Added calibration recovery-state helpers and `download_calibration_reference_to_cache()` in `utils/cloud_sync.py`.
- Wired the calibration dialog to surface cached cloud references and provide a download action.
- Added tests for cache path resolution, download behavior, and UI state.

### Worker-backed desktop media sync

Desktop uploads, downloads, and deletes now go through the authenticated Cloudflare media Worker by default. Normal users only need their Supabase session plus the public Worker URL (`SPORELY_MEDIA_WORKER_URL`, default `https://upload.sporely.no`); direct R2 secrets remain admin/developer-only behind `SPORELY_ENABLE_DIRECT_R2=1`.

Worker failures are treated as recoverable sync issues so a local image stays pending instead of creating a broken cloud row. When media is missing, the sync path now surfaces the object key so broken remote rows can be reuploaded or removed during repair.

### Cloud media integrity repair

A media-health tool was added after older active `observation_images` rows were found pointing to missing R2 objects. The tool can dry-run all active cloud image rows, detect missing original/thumb objects, and repair them from matching local desktop files by reuploading to existing keys. It does not create duplicate cloud rows, tombstone rows, or delete R2/local files.

Remaining hardening:
- prevent active DB rows from being treated as healthy when upload verification fails
- consider warning during sync if an active cloud image row points to missing media
- keep R2 garbage collection separate from tombstone sync

## Phase 7: Transparency, Social Trails, and Privacy Slots
*Implemented Q2 2026*

### Change of Plans: Open Science First
We moved from a "Safety First" (hidden by default) model to an **"Open Science First"** (transparent by default) model to improve data density.
- **Drafts are Public by default.** This allows the community to see the "Live Stream" of science happening via a subtle "Draft" badge in the feed.
- **Privacy Slots Introduced.** If a user wants total secrecy, they toggle it to **Private** or **Fuzzed Location**, which consumes 1 of 20 Free Tier "Privacy Slots". 
- **Visibility Schema Overhaul:** 
  - Legacy `'draft'` visibility value shifted to `'private'`. 
  - `is_draft` (boolean) now handles workflow independently.
  - `location_precision` (`'exact'` vs `'fuzzed'`) separated from general visibility.

### Completed Phase 7 Milestones
- Added `is_draft` and `location_precision` columns to `observations` table (SQLite & Supabase).
- Created `follows` table for social trails (`user_id`, `target_type`, `target_id`).
- Updated Postgres trigger `enforce_non_public_observation_limit()` to watch for `visibility != 'public'` OR `location_precision = 'fuzzed'`.
- Updated `observations_community_view` to return exact GPS by default.
- Refactored UI layouts across desktop/web to support the split Draft/Privacy scope.
- Preserved local `private` semantics while translating legacy cloud `draft` rows backward securely.

## R2 Media Migration
*Migrated Q2 2026*
- Shifted media from Supabase Storage `observation-images` to Cloudflare R2 bucket `sporely-media`.
- Deployed Cloudflare Upload Worker at `upload.sporely.no` enforcing ES256 JWT auth and updating user storage quotas.

## Refactor Notes & Lessons Learned
- **Splitter Collapse Traps:** Historically, fixed minimum widths on QSplitter children caused sidebars to get stuck. Standardized on `QSizePolicy.Ignored` horizontally with minimal guardrail limits.
- **Snapshot Staleness:** An issue in cloud pull where the stored snapshot missed the newly assigned `desktop_id`, causing the *next* sync to falsely read a conflict (shifting from `cloud:<id>` to `desktop:<id>`). Fixed by refreshing the snapshot *after* all ID injections.
- **Thread Garbage Collection:** `QThread` objects were prematurely collected because custom signals (`search_done`) shadowed built-in thread signals. Fixed by blocking local cleanup until `QThread.finished` properly emits.
- **Background Worker Lifecycles:** When `QThread` instances are assigned to transient local variables or overwritten before completion, Python's GC destroys them while the C++ thread is still running, crashing the app (`QThread: Destroyed while thread is still running`). Fixed by using a global tracking list (e.g., `_track_worker()`) to keep strong references until `QThread.finished` fires and safe deletion can occur.
- **OAuth Event Loop Blocking:** The local HTTP server used for iNaturalist OAuth2 callbacks blocks the main thread. To keep the PySide6 UI responsive (and allow cancellations), a `tick_callback` (which calls `QApplication.processEvents()`) must be propagated through the `authorize()` signature down to the server's `wait_for_callback` loop.
