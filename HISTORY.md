# Sporely Desktop — History & Debugging Notes

### RAW Auto Levels — three-way invariant between widget, pipeline, and Preferences

The shared `RawProcessingControls` widget (`ui/raw_processing_controls.py`) had three
independent bugs that only surfaced together when the user opened the advanced RAW
panel with a non-default bright cutoff in Preferences:

1. **Preferences cutoffs never reached the pipeline.** `_settings_from_controls`
   hard-coded `black_percentile=0.0, white_percentile=1.0`, so the auto-levels stage
   always stretched image min → 0 and image max → 1 regardless of the user's
   Preferences cutoff (defaults 0% dark, 0% bright — but Preferences let users set
   e.g. 0.05% bright to clip outlier hot pixels). The two seed-analysis helpers
   (`ImageImportDialog._raw_auto_level_settings_for_source`,
   `LiveLabTab._raw_auto_level_settings_for_source`) also passed `0.0, 1.0` to
   `compute_auto_level_bounds`, so the Light/Dark readout the widget cached also
   ignored Preferences.
2. **Curve-preview histogram was drawn on the output axis of the curve's input
   axis.** `_refresh_prepare_raw_curve_preview` / the Live Lab equivalent computed
   the histogram from `apply_post_decode_processing_fast(...).rgb` — post-pipeline
   values in [0, 1] — but then plotted them against `curve.input_values` (WB-applied
   pre-levels luminance). Bins landed in the wrong X position and the "clipped"
   red-tint comparison (`bin_center < dark_bound`) crossed spaces silently. Fixed
   by computing histograms on `compute_pre_levels_working_rgb(rgb, settings)`, a
   new helper that runs only the post-decode WB stage.
3. **Auto Levels toggle didn't preserve pixels.** The pipeline recomputes bounds
   live from percentiles when `auto_levels=True` and zeroes `light_ev`/`dark_ev`;
   when `auto_levels=False` it uses the slider `light_ev`/`dark_ev` through
   `apply_light_dark_levels`. Because the slider values weren't kept in sync with
   the pipeline's actual bounds, toggling Auto off caused a visible image jump.
   Fixed by adding `RawProcessingControls.sync_from_live_bounds(black, white)`,
   called by both callers after every `apply_post_decode_processing_fast` render,
   that converts the used bounds to `light_ev = -log2(white)`,
   `dark_ev = log2(1 - black)` (mirroring `apply_auto_level_bounds_to_settings`)
   and pushes them into the sliders under `QSignalBlocker`. Because
   `apply_light_dark_levels` collapses to the same `(x-black)/(white-black)`
   formula as `hard_luminance_levels`, toggling Auto off then reproduces the same
   pixels within LUT/quantisation tolerance (`< 4e-3` max per-pixel diff for the
   1000-step slider grid — see `test_auto_off_freezes_image_after_slider_sync`).

The pipeline zeroing block in `apply_post_decode_processing_fast` was **not**
removed — the sliders are display-only when Auto is on, and the pipeline stays
the source of truth for the actual applied stretch. The invariant is that after
`sync_from_live_bounds` runs, the sliders' EV values reconstruct the exact same
transform the pipeline just applied.

**Layout change.** The Auto Levels checkable QPushButton at the trailing edge of
the Light row was replaced by a labeled two-option pill (`AutoLevelsToggle`) on
its own row. The adapter exposes `isChecked/setChecked/toggled/setProperty("mixed")`
so every existing call site (`QSignalBlocker(...)`, mixed-state tri-flag,
hint-registration) keeps working; a `setProperty("mixed", True)` temporarily
flips the inner `QButtonGroup` to non-exclusive so both buttons can be visually
deselected.

**Cache key.** The curve-preview histogram cache on `_RawPreviewCacheEntry` now
keys on `(white_balance_mode, wb_sample_base_mode, wb_multipliers)` rather than
gains alone — Camera WB and Auto WB both leave `wb_multipliers=None` but rawpy
decodes them differently, so a gains-only key served the wrong histogram back
across WB-mode switches.

**Preferences live-refresh.** `_save_raw_processing_preferences` in `main_window`
now calls `LiveLabTab.refresh_raw_processing_preferences()` (public wrapper for
the private cutoff push) and iterates `QApplication.topLevelWidgets()` to notify
any open non-modal `ImageImportDialog`. Without this, changing bright/dark
cutoffs in Preferences while a panel was open only took effect on the next
panel rebuild.

### macOS cursor crash — avoid bitmap-fallback `Qt.CursorShape`

Repeated hard crashes were observed during Measure and in the species plate dialog on
macOS 26 (Tahoe) with PySide6 / Qt 6.11. The crash always looked the same:

```
QWidget.setCursor(...)
  → QWindowPrivate::setCursor
  → QCocoaCursor::convertCursor
  → QImage::toCGImage
  → CGImageCreate → verify_image_parameters → valid_image_colorspace
  → CGColorSpaceGetType → __CF_IS_OBJC → EXC_BREAKPOINT
```

Cause: only a subset of `Qt.CursorShape` values map to a native `NSCursor` on macOS.
Everything else (`SizeAllCursor`, `SizeFDiagCursor`, `SizeBDiagCursor`, `SizeVerCursor`,
`SizeHorCursor`, `SplitVCursor`, `SplitHCursor`, `DragCopy/Move/LinkCursor`,
`WhatsThisCursor`, `BusyCursor`) falls back to a Qt-embedded bitmap that is pushed
through `QImage::toCGImage`. In Qt 6.11 / macOS 26 that path hands CoreGraphics a
garbage colorspace pointer and traps.

Rule of thumb: on macOS, only use these cursor shapes in `setCursor(...)`:

- `Qt.ArrowCursor`
- `Qt.CrossCursor`
- `Qt.IBeamCursor`
- `Qt.PointingHandCursor`
- `Qt.OpenHandCursor` / `Qt.ClosedHandCursor`
- `Qt.WaitCursor`
- `Qt.ForbiddenCursor`

Anything else risks the same crash. If a resize/move affordance is wanted, prefer
setting a status/hint via `HintController` rather than a specialty cursor.

Fix locations (2026-07): [ui/zoomable_image_widget.py](ui/zoomable_image_widget.py)
(`_crop_corner_cursor` returns `None`), [ui/spore_preview_widget.py](ui/spore_preview_widget.py)
(hover-inside-rectangle uses `Qt.ArrowCursor`), [ui/species_plate_dialog.py](ui/species_plate_dialog.py)
(resize/border hover uses `Qt.ArrowCursor`), [ui/hint_status.py](ui/hint_status.py)
(`_apply_hint_affordance` always calls `unsetCursor()`).

Also worth noting: passing `Qt.CursorShape` directly to `setCursor(...)` is fine and
avoids constructing a PySide-owned `QCursor` wrapper, but it does **not** dodge the
crash on its own — the shape has to be a native one.

### Calibration history cloud indicator

The Calibration History table now shows a leading cloud-status column so cloud-synced calibrations
are visible at a glance. I also tightened the image-date column a bit and rounded MP to whole
numbers to keep the table compact.

### Stage I closeout

Stage I is now done. The desktop ships the conservative settings/status surface for full-resolution
original sync without adding canonical restore/promotion.

Covered changes:
- Exposed `sync_full_resolution_originals` as an opt-in `Sync full-resolution originals` checkbox in
  Preferences → `Profile & Cloud` and kept it off by default.
- Kept the help text explicit: eligible field and microscope originals can be uploaded for recovery,
  it uses more cloud storage, and it never replaces local originals.
- Appended concise original-upload status lines only when original sync is active enough to matter.
- Left full-resolution recovery as a cache/sidecar action only; restore-to-canonical remains deferred.

### Optional full-resolution original sync recovery slice

Stage I now has a helper-only recovery path. The desktop can download eligible cloud originals into
`app_data_dir()/cloud_cache/originals/...` as sidecar cache copies when
`sync_full_resolution_originals` is enabled, without mutating the canonical local image row or
replacing a better local original.

Covered changes:
- Added `recover_full_original_for_image(...)` in `utils/cloud_sync.py` with cache-path and
  sidecar helpers.
- Kept `sync_full_resolution_originals` as the opt-in gate and reused `should_download_full_original(...)`
  for the recovery decision.
- Wrote recovered originals into a separate cache tree and preserved their remote key in a local
  sidecar record.
- Kept recovery idempotent: existing readable cache files are reused rather than redownloaded.
- Added focused tests for disabled-state no-op, missing-key skips, overwrite protection,
  cache-writing success, failure cleanup, idempotency, and snapshot-backed lookup.
- No restore-to-canonical UI or overwrite action was added yet.

### Optional full-resolution original sync upload slice

Stage I has also moved from policy-only into an opt-in upload slice. The desktop can upload
eligible full-resolution originals as companion cloud objects when
`sync_full_resolution_originals` is enabled, and the local original stays authoritative.

Covered changes:
- Added `utils/original_sync_policy.py` with upload eligibility, source selection, size checks,
  and safe future recovery helpers.
- Kept `sync_full_resolution_originals` default-off and wired it into the desktop upload path.
- Added opt-in original uploads for `local_canonical` and `converted_local` rows with `field` or
  `microscope` purposes.
- Prefer `original_filepath` for converted-local lineage when readable, otherwise fall back to the
  readable working `filepath`.
- Enforced a 250 MiB upload ceiling and surface oversize originals as sync warnings instead of hard
  failures.
- Added nullable cloud contract support for `original_storage_path` and kept it passive metadata
  only.
- Added focused tests for eligibility, source selection, upload success/failure, and oversized
  source skipping.
- Kept the UI surface intentionally narrow: a single cloud sync checkbox, not a broad bulk-original
  manager.

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
