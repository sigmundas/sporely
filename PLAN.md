# Sporely Development Plan

This file tracks current implementation priorities. Detailed design decisions belong in `docs/supabase-sync-contract.md`; completed work belongs in `HISTORY.md`.

## Bugs
Scale bar does not show up on microscope images, published to inaturalist
Spore stats: language should be english for inatrualist. Replace Sporer with Spores
The thumbnail strip: when selecting a thumbnail near the end of the strip, and there are more thumbs outside, the selected thumb should center. Right now it bounces out of view.
in Analysis tab: Orient and Uniform scale should be on by default. 
Sync-handling: I added a bunch of microscope photos, measured, then closed the app. The images did not sync. A dialog should perhaps pop up here to remind people to sync? I hate dialogs though, so if this can be avoided I'm all ears.

Sync to cloud should allways recode to webp. I got this error:
obs 389: Image is too large for your plan. Make it smaller or upgrade to Pro.

Observation: Panaeolina foenisecii (ID 389)
Image: 20260603-173447.jpg (microscope) (ID 869)
Original file: /Users/sigmundas/Library/Application Support/Sporely/images/Panaeolina/foenisecii - 2026-06-02T18-12-47+00-00 (2)/20260603-173447.jpg
Original size: 4.6 MB
Original dimensions: 5184 × 3888 px
Prepared upload size: 1.5 MB
Prepared dimensions: 5184 × 3888 px
Plan cap: 4.8 MB
Upload mode: full / high

Note that this is from a pro account, so I should not have seen this. Anyway, upload should ahve been webp.

I openend the app again, and microscope images still don't sync. I dunno if the error above blocks all syncs? I have a button for download missing cloud media, pressing that does not upload microscope images either..


### Stage E1b — Image tombstone sync cleanup

Status: in progress.

- Treat `public.observation_images.deleted_at` as the deletion source of truth.
- Cloud image tombstones must sync to desktop without opening the conflict dialog when image identity is clear.
- Web-deleted images must create/update local tombstones and block reupload.
- Desktop-deleted images must set cloud `deleted_at`.
- Do not delete local files, local measurements, annotations, or R2 objects in this stage.
- Do not classify a matched cloud tombstone as both “cloud removed” and “desktop-only copy.”
- Keep bucket objects as retained cloud derivatives until media garbage collection is designed.



### Stage E3 — Cloud media garbage collection

Status: deferred.

Purpose: clean up R2 objects for tombstoned image rows after sync identity and provenance are stable.

Planned policy:

- Single-image delete immediately sets `observation_images.deleted_at`.
- R2 objects are retained during a recovery/undo/sync-safety window.
- A later cleanup job purges R2 `storage_path` and generated variants for tombstoned rows older than the retention period.
- Add `storage_purged_at` before automatic purging so missing media can be distinguished from intentionally purged media.
- Do not delete `observation_images` rows when purging bucket objects; keep tombstone identity for sync/reupload blocking.
- Do not purge full-resolution originals unless full-original sync is explicitly implemented and the user chose permanent deletion.

### AI identification retention

Status: deferred cleanup.

- Current behavior may retain historical/stale AI identification runs.
- UI should only replay rows matching the current active image/crop fingerprint.
- Tombstoned-image AI rows must not be replayed as current suggestions.
- Keep stale rows temporarily for debugging, but add retention cleanup before production:
  - delete stale rows older than 30 days, or
  - keep at most 2–3 stale rows per observation/service.
- Long-term: prefer one current row per `(observation_id, service)` plus optional short-lived debug history.

### Stage F — Calibration photo recovery/download cache

Status: Done.

- Download cloud calibration derivative to cache/recovery when local photo is missing.
- Mark as cloud-derived.
- Do not overwrite local originals.
- Do not write recovery paths into canonical local provenance fields unless explicitly designed.
- Implemented in `utils/cloud_sync.py` and `ui/calibration_dialog.py`, with coverage in `tests/test_cloud_calibration_sync.py` and `tests/test_calibration_reference_recovery_ui.py`.

### Stage G — Image-calibration linkage/reconciliation

Status: Done.

- Use portable `calibration_uuid` in image cloud payloads and snapshots.
- Reconcile local `images.calibration_id` from stored cloud snapshots after calibration sync.
- Keep scale fields and objective names in sync without automatic rescaling.
- Implemented in `utils/cloud_sync.py`, with focused coverage in `tests/test_cloud_image_calibration_linkage.py`.

### Stage H — Multi-asset calibration provenance

Status: Done.

- Added a dedicated local `calibration_assets` model/table for multiple calibration photos, crops,
  overlays, reference-cache rows, and derived artifacts.
- Preserve asset roles, hashes, and provenance without overloading `public.calibrations` with many
  path columns.
- Keep the table desktop-only for now; the cloud contract still uses `calibration_uuid` and
  calibration metadata, not a calibration-asset mirror.

### Stage J — Public spore mosaic: metadata-only microscope image sync

Status: proposed. Depends on a small Supabase schema loosening.

Goal:
Let the public spore mosaic include every public-eligible measurement in
an observation without uploading the underlying microscope frames. The
public visual is the atlas tile, not the 20-megapixel source, so full
image bytes must stay optional.

Diagnostic evidence (observation cloud 745, local id 449):

    total_local=28
    with_p1_p2_p3_p4=28
    image_has_cloud_id=8
    measurement_has_cloud_id=8
    excluded_by_measurement_type=2
    by_image_type={'microscope': 28}
    pusher_would_select=8
    remote_microscope_images=3
    remote_measurements=8
    public_rpc_sporePoints=8

Interpretation: 18 of 26 non-calibration spores live on microscope
images that were never uploaded, so their local `images.cloud_id` is
NULL, so the mosaic pusher's `image.cloud_id IS NOT NULL` gate drops
them. The fix is a metadata-only cloud image row per source frame — no
source bytes.

Findings against current schema:

- `public.observation_images.storage_path` is `text NOT NULL` in the
  baseline. Metadata-only rows are not currently insertable.
- INSERT RLS requires `storage_path LIKE '{auth.uid}/%'`.
- UPDATE RLS already allows `storage_path IS NULL`.
- `search_public_observation_images` derives `thumbUrl` from
  `storage_path`. Metadata-only rows would yield a broken URL, which
  we do not want on the observation image gallery.
- `search_public_species` derives `representativeThumbUrl` the same way.
- `get_public_observation` `sporePoints` join does not reference
  `storage_path` and works fine for metadata-only rows.
- `spore_measurement_mosaic_tiles` RLS only requires
  `mosaic.user_id = auth.uid()` plus the measurement's image belonging
  to the mosaic's observation; no `storage_path` dependency.
- Desktop `push_image_metadata(img, obs_cloud_id, storage_path)`
  currently requires a non-empty `storage_path` argument at the call
  sites.

Required Supabase migration (new, e.g. `20260703…_allow_metadata_only_observation_images.sql`):

1. `ALTER TABLE public.observation_images ALTER COLUMN storage_path DROP NOT NULL`.
2. Replace INSERT RLS: allow `storage_path IS NULL` OR the existing
   `'{auth.uid}/%'` prefix pattern (keep the prefix guard for uploaded
   rows).
3. Update `search_public_observation_images` /
   `search_public_species`: skip rows where `storage_path IS NULL OR
   btrim(storage_path) = ''` before building `thumbUrl` /
   `representativeThumbUrl`. Metadata-only rows must not appear in the
   observation image gallery or the species representative thumb.
4. `get_public_observation` sporePoints: no change; it never references
   `storage_path`.
5. Extend `supabase/tests/public_observation_rpc_validation.sql`:
   insert one row with `storage_path IS NULL` and one with a real
   path; assert the NULL row is excluded from image RPCs but its
   spore measurements still land in `sporePoints`.

Required desktop changes:

- New helper `_ensure_metadata_only_microscope_image(client, local_image_id) -> str | None`
  that upserts a remote `observation_images` row with the following
  fields (mirroring `push_image_metadata` today):

    - `observation_id`      = cloud observation id
    - `user_id`             = `auth.uid()`
    - `desktop_id`          = local image id
    - `image_type`          = `'microscope'`
    - `sort_order`, `micro_category`, `objective_name`,
      `calibration_uuid`, `scale_microns_per_pixel`,
      `resample_scale_factor`, `mount_medium`, `stain`, `sample_type`,
      `contrast`, `measure_color`, `crop_mode`, `notes`,
      `gps_source`, AI-crop fields when present, `original_filename`
    - `storage_path`        = **NULL**
    - `source_width` / `source_height` — populated from PIL when the
      local file is present (needed for later mosaic geometry and any
      display maths); left NULL when not resolvable.
  On success, write `images.cloud_id` locally.
- Modify the backfill / normal-sync spore pipeline: before pushing
  spore measurements for a given local microscope image, if
  `images.cloud_id IS NULL` AND the image carries at least one
  public-eligible measurement AND `spore_data_visibility='public'`
  on the observation, call the new helper. Do NOT call the helper
  for field images or microscope images that have no public spore
  measurements.
- Add `--ensure-image-metadata` (default on) and
  `--no-ensure-image-metadata` flags to
  `python -m utils.cloud_spore_mosaic_backfill`.

Kept as-is:

- Full microscope source upload remains opt-in
  (`sync_full_resolution_originals`) and untouched by this stage.
- Per-measurement `spore_measurements.thumb_key` / `cropUrl` fallback
  on the public RPC remains.
- Mosaic bytes are still built from local source files at
  backfill/sync time.
- Landing observation image gallery must not surface metadata-only
  rows (achieved by the RPC filter above).

Non-goals:

- Do not upload thumbnails for microscope images by default. If we
  later want a small on-image preview for the observation gallery,
  that is a separate opt-in and does not gate the mosaic.
- Do not fake or synthesise `measurement.image_id`.
- Do not bypass RLS.
- Do not remove the existing full-resolution / opt-in upload path.

Expected diagnostic after this stage ships and backfill runs for 745:

    total_local=28
    with_p1_p2_p3_p4=28
    image_has_cloud_id=26        # 28 minus 2 excluded by measurement_type
    measurement_has_cloud_id=26
    excluded_by_measurement_type=2
    pusher_would_select=26
    remote_microscope_images=26  # remote row per local microscope image
    remote_measurements=26
    public_rpc_sporePoints=26

Storage impact per additional metadata-only image row:

- Zero R2 bytes uploaded.
- One `observation_images` row (~200 bytes of metadata).
- One `spore_measurements` row per measurement on that image (existing
  size).
- One `spore_measurement_mosaic_tiles` row per measurement (existing
  size, unchanged by this stage).
- One `spore_measurement_mosaics` row per observation
  (mosaic-per-observation, already exists).

If the schema migration turns out to be non-trivial (e.g. downstream
code discovered later that requires `storage_path`), fall back to
proposing a `bytes_uploaded boolean` flag column instead of dropping
NOT NULL. Do NOT fall back to uploading source images without an
explicit user opt-in.

Rollout order:

1. Land the Supabase migration + RPC tests in `sporely-web`.
2. Land the desktop helper + backfill wiring + unit tests in
   `sporely-py`.
3. Run `python -m utils.cloud_spore_mosaic_backfill --observation-cloud-id 745 --diagnose`.
4. Verify diagnostic + landing display of the 26-tile mosaic before
   sweeping older observations.

### Stage K — Sync-time mosaic signature (skip unnecessary rebuilds)

Status: proposed.

Goal:
Normal sync should push a spore mosaic only when something that
actually affects the mosaic has changed. The full-observation backfill
is ~1.5 s per observation on a mid-range machine, most of which is
Pillow rotation + WebP encode; running it on every sync makes idle
syncs unacceptably slow.

Idea:
Compute a compact per-observation "mosaic signature" locally and
persist it (e.g. `observations.mosaic_signature` TEXT, or a
`spore_mosaic_state` side table). The signature is a stable SHA-1 over
the tuple that determines the mosaic bytes:

- ordered list of eligible measurement ids
- per-measurement (p1/p2/p3/p4, length_um, width_um,
  measurement_type, gallery_rotation, image cloud_id)
- source image mtime + size fingerprint per referenced local file
- `spore_data_visibility`
- desktop mosaic pipeline version constant (bumped when render code
  changes so old signatures re-run once)

Normal sync path
(`_push_spore_mosaic_for_observation` in `utils/cloud_sync.py`):

1. Compute `new_signature` from the local rows.
2. If `new_signature == observations.mosaic_signature` locally AND the
   remote `spore_measurement_mosaics` row is present, skip build +
   upload. Log `Mosaic skip obs N: signature unchanged`.
3. Otherwise, run the existing pipeline and persist `new_signature`
   after successful upload.

The metadata-only microscope image helper (Stage J) is also gated by
this signature: no unlinked microscope images with public-eligible
measurements → nothing to do. The helper itself is cheap so calling
it on every sync is fine; the expensive part is the mosaic rebuild.

Tests:
- signature stable across identical runs
- signature changes when a measurement is added, deleted, its
  geometry changes, or a source file mtime changes
- pipeline version bump forces one rebuild for every observation
- `--force` CLI flag on the backfill bypasses the signature guard for
  manual re-runs

Also:
- Move the ensure-metadata helper into the normal sync path once this
  signature guard is landed, so new measurements on unshared
  microscope images reach the public site without a manual backfill.
- Keep the backfill CLI for pipeline-version bumps and forced
  regeneration; day-to-day, it should be unused.

### Stage L — Anonymized public spore data from private observations

Status: proposed.

Goal:
Let users contribute spore measurements to the community dataset
without exposing the observation itself. Motivating cases: matsutake
sites, rare taxa, and psychoactive species where the finder does not
want the location tied to their name.

The schema already separates `spore_data_visibility` from
`visibility`, so "hidden observation, public measurements" is a
valid combination on the desktop side today — what is missing is a
public RPC that reads it and a landing surface that consumes it.

Model (desktop):

- Keep the existing `observations.visibility` (`private` /
  `friends` / `public`) and `spore_data_visibility`
  (`private` / `public`) as-is.
- The desktop Preferences dialog gets an explicit control: "Share
  spore measurements from private observations anonymously". When
  on, private observations still push measurements + mosaic tiles to
  the cloud through Stage J's metadata-only image path.
- The desktop helper that already creates metadata-only microscope
  image rows is unchanged — it fires when the observation is public
  OR when `spore_data_visibility='public'`, which is already the
  gate we use.

Model (cloud, sporely-web):

- New (or extended) public RPC — e.g.
  `search_public_anonymous_spore_points(taxon_slug, country, ...)` —
  that reads observations where `spore_data_visibility='public'`
  regardless of `visibility`. Projection intentionally strips:
  observation id, observer, GPS, exact date, unshared image URLs.
- Kept: `genus`, `species`, `length_um`, `width_um`, `q`,
  `country_code`, optionally `year_month` (`YYYY-MM`) but only when
  the (species, country, month) cohort has at least N points to
  avoid re-identification of rare taxa; otherwise coarsen to year
  only.
- Mosaic tile access: allow the tile URL + tile rect via a companion
  RPC, but drop `observationId` from the returned row so the tile
  cannot be linked back to the observation. Keep the polygon
  overlay.

Constraints:

- No new columns on `observations` — reuse existing visibility
  fields.
- RLS on `observations`, `observation_images`,
  `spore_measurements`, `spore_measurement_mosaics`, and
  `spore_measurement_mosaic_tiles` must continue to reject direct
  reads of hidden observations by anonymous / stranger roles. The
  new visibility comes only through the RPC, not through the
  underlying tables.
- Landing must not expose observation-level detail pages for
  anonymized points; those clicks land on the species aggregate
  chart instead.

Follow-up questions before implementing:

- Minimum cohort size for month-year vs year-only. Rough starting
  point: month-year only when `count(species, country, month) >= 5`,
  else year, else omit.
- Whether anonymized points should also feed `search_public_species`
  observation counts (probably no — count "publicly shared
  observations" separately from "anonymously shared spore points"
  in the UI).
- UX copy for the opt-in checkbox: "Share only my spore data. Your
  observation stays private; the community sees the measurements
  without any location or identity."

### Stage M — Draft expiry policy

Status: proposed.

Goal:
Give users a soft push toward either publishing an observation or
letting go of it, so long-abandoned drafts stop consuming R2 media
and DB rows. The paid-tier promise is "unlimited slots"; the
free-tier promise is "20 private slots + drafts that get cleaned up
if you never come back to them".

Policy sketch:

- Draft observations that have had no edits and no measurements
  added for D months are candidates for cleanup. Starting point:
  D = 6 months on free tier, D = 12 months on paid.
- Grace period: candidate observations are marked
  `expires_at = now() + 30 days` and the user is emailed once with
  a "keep", "publish now", or "let it go" link. A gentle in-app
  banner appears while `expires_at` is in the future.
- On `expires_at`:
  - Soft-delete the observation via the existing tombstone path
    (`deleted_at`), so a short recovery window applies.
  - R2 media garbage collection (see Stage E3) removes the image
    bytes when the tombstone crosses the media retention window.
- Drafts that flip `spore_data_visibility='public'` (Stage L) are
  exempt: they are contributing to the community dataset and should
  survive the expiry sweep as long as spore data is opted in.

Non-goals:

- Do not hard-delete anything at expiry — always go through the
  existing tombstone + recycle bin flow.
- Do not touch measurements on published observations; expiry is
  scoped to `is_draft = true` rows.
- Do not silently expire drafts without the email/banner grace
  window; the whole point is fair warning.

Rollout order:

1. sporely-web: add `observations.expires_at timestamptz NULL`,
   RPC + Edge Function to identify candidates and set expiry, RLS
   updates so users still see their own expiring drafts.
2. sporely-web: email hook + landing banner (or reuse existing
   notification surface).
3. sporely-py: banner + preferences copy explaining the free-tier
   draft policy; add "keep this draft" one-click action.
4. Enable the sweep in dry-run first (log candidates, no
   expiry set) and audit before flipping the switch.

### Stage I — Optional full-resolution original sync

Status: Done (default-off opt-in upload, recovery cache path, and conservative settings/status surface shipped; explicit restore/promotion remains deferred).

- Added a desktop-only policy helper for full-resolution original eligibility and safe recovery
  decisions.
- Added nullable cloud contract support for `public.observation_images.original_storage_path`.
- The opt-in setting name is `sync_full_resolution_originals`; it stays off by default unless
  explicitly enabled.
- The sync engine now supports opt-in original uploads for eligible rows and enforces an upload
  size guard on the desktop side.
- The Preferences dialog exposes a conservative `Sync full-resolution originals` checkbox in the
  `Profile & Cloud` section with a short warning about storage and local-original safety.
- Sync status stays quiet when the opt-in is off and shows concise original upload counts only when
  original sync is actually active.
- Deferred future work:
  - explicit restore/promotion action if needed
- Never replace better local originals with cloud copies.
- Keep any broader bulk original management UI deferred until a restore/promotion workflow is
  designed and tested.

---

## UI backlog
PASS: desktop blocks login/sync with account B when the local DB is already linked to account A.
PASS: no cross-account sync should occur.
TODO/UI: Reset Cloud Sync is referenced in the error text, but no visible Reset Cloud Sync tool exists.
TODO/UI: “Unable to save cloud login” is misleading; this is an account-link protection error, not really a credential-save failure.
TODO/UI: Add a menu link to Pro info/payment on `sporely.no`; do not embed desktop checkout.

Add a real Reset Cloud Sync / Reset Cloud Link tool, or remove that instruction from the account-mismatch message until the tool exists.

### iNat/artsobs publishing
- The plate layout changes for the upload picture. All bubbles will have images in them, even if I switch them off.
- Scale bar does not show up on publishedi mages



## Taxonomy Lookup / Local Species DB

Status: audit/documentation in progress.

- Current DB rebuilt with iNat IDs and Swedish Artportalen data.
- Document: `docs/taxonomy-lookup-status.md`
- Next tasks:
  - expose iNat/Artportalen IDs through the lookup service if not already exposed
  - verify case-insensitive vernacular dedupe remains in the builder
  - add Artsdatabanken red-list on-demand resolver later
  - verify AI Photo ID result mapping uses local iNat ID before name matching
  - verify desktop/web use the same taxonomy lookup rules

## Active QA / Verification

- [ ] Run live cloud-lock QA with two disposable Sporely Cloud accounts.
- [ ] Verify account mismatch blocking and Reset Cloud Link flow.
- [ ] Verify Profile parity between desktop and web:
  - `username`
  - `display_name`
  - `bio`
  - `avatar_url`
  - `profile_email`
- [ ] Add export coverage test:
  - observations/images/measurements/calibrations/reference data and image files included
  - `app_settings.json` and full profile state intentionally excluded
- [ ] Verify local DB values are prioritized over file EXIF in Prepare Images and Measure tab Info box.
- [ ] Fix cloud-synced image warning overlay in Prepare Images dialog.

---

## Active Testing Backlog

- [ ] Introduce Ruff.
- [ ] Consider mypy only after the codebase is stable enough for useful annotations.
- [ ] Broaden pytest coverage around:
  - cloud sync conflict resolution
  - local media signatures
  - image crop math
  - `utils/r2_storage.py`
  - SQLite migrations
  - `database/models.py`
- [ ] Test metadata auto-merge.
- [ ] Test true conflict dialog triggers.
- [ ] Update old “cloud deletion conflict” tests to reflect tombstone behavior.

---

## Image Handling Backlog

- [ ] Fix Android-imported JPG portrait rotation in thumbnails / Measure tab.
- [ ] Define HEIC import behavior clearly:
  - HEIC as import source
  - JPEG/PNG as local working/canonical file
  - cloud derivative generated from best available decoded pixels when practical
- [ ] Replace generated-media heuristics with explicit provenance tags in Stage H or a dedicated artifact-model stage.

---

## AI Photo ID / AI Crop Backlog

Status: review before acting; some earlier items may already be done.

- [ ] Verify Supabase has current AI crop fields on `public.observation_images`.
- [ ] Verify crop sync between web and desktop.
- [ ] Verify Artsorakel/iNaturalist result persistence and dropdown behavior.
- [ ] Verify Review, Import Review, and Find Detail all use the same AI Photo ID state model.
- [ ] Confirm AI crop is used only for AI requests, not gallery display or R2 originals.

Non-goals:
- Do not crop R2 originals.
- Do not make gallery display depend on AI crop.
- Do not add a separate AI crop table unless the current model breaks.

---

## Web / Infrastructure Backlog

- [ ] Deploy Worker secrets and route.
- [ ] Configure:
  - `SUPABASE_URL`
  - optional JWT issuer/audience overrides
  - `MEDIA_PUBLIC_BASE_URL`
  - `sporely-media` R2 binding
- [ ] Add offline queue for upload failures in field conditions.
- [ ] Re-check whether old R2 migration notes are obsolete after the Supabase baseline reset.
- [ ] Optional cloud summary RPC/view for observation/image change summaries.

---

## UI Backlog

### General UI

- [ ] Fix table highlight artifacts in AI suggestions and Observations table.
- [ ] Use the same clean selection style as the Measurements table.
- [ ] Make room for text on measure-type radio buttons.
- [ ] Consider renaming “Reference shape” to “Shape”.

### Camera Import / Ingestion

- [ ] Rename “Intestion tab” to “Camera import”.
- [ ] Rename “Sync shot” to “Camera time offset”.
- [ ] Rename “Microscope sessions” to “Live lab sessions”.
- [ ] Reorder groups:
  - Import folder
  - Camera time offset
  - Live lab sessions
  - Actions
- [ ] Update hint text for Camera Import buttons.
- [ ] Add richer manual reassignment tools for unmatched images.

### Measure / Analysis

- [ ] Implement fine-tune for multi-line segments.
- [ ] Add hint bar at bottom of Measure tab.
- [ ] Implement Cmd/Ctrl-click additive selection in Analysis tab.
- [ ] Implement histogram additive selection.

### Galleries

- [ ] Make thumbnail gallery height user-adjustable.
- [ ] Prevent cropped/hidden thumbnails in Prepare Images dialog.
- [ ] Allow thumbnails to shrink to around 100 px.

---

## Web-Native Analysis — app.sporely.no

Status: future.

- [ ] Responsive Plotly.js L × W scatter plots.
- [ ] Outlier verification UI linked to thumbnails.
- [ ] Mobile/desktop analysis layouts.
- [ ] Public dataset explorer.
- [ ] Taxon summaries.
- [ ] Reference-entry UI for literature statistics.
- [ ] In-browser measurement using Canvas.
- [ ] Pyodide integration for shared Python/Numpy logic.

---

## Community Spore Data

Status: active but secondary to sync foundation.

- [ ] Return QC metadata in RPC responses.
- [ ] Add stronger visual distinction for cloud-origin imported sources in the reference panel.
- [ ] Implement public reference dataset model before publishing comparison plots broadly.

---

## Design System Migration — Slate Lab / Clinical Nocturne

Status: ongoing.

- [ ] Apply surface/typography/component patterns to:
  - `ui/live_lab_tab.py`
  - `ui/ingestion_hub_tab.py`
  - `ui/calibration_dialog.py`
  - remaining dialogs
- [ ] Consolidate remaining inline `setStyleSheet()` calls into `styles.py`.

---

## Privacy, Social Feeds, and Costs

Status: paused / verify before continuing.

- [ ] Verify whether old Phase 7 SQL notes are obsolete after the Supabase baseline reset.
- [ ] Verify live RLS/feed behavior:
  - owner
  - accepted friend
  - stranger
  - blocked user
  - banned profile
  - non-public limit paths
- [ ] Strip GPS EXIF from public media serving path.
- [ ] Implement iNaturalist export with `sporely.no` deep link.
- [ ] Implement Bluesky share-card generator.
