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
