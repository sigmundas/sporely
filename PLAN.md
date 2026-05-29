# Sporely Development Plan

This file tracks current implementation priorities. Detailed design decisions belong in `docs/supabase-sync-contract.md`; completed work belongs in `HISTORY.md`.

## Current Focus — Desktop ↔ Cloud Sync Foundation

Goal: make `sporely-py`, `sporely-web`, Supabase, and R2 agree on image/calibration identity, deletion state, and file provenance before adding deeper recovery, multi-asset sync, or full-resolution cloud storage.

### Stage A — sporely-py local calibration UUID

Status: Done.

- Added `calibration_uuid` to local SQLite calibrations.
- Backfilled existing rows.
- Generated UUIDs for new calibrations.
- Preserved UUIDs in export/import.
- Validated UUIDs as canonical UUID text.

### Stage B — Supabase calibration UUID

Status: Done.

- Added `calibration_uuid uuid` to `public.calibrations`.
- Backfilled existing cloud rows.
- Added default `gen_random_uuid()`.
- Set `NOT NULL`.
- Added uniqueness on `(user_id, calibration_uuid)`.
- Did not add `desktop_id`.

### Stage C — Metadata-only calibration sync

Status: Done.

- Sync calibration metadata by `calibration_uuid`.
- Do not match by objective/date.
- Do not silently overwrite same-UUID conflicts.
- Keep local `image_filepath` out of cloud payloads.
- Keep cloud `image_storage_path` out of local canonical paths.

### Stage D1 — Calibration photo/reference-image design

Status: Done.

- Representative asset rule decided.
- Local original vs cloud derivative rules decided.
- Recovery/cache semantics deferred.

### Stage D2 — Representative calibration derivative sync

Status: Done.

- Upload one web-friendly derivative/reference image per calibration.
- Prefer `image_filepath`, then first readable `measurements_json.images[].path`.
- Store relative cloud key in `public.calibrations.image_storage_path`.
- Do not upload full-resolution originals.
- Do not write cloud paths into local `image_filepath` or `measurements_json`.
- Metadata sync still works when photo is missing.

### Stage E1 — Image tombstone deletion model

### Stage E1b — Image tombstone sync cleanup

Status: in progress.

- Treat `public.observation_images.deleted_at` as the deletion source of truth.
- Cloud image tombstones must sync to desktop without opening the conflict dialog when image identity is clear.
- Web-deleted images must create/update local tombstones and block reupload.
- Desktop-deleted images must set cloud `deleted_at`.
- Do not delete local files, local measurements, annotations, or R2 objects in this stage.
- Do not classify a matched cloud tombstone as both “cloud removed” and “desktop-only copy.”
- Keep bucket objects as retained cloud derivatives until media garbage collection is designed.

### Stage E2 — Image provenance/source tags

Status: Next.

Purpose: define explicit provenance roles so the app does not confuse import sources, local working files, cloud derivatives, cloud recovery/cache files, and generated artifacts.

Planned slices:

- E2a: document provenance vocabulary and rules.
- E2b: add local-only image provenance columns.
- E2c: tag new imports/conversions.
- E2d: tag cloud recovery/cache files.
- E2e: define generated artifact/spore crop model.
- E2f: optional cloud provenance fields.

Initial local fields under consideration:

- `source_role`
- `file_purpose`
- `original_mime_type`
- `working_mime_type`

Accepted vocabulary draft:

`source_role`:
- `import_source`
- `local_canonical`
- `converted_local`
- `cloud_derivative`
- `cloud_recovery_cache`
- `generated_artifact`

`file_purpose`:
- `field`
- `microscope`
- `calibration`
- `reference`
- `plot`
- `thumbnail`
- `spore_crop`
- `cache`

Important rules:

- HEIC is an import source.
- `sporely-py` may convert HEIC to JPEG/PNG for local work.
- `converted_local` can still be analysis-authoritative when it is the durable working copy.
- Cloud WebP/JPEG files are derivatives/cache, not scientific originals.
- Generated artifacts are vocabulary-only for now; implementation may need a later artifact table/model.

Deferred:
- cloud provenance fields
- full-resolution original sync
- generated artifact table
- multi-asset calibration provenance

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

Status: Not started.

- Download cloud calibration derivative to cache/recovery when local photo is missing.
- Mark as cloud-derived.
- Do not overwrite local originals.
- Do not write recovery paths into canonical local provenance fields unless explicitly designed.

### Stage G — Image-calibration linkage/reconciliation

Status: Not started.

- Link synced calibration records to images/calibration_id safely.
- Reconcile scale fields, objective names, and `calibration_uuid`.
- Avoid automatic rescaling unless conflicts are clear.

### Stage H — Multi-asset calibration provenance

Status: Not started.

- Add a dedicated `calibration_assets`-style model/table if needed.
- Support multiple calibration photos, crops, overlays, role labels, hashes, derived artifacts, and provenance.
- Do not overload `public.calibrations` with many path columns.

### Stage I — Optional full-resolution original sync

Status: Not started.

- Only after provenance, quotas, and user settings are clear.
- Never replace better local originals with cloud copies.

---

## UI backlog
PASS: desktop blocks login/sync with account B when the local DB is already linked to account A.
PASS: no cross-account sync should occur.
TODO/UI: Reset Cloud Sync is referenced in the error text, but no visible Reset Cloud Sync tool exists.
TODO/UI: “Unable to save cloud login” is misleading; this is an account-link protection error, not really a credential-save failure.

Add a real Reset Cloud Sync / Reset Cloud Link tool, or remove that instruction from the account-mismatch message until the tool exists.


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
- [ ] Replace generated-media heuristics with explicit provenance tags after E2.

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
