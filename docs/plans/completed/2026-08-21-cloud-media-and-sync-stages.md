# Completed cloud-media and sync plans

Status: Completed

Completed: 2026-08-21

Completed work collected from the former root `PLAN.md`. Stage labels below belong only to this historical cloud-media/sync plan. Git history is authoritative for the implementation.

Relevant commits include `919b3e7` for E1c Stage 4 and the implementation history recorded in each section.

# Stage E1c — Cloud sync metadata and image reconciliation audit

Status: Completed. Correctness stages 1–3 and cleanup Stage 4 are implemented.

Scope: fix correctness and convergence defects found in the desktop cloud-sync audit. Keep the work staged and narrow; do not refactor the whole `utils/cloud_sync.py` module as part of these fixes.

#### Stage 1 — Prevent acknowledged-but-unapplied remote changes (Done)

- [x] Separate image metadata reconciliation from image-byte materialization. `materialize_remote_images=False` still applies remote image metadata to existing local rows.
- [x] Do not advance stored remote image/measurement snapshots past skipped or failed local work.
- [x] Keep newly pulled observations retryable until required metadata imports complete.
- [x] Return image-import failures and missing-storage warnings to `pull_all`.
- [x] Preserve retry state for partially imported observations.
- [x] Reuse the authenticated `SporelyCloudClient` during new-observation image import.

Regression coverage:

- background/metadata-only sync applies image metadata without downloading bytes;
- a later materializing sync still detects any intentionally deferred bytes;
- one failed image in a newly pulled observation leaves retryable sync state and is reported;
- supplied sync client works even when no independently stored client can be loaded;
- snapshots are not advanced past failed or skipped reconciliation work.

#### Stage 2 — Make child-table changes visible to fast sync (Done)

- [x] Document the parent-timestamp limitation for child-table changes.
- [x] Preserve fast pull while adding a periodic child-safety reconciliation watermark.
- [x] Cover image metadata, tombstones, metadata-only microscope rows, and measurement-only edits.
- [x] Keep unchanged observations on the no-op fast path.

Regression coverage:

- web image metadata edit is detected by fast sync;
- web image deletion is detected by fast sync;
- metadata-only microscope image edit is detected by fast sync;
- measurement-only edit is detected by fast sync;
- unchanged observations still use the no-op fast path.

#### Stage 3 — Complete image metadata parity (Done)

- [x] Apply `sample_source` when creating or updating metadata-only microscope anchors.
- [x] Include `sample_source` in local image snapshots and conflict comparisons.
- [x] Keep field-image import behavior consistent for new and existing observations.
- [x] Align fetched image fields with the supported push/pull contract.

Regression coverage:

- existing metadata-only microscope anchor round-trips `sample_source`;
- conflict detail does not report a false remote-only `sample_source` change;
- the same field image imported through new-observation and existing-observation paths receives equivalent metadata.

#### Stage 4 — Remove obsolete and duplicate sync code (Done)

- [x] Remove or prove external use of the unreferenced internal helpers found by the audit: `_direct_r2_unavailable_warning`, `_is_direct_r2_unavailable_error`, `_client_uses_default_r2_loader`, `_baseline_measurement_compare_payload`, `_has_pending_local_push_work`, `_find_local_observation_for_remote`, `_remote_observation_changed_since_last_sync`, and `_set_observation_plan_image_blocked`.
- [x] Remove the unreachable `_prompt_for_deleted_cloud_observations` copy from `utils/cloud_sync.py`; the active UI implementations live in `ui/cloud_sync_dialog.py` and `ui/observations_tab.py`.
- [x] Re-check stale cloud-contract comments while touching these sections, without unrelated restructuring.

---

# Stage E1b — Image tombstone sync cleanup

Status: Done. Unchecking a selected field or microscope image now queues a tombstone, and the
global tombstone queue is flushed even when no observation is otherwise dirty. R2 object purging
remains separate Stage E3 work.

The gallery checkbox is the desired cloud state. Its shared transition queues deletion from
`UPLOADED`, cancels an unsynced `DELETE_PENDING` tombstone on recheck, and uses image-specific
explicit restore from `DELETED`. Badges show actual state only: normal for `UPLOADED`, delete
pending for `DELETE_PENDING`, and none for `NONE`/`DELETED`.

- Treat `public.observation_images.deleted_at` as the deletion source of truth.
- Cloud image tombstones must sync to desktop without opening the conflict dialog when image identity is clear.
- Web-deleted images must create/update local tombstones and block reupload.
- Desktop-deleted images must set cloud `deleted_at`.
- Do not delete local files, local measurements, annotations, or R2 objects in this stage.
- Do not classify a matched cloud tombstone as both “cloud removed” and “desktop-only copy.”
- Keep bucket objects as retained cloud derivatives until media garbage collection is designed.

---

# Stage E2 — Cloud media: per-image storage-intent ledger + anchor promotion

Status: Done. (2026-08-19 incident fix.)

Cloud byte-storage intent is now recorded per image in
`sporely_cloud_image_storage_intent_ids_<obs>`; only ledger membership
proves a decision exists — absence from the excluded set proves nothing.
The observation-level init sentinel is retired (stale sentinels let
late-imported microscope frames masquerade as explicitly checked and
mass-upload). `_ensure_cloud_image_storage_intent_initialized` seeds
defaults incrementally (tombstoned → excluded; field → desired; new
members of an initialized magnification group → excluded, never a
silent keeper; genuinely new/legacy groups → one deterministic keeper,
byte-backed members never excluded by default), performs zero cloud
I/O, and runs before the pending-media dirty scan (uninitialized rows
are never "pending"; fail closed). Explicit checkbox choices write the
ledger and are never reseeded.

Separately, a linked metadata-only anchor (valid `cloud_id`, remote
`storage_path` NULL) whose bytes become desired is promoted on its
existing row: the Worker key is reserved via an owner-scoped
conditional PATCH (`storage_path=is.null`), guarded by a local pending
marker written before the PATCH; upload failure rolls back partial
objects and conditionally releases the key
(`storage_path=eq.<exact key>`); a reserved-but-unconfirmed key is
never trusted as proof of bytes. Both new client writers are
pull-only-blocked.

Tests: `tests/test_cloud_storage_intent_ledger.py`,
`tests/test_cloud_anchor_promotion.py`.

Residual risks (documented, deliberately not fixed now):

- Cross-device reservation adoption: a device that holds a stale pending
  marker can adopt another device's completed same-key promotion as its
  own reservation on resume. If that device's re-upload then fails,
  rollback deletes the other device's real R2 objects. Self-heals on
  eventual successful re-upload. Candidate fix: check remote
  `upload_meta` (or an object HEAD) before adopting a reservation.
- Dangling reserved keys self-heal only on the device that still holds
  the pending marker. If that device never syncs again the key stays
  reserved on the row. Candidate fix: detect and surface in
  `tools/audit_cloud_media_health.py`.
- `cloud_image_id` is interpolated unencoded into PATCH URLs in the
  reserve/release helpers. This matches every other writer in
  `SporelyCloudClient` (server-issued UUIDs, RLS-authoritative); worth
  revisiting only if we ever accept non-UUID identity elsewhere.

---

# Stage J — Public spore mosaic: metadata-only microscope image sync

Status: Done. Metadata-only microscope anchors and normal-sync backfill are implemented. Retain the
design below as constraints for the shipped behavior.

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

---

# Stage K — Sync-time mosaic signature (skip unnecessary rebuilds)

Status: Done. The local mosaic signature and remote-row presence guard are implemented. Retain the
design below as constraints for future pipeline-version changes.

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
