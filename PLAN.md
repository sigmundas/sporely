# Sporely Development Plan

This file tracks current implementation priorities. Detailed design decisions belong in `docs/supabase-sync-contract.md`; completed work belongs in `HISTORY.md`.



## Active project — Spore orientation

Implement orientation-aware spore measurements in `sporely-py`.

The feature has exactly two user-facing orientation values:

- Face
- Side

Do not add an “Unspecified” option anywhere in the UI.

This is not a true 3D measurement tool. Each rectangle still measures two dimensions, `length × width`. The orientation determines what the transverse dimension means:

- Face: length × breadth in face view
- Side: length × width/thickness in side view

Do not attempt to pair face and side measurements as dimensions of the same physical spore.

Work in three strict phases:

1. Database and data model
2. Measure interface
3. Analysis/plot interface, including reference data

Finish and test each phase before starting the next. Do not redesign unrelated parts of the application.

## Phase 1 — database and data model

First inspect the current local database schema, cloud schema, migrations, measurement models, sync payloads, import/export paths and tests. Identify every path through which measurements and reference values are stored or synchronized.

### Individual measurements

Add a language-neutral field to spore measurement records:

```text
spore_view = "face" | "side"
```

Requirements:

* New spore measurements must always be saved as either `face` or `side`.
* Non-spore measurements should not use this field.
* Store stable internal values `face` and `side`; do not store translated labels.
* Include the field in all relevant local/cloud serialization, sync, comparison, merge, export and import paths.
* Changing the view of a measurement must count as a real measurement change and trigger any necessary statistics or summary refresh.
* Make sync backward-compatible with older rows and older clients as far as practical.

Do not guess the orientation of existing measurements.

For migration safety, the database column may remain nullable only for pre-existing legacy rows. This is an internal legacy condition, not a third application state:

* Never display “Unspecified.”
* Never offer NULL as a selectable value.
* Never create a new spore measurement with NULL.
* Legacy NULL rows must not silently be treated as Face or Side.
* Preserve all existing measurements without destructive backfilling.

Add an appropriate database constraint, for example permitting only:

```text
NULL
face
side
```

where NULL is accepted solely for legacy compatibility.

Update local and cloud migrations where applicable.

### Reference-data model

Extend reference data so one reference dataset can hold:

```text
Length
Face breadth
Side width
Q face
Q side
```

The statistical fields should follow the existing reference-value structure, including whichever of these the application currently supports:

```text
extreme minimum
typical minimum
mean
typical maximum
extreme maximum
```

Use stable internal dimension identifiers such as:

```text
length
face_width
side_width
q_face
q_side
```

The exact schema should follow the existing architecture. Do not create a parallel storage system if the current reference model can be extended cleanly.

Preserve:

* the original literature string;
* the source;
* existing ordinary two-dimensional reference records;
* backwards compatibility.

Do not automatically classify old two-dimensional reference widths as Face or Side. They may remain internally legacy/unclassified until edited or explicitly assigned, but “Unspecified” must not become a UI option.

Add migration and model tests covering:

* a legacy database opening without data loss;
* Face measurement round-trip;
* Side measurement round-trip;
* local/cloud serialization;
* sync in both directions;
* editing only the orientation;
* legacy NULL rows;
* rejection of invalid orientation strings.

Stop after Phase 1 if its tests do not pass.

## Phase 2 — Measure interface

Use the existing Measure screen shown in the supplied screenshot.

Do not add anything to the crowded left-hand Measure panel.

### Primary control

Add a compact segmented control to the right-hand `SPORES FINE TUNE` panel for the currently selected spore measurement:

```text
VIEW
[ Face ] [ Side ]
```

Requirements:

* Exactly two choices.
* No Unspecified choice.
* Use localized display labels, but store `face` and `side`.
* Changing the selected value updates the measurement immediately and safely.
* The control appears only for spore rectangle measurements.
* It should not affect Line, Multi-line or non-spore measurements.

### Creating a measurement

The first time the user draws a spore rectangle and no active orientation has yet been chosen:

* Show a small contextual chooser near the newly drawn rectangle:
  `[ Face ] [ Side ]`
* Do not permanently save the new measurement until one has been chosen.
* Keep the rectangle selected while waiting.
* Escape or cancellation should discard the incomplete rectangle cleanly.

After a choice is made:

* Save the measurement with that view.
* Remember the most recently selected view for the current measuring session.
* Subsequent spore rectangles inherit that view automatically.
* The user can change the active orientation before drawing more spores using the Fine Tune control or keyboard shortcut.

Add keyboard shortcuts while measuring spores:

```text
F = Face
S = Side
```

Do not trigger shortcuts while the user is typing in a text field.

### Canvas and table display

Avoid permanent clutter on the microscopy image.

Show an `F` or `S` indicator only where useful:

* on the selected rectangle;
* while hovering a rectangle;
* in the measurements table.

Add a compact `VIEW` column to the measurements table for spore rectangles:

```text
IMG   CAT      VIEW   L     W
4     Spores   F      4.6   4.0
4     Spores   S      4.9   4.3
```

Do not attach persistent Face/Side pills to every rectangle.

Legacy measurements with no stored view:

* must remain visible;
* must not be relabelled automatically;
* should show a neutral legacy marker such as `—` in the table rather than the word “Unspecified”;
* should require choosing Face or Side before orientation-aware editing or plotting.

Make sure changing Face to Side or Side to Face updates all derived statistics and dirty/sync state.

Add tests for:

* first rectangle requiring a choice;
* sticky orientation for subsequent measurements;
* F and S shortcuts;
* cancelling before selection;
* changing orientation in Fine Tune;
* table rendering;
* selected/hover badges;
* legacy rows;
* no new NULL measurements;
* no user-facing “Unspecified” label.

Stop after Phase 2 if its tests do not pass.

## Phase 3 — Analysis and plot interface

Update the Analysis screen shown in the supplied screenshot.

This phase includes:

* observation measurements;
* plots;
* histograms;
* Q statistics;
* gallery;
* statistics text/export;
* literature reference values;
* reference parsing and editing.

### Plot orientation selector

For the Spores category, add a compact selector near the main plot controls:

```text
View: [ Face ] [ Side ]
```

There must be exactly two plot modes:

* Face
* Side

Do not add:

* Unspecified
* All
* Both
* a combined orientation mode
* a 3D plot

Default behavior:

* Prefer the last selected analysis view.
* Otherwise select Face if Face data exists.
* Otherwise select Side.
* If the selected orientation has no data, show a clear empty state rather than silently falling back or mixing data.

### Face plot

Face mode uses only Face measurements:

```text
X = Length (µm)
Y = Breadth — face view (µm)
Qf = Length / face breadth
```

The plot, confidence ellipse, Width histogram and Q histogram must all be calculated only from Face measurements.

Use labels such as:

```text
Breadth — face view (µm)
Qf (L/Bf)
```

### Side plot

Side mode uses only Side measurements:

```text
X = Length (µm)
Y = Width — side view (µm)
Qs = Length / side width
```

The plot, confidence ellipse, Width histogram and Q histogram must all be calculated only from Side measurements.

Use labels such as:

```text
Width — side view (µm)
Qs (L/Ws)
```

Never pool Face and Side transverse dimensions into one ellipse, histogram or Q distribution.

Legacy measurements without a view:

* must not be included in either plot;
* must not be silently assigned;
* should produce a small warning such as:
  `Some legacy measurements need a Face or Side assignment.`
* do not use the word “Unspecified” as a selectable state.

### Gallery

In the spore gallery, show a small orientation badge:

```text
F  12.4 × 8.2
S  12.6 × 7.1
```

The gallery should follow the selected Analysis view, showing only Face or only Side measurements.

Do not add another orientation filter if the main Face/Side plot selector already controls it.

### Dataset and reference overlays

Keep the existing dataset/source color system.

The selected Face/Side view determines which measurements and which reference dimensions are plotted.

For a three-dimensional reference such as:

```text
11.5–14.5 × 5.5–9.5 × 5.5–8 µm
```

interpret it as:

```text
Length × face breadth × side width
```

Therefore:

* Face plot uses the reference Length and Face breadth ranges.
* Side plot uses the same reference Length and Side width ranges.
* Face mode uses Q face values.
* Side mode uses Q side values.

Do not show both reference rectangles simultaneously.

A reference dataset should remain one item in the dataset list. Switching Face/Side changes the dimensions used for its envelope.

Legacy two-dimensional reference data without an assigned orientation must not be silently shown in either mode. Indicate that the reference needs a Face or Side assignment.

### Reference editor

Extend the existing `Edit selected reference data` dialog without making it substantially more cluttered.

For references containing both orientations, expose two compact sections or a Face/Side segmented selector inside the existing Min/max and Spore data workflows:

```text
[ Face ] [ Side ]
```

Face fields:

```text
Length
Breadth — face
Qf
```

Side fields:

```text
Length
Width — side
Qs
```

Length is conceptually shared for three-dimensional literature strings. Avoid creating contradictory duplicate length values unless the source explicitly provides orientation-specific length statistics.

For a simple two-dimensional reference string, require the user to classify it as Face or Side before saving orientation-aware data. Offer only:

```text
[ Face ] [ Side ]
```

No Unspecified option.

### Literature parser

Extend the current measurement-string parser.

It must parse:

```text
11.5–14.5 × 5.5–9.5 × 5.5–8 µm
11.5–14.5 × 5.5–9.5 (f) × 5.5–8 µm (s)
11.5–14.5 × 5.5–9.5 (face) × 5.5–8 (side)
```

For an unmarked three-dimension string, show a visible confirmation message:

```text
Interpreted as length × face breadth × side width.
```

Recognize common aliases case-insensitively.

Face aliases should include at least:

```text
f
face
face view
front
frontal
frontal view
frontalansicht
frontansicht
framifrån
forfra
vue de face
aspectu frontali
```

Side aliases should include at least:

```text
s
side
side view
profile
profile view
lateral
seitenansicht
från sidan
fra siden
vue de profil
vue latérale
aspectu laterali
```

Store normalized values only as `face` and `side`.

Preserve the original pasted string exactly.

### Q handling

Keep Face and Side Q statistics separate:

```text
Qf = L / face breadth
Qs = L / side width
```

The parser should support literature that provides:

```text
Q
Qm
Qf
Qs
Q1
Q2
```

Do not assume naming conventions are globally consistent.

When only `Q` or `Qm` is present in a three-dimensional string:

* compare the supplied value with the parsed dimension means/ranges where possible;
* suggest whether it appears to refer to Face or Side;
* show the interpretation to the user;
* allow correction before saving;
* do not silently assign when the basis is ambiguous.

### Statistics and export

Statistics must remain separate by orientation.

The detailed export should be able to produce sections such as:

```text
Face view, n = 30
L = …
Bf = …
Qf = …

Side view, n = 18
L = …
Ws = …
Qs = …
```

The compact combined taxonomic output may use:

```text
Basidiospores 11.5–14.5 × 5.5–9.5 (f) × 5.5–8.0 (s) µm
```

Do not report a single pooled Width, Q or confidence ellipse across both orientations.

### Tests

Add or update tests for:

* Face plot filtering;
* Side plot filtering;
* switching plot mode;
* separate confidence ellipses;
* separate width histograms;
* separate Qf and Qs statistics;
* gallery filtering and badges;
* three-dimension parser;
* explicit `(f)` and `(s)` parsing;
* multilingual aliases;
* unmarked three-dimension confirmation;
* reference Face envelope;
* reference Side envelope;
* simple two-dimensional reference requiring Face or Side;
* legacy reference behavior;
* statistics export;
* no combined or pooled mode;
* no user-facing Unspecified option.

## General constraints

* Do not add a third orientation state to the UI.
* Do not rename the existing generic measurement `width_um` unless a broader migration is genuinely required.
* Do not implement a 3D scatter plot or estimated spore volume.
* Do not pair Face and Side records as one physical specimen.
* Do not clutter the left Measure panel.
* Do not mix Face and Side values in calculations.
* Do not destructively classify legacy data.
* Keep keyboard navigation and high-DPI rendering working.
* Follow the existing visual components, spacing, fonts and localization conventions.
* Avoid unrelated refactoring.

Run the complete relevant test suite after all three phases.

At the end, report:

* schema and migration decisions;
* legacy-data handling;
* every changed file;
* tests added or changed;
* commands run and results;
* any unresolved edge cases;
* screenshots of the Measure and Analysis interfaces showing both Face and Side states.



## Bugs
Scale bar does not show up on microscope images published to iNaturalist.
Spore statistics should be English for iNaturalist; replace "Sporer" with "Spores".
In the Analysis tab, Orient and Uniform scale should be on by default.
Consider a non-modal pending-cloud-media indicator on close; do not add a blocking reminder dialog.


### Stage E1c — Cloud sync metadata and image reconciliation audit

Status: correctness stages 1–3 implemented; cleanup stage 4 remains.

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

#### Stage 4 — Remove obsolete and duplicate sync code

- [ ] Remove or prove external use of the unreferenced internal helpers found by the audit: `_direct_r2_unavailable_warning`, `_is_direct_r2_unavailable_error`, `_client_uses_default_r2_loader`, `_baseline_measurement_compare_payload`, `_has_pending_local_push_work`, `_find_local_observation_for_remote`, `_remote_observation_changed_since_last_sync`, and `_set_observation_plan_image_blocked`.
- [ ] Remove the unreachable `_prompt_for_deleted_cloud_observations` copy from `utils/cloud_sync.py`; the active UI implementations live in `ui/cloud_sync_dialog.py` and `ui/observations_tab.py`.
- [ ] Re-check stale cloud-contract comments while touching these sections, without unrelated restructuring.


### Stage E1b — Image tombstone sync cleanup

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



### Stage E2 — Cloud media: per-image storage-intent ledger + anchor promotion

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

### Stage J — Public spore mosaic: metadata-only microscope image sync

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

### Stage K — Sync-time mosaic signature (skip unnecessary rebuilds)

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
- [ ] Replace remaining generated-media heuristics with explicit provenance tags in a dedicated artifact-model stage.

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
