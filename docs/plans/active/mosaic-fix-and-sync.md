# Cloud media completeness fast-path repair

Branch: `feature/mosaic-publication-selection-fix`

This is a follow-up cloud-sync repair discovered while validating the completed mosaic publication-selection work. Do not modify or reopen the completed publication-selection stages.

## Core invariant

A local media/render signature describes whether local render inputs changed.

It does **not** prove that required cloud image identities or bytes exist.

Before an image-preparation fast path is taken during an explicit `sync_images=True` run, cloud-upload completeness must be established from the canonical per-image storage-intent and cloud-link state.

Do not add `cloud_id` or cloud-storage intent to the local render signature.

---

## Stage 1 — Prove the stranded-media state and close the fast-path hole

### Goal

Make a dirty observation with unchanged local render inputs but genuinely pending selected cloud media enter full image preparation instead of the signature fast path.

### Investigation before editing

Trace these existing paths and reuse them rather than introducing another policy:

* `_ensure_cloud_image_storage_intent_initialized`
* `_pending_cloud_pushable_image_ids`
* `explain_pending_cloud_image_decision`
* `_local_cloud_image_media_signature`
* `_local_media_prep_diagnostics`
* the `image_render_unchanged`, `tombstone_cleanup_only`, and `metadata_only_image_sync` branches in `push_all`
* `_push_images_for_observation`

Also trace `_ensure_metadata_anchors_for_public_spore_observation` and answer explicitly:

> For a 917-shaped public observation, why would measured microscope images still have `cloud_id=NULL` after the measurement fast path runs?

Do not assume missing microscope bytes explain missing measurement rows; metadata-only microscope anchors are specifically designed to allow measurements without those bytes.

### Implementation

Only when `sync_images=True` and an existing cloud observation is being considered for image synchronization:

1. Ensure per-image cloud-storage intent is initialized before interpreting desiredness.
2. Compute the canonical pending cloud-pushable image ids once for the observation.
3. Define image-upload completeness separately from render-signature equality.
4. Permit `image_render_unchanged`, tombstone-only, and metadata-only image-prep fast paths only when no genuinely pending cloud image remains.
5. If pending ids exist, use the existing full image-preparation/upload path.

Do not alter `sync_images=False` Refresh/background behavior.

Do not add another pending-image predicate or another source of desired-state truth.

If the metadata-anchor investigation reveals a separate correctness defect, report it before broadening this stage. Fix it here only if it is tightly coupled and can be demonstrated by the same regression; otherwise create a later stage.

### Required regressions

* Matching stored/current render signature + desired initialized image + `cloud_id=NULL` → full image prep runs.
* Same state with no pending images → existing skip path remains.
* Uninitialized storage intent is initialized **before** pending completeness is evaluated.
* An intentionally excluded microscope image does not force full prep.
* Missing-file, duplicate/cache and other intentionally non-pushable rows do not create a dirty loop.
* `sync_images=False` remains unchanged.

### Verification

Run the focused cloud-media tests plus:

* `tests/test_cloud_sync_image_upload_policy.py`
* `tests/test_cloud_storage_intent_ledger.py`
* `tests/test_cloud_sync_fast_path.py`
* `tests/test_cloud_sync_dirty_loop_steady_state.py`

Use the repository's configured sporely-py virtual environment.

---

## Stage 2 — Recover already-synced stranded observations

### Goal

Make existing observations such as the reported 604-shaped case discoverable once after the corrected upload path ships, without adding a permanent broad scan.

### Implementation

Reuse `_mark_cloud_observations_dirty_for_pending_local_images`.

Do not add a second dirty scanner and do not enable pending-media scanning for ordinary `sync_images=False` Refresh/background sync.

Advance the existing versioned pending-image repair generation so installations that already recorded the current repair watermark perform one new scan on their next explicit `sync_images=True` synchronization.

Keep the existing periodic/throttled repair semantics after that one-time generation transition.

### Required regressions

* A `synced` observation with initialized, desired, uploadable `cloud_id=NULL` media is re-dirtied by the new repair generation.
* Once its upload succeeds, a subsequent scan finds no pending media and does not re-dirty it.
* Excluded/local-only microscope images do not cause repeated dirtying.
* `sync_images=False` does not run this recovery.
* Existing repair watermark/throttling behavior remains intact after the generation migration.

---

## Stage 3 — Prove the full image → measurement → mosaic chain and record the invariant

### Goal

Demonstrate that the repaired state converges completely rather than merely uploading image bytes.

### Tests

Add an integration-style regression for a 917-shaped observation:

* existing cloud observation;
* matching local render signature;
* desired microscope image with `cloud_id=NULL`;
* eligible local spore measurements;
* explicit media sync.

Prove that the run reaches the intended cloud-image identity state, measurement synchronization becomes eligible, and the mosaic pusher receives eligible cloud-linked measurements.

Separately pin the metadata-anchor contract:

* an unselected microscope image with public eligible measurements may retain no cloud image bytes;
* its metadata-only cloud image anchor can still support measurement synchronization;
* byte-selection state and measurement/mosaic participation remain independent.

This test must make it impossible to “fix” the incident later by requiring every measured microscope source image to upload its bytes.

### Documentation

Update the cloud-sync contract with the invariant:

> Local media signatures describe local input/render state. They never prove remote upload completeness. Required cloud-media work is determined from per-image storage intent and cloud/link state.

Update cloud-sync architecture documentation only if ownership/control-flow documentation needs to change.

Keep the corresponding shared sync-contract copy aligned according to the repository rules.

### Final review

Have the Sparrer challenge specifically:

* storage-intent initialization ordering;
* `sync_images=False` isolation;
* dirty-loop termination;
* duplicate upload/reassociation behavior;
* metadata-only microscope anchors;
* preservation of the no-op fast path.

Live Supabase mutation is human-gated. Automated tests should be sufficient for the stage commit; afterward, verify the reported observations with a deliberate Sync Now and a read-only before/after audit.
