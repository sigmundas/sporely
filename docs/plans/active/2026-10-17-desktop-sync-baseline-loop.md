# Desktop sync: image baseline wipe and unreviewable conflicts (0.9.26 data-loss fix)

Branch `fix/desktop-sync-baseline-loop` from released desktop `main` `47068ba` (0.9.26). Owner-approved 2026-10-17 ("do data-loss fix as you suggest"). One bounded stage. No server, migration, worker or web change.

## Evidence

The owner's first syncs with 0.9.26 (2026-10-17) showed the following (summary in the coordinator record; there is no database access for agents):

- **obs 10, 12 and 19 loop.** Every pull marks them dirty with `reasons=['local_media_changed']`, and re-downloads and re-materializes cloud media (38 downloads in the first sync). Every push then blocks them with `conflict push blocked … categories=['images'] action=review_required`. The UI completion path never opens the conflict dialog, and no other UI entry opens it.
- **obs 604, image 3258.**
  - Sync 1: `legacy recovery: image 3258 not linked (matches=1); uploading as a new row`, then `ImageBaselineMissingError … (cloud 7752) differs … ['scale_microns_per_pixel']`.
  - Sync 2: `ChildAdoptionConflictError: … its cloud copy 7752 (same sync key) differs from the local row; needs review`.
  - The dialog never opens for it.
- **Background.** On 2026-10-16 an obsolete desktop (`0520afd`) PATCHed image metadata with NULL upload fields on about 14 observations.

The read-only diagnosis below came from the coordinator's Opus planner at `47068ba`. Code paths are verified; which fields actually differ is inferred.

1. **The baseline is wiped on every pull.**
   - When `local_media_changed` is true, the stored media signature is not refreshed (`utils/cloud_sync.py:11839-11840`).
   - `store_full_snapshot` is false (`11841`), and `_store_remote_snapshot(..., include_images=False)` (`11905-11909`) writes no images and no measurements at all (`utils/cloud_sync_impl/baseline.py:389-390, 411-412, 144`).
   - With an empty image baseline, every cloud image is classified "added" (`utils/cloud_sync_impl/reconciliation/images.py:82,98`). The pull then re-applies every cloud image over the linked local rows: metadata (`utils/cloud_sync_impl/image_pull.py:687-710`) and microscope working files (`664-684`). This is the data-loss path.
2. **The signature goes stale after a push.** The push refreshes the media signature (`cloud_sync.py:10513`) before `_store_post_push_snapshot` (`10659`) runs `_adopt_remote_image_edits_before_baseline` (`image_push.py:543-596`), which can rewrite local image columns.
3. **The two classifiers diverge.**
   - The push gate treats a linked image with no baseline row as a conflict when any field of `_image_metadata_payload` differs exactly (`utils/cloud_sync_impl/preflight.py:252-263`; field set `reconciliation/values.py:183-201`).
   - The dialog gate (`conflict_detail.py:982-993, 1019-1022`) compares 12 fields with a float tolerance.
   - `finalize_sync_candidates` then drops a candidate that has neither automatic nor manual work silently, without clearing the review marker (`cloud_sync.py:4507-4510`).
   - A difference with `change_origin='baseline_unavailable'` (`conflict_detail.py:1055-1056`) is treated as automatic (`1171-1183`) and planned as cloud-wins (`conflict_plan.py:94`).
4. **`ChildAdoptionConflictError` is never routed to review.** It falls into the generic error branch (`cloud_sync.py:10771-10782`), and its text matches none of the conflict patterns (`utils/cloud_sync_impl/errors.py:17-29`).
5. Minor: the conflict summary sets `pull_skipped=True` for every review error (`progress.py:494-505`), even though the pull ran.

Re-verify each point in the code before changing it. Report any point that turns out wrong instead of fixing a non-problem.

## Stage 1 — Preserve the image baseline and make every blocked conflict reviewable

### Required changes

1. **Never wipe the baseline.** When a snapshot is stored with `include_images=False` and/or `include_measurements=False`, keep the previous snapshot's images and measurements instead of writing none. A pull that keeps `local_media_changed` true must leave the image baseline intact. Then the next pull does not classify every cloud image as "added", and does not re-apply cloud images over linked local rows.
2. **Refresh the signature after the post-push snapshot.** Refresh the media signature after `_store_post_push_snapshot` (including remote-edit adoption), not before. A successful push followed by a pull must not set `local_media_changed`.
3. **One conflict classification.**
   - The dialog and finalize gate must classify linked images with no baseline using the same field set and comparison as the push gate. Share one helper; don't duplicate field lists.
   - A difference with no baseline (`baseline_unavailable`) is **manual**, never automatic cloud-wins.
   - `finalize_sync_candidates` must never drop silently a candidate the push gate blocked as review-pending. If it finds neither automatic nor manual work, it returns a visible error, and the candidate stays reviewable or its marker is cleared consistently.
   - No path may apply cloud values over local values for a no-baseline image without a manual decision.
4. **Route `ChildAdoptionConflictError` to review**, the same way `ObservationAdoptionConflictError` is routed: format it as a review-needed error and set the observation's conflict-review-pending state, so it reaches the dialog.
5. Optional, only if it is trivial: report `pull_skipped` truthfully.

### Constraints

- Desktop only. Do not change server contracts, the sync-key or brake headers, the write-stop behaviour, migrations or the web repository.
- Do not change accepted adoption semantics (sync-key adoption, legacy recovery) beyond routing its conflicts to review.
- Keep the fix minimal and inside the existing modules: `utils/cloud_sync.py` and `utils/cloud_sync_impl/{baseline,conflict_detail,conflict_plan,preflight,errors,image_push,progress}.py`, plus tests. Follow the import-direction rules (`tests/test_cloud_sync_impl_import_direction.py`) and the reconciliation purity rules (`tools/cloud_sync_reconciliation_purity.py`).
- No real user database, and no network or production access. Tests use fixtures and fakes only.

### Regression tests (each must fail without its fix)

- **T1.** A stored full snapshot, a stale stored media signature, and a newer cloud row. After `pull_all`, the snapshot still has its images and measurements, and a second pull does not re-dirty the observation or re-apply cloud images over linked local rows.
- **T2.** A successful push that adopts a remote image edit in `_store_post_push_snapshot`. The next pull's `local_media_changed` is false.
- **T3.** A linked image with no baseline that differs only in a field outside the old 12-field dialog set (for example `calibration_uuid` or `captured_at`). The push gate and `get_conflict_detail` agree, and `finalize_sync_candidates` returns it as manual.
- **T4.** A no-baseline image that differs in one of the 12 fields. It is manual and is never planned or applied as cloud-wins automatically.
- **T5.** `finalize_sync_candidates` given a review-pending candidate with no work returns an error and does not drop it silently.
- **T6.** A `ChildAdoptionConflictError` (same sync key, content mismatch) produces a review-pending conflict entry that the dialog path receives.

Record in the stage notes which test fails without which fix.

### Verification

- The new tests, plus the neighbours:
  - `tests/test_cloud_conflict_dialog.py`
  - `tests/test_cloud_conflict_plan_execution.py`
  - `tests/test_cloud_sync_conflict_preflight.py`
  - `tests/test_observation_snapshot_persistence.py`
  - `tests/test_cloud_sync_no_baseline_identity_contradiction.py`
  - `tests/test_image_conflict_normalization.py`
  - `tests/test_cloud_media_pull_retry.py`
  - `tests/test_cloud_sync_impl_import_direction.py`
  - the reconciliation purity test

  The baseline at `47068ba` (coordinator): the first seven files give 204 passed.
- Then the cloud-sync test suite once: `tests/test_cloud_sync*.py tests/test_cloud_*.py`. Report the counts, and compare any failure against `47068ba`.
- Run with `/Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/python -m pytest -q -p no:cacheprovider` and `QT_QPA_PLATFORM=offscreen`.

### Acceptance criteria

- No pull can leave an observation with an empty image or measurement baseline while local-change flags remain.
- A linked observation that has converged stays converged across repeated syncs: no re-dirtying and no repeated re-materialization.
- Every candidate the push gate blocks as review-required either reaches the conflict dialog or produces a visible error. None is dropped silently.
- No automatic cloud-wins without a baseline.
- All six regression tests exist and are shown to fail without their fixes. The neighbour and cloud-sync suites show no new failures compared with `47068ba`.

### Review instructions for the sparring reviewer

Review the diff against `47068ba`. Verify the diagnosis points in the code yourself; don't rely on this brief. Concentrate on data-loss paths:
- any remaining path that writes a snapshot without images or measurements;
- any remaining automatic cloud-wins;
- consistency between the push gate and the dialog gate;
- regressions in sync-key adoption and legacy recovery.

Spot-check that T1, T3 and T6 fail without their fixes.

### Deferred and out of scope

- **Deferred human check, by the owner after merge:** a desktop release (0.9.27) and a re-sync of the owner's installation. Obs 10, 12 and 19 should then converge or open the dialog, and obs 604 should open the dialog. Not part of this stage.
- **Repairing values that previous loops may have overwritten** on obs 10, 12 and 19: the owner compares against the pre-upgrade backup `AM-K121C767FM-20261017T115756Z`. That's a separate, manual decision.
- **Spore-mosaic plan-cap failure,** the find-detail system-camera button, and stale thumbnails after a retry (INBOX).
