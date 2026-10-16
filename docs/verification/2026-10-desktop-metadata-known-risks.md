# Desktop metadata preservation: accepted behaviour and remaining inherited risks (2026-10)

Status: decided by the plan owner on 2026-10-16.

The accepted candidate is `846cbd1` on `feature/sync-key-desktop-metadata`. That is `9e71526`, reviewed systematically in `identity-impl/codex-meta-review-7.txt`, plus the isolated dialog correction, approved in `identity-impl/codex-dialog-review-1.txt`. It is integrated as a strict improvement over accepted desktop `3850b3f`. The owner explicitly accepts that some inherited silent-overwrite risks remain, given the limited desktop-cloud usage. The owner is the only desktop cloud-sync user.

**This is not a claim that synchronization is free of data-loss risk.**

## What the correction guarantees

All of this was verified by unit tests on real push and pull code, by cross-client runs on a disposable stack (`g4-evidence/runs/meta-*`), and by independent Codex reviews (`identity-impl/codex-meta-review-*.txt`).

- A desktop edit never sends upload metadata it did not produce: `upload_mode`, `source_*`, `stored_*`, `stored_bytes`.
- Blank local text never replaces a cloud NULL. A deliberate clear is still sent.
- Through `push_image_metadata`, every PATCH of an existing image row keeps every field that only the cloud changed since the stored baseline. Those cloud edits are copied into the local row before the baseline is stored. A field changed on both sides is a conflict.
- Every column the image push sends is in exactly one class:
  - three-way;
  - fill-only (`original_filename`);
  - upload-owned;
  - structural.

  A guard test enforces this.
- Capture times keep full precision. A capture time the pull injected is not treated as a user edit.
- On the drift, Refresh and push paths, an existing linked image with no stored baseline, whose local and cloud values differ, is refused and sent to conflict review. No baseline is stored for it.
  - This does **not** hold on two paths; see the accepted limitations below.
- The Edit Observation dialog does not change a field image's `objective_name` when the user edits another field. A deliberate microscope objective edit is saved. Changing a microscope image to a field image clears it. Tests: `tests/test_observation_dialog_objective.py`; live run `meta-g10-*` (no `objective_name` write; that run also stopped as a whole-image false conflict, which fails safe).

## Remaining inherited risks

These were enumerated by the systematic review of `9e71526` (`identity-impl/codex-meta-review-7.txt`, section B). They are grouped by mechanism. Most of them exist in `3850b3f` and in every released desktop.

### Silent overwrites (data can change without a conflict)

Accepted explicitly for this release: the no-baseline refusal is bypassed on two paths (`identity-impl/codex-meta-review-7.txt`, section A).

| Path | What happens | Where |
|---|---|---|
| Prepared-upload loop | An owned image with changed bytes and no snapshot: the loop absorbs `ImageBaselineMissingError` and a baseline is still stored. The next push treats stale local values as edits. | `utils/cloud_sync_impl/image_push.py:1559`; `utils/cloud_sync.py:10659` |
| Automatic conflict finalization | Unrelated observation operations are applied and unresolved images are snapshotted. The review can disappear and stale values are later pushed. | `utils/cloud_sync.py:4484,6065,4507` |

Other inherited mechanisms:

| Mechanism | Example | Where |
|---|---|---|
| Derived values pushed as edits | `species_guess` is recomputed from genus/species, which overwrites web identification text. A web genus change makes the desktop write a guess the web never asked for. | `utils/taxon_text.py:107,125`; `utils/cloud_sync.py:3598` |
| UI saves that manufacture edits (other than `objective_name`) | Saving re-applies sort order and metadata, and can select the active calibration or a derived scale | `ui/observations_tab.py:14174,14197` |
| Pulls that ignore web clears | A NULL scale, objective, stain, calibration or capture time from the web is not applied locally, so a later unrelated push restores the old value | `database/models.py:3194`; `utils/cloud_sync_impl/image_pull.py:365,369` |
| Representation normalization | NULL flags become false; legacy sample values are rewritten; local-time conversion can shift times around a daylight-saving change | `utils/cloud_sync_impl/image_push.py:501`; `sample_source.py:139`; `reconciliation/values.py:314` |
| Baselines advanced outside the adoption step | Identity-review finalization and media materialization store image baselines without adopting cloud edits | `utils/cloud_sync.py:10647,12823` |
| Writers outside the protected path | Full observation PATCHes; automatic plan field PATCHes; plan statistics recomputation | `utils/cloud_sync.py:7614,5404,5926` |
| Cover side effects | A metadata push of local image zero can reset a cover the web selected | `utils/cloud_sync.py:7955,6602` |
| Upload ownership asserted without an upload | Metadata pushes force `storage_exif_safe=True` | `utils/cloud_sync.py:7866` |
| Recovery and deletion replay | Missing-measurement reconciliation can resurrect web-deleted images; pending tombstones can replay a deletion over a web restoration; upload rollback can clear a key written concurrently | `measurement_reconcile.py:233`; `tombstones.py:225`; `image_push.py:1527` |
| Legacy capture-time guessing | For copies pulled before provenance was recorded, changing the observation datetime can make an injected time look user-edited | `utils/cloud_sync_impl/exif.py:435` |
| Gallery ordering | Mixed recovery galleries are renumbered; plan downloads take local ordering; order is not part of the image conflict check | `image_pull.py:206`; `cloud_sync.py:5077`; `reconciliation/images.py:63` |
| Read-then-write races | Ordinary image and observation PATCHes, and the location and taxonomy RPCs, are not conditional on the version they read | `utils/cloud_sync.py:7919,7614,2183`; `identity_push.py:93,185` |

Released desktops also overwrite unsynced local observation fields on pull when local ids collide (F1). That failure is recorded and accepted separately (`docs/verification/2026-10-sync-key-n1-two-desktop.md`).

### False conflicts that stop safely (no data changes)

- **The image conflict check compares whole images, not fields** (`utils/cloud_sync_impl/preflight.py:247`). When a desktop-owned dialog default (`crop_mode`, `measure_color`) changes alongside a web edit to a different field of the same image, the observation stops in conflict review.
- **A missing baseline is now refused and sent to review.**
- **No-op media-key `updated_at` PATCHes** waste a sync cycle but change no data.

## Failed attempt: no-baseline refusal state (rejected, kept as evidence)

`65a7d9c` tried to close the two bypasses with a persisted per-image refusal record. It is archived locally as the branch `archive/no-baseline-refusal-attempt-65a7d9c` and is not integrated. The independent review (`identity-impl/codex-meta-review-8.txt`) found that it:

- left an unresolvable review: automatic adoption removed the difference but kept the refusal, so the dialog had nothing to apply;
- did not gate byte uploads with the record;
- missed a listing-failure route in prepared uploads;
- cleared every refusal before a resolution was validated;
- saved image and observation records separately, not atomically;
- had no lifecycle for deletion, relinking or account reset.

That gives two new defect classes: safety state that is duplicated and saved non-atomically, and refusal enforcement whose lifecycle drifts from the data it protects. The plan owner rejected it and chose a structural redesign instead.

## Future project: structural redesign (not part of this release)

The underlying weakness: the desktop infers "the user edited this" by comparing its stored local row with the baseline. That row also holds derived, defaulted, normalized and unconverged values. The redesign should:

1. **Record edit intent.** Record the fields the user actually edited or cleared, at edit time, separately from hydration, defaults and derivation.
2. **Enforce in common.** Use that intent on every write path, through one enforcement point.
3. **Make writes conditional.** Use version-conditional writes for images, observations and RPCs.
4. **Keep baselines reliable, with atomic refusal state.** Never advance a baseline for a field that failed or is unresolved. Hold refusal and conflict state atomically with the data it protects, with a defined lifecycle for deletion, relinking, reset and resolution.
5. **Make ownership explicit.** Bytes, deletion, cover and derived fields need explicit ownership.
6. **Keep it tested.** A standing cross-client suite, parameterized over fields, must show that editing one field never changes another on any client.

The redesign should be planned as its own project, with a cross-client test suite written before the code changes.
