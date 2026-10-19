# Development inbox

Quick observations captured while using Sporely.
Items here are not approved implementation plans.
Add a note under the nearest heading, or use “Unsorted / needs review” when its home is unclear.

## Cloud sync / recovery
- Value-identical image metadata PATCHes: syncs of the identity backfill logged "metadata patch … source bytes unchanged" on observations whose stored image baselines did not change (456, 460, 462, 502, 505, 442), followed by a child-change pull. The contract forbids no-op cloud writes (they advance the child-change cursor). Find which field the PATCH sends and skip it when the cloud already matches.
- The two copies of `docs/supabase-sync-contract.md` (sporely-py and sporely-web `main`) differ by about 600 lines that predate 2026-10-19 (for example the personal-copy provenance paragraph and the pulled-identity paragraph). Reconcile them in one docs change.
- Sync summary counters mislead: "touched N dirty observation(s) with no real change" ignores identity RPCs, and `measurement push … pushed=N` counts unchanged rows.
- Old v1/v2 spore-mosaic rows (and storage objects) remain next to the current v4 row per observation. Decide whether to clean them up.

- Add a real Reset Cloud Sync / Reset Cloud Link tool, or remove that instruction from the account-mismatch message until the tool exists.
- Replace “Unable to save cloud login” for account-link protection failures with wording that identifies the account mismatch.
- Consider a non-modal pending-cloud-media indicator on close; do not add a blocking reminder dialog.
- Run live cloud-lock QA with two disposable Sporely Cloud accounts; verify account-mismatch blocking and the Reset Cloud Link flow.
- Verify Profile parity between desktop and web for `username`, `display_name`, `bio`, `avatar_url`, and `profile_email`.
- Review E2 anchor-promotion residual risks before changing that pipeline: cross-device reservation adoption, dangling reserved keys, and URL encoding assumptions.
- Decide whether to repair the known `G_conflicting_intent` row and historical duplicate observation/image records outside the extraction plan.
- Harden standalone migration tooling if it becomes necessary; it is explicitly outside the cloud-sync extraction plan.
- Design a safe fix for the observation-deletion ordering and original-media coverage noted in `docs/cloud-media-incident-audit.md`.
- Reference attach/detach should mark the observation as having unsynced changes via a lightweight row-level pending state (not full `dirty`: no image re-upload, prep, mosaic rebuild or measurement re-push). Clears only when the reference-use outbox work succeeds. Also consider telling the user that edits saved mid-sync upload next sync. Removed from the cloud-sync refactor plan (old 6.5k, 2026-10-03); plan it after the refactor's Stage 4 coordinator lands.
- Spore mosaic after conflict resolution: the mosaic is skipped when local and cloud differ in any way the user accepted. Narrow that so only microscope-relevant differences block it.
- Spore mosaic after conflict resolution: build the mosaic even when local and cloud differ, using only measurements whose images match on both sides.
- Spore mosaic after conflict resolution: generate a mosaic for observations that only came in by import (cloud pull), not just ones pushed from this device.
- Spore mosaic size: the mosaic is encoded once as WebP (quality 82) with every spore as a tile, with no size loop and no tile cap. Large mosaics exceed the plan cap: on 2026-10-17, obs 11 (302 tiles of 262×320) failed with "Image too large for plan", and the failure was not listed among the sync issues. Make the mosaic fit the plan cap the way the web app does: step down tile resolution and/or quality until it fits, and consider capping the tile count. Also report the failure as a sync issue. The alternative, raising the worker cap for mosaic keys, costs storage, and every worker deploy now needs the brake checks.
- Follow-ups from the first 0.9.26 syncs (2026-10-17):
  - Obs 604, image 3258: the cloud row 7752 (created by legacy-recovery as a "possible duplicate") carries 3258's sync key, but differs in `scale_microns_per_pixel`, which gives a stable `ChildAdoptionConflictError`. Decide the correct scale in conflict review, then clean up the duplicate cloud row.
  - **Defect, obs 10, 12 and 19 (non-converging conflict with no way to review it):** every pull re-marks them dirty (`local_media_changed`, right after remote-media materialization), and every push blocks them with `conflict push blocked … categories=['images'] action=review_required`. But no conflict dialog opens: `_show_cloud_conflict_dialog` (`ui/observations_tab.py:4044`) runs `finalize_sync_candidates`, which re-evaluates them and returns no manual entries, so the dialog is skipped, and there is no other way to open it. The push-time conflict check and the final gate disagree. **Not just frozen; there is a data-loss risk** (read-only diagnosis 2026-10-17; code paths verified, which fields actually differ is inferred):
    - when `local_media_changed` is set, the pull writes the snapshot with `include_images=False`, which stores no images or measurements (`baseline.py:389-412`; `cloud_sync.py:11839-11909`). The image baseline is wiped on every pull, so every cloud image looks "added" and is re-applied over the linked local rows: image metadata (`image_pull.py:687-710`) and microscope working files (`664-684`);
    - the dialog gate compares 12 fields with a tolerance (`conflict_detail.py:982-1022`), but the push gate compares the full snapshot field set (`preflight.py:252-263`). `finalize_sync_candidates` drops candidates silently (`cloud_sync.py:4507-4510`), and a `baseline_unavailable` difference is treated as automatic and applied as cloud-wins (`conflict_detail.py:1055-1183`, `conflict_plan.py:94`);
    - the media signature is refreshed before `_store_post_push_snapshot` adopts remote image edits (`cloud_sync.py:10513` vs `10659`);
    - `ChildAdoptionConflictError` (obs 604) is never routed to review (`cloud_sync.py:10771-10782`, `errors.py:17-29`).
    Proposed fixes: keep the previous images and measurements when a snapshot is written without them; refresh the signature after the post-push snapshot; align the dialog fields and comparison with the push gate, treat `baseline_unavailable` as manual, and make finalize error on a review-pending candidate that has no work; route `ChildAdoptionConflictError` to review. Regression tests for each. Until fixed: do not sync these observations; keep the pre-upgrade backup. Investigate why materialization changes the local media signature, and why the push gate and `finalize_sync_candidates` classify the same candidates differently. Seen on 0.9.26 (47068ba), 2026-10-17.
  - Obs 372 (image PATCHes interrupted by the legacy `0520afd` sync on 2026-10-16): make a trivial edit to mark it dirty, sync, and check whether image prep reports `changed_keys` (the backfill edits are then pushed) or none (the cloud is already complete).
  - Storage clean-up from the pre-upgrade reconciliation report: C = 10 images marked not uploaded that still hold cloud bytes, and F = 84 soft-deleted cloud rows whose bytes remain. Separate decision; nothing is deleted automatically.

Source: former `PLAN.md`, the active cloud-sync extraction plan, the cloud-media incident audit, and the deferred items from `completed/2026-09-07-conflict-resolution-spore-mosaic.md`.

## Images / galleries / publishing

- In Prepare Images, editing a raw image and then zooming can revert the preview to the old, unedited JPEG.
- Prepare Images thumbnails do not update after edits.
- Returning to Live Lab after editing a “from raw” image leaves the main preview stale until another image is selected.
- Add multi-select auto white balance, calculated independently for each selected image.
- Apply one custom/picked white balance to all selected images.
- Hide the “stain” pill on field images in Measure/observation main-image surfaces.
- Fix missing scale bars on microscope images published to iNaturalist/Artsobservasjoner.
- Ensure the publication plate respects disabled image bubbles.
- Fix Android-imported JPEG portrait rotation in thumbnails and Measure.
- Define HEIC behavior: HEIC as import source, JPEG/PNG as local working form, and cloud derivatives from the best decoded pixels when practical.
- Replace remaining generated-media heuristics with explicit provenance tags in a dedicated artifact-model plan.
- Make the thumbnail gallery height adjustable; prevent cropped/hidden thumbnails in Prepare Images; allow thumbnails around 100 px.

Source: former `PLAN.md`.

## UI / usability

- In Analysis, default “Orient” and “Uniform scale” to on.
- Fix selection/highlight artifacts in AI suggestions and the Observations table; align them with the Measurements table.
- Make room for measure-type radio-button labels.
- Consider renaming “Reference shape” to “Shape”.
- Camera Import naming: fix “Intestion”, rename “Sync shot” to “Camera time offset”, and rename “Microscope sessions” to “Live lab sessions”.
- Camera Import layout: order Import folder, Camera time offset, Live lab sessions, Actions; update button hints.
- Add richer manual reassignment tools for unmatched images.
- Add fine-tuning for multi-line measurement segments.
- Add a hint bar at the bottom of Measure.
- Add Cmd/Ctrl-click additive selection and histogram additive selection in Analysis.
- Continue the Slate Lab / Clinical Nocturne migration in Live Lab, Ingestion Hub, Calibration, and remaining dialogs; consolidate remaining inline style sheets.
- Add a desktop menu link to Pro information/payment on `sporely.no`; do not embed checkout.

Source: former `PLAN.md`; unmatched-image work also appears in `docs/hardware-sync.md`.

## Taxonomy
- Identity backfill follow-ups (plan closed 2026-10-19, see `completed/2026-10-18-identity-backfill.md`):
  - Observation 701 (*Russula queletii* coll.): bind its NorTaxa identity (group `nortaxa_self_unique_name`) once the image-create/link fix is in the build used for syncing. Its unlinked microscope image 5042 carries 3 `other` measurements; the sync will create a metadata-only owner-sync anchor and push them. Keep both; fresh dry run, owner GO, preview and authorized sync as for the other batches.
  - Picking a species from the **Species** dropdown stores no identity; only the **Scientific** dropdown binds. Bind the clicked entry's own concept ID and never choose between same-name concepts (`ui/taxon_input_controller.py` `on_species_selected`).
  - Deferred by owner decision: 419 (copied Artsorakel selection unconfirmed) and 183 `same_name_ai_history_only` rows. Unresolved without new evidence: 147 no identity evidence, 87 unresolved external IDs, 24 cloud-selected unverified, 16 AI suggestions not adopted, 5 identifier/name mismatches, 3 numeric collisions.
  - Observations 382, 495 and 580 have no name locally or in the cloud; the only candidate is an unaccepted low-probability AI pick (`restore_lost_names` in the audit). Left unchanged by owner rule.

- Consider the deferred observer-safe `Help → Taxonomy` menu described in `database/taxonomy/docs/ui-menu-recommendation.md`; keep acquisition, compilation, promotion, mapping edits, and deletion CLI-only.
- Reconcile NorTaxa/COL duplicate candidates (Taxonomy v3 Group B): 7,338 NorTaxa concepts in 7,423 same-name pairs with a separate COL concept in `tax-2026.09.26-02`; the COL side is in the cloud scope for 2,132 pairs, the NorTaxa side never. Names and ids sit on the NorTaxa concept, so web users find no common name for species such as Cantharellus cibarius. Name equality only sizes this; identity needs reviewed evidence. Counts: `database/taxonomy/evidence/taxonomy-v3/stage0/coverage-report.json`.
- Group-A bridge coverage (Taxonomy v3): 19,807 NorTaxa ids on COL concepts only through automatic exact matches, so production cannot resolve them (7,099 in the cloud scope: 1,888 `shared_synonymy`, 5,211 without a published cross-reference). Review and emit approved bridges from the Stage 0 candidate manifests.
- National scientific names (Taxonomy v3): show the approved NorTaxa (later Dyntaxa) preferred scientific name alongside canonical COL; search accepts both; identity never changes.
- Historical unresolved-observation repair (Taxonomy v3): after new Group-A bridges are active, idempotently re-resolve cloud `external_unresolved` observations from preserved source + namespace + external id.
- Review the 448 Artportalen overlay cases left out of `tax-2026.09.26-02` (`database/taxonomy/evidence/artportalen-overlay/`).
- Decide whether the cloud should carry Artportalen/iNaturalist publishing ids as a namespaced channel; today the scoped export suppresses the legacy integer channel.
- Consider Dyntaxa as a national source so Swedish names and Artportalen ids stop depending on the frozen legacy database.
- Decide whether `TaxonChoice` should expose external IDs directly or through a richer match object.
- Add list-returning iNaturalist and Artportalen ID lookup APIs; both identifier types can map to multiple local concepts.
- Define accepted-backbone versus Artportalen-only tie-breaking for duplicate scientific names.
- Decide normalization rules for exact vernacular matching.
- Add an on-demand Artsdatabanken red-list resolver and caching policy.
- Verify AI Photo ID uses a local iNaturalist ID before name matching and that desktop/web apply compatible lookup rules.

Source: former `PLAN.md` and the legacy-database lookup audit `docs/taxonomy-lookup-status.md` (removed 2026-09-26; see Git history).

## AI identification / crop

- Verify the current Supabase AI-crop fields and desktop/web crop sync.
- Verify Artsorakel/iNaturalist result persistence and dropdown behavior.
- Verify Review, Import Review, and Find Detail share one AI Photo ID state model.
- Confirm AI crop is used only for AI requests, not gallery display or R2 originals.
- Prevent replay of stale or tombstoned-image AI runs as current suggestions.
- Define retention before production: remove stale runs after 30 days or retain at most 2–3 stale rows per observation/service; long term, prefer one current row plus short-lived debug history.

Do not crop R2 originals, make gallery display depend on AI crop, or add a separate crop table unless the current model fails.

Source: former `PLAN.md`.

## Reference data / community data

- Return QC metadata in community-data RPC responses.
- Make cloud-origin imported sources more visually distinct in the reference panel.
- Complete the public reference dataset model before broadly publishing comparison plots.

The scoped reference-library work, including its deferred editor, quick-add, revision, cloud-sync, and public-rendering work, remains in the active reference-library plan.

## Testing / infrastructure

- Add export coverage for observations, images, measurements, calibrations, reference data, and image files; verify `app_settings.json` and full profile state are excluded.
- Verify local database values take priority over file EXIF in Prepare Images and the Measure Info box.
- Fix the cloud-synced image warning overlay in Prepare Images.
- Introduce Ruff.
- Consider mypy after the codebase has stable, useful annotations.
- Broaden focused coverage for cloud conflicts, local media signatures, image crop math, `utils/r2_storage.py`, SQLite migrations, and `database/models.py`.
- Test metadata auto-merge and true conflict-dialog triggers.
- Reconcile old “cloud deletion conflict” tests with tombstone behavior.
- Add direct coverage for deterministic pagination ordering, metadata-only anchor byte-fetch prevention, broader pull-only zero-write surfaces, affirmative measurement/calibration identity repair, cross-restart retryability, and snapshot schema compatibility.

Source: former `PLAN.md` and the active cloud-sync extraction plan.

## Hardware / ingestion

- Design the field-device temporal anchor for DSLR/phone batches.
- Add Artsobservasjoner support for per-image note upload.

Source: `docs/hardware-sync.md`.

## Web / infrastructure

- Add an offline queue for upload failures in field conditions.
- Consider a cloud summary RPC/view for observation/image change summaries.
- Future web-native analysis ideas: responsive Plotly L × W plots, thumbnail-linked outlier review, mobile/desktop layouts, a public dataset explorer, taxon summaries, literature reference entry, browser measurement, and possible Pyodide sharing of Python/Numpy logic.
- Privacy/social follow-ups: verify live owner/friend/stranger/blocked/banned RLS and feed behavior, strip GPS EXIF from public media, add an iNaturalist export deep link, and generate Bluesky share cards.

Source: former `PLAN.md`.

## Unsorted / needs review

- Confirm whether Worker secrets/routes and `SUPABASE_URL`, JWT overrides, `MEDIA_PUBLIC_BASE_URL`, and the `sporely-media` R2 binding are already deployed.
- Re-check whether old R2 migration notes are obsolete after the Supabase baseline reset.
- Re-check whether old Phase 7 SQL notes are obsolete after the Supabase baseline reset.
- Verify whether the earlier AI crop backlog has already been completed before promoting any item.

Source: former `PLAN.md`; status could not be established safely from this repository alone.


## Cloud media recovery: handle local image whose matching cloud image is soft-deleted

Reproduce with observation 604 / local image 3258 / cloud image 3694. Determine when recovery should explicitly restore the existing cloud identity versus preserve deletion and report a conflict. Do not silently create a duplicate image.