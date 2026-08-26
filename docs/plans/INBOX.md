# Development inbox

Quick observations captured while using Sporely.
Items here are not approved implementation plans.
Add a note under the nearest heading, or use “Unsorted / needs review” when its home is unclear.

## Cloud sync / recovery

- Add a real Reset Cloud Sync / Reset Cloud Link tool, or remove that instruction from the account-mismatch message until the tool exists.
- Replace “Unable to save cloud login” for account-link protection failures with wording that identifies the account mismatch.
- Consider a non-modal pending-cloud-media indicator on close; do not add a blocking reminder dialog.
- Run live cloud-lock QA with two disposable Sporely Cloud accounts; verify account-mismatch blocking and the Reset Cloud Link flow.
- Verify Profile parity between desktop and web for `username`, `display_name`, `bio`, `avatar_url`, and `profile_email`.
- Review E2 anchor-promotion residual risks before changing that pipeline: cross-device reservation adoption, dangling reserved keys, and URL encoding assumptions.
- Decide whether to repair the known `G_conflicting_intent` row and historical duplicate observation/image records outside the extraction plan.
- Harden standalone migration tooling if it becomes necessary; it is explicitly outside the cloud-sync extraction plan.
- Design a safe fix for the observation-deletion ordering and original-media coverage noted in `docs/cloud-media-incident-audit.md`.

Source: former `PLAN.md`, the active cloud-sync extraction plan, and the cloud-media incident audit.

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

- Consider the deferred observer-safe `Help → Taxonomy` menu described in `database/taxonomy/docs/ui-menu-recommendation.md`; keep acquisition, compilation, promotion, mapping edits, and deletion CLI-only.
- Reconcile `docs/taxonomy-lookup-status.md` against current code before implementation; its audit records external-ID, duplicate-binomial, common-name, and case-sensitivity gaps.
- Decide whether `TaxonChoice` should expose external IDs directly or through a richer match object.
- Add list-returning iNaturalist and Artportalen ID lookup APIs; both identifier types can map to multiple local concepts.
- Define accepted-backbone versus Artportalen-only tie-breaking for duplicate scientific names.
- Decide normalization rules for exact vernacular matching.
- Add an on-demand Artsdatabanken red-list resolver and caching policy.
- Verify AI Photo ID uses a local iNaturalist ID before name matching and that desktop/web apply compatible lookup rules.

Source: former `PLAN.md` and `docs/taxonomy-lookup-status.md`.

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
