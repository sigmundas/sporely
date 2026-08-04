# Sporely Cloud Sync Contract and Repair Plan

Status: required behavior; media-repair work is pending.

This document defines how `sporely-py` and `sporely-web` must exchange observations, photos, microscope data, and deletions.

The same document is kept in both repositories:

- `sporely/docs/supabase-sync-contract.md`
- `sporely-web/docs/supabase-sync-contract.md`

A change that alters sync behavior must update both copies in the same work item.

## Why this document exists

Sporely is local-first on the desktop and cloud-connected across desktop, web, and Android.

The desktop must be able to work without the cloud. When cloud sync is enabled, it should copy shared scientific and observation data without silently destroying local files, cloud photos, or work performed on another device.

On 3 August 2026, desktop image-selection logic was connected to cloud deletion logic. Images that were not selected in a publishing gallery could be treated as unwanted cloud images. This caused some cloud photos to disappear while their observations remained visible. The repair plan is included below.

## Plain-English vocabulary

The plain-English term comes first. The technical name is in parentheses.

- **Local record** (`SQLite row`): a desktop database entry.
- **Cloud record** (`Supabase/Postgres row`): a shared database entry used by the web and Android apps.
- **Cloud file** (`R2 object`): uploaded image bytes stored behind `media.sporely.no`.
- **Local-to-cloud link** (`cloud_id`): the cloud record ID stored on the desktop.
- **Cloud-to-local link** (`desktop_id`): the desktop record ID stored in the cloud.
- **Known-good baseline** (`sync snapshot`): the last state both sides agreed on.
- **Removal marker** (`tombstone`): a durable record that the user explicitly deleted something.
- **Recoverable deletion** (`soft delete`): the record remains but is marked deleted, normally with `deleted_at`.
- **Permanent deletion** (`hard delete`): the database record is removed.
- **Measurement-only image record** (`metadata-only anchor`): an image record kept so measurements can refer to it even when no cloud image file exists.
- **Retry-safe operation** (`idempotent operation`): running the same sync again produces the same correct result without duplicate uploads or extra deletions.
- **Three-way comparison** (`three-way merge`): compare local state, cloud state, and the known-good baseline before deciding what changed.
- **Cloud file address** (`storage_path`): the key that points from an image record to its cloud file.
- **Prepared upload list** (`prepared_items`): images for which the desktop has prepared bytes to upload during this sync.
- **Protected cloud list** (`kept_cloud_ids`): existing cloud image records that must not be removed during this sync.

## Non-negotiable safety rules

1. **Ordinary sync must not delete photos.**

   A normal refresh or bidirectional sync may add and update data. It must not remove a cloud record or cloud file merely because an image was omitted from an upload list.

2. **Not selected does not mean deleted.**

   An unchecked gallery box may mean “do not include this in this publication” or “do not upload new bytes now.” It must never be interpreted as “delete the existing cloud copy.”

3. **Deletion requires a separate, explicit user action.**

   Deletion intent must be recorded as a removal marker (`tombstone`) or an equally explicit cloud-removal command. It must not be inferred from filtering, missing preparation output, publication settings, or a changed sort order.

4. **Normal sync must not permanently delete cloud image records.**

   Permanent deletion (`hard delete`) is reserved for verified maintenance, account deletion, or a later cleanup job after a safe retention period. Normal image deletion should first use a recoverable deletion (`soft delete`).

5. **Cloud files must not be removed before deletion intent is durable.**

   The database must first contain a durable deletion state. File cleanup may then run as a retry-safe follow-up. A failed cleanup must not make the database claim that a still-needed photo is gone.

6. **Local originals remain authoritative.**

   A smaller cloud copy must never overwrite a better local original. A downloaded cloud copy is a recovery copy (`cloud_recovery_cache`), not automatically the new local original.

7. **A failed sync remains retryable.**

   Sync must not mark an observation as fully synced when any required child operation failed. This includes image upload, image metadata, measurement upload, calibration sync, summary sync, and deletion cleanup.

8. **Conflicts block automatic writes.**

   When local and cloud both changed since the known-good baseline, sync must stop that observation and ask for a choice. It must not silently choose one side.

9. **Stable IDs define identity.**

   Observations, images, measurements, and calibrations are matched by stable IDs. Dates, filenames, paths, species names, and sort order are descriptive data, not identity.

10. **Desktop, web, Android, and public read functions must share the same meaning.**

    A state that is valid on the desktop must be represented deliberately in the cloud and displayed deliberately by the clients.

## What belongs where

| Information | Desired ownership |
| --- | --- |
| Observation identity, taxonomy, notes, habitat, location, privacy, and draft state | Shared between desktop and cloud |
| Image description, type, order, microscope context, calibration link, crop, and scale | Shared between desktop and cloud |
| Desktop file paths and import/watch-folder state | Desktop only |
| High-quality local originals | Desktop-owned; optional companion upload only |
| Web-friendly image copies | Cloud files (`R2 objects`) |
| Spore measurement geometry and values | Shared between desktop and cloud |
| Calibration identity and numeric calibration data | Shared between desktop and cloud |
| Window layout, device settings, caches, and temporary state | Device-local |
| Social graph, comments, reports, blocks, billing, and moderation | Cloud/web only |
| Generated thumbnails, plots, mosaics, and crops | Rebuildable outputs unless deliberately persisted |

## Separate the three image decisions

One checkbox cannot safely represent all image behavior. Sporely must keep these decisions separate:

1. **Include in an external publication.**

   Example: include the image in an Artsobservasjoner or iNaturalist upload.

2. **Keep image bytes in Sporely Cloud.**

   This controls whether a cloud file should be uploaded for cross-device use or web display.

3. **Delete the image from Sporely Cloud.**

   This is a destructive action and requires explicit confirmation and a durable removal marker (`tombstone`).

Changing decision 1 must not silently change decision 2 or 3.

Until separate controls exist, existing publication checkboxes must be treated only as publication choices. They must not remove already-uploaded cloud data.

## Desired observation behavior

### First desktop upload

- Create or find the cloud observation using the stable desktop link (`desktop_id`) and owner (`user_id`).
- Upload only images that are eligible for a new cloud copy.
- Store the resulting cloud link (`cloud_id`) locally only after the cloud record is confirmed.
- Save a known-good baseline (`sync snapshot`) after the observation and required child data are complete.
- If one child operation fails, keep the observation retryable instead of declaring the whole observation synced.

### Repeated sync with no changes

- Do no writes.
- Do no uploads.
- Do no deletions.
- Do not open or re-encode image files unnecessarily.
- Return a clear “nothing changed” result.

### Local metadata change

- Compare local, cloud, and the known-good baseline.
- Patch only the changed fields.
- Preserve cloud-only fields and local-only fields.
- Refresh the known-good baseline after a successful result.

### Cloud metadata change

- Pull the changed shared fields to the desktop.
- Preserve local file paths and better local originals.
- Record cloud-only child records without turning them into local workflow state.

### Both sides changed

- Detect the conflict using a three-way comparison (`three-way merge`).
- Block automatic push and pull for that observation.
- Show the changed categories: observation details, images, measurements, calibrations, or deletions.
- Let the user choose “use this device,” “use Sporely Cloud,” or a future field-by-field merge.
- Advance the known-good baseline only after resolution.

## Desired image behavior

### Existing field photo

If the desktop image still exists and is linked to an active cloud image record:

- keep the cloud image record in the protected cloud list (`kept_cloud_ids`);
- keep its cloud file even when the image is not in the prepared upload list (`prepared_items`);
- patch metadata when needed;
- upload replacement bytes only when the local source really changed or the cloud file is missing and restoration is intended;
- never classify it as stale solely because it was unchecked or skipped during preparation.

### New field photo

- If cloud upload is selected, create the cloud file and cloud image record.
- If cloud upload is not selected, leave it local-only.
- A later sync may upload it when the user chooses cloud storage.
- Its local-only state must not affect other cloud images on the same observation.

### Existing microscope photo

- Keep metadata, measurements, and image bytes as independent concerns.
- If bytes already exist in the cloud, ordinary sync must keep them.
- Unchecking a publication box must not strip `storage_path`, delete the cloud file, or delete the original companion file.
- Measurement sync must continue even when no cloud bytes are present.

### Measurement-only microscope record

A measurement-only image record (`metadata-only anchor`) is valid when:

- the user never uploaded the microscope image bytes but chose to share measurement data; or
- the user explicitly chose “remove cloud image, keep measurements.”

It must not be created merely because an external-publication checkbox was unchecked.

The owner-facing UI should distinguish:

- image available in cloud;
- measurement-only record;
- cloud file missing unexpectedly;
- image deleted intentionally.

### Missing cloud file

When the cloud image record exists but its cloud file address (`storage_path`) points to a missing object:

- mark the image as broken or needing repair;
- do not silently delete the record;
- restore from a trusted local file when possible;
- otherwise show a recoverable error to the owner;
- public and mobile clients may omit the broken photo from display, but the owner must be able to diagnose it.

### Image order

Changing image order (`sort_order`) is metadata. It must never cause images to be treated as new or deleted.

## Desired deletion behavior

### User deletes an image on the desktop

- Ask for confirmation when the image has cloud data, measurements, annotations, or generated evidence.
- Record explicit deletion intent locally (`tombstone`).
- On sync, mark the cloud image recoverably deleted (`deleted_at`).
- Keep enough identity and storage information for retry and audit.
- Remove cloud files only after the recoverable deletion is confirmed.
- Keep file cleanup retry-safe.
- Do not permanently delete the cloud database record during ordinary sync.

### User removes only the cloud copy

This must be a separate command from deleting the local image.

- Preserve the local image and local measurements.
- If measurements need the image record, keep a measurement-only record (`metadata-only anchor`).
- Mark the cloud-media removal as explicit user intent.
- Remove the cloud file only after the new state is safely stored.
- Make the resulting state visible to the owner.

### User deletes an observation

- Use one explicit observation-level deletion action.
- Treat child image and measurement cleanup as part of that action.
- Do not infer observation deletion from missing local folders, failed imports, or absent upload candidates.

### Cloud deletion discovered by the desktop

- Record the remote deletion locally.
- Do not automatically erase the local original.
- Ask whether to accept the cloud deletion, restore the cloud copy from local data, or keep the observation local-only.
- Do not repeatedly re-upload a remotely deleted image without a user decision.

## Retry and recovery behavior

Every multi-step sync operation must be safe to run again.

For an image upload:

1. create or identify the cloud image record;
2. upload or verify the cloud file;
3. patch the cloud file address (`storage_path`);
4. store the local-to-cloud link (`cloud_id`);
5. update the known-good baseline (`sync snapshot`).

If a later step fails, the next sync must detect the partial state and continue from it rather than creating duplicates or deleting the row.

For deletion:

1. store explicit deletion intent;
2. mark the cloud record recoverably deleted (`soft delete`);
3. remove cloud files;
4. retain the deletion marker long enough to prevent accidental recreation;
5. permanently purge only through deliberate maintenance.

## Public and mobile display behavior

The web and Android apps normally show only image records with usable cloud files.

That is correct for a deliberate measurement-only record (`metadata-only anchor`). It is not sufficient for owner diagnostics.

Owner-facing reads should expose enough state to distinguish:

- active image with a valid cloud file;
- active image with no cloud file by design;
- active image whose file is unexpectedly missing;
- recoverably deleted image;
- permanently purged image.

Public read functions (`RPCs`) may hide measurement-only and broken images from galleries, but they must not turn an accidental data-loss state into an invisible success.

## Current incident: missing photos after the 3 August 2026 desktop changes

### What changed

The desktop began reading the Artsobservasjoner publication exclusion setting:

`artsobs_publish_excluded_image_ids_<observation_id>`

as if it were also the Sporely Cloud image-storage choice.

### How field photos could disappear

- Unchecked images were omitted from the prepared upload list (`prepared_items`).
- The image-sync loop protected only cloud records that appeared in the prepared or explicitly protected set (`kept_cloud_ids`).
- Existing cloud rows outside that set were labelled “stale.”
- The stale cleanup removed the cloud files (`R2 objects`), permanently deleted the cloud image records (`hard delete`), and cleared the local cloud links (`cloud_id`).

The image still existed locally, but Android and web no longer had a cloud image record to display.

### How microscope photos could disappear

For measured microscope images, the desktop protected the database record so measurements could keep their foreign-key relationship. However, unchecked images could be converted to measurement-only records (`metadata-only anchors`):

- cloud derivative and original files were removed;
- `storage_path` and `original_storage_path` were set to null;
- `deleted_at` remained null.

The public image read function intentionally hides rows without `storage_path`, so Android correctly displayed no photo even though the measurement anchor remained.

### Why the follow-up did not restore them

A follow-up change stopped the desktop from re-uploading the bytes after converting an image to a measurement-only record. That fixed an orphan-upload loop, but it made the missing-photo state stable instead of restoring the original cloud image.

### Possible affected states

Each affected image must be classified before recovery:

1. **Cloud record permanently deleted.**
2. **Cloud record still active but file address cleared** (`storage_path IS NULL`).
3. **Cloud record recoverably deleted** (`deleted_at IS NOT NULL`).
4. **Cloud record active but cloud file missing.**
5. **Local removal marker present by mistake** (`tombstone`).
6. **No damage; client cache or unrelated display issue.**

Known examples reported during the incident include Mica cap, Boletales, and *Mycena haematopus*. These names are starting points for the audit, not a complete affected set.

## Repair plan

### Phase 0 — stop further damage

Desktop repository (`sporely`):

- Disable stale-image deletion during ordinary image sync.
- Make omitted upload candidates harmless.
- Stop converting existing cloud-backed microscope images to measurement-only records based on publication checkboxes.
- Keep all active linked cloud images in the protected cloud list (`kept_cloud_ids`).
- Do not remove cloud files from the metadata-anchor pre-step.
- Keep explicit removal-marker processing (`tombstones`) separate and disabled only if its provenance cannot be trusted.

Operational actions:

- Avoid running the affected desktop sync build against important data until the hotfix is available.
- Back up the local SQLite database before recovery.
- Export affected cloud image rows before changing them.
- Preserve any sync logs from 3–4 August 2026.

### Phase 1 — build an evidence report

Add a read-only audit that reports, per observation and image:

- local observation ID and cloud observation ID;
- local image ID and cloud image ID;
- image type and sort order;
- local file existence;
- local cloud link (`cloud_id`);
- local removal marker (`tombstone`);
- cloud deletion timestamps (`deleted_at`, `purged_at`);
- cloud file addresses (`storage_path`, `original_storage_path`);
- whether each cloud file actually exists;
- measurement count;
- last known sync time and source app version.

The audit must have a dry-run mode and must not mutate data.

Use it first on the reported observations, then on every observation synced by the affected desktop versions.

### Phase 2 — separate selection from deletion

Desktop repository (`sporely`):

- Keep external-publication exclusions in their existing publication setting.
- Introduce a separate cloud-media choice only when the product has an explicit cloud-storage control.
- Represent deletion only through explicit user actions and durable removal markers (`tombstones`).
- Do not derive cloud deletion from:
  - publication exclusions;
  - missing `prepared_items`;
  - missing local temporary files;
  - changed sort order;
  - a filtered image type;
  - a measurement visibility change.
- Rename helpers and settings so their purpose is unambiguous.
- Add migration or compatibility code that treats old publication exclusions as non-destructive.

### Phase 3 — fix the desktop image-sync algorithm

Desktop repository (`sporely`):

- Build the protected cloud list (`kept_cloud_ids`) from every active cloud image that still maps to an existing local image, not only from prepared uploads.
- Make the prepared upload list (`prepared_items`) represent upload work only.
- Remove the “delete every remote row not kept” rule from ordinary sync.
- Process explicit removal markers (`tombstones`) in a separate deletion phase.
- Use recoverable deletion (`soft delete`) instead of permanent row deletion (`hard delete`) in ordinary user workflows.
- Make file removal a follow-up to a confirmed deletion state.
- Never clear a valid local cloud link (`cloud_id`) merely because no bytes were uploaded in the current run.
- Keep measurement-only anchor creation explicit and non-destructive.
- Rebuild the known-good baseline only after all required image operations succeed.

### Phase 4 — harden the cloud and client behavior

Web repository (`sporely-web`):

- Keep public galleries limited to usable cloud files.
- Add an owner-facing audit surface or admin query that exposes measurement-only, broken, and deleted image states.
- Ensure the upload worker can verify file existence without changing data.
- Make media deletion retry-safe and observable.
- Consider a clearer stored state only if current fields cannot distinguish:
  - intentionally measurement-only;
  - unexpectedly missing file;
  - delete requested;
  - recoverably deleted.
- Do not add a new state column merely to hide an algorithmic bug; first fix the desktop behavior.

### Phase 5 — recover damaged data

Run recovery only after Phases 0–3 are deployed.

For permanently deleted field-photo records:

- find the matching local image by stable desktop identity and observation identity;
- recreate or relink the cloud image record;
- re-upload a web-friendly cloud file from the trusted local source;
- restore order and image metadata;
- stamp the local cloud link only after success.

For measurement-only records that should still have photos:

- upload the derivative from the trusted local source;
- patch `storage_path`;
- optionally restore `original_storage_path` only when the original-upload policy allows it;
- preserve the existing cloud image ID so measurement links remain valid.

For erroneous removal markers:

- clear only markers proven to come from the publication-selection bug;
- do not clear genuine user deletions.

For every recovered image:

- verify the cloud file exists;
- verify owner, observation, and image identity;
- verify web/Android read functions return the expected photo;
- update the local known-good baseline and file signature only after verification.

### Phase 6 — add regression tests

Desktop tests must prove:

- unchecking an existing field image does not remove its cloud row or cloud file;
- unchecking an existing microscope image does not strip existing bytes;
- omitting an image from `prepared_items` does not make it stale;
- an active linked image is always added to `kept_cloud_ids`;
- explicit `tombstone` deletion still works;
- normal deletion uses `deleted_at` before file cleanup;
- repeated unchanged sync performs no writes, uploads, or deletions;
- interruption after upload, metadata patch, or file deletion is recoverable;
- a missing cloud file is reported as broken instead of silently deleted;
- conflict detection covers observations, images, measurements, and remote removals;
- local originals and paths are never overwritten by recovery copies.

Web/cloud tests must prove:

- public image functions hide deliberate measurement-only records;
- owner/admin diagnostics expose deliberate and broken no-file states;
- recovered images appear in Android/web image queries;
- recoverably deleted images stay hidden;
- media worker deletion is retry-safe;
- storage keys remain scoped to the authenticated owner.

Cross-repository tests or fixtures must cover the same image-state examples in both implementations.

### Phase 7 — release and monitor

- Ship the desktop safety hotfix before running bulk recovery.
- Release the read-only audit before the write-capable recovery tool.
- Require a dry-run report and explicit confirmation for each recovery batch.
- Record counts for:
  - rows restored;
  - files restored;
  - measurement-only rows left intentionally unchanged;
  - genuine deletions preserved;
  - missing local sources;
  - unresolved conflicts.
- Monitor image-row deletion, file deletion, null `storage_path`, and recovery counts by app version.
- Do not remove the recovery code until all affected desktop databases have had a reasonable opportunity to sync safely.

## Definition of done

The incident is resolved when all of these are true:

- ordinary sync cannot delete an image record or file without explicit deletion intent;
- publication selection and cloud deletion are separate concepts;
- existing field and microscope photos survive unchecked publication state;
- measurement-only records are deliberate and visible to owners;
- all affected observations have been audited;
- recoverable images have been restored from trusted local sources;
- genuine user deletions remain deleted;
- repeated sync is retry-safe and produces no additional changes;
- Android and web show the restored photos;
- both repositories contain matching contract documentation and regression tests.

## Repository responsibilities

### Desktop repository (`sporely`)

Owns:

- local database state and local originals;
- three-way comparison and conflict blocking;
- upload preparation;
- local/cloud identity links;
- explicit removal markers (`tombstones`);
- measurement and calibration push/pull;
- recovery from trusted local files;
- desktop regression tests.

Primary implementation areas:

- `utils/cloud_sync.py`
- `ui/observations_tab.py`
- `ui/image_gallery_widget.py`
- `database/models.py`
- `tests/test_cloud_sync_*.py`
- `tests/test_observations_tab_cloud_sync.py`

### Web repository (`sporely-web`)

Owns:

- cloud schema and access rules (`RLS`);
- public and owner read functions (`RPCs`);
- Android/web image display;
- cloud upload and delete worker behavior;
- admin and owner diagnostics;
- web/cloud regression tests.

Primary implementation areas:

- `supabase/migrations/`
- `supabase/tests/`
- `src/images.js`
- `src/sync-queue.js`
- `supabase/functions/`
- Android/web observation gallery code.

## Change-control checklist

Any change that can remove a cloud record or cloud file must answer all of these in its pull request:

- What explicit user action created deletion intent?
- Where is that intent stored durably?
- Is the database change recoverable (`soft delete`)?
- What happens if file cleanup fails?
- What happens if the app crashes after each step?
- Can repeating the operation delete anything extra?
- Which test proves an omitted upload candidate is not deleted?
- Which test proves a publication checkbox is not a deletion control?
- How can an owner audit and recover the result?
- Were both copies of this contract updated?
