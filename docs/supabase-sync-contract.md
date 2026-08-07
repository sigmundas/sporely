# Sporely Cloud Sync Contract and Repair Plan

Status: required behavior; repair work pending.

This is the shared sync specification for `sporely` (desktop) and `sporely-web` (web, Android, Supabase, and cloud media). Keep an identical copy at:

- `sporely/docs/supabase-sync-contract.md`
- `sporely-web/docs/supabase-sync-contract.md`

Any change to sync behavior must update both copies in the same work item.

## Purpose

Sporely is local-first on desktop and cloud-connected across desktop, web, and Android. Sync should copy shared observation and scientific data without silently destroying local files, cloud photos, or work from another device.

A regression introduced on 3 August 2026 connected publication-gallery selection to cloud image deletion. Photos omitted from an upload list could be treated as unwanted cloud images. This document defines the intended behavior and the plan to repair and recover affected data.

## Plain-English vocabulary

Plain English comes first; technical terms are in parentheses.

- **Local record** (`SQLite row`): a desktop database entry.
- **Cloud record** (`Supabase/Postgres row`): a shared database entry.
- **Cloud file** (`R2 object`): uploaded image bytes.
- **Local-to-cloud link** (`cloud_id`): cloud record ID stored locally.
- **Cloud-to-local link** (`desktop_id`): desktop record ID stored in the cloud.
- **Known-good baseline** (`sync snapshot`): the last state both sides agreed on.
- **Removal marker** (`tombstone`): durable evidence of an explicit user deletion.
- **Recoverable deletion** (`soft delete`): keep the record but mark it deleted, usually with `deleted_at`.
- **Permanent deletion** (`hard delete`): remove the database record.
- **Measurement-only image record** (`metadata-only anchor`): an image row retained for measurement links without cloud image bytes.
- **Retry-safe operation** (`idempotent operation`): repeating it produces the same correct result without duplicates or extra deletion.
- **Three-way comparison** (`three-way merge`): compare local state, cloud state, and the known-good baseline.
- **Cloud file address** (`storage_path`): the key pointing to a cloud file.
- **Prepared upload list** (`prepared_items`): images whose bytes were prepared for this sync.
- **Protected cloud list** (`kept_cloud_ids`): existing cloud image rows that must remain.

## Non-negotiable safety rules

1. **Ordinary sync must not infer deletion.** A routine refresh may add or update data. It must not remove a cloud record or file merely because an image was omitted from an upload list.
2. **The gallery checkbox is desired cloud state.** Unchecking a live cloud-backed image is explicit cloud-copy deletion intent; an external-publication choice is not.
3. **Deletion requires an explicit transition.** Store checkbox uncheck or “Delete cloud copy” intent as a removal marker (`tombstone`) before sync performs the remote deletion.
4. **Normal sync must not permanently delete image rows.** Use recoverable deletion (`soft delete`) first. Permanent deletion belongs to verified maintenance, account deletion, or retention cleanup.
5. **Store deletion intent before removing bytes.** Confirm the database deletion state first, then clean up cloud files as a retry-safe follow-up.
6. **Local originals remain authoritative.** A smaller cloud copy never overwrites a better local original. Downloaded files are recovery copies (`cloud_recovery_cache`) until explicitly promoted.
7. **Failures remain retryable.** Do not mark an observation fully synced if required image, measurement, calibration, summary, or deletion work failed.
8. **Conflicts block automatic writes.** When local and cloud both changed since the known-good baseline, stop and ask for a choice.
9. **Stable IDs define identity.** Match by IDs, not dates, filenames, file paths, species names, or sort order.
10. **All clients share the same state meanings.** Desktop, web, Android, cloud functions, and media workers must distinguish active, measurement-only, broken, and deleted images consistently.

## Ownership boundary

| Information | Desired ownership |
| --- | --- |
| Observation identity, taxonomy, notes, habitat, location, privacy, and draft state | Shared |
| Image metadata, type, order, microscope context, calibration link, crop, and scale | Shared |
| Desktop paths, import state, watch folders, caches, and UI state | Desktop/device only |
| High-quality local originals | Desktop-owned; optional companion upload |
| Web-friendly image copies | Cloud files (`R2 objects`) |
| Spore measurements and calibration identity/data | Shared |
| Social graph, social comments, reports, blocks, billing, moderation | Cloud/web only |
| Thumbnails, plots, mosaics, and crops | Rebuildable unless deliberately persisted |

## Keep image decisions explicit

The Observations and Measure gallery checkbox represents the desired Sporely Cloud state only:

- checked means the image should have a cloud copy;
- unchecked means it should not have a cloud copy;
- the cloud badge reports actual state: uploaded, delete pending, or absent.

External-publication inclusion (for example Artsobservasjoner or iNaturalist) is a separate workflow decision. Ordinary upload filtering, a missing prepared file, or external-publication exclusion must never be reinterpreted as cloud deletion intent.

Canonical desktop image states are `NONE`, `UPLOADED`, `DELETE_PENDING`, and `DELETED`. A retained `cloud_id` with a synced tombstone is `DELETED`, not `UPLOADED`.

## Desired observation sync

### First upload

- Create or find the cloud observation by owner (`user_id`) and stable desktop link (`desktop_id`).
- Upload only images eligible for a new cloud copy.
- Store local cloud links (`cloud_id`) only after confirmation.
- Save the known-good baseline (`sync snapshot`) after all required child work succeeds.
- Keep partial failures dirty and retryable.

### No changes

- Perform no writes, uploads, deletions, or unnecessary image encoding.
- Return a clear “nothing changed” result.

### One side changed

- Compare local, cloud, and baseline.
- Patch only shared fields that changed.
- Preserve cloud-only and local-only fields.
- Refresh the baseline after success.

### Both sides changed

- Detect a conflict using a three-way comparison (`three-way merge`).
- Block automatic push and pull for that observation.
- Show affected categories: details, images, measurements, calibrations, or deletion.
- Offer “use this device,” “use Sporely Cloud,” or a future field-level merge.

## Desired image sync

### Existing cloud-backed image

When a local image still maps to an active cloud image:

- add it to the protected cloud list (`kept_cloud_ids`) whether or not bytes are prepared;
- keep the cloud row and file when omitted from `prepared_items` unless an explicit checkbox/context-menu tombstone is pending;
- patch metadata when necessary;
- replace bytes only when the trusted source changed or a missing cloud file is deliberately restored;
- never call it stale merely because it was filtered, skipped, or reordered.

### New local image

- Upload when cloud upload is selected.
- Otherwise leave it local-only.
- Its local-only state must not affect existing cloud images.

### Existing microscope image

- Treat metadata, measurements, and bytes as separate concerns.
- Preserve existing cloud bytes during ordinary sync.
- External-publication selection must not clear `storage_path`, clear `original_storage_path`, or delete cloud files.
- Measurements may sync without bytes.

### Measurement-only image record

A measurement-only record (`metadata-only anchor`) is valid only when:

- bytes were never uploaded but measurements are shared; or
- the user explicitly chose “remove cloud image, keep measurements.”

It must not be created from an external-publication choice. Owner-facing UI must distinguish deliberate measurement-only state from a broken or deleted image.

### Missing cloud file

When an active row points to missing bytes:

- mark it broken or needing repair;
- do not silently delete the row;
- restore from a trusted local source where possible;
- otherwise show the owner a recoverable error;
- public clients may omit the broken photo, but owner diagnostics must expose the problem.

Changing image order (`sort_order`) is metadata only and cannot imply creation or deletion.

## Desired deletion flow

### Delete image everywhere

- Confirm when cloud data, measurements, annotations, or evidence are attached.
- Record explicit local deletion intent (`tombstone`).
- Mark the cloud row recoverably deleted (`deleted_at`).
- Keep identity and storage information for retry and audit.
- Remove cloud files only after the deletion state is confirmed.
- Do not hard-delete the row during routine sync.

### Remove only the cloud copy

This is separate from deleting the local image.

- Preserve local files and measurements.
- Keep a measurement-only record when required.
- Store explicit cloud-removal intent.
- Remove bytes after the new state is safely recorded.
- Show the resulting state to the owner.

The gallery checkbox and context-menu command share this lifecycle:

- `UPLOADED` + uncheck queues an unsynced tombstone and shows `DELETE_PENDING` immediately;
- recheck before sync cancels that tombstone, preserves the existing cloud ID, and performs no upload or delete;
- after successful remote deletion, the checkbox remains unchecked and the synced tombstone yields `DELETED` with no cloud badge;
- recheck after `DELETED` preserves the historical tombstone, detaches the old identity, and explicitly restores exactly that image as a new cloud row.

### Remote deletion discovered on desktop

- Record it locally without erasing the local original.
- Ask whether to accept the deletion, restore cloud data from local files, or remain local-only.
- Do not repeatedly re-upload without a user decision.

## Retry-safe sequencing

Image upload should proceed as:

1. create or identify cloud image row;
2. upload or verify cloud file;
3. patch cloud file address (`storage_path`);
4. save local cloud link (`cloud_id`);
5. update known-good baseline (`sync snapshot`).

Deletion should proceed as:

1. store explicit intent;
2. soft-delete cloud row;
3. remove cloud files;
4. retain the removal marker long enough to prevent accidental recreation;
5. purge permanently only through deliberate maintenance.

An interruption after any step must be recoverable by repeating sync.

## Public, Android, and owner display

Public galleries should show only active rows with usable cloud files. That is correct for a deliberate measurement-only record, but not enough for diagnosis.

Owner/admin reads must distinguish:

- active image with valid bytes;
- deliberate measurement-only record;
- active row with unexpectedly missing bytes;
- recoverably deleted image;
- permanently purged image.

Public read functions (`RPCs`) may hide measurement-only and broken images, but must not turn accidental data loss into an invisible success.

## Incident: missing photos after 3 August 2026

### Root cause

The desktop began reading:

`artsobs_publish_excluded_image_ids_<observation_id>`

as both an external-publication choice and a Sporely Cloud media-storage choice.

### Field-photo failure

- Unchecked images were omitted from `prepared_items`.
- The sync loop protected only rows in prepared or explicitly protected sets (`kept_cloud_ids`).
- Other existing rows were classified as stale.
- Stale cleanup deleted cloud files, hard-deleted cloud image rows, and cleared local `cloud_id` values.

Local files remained, but Android/web had no cloud image row to display.

### Microscope-photo failure

Measured microscope rows were sometimes retained for foreign-key links, but unchecked images could be converted to metadata-only anchors:

- derivative and companion-original files were removed;
- `storage_path` and `original_storage_path` became null;
- `deleted_at` remained null.

Public image queries intentionally hide rows without `storage_path`, so the photo disappeared while measurements remained.

A follow-up prevented automatic re-upload after this conversion. It stopped an orphan-upload loop but did not restore removed bytes.

Reported examples include Mica cap, Boletales, and *Mycena haematopus*. These are audit starting points, not a complete list.

## Repair plan

### Phase 0 — stop further damage

Desktop (`sporely`):

- Disable stale-row deletion in ordinary image sync.
- Make omitted upload candidates harmless.
- Preserve all active linked cloud images in `kept_cloud_ids`.
- Stop publication checkboxes from converting existing images to metadata-only state.
- Stop metadata-anchor helpers from deleting cloud bytes.
- Keep explicit tombstone processing separate; pause it too if old markers cannot be trusted.

Operations:

- Avoid affected desktop sync builds on important data.
- Back up local SQLite before repair.
- Export affected cloud image rows.
- Preserve sync logs from 3–4 August 2026.

### Phase 1 — read-only evidence report

Report per observation/image:

- local and cloud observation IDs;
- local and cloud image IDs;
- image type and order;
- local file existence;
- `cloud_id` and local tombstone state;
- `deleted_at` and `purged_at`;
- `storage_path` and `original_storage_path`;
- actual cloud-file existence;
- measurement count;
- last sync time and source app version.

The audit must be dry-run and non-mutating. Run it first on reported observations, then all observations touched by affected desktop versions.

### Phase 2 — make cloud selection explicit

Desktop:

- Keep Artsobservasjoner/iNaturalist selection as publication state only.
- Treat the Observations/Measure gallery checkbox as desired Sporely Cloud state.
- Represent checkbox/context-menu deletion only through explicit tombstones.
- Never infer deletion from publication exclusions, absent `prepared_items`, missing temporary files, changed order, filtered image type, or measurement visibility.
- Treat legacy publication exclusions as non-destructive.

### Phase 3 — repair desktop image algorithm

Desktop:

- Build `kept_cloud_ids` from every active cloud row that maps to an existing local image.
- Use `prepared_items` only for upload work.
- Remove “delete every remote row not kept” from ordinary sync.
- Process tombstones in a separate deletion phase.
- Use soft deletion before file cleanup.
- Never clear a valid `cloud_id` because the current run uploaded no bytes.
- Make metadata-only anchor creation explicit and non-destructive.
- Advance snapshots only after all required image operations succeed.

### Phase 4 — harden cloud and clients

Web/cloud (`sporely-web`):

- Keep public galleries limited to usable files.
- Add owner/admin diagnostics for measurement-only, broken, and deleted states.
- Add non-mutating file-existence verification.
- Make media deletion retry-safe and observable.
- Add state fields only if existing fields cannot distinguish intentional measurement-only state from broken or deleting state; do not add fields merely to hide an algorithm bug.

### Phase 5 — recover data

Only after the safety fix is deployed:

- **Hard-deleted field row:** recreate or relink by stable identity, re-upload from trusted local source, restore metadata/order, and stamp `cloud_id` after verification.
- **Pathless microscope anchor that should have bytes:** upload derivative, patch `storage_path`, preserve cloud image ID and measurement links, and restore `original_storage_path` only when policy allows.
- **Erroneous tombstone:** clear only when evidence proves it came from the regression; preserve genuine deletion.
- Verify cloud-file existence, identity, client visibility, and baseline updates for every recovered image.

### Phase 6 — regression tests

Desktop tests must prove:

- unchecked existing field images keep local row/bytes and queue only cloud deletion;
- unchecked existing microscope images keep local row/bytes, measurements, and annotations;
- rechecking an unsynced delete cancels it without upload or remote deletion;
- rechecking a completed delete uses image-specific explicit restore without touching live siblings;
- omission from `prepared_items` is not stale/deleted;
- every active linked image enters `kept_cloud_ids`;
- explicit tombstone deletion still works;
- soft deletion precedes cleanup;
- repeated unchanged sync performs no writes/uploads/deletions;
- interrupted upload or deletion recovers safely;
- missing cloud files are reported, not silently deleted;
- conflicts cover observation, image, measurement, and deletion changes;
- recovery copies never overwrite local originals.

Web/cloud tests must prove:

- public functions hide deliberate measurement-only rows;
- owner/admin diagnostics expose deliberate and broken no-file states;
- restored images appear in Android/web queries;
- soft-deleted rows remain hidden;
- media deletion is retry-safe;
- storage keys remain owner-scoped.

### Phase 7 — release and monitor

- Ship the desktop safety hotfix before bulk recovery.
- Release read-only audit before write-capable recovery.
- Require dry-run reports and explicit approval for recovery batches.
- Record restored rows/files, intentional metadata-only rows, genuine deletions, missing local sources, and unresolved conflicts.
- Monitor hard deletes, file deletes, null `storage_path`, recovery counts, and source app versions.

## Definition of done

- Ordinary sync cannot delete a row or file without explicit intent.
- Publication selection and cloud deletion are separate.
- Existing field and microscope photos survive unchecked publication state.
- Measurement-only records are deliberate and diagnosable.
- Affected observations have been audited and recoverable media restored.
- Genuine deletions remain deleted.
- Repeated sync is retry-safe and causes no extra changes.
- Android and web show restored photos.
- Both repositories contain matching docs and regression tests.

## Repository responsibilities

### Desktop (`sporely`)

Owns local originals, SQLite state, three-way comparison, conflict blocking, upload preparation, ID links, tombstones, measurement/calibration sync, recovery from trusted local files, and desktop tests.

Primary areas:

- `utils/cloud_sync.py`
- `ui/observations_tab.py`
- `ui/image_gallery_widget.py`
- `database/models.py`
- `tests/test_cloud_sync_*.py`
- `tests/test_observations_tab_cloud_sync.py`

### Web/cloud (`sporely-web`)

Owns schema and access rules (`RLS`), public/owner functions (`RPCs`), Android/web display, media worker behavior, diagnostics, and web/cloud tests.

Primary areas:

- `supabase/migrations/`
- `supabase/tests/`
- `src/images.js`
- `src/sync-queue.js`
- `supabase/functions/`
- observation gallery code.

## Destructive-change checklist

Every pull request that can remove a cloud row or file must state:

- What explicit user action created deletion intent?
- Where is that intent stored durably?
- Is the database change recoverable (`soft delete`)?
- What happens if cleanup fails or the app crashes after each step?
- Can retrying delete anything extra?
- Which test proves an omitted upload candidate is preserved?
- Which test proves publication selection is not deletion consent?
- How can the owner audit and recover the result?
- Were both copies of this contract updated?
