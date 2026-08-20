# Sporely Cloud Sync Contract and Repair Plan

Status: required behavior; Stage 1 landed; Stage 2 pending.

This is the shared sync specification for `sporely` (desktop) and `sporely-web` (web, Android, Supabase, and cloud media). Keep an identical copy at:

- `sporely/docs/supabase-sync-contract.md`
- `sporely-web/docs/supabase-sync-contract.md`

Any change to sync behavior must update both copies in the same work item.

This contract is the behavioral specification. For implementation
navigation — entry points, canonical function ownership, cloud-write and
destructive boundaries, and the staged refactoring plan — see
[`cloud-sync-architecture.md`](cloud-sync-architecture.md) (desktop
repository only).

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
- **Cloud-storage-desired excluded set** (`sporely_cloud_image_storage_excluded_ids_<obs>`): the local image ids for one observation whose bytes are *not* desired in Sporely Cloud image storage. Everything else is desired.

## Non-negotiable safety rules

1. **The gallery "Keep image in Sporely Cloud" checkbox is desired Sporely Cloud image-storage state.**
   - Checked = a cloud image copy is desired.
   - Unchecked, local-only image = do not upload bytes.
   - Unchecking an already-uploaded image = explicit cloud-copy deletion intent, which queues the tombstone/delete-pending lifecycle.
   - Rechecking before the tombstone syncs cancels the pending deletion and preserves the existing cloud copy.
2. **Internal omission is never deletion intent.** Filtering, failed preparation, missing files, metadata-only sync passes, `prepare_images_cb`=None fallbacks, `include_image_ids` subsets, or any other implementation detail must never independently create deletion intent. Deletion only comes from an explicit uncheck or an explicit context-menu removal.
3. **Deletion requires an explicit transition.** Store checkbox uncheck or "Delete cloud copy" intent as a removal marker (`tombstone`) before sync performs the remote deletion.
4. **Image bytes and metadata-only microscope anchors are different things.** Unchecking a measured microscope image removes its cloud image bytes but may still retain a metadata-only microscope anchor so spore measurements and the public spore mosaic keep working. Never confuse the two: the byte-storage predicate governs bytes only.
5. **Normal sync must not permanently delete image rows.** Use recoverable deletion (`soft delete`) first. Permanent deletion belongs to verified maintenance, account deletion, or retention cleanup.
6. **Store deletion intent before removing bytes.** Confirm the database deletion state first, then clean up cloud files as a retry-safe follow-up.
7. **Local originals remain authoritative.** A smaller cloud copy never overwrites a better local original. Downloaded files are recovery copies (`cloud_recovery_cache`) until explicitly promoted.
8. **Failures remain retryable.** Do not mark an observation fully synced if required image, measurement, calibration, summary, or deletion work failed.
9. **Conflicts block automatic writes.** When local and cloud both changed since the known-good baseline, stop and ask for a choice.
10. **Stable IDs define identity.** Match by IDs, not dates, filenames, file paths, species names, or sort order.
11. **All clients share the same state meanings.** Desktop, web, Android, cloud functions, and media workers must distinguish active, measurement-only, broken, and deleted images consistently.
12. **Identity repair is checkbox-independent.** A local row with a lost `cloud_id` must be reconciled with the matching remote row by `desktop_id` regardless of the current gallery checkbox. Repair restores identity; it does not upload bytes. Recovery of identity for an unchecked image lets the byte gate correctly refuse re-uploads and lets a pending tombstone complete instead of creating a duplicate.
13. **The legacy `artsobs_publish_excluded_image_ids_<obs>` setting is publication-only.** Its values must never automatically tombstone or otherwise remove cloud media. They may surface as audit hints in a later stage but never independently drive cloud-side change.
14. **A partial remote collection is never authoritative remote state.** PostgREST (and any bounded API) can silently cap a response at the server row limit while returning HTTP 200. A truncated collection must never be used to conclude that rows are absent, to compute deletions, or to drive any diff against local state.
15. **Bounded or paginated remote reads must be exhausted successfully before absence may be interpreted as deletion.** If any page fails, the whole read fails — the client must raise, not return the pages fetched so far. Absence of a row is meaningful only after a complete, successful paginated read of the relevant scope.
16. **Incomplete remote data must never be stored as a sync snapshot.** The known-good baseline may only be persisted from a complete, successful remote read (and after all required child work succeeds). A snapshot recorded from truncated data poisons every future three-way comparison for that observation.
17. **PostgREST pagination must use deterministic ordering.** Every paginated bulk read must include an explicit `order=` clause with a unique tie-breaker (`id.asc`); offset-based paging over an unstable order can silently skip or duplicate rows across pages.
18. **Download from Cloud is a strict zero-cloud-write mode.** A pull-only run must complete with `cloud_writes_completed == 0` and an empty blocked-write list. Every write path — PATCH, POST, DELETE, RPC mutations, storage upload, storage removal, and identity write-backs — is out of scope for this mode.
19. **Pull-only write blocking is defense in depth, not expected control flow.** The fail-closed pull-only client wrapper exists to catch mistakes, not to be exercised. Pull-side code must gate its own writes at the source; any blocked write attempt recorded during a Download-from-Cloud run is a defect to fix at the source, not a handled event. New client writer methods must be added to the pull-only blocklist; new read methods join the allowlist only as an explicit, reviewed choice.
20. **The local `cloud_id` is the primary direct local→cloud observation identity.** Once it verifies against an existing same-owner cloud row, that row is the push target. The remote `desktop_id` is the reverse cloud→local link and an identity recovery mechanism — never the primary lookup.
21. **A missing remote `desktop_id` must never cause object creation when a verified local `cloud_id` already identifies the remote object.** Download from Cloud legitimately creates local rows with `cloud_id` set while the remote `desktop_id` remains NULL until a future normal (write-enabled) sync heals the reverse link. Resolving push identity by reverse link alone creates duplicate cloud observations.
22. **Direct and reverse identity links resolving to different objects is an identity conflict, not permission to create a third object.** The same applies to an ambiguous reverse-link match (multiple rows carrying one `desktop_id`). The push must fail safely — no update of either candidate, no creation, no snapshot — and the local observation stays dirty/retryable for review.
23. **Observation creation (POST) is a last resort.** It is allowed only after direct identity verification and reverse-link recovery both find no target. "Lookup failed" never automatically means "create" while the local row carries a direct cloud identity.

## Storage of desired cloud image-byte state

Local SQLite is authoritative:

- `images.cloud_id` — the current cloud identity link (if any).
- `image_tombstones` — durable explicit-deletion intent.
- `settings["sporely_cloud_image_storage_excluded_ids_<obs>"]` — the per-observation set of local image ids the user has excluded from Sporely Cloud image storage.
- `settings["sporely_cloud_image_storage_intent_ids_<obs>"]` — the per-observation storage-intent ledger. Only membership in this ledger proves an explicit decision (excluded or desired) exists for that image. Absence from the excluded set alone proves nothing — it may mean "explicitly desired" or "never decided." The observation-level init sentinel is retired; late-imported microscope frames must not inherit a stale "already initialized" flag.
- `settings["sporely_cloud_image_promotion_pending_<obs>_<img>"]` — a local pending marker written *before* the anchor-promotion reservation PATCH. Its presence together with a non-NULL remote `storage_path` marks the Worker key as UNCONFIRMED (reserved but not yet proven to hold bytes) and must not be trusted as proof of bytes.

The canonical byte-storage predicate is `cloud_image_bytes_desired(observation_id, image_id)`:

- returns `False` when `image_id` is in the excluded set for that observation;
- returns `True` otherwise;
- concerns bytes only — anchor lifecycle for measurements is separately governed by the metadata-only microscope anchor helper.

### Anchor promotion (metadata-only → byte-backed)

A linked metadata-only anchor (valid `cloud_id`, remote `storage_path` NULL) whose bytes become desired is promoted on its existing row rather than by creating a new row:

- The Worker media key is reserved via an owner-scoped conditional PATCH with filter `storage_path=is.null` — writing a candidate key only when the remote row is still NULL. The write is guarded by the local pending marker written *before* the PATCH.
- On successful reservation the upload proceeds; on confirmed upload the marker is cleared.
- On upload failure the partial R2 objects are removed and the key is released via a second owner-scoped conditional PATCH with filter `storage_path=eq.<exact reserved key>`, so a competing writer's key is never clobbered.
- A non-NULL `storage_path` combined with a live promotion-pending marker means UNCONFIRMED and must not be treated as bytes present. Once the marker is cleared, `storage_path` is authoritative again.

`SporelyCloudClient.upload_image_file` and `SporelyCloudClient.upload_original_image_file` refuse to send bytes when the predicate returns `False`, raising `CloudImageBytesNotDesiredError`. Recovery flows opt in by passing `recovery_authorized=True`.

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

External-publication inclusion (for example Artsobservasjoner or iNaturalist) is a separate workflow decision. It has its own persistence key (`artsobs_publish_excluded_image_ids_<obs>`) and does not participate in cloud storage sync in either direction. Ordinary upload filtering, a missing prepared file, or external-publication exclusion must never be reinterpreted as cloud deletion intent.

Canonical desktop image states are `NONE`, `UPLOADED`, `DELETE_PENDING`, and `DELETED`. A retained `cloud_id` with a synced tombstone is `DELETED`, not `UPLOADED`.

## Desired observation sync

### First upload

- Create or find the cloud observation by owner (`user_id`) and stable desktop link (`desktop_id`).
- Upload only images eligible for a new cloud copy (byte-storage desired) and whose bytes are actually available.
- Store local cloud links (`cloud_id`) only after confirmation.
- Save the known-good baseline (`sync snapshot`) after all required child work succeeds.
- Keep partial failures dirty and retryable.

### No changes

- Perform no writes, uploads, deletions, or unnecessary image encoding.
- Return a clear "nothing changed" result.

### One side changed

- Compare local, cloud, and baseline.
- Patch only shared fields that changed.
- Preserve cloud-only and local-only fields.
- Refresh the baseline after success.

### Both sides changed

- Detect a conflict using a three-way comparison (`three-way merge`).
- Block automatic push and pull for that observation.
- Show affected categories: details, images, measurements, calibrations, or deletion.
- Offer "use this device," "use Sporely Cloud," or a future field-level merge.

## Desired image sync

### Existing cloud-backed image

When a local image still maps to an active cloud image:

- add it to the protected cloud list (`kept_cloud_ids`) whether or not bytes are prepared;
- keep the cloud row and file when omitted from `prepared_items` unless an explicit checkbox/context-menu tombstone is pending;
- patch metadata when necessary;
- replace bytes only when the trusted source changed or a missing cloud file is deliberately restored;
- never call it stale merely because it was filtered, skipped, or reordered.

### Lost local `cloud_id` — identity repair

When a local row has no `cloud_id` but a unique remote row shares the same `desktop_id`, matches owner scope, and matches image type:

- restore the local `cloud_id` from the remote row without uploading bytes;
- do this whether or not the image is currently desired (unchecked images repair too);
- if two or more remote rows share the `desktop_id`, do not auto-repair — log a warning and skip.

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
- the user explicitly chose "remove cloud image, keep measurements."

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

- `UPLOADED` + uncheck queues an unsynced tombstone and shows `DELETE_PENDING` immediately; the storage-excluded set gets the image id;
- recheck before sync cancels that tombstone, preserves the existing cloud ID, removes the image id from the storage-excluded set, and performs no upload or delete;
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

## Incident: truncated bulk reads, August 2026

A bulk image-metadata fetch issued one unpaginated PostgREST GET for the
image rows of many observations at once. The server silently capped the
response at its row limit (1000 rows) while returning HTTP 200, so the
client treated a truncated collection as the complete remote state. Effects:

- newly pulled observations whose image rows fell past the cap appeared to
  have zero images;
- the diff against local state produced false "cloud removed local image
  files" conflicts.

The repair made `_get_paginated` the canonical bulk reader: explicit
`limit`/`offset` paging until a short page, a mandatory deterministic
`order=` clause with `id.asc` tie-breaker, and hard failure (no partial
result) when any page errors. Batched `in.(…)` queries additionally
paginate each batch — batching bounds URL length and is not a substitute
for pagination. Safety rules 14–17 encode the lessons; regression tests
live in `tests/test_cloud_download_only.py` (desktop), including the guard
that a page failure can never yield a page-1-only snapshot.

## Incident: duplicate cloud observations after pull-only import, August 2026

Observations imported by Download from Cloud carry a valid local `cloud_id`
while the remote `desktop_id` remains NULL (pull-only performs zero cloud
writes, so the reverse link is deliberately not written back). The desktop
push path resolved identity only by the reverse link: when such an
observation later became dirty, the `desktop_id` lookup found nothing, the
client POSTed a new cloud observation, and the caller overwrote the correct
local `cloud_id` with the duplicate's id. The next pull then saw the original
cloud row as unlinked and imported it as a second local observation. Fourteen
duplicate cloud observations were created before the defect was found.

The repair established a canonical push-identity resolver: a verified local
`cloud_id` is the primary identity (PATCH, never POST); the reverse
`desktop_id` link is recovery-only and must match uniquely; disagreement or
ambiguity fails safely and keeps the observation retryable. Safety rules
20–23 encode the lessons; regression tests live in
`tests/test_observation_push_identity.py` (desktop).

## Repair plan

### Phase 0 — stop further damage (shipped)

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

### Phase 2 — make cloud selection explicit (Stage 1 shipped)

Stage 1 implemented on desktop:

- The gallery "Keep image in Sporely Cloud" checkbox now persists to a dedicated per-observation setting `sporely_cloud_image_storage_excluded_ids_<obs>`, separate from `artsobs_publish_excluded_image_ids_<obs>`.
- A canonical predicate `cloud_image_bytes_desired(observation_id, image_id)` is the single source of truth for whether cloud image bytes should be sent for one local image.
- `SporelyCloudClient.upload_image_file` and `SporelyCloudClient.upload_original_image_file` refuse to send bytes for an image the user has unchecked, raising `CloudImageBytesNotDesiredError`. The gate fails closed on missing identity: normal-path callers must pass valid `observation_id` and `image_id`; recovery flows opt in by passing `recovery_authorized=True` (the only intentional exception, waiving the identity requirement for auditability).
- `_push_images_for_observation` guards the two byte-upload call sites and the `prepare_images_cb=None` fallback branch with the same predicate.
- Identity repair (`_associate_persisted_cloud_images`) is decoupled from the checkbox and works by unambiguous `desktop_id` match.
- A non-destructive initializer `_initialize_cloud_image_storage_desired_state_for_observation` seeds the storage-excluded set on the first sync path (invoked at the top of `_push_images_for_observation`, immediately after pending image tombstones are pushed, before any candidate filtering or byte-upload boundary) — not on gallery open — so background/headless sync cannot upload the full local microscope set for an observation the user has never viewed. UI still calls the same initializer for display consistency but is not required for correctness. Rules: uploaded images stay desired; delete-pending / deleted images are excluded; local-only field images stay desired; local-only microscope images use a sparse per-magnification default when no image in the group already has a cloud identity.
- Legacy Artsobs publication exclusions are not migrated into the new set.
- The reason enum split: `PENDING_REASON_EXCLUDED` remains for internal `excluded_ids` arguments; the new `PENDING_REASON_NOT_DESIRED_BY_USER` reports gallery-desired-state rejects.

Not yet:

- Fold gallery UI to expose the checkbox as a Sporely Cloud state control (already the case) with matching help text and audit surface.
- Move Artsobservasjoner/iNaturalist publication selection to its own, separately-scoped UI decision surface with its own default rules.
- Surface any legacy `artsobs_publish_excluded_image_ids_<obs>` values that correspond to visible cloud photos as audit hints without mutating cloud state.

### Phase 3 — repair desktop image algorithm (future work)

Desktop:

- Build `kept_cloud_ids` from every active cloud row that maps to an existing local image.
- Use `prepared_items` only for upload work.
- Remove "delete every remote row not kept" from ordinary sync.
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
- recovery copies never overwrite local originals;
- `cloud_image_bytes_desired` returns the correct answer for every state (uploaded, delete-pending, deleted, local-only field, local-only microscope with and without measurements);
- `SporelyCloudClient.upload_image_file` and `SporelyCloudClient.upload_original_image_file` raise `CloudImageBytesNotDesiredError` for undesired images and pass for desired ones;
- identity repair runs even for undesired images;
- legacy `artsobs_publish_excluded_image_ids_<obs>` values do not migrate into `sporely_cloud_image_storage_excluded_ids_<obs>`.

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
- `utils/cloud_media_recovery.py`
- `ui/observations_tab.py`
- `ui/main_window.py`
- `ui/image_gallery_widget.py`
- `database/models.py`
- `tests/test_cloud_sync_*.py`
- `tests/test_cloud_image_bytes_desired.py`
- `tests/test_cloud_storage_desired_initializer.py`
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
