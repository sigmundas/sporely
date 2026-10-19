# Observation identity backfill

Written 2026-10-18 on `feature/identity-backfill-dry-run`, based on
`feature/nortaxa-self-identity` (`a4e8131`). This plan repairs historical
observation identities with the existing audit tool
(`database/audit_observation_identity.py`). No new taxonomy subsystem.

Nothing in this plan authorizes a production database write or a cloud sync.
Each write and each sync waits for the owner's explicit go-ahead at its gate.

## Owner decisions (2026-10-18)

- **Approved direction:**
  - 125 bindings through reviewed NorTaxa→COL bridges (`reviewed_bridge`);
  - 25 bindings through NorTaxa self-identity where the name is unique in
    the release (`nortaxa_self_unique_name`).

  Both are subject to backup and apply checks.
- **The 199 same-name bindings** (a COL concept shares the name), split:
  - **16** `same_name_recorded_identity`: the NorTaxa identifier is the
    observation's own recorded identity (13 accepted on web or Android,
    3 by a desktop Copy). These may be approved separately.
  - **183** `same_name_ai_history_only`: the identifier exists only in the
    AI history, and only name agreement links it to the identification.
    **Deferred.** The manifest refuses this group.
- **Deferred and kept in the report:**
  - the 144 name-only candidates;
  - the 33 ambiguous names.
- **Approved repairs are marked `dirty`** in the same transaction
  (`--mark-dirty`).

## Why dirty marking is needed (current `main` sync behaviour)

- The push selects only rows that are `dirty` or have no cloud ID
  (`utils/cloud_sync.py`, `push_all`).
- The pull reconciles an observation only when the cloud copy changed.
  Otherwise it stamps the row `synced` again.
- So an unmarked repair would stay local until an unrelated cloud change.
- A marked row is classified `local_only` by
  `_classify_identity_sync_change`, and is pushed through
  `set_observation_selected_taxon_v2`.

## Tooling (this branch)

- `--manifest-out` / `--manifest-groups` / `--batch-size` / `--batch` write a
  deterministic approved-batch manifest. It is bound to the dry run's
  artifact digest, and rows are ordered by observation ID.
- `--approved-manifest` with `--apply` writes only those rows. The manifest
  is refused if:
  - the digest differs;
  - it names a row outside the approvable groups.
- `--mark-dirty` sets bound rows with a cloud ID to `dirty` and clears their
  `synced_at`, in the same transaction.
- `tools/sync_push_preview.py` lists everything a global Sync would push. It
  runs the app's own pre-push scans on a copy.

## Rehearsal on a copy (2026-10-18)

Batch 1 was rehearsed on a copy, `reviewed_bridge`, 5 rows: 24, 211, 372,
377, 381.

- Bound with proof `external_id_resolution`; `dirty`; `synced_at` cleared.
- No other row changed.
- The re-audit reconciles (proven 3 → 8).
- Re-applying the same artifact is refused as stale.
- Restoring the backup is byte-identical.
- None of these rows has images missing from the cloud.

## NorTaxa concepts missing from the cloud release (option A)

The scoped cloud release (`tax-2026.10.07-01`, 52,917 concepts) contains
none of the 12,566 NorTaxa-only concepts. The guarded RPC refuses them with
`22023`, which previously failed the observation's push.

The fix is in `utils/cloud_sync_impl/identity_push.py`:
- the row's NorTaxa tuple is preserved in the cloud as `external_unresolved`
  through `set_observation_identification_v2`, without writing the name;
- the write is skipped when the cloud already holds that tuple;
- any other refusal still fails the push.

Supporting changes:
- the new writer is blocked in pull-only mode;
- the conflict-plan "keep local identity" step records the identity read back
  from the cloud;
- contract rule 28 is updated in both repositories.

**Live canary (2026-10-18), observation 1032 → cloud 1502:**

- **NorTaxa identity canary: PASS.**
  - The cloud holds `external_unresolved`
    `nortaxa/nortaxa_taxon_id/52330` (`NBIC:52330`), with the name unchanged.
    Verified by the owner against live Supabase.
  - The desktop kept 624659 (`external_id_resolution`) after the pull.
  - No identity error was reported.
- **Overall sync/image completion: FAIL, tracked independently.** New image
  rows (7774, 7775, plus those of 1013 and 1016) were created in the cloud
  but never linked locally (`ImageBaselineMissingError` on
  normalization-only differences; released v0.9.27 defect). This stage does
  not claim that the complete sync canary passed.

## Pilot batch 1: passed (2026-10-18)

`reviewed_bridge`, 5 rows: 24, 211, 372, 377, 381.

- **Backup:** `~/Documents/Sporely-backups/identity-backfill-pilot-20261018T184529Z`
  (SHA-256 `3c834a50…621335`, integrity ok).
- **Reviewed artifact:** digest `d640f234…1fff4ce`.
- **Apply:** `identities_bound 5, marked_dirty 5, rows_written 5`. The
  database diff against the backup was confined to those 5 rows' identity
  and sync columns. The re-audit reconciled: proven 3 → 8, bindable 349 → 344.
- **Owner-authorized sync:** exactly those 5 observations, identity RPCs
  only.
  - Cloud baselines record `sporely:614315 / 98451 / 34615 / 105619 / 146432`.
  - The owner verified the cloud concepts and the active release live.
  - All 5 are `synced`.
  - The `images`, `spore_measurements` and other tables are unchanged.
  - The final preview showed nothing pending.

**Note:** the sync summary line "touched N dirty observation(s) with no real
change (patched=0, …)" does not count the identity RPC. Like the measurement
`pushed=N` counter (see
`docs/verification/2026-10-desktop-metadata-known-risks.md` on
`fix/image-create-link`), it is misleading but not a correctness problem.
Low priority.

## Batch 2 part 1: passed (2026-10-18)

`reviewed_bridge`, 39 rows. Observation 419 was cut into this batch but
excluded by the owner: the Artsorakel selection it was copied from could not
be confirmed. It stays a manual-review item and was verified unchanged
locally and in the cloud (cloud 712, identity all NULL).

- **Backup:** `~/Documents/Sporely-backups/identity-backfill-bridge-part1-20261018T190530Z`
  (SHA-256 `dda5d7a6…28f1`, integrity ok).
- **Reviewed artifact:** digest `43f0362d…`; manifest `bridge-39-part1.json`.
- **Apply:** `identities_bound 39, marked_dirty 39, rows_written 39`. The
  database diff was confined to those rows' identity and sync columns.
  Re-audit: proven 47, bindable 305.
- **Owner-authorized sync:** 39 candidates, all `dirty` → `synced`, no
  errors, no image creates or byte uploads.
  - All 39 cloud baselines record the approved concept. The owner verified
    `selected_sporely_taxon_id` live: 39/39 match.
  - `images`, `spore_measurements` and all other tables are unchanged since
    the backup. Only the 39 observation rows and sync bookkeeping settings
    differ.
  - "Metadata patch" log lines on observations 456, 460, 462, 502 and 505
    changed no stored cloud image field (existing-link reconciliation).
  - The final preview showed nothing pending.

**Unexpected: 7 spore mosaics rebuilt and re-uploaded.** Observations 518,
525, 537, 566, 568, 570 and 637 (cloud 815, 822, 841, 873, 875, 879, 957), all
within the 39. The preview did not report mosaics at the time.

- **Cause:** their stored `mosaic_signature` was already stale before the
  apply. Recomputing on the pre-apply backup yields the new signatures, and
  no source file changed. Any push of these observations would have rebuilt
  them; the backfill only made them push.
- **Effect:** the existing v4 mosaic row is updated in place and the
  superseded storage object removed (`utils/cloud_sync_impl/spore_mosaic.py`).
  The owner verified one v4 row per observation, with the logged storage key.
- **Follow-up (low priority):** older v1/v2 mosaic rows also exist for these
  observations (2–3 rows each). They are other pipeline versions, not
  duplicates. Whether they and their storage objects should be cleaned up is
  outside this plan.
- **Preview:** `tools/sync_push_preview.py` now reports `mosaic_rebuilds`.
  Run retrospectively on the pre-sync copy of this batch, it predicts exactly
  these 7.

## Batch 2 part 2: passed (2026-10-19)

40 `reviewed_bridge` rows, 653…838; 419 excluded again.

- **Backup:** `~/Documents/Sporely-backups/identity-backfill-bridge-part2-20261018T194803Z`
  (SHA-256 `66d8f8ff…550aef`, integrity ok).
- **Reviewed artifact:** digest `3003e3f0…`; manifest `bridge-40-part2.json`.
  All 40 concepts are in the cloud release; none had a cloud identity.
- **Rehearsal on a copy:** `identities_bound 40, marked_dirty 40`; the preview
  predicted exactly the 40 and `mosaic_rebuilds` 670, 676, 696, 697, 732, 835.
- **Apply:** `identities_bound 40, marked_dirty 40, rows_written 40`,
  `names_restored 0`. The database diff was confined to those rows' identity
  and sync columns. Re-audit: proven 47 → 87, bindable 305 → 265.
- **Pre-sync preview (07:13 UTC):** exactly the 40, the due pending-image scan
  added nothing, no image creates or uploads, no new measurements, no deletes,
  and the six predicted mosaic rebuilds. The owner approved this scope.
- **Owner-authorized sync:** 40 candidates, all `dirty` → `synced`; the
  pending-image scan ran (`re_dirtied=0`); calibrations `pushed=0`; no image
  uploads, creates or metadata patches; no issues reported.
  - Exactly the six predicted mosaics were rebuilt (`Mosaic done` on 670, 676,
    696, 697, 732, 835), with no cleanup failure. Duplicate v4 rows are
    excluded by `UNIQUE (observation_id, version)`.
  - The owner verified `selected_sporely_taxon_id` live: 40/40 match. Cloud 712
    (local 419) is still NULL.
  - Locally: all 40 `synced` with their cloud baselines on the approved
    concept; `mosaic_signature` changed only on the six; `images`,
    `spore_measurements` and every other table are unchanged since the backup.
    Settings changes are sync bookkeeping only.
  - The final preview showed nothing pending and no mosaic rebuilds.

## Batch 2 part 3: passed (2026-10-19)

The last 40 `reviewed_bridge` rows, 840…1004; 419 excluded again. Only 419
remains in the group.

- **Apply:** `identities_bound 40, marked_dirty 40, rows_written 40`,
  `names_restored 0`; diff confined to those rows' identity and sync columns.
  Re-audit: proven 87 → 127, bindable 265 → 225.
- **Owner-authorized sync** of the previewed scope: 40 candidates, all
  `synced`; calibrations `pushed=0`; no image uploads, creates or patches; no
  issues. Exactly the two predicted mosaics were rebuilt (901, 914), with no
  cleanup failure.
- **Verification:** the owner confirmed 40/40 `selected_sporely_taxon_id`
  live and cloud 712 still NULL. Locally all 40 have cloud baselines on the
  approved concept; only the 40 observation rows and sync bookkeeping changed
  (including a new storage-intent list for 880); `images`,
  `spore_measurements` and every other table are unchanged. The final preview
  showed nothing pending.

Preparation:

- **Backup:** `~/Documents/Sporely-backups/identity-backfill-bridge-part3-20261019T075946Z`
  (SHA-256 `1b74e357…50ca8c`, integrity ok).
- **Reviewed artifact:** digest `f760f385…`; manifest `bridge-40-part3.json`.
  All 40 concepts are in the cloud release; none has a cloud identity yet.
- **Rehearsal on a copy:** `identities_bound 40, marked_dirty 40`. The preview
  lists exactly the 40; 241 unlinked microscope images, none byte-desired and
  none with measurements; `mosaic_rebuilds` 901 and 914.

## NorTaxa-only identities: prepared, not authorized (2026-10-19)

Both groups bind to the NorTaxa-only concept. These concepts are absent from
the cloud release, so the sync takes the option-A fallback: the guarded RPC
refuses them, and the NorTaxa tuple is written as `external_unresolved`
unless the cloud already holds it. No COL equivalence is chosen.

- **Group A, `nortaxa_self_unique_name` (24 rows):** the identification name
  maps to exactly one concept. The NorTaxa ID comes from the Artsorakel
  suggestion the user selected: 16 from `ai_selected_taxon_id`, 7 recovered
  from the selected prediction (name equal to the selection), 1 recorded
  identity (1025).
- **Group B, `same_name_recorded_identity` (16 rows):** the row's own recorded
  NorTaxa identity; a same-name COL concept exists in the cloud release and is
  not chosen (D3). 13 already hold this tuple in the cloud; 3 (973, 992, 995)
  are recorded only on the desktop.
- **Observation 701 excluded until the image-link fix lands.** Its public
  microscope image 5042 is unlinked and carries 3 `other` measurements without
  cloud IDs. With the server's owner-sync parent capability, a push would
  create a metadata-only anchor and push those measurements: the image-create
  path that fails in v0.9.27 and is fixed only on `fix/image-create-link`.
- **Backup:** `~/Documents/Sporely-backups/identity-backfill-nortaxa-20261019T081717Z`
  (SHA-256 `d3856f9d…5cfb58c`, integrity ok).
- **Reviewed artifact:** digest `94757a60…`. Manifests `nortaxa-A-unique-24.json`,
  `nortaxa-B-recorded.json` and `nortaxa-AB-40.json` (one apply, whichever is
  approved).
- **Rehearsal of A+B on a copy:** `identities_bound 40, marked_dirty 40`,
  `names_restored 0`; the prior NorTaxa ID and raw ID are kept on every row
  that had one. The preview lists exactly the 40; 150 unlinked microscope
  images, none byte-desired or with measurements; no new measurements;
  `mosaic_rebuilds` 639, 668, 816, 826 (A) and 930, 985 (B).

## Gate procedure

The commands are in the owner hand-off that accompanies this plan. In order:

1. **Read-only cloud check** (owner, Supabase SQL): active release, concept
   presence, current cloud identity of the affected rows.
2. **Deferred Test 3** (NorTaxa round trip, observation 1032). Run the sync
   preview first; the owner authorizes the listed upload explicitly.
3. **Batch 1:**
   - quit Sporely;
   - `release-ops/sporely-desktop-backup.sh`;
   - dry run with the manifest;
   - review;
   - apply;
   - validation queries;
   - sync preview, then owner-authorized Sync;
   - cloud verification.
4. Later batches repeat step 3 with `--batch N`. Each needs a fresh dry run,
   because an applied batch makes the previous artifact stale.

**Rollback:** quit Sporely and restore the backup file from step 3. Already
synced cloud identities are not reverted by this. Each would need a new
explicit selection.

**Watch:** the pending-image repair scan (24 h) runs during a sync when due.
It may re-dirty observations with local images not yet uploaded: 2,471 such
images exist, as an upper bound. Run the preview immediately before every
Sync.

Pushing an observation also rebuilds its spore mosaic when the stored
signature is stale. The preview's `mosaic_rebuilds` lists these; report them
to the owner with the pending list.
