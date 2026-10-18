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
