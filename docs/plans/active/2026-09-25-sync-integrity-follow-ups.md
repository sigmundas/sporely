# Sync and privacy integrity follow-ups (from the taxonomy-v2 closeout)

Status: planned, not started. Each item is its own stage with its own
reproduction, fix, review and (where it touches auth/RLS/visibility/deletion)
security review.

Found while auditing and fixing observation 917 during the taxonomy-v2
closeout (sporely-py `feature/taxonomy-v2-identity-reconciliation` at
`4d15960`, sporely-web `feature/owner-sync-metadata-parents` at `94c7f2f`).
None of these was introduced by that work; they were exposed by it.

Priority order was set by the owner on 2026-09-25.

| # | Item | Repo(s) | Severity | Evidence so far |
|---|---|---|---|---|
| 1 | Measurement writes do not require owning the image | sporely-web | Release blocker (security) | Reproduced by the security reviewer at runtime |
| 2 | Measurement deletions do not sync | sporely-py (+ web contract) | Data integrity | Code reading only — reproduce first |
| 3 | Cloud-only field changes are not rebased locally on push | sporely-py | High (silent overwrite) | First half proven by probe; revert inferred |
| 4 | Manual desktop rename does not clear cloud identity | sporely-py | Taxonomy correctness | Code reading |
| 5 | Community/stats functions hard-code the spore type set | sporely-web | Required before publishing cystidia | Listed in migration 20260925120000 header |

---

## 1. Measurement writes do not require owning the image (release blocker)

**Problem.** `spore_measurements` insert/update RLS
(`20260803120000_lock_down_observation_sync_tables.sql`,
`spore_measurements_owner_insert` / `_owner_update`) checks only
`user_id = auth.uid()`. It does not check that the caller owns `image_id`;
the foreign-key check bypasses RLS. Any signed-in user who knows or
enumerates an image id can attach measurements to someone else's image.

**Impact.** Public SECURITY DEFINER RPCs count every spore-type measurement on
an eligible image regardless of who wrote it, so a foreign user can inject
points, counts and summary values into another user's public observation
(`_get_public_observation_stage2a`, `search_public_observations`, species and
distribution summaries, map points, comparison set, mosaics, community spore
datasets, `get_person_stats`). Migration 20260925120000 already stops this from
unlocking owner-sync parents (its helpers count only `m.user_id = i.user_id`),
but the general injection remains.

**Proposed fix.** See `sporely-web/docs/proposals/spore-measurement-image-ownership-rls.md`
(on `feature/owner-sync-metadata-parents`): a new migration adding
`EXISTS (image owned by auth.uid())` to both policies' `WITH CHECK`.

**Before applying.**
- Audit existing cross-owner rows
  (`spore_measurements m JOIN observation_images i ... WHERE m.user_id <> i.user_id`)
  and decide separately what to do with them; never delete automatically.
- Confirm no legitimate client writes measurements on another user's image.
- Consider also filtering `m.user_id = i.user_id` in the public RPCs as
  defence in depth for rows that predate the policy change.

**Tests.** RLS tests for a foreign insert and a foreign re-parenting update;
public-RPC tests showing an injected foreign row does not change counts.

**Review.** `sporely-security-reviewer` required.

---

## 2. Measurement deletions do not sync (reproduce next)

**Problem (from code reading, not yet reproduced).**
- `MeasurementDB.delete_measurement` (`database/models.py`) deletes the local
  row and marks the observation dirty, but records no cloud deletion intent.
- `_push_measurements_for_observation` upserts by `desktop_id` and never
  deletes cloud rows that have no local counterpart. Its docstring's "stale
  cloud rows ... are cleaned up" is out of date.
- `_import_remote_measurements_for_observation` never deletes local
  measurements that the cloud no longer has.

**Likely consequences.**
- A measurement deleted on device A stays in the cloud.
- It may be re-imported on device A's next pull.
- It survives on device B, and B's next push can re-upload it.
- Parent retirement (`_retire_unneeded_owner_sync_parent`) is therefore
  rarely reachable, because it requires zero cloud measurements.

**Next step.** Prove the mechanism at runtime before designing a fix: delete
one measurement on device A, sync, inspect cloud, sync device B, sync A again.
Record which of the consequences above actually occur.

**Design questions for the fix.**
- Durable local deletion intent (a measurement tombstone), analogous to image
  tombstones, pushed before measurement upserts.
- Pull-side handling of cloud-deleted measurements: a complete, successful,
  paginated read is required before absence may mean deletion
  (`.claude/rules/cloud-sync.md`).
- Interaction with the three-way measurement snapshot and conflict review.
- Web client deletions must follow the same contract.

**Review.** `sporely-reviewer`; `sporely-security-reviewer` because it
touches deletion.

---

## 3. Cloud-only field changes are not rebased locally on push (high)

**Problem.** In `push_all`, when the three-way analysis classifies an
ordinary observation field as remote-only (edited only in the cloud), the
value is merged into the push payload but is not written to the local row.
The post-push snapshot then records the cloud value as the baseline while
the local row keeps the old value.

**Evidence.** A throwaway probe on the preflight harness confirmed that after
the push the local `notes` stayed at the baseline value while the cloud had
the edited value. The next step — a later dirty push classifying the stale
local value as local-only and sending it back over the cloud edit — is
inferred by analogy with the identity case (which was reproduced and fixed in
`09d5aeb`: push now adopts a remote-only identity locally). It has not yet
been run for ordinary fields.

**Next step.** Reproduce with two consecutive pushes and real snapshot
bookkeeping (the `_SnapshotStore` pattern in
`tests/test_cloud_identity_fail_closed.py`).

**Likely fix.** Adopt remote-only fields locally before pushing, through the
canonical field-apply path, exactly as the identity fix does; keep the
no-no-op-write rule.

**Review.** `sporely-reviewer`.

---

## 4. Manual desktop rename does not clear cloud identity (taxonomy correctness)

**Problem.** When the owner renames an observation by typing a new name on
the desktop, the local identity becomes manual/unresolved. The push gate
(`_sync_observation_selected_taxon`) only asserts proven identities and
treats everything else as "skip, never clear". The cloud row can therefore
end up with the new genus/species and the old `selected_sporely_taxon_id`.
Contract §28 ("Selecting a new candidate is a change of identification")
expects the previous concept to be cleared.

**Design questions.**
- How the desktop expresses an explicit clear through the guarded RPC
  (`set_observation_selected_taxon_v2(p, NULL)` or
  `set_observation_identification_v2`), distinct from "unproven, skip".
- Distinguishing a deliberate rename from an identity the desktop merely
  cannot prove (legacy integer, cloud-selected unverified).
- Interaction with the identity-in-change-detection rules (a clear is a
  local change and must not be downgraded or conflict-looped).

**Review.** `sporely-reviewer`; `sporely-security-reviewer` (identity RPC).

---

## 5. Community/stats functions hard-code the spore type set

**Problem.** Migration 20260925120000 routes every function it redefines
through `public.is_public_microscopy_measurement_type`, but these still
inline `('manual','spore','spores')`: `get_community_spore_dataset`,
`search_community_spore_datasets`, `community_spore_taxon_summary`,
`get_person_stats`. They fail closed, but adding cystidia to the single
definition would not reach them.

**Required before** any cystidia or other non-spore measurement is published
on sporely.no. That publication is itself a separate, explicit decision, and
it also needs thumbnails/mosaics for non-spore types and an owner-facing
publish control that sets `metadata_purpose = 'public_microscopy'`.

**Fix.** Redefine those functions to call the shared definition; prove
identical behaviour with before/after function-body diffs and their SQL
tests.

**Review.** `sporely-security-reviewer` (public visibility).
