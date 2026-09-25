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

**Status (2026-09-25, Stage B candidate on `feature/sync-persist-remote-only-fields`).**
The revert is now reproduced end to end through the real `sync_all` (push,
then pull) with real snapshot bookkeeping, for `notes` and `location`: after
Sync the local row kept A while cloud and baseline held B, an idle Sync did
not repair it, and the next unrelated local edit pushed A back to the cloud.
The same probe showed a worse case for fields outside
`_remote_observation_update_kwargs` (`inaturalist_id`, `author`, …): they
were not even kept in the payload, so the same push overwrote the cloud edit.
Fix: after the conflict preflight passes, `push_all` adopts the ordinary
remote-only fields through `_apply_remote_observation_fields` and builds the
payload from the adopted local row; identity adoption is unchanged.
Regression: `tests/test_cloud_sync_remote_only_adoption.py`. Awaiting review;
Stage C (item 4) not started.

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

**Status (2026-09-25, Stage C candidate on `feature/stage-c-identity-clear`,
based on the Stage B candidate `feature/sync-persist-remote-only-fields` at
`53f94b2`).** Reproduced end to end through the real `sync_all` (push, then
pull) with real snapshot bookkeeping: a manual free-text rename or a
genus/species dropdown change away from a proven cloud identity pushed the new
name while leaving the stale `selected_sporely_taxon_id`, and a fresh device's
next pull preserved it as `cloud_selected_unverified` beside the contradictory
name. Fix: `_sync_observation_selected_taxon` now issues an explicit clear
through the atomic `set_observation_identification_v2` RPC (never a bare PATCH)
when the local identity reads `no_identity_evidence`, the stored sync baseline
proves this desktop last synced a proven Sporely identity, and the committed
genus/species has changed against that same baseline; an unrelated edit, a
picker re-selection, a legacy/no-baseline row and a preserved
`external_unresolved` identity are all left alone. `push_all` now threads the
stored baseline into `push_observation` unconditionally (not only when the
cloud has diverged), since the clear is triggered by a local change against
the baseline, not a remote one. Regression:
`tests/test_cloud_sync_identity_clear.py` (9 cases; 4 fail on pre-fix
`53f94b2`: manual rename, dropdown change, fresh-device download, second-sync
convergence). Focused identity/conflict/Stage-B suites and the full desktop
suite pass with the same 33 pre-existing failures and 54 pre-existing errors
as the `53f94b2` baseline (unrelated Qt/GUI fixture issues). Awaiting review.

**Round 2 (2026-09-25, `feature/stage-c-identity-clear-v2` after Stage B
merged to main at `53f94b2`).** Two follow-ups on the reviewed candidate,
neither changing the production diff carried over from round 1 (identical
`utils/cloud_sync.py` diff before and after the rebase, confirmed byte for
byte):
1. `_maybe_clear_stale_cloud_identity` now fails closed when the cloud's
   CURRENT selected identity is neither empty nor still the baseline (a third
   concept C moved in since) — logs and withholds the clear instead of
   wiping C's name/identity/shared-reference contributions. Confirmed at
   runtime that push_all's existing preflight conflict classification already
   intercepts this exact scenario before `push_observation` is reached; the
   new check is a second, independent fail-closed layer, not dead code
   reachable only in theory.
2. **Case F closed**: no usable identity baseline (no stored snapshot, or one
   that predates identity tracking), cloud holds identity A, local's own
   committed genus/species (no identity claim) contradict A. Runtime-proven
   defect: the existing no-baseline identity check only fires for a local row
   that already CLAIMS a conflicting identity, so a non-claiming local's
   genus/species PATCHed onto the cloud unconditionally while
   `selected_sporely_taxon_id` stayed A; the following pull then adopted A
   locally too (its own `identity_fail_closed` guard has the same gap). Fixed
   with two symmetric guards (`push_all`, `pull_all`'s missing-snapshot
   branch) using the SAME existing conflict-review mechanism
   (`_format_review_needed_error`, `_set_observation_conflict_review_pending`,
   `CONFLICT_REVIEW_PENDING_MARKER`) — no new conflict system. Regression:
   `tests/test_cloud_sync_no_baseline_identity_contradiction.py` (6 cases, all
   fail on pre-fix); confirms the cloud row is byte-for-byte unchanged, an
   unrelated field on the same observation is also blocked (contrasted with
   the already-accepted partial-push policy for the claim-vs-claim case,
   cited in the test), an explicit picker resolution to the cloud's own
   concept clears the conflict and syncs normally, resolving to a genuinely
   different concept instead falls into that pre-existing claim-vs-claim
   review policy (ordinary fields go out, identity withheld) rather than
   silently binding or clearing, and a further manual free-text rename does
   NOT clear the conflict (Stage C's explicit-clear condition requires a
   known baseline, which Case F lacks by definition — only a picker
   resolution clears it).

**Round 3 (2026-09-25, same branch).** Three more follow-ups:
1. **Case F compatibility semantics.** A blank/missing local genus or species
   is UNKNOWN, not contradictory — the known production shape is a bound
   Sporely concept with no local genus/species. `_identification_contradicts_remote`
   replaces the plain inequality; a populated, differing component is still a
   true contradiction and still blocks. When compatible, the push side now
   adopts the cloud's fields/identity onto local first (mirroring the pull
   side's existing full apply), so the still-partial local payload never
   PATCHes the cloud's canonical name down to blank.
2. **Privacy while blocked.** Runtime-reproduced release blocker: a true
   Case F contradiction blocked the WHOLE push, including visibility, so
   marking a blocked observation private never reached the cloud (stayed
   public). Fixed with the smallest safe rule: a strictly MORE restrictive
   visibility change (narrowing) may propagate through a single scoped PATCH
   of only the `visibility` column while blocked; a widening change stays
   blocked. Never carries identification/name/any other field, never clears
   the conflict marker, never stores a baseline.
3. **Rate-limit false success, closed.** Runtime-proven against a real local
   Supabase: `observation_taxon_shared_reference_rate_row_trg`
   (sporely-web migration 20260830193144) can cancel the identity RPC's own
   row update while PostgREST correctly reports HTTP 429 — the existing
   retry/exception layer either succeeds for real after backoff or raises
   `CloudTemporarilyUnavailableError` (no false success observed there). The
   real defect: the ordinary genus/species half of `push_observation`'s
   payload lands via a separate, unconditional PATCH before the identity RPC
   runs, so when the identity RPC then fails (rate limit or otherwise), the
   cloud is left with the NEW name beside the OLD identity — the exact
   contradiction Stage C exists to prevent, reintroduced through a failed
   retry. Fixed at the narrowest layer: `clear_observation_selected_taxon`
   now reads the row back after the RPC and fails closed
   (`CloudSyncError`) if the identity is still attached, regardless of what
   the RPC call itself reported; push_all routes that specific failure
   through the existing conflict-review marker instead of a plain dirty
   flag, and pull_all's own convergence check (only at the point where it
   decides synced-vs-dirty, not the field-merge steps before it) now leaves
   an observation carrying that marker dirty instead of silently
   re-stamping it synced just because the (already-patched) ordinary fields
   happen to agree — otherwise the SAME sync cycle's pull immediately undid
   the push-side fail-closed signal. No server-side/migration change is
   needed based on what was observed; the desktop read-back is genuine
   defence-in-depth for a theoretical silent-suppression case the runtime
   proof did not actually exhibit.

**Round 4 (2026-09-25, same branch).** Three more follow-ups on the round-3
mechanisms:
1. **Rejected narrowing-while-blocked must fail closed.** The Case F
   privacy-while-blocked exception (`_push_narrower_visibility_while_blocked`)
   issued its scoped visibility-only PATCH and, on any failure (e.g. a
   privacy-slot-limit quota rejection), only logged a warning — nothing
   recorded the failure anywhere a caller or a later sync could see. Fixed:
   the function now returns an outcome (`'patched'` / `'lost_race'` /
   `'failed'` / `None`), and the caller records a failure through
   `_set_observation_sync_error_detail_only` (sets `sync_error_code`/
   `sync_error_message` only, leaving `sync_status`/`sync_blocked_reason`
   untouched so the Case F conflict-review marker stays authoritative) and
   appends to `errors`, instead of only a log line.
2. **Narrowing must use fresh state.** `remote` is a snapshot fetched once,
   early in the sync cycle (`push_all`'s bulk `remote_lookup`), not
   immediately before the write — a concurrent write to the SAME
   `visibility` column in between could have been silently overwritten by
   a blind PATCH. Fixed with a PostgREST equality precondition
   (`visibility=eq.<expected>`, `Prefer: return=representation`, via the
   new `SporelyCloudClient._patch_with_precondition`): a lost race reports
   success with an empty body, never an error, so the caller inspects the
   returned rows and treats zero rows as "did not take effect" (never
   overwriting the concurrent value, never advancing sync/baseline state).
3. **Failed identity-clear read-back must fail closed for every failure
   shape.** `_verify_identity_clear_landed` already failed closed when the
   read-back explicitly showed the identity still attached. It did not
   fail closed when the read-back call itself raised (transport failure —
   previously uncaught, would have crashed the whole `push_all` loop
   instead of failing just this observation), nor when it returned a
   malformed/incomplete row (missing `selected_sporely_taxon_id` entirely)
   or `None` for a row that must exist (the RPC just ran against this
   exact `cloud_id` without raising) — those were previously treated as
   "nothing to enforce" and silently let the clear count as confirmed. All
   three now raise the same `CloudSyncError`, distinguished from a
   genuinely confirmed-empty read-back (a well-formed row explicitly
   reporting no selected identity), which still counts as success.

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
