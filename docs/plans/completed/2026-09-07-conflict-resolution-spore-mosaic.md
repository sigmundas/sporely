# Spore mosaic after conflict resolution

## Current stage / handoff

### Stage mosaic-1 — independently accepted — 2026-09-08

Fresh-session `sporely-sparring` review of the second follow-up correction
below, independent of the implementation session. Verified directly against
code, not accepted on the report alone: `_mosaic_render_state_unverified`
(`utils/cloud_sync.py:13930-13998`) contains the image_type check exactly as
described, wired into Step 9b at `utils/cloud_sync.py:14816`, guarding
against the mosaic SQL's local-only `image_type = 'microscope'` selection
(`utils/cloud_sync.py:23177-23206`) exactly as claimed. Read
`test_unresolved_image_type_disagreement_skips_mosaic`
(`tests/test_cloud_conflict_plan_execution.py:795-835`) directly: it builds
the plan through `_build_plan_from_automatic_decisions`, gives the matched
measurement identical geometry/width/p1-p2 and only the remote image's
`image_type` differing, and asserts the mosaic call never fires — a genuine
reproduction of the gap, not a coincidental exclusion. Independently reran
the full required suite (271 passed), `py_compile`, and `git diff --check`
in both `sporely-py` and `sporely-web` — all clean, matching the report.
Confirmed both `docs/supabase-sync-contract.md` copies carry the identical
53-line "Public spore mosaic after conflict resolution" section.

The user reports all 3 numbered manual tests in
`.sparring/prompts/sporely-py/stage-mosaic-1.md` pass, including the
disposable-observation mosaic-after-conflict-resolution scenario this
correction specifically targeted.

`AGENTS.md` and `docs/plans/active/2026-07-12-parmasto-matching-foundation.md`
carry unrelated pre-existing uncommitted changes that predate this stage and
are not part of it; left uncommitted, untouched.

Commit: `6db603c2f987423a87360ee34b679ab453737b71`. Prompt archived to
`.sparring/prompts/sporely-py/completed/stage-mosaic-1.md`. Stage mosaic-1 is
closed. Retained-asymmetry atlas support, import-only mosaic generation, and
narrowing the accepted-asymmetry guard to microscope-relevant asymmetry only
remain deferred to a future stage, unchanged from the implementation report.

### Implementation report reviewed above

2026-09-07 second follow-up correction: **Image-type guard gap closed; still
uncommitted, prompt still pending, acceptance requires a fresh independent
review.** Starting HEAD unchanged: `5faf758680800b4f2ff781657eb43a2aed18c2e5`.

Reproduced the review's exact scenario first, via the second reviewer's own
hermetic fixtures: image 5 local type `microscope`, remote type `field`,
identical scale, matched measurement 99 with identical geometry, `width_um`,
and valid `p1`/`p2`; automatic upload of separate local-only measurement 42.
Before this fix `push_spore_mosaic` still fired, confirming
`_mosaic_render_state_unverified` checked only the local image's type.

Added one check to `_mosaic_render_state_unverified`
(`utils/cloud_sync.py:13930`, ~7 new lines immediately after the existing
remote-image lookup, before the scale-tolerance loop): once a cloud-linked
microscope-owned measurement's remote counterpart is found, its remote
image's `image_type` must also equal `'microscope'`; any other value (or a
missing remote image, already handled) counts as unresolved render state and
skips the mosaic. The mosaic SQL (`utils/cloud_sync.py:23177-23206`) selects
purely on the *local* image's type, so this closes exactly the gap the
reviewer found — a local image already reclassified to microscope, with a
cloud side that still disagrees, could otherwise render a tile the cloud
side never approved. No second renderer, no new persisted eligibility
format, no change to automatic-conflict ownership — reuses the same
reconciled state and the same function.

Also corrected the fixture defect the same review flagged: the three
existing render-state-guard tests (`test_unresolved_matched_measurement_...`,
`test_unresolved_render_relevant_image_scale_...`,
`test_fully_reconciled_matched_measurement_and_image_mosaic_still_runs`) all
omitted `width_um` on measurement 99, and the first also omitted `p1`/`p2` —
fields the real mosaic SQL requires, so an unpatched guard would have passed
these tests for the wrong reason (the row would already have been excluded
by the SQL, not proven safe by the guard). All three now set `width_um` and
valid `p1`/`p2` on both the local and remote measurement 99 row, isolating
each test's actual unresolved difference (length, image scale, or — new —
image_type) as the only disagreement. All three, plus the new
`test_unresolved_image_type_disagreement_skips_mosaic`, now build their plan
through the real `_build_plan_from_automatic_decisions`
(`utils/cloud_sync.py:11399`) instead of a hand-built `items` list, per the
review's explicit request, so the plan shape matches what
`finalize_sync_candidates` actually produces before the conflict dialog
opens.

For "actual helper-selection coverage" (not just call-routing through the
stubbed `_push_spore_mosaic_for_observation` in `_patch_common`): rather than
duplicating the render pipeline inside the conflict-plan test file, this
correction relies on the already-existing, unmodified
`tests/test_cloud_spore_mosaic_unchanged_sync.py::test_successful_rebuild_persists_signature_only_after_all_writes`,
which calls the real `_push_spore_mosaic_for_observation` against a real
SQLite fixture seeded with a microscope image and a measurement row carrying
the identical field set now used in the corrected conflict-plan fixtures
(`cloud_id`, `length_um`, `width_um`, `p1`-`p4`, image `cloud_id` +
`scale_microns_per_pixel`) and asserts `MOSAIC_STATUS_GENERATED` — i.e. the
real SQL does select this row shape. That test was not changed by this
correction; it is cited here as the proof the corrected fixture shape is
real-SQL-eligible, not re-implemented.

Verification: `./.venv/bin/pytest -q tests/test_cloud_conflict_plan_execution.py
tests/test_cloud_spore_mosaic_unchanged_sync.py tests/test_cloud_spore_mosaic_signature.py
tests/test_cloud_download_only.py tests/test_cloud_sync_fast_path.py
tests/test_cloud_sync_dirty_loop_steady_state.py tests/test_image_tombstones.py
tests/test_cloud_image_bytes_desired.py` — 271 passed (102 in the
conflict-plan file, up from 101: one new image-type-disagreement test).
`py_compile` clean on both changed files. `git diff --check` clean in both
repos.

Updated the "Public spore mosaic after conflict resolution" section in both
`docs/supabase-sync-contract.md` copies (sporely-py and sporely-web) to
document the image_type check alongside the existing geometry/scale check.

Still human-gated, uncommitted, and pending: none of the three numbered
manual tests have been run since this correction; they still need a human
with a real Sporely Cloud account and a second client. This session does not
accept its own work — acceptance requires a fresh, independent
`sporely-sparring` review. Retained-asymmetry atlas support, import-only
mosaic generation, and narrowing the accepted-asymmetry guard to
microscope-relevant asymmetry only remain deferred, unchanged from before.
Existing unrelated `AGENTS.md`/Parmasto-plan edits remain untouched.

### Independent review that requested this correction

2026-09-07 fresh independent review: **Partly confirmed; mosaic-1 remains open.**
The measurement-geometry and image-scale checks are present, and the reviewer
ran `./.venv/bin/pytest -q tests/test_cloud_conflict_plan_execution.py`: 101 passed.
`git diff --check` passed. HEAD remains
`5faf758680800b4f2ff781657eb43a2aed18c2e5`; stage changes are uncommitted.
The existing Claude handoff identifies this repo, stage, starting HEAD and
correction; no implementation evidence was regenerated by the reviewer.

Remaining finding: `_mosaic_render_state_unverified` (utils/cloud_sync.py:13930)
checks local image type but never checks its remote counterpart's image type.
The canonical mosaic SQL (23200 onward) selects only microscope images.
Hermetic reviewer reproduction using `_patch_common`, `_baseline_from_state`,
and `_RecordingClient`: eligible matched measurement 99 (including width and
p1/p2 coordinates), identical geometry and scale, image 5 local type microscope
versus cloud type field, and a separate measurement 42 upload. Actual result:
only 42 pushed, but `push_spore_mosaic` was called once for observation 1.
This proves the guard allows an unresolved selection-relevant image difference;
the mosaic helper was stubbed, so no actual atlas or network write occurred.
Conservatively guard image-type disagreement before generating the atlas.

Coverage claim is overstated: new tests at 697–824 stub the canonical mosaic
helper; their matched measurement rows lack width_um (the first also lacks
p1/p2), so the real SQL would exclude those rows. They also hand-build items
instead of exercising `_build_plan_from_automatic_decisions` as requested.
Strengthen these fixtures and include actual helper-selection coverage.

The pending stage-mosaic-1 prompt now records this bounded follow-up. No source
patch, acceptance commit, archival, or next-stage advancement. Human checks
have not been reported for this corrected candidate; rerun affected checks
after the remaining correction. A further focused independent stage review is
needed; no additional security specialist is warranted for this eligibility fix.

### Implementation report reviewed below

2026-09-07 follow-up correction: **Guard gap closed; still uncommitted,
prompt still pending, acceptance requires a fresh independent review.**
Starting HEAD unchanged: `5faf758680800b4f2ff781657eb43a2aed18c2e5`.

Reproduced the review's exact scenario first (automatic upload of measurement
42 alongside matched measurement 99, local length 9 vs cloud length 12, 99
absent from `items`): before this fix, `push_spore_mosaic` fired anyway,
confirming the `merged_asymmetry`-only guard is insufficient for matched
differences an automatic plan never names.

Added `_mosaic_render_state_unverified` (`utils/cloud_sync.py`, new helper
placed immediately before `resolve_conflict_plan`, ~65 lines) and wired it
into the existing Step 9b guard at the point `merged_asymmetry` is already
computed: when accepted-asymmetry is empty, this second check walks
`reconciled_local_measurements` for every measurement that is itself
cloud-linked and owned by a cloud-linked `image_type='microscope'` image —
the same identity the mosaic helper's own SQL requires (23100 onward) — and,
for each, compares it against its `reconciled_remote_measurements` /
`reconciled_remote_images` counterpart using the existing canonical
`_measurement_payloads_match` (measurement geometry) and a tolerant numeric
compare of `scale_microns_per_pixel`/`resample_scale_factor` (image render
scale), reusing the module's existing `_MEASUREMENT_FLOAT_REL_TOL`/`_ABS_TOL`
constants. Any mismatch, or a missing remote counterpart, skips the mosaic
step (`skipped_asymmetry`) alongside the pre-existing accepted-asymmetry
guard. No second renderer, no new persisted eligibility/retry format, no
change to automatic-conflict ownership — this only reads state already
fetched at Step 9 and reuses an existing comparison helper.

Added three regression tests to
`tests/test_cloud_conflict_plan_execution.py` (after
`test_mixed_plan_retry_still_skips_mosaic`):
`test_unresolved_matched_measurement_skips_mosaic_without_accepted_asymmetry`
(the reviewer's exact reproduction, red before the fix — verified by
temporarily disabling the new check and confirming it fails, then restoring
it), `test_unresolved_render_relevant_image_scale_skips_mosaic` (same gap via
image scale instead of measurement geometry), and
`test_fully_reconciled_matched_measurement_and_image_mosaic_still_runs`
(a matched, fully-agreeing cloud-linked measurement/image must not
over-block the mosaic). All three use realistic cloud-linked
microscope-image/measurement fixture rows (not a mocked helper call), per
the stage's requirement. The eight pre-existing mosaic tests from the
original mosaic-1 implementation pass unchanged — none of their fixtures
happen to set both a measurement `cloud_id` and a matching cloud-linked
microscope image, so the new check was never coincidentally exercised by
them before this pass, and it does not need to be to leave them green.

Verification: `./.venv/bin/pytest -q tests/test_cloud_conflict_plan_execution.py
tests/test_cloud_spore_mosaic_unchanged_sync.py tests/test_cloud_spore_mosaic_signature.py`
— 150 passed (101 in the conflict-plan file, up from 98).
`./.venv/bin/pytest -q tests/test_cloud_download_only.py tests/test_cloud_sync_fast_path.py
tests/test_cloud_sync_dirty_loop_steady_state.py tests/test_image_tombstones.py
tests/test_cloud_image_bytes_desired.py` — 120 passed. `py_compile` clean on both
changed files. `git diff --check` clean in both repos.

Updated the "Public spore mosaic after conflict resolution" section in both
`docs/supabase-sync-contract.md` copies (sporely-py and sporely-web) to
document the second, matched-difference eligibility check alongside the
existing accepted-asymmetry guard.

Still human-gated, uncommitted, and pending: none of the three numbered
manual tests have been run since this correction; they still need a human
with a real Sporely Cloud account and a second client. This session does not
accept its own work — acceptance requires a fresh, independent
`sporely-sparring` review. Retained-asymmetry atlas support, import-only
mosaic generation, and narrowing the accepted-asymmetry guard to
microscope-relevant asymmetry only remain deferred, unchanged from before.
The separately noted "to review" pre-final-gate summary wording issue is out
of scope for this correction. Existing unrelated `AGENTS.md`/Parmasto-plan
edits remain untouched.

### Independent review that requested this correction

2026-09-07 independent review: **Partly confirmed; mosaic-1 remains open.**
The user reports all prescribed manual tests passed; their supplied log and
visual report specifically show a new measurement upload followed by a 20-tile
mosaic. The dialog did not open in that run, so it demonstrates the automatic
conflict-plan path, not a witnessed manual-choice interaction. These results
are recorded as user-reported passes, not independently observed UI evidence.

Review found a blocking eligibility gap at `utils/cloud_sync.py:14729`:
`merged_asymmetry` covers accepted local/cloud-only entries, not unresolved
matched measurement or render-relevant image differences omitted from an
automatic plan. `finalize_sync_candidates` (11461) executes automatic items
before remaining manual conflicts, through `_build_plan_from_automatic_decisions`
(11399). The mosaic helper (23031; selection at 23100 onward) uses local
cloud-linked measurement geometry across the observation. Thus the new call
can render unresolved local values before the user has chosen them.

Hermetic reviewer reproduction used the existing `_patch_common` and
`_baseline_from_state` fixtures: automatic upload of measurement 42 alongside
matched measurement 99 with local length 9 and cloud length 12, left out of
the automatic items. Actual result: only 42 pushed, all accepted-asymmetry
lists empty, and the mosaic helper called once. No production access or source
edits were used. This confirms unsafe eligibility; the helper was stubbed,
so the reproduction itself did not render or upload an atlas.

Reviewer verification: conflict-plan suite 98 passed in 0.79s; `git diff
--check` passed. The existing success tests miss this case. The original
implementation handoff still describes the unchanged implementation candidate
at HEAD `5faf758680800b4f2ff781657eb43a2aed18c2e5`; this review only changes
workflow documents. No acceptance commit or archival. The pending mosaic-1
prompt now includes a bounded follow-up to guard unresolved rendering state
and test automatic/manual coexistence. Retained-asymmetry atlas support and
import-only generation remain deferred. A focused stage review is warranted;
no auth/RLS/storage-access policy change is requested.

### Implementation report before independent review

2026-09-07: **Stage mosaic-1 is implemented and human-gated, not yet accepted.**
Prompt: `/Users/sigmundas/Documents/Code/sporely/.sparring/prompts/sporely-py/stage-mosaic-1.md`
(still pending — not archived). Starting HEAD:
`5faf758680800b4f2ff781657eb43a2aed18c2e5` (unchanged; nothing committed).

`resolve_conflict_plan` (`utils/cloud_sync.py`) now calls
`_push_spore_mosaic_for_observation` once per plan, after every
measurement push/import op and before snapshot/signature/synced
finalization, whenever this call or a verified prior attempt (carried
forward on retry via ops with status `completed`/`already_complete`)
completed at least one `push_measurement`. New Step 9b, inserted right
before Step 10 (finalize) at the point where `merged_asymmetry` is already
computed. A conservative guard skips the mosaic step whenever
`merged_asymmetry` has any `local_only_images`/`cloud_only_images`/
`local_only_measurements`/`cloud_only_measurements` entry — new this round,
carried from a retried plan, or inherited from a previous snapshot — since
the mosaic helper selects an observation-wide set and a partial upload
cannot be proven to match it. This is deliberately coarser than
microscope-only asymmetry; documented as a known limitation, not narrowed
this stage. Auth/temporary mosaic errors raise through the existing
`_partial_error` path (blocking finalization, revisited on retry); ordinary
errors are logged best-effort and do not block finalization, matching the
legacy `resolve_conflict_keep_local` (11825) / `push_all` (19303) policy.
The mosaic step's own `executed` entries use non-'completed' statuses
(`best_effort_completed` / `best_effort_failed` / `skipped_asymmetry`) so
they are invisible to `_verify_completed_ops_and_rebase`'s completed-op
schema — no new persisted retry format was introduced, per the stage's
explicit boundary.

Files touched: `utils/cloud_sync.py` (new Step 9b, ~40 lines, no other
lines changed), `tests/test_cloud_conflict_plan_execution.py` (added a
`push_spore_mosaic` stub + tracker to `_patch_common`, 8 new tests, and one
assertion added to the existing `test_keep_local_only_measurement_produces_no_writes`),
`docs/supabase-sync-contract.md` (new "Public spore mosaic after conflict
resolution" section, both this repo and `sporely-web`).

Verification: `./.venv/bin/pytest -q tests/test_cloud_conflict_plan_execution.py
tests/test_cloud_spore_mosaic_unchanged_sync.py tests/test_cloud_spore_mosaic_signature.py
tests/test_cloud_download_only.py tests/test_cloud_sync_fast_path.py
tests/test_cloud_sync_dirty_loop_steady_state.py tests/test_image_tombstones.py
tests/test_cloud_image_bytes_desired.py` — 267 passed, 0 failed
(98 in the conflict-plan file, up from 90 baseline). `py_compile` clean on
both changed files. `git diff --check` clean in both repos.

Human-gated: this changes live sync conflict resolution. Implementation is
**uncommitted**; the stage prompt remains **pending**. The three numbered
manual tests from the prompt (disposable-observation upload generates the
mosaic; re-sync is a no-op; retained asymmetry stays skipped and does not
replace an existing cloud mosaic) are not yet run — they need a human with
a real Sporely Cloud account and a second client. Acceptance requires a
fresh, independent `sporely-sparring` review in a new session; this session
does not accept its own work. The prior Muscaria browser symptom remains a
separate, unconfirmed report. Existing unrelated `AGENTS.md`/Parmasto-plan
edits are untouched by this stage.

Code verification confirmed the missing call in `resolve_conflict_plan`
(measurement push at 14511, finalization at 14705–14717), versus legacy
keep-local (11825) and normal push (19303). Retry filtering at 14051 removes
completed measurement operations, so the implementation must account for
verified prior effects. The mosaic query at 23044–23078 selects across the
observation; current/prior accepted measurement and render-relevant image
asymmetry therefore needs a conservative guard.

Selected scope: restore mosaics after safe reviewed local measurement uploads,
including partial retries. Defer import-only generation and full mixed-asymmetry
atlas support. Add focused regression tests and matching sync-contract updates
in sporely-py and sporely-web. The prompt lists exact verification commands.
Implementation is human-gated and remains uncommitted/pending until manual
checks and fresh independent review. The prior Muscaria browser symptom remains
separate. Existing unrelated AGENTS.md/Parmasto-plan edits are not stage work.

Prompt registration verification passed: the read-only selector returned the
exact prompt above from both the parent with `--repo sporely-py` and the child
root (both exit 0). Only the prompt and this plan were written; no implementation
tests were run during prompt preparation.

### Prior recovery record

2026-09-07: production symptom investigated and the missing mosaic repaired
through the existing targeted backfill helper. No production source changes,
commits, or deployments. Permanent conflict-resolution fix remains pending.

### Evidence and repair

- User reported missing public mosaics for local observation 879 / cloud 1241
  (Collybiopsis) and local 881 / cloud 1243 (Amanita muscaria).
- Supplied log shows an image conflict blocking 879 in normal sync, followed
  by a later push of 59 measurements without a mosaic stage.
- Live database initially contained no mosaic for 1241. Anonymous
  `get_public_observation(1241)` returned 59 measurements and a null mosaic.
- Muscaria already had mosaic 348, 33 tile rows, and all 33 public point crop
  coordinates. Its worker and legacy image URLs both returned HTTP 200 WebP;
  the site's live CSP permits both origins. Its reported browser symptom was
  not reproduced; no connected browser was available for visual verification.
- Before repairing 1241, verified the linked and active account, the
  owner-filtered remote observation lookup, public spore visibility, all 59
  local source files, and exact equality of local-linked and remote
  measurement ID sets. The observation remained dirty; no observation or
  measurement edits were pushed.
- Called existing `backfill_public_spore_mosaics` with whitelist `['1241']`,
  limit 1, `push_measurements=False`, and `ensure_image_metadata=False`.
  Result: one generated, zero failures. This rebuilt only the mosaic and its
  tile manifest, using the existing helper's local signature bookkeeping.
- Readback as anon: 59 measurements, 59 points with tile geometry, mosaic
  URL `https://upload.sporely.no/mm/350?v=1`; image returned HTTP 200 WebP.

### Owning path and proposed next stage

`utils/cloud_sync.py::resolve_conflict_plan` calls
`_push_measurements_for_observation` for selected measurement pushes, then
eventually snapshots, refreshes the media signature, and stamps synced. It
does not call `_push_spore_mosaic_for_observation`. Normal `push_all` and
legacy `resolve_conflict_keep_local` do call that helper.

Implement and review one bounded conflict-resolution stage:

1. Add regression coverage in `tests/test_cloud_conflict_plan_execution.py`
   for a reviewed measurement push followed by mosaic work before finalization.
2. Reuse the canonical mosaic helper; do not create another selection or
   rendering implementation. Determine safe behavior for cloud imports and
   mixed accepted-asymmetry plans before enabling them: the helper selects an
   observation-wide set, so an operation-level boolean alone is not proof
   that every selected tile respects the reviewed plan.
3. Preserve field-only/no-op and accepted local-only no-cloud-write behavior,
   account binding, download-only guarantees, and existing mosaic error policy.
   Auth/temporary failures must not silently seal a failed resolution.
4. Update the sync contract in both canonical repository copies, reading
   the other repository's AGENTS.md before accessing it.
5. Run focused conflict execution and mosaic unchanged-sync tests, then the
   required download-only, fast-path, dirty-loop, and affected policy tests.
   Use the project virtual environment. No heavy builds or dependencies.

Verification tier for current recovery: production API and image readback
passed; browser display remains human-gated. The user should reload public
observation 1241 and verify its mosaic and spore thumbnail strip, then check
1243. A permanent sync behavior patch requires its own tests and live conflict
resolution verification before committing under the repository rules.
