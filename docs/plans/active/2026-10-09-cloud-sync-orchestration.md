# Cloud sync orchestration: single completion owner

Written 2026-10-09 against `main` @ `03a9adf`. Nothing in this plan is
approved or started by writing it. It implements the target model of
`docs/cloud-sync-orchestration-design.md` (sections 3–7) under the settled
decision record in its section 9 (D1–D10, 9.1, 9.2). It does not reuse the
stage structure of `docs/plans/active/2026-10-07-cloud-sync-extraction-and-orchestration.md`;
that plan is the completed extraction and is cited only for its invariant
list (its Stage S1, "Invariants the target model must keep", items 1–20,
referred to below as "inv. N").

## What this plan is for

Today several writers decide observation completion (design section 1): push
stamps `synced` before child work and compensates afterwards, pull stamps
before the snapshot, conflict execution stamps before required work, and
`materialize_cloud_media_for_observation` and the manual cloud import write
completion state outside sync. The plan delivers, in order:

1. Characterization of current behavior, as reviewable goldens, before any
   change.
2. A pure move of orchestration execution (`push_all`, `pull_all`,
   `sync_all`, `_create_local_from_remote`, the completion and
   conflict-review writers, conflict execution, `finalize_sync_candidates`,
   the summary push and backfill) out of the `utils/cloud_sync.py` facade
   into owners, keeping every public import and signature.
3. One shared, pure reconciliation classifier for push and pull, proven
   decision-preserving.
4. Typed sync issues with explicit required, best-effort, review, blocked and
   non-observation severity and named retry responsibility, keeping legacy
   result shapes.
5. A single completion owner for `sync_status`, `synced_at`, error columns,
   the conflict-review marker and snapshot acceptance, switched in per path
   (push, pull, conflict execution, other writers), each behind a stage with
   an explicit intended-diff list.

## Settled decisions this plan implements (design section 9)

| Id | Choice | Where implemented |
| --- | --- | --- |
| D1 | (a) for existing observations only; a new row created with `materialize_remote_images=False` stays `dirty` (9.1) | Stage 11 (pinned in Stage 2) |
| D2 | (a) pull-only uses pull completion rules; `synced` written locally, zero cloud writes | Stage 11 |
| D3 | (a) on-demand materialization writes the baseline only through the owner and only when complete | Stage 13 |
| D4 | (a) spore mosaic is best-effort and visible in `errors` | Stage 10, Stage 12 |
| D5 | (b) backfills retry independently; typed issues; never dirty the observation; 9.2 gaps 1, 2 and 4 | Stage 9 |
| D6 | (b) rules change to match code: Observations-tab Refresh equals Sync now (`sync_images=True`) | Stage 1 (rules text only) |
| D7 | (a) manual import of a cloud observation becomes a `create` outcome under the owner | Stage 13 |
| D8 | (a) already landed on `main` (`23fae1b`) | pinned in Stage 2 |
| D9 | (a) tombstone failures do not block observation completion; the unsynced tombstone is the retry record | Stage 10 (pinned); contract wording in the sporely-web track |
| D10 | (a) once-per-session device capability report is a documented exception to inv. 18 | Stage 1 (rules text), every no-op measurement |

## Invariants held by every stage

Inv. 1–20 of the extraction plan hold through every stage. Each stage names
the ones its candidate could plausibly violate. In addition:

- **Public surface.** Every name importable from `utils.cloud_sync` today stays
  importable, with the same signature, and is the same object as its owner's
  (`tests/test_cloud_sync_facade_identity.py`). `sync_all` keeps every result
  key listed in design 3.6.
- **Taxonomy identity.** `tests/fixtures/cloud_sync_taxonomy_identity_golden.json`
  never changes in this plan. Snapshot serialization, including the
  identity-less variant (design 3.1 rule 6) and the variants without images or
  measurements, stays byte-identical to the Stage 1 golden.
- **Import direction and purity.** Owners never import the facade
  (`FACADE_IMPORT_ALLOWLIST` stays empty unless a stage records an entry with a
  reason and a retirement condition); the classifier is pure
  (`tools/cloud_sync_reconciliation_purity.py`).
- **No new remote writes.** Completion is local-only. No stage adds a remote
  write; Stage 13 removes some (D7). Pull-only stays zero-cloud-write.
- **Separation.** A stage is either a pure move (relocation checker reports
  every entry `identical`), a decision-preserving refactor (goldens unchanged),
  or a behavior change (goldens change only in the rows its intended-diff table
  lists). No stage mixes these.
- **Golden discipline.** The Stage 1–2 goldens are keyed by row id (design
  2.2 row ids P1–P10, L1–L8; C-* for conflict execution; W-* for other
  writers; R-* for result shapes; N-* for no-op writes). A behavior stage
  regenerates goldens and its diff must touch exactly the row ids in its
  intended-diff table, each annotated with the diff id. Any other golden change
  is a regression until the person decides otherwise.

## Stages, route and gates

All stages run in `sporely-py`, in order, as one run slice.

| Stage | Kind | Builds on |
| --- | --- | --- |
| 1 Classifier and snapshot characterization | tests + rules text | `main` @ `03a9adf` |
| 2 Completion, result and no-op characterization | tests only | 1 |
| 3 Canary observability tooling | additive diagnostic, default off | 2 |
| 4 Pure move: completion writers and leaf helpers | pure move | 2, 3 |
| 5 Pure move: push, pull and sync execution | pure move | 4 |
| 6 Pure move: conflict execution and finalization | pure move | 5 |
| 7 Shared reconciliation classifier | decision-preserving refactor | 1, 6 |
| 8 Typed sync issues | result-preserving refactor + additive key | 2, 7 |
| 9 Backfill issues and retry responsibility (D5) | behavior change (visibility) | 8 |
| 10 Completion owner: push | behavior change | 7, 8, 9 |
| 11 Completion owner: pull, create and pull-only | behavior change | 10; blocked by Gate G1 |
| 12 Completion owner: conflict execution | behavior change | 11 |
| 13 Other writers: on-demand materialization and manual import | behavior change (incl. UI path) | 12; blocked by Gate G2 |
| 14 Single-writer guard and desktop documentation | tests + docs | 13 |

Gates (manual, person only; see their sections): **Gate G1** after Stage 10
(push owner), blocking Stage 11 (pull owner). **Gate G2** after Stage 12
(conflict owner), blocking Stage 13 (D3/D7 writers). **Completion gate G3**
after Stage 14, blocking plan completion.

Stage 5 was split into Stages 5 and 6 for reviewability: push/pull/sync
execution and conflict execution have disjoint call graphs (no
`resolve_conflict_*` or `finalize_sync_candidates` call in `push_all`,
`pull_all` or `sync_all`), so each relocation diff can be checked alone.

**Execution route: compile intake only.** Stages use the direct
`## Stage <n> — <title>` convention so `sparring check-plan` parses them.
`check-plan` and a plain Markdown `run-plan` read only the stage sections:
they ignore the three gate sections, so a plain Markdown run would execute
Stages 1–14 with no live-canary hold at all. **That route is not acceptable
for this plan.** Only compile intake (`start-plan`, `--mode compile`) turns
the gate sections into engine gates, in a version 2 manifest:

| Gate | Manifest field | Blocks | Kind | Suggested id |
| --- | --- | --- | --- | --- |
| G1 (after Stage 10, push owner) | `gates_before` of Stage 11 | Stage 11 is not created | `manual` | `g1-push-completion-canary` |
| G2 (after Stage 12, conflict owner) | `gates_before` of Stage 13 | Stage 13 is not created | `manual` | `g2-pull-conflict-completion-canary` |
| G3 (after Stage 14) | `completion_gates` | the plan does not report COMPLETE | `manual` | `g3-end-to-end-completion-canary` |

**Run precondition (the person, before approving the intake):** the compiled
manifest is `"version": 2` and contains exactly these three gates at these
positions. If any is missing or misplaced, do not approve or start the run;
re-run intake. Stage 1 repeats this check read-only (its "Run-shape check").

**Engine semantics relied on** (agent-sparring `docs/plans.md`, "Version 2:
plan-declared gates"): after the preceding stage is accepted the run mints
one obligation per gate and stops with `deferred_verification_required`.
Reaching a gate never satisfies it. Only
`sparring resume-plan … --deferred-result '<instance>:<gate id>=pass=<note>'`
releases it; `fail` and `blocked` keep the run stopped. The engine accepts a
`pass` without a note; this plan does not: a `pass` must carry the evidence
note listed in "Gate evidence" below, and nobody records `pass` over failed
evidence or with an empty note. A `fail` is written to the originating
stage's `notes.md`; the engine does not rewind accepted stages.

**Gate evidence (required in every recorded answer, aggregate only, no
personal data):**
1. Build: the accepted candidate SHA run from source.
2. Profile: the `SPORELY_PROFILE` value.
3. Account class: `disposable` or `known` (no email, user id or name).
4. Database path and backup path (profile-relative is enough).
5. Reconciliation report category counts A–H before and after, and their diff.
6. State report counts (`synced`, `dirty`, `blocked`, NULL, review markers,
   rows with error columns, pending tombstones) before and after.
7. Write counter: instrumented mutating-request totals per sync, per
   (method, table/RPC name/bucket); the uninstrumented and `UNCOVERED` paths
   from Stage 3's coverage table that could have run in that sync; and the
   Supabase API-log cross-check result (`matched`, `mismatch: …`, or
   `not available`). Zero writes may be claimed only when no such path could
   have run or the cross-check matched.
8. Sync errors: count and issue kinds per sync (no observation content).
9. Per check step: `pass`, `fail` or `not exercised` with its pass criterion.

**Repair route for `fail` or `blocked`:** the next stage (or completion)
stays closed. The fix is new reviewed candidate work (a new stage or a
revert), and the gate is answered again for the repaired build with fresh
evidence.

**Live canaries are human actions.** The G1–G3 live syncs are canary actions
the person authorizes and performs, on an isolated profile and a disposable
or known account. They are not a general authorization for stage agents to
write to Supabase or any production data, and no agent performs them, runs
any live sync, or reads the live databases.

Engine-gated verification: Stages 10–13 are committed, pushed and accepted on
automated evidence; the gate that follows holds the next stage until the
person records `pass` (AGENTS.md, engine-gated exception). Agents never run a
live sync against Supabase, never perform live Supabase writes, and never run
the canaries.

**Retirement of transitional scaffolding is not in this plan** (U4): it
belongs in a later plan with its own retirement conditions and live-canary
hold.

## Assumptions

- A1. Characterization runs on fake clients and real SQLite (the existing
  test pattern); the local Supabase harness is not used for media paths
  (design 7). Stage 2 may add observation metadata device actions to
  `tests/local_supabase/device.py` only as opt-in tests.
- A2. From code at the base, Sporely cloud HTTP has three funnels:
  `SporelyCloudClient._request_with_refresh` (`utils/cloud_sync.py:5681`,
  and the read-only client's override at `:8268`) →
  `_request_with_transient_retry` (`:1633`), carrying `_post` (`:6117`),
  `_rpc` (`:6129`), `_patch` (`:6448`), `_delete` (`:6490`) and
  `_storage_remove` (`:6500`); `CloudflareR2Client._request`
  (`utils/r2_storage.py:410`, request at `:481`); and
  `CloudflareMediaWorkerClient._request` (`utils/r2_storage.py:838`, request
  at `:868`; the upload probe `probe_object_status` at `:688–694` is a GET with
`Range: bytes=0-0`, a read). Callables are also passed **by reference** into
`_request_with_transient_retry`: `self._s.request` (`:5683`, `:8276`) and
`requests.request` (`login` `:5851`, `refresh_login` `:5873`, auth token
POSTs). Auth token traffic
  (`SporelyCloudClient.login` `:5849`, `refresh_login` `:5868`,
  `utils/sporely_cloud_auth.py:192`, `:221`) is not a data write. No direct
  `session.<verb>` bypass was found under `utils/`. Stage 3 proves this with a
  coverage test rather than assuming it; canaries never assume zero
  uninstrumented writes (see "Gate evidence" item 7).
- A3. The snapshot store `_store_remote_snapshot` stays read-only remotely;
  completion adds no remote write.
- A4. `keep_cloud` image-application warnings stay best-effort (visible, not
  blocking), as today (design 1.6); only snapshot failure becomes blocking in
  conflict execution.
- A5. A summary skip because the remote table is missing (9.2 gap 2) becomes a
  typed `best_effort` issue in `result['issues']` only, with no new `errors`
  string, because it is a deployment condition rather than a failure.
- A6. Canaries run from source at the accepted candidate SHA
  (`.venv/bin/python main.py` in a clean checkout of that SHA). No local
  release binary is built; release binaries come only from GitHub Actions.
- A7. 9.2 gap 3 (remote deletion of locally stamped measurements caught only
  on deep verification) is out of scope. The calibration raw `UPDATE`
  (design 3.5) stays as it is and is an enumerated local-edit dirty signal
  under the ownership rule below (see "Completion ownership rule"); rerouting
  it through `mark_observation_sync_dirty` would also clear error, blocked and
  review-marker columns on calibration change, which is a behavior change not
  authorized here.

## Decisions settled by the person (2026-10-09)

Recorded after the first draft of this plan. Binding on every stage. There
are no open decisions left in this plan.

- **U1 — pull-side conflict review: preserve current outcomes.** Pull field
  conflicts keep today's result: row `dirty`, **no** review marker, no
  snapshot, the legacy error string. `removed_keys` keeps today's result: the
  legacy error string, **no** state write, no snapshot (a clean row stays
  `synced`). The goldens must show these marker and state outcomes unchanged
  in every stage. Any change to the conflict dialog or to which observations
  it lists is out of scope.
  The two pull-side identity-conflict outcomes are also preserved exactly,
  as explicit owner dispositions, and are **not** interpreted through design
  3.1 rule 6 (code at `03a9adf`):
  - `IDENTITY_APPLY_CONFLICT` in the no-baseline, remote-changed branch
    (`utils/cloud_sync.py:10555–10564`, state at `:10588–10589`, snapshot at
    `:10828–10837`): legacy review-needed error string; row `dirty` via
    `_set_observation_sync_state(dirty=True, synced_at=None)`, which sets
    `synced_at` NULL and clears all error columns
    (`clear_sync_error_state=True`, `:3164–3170`), so **no** review marker;
    an identity-free snapshot (`_remote_row_without_identity`) is stored,
    observation-only when media or measurements are pending, i.e. **even when
    media materialization failed**.
  - `pull_contradicts_bound_identity` (`:10526–10542`): legacy review-needed
    error string (`pull_blocked`); `_set_observation_conflict_review_pending`
    sets `dirty` + review marker; `should_store_snapshot = False`, so no
    snapshot.
- **U2 — WebP: required only when it blocks required image sync.** From code
  at the base:
  - `required` (`webp_required`): `_prepare_cloud_image_upload_file` raises
    `WEBP_REQUIRED_FOR_CLOUD_MEDIA_UPLOAD_MESSAGE` when
    `features.check('webp')` is false (`utils/cloud_sync.py:3978–3979`);
    `SporelyCloudClient.upload_image_file` re-raises it for the full-image
    upload (`utils/cloud_sync.py:7277–7278`); the image-preparation callback
    re-raises it (`utils/cloud_sync_impl/image_push.py:722–723`); `push_all`
    receives it in its `CloudSyncError` handler and today makes no state
    write (`utils/cloud_sync.py:9688`). These block the selected image's
    byte upload, i.e. required work under contract rule 8.
  - `best_effort` (`original_upload_failed`, covering a WebP failure):
    `SporelyCloudClient.upload_original_image_file` raises the same message
    for an original upload (`utils/cloud_sync.py:7448–7449`, `:7520–7521`);
    the image push catches every `CloudSyncError` there, removes the partial
    original and records an original-upload warning in the original-upload
    summary, not in `errors` (`utils/cloud_sync_impl/image_push.py:1318–1337`,
    `_record_original_upload_warning` at `:754`). This stays best-effort and
    never changes observation status.
- **U3 — classifier outcomes preserved.** Stage 7 changes no push or pull
  outcome. Every push/pull difference Stage 1 finds is recorded, not
  resolved. A later change to any single outcome needs separate
  authorization and its own Δ entry.
- **U4 — scaffolding retirement is a later plan.** Non-goal here (see "Out
  of scope").
- **U5 — positional image pairing preserved** in the manual import (Stage 13,
  D7).

## Deferred technical debt (out of scope, needs a separate decision)

- **Identity-free snapshot stored on media failure in
  `IDENTITY_APPLY_CONFLICT`.** The no-baseline pull branch stores an
  identity-free (observation-only) snapshot even when media materialization
  or measurement import failed (`utils/cloud_sync.py:10588–10594`,
  `:10828–10837`), and leaves no review marker. This conflicts with the
  target "old baseline kept on required failure" rule but is preserved
  unchanged here (U1). Changing it needs its own decision and Δ.

## Completion ownership rule

Evidence that a strict "one writer of `sync_status`" claim would be false:

- `database/models.py:5101–5109` (`recalculate_measurements_for_calibration`)
  runs `UPDATE observations SET sync_status = 'dirty' WHERE id IN (…) AND
  cloud_id IS NOT NULL`. It writes `sync_status` only: not `synced_at`, the
  error or blocked columns, the review marker or the snapshot. A `blocked`
  linked row becomes `dirty` with its blocked reason kept.
- `database/models.py:385–412` (`mark_observation_sync_dirty`) sets `dirty`
  **and clears** `sync_error_code`, `sync_error_message`,
  `sync_blocked_reason` (which holds the review marker) and `sync_blocked_at`
  for linked or `blocked` rows. Every local-edit signal in design 1.2 reaches
  it.
- `database/models.py:431–438` (`reset_cloud_sync_state`) and
  `utils/cloud_sync.py:2749` (`unlink_local_observation_from_cloud`) reset
  completion state for account reset and explicit unlink.

The rule this plan implements and Stage 14 enforces is therefore:

1. **Owned exclusively by `utils/cloud_sync_impl/observation_completion.py`:**
   every transition **to** a completion state during sync and in the D3/D7
   paths: writing `synced` and `synced_at`, accepting or replacing the
   snapshot baseline, writing `blocked`, setting or clearing the review
   marker, and clearing error columns as part of completion; outcome
   dirtying (a `required` issue making a row `dirty`); the explicit pull
   identity dispositions (U1); and the **initial `cloud_id` binding**: after
   a successful remote observation creation, the owner persists `cloud_id`
   with `sync_status='dirty'` before any required child work, and never
   writes `synced` or accepts the completion snapshot until that work
   succeeds. On push this replaces the binding inside today's early `synced`
   write (`utils/cloud_sync.py:9174–9183`); on pull/create it is today's
   `_create_local_from_remote` bind (`:10962–10973`, `cloud_id` + `dirty` +
   `synced_at` NULL first), kept as is. Desktop-id recovery is unchanged: a
   POST carries `desktop_id` = local id (`utils/cloud_sync_impl/push_payloads.py:157–159`),
   and `_resolve_existing_observation_for_push`
   (`utils/cloud_sync_impl/image_identity.py:212`) remains the only recovery
   path when the local bind is lost.
2. **Enumerated local-edit dirty signals (not owned, allow-listed):**
   `mark_observation_sync_dirty` and its callers (`_touch_observation(mark_dirty=True)`,
   `update_observation`, `reconcile_legacy_publish_exclusion_tombstones`),
   `utils/cloud_sync_impl/sync_state.py::mark_observation_dirty` /
   `mark_observation_media_dirty` as called from local-edit paths
   (`set_image_cloud_selected`, `ui/main_window.py`), the calibration raw
   `UPDATE` above, and the sync-time dirty scans (design 1.3). They may only
   move a row toward `dirty` (and, for `mark_observation_sync_dirty`, clear
   error/blocked columns as today). Their behavior is unchanged.
3. **Enumerated account/link exceptions:** `reset_cloud_sync_state`,
   `unlink_local_observation_from_cloud` (including its snapshot clear).

4. **INSERT initialization of a brand-new local row is allowed and is not a
   completion transition.** The guard is scoped to UPDATE writes of existing
   rows; INSERT sites are checked separately against an enumerated insert
   allow-list:
   - `utils/archive/portable_import.py:2762–2770` (portable import: clears
     `cloud_id`, `synced_at` and the error/blocked columns, sets
     `sync_status='local'`, inserted through `_insert_row` at `:551–557`);
   - `utils/db_share.py:768–779` (shared-database import: copies every
     `observations` column, completion columns included, verbatim);
   - `database/models.py:1420` (`create_observation`; `sync_status` from the
     schema default `'local'`, `database/schema.py:1958`).
   `_create_local_from_remote` inserts through `create_observation` and then
   binds through the owner (item 1). A new `INSERT INTO observations` site
   not on the list fails the guard.

Stage 14's test checks this exact list: any other UPDATE writer of these
columns or of the snapshot fails it, any new INSERT site fails it, and any
allow-list entry that no longer exists fails it too.

## Security and privacy

- No stage touches auth/session, RLS, SECURITY DEFINER RPCs, storage
  policies or secrets. Stage 4 relocates the account-binding helpers
  (`ensure_database_linked_to_cloud_user`, `_load_linked_cloud_user_id`,
  `_save_linked_cloud_user_id`, `_normalize_cloud_user_id`) as a pure move;
  its review includes `sporely-security-reviewer` because account binding is
  in scope of that reviewer.
- Stage 3's write counter logs only HTTP method and table, RPC or bucket name.
  It never logs URLs with query values, payloads, headers, tokens or
  coordinates.
- Canary evidence shared with agents is aggregate only (counts, category
  totals, status counts, pass/fail). Live databases, reconciliation report
  JSON and observation content stay with the person; agents do not read them
  (organization rule on personal data).
- Pull-only remains strictly zero-cloud-write; every new client method (none
  expected) would join `_PULL_ONLY_BLOCKED_CLIENT_METHODS`.

## Not authorized by this plan

No Supabase migration, RLS or policy change, production data write, cloud
cleanup or garbage collection, `sporely-web` code or contract edit, web
deployment, desktop release, or local release build. Each would need a
separate, explicit approval. The sporely-web contract track below is a
description of wording for later approval, not work this run may perform.

## Out of scope

Any change to the conflict dialog or to which observations it shows (U1).
Retirement of transitional scaffolding — `tests/cloud_sync_owner_patching.py`
and its autouse conftest fixture, the `S6_*`/`S7_KEPT_IN_FACADE` stay-lists in
`tests/test_cloud_sync_facade_identity.py`, and the relocation-check tooling —
which belongs in a later plan (U4). Rerouting the calibration raw `UPDATE`
(A7). Reference-library sync (including making the D10 device report
change-driven), R2 garbage collection, media selection and anchor logic
(inv. 1–2, 7–9, 16–17), child-change cursor semantics, caller-preset changes
(D6 is a rules-text change only), a persisted media-pending marker for new
rows (9.1 follow-up), account link/reset, `SporelyCloudClient` class
relocation, UI redesign.

## Stage 1 — Classifier and snapshot characterization

**Builds on:** `main` @ `03a9adf`. Plan:
`docs/plans/active/2026-10-09-cloud-sync-orchestration.md` (read its
"Invariants held by every stage", "Decisions settled by the person" and
"Execution route" sections).

**Kind:** tests and rules text only. No production code changes.

**Outcome:** a stored decision corpus and snapshot byte golden on the
pre-change code (design 7 items 1–2), and the D6/D10 rules text.

**Run-shape check (first, read-only):** confirm from the compiled manifest
under `.sparring/intake/` or the run state under `.sparring/plans/` that this
run carries the three plan gates (G1, G2, G3) at the positions in the plan's
"Execution route" table. If the run carries no gates or they are misplaced,
make no change and return `NEEDS_YOU` (category `OTHER`) saying the plan must
be run through compile intake.

**Scope:**
1. `tests/fixtures/cloud_sync_orchestration/classifier_corpus.json`: a table
   of (local, remote, baseline) triples with, per row, the push decision
   (`_analyze_observation_push_conflicts` in
   `utils/cloud_sync_impl/preflight.py`, push Case F, `identity_review_pending`)
   and the pull decision (`_remote_snapshot_has_meaningful_changes`,
   `_analyze_observation_field_changes`, `_analyze_image_changes`
   (`utils/cloud_sync_impl/reconciliation/images.py`),
   `_remaining_local_changes_after_remote_merge`, pull Case F), plus
   `_local_has_real_changes_since_snapshot`. Coverage: representation-only
   drift, three-way divergence, one-sided changes, taxonomy identity (no
   baseline, baseline without identity, claim/no-claim, contradiction),
   accepted asymmetry, removed remote images, measurement-only changes.
2. `tests/fixtures/cloud_sync_orchestration/snapshot_golden.json`: serialized
   `_cloud_observation_snapshot` output for the corpus, including without
   identity and without images/measurements.
3. A generator script under `tests/` (not shipped) and tests
   `tests/test_cloud_sync_orchestration_classifier_golden.py` asserting the
   current code reproduces both goldens.
4. A list, in the test module docstring and stage notes, of every row where
   push and pull decide differently. Recorded only, not resolved (U3).
5. `.claude/rules/cloud-sync.md`: replace the Refresh bullet with the D6
   preset (`sync_images=True`, same as Sync now) and add the D10 exception
   (one `record_reference_client_capabilities` RPC per app process and
   account on a non-pull-only sync is permitted on a no-op sync).

**Non-goals:** no production change; no classifier change; no test removal.

**Acceptance:**
- Run-shape check recorded in stage notes.
- Both goldens are produced by the unchanged code and the new tests pass.
- Every corpus class above has at least one row; the push/pull difference
  list is complete for the corpus.
- `tests/fixtures/cloud_sync_taxonomy_identity_golden.json` unchanged.
- Rules text matches design section 9 D6 and D10 and nothing else changes in
  the rules file.

**Verification:**
`.venv/bin/pytest -q tests/test_cloud_sync_orchestration_classifier_golden.py tests/test_cloud_sync_taxonomy_identity_golden.py tests/test_cloud_sync_reconciliation_purity.py`

## Stage 2 — Completion, result and no-op characterization

**Builds on:** Stage 1. Plan:
`docs/plans/active/2026-10-09-cloud-sync-orchestration.md`.

**Kind:** tests only. No production code changes.

**Outcome:** goldens that pin today's completion outcomes, legacy results and
no-op write counts (design 7 items 3–5, design 2.2, 1.4–1.7, section 5), so
later stages can show every intended diff row by row.

**Scope (real SQLite, fake client; fixtures under
`tests/fixtures/cloud_sync_orchestration/`):**
1. `completion_states.json` + `tests/test_cloud_sync_orchestration_completion_golden.py`:
   per row id the final `sync_status`, `synced_at` set/unchanged, error
   columns, review marker, snapshot presence and snapshot hash (old/new/none).
   Rows:
   - P1 (generic `CloudSyncError` and the WebP-required branch), P2, P3, P4,
     P5 (success and failure), P6, P7, P8 (both backfills), P9, P10 (both the
     `CloudSyncError` and the non-`CloudSyncError` snapshot failure), push
     preflight `_clear_observation_dirty_if_no_real_changes`, push Case F with
     and without a narrowing PATCH failure.
   - L1, L2a, L2b, L2c, L3a, L3b (9.1: new row with `materialize=False` stays
     `dirty`), L4a (both `materialize` values), L4b, L5 (existing row and
     `_create_local_from_remote`), L6 (including a `blocked` row being
     re-stamped `synced`), L7, L8, pull Case F, pull field conflict
     (`dirty`, no marker, no snapshot; U1), `removed_keys` (no state write, no
     snapshot; U1), remote-unchanged/local-clean re-stamp (including a
     `blocked` row).
   - P1-webp-prep (`features.check('webp')` false), P1-webp-full-upload, and
     P2-original-webp (original upload WebP failure: warning in the
     original-upload summary, status unchanged) (U2).
   - L-identity-apply-conflict (U1): no-baseline remote-changed pull with
     `IDENTITY_APPLY_CONFLICT`, in three variants (media complete; media
     materialization failed; measurement conflict): row `dirty`, `synced_at`
     NULL, error columns cleared, **no** marker, identity-free snapshot
     stored (observation-only when media/measurements pending), legacy
     string.
   - L-bound-identity-contradiction (U1): `pull_contradicts_bound_identity`:
     `dirty` + review marker, no snapshot written (old baseline unchanged),
     legacy `pull_blocked` string.
   - P-create-* (`cloud_id` binding): (a) child work interrupted after the
     remote POST (exception after `push_observation` returns); (b) restart
     and retry after (a): the second push reuses the bound `cloud_id`, the
     fake records **no second POST**; (c) remote POST succeeds but the local
     `cloud_id` write fails: the retry recovers through
     `_resolve_existing_observation_for_push` desktop-id dedupe, again with
     no duplicate POST. Record today's state for each (today (a) leaves
     `synced` + `cloud_id` from the early stamp).
   - E-* local-edit allow-list rows (ownership rule item 2): calibration
     rescale on a `synced`, a `blocked` and a review-marked linked row
     (`database/models.py:5101–5109`); `mark_observation_sync_dirty` on the
     same three rows.
   - Combined outcomes (design 7 item 4): no-baseline identity review with
     each of image upload, measurement push and summary failure; with all
     work succeeding; with a snapshot failure.
   - C-keep_local, C-keep_cloud, C-merge, C-plan, C-finalize: each success
     path and each failure branch in design 1.6 (measurement push failure
     after stamp; snapshot failure after stamp; privacy limit; image failure).
   - W-materialize (complete, failed download, measurement conflict) and
     W-import (`_mark_cloud_observation_imported`: status, naive local
     `synced_at`, no snapshot, unconditional `set_image_desktop_id` /
     `set_desktop_id` calls).
2. `legacy_results.json` + `tests/test_cloud_sync_orchestration_result_golden.py`:
   for each caller preset in design 2.1 and each failure branch above, the
   `sync_all` result keys and `errors` strings in order, plus
   `summarize_sync_issues(errors)` and
   `partition_download_from_cloud_issues(errors)` (R-* rows).
3. `noop_writes.json` + `tests/test_cloud_sync_orchestration_noop_golden.py`:
   a recording fake client lists every mutating call on a fast no-op sync for
   each non-pull-only preset (N-* rows): expected zero, except the D10 device
   capability RPC, asserted explicitly as exactly one on the first sync of a
   process and zero on the second; pull-only: zero and empty
   `blocked_write_attempts`.
4. Retarget the two masking tests named in design section 4
   (`test_spore_summary_sync.py::test_call_site_unexpected_error_recorded_in_errors_list`,
   `…::test_measurement_reconcile_records_per_observation_errors`) only if
   still one-argument fakes after D8; otherwise record that D8 already fixed
   them.

**Non-goals:** no production change; no behavior fixed, even where the
golden records a defect (record it as `current` with a note).

**Acceptance:**
- Every row listed above exists, is produced by the unchanged code, and the
  tests pass. Rows the design marks "unverified" are now verified.
- The D10 RPC is the only mutating call on a no-op sync.
- No existing test changes except item 4.

**Verification:**
`.venv/bin/pytest -q tests/test_cloud_sync_orchestration_completion_golden.py tests/test_cloud_sync_orchestration_result_golden.py tests/test_cloud_sync_orchestration_noop_golden.py tests/test_cloud_sync_fast_path.py tests/test_cloud_sync_dirty_loop_steady_state.py tests/test_cloud_download_only.py tests/test_spore_summary_sync.py tests/test_summary_failure_marks_observation_dirty.py`

## Stage 3 — Canary observability tooling

**Builds on:** Stage 2. Plan:
`docs/plans/active/2026-10-09-cloud-sync-orchestration.md` (read "Security
and privacy", A2 and "Gate evidence").

**Kind:** additive diagnostic only. Default off. No sync behavior change.

**Outcome:** the person can measure, during a live canary, completion state
and remote writes, with proven counter coverage, without an agent touching
live data.

**Scope:**
1. `tools/cloud_sync_state_report.py` (read-only SQLite via `mode=ro` URI,
   `--db PATH`, `--json OUT`, `--observation ID`): counts of `sync_status` per
   value (`synced`, `dirty`, `blocked`, NULL), review-marker count, rows with
   error columns, pending unsynced tombstone count, and per observation id a
   snapshot hash. Never imports Qt or the cloud client.
2. A per-sync mutating-request counter, enabled only by an environment
   variable (`SPORELY_DEBUG_CLOUD_WRITES=1`), recorded in the three funnels of
   A2: `_request_with_transient_retry` (counts every non-GET/HEAD; an RPC is
   counted as `rpc:<name>` and classified read or write from
   `_PULL_ONLY_ALLOWED_READ_METHODS` / `_PULL_ONLY_BLOCKED_CLIENT_METHODS` in
   `utils/cloud_sync_impl/pull_only.py`), `CloudflareR2Client._request` and
   `CloudflareMediaWorkerClient._request`. At the end of `sync_all` it logs one
   line: instrumented totals per (method, table/RPC/bucket) and the names of
   allow-listed uninstrumented paths that executed (each such path increments
   an "uninstrumented path ran" marker even though its request is not counted).
   Off: no behavior or output change.
3. **Coverage enumeration and table.** Enumerate from code every mutating
   cloud path reachable from `sync_all`, conflict execution,
   `materialize_cloud_media_for_observation` and the manual import: PATCH,
   POST, DELETE, PUT, RPC mutations, storage upload and remove (R2 and Worker),
   identity write-backs (`set_desktop_id`, `set_image_desktop_id`,
   identity-clear RPCs), the device capability RPC (D10), the original-upload
   cleanup (`_storage_remove` + `_patch` at
   `utils/cloud_sync_impl/image_push.py:1318–1326`). Record a coverage table
   (path, call site, funnel, counted yes/no, reason) in
   `docs/cloud-sync-architecture.md` (verification section) and stage notes.
4. **Coverage test** `tests/test_cloud_write_counter_coverage.py`:
   - AST scan of the sync-reachable modules (`utils/cloud_sync.py`,
     `utils/cloud_sync_impl/**`, `utils/r2_storage.py`, the reference-sync
     modules, `utils/spore_summary_sync.py`, `utils/cloud_spore_mosaic*.py`,
     `utils/cloud_media_recovery.py`, `utils/curated_reference_sync.py`,
     `utils/reference_client_capabilities.py`, `utils/sporely_cloud_auth.py`,
     and `ui/observations_tab.py` for the import path). Every HTTP call site
     (`requests.<verb>`, `<session>.request`, `<session>.<verb>`, `urlopen`)
     must be one of the three instrumented funnels or appear in
     `UNINSTRUMENTED_MUTATING_PATHS` (with reason) or
     `NON_SYNC_HTTP_ALLOWLIST` (auth token exchange, iNaturalist suggest at
     `ui/observations_tab.py:1809`/`:1822`, with reason). The scan detects
     HTTP functions **passed by reference** as well as called directly: any
     attribute or name resolving to `requests.request`/`.get`/`.post`/…,
     `<Session>.request`/`<verb>`, or `urlopen` used as an argument (today
     `self._s.request` at `utils/cloud_sync.py:5683` and `:8276`,
     `requests.request` at `:5851` and `:5873`). An unlisted site fails the
     test; a listed site that no longer exists fails it too.
   - `READ_ONLY_HTTP_ALLOWLIST`, explicit, for read calls reachable from sync
     or its UI entry points, each with its site: `_download_public_media_file`
     (`requests.Session().get`, `utils/cloud_sync.py:7526–7536`), the Worker
     upload probe (GET with `Range: bytes=0-0`, `utils/r2_storage.py:688–694`),
     and the `ui/observations_tab.py` GETs (`:1278–1289`, `:1419`, `:1433`,
     `:1480`, `:1527`, `:10984`). A read site must use GET/HEAD; anything else
     fails.
   - `RPC_CLASSIFICATION`: a table keyed by **RPC name** (not client method
     name) → `read` or `write`, e.g. `list_reference_library_feed` and
     `list_my_reference_sharing` are `read` although sent as POST. The counter
     counts an RPC as a write unless its name is classified `read`; a name
     that is not a string literal at the call site, or is not in the table,
     counts as a write. The scan resolves names from the AST first argument
     (multi-line calls such as `utils/cloud_sync.py:7787`, `:7796` and
     `utils/cloud_sync_impl/spore_mosaic.py:369` carry literal names on the
     next line); the pull-only passthrough
     `utils/cloud_sync_impl/pull_only.py:149` is a wrapper, classified by the
     wrapped call.
   - For every name in `_PULL_ONLY_BLOCKED_CLIENT_METHODS`, call it on a client
     with a fake transport and assert the counter increments (or that the
     method is in `UNINSTRUMENTED_MUTATING_PATHS`).
5. Tests: the state report on a fixture database; counter off is a no-op
   (Stage 1–2 goldens unchanged); counter on matches the Stage 2 N-* rows.

**Non-goals:** no new `sync_all` result key; no change to what is written; no
change to which requests are made.

**Acceptance:**
- Stage 1–2 goldens unchanged; with the env var unset, logs are unchanged.
- The coverage test passes and fails on a synthetic unrouted call site.
- The coverage table lists every mutating path, and every uninstrumented one
  with its reason and how a canary can cross-check it (Supabase API log,
  Cloudflare dashboard, or `not available`).
- The stage either proves the instrumentation covers every reachable
  mutating path (the uninstrumented list is empty apart from auth token
  traffic) or lists each uncovered path as `UNCOVERED` in the table, so that
  no canary can claim zero writes for a sync in which that path could run.
- Logged content contains no URL query values, payloads, headers or tokens.

**Verification:**
`.venv/bin/pytest -q tests/test_cloud_write_counter_coverage.py tests/test_cloud_sync_orchestration_noop_golden.py tests/test_cloud_sync_orchestration_completion_golden.py <state report tests>`;
`.venv/bin/python -m py_compile tools/cloud_sync_state_report.py`.

## Stage 4 — Pure move: completion writers and leaf helpers

**Builds on:** Stages 2 and 3. Plan:
`docs/plans/active/2026-10-09-cloud-sync-orchestration.md`.

**Kind:** pure move. Every relocated entry must be `identical`.

**Outcome:** the facade-resident dependencies of orchestration live in owners,
so Stages 5 and 6 can move orchestration without an upward facade import.

**Scope (destinations are proposals; the stage records the final map):**
- New `utils/cloud_sync_impl/observation_completion.py`: `_stamp_observation_synced`,
  `_set_observation_sync_state`, `_set_observation_sync_blocked`,
  `_set_observation_sync_error_detail_only`, `_set_observation_privacy_blocked`,
  `_set_observation_plan_image_retryable`,
  `_set_observation_conflict_review_pending`,
  `_clear_observation_conflict_review_pending`,
  `_table_columns_for_conflict_marker`,
  `_clear_observation_dirty_if_no_real_changes`, `_format_review_needed_error`.
- Remote field application and AI merge (`_apply_remote_observation_fields`,
  `_remote_observation_update_kwargs`, `_remote_observation_extra_values`,
  `_merge_cloud_selected_ai_fields`, `_adopt_merge_filled_ai_fields_locally`,
  the `_cloud_identification_*` helpers,
  `build_cloud_ai_state_from_observation_identifications`) into an owner.
- Push helpers used by `push_all` (`_push_narrower_visibility_while_blocked`,
  `_cloud_visibility_to_sharing_scope`, `_is_strictly_narrower_visibility`,
  `_local_media_*` diagnostics, `_mark_cloud_observations_dirty_for_*`,
  `_normalize_observation_sync_field`, `_non_blocking_local_only_fields`,
  `_remaining_local_changes_after_remote_merge`,
  `_remote_snapshot_has_meaningful_changes`, original-upload summary helpers).
- Pull helpers (`_load_local_observation_lookup`,
  `_find_local_observation_for_remote_cached`,
  `_detect_deleted_remote_observations`).
- Child-change cursor and child-safety helpers
  (`_load_child_change_cursor`, `_store_child_change_cursor`,
  `_child_change_cursor_id_key`, `_cloud_child_safety_pull_due`,
  `_push_phase_requires_remote_observation_refresh` and their settings
  constants), unchanged.
- Account binding (`_normalize_cloud_user_id`, `_load_linked_cloud_user_id`,
  `_save_linked_cloud_user_id`, `ensure_database_linked_to_cloud_user`).
  Security review applies.
- Conflict-plan helpers (`_format_recomputed_spore_statistics`,
  `_capture_local_presentation`, `_restore_local_presentation`,
  `_assign_downloaded_image_order`).
- Update `OWNER_LAYERS` in `tests/test_cloud_sync_impl_import_direction.py`,
  the owned-name lists and the `S6_*`/`S7_KEPT_IN_FACADE` stay-lists in
  `tests/test_cloud_sync_facade_identity.py` (names that moved leave the
  stay-lists), and the ownership table in `docs/cloud-sync-architecture.md`.

**Non-goals:** no behavior change, no renames, no signature change, no
`push_all`/`pull_all`/`sync_all`/conflict execution yet, no new
`FACADE_IMPORT_ALLOWLIST` entry unless justified with a retirement condition.

**Invariants at risk:** inv. 4–6, 12, 18, 19; account binding.

**Acceptance:**
- `tools/cloud_sync_relocation_check.py <stage base> <candidate>` reports
  every entry `identical` and exits 0.
- All Stage 1–2 goldens and the taxonomy golden unchanged.
- Facade identity, import direction and purity tests pass.

**Verification:**
`.venv/bin/python tools/cloud_sync_relocation_check.py <base-sha> <candidate-sha>`;
`.venv/bin/pytest -q tests/test_cloud_sync_facade_identity.py tests/test_cloud_sync_impl_import_direction.py tests/test_cloud_sync_reconciliation_purity.py tests/test_cloud_sync_taxonomy_identity_golden.py tests/test_cloud_sync_orchestration_classifier_golden.py tests/test_cloud_sync_orchestration_completion_golden.py tests/test_cloud_sync_orchestration_result_golden.py tests/test_cloud_sync_orchestration_noop_golden.py tests/test_cloud_conflict_plan_execution.py tests/test_cloud_identity_fail_closed.py`

## Stage 5 — Pure move: push, pull and sync execution

**Builds on:** Stage 4. Plan:
`docs/plans/active/2026-10-09-cloud-sync-orchestration.md`.

**Kind:** pure move. Every relocated entry must be `identical`.

**Outcome:** push, pull and sync execution live in owners; the facade only
re-exports them.

**Scope (proposed owners: `push_orchestration.py`, `pull_orchestration.py`,
`sync_orchestration.py`, `spore_summary_push.py`):** `push_all`,
`_push_summary_for_current_observation`, `_reconcile_missing_spore_summaries`,
`pull_all`, `_create_local_from_remote`,
`_import_remote_measurements_for_observation`, `sync_all`, and their
module-level state.
- `SporelyCloudClient` stays in the facade. Annotation-only references use
  `TYPE_CHECKING` imports. The runtime fallback
  `SporelyCloudClient.from_stored_credentials()` in
  `_import_remote_measurements_for_observation` is the one known obstacle:
  either the function stays in the facade (preferred if every sync caller
  passes a client), or one `FACADE_IMPORT_ALLOWLIST` entry is recorded with a
  reason and the retirement condition "client construction leaves the
  facade". `materialize_cloud_media_for_observation` stays in the facade
  (UI entry point; Stage 13 routes its baseline through the owner).
- Update import-direction layers, facade identity lists and the architecture
  ownership table.

**Non-goals:** no behavior change; conflict execution and
`finalize_sync_candidates` (Stage 6), `unlink_local_observation_from_cloud`,
`reset_cloud_sync_state` and client methods stay where they are.

**Invariants at risk:** inv. 1–20 on the sync path; especially 3, 4, 10, 18,
20.

**Acceptance:** relocation all `identical`; all goldens unchanged; identity,
direction and purity tests pass; the sync safety tests below pass.

**Verification:**
`.venv/bin/python tools/cloud_sync_relocation_check.py <base-sha> <candidate-sha>`;
`.venv/bin/pytest -q tests/test_cloud_sync_facade_identity.py tests/test_cloud_sync_impl_import_direction.py tests/test_cloud_sync_taxonomy_identity_golden.py tests/test_cloud_sync_orchestration_completion_golden.py tests/test_cloud_sync_orchestration_result_golden.py tests/test_cloud_sync_orchestration_noop_golden.py tests/test_cloud_sync_fast_path.py tests/test_cloud_sync_dirty_loop_steady_state.py tests/test_cloud_download_only.py tests/test_image_tombstones.py tests/test_cloud_image_bytes_desired.py tests/test_child_change_probe.py tests/test_reference_client_capability_stage_m.py tests/test_spore_summary_sync.py`

## Stage 6 — Pure move: conflict execution and finalization

**Builds on:** Stage 5. Plan:
`docs/plans/active/2026-10-09-cloud-sync-orchestration.md`.

**Kind:** pure move. Every relocated entry must be `identical`.

**Outcome:** conflict execution lives in an owner (proposed
`utils/cloud_sync_impl/conflict_execution.py`); the facade only re-exports
it.

**Scope:** `finalize_sync_candidates`, `resolve_conflict_keep_local`,
`resolve_conflict_keep_cloud`, `resolve_conflict_merge`,
`resolve_conflict_plan`, and their module-level state. Their facade-resident
helpers already moved in Stage 4. Update import-direction layers, the
`S7_KEPT_IN_FACADE` stay-list and `test_s7_owners_reference_no_writer` scope
in `tests/test_cloud_sync_facade_identity.py` (the read-only
`conflict_plan`/`conflict_detail` owners must still reference no writer),
and the architecture ownership table.

**Non-goals:** no behavior change; no change to `PartialConflictPlanError`,
drift abort or the plan model in `conflict_plan.py`.

**Invariants at risk:** inv. 2, 3, 12, 14, 15.

**Acceptance:** relocation all `identical`; all goldens (including C-* rows)
unchanged; identity, direction and purity tests pass.

**Verification:**
`.venv/bin/python tools/cloud_sync_relocation_check.py <base-sha> <candidate-sha>`;
`.venv/bin/pytest -q tests/test_cloud_sync_facade_identity.py tests/test_cloud_sync_impl_import_direction.py tests/test_cloud_sync_orchestration_completion_golden.py tests/test_cloud_conflict_plan_execution.py tests/test_cloud_conflict_dialog.py tests/test_cloud_visibility_phase7.py tests/test_cloud_identity_fail_closed.py`

## Stage 7 — Shared reconciliation classifier

**Builds on:** Stages 1 and 6. Plan:
`docs/plans/active/2026-10-09-cloud-sync-orchestration.md` (U3 settled).

**Kind:** decision-preserving refactor. No golden may change.

**Outcome:** one pure function `classify_observation(local, remote, baseline)
-> ReconciliationDecision` (design 3.4) in `utils/cloud_sync_impl/reconciliation/`,
used by push and pull in the same commit (no period with two classifiers).

**Scope:**
- `ReconciliationDecision` carries local-only, remote-only and conflict sets
  for fields, identity (`taxon_identity` as a virtual field), images and
  measurements, plus `no_baseline_contradiction` and `remote_removed_images`,
  and exposes push and pull views that reproduce each side's current
  decision exactly, including every difference recorded in Stage 1. No
  outcome changes (U3); a difference is never resolved in this stage.
- Push (`_analyze_observation_push_conflicts`, Case F,
  `identity_review_pending`) and pull (`_remote_snapshot_has_meaningful_changes`,
  `_analyze_observation_field_changes`, `_analyze_image_changes`,
  `_remaining_local_changes_after_remote_merge`, Case F) call sites switch to
  the classifier. Old analysis functions are either the classifier's
  internals or removed if no caller remains; public names stay importable.
- Purity enforced by `tools/cloud_sync_reconciliation_purity.py`.

**Non-goals:** no completion change, no issue types, no change to any push or
pull outcome, no resolution of push/pull differences (U3).

**Invariants at risk:** inv. 5, 12, 13, 19.

**Acceptance:**
- The classifier reproduces the Stage 1 corpus (both views) and snapshot
  golden; every other golden unchanged.
- One classifier call per observation per direction; grep shows no remaining
  independent push or pull field/identity/image classification.

**Verification:**
`.venv/bin/pytest -q tests/test_cloud_sync_orchestration_classifier_golden.py tests/test_cloud_sync_reconciliation_purity.py tests/test_cloud_sync_taxonomy_identity_golden.py tests/test_cloud_identity_fail_closed.py tests/test_cloud_sync_orchestration_completion_golden.py tests/test_cloud_sync_orchestration_result_golden.py tests/test_cloud_metadata_sync.py tests/test_cloud_sync_fast_path.py`

## Stage 8 — Typed sync issues

**Builds on:** Stages 2 and 7. Plan:
`docs/plans/active/2026-10-09-cloud-sync-orchestration.md`.

**Kind:** result-preserving refactor plus one additive result key.

**Outcome:** `SyncIssue`, `SyncIssueKind` and severity (design 3.1, 3.3) in
`utils/cloud_sync_impl/sync_issues.py`; every error-producing branch creates
an issue; `result['errors']` is rebuilt as `[issue.message for issue in
issues]` in production order; new `result['issues']`.

**Scope:**
- Severity values `required`, `best_effort`, `review`, `blocked`, and
  `non_observation` (tombstones, calibrations, backfills), each issue with
  `stage`, `local_id`, `cloud_id`, `error_code` and a `retry` field naming the
  retry owner: `observation_dirty` (next sync re-pushes/re-pulls the row),
  `tombstone_row` (unsynced tombstone, D9), `backfill_selection` (persisted-data
  candidate query, D5), `calibration_dirty`, `none` (best-effort/report).
- WebP distinction (U2, conditions as recorded in "Decisions settled by the
  person"): `webp_required` has severity `required` and is produced only for
  the preparation check (`utils/cloud_sync.py:3978–3979`) and the full-image
  upload (`:7277–7278`, re-raised at `image_push.py:722–723`). An original
  upload failure, including WebP (`:7448–7449`, `:7520–7521`, caught at
  `image_push.py:1318`), is `original_upload_failed` with severity
  `best_effort`, reported in `result['issues']` only; `errors` and the
  original-upload summary stay as today. Unit tests pin both producers.
- Pull field conflicts are kind `conflict_review` with severity `required`
  and no marker; `removed_keys` is kind `cloud_removed_local_images` with a
  report-only disposition (no state write) (U1). The two pull identity
  conflicts carry explicit dispositions, not rule-6 severities:
  `pull_identity_apply_conflict` and `pull_bound_identity_review` (U1).
- Issues are produced but **not yet consumed** for state: existing state
  writes stay where they are.
- `sync_all` assembles `errors` from issues; `pull_only` result unchanged
  except the additive key.

**Non-goals:** no state change, no new strings, no D5 work.

**Invariants at risk:** inv. 10, 15, 20; UI parsing of `errors`.

**Acceptance:**
- `legacy_results.json` R-* rows byte-identical except the new `issues` key;
  `summarize_sync_issues` and `partition_download_from_cloud_issues` outputs
  identical.
- Every `errors` string in the golden maps to exactly one issue kind;
  completion and no-op goldens unchanged.

**Verification:**
`.venv/bin/pytest -q tests/test_cloud_sync_orchestration_result_golden.py tests/test_cloud_sync_orchestration_completion_golden.py tests/test_cloud_sync_orchestration_noop_golden.py tests/test_cloud_download_only.py tests/test_cloud_sync_fast_path.py <new sync_issues tests>`

## Stage 9 — Backfill issues and retry responsibility (D5)

**Builds on:** Stage 8. Plan:
`docs/plans/active/2026-10-09-cloud-sync-orchestration.md`.

**Kind:** behavior change (visibility only). Backfills never dirty the
observation (unchanged).

**Intended diffs (only these golden rows may change):**
- **Δ9a (9.2 gap 1).** `_push_measurements_for_observation`
  (`utils/cloud_sync_impl/measurements.py`) reports per-measurement push
  failures; the measurement backfill
  (`utils/cloud_sync_impl/measurement_reconcile.py`) emits one
  `non_observation` `reconcile_failed` issue per affected observation with a
  new `errors` string (P8, R-* rows with a forced per-measurement failure).
- **Δ9b (9.2 gap 2).** A summary skip because the table is missing emits a
  `best_effort` issue in `result['issues']` only (A5); `errors` unchanged.
- **Δ9c (9.2 gap 4).** The summary backfill skips observations whose summary
  push was already attempted in the same `push_all`; the duplicate error
  string disappears and the failure is one typed issue (P6/P8 combined row).

**Non-goals:** no change to which observations are selected next sync; 9.2
gap 3; no completion change.

**Acceptance:**
- Golden diffs touch only the rows above, each annotated Δ9a–Δ9c.
- A test proves a backfill failure leaves the observation's status and
  snapshot unchanged and that the next sync selects it again (retry owner
  `backfill_selection`).
- `test_spore_summary_sync.py::test_measurement_reconcile_is_idempotent_after_successful_push`
  and the no-op golden unchanged.

**Verification:**
`.venv/bin/pytest -q tests/test_spore_summary_sync.py tests/test_summary_failure_marks_observation_dirty.py tests/test_cloud_sync_fast_path.py tests/test_cloud_sync_orchestration_result_golden.py tests/test_cloud_sync_orchestration_completion_golden.py tests/test_cloud_sync_orchestration_noop_golden.py`

## Stage 10 — Completion owner: push

**Builds on:** Stages 7, 8 and 9. Plan:
`docs/plans/active/2026-10-09-cloud-sync-orchestration.md` (U2 settled;
"Completion ownership rule").

**Kind:** behavior change.

**Outcome:** `complete_observation(outcome, client)` in
`utils/cloud_sync_impl/observation_completion.py` implementing design 3.1
rules 1–6 and their precedence, with `ObservationOutcome` and `SnapshotIntent`
(`None` keeps the old baseline; `without_identity=True` for rule 6), and a
`report_only` outcome disposition that writes nothing (needed for U1
`removed_keys` in Stage 11), and `bind_cloud_id(local_id, cloud_id)`, the
owner's initial-bind transition (`cloud_id` + `dirty`, nothing else) used
after a successful remote creation. Stage 11 adds the two pull identity
dispositions. `push_all`
builds one outcome per observation and calls the owner; domain helpers return
issues and stop calling `mark_observation_dirty`, `_stamp_observation_synced`
or the snapshot store on the push path.

**Intended diffs (only these golden rows may change):**
- **Δ10a (3.2; P-create-*).** No `synced` stamp before child work; stamp
  only after required work and snapshot. After a successful remote
  **creation** the owner first persists `cloud_id` with `dirty` (no
  `synced`, no snapshot), replacing the bind inside today's early stamp
  (`utils/cloud_sync.py:9174–9183`); an update of an already-linked row
  writes nothing before completion. An interruption after the POST or PATCH
  leaves `dirty` with `cloud_id` bound, and the retry neither POSTs again nor
  loses the link (golden rows P-create-(a)/(b)). If the local bind itself
  fails after the POST, recovery is today's desktop-id dedupe in
  `_resolve_existing_observation_for_push`, unchanged (P-create-(c)).
- **Δ10b (P2, P3, P5, P6; inv. 12, 15).** A required child failure leaves
  `dirty` with the **old** baseline (today the baseline advances).
- **Δ10c (P10, 3.6(b)).** Any snapshot-store exception becomes a
  `snapshot_failed` required issue with a new `errors` string; the row is
  `dirty`; `push_all` no longer aborts on a non-`CloudSyncError` snapshot
  failure (other non-`CloudSyncError` exceptions still propagate as today).
- **Δ10d (P4, D4, 3.6(c)).** Mosaic failure adds a visible `best_effort`
  `mosaic_failed` string; status unchanged.
- **Δ10e (P1-webp-prep, P1-webp-full-upload; U2).** `webp_required` is a
  `required` issue: the row ends `dirty` with the old baseline (today it
  keeps the early `synced` stamp). P2-original-webp is unchanged: a
  best-effort original-upload warning, status and baseline unaffected.
  Completion tests cover all three rows.
- **Δ10f (3.1 rule 6, design 7 item 4).** No-baseline identity review is one
  `review` issue (no stamp-then-unstamp). With any required failure: marker,
  `dirty`, baseline unchanged. With all work succeeding: identity-less
  snapshot, marker, `dirty`. With a snapshot failure: marker kept, `dirty`,
  `snapshot_failed` via error-detail columns only.
- **Δ10g (3.9).** Accepted-asymmetry pruning on push happens one sync later
  where the baseline no longer advances after failure.
- Unchanged and pinned: P7 tombstone failure stays `non_observation`,
  retry owner `tombstone_row`, observation status untouched (D9); privacy slot
  limit `blocked`; identity-clear verification review; preflight
  `_clear_observation_dirty_if_no_real_changes` stamps `synced` with the old
  baseline (`SnapshotIntent=None`); the `dirty→synced caller=push_all` log text.

**Tests changed (design section 4):** `test_gap_c_…` asserts `dirty` state
instead of the call; `test_gap_d_…`/`test_gap_e_…` add "old baseline kept"
without stubbing `_store_remote_snapshot`;
`test_push_all_metadata_only_refreshes_signature_after_stamp` updated for
order; snapshot stubs that suppress owner behavior are retargeted to the
owner.

**Invariants at risk:** inv. 3, 4 (no duplicate remote observation), 5, 6,
12, 15, 18, 19.

**Acceptance:**
- Golden diffs touch only P-*, R-* and combined-outcome rows annotated
  Δ10a–Δ10g; no-op golden unchanged (no new remote writes).
- On the push path, only `observation_completion.py` writes `sync_status`,
  `synced_at`, the marker or the snapshot (grep/AST evidence in notes).
- Stage notes record whether each gate measurement in Gate G1 is reachable.

**Verification:**
`.venv/bin/pytest -q tests/test_cloud_sync_orchestration_completion_golden.py tests/test_cloud_sync_orchestration_result_golden.py tests/test_cloud_sync_orchestration_noop_golden.py tests/test_sync_observation_dirty_propagation.py tests/test_cloud_sync_dirty_loop_steady_state.py tests/test_cloud_sync_fast_path.py tests/test_cloud_sync_upload_completeness.py tests/test_cloud_media_measurement_mosaic_chain.py tests/test_spore_summary_sync.py tests/test_summary_failure_marks_observation_dirty.py tests/test_cloud_identity_fail_closed.py tests/test_cloud_visibility_phase7.py tests/test_cloud_metadata_sync.py tests/test_image_tombstones.py tests/test_cloud_image_bytes_desired.py`

## Gate G1 — Push completion live canary

**Kind:** manual canary that only the person performs. The live sync below is
a human-authorized canary action taken by the person; it does not authorize
any stage agent to write to Supabase or other live data. Agents never perform
it and never perform live Supabase writes.

**Position:** follows Stage 10 and blocks Stage 11 (compiled as
`gates_before` of Stage 11): after Stage 10 is accepted, Stage 11 is not
created until this gate records `pass`. Push completion ordering is the
first change that can leave live rows in a new state.

**Check `push-completion-canary`:**
1. Build: run from source at Stage 10's accepted candidate SHA in a clean
   checkout. Record the SHA.
2. Profile and account: `SPORELY_PROFILE=orch-canary` (an isolated profile)
   signed in to a disposable or known account; record which. Never a
   database linked to another account.
3. Backup: copy the profile's SQLite database and settings before step 4.
4. Baseline: run `tools/cloud_reconciliation_report.py --db <profile db>` and
   `tools/cloud_sync_state_report.py --db <profile db>`. Categories C, D1, D2,
   E and H must be zero or carry a documented exception; record A–H.
5. With `SPORELY_DEBUG_CLOUD_WRITES=1`, Sync now after adding
   one selected image and spore measurements to an observation. Expect no
   errors; that observation ends `synced` with a new snapshot hash.
6. Forced required failure, if a safe trigger exists (an image over the plan
   limit): Sync now. Expect `image_too_large_for_plan` in errors, the
   observation `dirty`, and its snapshot hash **unchanged** from before.
   If no safe trigger exists, record `not exercised` (automated Δ10b evidence
   stands).
7. Restart the app (new process), Sync now twice with no changes. Expect the
   first to count at most one instrumented mutating request (the D10
   capability RPC) and the second zero; zero errors. List any
   uninstrumented mutating path the counter reports as having run, and
   cross-check both syncs' time windows in the Supabase API log for
   non-GET requests where available.
8. Rerun both reports and diff against step 4.

**Evidence:** the plan's "Gate evidence" items 1–9, aggregate only.

**Pass criteria:** no unexpected sync errors; C, D1, D2, E, H zero or
unchanged; F and G unchanged; A/B change only by the step-5 image; status
counts change only for the observations touched; step 6 (if exercised)
shows `dirty` with the old snapshot hash; step 7 instrumented counts as
stated; and for the second no-op sync either no uninstrumented or
`UNCOVERED` path (Stage 3 table) could have run, or the Supabase API-log
cross-check shows no non-GET request in its window. If such a path could
have run and no cross-check is available, the result is `blocked`, not
`pass`: zero writes cannot be claimed.

**Fail or blocked:** either keeps Stage 11 closed. A failure is repaired as
new reviewed candidate work (a new stage or a revert), and this check is
then answered again for the repaired build with fresh evidence. Nobody
records `pass` over failed evidence or with an empty note. Restore from the
step-3 backup if local state is damaged.

## Stage 11 — Completion owner: pull, create and pull-only

**Builds on:** Stage 10. Plan:
`docs/plans/active/2026-10-09-cloud-sync-orchestration.md` (U1 settled).

**Kind:** behavior change.

**Outcome:** every `_stamp_observation_synced` and snapshot store in
`pull_all`, `_create_local_from_remote` and the cheap-convergence path becomes
an `ObservationOutcome(direction='pull'|'create')` completed by the owner.

**Intended diffs (only these golden rows may change):**
- **Δ11a (L6, 3.2).** Cheap convergence: snapshot first (reusing previous
  images and measurements), then `synced`; a snapshot failure leaves `dirty`
  with `snapshot_failed` (today swallowed, row `synced`).
- **Δ11b (1.5, 3.2).** Rows needing no work are not re-stamped; a `blocked`
  row is no longer cleared to `synced` by cheap convergence or by the
  remote-unchanged/local-clean branch.
- **Δ11c (L4a).** A measurement import failure or skipped materialization on
  an existing row is a `required` issue in every mode (today blocks only with
  `materialize=True`).
- **Δ11d (L5).** Snapshot failure blocks `synced` on existing rows and in
  `_create_local_from_remote` (no longer swallowed; new `snapshot_failed`
  string).
- **Δ11e (L2b).** A failed download in the baseline/remote-changed branch is a
  direct `remote_media_pending` required issue (end state equal to today's
  retry-block result; mechanism changes).
- Unchanged and pinned (U1): pull field conflicts end `dirty` with no review
  marker and no snapshot; `removed_keys` makes no state write and no snapshot
  (owner `report_only` disposition); the legacy strings are unchanged.
- Unchanged and pinned (U1), each as an explicit owner disposition, never
  through design 3.1 rule 6: `pull_identity_apply_conflict` reproduces
  L-identity-apply-conflict exactly (`dirty`, `synced_at` NULL, errors
  cleared, no marker, identity-free snapshot stored even on media failure —
  recorded as deferred technical debt); `pull_bound_identity_review`
  reproduces L-bound-identity-contradiction exactly (`dirty` + marker, no
  snapshot). `_create_local_from_remote` keeps binding `cloud_id` + `dirty`
  first (now through the owner's initial-bind transition).
- Unchanged and pinned: D1 — existing rows with `materialize=False` may
  complete without bytes (L3a); new rows created with `materialize=False`
  stay `dirty` (L3b, 9.1); D2 — pull-only writes `synced` locally with
  `cloud_writes_completed == 0` and empty `blocked_write_attempts` (L8);
  `set_desktop_id` guard and
  child-change cursor advancement unchanged.

**Non-goals:** no conflict-dialog change; no change to which observations are
review candidates (U1).

**Invariants at risk:** inv. 5, 10, 11, 12, 18, 20; contract rule 16.

**Acceptance:**
- Golden diffs touch only L-*, R-* rows annotated Δ11a–Δ11e; no-op golden
  unchanged. The pull-field-conflict, `removed_keys`,
  L-identity-apply-conflict and L-bound-identity-contradiction rows are
  byte-identical (U1).
- In `pull_all`/`_create_local_from_remote`, only the owner writes completion
  state (grep/AST evidence).
- Stage notes record whether each Gate G2 measurement is reachable.

**Verification:**
`.venv/bin/pytest -q tests/test_cloud_sync_orchestration_completion_golden.py tests/test_cloud_sync_orchestration_result_golden.py tests/test_cloud_sync_orchestration_noop_golden.py tests/test_cloud_sync_fast_path.py tests/test_cloud_download_only.py tests/test_child_change_probe.py tests/test_cloud_identity_fail_closed.py tests/test_cloud_sync_dirty_loop_steady_state.py tests/test_cloud_image_bytes_desired.py tests/test_image_tombstones.py`

## Stage 12 — Completion owner: conflict execution

**Builds on:** Stage 11. Plan:
`docs/plans/active/2026-10-09-cloud-sync-orchestration.md`.

**Kind:** behavior change.

**Outcome:** `resolve_conflict_keep_local`, `resolve_conflict_keep_cloud`,
`resolve_conflict_merge` and `resolve_conflict_plan` build
`ObservationOutcome(direction='conflict_resolution')` and complete through the
owner; `finalize_sync_candidates` clears the marker through the owner
(design 3.7).

**Intended diffs (only these golden rows may change):**
- **Δ12a (C-keep_local).** No stamp before images/measurements; a measurement
  push failure leaves `dirty` with the old snapshot (today `synced` with old
  snapshot); the raised exception type and message unchanged.
- **Δ12b (C-keep_cloud, C-merge).** A snapshot failure leaves `dirty` (today
  `synced` with old baseline); merge stamps only after images and snapshot.
- **Δ12c (D4).** Mosaic failure in keep-local is visible as `best_effort`.
- Unchanged and pinned: `resolve_conflict_plan` order snapshot → signature →
  stamp; `PartialConflictPlanError` (retry with `prior_result`, drift abort,
  no media deletion; the owner raises the existing `_partial_error` on
  `snapshot_failed`); keep-cloud image-application warnings stay best-effort
  (A4); privacy limit `blocked`; tombstone-before-push in keep-local.

**Non-goals:** no conflict-dialog change (U1).

**Tests changed:** `_patch_common` stamp/snapshot recorders move to the owner;
`test_keep_cloud_disables_deletion_…` call-name assertions retargeted; outcome
assertions unchanged.

**Invariants at risk:** inv. 2, 3, 12, 14, 15.

**Acceptance:**
- Golden diffs touch only C-* rows annotated Δ12a–Δ12c.
- `test_finalization_order_snapshot_before_stamp` and
  `test_snapshot_failure_leaves_conflict_unsealed` pass unchanged in intent.
- Stage notes record whether each Gate G2 measurement is reachable.

**Verification:**
`.venv/bin/pytest -q tests/test_cloud_conflict_plan_execution.py tests/test_cloud_conflict_dialog.py tests/test_cloud_visibility_phase7.py tests/test_cloud_sync_orchestration_completion_golden.py tests/test_cloud_sync_orchestration_result_golden.py tests/test_cloud_identity_fail_closed.py`

## Gate G2 — Pull and conflict completion live canary

**Kind:** manual canary that only the person performs. The live sync below is
a human-authorized canary action taken by the person; it does not authorize
any stage agent to write to Supabase or other live data. Agents never perform
it and never perform live Supabase writes.

**Position:** follows Stage 12 and blocks Stage 13 (compiled as
`gates_before` of Stage 13): after Stage 12 is accepted, Stage 13 is not
created until this gate records `pass`. Pull,
create, pull-only and conflict execution completion have all switched owner.

**Check `pull-conflict-completion-canary`:**
1. Build: source run at Stage 12's accepted candidate SHA; record it.
2. Profile/account and backup as Gate G1 steps 2–3 (fresh backup).
3. Baseline reports as Gate G1 step 4.
4. On the web or Android client, edit a field and add one image to an
   existing observation; Refresh on desktop with
   `SPORELY_DEBUG_CLOUD_WRITES=1`. Expect the field and image locally, the
   observation `synced`, a new snapshot hash, no errors.
5. D1: on the web, create a new observation with one image and edit another
   existing one. In the cloud sync dialog uncheck "pull images" and sync.
   Expect the new local row `dirty` (9.1) and the edited existing row
   `synced`. Then Sync now (materialize on): the new row becomes `synced`.
6. D2: Download from Cloud. Expect zero instrumented mutating requests, no
   uninstrumented mutating path reported as run, no blocked write attempts,
   and completion states written locally.
7. Conflict review: edit the same field on web and desktop, Sync now. Expect
   the review marker and `dirty` (push-side preflight; pull-side marker
   outcomes are unchanged by U1). Resolve in the conflict dialog (keep local).
   Expect marker cleared, `synced`, and no further conflict on the next sync.
8. D9 tombstone: delete one cloud image on desktop ("remove cloud copy"),
   Sync now. Expect the pending tombstone count 0 afterwards and the
   observation `synced`. A tombstone failure is not safely triggerable live;
   record `failure path: automated evidence only`.
9. Restart, two no-change Sync now runs: at most one instrumented mutating
   request (D10) on the first, zero on the second; list uninstrumented paths
   reported as run; Supabase API-log cross-check where available.
10. Rerun both reports and diff against step 3.

**Evidence:** the plan's "Gate evidence" items 1–9, aggregate only.

**Pass criteria:** steps 4–9 as stated; no unexpected errors; C, D1, D2, E, H
zero or unchanged; F, G unchanged; A/B change only by images added in steps
4–5 or removed in step 8; status counts change only for touched
observations; no `blocked` row cleared without a local edit; on steps 6 and
9 (second sync) instrumented counts as stated and either no uninstrumented
or `UNCOVERED` path could have run or the API-log cross-check shows no
non-GET request; otherwise the result is `blocked`, not `pass`.

**Fail or blocked:** either keeps Stage 13 closed. A failure is repaired as
new reviewed candidate work, and this check is then answered again for the
repaired build with fresh evidence. Nobody records `pass` over failed
evidence or with an empty note.

## Stage 13 — Other writers: on-demand materialization and manual import

**Builds on:** Stage 12. Plan:
`docs/plans/active/2026-10-09-cloud-sync-orchestration.md` (U5 settled).

**Kind:** behavior change, including a UI path (`ui/observations_tab.py`).

**Outcome:** design 3.8 for D3 and D7.

**Intended diffs (only these golden rows may change):**
- **Δ13a (D3, W-materialize).** `materialize_cloud_media_for_observation`
  persists the baseline only through the owner and only when downloads and
  measurement link-up are complete; otherwise the old baseline stays. It does
  not write `sync_status` (as today). Warning text unchanged.
- **Δ13b (D7, W-import).** `ui/observations_tab.py::_mark_cloud_observation_imported`
  becomes a `create` outcome: snapshot and `synced` only after image downloads
  complete; `synced_at` in UTC; the unconditional `set_image_desktop_id` /
  `set_desktop_id` writes are replaced by the guarded helpers
  (`_remote_image_desktop_id_current`, inequality guard), so value-identical
  writes are skipped; swallowed exceptions become issues surfaced through the
  existing UI message path. Positional image pairing (`zip` over list
  order) is preserved exactly (U5); a test pins it.

**Non-goals:** no UI layout change; no new dialogs; no change to which
images are downloaded.

**Invariants at risk:** inv. 4, 12, 16, 18; the no-op write rule.

**Acceptance:**
- Golden diffs touch only W-* rows annotated Δ13a–Δ13b.
- A test proves the manual import performs no `desktop_id` write when the
  remote value already matches.
- Interactive verification of the import path is held by Completion gate G3.

**Verification:**
`.venv/bin/pytest -q tests/test_cloud_sync_orchestration_completion_golden.py tests/test_observations_tab_cloud_sync.py tests/test_cloud_download_only.py tests/test_child_change_probe.py <new manual-import tests>`

## Stage 14 — Single-writer guard and desktop documentation

**Builds on:** Stage 13. Plan:
`docs/plans/active/2026-10-09-cloud-sync-orchestration.md`.

**Kind:** tests and docs. No behavior change.

**Outcome:** the single-writer rule is enforced by a test and documented.

**Scope:**
- AST/SQL-text test `tests/test_cloud_sync_completion_single_writer.py`
  enforcing the plan's "Completion ownership rule" exactly, over
  `utils/`, `database/` and `ui/`: outside `observation_completion.py`, no
  code calls `update_observation_sync_state`, `_store_remote_snapshot`,
  `_store_cloud_observation_snapshot`, `_stamp_observation_synced`,
  `_set_observation_sync_state`, the blocked/marker/error writers, or
  executes SQL writing `observations.sync_status`, `synced_at`,
  `sync_error_*` or `sync_blocked_*`, except the enumerated allow-list of
  rule items 2 and 3 (`mark_observation_sync_dirty`, the calibration raw
  `UPDATE` at `recalculate_measurements_for_calibration`,
  `mark_observation_dirty`/`mark_observation_media_dirty` from local-edit
  callers, the sync-time scans, `reset_cloud_sync_state`,
  `unlink_local_observation_from_cloud`), each with its reason. Allow-listed
  writers are checked to write only `dirty` (plus, for
  `mark_observation_sync_dirty`, the existing column clears). A stale
  allow-list entry fails the test. The owner's initial `cloud_id` bind and
  the two pull identity dispositions are owner transitions on the guard's
  owned list. The guard covers UPDATE writes; INSERT sites are checked
  against the insert allow-list of ownership rule item 4
  (`utils/archive/portable_import.py:2770`, `utils/db_share.py:777`,
  `database/models.py:1420`). The `images.synced_at` writes are image link
  state and out of the rule.
- `docs/cloud-sync-architecture.md`: ownership table, sections C, H and I
  rewritten to the implemented model; the 1.8 disagreement table resolved.
- `.claude/rules/cloud-sync.md`: one bullet naming the completion owner, the
  local-edit allow-list, and that domain helpers return issues.
- `docs/cloud-sync-orchestration-design.md`: status line "implemented by …".
- `docs/supabase-sync-contract.md` is **not** edited here (see the
  sporely-web track).

**Acceptance:** all goldens unchanged from Stage 13; the guard test fails on a
synthetic violation and passes on the tree; docs cite symbols that exist.

**Verification:**
`.venv/bin/pytest -q <new single-writer test> tests/test_cloud_sync_facade_identity.py tests/test_cloud_sync_impl_import_direction.py tests/test_cloud_sync_orchestration_completion_golden.py tests/test_cloud_sync_orchestration_result_golden.py tests/test_cloud_sync_orchestration_noop_golden.py`

## Completion gate G3 — End-to-end completion live canary

**Kind:** manual canary that only the person performs. The live sync below is
a human-authorized canary action taken by the person; it does not authorize
any stage agent to write to Supabase or other live data. Agents never perform
it and never perform live Supabase writes.

**Position:** follows Stage 14 and blocks plan completion (compiled as
`completion_gates`): after Stage 14 is accepted, the plan does not report
COMPLETE until this gate records `pass`. Merging
the plan's work to `main` is a separate human decision after this pass.

**Check `end-to-end-completion-canary`:**
1. Build: source run at Stage 14's accepted candidate SHA; record it.
2. Profile/account, backup and baseline reports as Gate G1 steps 2–4.
3. Repeat Gate G1 steps 5–7 and Gate G2 steps 4–9 on this build.
4. Manual import (D7): import one cloud observation into a new local row from
   the Observations tab. Expect images present, the row `synced` with a
   snapshot hash, images paired as before (positional, U5), and on the next
   no-change sync zero instrumented `desktop_id` writes.
5. On-demand materialization (D3): open a cloud observation whose media is
   missing and download it. Expect the snapshot hash to change only if the
   download completed; with the network disabled mid-download, expect the
   hash unchanged.
6. Rerun both reports and diff against the baseline.

**Evidence:** the plan's "Gate evidence" items 1–9, aggregate only.

**Pass criteria:** every expectation of the repeated G1/G2 steps and steps
4–5 holds; no unexpected errors; C, D1, D2, E, H zero or unchanged; F, G
unchanged; no-op syncs count zero instrumented mutating requests apart from
the single D10 RPC per process, and either no uninstrumented or `UNCOVERED`
path could have run or the API-log cross-check shows no non-GET request;
otherwise the result is `blocked`, not `pass`.

**Fail or blocked:** either keeps plan completion closed. A failure is
repaired as new reviewed candidate work, and this check is then answered
again for the repaired build with fresh evidence. Nobody records `pass` over
failed evidence or with an empty note.

## Track W — sporely-web shared contract wording (separate, not authorized)

Design section 6: no shared-contract change is required for this plan. The
following wording is optional and, if chosen, must land identically in
`sporely-py/docs/supabase-sync-contract.md` and the `sporely-web` copy, under
its own plan and approval, following `sporely-web`'s instructions. Nothing
here authorizes editing `sporely-web`, a migration, or a deployment.

- **W1 (D9, rule 8).** Append: "Deletion work stays retryable through the
  durable unsynced tombstone; a failed tombstone flush does not keep the
  observation unsynced."
- **W2 (optional, rule 8).** Append the design 6 sentence that the rule
  applies to pull as well as push.
- **W3 (optional, D4).** In "Public spore mosaic after conflict resolution":
  "Mosaic generation is best-effort and never blocks observation sync
  completion."

Approvals that would be needed and are not given here: a sporely-web
candidate (contract text only), its review, and the person's merge decision.
No database migration, RLS change, production write or web deployment is
implied by any of W1–W3.
