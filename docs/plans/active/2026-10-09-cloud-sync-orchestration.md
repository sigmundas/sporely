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
| D1 | (a) for existing observations only; a new row created with `materialize_remote_images=False` stays `dirty` (9.1) | Stage 10 (pinned in Stage 2) |
| D2 | (a) pull-only uses pull completion rules; `synced` written locally, zero cloud writes | Stage 10 |
| D3 | (a) on-demand materialization writes the baseline only through the owner and only when complete | Stage 12 |
| D4 | (a) spore mosaic is best-effort and visible in `errors` | Stage 9, Stage 11 |
| D5 | (b) backfills retry independently; typed issues; never dirty the observation; 9.2 gaps 1, 2 and 4 | Stage 8 |
| D6 | (b) rules change to match code: Observations-tab Refresh equals Sync now (`sync_images=True`) | Stage 1 (rules text only) |
| D7 | (a) manual import of a cloud observation becomes a `create` outcome under the owner | Stage 12 |
| D8 | (a) already landed on `main` (`23fae1b`) | pinned in Stage 2 |
| D9 | (a) tombstone failures do not block observation completion; the unsynced tombstone is the retry record | Stage 9 (pinned); contract wording in the sporely-web track |
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
  write; Stage 12 removes some (D7). Pull-only stays zero-cloud-write.
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
| 5 Pure move: orchestration and conflict execution | pure move | 4 |
| 6 Shared reconciliation classifier | decision-preserving refactor | 1, 5 |
| 7 Typed sync issues | result-preserving refactor + additive key | 2, 6 |
| 8 Backfill issues and retry responsibility (D5) | behavior change (visibility) | 7 |
| 9 Completion owner: push | behavior change | 6, 7, 8 |
| 10 Completion owner: pull, create and pull-only | behavior change | 9 (and Gate G1) |
| 11 Completion owner: conflict execution | behavior change | 10 |
| 12 Other writers: on-demand materialization and manual import | behavior change (incl. UI path) | 11 (and Gate G2) |
| 13 Single-writer guard and desktop documentation | tests + docs | 12 |

Gates (manual, person only; see their sections): **Gate G1** after Stage 9,
holding Stage 10. **Gate G2** after Stage 11, holding Stage 12. **Completion
gate G3** after Stage 13, holding plan completion.

**Execution route.** Stages use the direct `## Stage <n> — <title>` convention
so `sparring check-plan` parses them. The gates can only be carried by compile
intake (version 2 manifest): G1 as `gates_before` on Stage 10, G2 as
`gates_before` on Stage 12, G3 as `completion_gates`. A direct Markdown run
would drop the gates and is therefore not an acceptable route for this plan.

Engine-gated verification: Stages 9–12 are committed, pushed and accepted on
automated evidence; the gate that follows holds the next stage until the
person records `pass` (AGENTS.md, engine-gated exception). Agents never run a
live sync against Supabase, never perform live Supabase writes, and never run
the canaries.

**Retirement of transitional scaffolding is not in this plan.** The design
document does not define it (no scope, no retirement conditions for
`tests/cloud_sync_owner_patching.py`, the conftest fixture
`_facade_patches_reach_cloud_sync_owners`, or the facade-identity stay-lists).
See unresolved decision U4.

## Assumptions

- A1. Characterization runs on fake clients and real SQLite (the existing
  test pattern); the local Supabase harness is not used for media paths
  (design 7). Stage 2 may add observation metadata device actions to
  `tests/local_supabase/device.py` only as opt-in tests.
- A2. All mutating client HTTP requests pass through
  `_request_with_transient_retry` or an enumerable small set of other call
  sites (R2/Worker upload); Stage 3 enumerates them. If that is false, the
  write counter is incomplete and Stage 3 says so; the canaries then fall back
  to the Supabase project API log for the account and time window, read by the
  person.
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
  on deep verification) and the optional calibration raw `UPDATE` reroute
  (design 3.5) are out of scope.

## Unresolved decisions (not settled by design section 9)

These must be answered at intake (compile `needs_decision`) or before plan
approval. Each names the stage that depends on it.

- **U1. Pull-side review marker (Stage 10).** Design 3.3 lists pull conflict
  fields and `removed_keys` as `review` producers, which under 3.1 rule 2 sets
  the conflict-review marker. Today the pull path never sets the marker for
  ordinary field conflicts (row ends `dirty`, no snapshot, error string) and
  `removed_keys` makes no state write (a clean row stays `synced`). Options:
  (a) adopt design 3.3 literally (pull conflicts and `removed_keys` set
  `dirty` + marker and appear as review candidates); (b) preserve today's
  state outcomes (pull field conflicts become `required`-severity
  `conflict_review` issues without a marker; `removed_keys` stays report-only
  with no state write and no snapshot). Recommendation: (b) in this plan,
  (a) as a later product change, because (a) changes what the conflict dialog
  shows.
- **U2. `webp_required` severity (Stage 9).** Today the WebP-required branch
  makes no state write and the row keeps the early `synced` stamp. Removing the
  early stamp forces a choice: (a) `required` (row `dirty`, re-pushed every
  sync until WebP is available; no remote writes thanks to the no-op PATCH
  skip, but repeated preflight reads); (b) `blocked` with a distinct reason
  (visible, not retried until a local change). Recommendation: (a), matching
  contract rule 8 ("image … work failed").
- **U3. Push/pull classifier differences (Stage 6).** Stage 1 records every
  corpus row where today's push and pull decisions differ. Stage 6 preserves
  each side's decision exactly (side-specific views over one classification).
  Any difference the person wants removed is a separate decision after Stage 1;
  none is removed in this plan by default.
- **U4. Transitional scaffolding retirement.** Whether and when to retire
  `tests/cloud_sync_owner_patching.py` and its autouse fixture, the
  `S6_*`/`S7_KEPT_IN_FACADE` stay-lists in
  `tests/test_cloud_sync_facade_identity.py`, and the relocation-check tooling.
  Not in the design; would need its own plan with retirement conditions and a
  live-canary gate before it.
- **U5. Manual-import image pairing (Stage 12).** `_mark_cloud_observation_imported`
  pairs local and cloud images by list position. D7 brings completion under
  the owner; it does not decide whether pairing moves to stable ids. Stage 12
  keeps positional pairing unless the person decides otherwise.

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

Reference-library sync (including making the D10 device report
change-driven), R2 garbage collection, media selection and anchor logic
(inv. 1–2, 7–9, 16–17), child-change cursor semantics, caller-preset changes
(D6 is a rules-text change only), a persisted media-pending marker for new
rows (9.1 follow-up), account link/reset, `SporelyCloudClient` class
relocation, UI redesign, scaffolding retirement (U4).

## Stage 1 — Classifier and snapshot characterization

**Builds on:** `main` @ `03a9adf`. Plan:
`docs/plans/active/2026-10-09-cloud-sync-orchestration.md` (read its
"Invariants held by every stage", "Golden discipline" and "Unresolved
decisions" sections).

**Kind:** tests and rules text only. No production code changes.

**Outcome:** a stored decision corpus and snapshot byte golden on the
pre-change code (design 7 items 1–2), and the D6/D10 rules text.

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
   push and pull decide differently (input to U3).
5. `.claude/rules/cloud-sync.md`: replace the Refresh bullet with the D6
   preset (`sync_images=True`, same as Sync now) and add the D10 exception
   (one `record_reference_client_capabilities` RPC per app process and
   account on a non-pull-only sync is permitted on a no-op sync).

**Non-goals:** no production change; no classifier change; no test removal.

**Acceptance:**
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
     re-stamped `synced`), L7, L8, pull Case F, pull field conflict,
     `removed_keys`, remote-unchanged/local-clean re-stamp (including a
     `blocked` row).
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
and privacy").

**Kind:** additive diagnostic only. Default off. No sync behavior change.

**Outcome:** the person can measure, during a live canary, completion state
and remote writes without an agent touching live data.

**Scope:**
1. `tools/cloud_sync_state_report.py` (read-only SQLite, `--db PATH`,
   `--json OUT`, `--observation ID`): counts of `sync_status` per value
   (`synced`, `dirty`, `blocked`, NULL), review-marker count, rows with error
   columns, pending unsynced tombstone count, and per observation id a
   snapshot hash. Opens SQLite read-only (`mode=ro` URI); never imports Qt or
   the cloud client.
2. A per-sync remote mutating-request counter, enabled only by an environment
   variable (name chosen in the stage, e.g. `SPORELY_DEBUG_CLOUD_WRITES=1`),
   recorded at `_request_with_transient_retry` and at every other mutating
   call site the stage enumerates (A2). At the end of `sync_all` it logs one
   line with the total and per (method, table/RPC/bucket) counts. Off: no
   behavior or output change.
3. Tests: the report on a fixture database; the counter off is a no-op
   (the Stage 2 goldens are unchanged) and on counts the Stage 2 N-* rows.
4. Short usage note in `docs/cloud-sync-architecture.md` (verification
   section).

**Non-goals:** no new `sync_all` result key; no change to what is written.

**Acceptance:**
- Stage 1–2 goldens unchanged; counter off produces byte-identical logs
  except where the env var is set.
- The stage notes list every mutating request path and whether it is
  counted. Any uncounted path is named.
- Logged content contains no URL query values, payloads, headers or tokens.

**Verification:**
`.venv/bin/pytest -q tests/test_cloud_sync_orchestration_noop_golden.py tests/test_cloud_sync_orchestration_completion_golden.py <new tool/counter tests>`;
`.venv/bin/python -m py_compile tools/cloud_sync_state_report.py`.

## Stage 4 — Pure move: completion writers and leaf helpers

**Builds on:** Stages 2 and 3. Plan:
`docs/plans/active/2026-10-09-cloud-sync-orchestration.md`.

**Kind:** pure move. Every relocated entry must be `identical`.

**Outcome:** the facade-resident dependencies of orchestration live in owners,
so Stage 5 can move orchestration without an upward facade import.

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

## Stage 5 — Pure move: orchestration and conflict execution

**Builds on:** Stage 4. Plan:
`docs/plans/active/2026-10-09-cloud-sync-orchestration.md`.

**Kind:** pure move. Every relocated entry must be `identical`.

**Outcome:** orchestration execution lives in owners; the facade only
re-exports it.

**Scope (proposed owners: `push_orchestration.py`, `pull_orchestration.py`,
`sync_orchestration.py`, `conflict_execution.py`, `spore_summary_push.py`):**
`push_all`, `_push_summary_for_current_observation`,
`_reconcile_missing_spore_summaries`, `pull_all`, `_create_local_from_remote`,
`_import_remote_measurements_for_observation`, `sync_all`,
`finalize_sync_candidates`, `resolve_conflict_keep_local`,
`resolve_conflict_keep_cloud`, `resolve_conflict_merge`,
`resolve_conflict_plan`, and their module-level state.
- `SporelyCloudClient` stays in the facade. Annotation-only references use
  `TYPE_CHECKING` imports. The runtime fallback
  `SporelyCloudClient.from_stored_credentials()` in
  `_import_remote_measurements_for_observation` is the one known obstacle:
  either the function stays in the facade (preferred if every sync caller
  passes a client), or one `FACADE_IMPORT_ALLOWLIST` entry is recorded with a
  reason and the retirement condition "client construction leaves the
  facade". `materialize_cloud_media_for_observation` stays in the facade
  (UI entry point; Stage 12 routes its baseline through the owner).
- Update import-direction layers, facade identity lists and the architecture
  ownership table.

**Non-goals:** no behavior change; `unlink_local_observation_from_cloud`,
`reset_cloud_sync_state` and client methods stay where they are.

**Invariants at risk:** all of inv. 1–20 (whole-path move); especially 3, 10,
18, 20.

**Acceptance:** as Stage 4 (relocation all `identical`; all goldens
unchanged; identity, direction and purity tests pass), plus the sync safety
tests below.

**Verification:**
`.venv/bin/python tools/cloud_sync_relocation_check.py <base-sha> <candidate-sha>`;
`.venv/bin/pytest -q tests/test_cloud_sync_facade_identity.py tests/test_cloud_sync_impl_import_direction.py tests/test_cloud_sync_taxonomy_identity_golden.py tests/test_cloud_sync_orchestration_completion_golden.py tests/test_cloud_sync_orchestration_result_golden.py tests/test_cloud_sync_orchestration_noop_golden.py tests/test_cloud_sync_fast_path.py tests/test_cloud_sync_dirty_loop_steady_state.py tests/test_cloud_download_only.py tests/test_image_tombstones.py tests/test_cloud_image_bytes_desired.py tests/test_child_change_probe.py tests/test_cloud_conflict_plan_execution.py tests/test_cloud_conflict_dialog.py tests/test_reference_client_capability_stage_m.py`

## Stage 6 — Shared reconciliation classifier

**Builds on:** Stages 1 and 5. Plan:
`docs/plans/active/2026-10-09-cloud-sync-orchestration.md` (U3).

**Kind:** decision-preserving refactor. No golden may change.

**Outcome:** one pure function `classify_observation(local, remote, baseline)
-> ReconciliationDecision` (design 3.4) in `utils/cloud_sync_impl/reconciliation/`,
used by push and pull in the same commit (no period with two classifiers).

**Scope:**
- `ReconciliationDecision` carries local-only, remote-only and conflict sets
  for fields, identity (`taxon_identity` as a virtual field), images and
  measurements, plus `no_baseline_contradiction` and `remote_removed_images`,
  and exposes push and pull views that reproduce each side's current
  decision exactly, including every difference recorded in Stage 1 (U3).
- Push (`_analyze_observation_push_conflicts`, Case F,
  `identity_review_pending`) and pull (`_remote_snapshot_has_meaningful_changes`,
  `_analyze_observation_field_changes`, `_analyze_image_changes`,
  `_remaining_local_changes_after_remote_merge`, Case F) call sites switch to
  the classifier. Old analysis functions are either the classifier's
  internals or removed if no caller remains; public names stay importable.
- Purity enforced by `tools/cloud_sync_reconciliation_purity.py`.

**Non-goals:** no completion change, no issue types, no removal of push/pull
differences.

**Invariants at risk:** inv. 5, 12, 13, 19.

**Acceptance:**
- The classifier reproduces the Stage 1 corpus (both views) and snapshot
  golden; every other golden unchanged.
- One classifier call per observation per direction; grep shows no remaining
  independent push or pull field/identity/image classification.

**Verification:**
`.venv/bin/pytest -q tests/test_cloud_sync_orchestration_classifier_golden.py tests/test_cloud_sync_reconciliation_purity.py tests/test_cloud_sync_taxonomy_identity_golden.py tests/test_cloud_identity_fail_closed.py tests/test_cloud_sync_orchestration_completion_golden.py tests/test_cloud_sync_orchestration_result_golden.py tests/test_cloud_metadata_sync.py tests/test_cloud_sync_fast_path.py`

## Stage 7 — Typed sync issues

**Builds on:** Stages 2 and 6. Plan:
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

## Stage 8 — Backfill issues and retry responsibility (D5)

**Builds on:** Stage 7. Plan:
`docs/plans/active/2026-10-09-cloud-sync-orchestration.md`.

**Kind:** behavior change (visibility only). Backfills never dirty the
observation (unchanged).

**Intended diffs (only these golden rows may change):**
- **Δ8a (9.2 gap 1).** `_push_measurements_for_observation`
  (`utils/cloud_sync_impl/measurements.py`) reports per-measurement push
  failures; the measurement backfill
  (`utils/cloud_sync_impl/measurement_reconcile.py`) emits one
  `non_observation` `reconcile_failed` issue per affected observation with a
  new `errors` string (P8, R-* rows with a forced per-measurement failure).
- **Δ8b (9.2 gap 2).** A summary skip because the table is missing emits a
  `best_effort` issue in `result['issues']` only (A5); `errors` unchanged.
- **Δ8c (9.2 gap 4).** The summary backfill skips observations whose summary
  push was already attempted in the same `push_all`; the duplicate error
  string disappears and the failure is one typed issue (P6/P8 combined row).

**Non-goals:** no change to which observations are selected next sync; 9.2
gap 3; no completion change.

**Acceptance:**
- Golden diffs touch only the rows above, each annotated Δ8a–Δ8c.
- A test proves a backfill failure leaves the observation's status and
  snapshot unchanged and that the next sync selects it again (retry owner
  `backfill_selection`).
- `test_spore_summary_sync.py::test_measurement_reconcile_is_idempotent_after_successful_push`
  and the no-op golden unchanged.

**Verification:**
`.venv/bin/pytest -q tests/test_spore_summary_sync.py tests/test_summary_failure_marks_observation_dirty.py tests/test_cloud_sync_fast_path.py tests/test_cloud_sync_orchestration_result_golden.py tests/test_cloud_sync_orchestration_completion_golden.py tests/test_cloud_sync_orchestration_noop_golden.py`

## Stage 9 — Completion owner: push

**Builds on:** Stages 6, 7 and 8. Plan:
`docs/plans/active/2026-10-09-cloud-sync-orchestration.md` (U2).

**Kind:** behavior change.

**Outcome:** `complete_observation(outcome, client)` in
`utils/cloud_sync_impl/observation_completion.py` implementing design 3.1
rules 1–6 and their precedence, with `ObservationOutcome` and `SnapshotIntent`
(`None` keeps the old baseline; `without_identity=True` for rule 6). `push_all`
builds one outcome per observation and calls the owner; domain helpers return
issues and stop calling `mark_observation_dirty`, `_stamp_observation_synced`
or the snapshot store on the push path.

**Intended diffs (only these golden rows may change):**
- **Δ9a (3.2).** No `synced` stamp before child work; stamp only after
  required work and snapshot. An interruption after the PATCH leaves `dirty`
  (re-push is idempotent, inv. 4 and the no-op PATCH skip).
- **Δ9b (P2, P3, P5, P6; inv. 12, 15).** A required child failure leaves
  `dirty` with the **old** baseline (today the baseline advances).
- **Δ9c (P10, 3.6(b)).** Any snapshot-store exception becomes a
  `snapshot_failed` required issue with a new `errors` string; the row is
  `dirty`; `push_all` no longer aborts on a non-`CloudSyncError` snapshot
  failure (other non-`CloudSyncError` exceptions still propagate as today).
- **Δ9d (P4, D4, 3.6(c)).** Mosaic failure adds a visible `best_effort`
  `mosaic_failed` string; status unchanged.
- **Δ9e (P1 WebP branch, U2).** WebP-required follows the U2 answer
  (recommended: `required`, row `dirty`).
- **Δ9f (3.1 rule 6, design 7 item 4).** No-baseline identity review is one
  `review` issue (no stamp-then-unstamp). With any required failure: marker,
  `dirty`, baseline unchanged. With all work succeeding: identity-less
  snapshot, marker, `dirty`. With a snapshot failure: marker kept, `dirty`,
  `snapshot_failed` via error-detail columns only.
- **Δ9g (3.9).** Accepted-asymmetry pruning on push happens one sync later
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

**Invariants at risk:** inv. 3, 4, 5, 6, 12, 15, 18, 19.

**Acceptance:**
- Golden diffs touch only P-*, R-* and combined-outcome rows annotated
  Δ9a–Δ9g; no-op golden unchanged (no new remote writes).
- On the push path, only `observation_completion.py` writes `sync_status`,
  `synced_at`, the marker or the snapshot (grep/AST evidence in notes).
- Stage notes record whether each gate measurement in Gate G1 is reachable.

**Verification:**
`.venv/bin/pytest -q tests/test_cloud_sync_orchestration_completion_golden.py tests/test_cloud_sync_orchestration_result_golden.py tests/test_cloud_sync_orchestration_noop_golden.py tests/test_sync_observation_dirty_propagation.py tests/test_cloud_sync_dirty_loop_steady_state.py tests/test_cloud_sync_fast_path.py tests/test_cloud_sync_upload_completeness.py tests/test_cloud_media_measurement_mosaic_chain.py tests/test_spore_summary_sync.py tests/test_summary_failure_marks_observation_dirty.py tests/test_cloud_identity_fail_closed.py tests/test_cloud_visibility_phase7.py tests/test_cloud_metadata_sync.py tests/test_image_tombstones.py tests/test_cloud_image_bytes_desired.py`

## Gate G1 — Push completion live canary

**Kind:** manual canary that only the person performs. Agents never perform
it and never perform live Supabase writes.

**Position:** follows Stage 9 and holds Stage 10: after Stage 9 is accepted,
Stage 10 does not start until this gate records `pass`. Push completion
ordering is the first change that can leave live rows in a new state.

**Check `push-completion-canary`:**
1. Build: run from source at Stage 9's accepted candidate SHA in a clean
   checkout (A6). Record the SHA.
2. Profile and account: `SPORELY_PROFILE=orch-canary` (an isolated profile)
   signed in to a disposable or known account; record which. Never a
   database linked to another account.
3. Backup: copy the profile's SQLite database and settings before step 4.
4. Baseline: run `tools/cloud_reconciliation_report.py --db <profile db>` and
   `tools/cloud_sync_state_report.py --db <profile db>`. Categories C, D1, D2,
   E and H must be zero or carry a documented exception; record A–H.
5. With `SPORELY_DEBUG_CLOUD_WRITES=1` (Stage 3 name), Sync now after adding
   one selected image and spore measurements to an observation. Expect no
   errors; that observation ends `synced` with a new snapshot hash.
6. Forced required failure, if a safe trigger exists (an image over the plan
   limit): Sync now. Expect `image_too_large_for_plan` in errors, the
   observation `dirty`, and its snapshot hash **unchanged** from before.
   If no safe trigger exists, record `not exercised` (automated Δ9b evidence
   stands).
7. Restart the app (new process), Sync now twice with no changes. Expect the
   first to log at most one mutating request (the D10 capability RPC) and the
   second zero; zero errors.
8. Rerun both reports and diff against step 4.

**Pass criteria:** no unexpected sync errors; C, D1, D2, E, H zero or
unchanged; F and G unchanged; A/B change only by the step-5 image; status
counts change only for the observations touched; step 6 (if exercised)
shows `dirty` with the old snapshot hash; step 7 write counts as stated.

**Fail or blocked:** either keeps Stage 10 from starting. A failure is
repaired as new reviewed candidate work (a new stage or a revert), and this
check is then answered for the repaired build. Nobody records `pass` over
failed evidence. Restore from the step-3 backup if local state is damaged.

## Stage 10 — Completion owner: pull, create and pull-only

**Builds on:** Stage 9. Plan:
`docs/plans/active/2026-10-09-cloud-sync-orchestration.md` (U1).

**Kind:** behavior change.

**Outcome:** every `_stamp_observation_synced` and snapshot store in
`pull_all`, `_create_local_from_remote` and the cheap-convergence path becomes
an `ObservationOutcome(direction='pull'|'create')` completed by the owner.

**Intended diffs (only these golden rows may change):**
- **Δ10a (L6, 3.2).** Cheap convergence: snapshot first (reusing previous
  images and measurements), then `synced`; a snapshot failure leaves `dirty`
  with `snapshot_failed` (today swallowed, row `synced`).
- **Δ10b (1.5, 3.2).** Rows needing no work are not re-stamped; a `blocked`
  row is no longer cleared to `synced` by cheap convergence or by the
  remote-unchanged/local-clean branch.
- **Δ10c (L4a).** A measurement import failure or skipped materialization on
  an existing row is a `required` issue in every mode (today blocks only with
  `materialize=True`).
- **Δ10d (L5).** Snapshot failure blocks `synced` on existing rows and in
  `_create_local_from_remote` (no longer swallowed; new `snapshot_failed`
  string).
- **Δ10e (L2b).** A failed download in the baseline/remote-changed branch is a
  direct `remote_media_pending` required issue (end state equal to today's
  retry-block result; mechanism changes).
- **Δ10f (U1).** Pull field conflicts and `removed_keys` follow the U1 answer.
- Unchanged and pinned: D1 — existing rows with `materialize=False` may
  complete without bytes (L3a); new rows created with `materialize=False`
  stay `dirty` (L3b, 9.1); D2 — pull-only writes `synced` locally with
  `cloud_writes_completed == 0` and empty `blocked_write_attempts` (L8);
  no-baseline pull identity conflict uses rule 6; `set_desktop_id` guard and
  child-change cursor advancement unchanged.

**Invariants at risk:** inv. 5, 10, 11, 12, 18, 20; contract rule 16.

**Acceptance:**
- Golden diffs touch only L-*, R-* rows annotated Δ10a–Δ10f; no-op golden
  unchanged.
- In `pull_all`/`_create_local_from_remote`, only the owner writes completion
  state (grep/AST evidence).
- Stage notes record whether each Gate G2 measurement is reachable.

**Verification:**
`.venv/bin/pytest -q tests/test_cloud_sync_orchestration_completion_golden.py tests/test_cloud_sync_orchestration_result_golden.py tests/test_cloud_sync_orchestration_noop_golden.py tests/test_cloud_sync_fast_path.py tests/test_cloud_download_only.py tests/test_child_change_probe.py tests/test_cloud_identity_fail_closed.py tests/test_cloud_sync_dirty_loop_steady_state.py tests/test_cloud_image_bytes_desired.py tests/test_image_tombstones.py`

## Stage 11 — Completion owner: conflict execution

**Builds on:** Stage 10. Plan:
`docs/plans/active/2026-10-09-cloud-sync-orchestration.md`.

**Kind:** behavior change.

**Outcome:** `resolve_conflict_keep_local`, `resolve_conflict_keep_cloud`,
`resolve_conflict_merge` and `resolve_conflict_plan` build
`ObservationOutcome(direction='conflict_resolution')` and complete through the
owner; `finalize_sync_candidates` clears the marker through the owner
(design 3.7).

**Intended diffs (only these golden rows may change):**
- **Δ11a (C-keep_local).** No stamp before images/measurements; a measurement
  push failure leaves `dirty` with the old snapshot (today `synced` with old
  snapshot); the raised exception type and message unchanged.
- **Δ11b (C-keep_cloud, C-merge).** A snapshot failure leaves `dirty` (today
  `synced` with old baseline); merge stamps only after images and snapshot.
- **Δ11c (D4).** Mosaic failure in keep-local is visible as `best_effort`.
- Unchanged and pinned: `resolve_conflict_plan` order snapshot → signature →
  stamp; `PartialConflictPlanError` (retry with `prior_result`, drift abort,
  no media deletion; the owner raises the existing `_partial_error` on
  `snapshot_failed`); keep-cloud image-application warnings stay best-effort
  (A4); privacy limit `blocked`; tombstone-before-push in keep-local.

**Tests changed:** `_patch_common` stamp/snapshot recorders move to the owner;
`test_keep_cloud_disables_deletion_…` call-name assertions retargeted; outcome
assertions unchanged.

**Invariants at risk:** inv. 2, 3, 12, 14, 15.

**Acceptance:**
- Golden diffs touch only C-* rows annotated Δ11a–Δ11c.
- `test_finalization_order_snapshot_before_stamp` and
  `test_snapshot_failure_leaves_conflict_unsealed` pass unchanged in intent.
- Stage notes record whether each Gate G2 measurement is reachable.

**Verification:**
`.venv/bin/pytest -q tests/test_cloud_conflict_plan_execution.py tests/test_cloud_conflict_dialog.py tests/test_cloud_visibility_phase7.py tests/test_cloud_sync_orchestration_completion_golden.py tests/test_cloud_sync_orchestration_result_golden.py tests/test_cloud_identity_fail_closed.py`

## Gate G2 — Pull and conflict completion live canary

**Kind:** manual canary that only the person performs. Agents never perform
it and never perform live Supabase writes.

**Position:** follows Stage 11 and holds Stage 12: after Stage 11 is
accepted, Stage 12 does not start until this gate records `pass`. Pull,
create, pull-only and conflict execution completion have all switched owner.

**Check `pull-conflict-completion-canary`:**
1. Build: source run at Stage 11's accepted candidate SHA (A6); record it.
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
6. D2: Download from Cloud. Expect zero mutating requests logged, no blocked
   write attempts, and completion states written locally.
7. Conflict review: edit the same field on web and desktop, Sync now. Expect
   the review marker and `dirty`. Resolve in the conflict dialog (keep local).
   Expect marker cleared, `synced`, and no further conflict on the next sync.
8. D9 tombstone: delete one cloud image on desktop ("remove cloud copy"),
   Sync now. Expect the pending tombstone count 0 afterwards and the
   observation `synced`. A tombstone failure is not safely triggerable live;
   record `failure path: automated evidence only`.
9. Restart, two no-change Sync now runs: at most one mutating request (D10)
   on the first, zero on the second.
10. Rerun both reports and diff against step 3.

**Pass criteria:** steps 4–9 as stated; no unexpected errors; C, D1, D2, E, H
zero or unchanged; F, G unchanged; A/B change only by images added in steps
4–5 or removed in step 8; status counts change only for touched
observations; no `blocked` row cleared without a local edit.

**Fail or blocked:** either keeps Stage 12 from starting. A failure is
repaired as new reviewed candidate work, and this check is then answered for
the repaired build. Nobody records `pass` over failed evidence.

## Stage 12 — Other writers: on-demand materialization and manual import

**Builds on:** Stage 11. Plan:
`docs/plans/active/2026-10-09-cloud-sync-orchestration.md` (U5).

**Kind:** behavior change, including a UI path (`ui/observations_tab.py`).

**Outcome:** design 3.8 for D3 and D7.

**Intended diffs (only these golden rows may change):**
- **Δ12a (D3, W-materialize).** `materialize_cloud_media_for_observation`
  persists the baseline only through the owner and only when downloads and
  measurement link-up are complete; otherwise the old baseline stays. It does
  not write `sync_status` (as today). Warning text unchanged.
- **Δ12b (D7, W-import).** `ui/observations_tab.py::_mark_cloud_observation_imported`
  becomes a `create` outcome: snapshot and `synced` only after image downloads
  complete; `synced_at` in UTC; the unconditional `set_image_desktop_id` /
  `set_desktop_id` writes are replaced by the guarded helpers
  (`_remote_image_desktop_id_current`, inequality guard), so value-identical
  writes are skipped; swallowed exceptions become issues surfaced through the
  existing UI message path. Positional pairing unchanged (U5).

**Non-goals:** no UI layout change; no new dialogs; no change to which
images are downloaded.

**Invariants at risk:** inv. 4, 12, 16, 18; the no-op write rule.

**Acceptance:**
- Golden diffs touch only W-* rows annotated Δ12a–Δ12b.
- A test proves the manual import performs no `desktop_id` write when the
  remote value already matches.
- Interactive verification of the import path is held by Completion gate G3.

**Verification:**
`.venv/bin/pytest -q tests/test_cloud_sync_orchestration_completion_golden.py tests/test_observations_tab_cloud_sync.py tests/test_cloud_download_only.py tests/test_child_change_probe.py <new manual-import tests>`

## Stage 13 — Single-writer guard and desktop documentation

**Builds on:** Stage 12. Plan:
`docs/plans/active/2026-10-09-cloud-sync-orchestration.md`.

**Kind:** tests and docs. No behavior change.

**Outcome:** the single-writer rule is enforced by a test and documented.

**Scope:**
- AST test: outside `observation_completion.py`, no module under
  `utils/cloud_sync_impl/` or `utils/cloud_sync.py` calls
  `update_observation_sync_state`, `_store_remote_snapshot`,
  `_store_cloud_observation_snapshot`, `_stamp_observation_synced`,
  `_set_observation_sync_state`, the marker writers, or issues raw
  `sync_status` UPDATEs; enumerated exceptions with reasons:
  `unlink_local_observation_from_cloud`, `reset_cloud_sync_state`, local-edit
  signals (design 1.2) and sync-time scans (1.3), `mark_observation_dirty` /
  `mark_observation_media_dirty`.
- `docs/cloud-sync-architecture.md`: ownership table, sections C, H and I
  rewritten to the implemented model; the 1.8 disagreement table resolved.
- `.claude/rules/cloud-sync.md`: one bullet naming the completion owner and
  that domain helpers return issues.
- `docs/cloud-sync-orchestration-design.md`: status line "implemented by …".
- `docs/supabase-sync-contract.md` is **not** edited here (see the
  sporely-web track).

**Acceptance:** all goldens unchanged from Stage 12; the guard test fails on a
synthetic violation and passes on the tree; docs cite symbols that exist.

**Verification:**
`.venv/bin/pytest -q <new single-writer test> tests/test_cloud_sync_facade_identity.py tests/test_cloud_sync_impl_import_direction.py tests/test_cloud_sync_orchestration_completion_golden.py tests/test_cloud_sync_orchestration_result_golden.py tests/test_cloud_sync_orchestration_noop_golden.py`

## Completion gate G3 — End-to-end completion live canary

**Kind:** manual canary that only the person performs. Agents never perform
it and never perform live Supabase writes.

**Position:** follows Stage 13 and holds plan completion: after Stage 13 is
accepted, the plan does not complete until this gate records `pass`. Merging
the plan's work to `main` is a separate human decision after this pass.

**Check `end-to-end-completion-canary`:**
1. Build: source run at Stage 13's accepted candidate SHA (A6); record it.
2. Profile/account, backup and baseline reports as Gate G1 steps 2–4.
3. Repeat Gate G1 steps 5–7 and Gate G2 steps 4–9 on this build.
4. Manual import (D7): import one cloud observation into a new local row from
   the Observations tab. Expect images present, the row `synced` with a
   snapshot hash, and on the next no-change sync zero `desktop_id` writes.
5. On-demand materialization (D3): open a cloud observation whose media is
   missing and download it. Expect the snapshot hash to change only if the
   download completed; with the network disabled mid-download, expect the
   hash unchanged.
6. Rerun both reports and diff against the baseline.

**Pass criteria:** every expectation of the repeated G1/G2 steps and steps
4–5 holds; no unexpected errors; C, D1, D2, E, H zero or unchanged; F, G
unchanged; no-op syncs perform zero mutating requests apart from the single
D10 RPC per process.

**Fail or blocked:** either keeps the plan from completing. A failure is
repaired as new reviewed candidate work, and this check is then answered for
the repaired build. Nobody records `pass` over failed evidence.

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
