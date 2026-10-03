# Cloud Sync Extraction and Orchestration Refactor Plan

Status: authoritative plan for decomposing and hardening `utils/cloud_sync.py`.
Rewritten 2026-10-03 against `main` @ `65f09d2`; supersedes the 2026-08-23
version (20 checkpoints), which remains in git history.

## Agent handoff

- **Status:** Active. No stage accepted on `main`.
- **Donor evidence:** `feature/cloud-sync-transport-boundary` (`4002270`, merge
  base `7acaad1`, 271 commits behind `main`) implemented old Stages 0–1 and
  passed review in September. It is a donor, never a merge source (§4).
- **Current/next stage:** Stage 1 (Boundaries), starting with slice 1.0 (preflight).
- **Known red baseline:** `tests/test_cloud_sync_progress_reset_and_prepare.py::
  test_reconcile_metadata_only_linked_images_skips_unchanged_siblings` fails on
  `main` (stub `push_image_metadata` lacks `*, remote_row=None`). Fixed on the
  donor in `ca16130`; reapply in slice 1.0.
- **Execution model:** implementation by GPT-5.5-low; independent audit by
  GPT-6.1-Sol; plan refinement, review and acceptance through agent-sparring.
  Stages are large coherent slices made of small bisectable commits.
- **Principle:** preserve contracts, not accidents. Increase stage size; do not
  reduce evidence.

## 1. What changed since the 2026-08-23 plan

| Fact on `main` today | Consequence for the plan |
|---|---|
| `cloud_sync.py` is 28,305 lines (24,854 then); 712 defs; `SporelyCloudClient` alone is 3,060 lines / 113 methods (L15867). | Extraction is still warranted; the client holds domain identity logic, not just transport. |
| Donor branch's moved symbols (errors 21, profiling 14, progress 15, summary 8, common 1, transport 15, pagination 2) are AST-identical on `main`. Only pull-only registries grew (17→26, 44→46, 11→13). `git merge-tree` shows 4 conflict hunks in `cloud_sync.py`. | Stage 0/1 work is cheap to reapply; it is not worth re-deriving. |
| Snapshot storage calls `_reconcile_accepted_asymmetry` (L12593); conflict resolution calls `_store_remote_snapshot` (L12755, L15789…). | Snapshots, accepted asymmetry, comparison and conflict-plan form **one cycle** and must move together (old 4a/4b split is invalid). |
| Measurement push needs image `cloud_id`s, metadata anchors, tombstones and the portable identity guard; measurement import calls `_apply_remote_images_to_local`. | Old order (5b measurements before 6a–6c images) contradicts the dependency graph. Images → measurements. |
| Calibrations (~1,050 lines) depend only on infrastructure + `_reconcile_local_image_calibration_links`. | Calibrations are a leaf; they move in Stage 1. |
| Early synced stamp: `push_all` L21016, compensating `mark_observation_dirty` at L21085/L21354/L21445/L21573, snapshot stored after the stamp (L21493). Pull stamps early at L26367, L26659, L26808, L26846, L26891, L27085. | Final-commit work must cover **pull** too, not only push. |
| Unhoused regions not in the old target tree: taxonomy-identity classification (L10898–11240, ~900 lines incl. `_RemoteIdentityClaim`), location-precision guard (L3792–3910), EXIF inject/backfill (L11310, L26041), original/calibration recovery cache (L1167–1877), image-too-large formatting (L2535–2790, L19174), `get_conflict_detail` (874 lines, L19593), derived summary/mosaic glue (L24217–25770, ~1,550 lines). | Every region now has a named owner (§3). |
| New invariants (owner-sync metadata parents, upload completeness vs render signature, taxonomy identity in conflict detection, verified identity clear via `_patch_with_precondition`, mosaic signature carry-forward, pending-image repair generation, reference-sharing pull-only writers). | Added to §2. |
| 1,035 `monkeypatch.setattr(<cloud_sync>, …)` calls on 130 names; top: `get_connection` 122, `get_app_settings` 44, `update_app_settings` 38, `_store_remote_snapshot` 37. 84 test files / ~1,593 tests import `cloud_sync`. | The patching rule must be decided once, in Stage 1 (§5), not rediscovered per stage. |
| `reference_cloud_adapter.py:8` imports error classes from the facade, forcing lazy imports in `sync_all` (L6877, L7214). | Import from `errors.py` once it exists; removes the cycle. |

## 2. Invariants that no stage may change

Unless a stage explicitly lists a change under "Intentional behavior change",
these hold. Each is backed by the named contract section or suite.

1. Local SQLite decides which image bytes are desired in the cloud; only ledger
   membership (`sporely_cloud_image_storage_intent_ids_<obs>`) proves initialized
   intent. Initializer performs zero cloud I/O. (`test_cloud_storage_intent_ledger`,
   `test_cloud_image_bytes_desired`)
2. Explicit checkbox/context-menu removal is the only source of routine cloud
   image deletion. Omission, filtering, prep failure, missing files and partial
   reads are never deletion intent. (`test_image_tombstones`)
3. Pending tombstone flush runs **before** dirty-observation pruning; explicit
   deletion converges in the same sync. A same-run tombstone is not a
   concurrent remote edit.
4. Verified local `cloud_id`s are primary push identity; remote `desktop_id` is
   recovery only. Disagreement, ambiguity, soft-deleted matches and 23505 races
   fail closed — never POST, never reparent. (`test_cloud_identity_fail_closed`,
   `test_image_push_identity`, `test_portable_cloud_identity_guard`)
5. Taxonomy identity participates in change/conflict detection; a no-baseline
   identity contradiction fails closed. (contract L874, L910;
   `test_cloud_sync_no_baseline_identity_contradiction`)
6. Stale cloud identity is cleared explicitly and its landing is verified
   (`_verify_identity_clear_landed`, `_patch_with_precondition`).
7. Metadata-only microscope anchors are valid rows, not broken uploads.
   Byte-excluded microscope images with public measurements still get
   metadata parents (owner-sync, contract L642). Byte selection and
   measurement/mosaic participation are independent.
   (`test_cloud_sync_metadata_only`, `test_cloud_media_measurement_mosaic_chain`)
8. Anchor promotion reuses the existing row; pending marker before reserve
   PATCH; reserve conditional on `storage_path IS NULL`; failure releases only
   the exact reserved key; `None` upload return is failure; reserved path is
   never proof of bytes. (`test_cloud_anchor_promotion`)
9. A local render signature never proves remote upload completeness; image-prep
   fast paths require canonical pending-image completeness (contract L716, L768).
   Bump `_CLOUD_PENDING_IMAGE_REPAIR_VERSION` only when the repair scan's
   selection changes.
10. Pull-only performs zero cloud writes; every public client method is
    classified read or write, including reference-sharing writers.
    (`test_cloud_download_only`)
11. Partial or bounded remote collections are never authoritative; paginated
    reads use deterministic ordering.
12. Snapshot = accepted shared baseline, never "whatever we read". Accepted
    asymmetry persists inside it. Never written after truncated reads,
    unresolved conflicts, incomplete required work or ambiguous identity.
13. Representation-only differences (`sample_source` case, naive vs UTC
    `captured_at`, `calibration_id` vs `calibration_uuid`, absent vs `None`)
    never conflict; genuine three-way divergence always does.
14. Conflict plans: reviewed-baseline drift aborts apply; partial retries are
    idempotent; media deletion is unreachable from a plan.
15. Required child failure leaves the observation retryable and visible in the
    result. (`test_sync_observation_dirty_propagation`)
16. Cloud recovery-cache bytes are never re-uploaded; local originals are never
    deleted or downgraded by cloud-side disappearance.
17. Mosaic signature survives the sync's own working-file swap
    (`_carry_forward_local_mosaic_signature`, contract L999); publication
    selection is not part of the mosaic key.
18. Fast no-op sync performs zero remote writes (`test_cloud_sync_dirty_loop_steady_state`
    L710, `test_cloud_sync_fast_path`); child-change cursor semantics unchanged
    (`test_child_change_probe`).
19. Red List follows identification (contract L979); location precision guard
    holds; cloud-only field edits are persisted locally during push.
20. `sync_all` caller modes (`sync_images`, `materialize_remote_images`,
    `full_pull`, `child_safety_pull`, `pull_only`) keep their meaning; nothing is
    "turned on" to simplify orchestration.

## 3. Target architecture

Ownership matters more than file count. `cloud_sync_impl/` modules may not
import `utils.cloud_sync` except through the documented transitional
`_facade()` hook (retired in Stage 5).

```text
utils/cloud_sync.py                     facade: public API, re-exports, legacy result dicts
utils/cloud_sync_impl/
  common.py  errors.py  profiling.py  progress.py  summary.py   leaf infrastructure
  settings_keys.py                      per-observation settings key builders
  transport.py  pagination.py           client mixins: HTTP, refresh, paging
  pull_only.py                          read/write registry, PullOnlyCloudClient
  image_policy.py                       desired bytes, intent ledger, anchor predicates
  tombstones.py                         tombstone lifecycle
  calibrations.py                       calibration payload/identity/push/pull/recovery cache
  reconciliation/                       PURE classification (no I/O, no SQLite writes)
    compare.py                          canonical observation/image/measurement payloads
    taxonomy_identity.py                identity claims, contradiction classification
    preflight.py                        ObservationPushConflictReport, push preflight
    types.py                            SyncIssue, OperationOutcome, ReconciliationPlan (Stage 4)
  baseline.py                           snapshots + accepted asymmetry (persisted)
  conflict_plan.py                      build/resolve/finalize plans, fingerprints, ops
  conflict_detail.py                    get_conflict_detail UI payload
  image_identity.py                     resolve/find/link, portable guard, identity-clear; client mixin
  anchors.py                            metadata-only anchors, owner-sync parents, promotion
  images.py                             push/pull/prep/materialize, signatures, EXIF, size limits
  measurements.py                       payloads, push, reconcile, import
  derived.py                            spore summary + mosaic glue (calls sibling owners)
  observation_coordinator.py            ONLY owner of final sync_status + snapshot commit (Stage 4)
  push_executor.py  pull_executor.py    execute classified actions (Stage 4)
  orchestration.py                      thin sync_all (Stage 4)
```

Dependency direction (arrows = "may import"):

```text
orchestration → coordinator → {push_executor, pull_executor} → domain owners
coordinator → reconciliation/*, baseline
domain owners: calibrations, image_identity → anchors → images → measurements → derived
domain owners → image_policy, tombstones, baseline, transport/pull_only, infrastructure
conflict_plan → baseline, reconciliation/*, domain owners (execution of plan ops)
reconciliation/* → infrastructure only            (never transport, never SQLite writes)
infrastructure → nothing in cloud_sync_impl
```

Sibling owners stay where they are: `cloud_media_policy`, `original_sync_policy`,
`cloud_media_recovery`, `cloud_media_audit`, `cloud_spore_mosaic[_backfill]`,
`spore_summary_sync`, `r2_storage`, and the reference-sync stack
(`reference_cloud_sync`, `reference_cloud_adapter`, `curated_reference_sync`,
`database/reference_sync_*`). External publishing is out of scope.

## 4. Using the donor branch

- Start every stage from current `main`. Never merge, rebase or cherry-pick the
  donor wholesale.
- **Reuse as-is:** the module layout and docstrings of `errors.py`,
  `profiling.py`, `progress.py`, `summary.py`, `common.py`, `pagination.py`;
  the ownership tests `tests/test_cloud_sync_stage0_ownership.py` and
  `tests/test_cloud_sync_stage1_ownership.py`; test fixes from `ca16130`
  (stub signature + 3 materialization-state regressions), the dry-run WAL fix
  `7297bf9` and the `reference_library_schema` index idempotence fix — each
  only after re-checking it still applies.
- **Regenerate from `main`, never copy:** `pull_only.py` registries (main has
  more entries); `transport.py` (add `_patch_with_precondition`, the session-
  cache clearing hook near `_cloud_timing_log`).
- **Lessons to apply up front:**
  1. *Shadowing:* the donor's first cut left facade redefinitions after the
     import, so facade and owner held different objects. Rule: delete the
     facade definition in the same commit as the move; ownership tests assert
     `facade.X is owner.X`.
  2. *Late binding:* donor mixins resolve `SUPABASE_KEY`, `CloudSyncError`,
     `_response_indicates_auth_error`, `_decode_jwt_subject` via `_facade()`.
     Keep this only for those transport names, list them, and retire it in
     Stage 5.
- Add to the classification on reapply: `fetch_image_metadata_purpose` (read,
  push-side only), `_patch_with_precondition` (write, explicit — today only
  default-deny blocks it). Move `is_identity_clear_verification_failed_error`
  (L3026) into `errors.py`.
- After Stage 1 lands, delete the donor and `review/cloud-sync-prestage-2026-09-08`
  branches, and the untracked `utils/cloud_sync_impl/__pycache__` on `main`.

## 5. Monkeypatch rule (decided once, applied in every stage)

1,035 test patches target the facade. A moved function that resolves
`get_connection` in its own module escapes them silently.

- Production owner modules import what they use directly
  (`from database.schema import get_connection`), never via the facade —
  except the Stage-1 `_facade()` transport list.
- Add `tests/cloud_sync_patching.py::patch_cloud_sync(monkeypatch, name, value)`:
  sets the attribute on the facade **and** every `utils.cloud_sync_impl.*`
  module that binds `name`, and fails if none does. Test-only; no production
  mirror state.
- Each move commit rewrites the affected tests' `monkeypatch.setattr(cloud_sync, "<name>", …)`
  to `patch_cloud_sync(...)` for the moved or newly-bound names (mechanical codemod,
  committed separately from the production move so the diff is reviewable).
- An ownership test per stage asserts identity (`facade.X is owner.X`) for every
  re-exported symbol and that no `cloud_sync_impl` module defines a name the
  facade also defines.

## 6. Mechanical-move verification (makes large stages reviewable)

Slice 1.0 adds `tools/verify_cloud_sync_move.py`: given a base SHA and a
candidate, for every function/class removed from `cloud_sync.py` it finds the
definition in `cloud_sync_impl/` and compares normalized ASTs (ignoring import
lines and module-qualified name rewrites). Output: identical / differs (with
diff) / missing. Reviewers require "all identical" for any commit labelled
`move:`; any non-identical symbol must be in a commit labelled `adapt:` with a
one-line reason. This is what lets one stage contain thousands of moved lines
without weakening review.

Commit labels used throughout: `move:` (AST-identical), `adapt:` (import/binding
only), `test:` (patch retargeting, new tests), `behavior:` (intentional change,
own tests), `docs:`.

---

# Stage 1 — Boundaries

**Objective:** land a green baseline and extract every dependency-graph leaf:
infrastructure, remote boundary, image policy, tombstones, calibrations.

**Absorbs:** old pre-stage, Stage 0, Stage 1, Stage 2, Stage 3, Stage 5a.

**Ownership after stage:** `common`, `errors`, `profiling`, `progress`,
`summary`, `settings_keys`, `transport`, `pagination`, `pull_only`,
`image_policy` (incl. `_is_metadata_only_microscope_cloud_image`,
`_is_local_metadata_only_microscope_anchor`, intent ledger L5781–6241),
`tombstones` (L6287–6560), `calibrations` (L650–1166, L7458–8225, plus the
original/calibration recovery cache L1167–1877 if its only consumers are
calibration/original paths — otherwise leave it for Stage 3 `images`).
`SporelyCloudClient` inherits the transport/pagination mixins; its domain
methods stay in the facade until Stage 3.

**Behavior:** preserving only. The one allowed non-move is the baseline test fix.

**Prerequisites:** none.

**Commit slices:**
1. 1.0 preflight — `test:` reapply `ca16130` stub fix + regressions; confirm
   green. `docs:` refresh `docs/cloud-sync-architecture.md` line references and
   record the early-stamp model as current truth. `test:` add
   `tests/cloud_sync_patching.py`, `tools/verify_cloud_sync_move.py`, and an
   inventory file `docs/cloud-sync-refactor-inventory.md` (production imports
   from `utils.cloud_sync`, tools/scripts importing private helpers, string-based
   references, patched-name counts).
2. `move:` leaf infrastructure (donor layout) + `adapt:` facade re-exports;
   `move:` `is_identity_clear_verification_failed_error`; `adapt:`
   `reference_cloud_adapter` imports errors from `errors.py`, drop the lazy
   imports in `sync_all`. `test:` stage-0 ownership test.
3. `move:` transport/pagination mixins, `_patch_with_precondition`; regenerate
   `pull_only.py` from main; classify every client method. `test:` stage-1
   ownership + classification test (future-enforcing).
4. `move:` settings keys, image policy, tombstones (one commit each).
5. `move:` calibrations (+ recovery cache if eligible).
6. `test:` patch retargeting codemods, one per production move commit.

**Must not change:** error text, summary keys, progress phase names, exception
hierarchy, headers/`Prefer` semantics, refresh/retry/timeouts, pagination order,
pull-only allow/block results, ledger key format, tombstone flush ordering,
calibration no-op matching. Invariants 1–3, 10, 11.

**Focused suites:** `test_cloud_download_only`, `test_cloud_sync_stage0_ownership`,
`test_cloud_sync_stage1_ownership`, pagination tests,
`test_cloud_storage_intent_ledger`, `test_cloud_image_bytes_desired`,
`test_cloud_sync_metadata_only`, `test_image_tombstones`, gallery checkbox
deletion tests, `test_cloud_calibration_sync`, `test_cloud_sync_change_notification`.

**Broad gate:** all 84 test files importing `cloud_sync` (record count; expect
~1,593 passing) + `py_compile` of touched modules + fresh-process import of
facade and each new module + verifier "all identical".

**GPT-6.1-Sol audit:** required before Stage 2 (focus: shadowing, pull-only
classification completeness, any `adapt:` commit).

**Live canary:** no. No remote-state semantics change.

**Rollback/bisect:** each `move:` commit is independently revertible; the stage
merges as one merge commit.

**Acceptance:** green broad gate; verifier clean; no facade redefinition of any
moved name; every client method classified; `cloud_sync.py` shrinks by roughly
4–5k lines; architecture doc points to new owners.

---

# Stage 2 — Reconciliation substrate

**Objective:** move the snapshot / comparison / conflict cycle as one unit, and
separate its pure classification from persistence.

**Absorbs:** old 4a, 4b, and the unhoused taxonomy-identity and conflict-detail
regions. Does **not** absorb the 6.5 typed design (that is Stage 4 — types are
only consumed there; designing them now would be speculative).

**Ownership after stage:**
- `reconciliation/compare.py`: `_observation_compare_payload` (9 consumers),
  image compare (`_image_compare_key` L4061 … `_analyze_image_changes` L4441),
  measurement compare (L9073–9373), location-precision guard (L3792–3910).
- `reconciliation/taxonomy_identity.py`: L10898–11240 incl. `_RemoteIdentityClaim`.
- `reconciliation/preflight.py`: `ObservationPushConflictReport`,
  `_analyze_observation_push_conflicts` (L4520–).
- `baseline.py`: snapshot parse/build/load/store (L3555, L5585, L5716, L12483),
  `_reconcile_accepted_asymmetry` (L14676).
- `conflict_plan.py`: L12319–15811 (build/resolve/finalize, fingerprints,
  `_build_plan_operations`, `_verify_completed_ops_and_rebase`,
  `PartialConflictPlanError`, `_mosaic_render_state_unverified`).
- `conflict_detail.py`: `get_conflict_detail` (L19593).

**Behavior:** preserving only. Where a compare helper reads SQLite, split it into
a pure classifier plus a loader in an `adapt:` commit with an equivalence test;
do not change results.

**Prerequisites:** Stage 1 accepted.

**Commit slices:** (1) `move:` compare + location precision; (2) `move:`
taxonomy identity; (3) `move:` preflight; (4) `move:` baseline + accepted
asymmetry together with conflict_plan (single commit if the verifier cannot
split the cycle cleanly); (5) `move:` conflict_detail; (6) `adapt:` pure/loader
splits; (7) `test:` purity test — `reconciliation/*` imports no transport,
`database` writer, or `PySide6`.

**Must not change:** invariants 5, 12, 13, 14; snapshot schema/versions;
`_CONFLICT_PLAN_BASELINE_SCHEMA_VERSION`; fingerprint output.

**Focused suites:** snapshot persistence, `test_cloud_sync_conflict_preflight`,
`test_image_conflict_normalization`, `test_cloud_conflict_plan_execution`,
`test_cloud_conflict_dialog`, baseline-drift and partial-retry tests,
no-media-deletion-plan tests, `test_cloud_sync_no_baseline_identity_contradiction`,
location-precision tests.

**Broad gate:** as Stage 1.

**GPT-6.1-Sol audit:** required (focus: cycle moved intact, purity boundary,
no snapshot-ordering change).

**Live canary:** no.

**Rollback/bisect:** per commit; the cycle commit is the only large one.

**Acceptance:** green gates; verifier clean apart from listed `adapt:`s;
`reconciliation/*` purity test passes; `cloud_sync.py` loses roughly 6–7k lines.

---

# Stage 3 — Domain executors

**Objective:** give images, anchors, measurements and derived products owners,
in dependency order.

**Absorbs:** old 5b, 6a, 6b, 6c, plus unhoused EXIF, image-size formatting,
derived summary/mosaic glue.

**Review shape:** one stage, **two review candidates** because of size:
- **3A** — `image_identity.py` (incl. the client's `_resolve_existing_*_for_push`,
  `push_observation` identity legs, identity-clear trio, portable guard, as an
  identity mixin) and `anchors.py` (metadata-only anchors, owner-sync parents
  L9490/L23488–23580, promotion/rollback, `_cancel_microscope_anchor_tombstones`).
- **3B** — `images.py` (`_push_images_for_observation` core, prep, upload, metadata
  apply, materialization, media signatures incl. `_carry_forward_local_mosaic_signature`,
  EXIF inject/backfill, size-limit formatting, recovery cache if not in Stage 1),
  then `measurements.py` (L9982–10050, L23759, L23977, L27355), then `derived.py`
  (L24217–25770).

3B starts after 3A is accepted; both use the same plan section and gate.

**Behavior:** preserving only. Deep `mark_observation_dirty` calls move
unchanged; Stage 4 removes them.

**Prerequisites:** Stage 2 accepted.

**Must not change:** invariants 4, 6, 7, 8, 9, 15, 16, 17; image push ordering
(intent init → identity/link → tombstone/protection filter → prep → metadata
reserve/create → byte upload → metadata finalize → local `cloud_id`
bookkeeping); `prepared_items` never becomes desired-state truth; per-image
failure keeps the observation retryable; required measurement failure stays
visible and retryable.

**Focused suites:** `test_image_push_identity`, `test_cloud_identity_fail_closed`,
`test_portable_cloud_identity_guard`, `test_cloud_anchor_promotion`,
`test_cloud_sync_metadata_only`, `test_cloud_sync_image_upload_policy`,
dirty-pending-image, media-pull retry, original sync/recovery,
`test_cloud_measurement_sync_v1`, `test_sync_observation_dirty_propagation`,
`test_cloud_spore_mosaic_signature`, `test_cloud_spore_mosaic_unchanged_sync`,
`test_cloud_media_measurement_mosaic_chain`, spore-summary tests,
`test_cloud_sync_fast_path`, `test_cloud_sync_dirty_loop_steady_state`.

**Broad gate:** as Stage 1.

**GPT-6.1-Sol audit:** required on 3A and on 3B.

**Live canary:** no, unless an `adapt:` commit touches an identity or anchor
write path — then one canary per §9.

**Rollback/bisect:** per commit; 3A and 3B are separate merges.

**Acceptance:** green gates; verifier clean; `SporelyCloudClient` keeps only
transport mixin wiring, observation/calibration/reference RPCs and capability
probes; `push_all`/`pull_all`/`sync_all` are the main remaining bulk in the facade.

---

# Stage 4 — Orchestration replacement

**Objective:** replace the implicit state machine with typed reconciliation,
one observation coordinator, and a final `synced` commit on both push and pull.
This is the project's main risk checkpoint.

**Absorbs:** old 6.5a–j, 7a–7e, 8a (duplicate reconciliation semantics).
6.5k is **removed** (§ "What we removed").

**Commit slices:**
1. `docs:` **design note** in this plan (append §4-design): state machine,
   `SyncIssue` / `OperationOutcome` / `ObservationSyncOutcome` /
   `ReconciliationPlan` definitions, required-vs-best-effort table (audited from
   tests and contract, not guessed), snapshot-commit owner, the list of tests
   that encode accidental behavior (start from: `test_cloud_conflict_dialog.py:797,826`
   `("stamp",)` call sequences; `test_cloud_conflict_plan_execution.py:120,416,2950`
   patching `_stamp_observation_synced`; `test_cloud_sync_conflict_preflight.py`
   L747–1248 dirty+marker asserts; the 39 files with exact call-count asserts —
   classify each). **Sol reviews the design commit before slice 2.** This is an
   internal checkpoint, not a separate stage or human approval cycle.
2. `test:` new final-commit and pure-reconciliation tests (red).
3. `behavior:` `reconciliation/types.py`; push and pull classification routed
   through `reconciliation/*` (removes duplicated rules; `_mosaic_render_state_unverified`
   becomes a plan query).
4. `behavior:` `observation_coordinator.py` owns snapshot store + `sync_status`;
   remove the early stamp at L21016 and compensations L21085/L21354/L21445/L21573;
   pull-side stamps L26367–L27085 routed to the coordinator.
5. `behavior:` `push_executor.py`, `pull_executor.py` return outcomes; deep
   `mark_observation_dirty` calls removed or documented as exceptions.
6. `behavior:` structured issue pipeline; `summarize_sync_issues()` consumes
   issues; legacy `result["errors"]` strings generated from them.
7. `move:`/`adapt:` thin `orchestration.sync_all`; facade delegates.
8. `test:` reviewed updates to accidental-behavior tests (each change cites the
   design note).

**Intentional behavior changes (exhaustive):** `synced` written once, after
required work and snapshot persistence; snapshot failure prevents `synced`;
issue categorization from types, not strings; push and pull share one
classifier. Everything in §2 still holds.

**Prerequisites:** Stage 3 (3A and 3B) accepted.

**Focused suites:** new final-commit and reconciliation tests;
`test_sync_observation_dirty_propagation`; snapshot, preflight and conflict-plan
suites; identity suites; measurement/calibration; retry propagation;
download-only; metadata-only; child-change probe; fast path; dirty-loop;
UI summary, privacy-blocked, plan-limit and conflict-count tests; caller-mode,
startup/refresh and sync-now tests.

**Broad gate:** as Stage 1, plus a before/after diff of
`sync_all` result dicts over the fixture matrix (keys and legacy strings
identical except the documented changes).

**GPT-6.1-Sol audit:** required twice — the design note (slice 1) and the full
candidate.

**Live canary:** **yes**, on the full candidate (§9). Optionally a second canary
after slice 4 if Sol judges the coordinator change risky on its own.

**Rollback/bisect:** slices 3–7 each leave tests green; the stage merges as one
merge commit, revertible as a unit.

**Acceptance:** no `sync_status='synced'` write outside the coordinator (grep
test); no early stamp in push or pull; push and pull import the same classifier;
legacy results compatible; canary clean (C=D1=D2=E=H=0, no new anomalies).

---

# Stage 5 — Hardening and facade

**Objective:** remove transitional architecture and avoidable side effects now
that ownership is final.

**Absorbs:** old 8b–8e, 9, 10.

**Commit slices (each its own commit; `behavior:` where results can change):**
1. No-op remote write audit and suppression (protect `updated_at` cursors,
   reverse-link healing, image metadata, measurement upserts, calibrations).
2. Deterministic per-observation diagnostics (candidate reason → plan →
   execution → final state) in the result, not prints.
3. Hidden-state audit: remaining `mark_observation_dirty`,
   `mark_observation_media_dirty`, `update_observation_sync_state`, snapshot
   clear, review-pending markers — each removed or documented.
4. Retire `_facade()` late binding; retire facade-only patch targets
   (`patch_cloud_sync` keeps working via owner modules).
5. Anchor reservation risks (cross-device adoption, dangling reservation):
   decide fix or documented acceptance; fix only as `behavior:` with tests.
6. Client split: move reference RPCs + capability probes into mixins; stop if
   the remaining client is understandable.
7. Facade review: keep `utils/cloud_sync.py` as re-exports + legacy adapters.
8. `docs:` architecture doc and contract navigation final refresh.

**Prerequisites:** Stage 4 accepted and canaried.

**Focused suites:** per slice; slice 1 adds zero-write assertions for each
suppressed path.

**Broad gate:** as Stage 4.

**GPT-6.1-Sol audit:** required on the full candidate.

**Live canary:** yes if slice 1 or 5 lands; otherwise no.

**Rollback/bisect:** per commit.

**Acceptance:** Definition of done (below).

---

## 7. What we removed from the old plan and why

- **20 checkpoints → 5 stages (6 review candidates).** The old boundaries were
  safety boundaries; the verifier (§6), commit labels and per-stage Sol audit
  keep that safety without a review cycle per module.
- **Separate pre-stage.** Now slice 1.0; it has no independent review value.
- **Stage 2/3 split (policy vs tombstones).** Coupling is one-directional and
  small; both are leaves.
- **Stage 4a/4b split.** Snapshots and conflict plans are mutually recursive.
- **Stage 5a as its own stage.** Calibrations are a leaf; moved in Stage 1.
- **Measurements before images.** Reversed to match the dependency graph.
- **6a/6b/6c as three stages.** One stage with two review candidates.
- **Stage 6.5 as a standalone checkpoint.** Became Stage 4's first slice with
  its own Sol review; the types are designed next to their only consumers.
- **7a–7e as five sequential architectural states.** Intermediate states (new
  coordinator, old executors) were transitional architecture that would
  have lived across several review cycles; now one stage, internally sliced.
- **6.5k (reference-use pending-change signal).** A product feature crossing
  `database/reference_library*` triggers and outboxes, not refactoring. Write
  it as its own plan after Stage 4, on top of the coordinator.
- **Stage 9 as a gated "optional stage".** Now Stage 5 slice 6; it is cheap
  once mixins exist and callers never construct sub-clients.
- **Line references and suite counts from August.** Replaced; treat any line
  number here as a search hint valid at `65f09d2`.

## 8. Things that still deserve small commits even though they no longer deserve separate stages

- The red-baseline test fix (slice 1.0), alone.
- Each `move:` of one module, separate from its facade re-export `adapt:` and
  from its `test:` patch retargeting.
- The `reference_cloud_adapter` import switch (removes a cycle; easy to bisect).
- Every pull-only registry change, so classification diffs are readable.
- The snapshot/conflict-plan cycle move (Stage 2 slice 4), isolated.
- Any pure/loader split in reconciliation.
- 3A identity mixin moving off `SporelyCloudClient`.
- `_carry_forward_local_mosaic_signature` and the pending-image repair
  generation move — small, incident-prone.
- Stage 4: the design note; removal of the early push stamp; routing of pull
  stamps; each executor; the issue pipeline; each accidental-test update.
- Stage 5: each no-op suppression path; `_facade()` retirement; client split.

## 9. Live-canary policy

Canary only where remote-state semantics can change: Stage 4 (required),
Stage 5 slices 1/5, any Stage 3 `adapt:` on an identity or anchor write path.
Before: fresh SQLite backup; read-only reconciliation report with
C=D1=D2=E=H=0 (or a documented exception). Use a deliberate Sync Now on a
disposable or known account; record account/DB/build; rerun the report after
and diff it. Never combine with cleanup or GC. Live Supabase writes are
human-gated per `AGENTS.md`.

## 10. Execution estimate (agent slices / review cycles)

| Stage | Implementation slices | Sol audits | Sparring cycles (expected) | Canary |
|---|---|---|---|---|
| 1 Boundaries | 3–4 | 1 | 1–2 | — |
| 2 Reconciliation substrate | 2–3 | 1 | 1–2 | — |
| 3A Identity + anchors | 2 | 1 | 1–2 | conditional |
| 3B Images, measurements, derived | 3–4 | 1 | 1–2 | — |
| 4 Orchestration | 5–7 | 2 | 2–3 | 1 (maybe 2) |
| 5 Hardening + facade | 3–5 | 1 | 1–2 | conditional |
| **Total** | **18–25** | **7** | **7–13** | **1–3** |

Stages 1–3 are mechanical and bounded by the verifier; most fix cycles are
expected in Stage 4. Branch drift is the main schedule risk: keep each stage's
branch short-lived and merge on acceptance; do not run another
`cloud_sync.py` feature branch in parallel with a `move:` stage.

## 11. Out of scope

E3 R2 garbage collection; `G_conflicting_intent` repair; historical duplicate
cleanup; migration-tool hardening; cloud schema changes not required by the
Stage 4 design; spore orientation; UI redesign; account-link/reset; broad
lint/type migration; external publishing; 6.5k pending-change signalling.

## 12. Definition of done

- **Compatibility:** `utils/cloud_sync.py` is re-exports plus thin legacy
  adapters; production imports unchanged or migrated; legacy result dicts
  supported; no mirror mutable state; no `_facade()` late binding.
- **Ownership:** every region in §3 has exactly one owner; ownership tests
  assert facade identity; verifier history shows all moves AST-identical or
  justified.
- **Reconciliation:** `reconciliation/*` is pure (enforced by test); push and
  pull share it; candidate selection remains a separate optimizer; derived
  products query the plan instead of re-deriving agreement.
- **State:** only the coordinator writes `sync_status='synced'` (enforced by
  grep test), after required work and snapshot persistence; required failures
  stay retryable and visible; unresolved conflicts never advance the baseline.
- **Diagnostics:** issues are typed; each failed observation is explainable as
  candidate reason → plan → execution → final state.
- **Safety:** all §2 invariants pass their suites; pull-only zero writes; fast
  no-op sync zero writes.
- **Validation:** all `cloud_sync`-importing tests green; final canary clean;
  `docs/cloud-sync-architecture.md` and `docs/supabase-sync-contract.md`
  (both repo copies, per contract rules) point to the new owners.

## 13. Anti-goals

A split that keeps the implicit state machine; competing conflict definitions
in push and pull; a new multi-thousand-line `reconciliation` or `coordinator`
module; domain helpers that still decide observation completion; `synced`
meaning "started and hoping"; removing the facade to reduce file count;
pulling external publishing into cloud sync.

> **Target:** a boring facade over focused owners, pure reconciliation, explicit
> executors, and one coordinator that commits `synced` only when the whole
> required sync transaction is actually complete.
