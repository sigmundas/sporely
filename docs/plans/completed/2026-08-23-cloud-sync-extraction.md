# Cloud Sync Extraction and Orchestration Refactor Plan

Status: authoritative plan for decomposing and hardening `utils/cloud_sync.py`.
Rewritten 2026-10-03 against `main` @ `65f09d2` (plan `5a41cd1`); refined for
execution the same day (dependency corrections in §1.1). The 2026-08-23
version (20 checkpoints) remains in git history.

## Agent handoff

- **Status:** Active, ready for execution. No stage accepted on `main`.
- **Donor evidence:** `feature/cloud-sync-transport-boundary` (`4002270`, merge
  base `7acaad1`, 271 commits behind `main`) implemented old Stages 0–1 and
  passed review in September. It is a donor, never a merge source (§4).
- **Current/next slice:** 1.0 (baseline repair + inventory), then 1.1 (tooling).
- **Known red baseline:** `tests/test_cloud_sync_progress_reset_and_prepare.py::
  test_reconcile_metadata_only_linked_images_skips_unchanged_siblings` fails on
  `main` (stub `push_image_metadata` lacks `*, remote_row=None`). Fixed on the
  donor in `ca16130`.
- **Execution model:** implementation by GPT-5.5-low; independent audit by
  GPT-6.1-Sol; refinement, review and acceptance through agent-sparring.
  Stages are coherent slices made of small bisectable commits.
- **Principle:** preserve contracts, not accidents. Increase stage size; do not
  reduce evidence.

## 1. What changed since the 2026-08-23 plan

| Fact on `main` today | Consequence for the plan |
|---|---|
| `cloud_sync.py` is 28,305 lines (24,854 then); 712 defs; `SporelyCloudClient` alone is 3,060 lines / 113 methods (L15867). | Extraction is still warranted; the client holds domain identity logic, not just transport. |
| Donor branch's moved symbols (errors 21, profiling 14, progress 15, summary 8, common 1, transport 15, pagination 2) are AST-identical on `main`. Only pull-only registries grew (17→26, 44→46, 11→13). `git merge-tree` shows 4 conflict hunks in `cloud_sync.py`. | Stage 0/1 work is cheap to reapply; it is not worth re-deriving. |
| Snapshot storage calls `_reconcile_accepted_asymmetry` (L12593); conflict resolution calls `_store_remote_snapshot` (L12755, L15789…). | The current code is cyclic; the target modules must not be (§1.1). |
| Measurement push needs image `cloud_id`s, metadata anchors, tombstones and the portable identity guard; measurement import calls `_apply_remote_images_to_local`. | Images → measurements (old plan had the reverse). |
| Early synced stamp: `push_all` L21016, compensating `mark_observation_dirty` at L21085/L21354/L21445/L21573, snapshot stored after the stamp (L21493). Pull stamps early at L26367, L26659, L26808, L26846, L26891, L27085. | Final-commit work covers **pull** too. |
| Unhoused regions not in the old target tree: taxonomy-identity classification (L10898–11240, incl. `_RemoteIdentityClaim`), location-precision guard (L3792–3910), EXIF inject/backfill (L11310, L26041), original/calibration recovery cache (L1167–1877), image-too-large formatting (L2535–2790, L19174), `get_conflict_detail` (874 lines, L19593), derived summary/mosaic glue (L24217–25770). | Every region has a named owner (§3). |
| New invariants since August (owner-sync parents, upload completeness, taxonomy identity in conflicts, verified identity clear, mosaic signature carry-forward, pending-image repair generation, reference-sharing pull-only writers). | §2. |
| 1,035 `monkeypatch.setattr(<cloud_sync>, …)` on 130 names; top: `get_connection` 122, `get_app_settings` 44, `update_app_settings` 38, `_store_remote_snapshot` 37. 84 test files / ~1,593 tests import `cloud_sync`. | Patching rule fixed once (§5). |
| `reference_cloud_adapter.py:8` imports error classes from the facade, forcing lazy imports in `sync_all` (L6877, L7214). | Import from `errors.py`; removes the cycle. |
| No commits to `main` between `65f09d2` and `5a41cd1`; no `cloud_sync.py` change since. | Line numbers valid at `5a41cd1`; treat as search hints after. |

### 1.1 Dependency corrections from the execution audit (2026-10-03)

1. **Calibrations are not a Stage 1 leaf.** `pull_calibrations` reads snapshots
   via `_parse_cloud_observation_snapshot` (L7738 → L3555), and calibration
   payloads use the sample-source helpers (L7519–7605:
   `_desktop_to_cloud_sample_source`, `_cloud_to_desktop_sample_source`,
   `_split_legacy_sample_type_into_source`,
   `_apply_image_sample_fields_to_push_payload`). Calibrations move as the last
   Stage 2 slice, after `baseline` and `sample_source` exist.
2. **Tombstone flush needs the owner-sync capability probe.**
   `_push_pending_image_tombstones` (L6471) calls `_owner_sync_parents_supported`
   (L9524). The probe and `_remote_metadata_purpose` (L9540) plus the
   `METADATA_PURPOSE_*` constants move in Stage 1 to `capabilities.py`.
3. **The tombstone "region" L6287–6560 is mixed.** Tombstone owner:
   `_local_tombstoned_cloud_image_ids`, `_local_tombstoned_local_image_ids`,
   `_record_remote_image_tombstones`, `_tombstoned_cloud_image_warning`,
   `_push_pending_image_tombstones`. **Not** tombstones:
   `_reconcile_local_image_cloud_id` (L6242 → 3A identity),
   `_pull_remote_images_for_sync` (L6297) and `_pull_remote_measurements_for_images`
   (L6570) (→ Stage 1 `remote_reads.py`), `_remote_images_missing_locally`
   (L6429 → 3B images), `_load/_store_local_cloud_media_signature` (L6559 →
   Stage 1 `sync_state.py`).
4. **The snapshot cycle breaks without an adapter.** `_reconcile_accepted_asymmetry`
   (L14676) and its helpers `_accepted_asymmetry_key` (L13671),
   `_identities_referenced_by_plan` (L14652), `_merge_accepted_asymmetry`
   (L14815) and the `_still_*` predicates are pure dict functions. Move them to
   `reconciliation/accepted_asymmetry.py`; then
   `baseline → reconciliation`, `conflict_plan → baseline, reconciliation`,
   no back-edge. This is `move:`, not `adapt:`.
5. **`_store_remote_snapshot` is not pure persistence.** When not given rows it
   reads the remote observation, images and measurements through the client
   (`client.get_observation`, `client.pull_image_metadata`,
   `_pull_remote_measurements_for_images`). `baseline.py` therefore depends on
   `remote_reads.py` (Stage 1), not on `measurements.py`. Do not "purify" it in
   Stage 2.
6. **Anchors are callable before images move.** Anchor-ensure functions
   (L23063–23640) use policy/state helpers, identity, transport and
   `_metadata_only_microscope_image_payload` (L22953); none call image upload.
   Promotion helpers `_reserve_anchor_promotion_key` / `_rollback_anchor_promotion`
   (L22094/L22135) are called **from** image push (L22663–22730). Direction is
   images → anchors, so 3A-then-3B holds.
7. **Per-image local state helpers (L5325–5800) are a shared leaf**: settings
   keys, pending-promotion keys, file-signature store
   (`_clear_cloud_image_file_signature` L5754), `_set_cloud_image_metadata_only_state`
   (L5792), `_cloud_metadata_only_image_ids` (L5336). They move in Stage 1 to
   `sync_state.py`, or anchors would import upward from the facade.
8. **Pending-image repair** (`_mark_cloud_observations_dirty_for_pending_local_images`
   L9846, `_CLOUD_PENDING_IMAGE_REPAIR_VERSION` L215) is candidate selection.
   It moves with images in 3B, constant and scan in one commit.

## 2. Invariants that no stage may change

Unless a stage lists a change under "Intentional behavior change", these hold.

1. Local SQLite decides which image bytes are desired; only ledger membership
   (`sporely_cloud_image_storage_intent_ids_<obs>`) proves initialized intent;
   the initializer performs zero cloud I/O. (`test_cloud_storage_intent_ledger`,
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
   contradiction fails closed. (contract L874, L910;
   `test_cloud_sync_no_baseline_identity_contradiction`)
6. Stale cloud identity is cleared explicitly and its landing verified
   (`_verify_identity_clear_landed`, `_patch_with_precondition`).
7. Metadata-only microscope anchors are valid rows. Byte-excluded microscope
   images with public measurements still get metadata parents (owner-sync,
   contract L642), only when `_owner_sync_parents_supported` confirms (fails
   closed). Byte selection and measurement/mosaic participation are independent.
   (`test_cloud_sync_metadata_only`, `test_cloud_media_measurement_mosaic_chain`)
8. Anchor promotion reuses the existing row; pending marker before reserve
   PATCH; reserve conditional on `storage_path IS NULL`; failure releases only
   the exact reserved key; `None` upload return is failure; reserved path is
   never proof of bytes. (`test_cloud_anchor_promotion`)
9. A local render signature never proves remote upload completeness (contract
   L716, L768). Bump `_CLOUD_PENDING_IMAGE_REPAIR_VERSION` only when the repair
   scan's selection changes.
10. Pull-only performs zero cloud writes; every public client method is
    classified read or write, including reference-sharing writers.
    (`test_cloud_download_only`)
11. Partial or bounded remote collections are never authoritative; paginated
    reads use deterministic ordering.
12. Snapshot = accepted shared baseline. Accepted asymmetry persists inside it.
    Never written after truncated reads, unresolved conflicts, incomplete
    required work or ambiguous identity.
13. Representation-only differences (`sample_source` case, naive vs UTC
    `captured_at`, `calibration_id` vs `calibration_uuid`, absent vs `None`)
    never conflict; genuine three-way divergence always does.
14. Conflict plans: reviewed-baseline drift aborts apply; partial retries are
    idempotent; media deletion is unreachable from a plan.
15. Required child failure leaves the observation retryable and visible.
    (`test_sync_observation_dirty_propagation`)
16. Cloud recovery-cache bytes are never re-uploaded; local originals are never
    deleted or downgraded by cloud-side disappearance.
17. Mosaic signature survives the sync's own working-file swap
    (`_carry_forward_local_mosaic_signature` L24828, contract L999);
    publication selection is not part of the mosaic key.
18. Fast no-op sync performs zero remote writes
    (`test_cloud_sync_dirty_loop_steady_state` L710, `test_cloud_sync_fast_path`);
    child-change cursor semantics unchanged (`test_child_change_probe`).
19. Red List follows identification (contract L979); location precision guard
    holds; cloud-only field edits are persisted locally during push.
20. `sync_all` caller modes (`sync_images`, `materialize_remote_images`,
    `full_pull`, `child_safety_pull`, `pull_only`) keep their meaning.

## 3. Target architecture

`cloud_sync_impl/` modules never import `utils.cloud_sync`, except the
transitional `_facade()` hook in `transport.py`/`pagination.py` for exactly
`SUPABASE_KEY`, `CloudSyncError`, `_response_indicates_auth_error`,
`_decode_jwt_subject` (retired in Stage 5). A module may import only from
modules at its own layer or below.

```text
utils/cloud_sync.py                     facade: public API, re-exports, legacy result dicts

layer 0  common, errors, profiling, progress, summary
layer 1  transport, pagination (client mixins); pull_only; remote_reads;
         capabilities; sync_state (settings keys, per-image local state,
         media/file signature stores)
layer 2  image_policy (desired bytes, ledger, anchor predicates); tombstones
layer 3  reconciliation/ (PURE: no client, no SQLite writes, no Qt)
           sample_source, compare (+ location precision), taxonomy_identity,
           preflight, accepted_asymmetry, types (Stage 4)
layer 4  baseline (snapshot codec + load/store; reads remote via remote_reads)
layer 5  domain owners, in dependency order:
           calibrations
           image_identity (+ client identity mixin) → anchors → images → measurements → derived
layer 6  conflict_plan, conflict_detail
layer 7  observation_coordinator (ONLY writer of sync_status='synced' and snapshots, Stage 4)
         push_executor, pull_executor (Stage 4)
layer 8  orchestration (thin sync_all, Stage 4)
```

Sibling owners stay outside: `cloud_media_policy`, `original_sync_policy`,
`cloud_media_recovery`, `cloud_media_audit`, `cloud_spore_mosaic[_backfill]`,
`spore_summary_sync`, `r2_storage`, the reference-sync stack
(`reference_cloud_sync`, `reference_cloud_adapter`, `curated_reference_sync`,
`database/reference_sync_*`). External publishing is out of scope.

A layering test (added 1.1, extended each stage) parses imports of every
`cloud_sync_impl` module and fails on upward or facade imports.

## 4. Using the donor branch

- Start every stage from current `main`. Never merge, rebase or cherry-pick the
  donor wholesale.
- **Reuse:** module layout of `errors`, `profiling`, `progress`, `summary`,
  `common`, `pagination`; ownership tests `tests/test_cloud_sync_stage0_ownership.py`,
  `tests/test_cloud_sync_stage1_ownership.py`; test fixes from `ca16130`
  (stub signature + 3 materialization-state regressions), the dry-run WAL fix
  `7297bf9`, the `reference_library_schema` index idempotence fix — each only
  after confirming it still applies on `main`.
- **Regenerate from `main`, never copy:** `pull_only.py` registries;
  `transport.py` (add `_patch_with_precondition` and the session-cache hook
  near `_cloud_timing_log`, L210).
- **Lessons applied up front:** (1) *shadowing* — delete the facade definition
  in the same commit as the move; ownership tests assert `facade.X is owner.X`;
  (2) *late binding* — only the four `_facade()` names above.
- Classify on reapply: `fetch_image_metadata_purpose` (read),
  `_patch_with_precondition` (write, explicit). Move
  `is_identity_clear_verification_failed_error` (L3026) to `errors.py`.
- After Stage 1 merges: delete `feature/cloud-sync-transport-boundary`,
  `review/cloud-sync-prestage-2026-09-08`, and the untracked
  `utils/cloud_sync_impl/__pycache__` on `main`.

## 5. Monkeypatch rule

A moved function resolves globals in its new module, so a facade patch can
silently miss it; a re-exported name used by facade code is still resolved on
the facade, so an owner-only patch can miss that. During transition both
bindings exist.

**`tests/cloud_sync_patching.py::patch_cloud_sync(monkeypatch, name, value)`**
(transitional, added 1.1):
- Reads `original = getattr(utils.cloud_sync, name)`.
- Patches the facade and every `utils.cloud_sync_impl.*` module whose binding
  `is original` — the *same object*. A module binding a different object under
  the same name is never patched; this prevents patching unrelated symbols.
- Fails if the facade lacks `name`, or if no binding exists anywhere.
- Records each call; a session-end report lists names per test file.

**When to patch the true owner directly instead (required):**
- the test's subject *is* the moved owner (e.g. tombstone tests patch
  `cloud_sync_impl.tombstones`, not the facade);
- the patched name is defined (not imported) in an owner module and the test
  only exercises that owner;
- any new test written after this plan.

**Helper is acceptable only for** cross-cutting imported dependencies
(`get_connection`, `get_app_settings`, `update_app_settings`, `generate_all_sizes`,
…) and for orchestration-level tests whose call path still crosses the facade.

**Guardrails:** the helper may not be used inside production code; an
ownership test fails if a production module defines a name solely so the helper
can find it; Stage 5 removes facade-level patching for every name whose callers
are all in owner modules, and the DoD requires the helper to be gone or limited
to a documented list of imported dependencies.

Retargeting is a `test:` commit after each `move:` commit: `monkeypatch.setattr(cloud_sync, "<name>", v)`
→ owner patch if the rule above requires it, else `patch_cloud_sync(...)`.
String targets (`mock.patch("utils.cloud_sync.<name>")`) are inventoried in
1.0 and treated the same way.

## 6. Mechanical-move verifier

`tools/verify_cloud_sync_move.py --base <sha> --candidate <sha>` (added 1.1).
For every top-level function, class, method and module-level statement removed
from `utils/cloud_sync.py` (or from `SporelyCloudClient`) it locates the new
definition and reports `identical`, `differs` (with a diff), or `missing`.
Every symbol in a `move:` commit must be `identical`. Anything else must be in
an `adapt:` or `behavior:` commit.

**Compared exactly (no normalization):** the full AST of the definition,
including decorators, default values, annotations, docstrings, keyword
arguments, `global`/`nonlocal` statements, and nested functions/classes.
Comments and whitespace are not in the AST and are therefore ignored.

**Additionally verified (a mismatch makes the symbol `differs`):**
1. **Free-name resolution.** For each global name the definition reads, the
   origin in the candidate owner module (defining module + qualname for
   functions/classes; module object for modules; `repr` for literals) must equal
   the origin at base in the facade. Same spelling resolving to a different
   object is a semantic change.
2. **Module-level state.** A moved assignment, registry, cache dict,
   `ContextVar`, lock or `logging.getLogger(__name__)` must exist exactly once
   across facade + owners. Two instances = `differs`. A logger whose name
   changes because `__name__` changed is reported; it may land only in `adapt:`.
3. **Rebinding hazards.** Any moved function using `global`, `globals()`,
   `sys.modules[__name__]`, `getattr(<module>, …)`, `importlib`, or string-based
   patch targets is reported `adapt-required` even when its AST is identical.
4. **Client methods.** For methods moved to a mixin, the set of attribute names
   on `SporelyCloudClient` and their resolved function origins (via the MRO)
   must be unchanged.
5. **Facade binding.** After the move the facade binding `is` the owner object
   (checked by importing the candidate in a subprocess).

**Never ignored:** renamed identifiers (local or global), reordered statements,
changed literals, changed imports *inside* function bodies, changed exception
types, added/removed `try`, changed default arguments. The verifier errs toward
`differs`; a false `differs` costs a short `adapt:` review, a false `identical`
is not acceptable.

Commit labels: `move:` (all symbols `identical`), `adapt:` (import/binding/
logger-name/signature plumbing only, each listed with a one-line reason),
`test:` (retargeting, new tests), `behavior:` (intentional change with its own
tests), `docs:`.

## 7. Slice protocol (every implementation slice)

The implementer (GPT-5.5-low) receives one slice at a time and:

1. Confirms clean `git status`, records base SHA, reads `AGENTS.md`,
   `.claude/rules/cloud-sync.md`, this slice and §2.
2. Runs the slice's **pre** tests; stops and reports if not green.
3. Makes the commits in the listed order, one label per commit.
4. After each `move:` commit runs the verifier and the slice's focused tests.
5. Runs the slice's **post** tests, the layering test, ownership tests,
   `py_compile` of touched files, and a subprocess import of the facade.
6. Does not touch anything listed under "Untouched".
7. Pushes; hands the reviewer: base/candidate SHAs, `git log --oneline`,
   verifier output, test commands + pass counts, list of `adapt:` reasons,
   patch-retargeting counts (owner vs helper).

**Stage-wide broad gate** (run once per review candidate, not per slice): all
test files importing `cloud_sync` (record count; ~1,593 at `5a41cd1`),
layering test, ownership tests, verifier over base..candidate.

---

# Stage 1 — Boundaries

**Objective:** green baseline, tooling, and every layer 0–2 module.
**Absorbs:** old pre-stage, 0, 1, 2, 3. **Behavior:** preserving only.
**Prerequisites:** none. **Review candidate:** one.

| Slice | Label(s) | Content | Pre / post tests | Untouched |
|---|---|---|---|---|
| 1.0 | `test:`, `docs:` | Reapply `ca16130` stub fix + 3 regressions; reapply `7297bf9` and index-idempotence fix if still applicable. Add `docs/cloud-sync-refactor-inventory.md`: production imports from `utils.cloud_sync`, tools/scripts using private helpers, string patch targets, patched-name counts. Refresh line refs in `docs/cloud-sync-architecture.md`; state the early-stamp model as current truth. | pre: `test_cloud_sync_progress_reset_and_prepare` (expect 1 fail); post: broad gate green | all production code |
| 1.1 | `test:` | `tools/verify_cloud_sync_move.py` (§6) with its own unit tests on synthetic before/after modules (identical, renamed free name, duplicated ContextVar, `global`, logger name); `tests/cloud_sync_patching.py` (§5) with tests (same-object rule, missing-name failure); layering test (§3). | post: new tests green; verifier on `HEAD..HEAD` reports nothing | production code |
| 1.2 | `move:`, `adapt:`, `test:` | Layer 0: `errors` (+`is_identity_clear_verification_failed_error`), `profiling`, `progress`, `summary`, `common`. `adapt:` facade re-exports; `reference_cloud_adapter` imports from `errors`; drop lazy imports in `sync_all` (L6877, L7214). Ownership test from donor. | pre/post: `test_cloud_sync_change_notification`, profiler/progress tests, `test_cloud_sync_stage0_ownership`, reference adapter tests | error text, summary keys, progress phases |
| 1.3 | `move:`, `adapt:`, `test:` | Layer 1 remote: `transport` (incl. `_patch_with_precondition`), `pagination`, `pull_only` (registries regenerated from `main`, classify `fetch_image_metadata_purpose`, `_patch_with_precondition`), `remote_reads` (`_pull_remote_images_for_sync`, `_pull_remote_measurements_for_images`, `_group_remote_measurements_by_observation`), `capabilities` (`_owner_sync_parents_supported`, `_remote_metadata_purpose`, `METADATA_PURPOSE_*`). The `_facade()` hook is the only `adapt:` allowed. | pre/post: `test_cloud_download_only`, pagination tests, `test_cloud_sync_stage1_ownership` (future-enforcing classification) | headers, refresh, retry, timeouts, page order |
| 1.4 | `move:`, `test:` | Layer 1 `sync_state` (L5325–5800 keys and per-image state, L6559–6566 media signature store) then layer 2 `image_policy` (L5466/L5486 predicates, L5781–6241 minus L6242) then `tombstones` (5 functions in §1.1.3). One `move:` commit per module. | pre/post: `test_cloud_storage_intent_ledger`, `test_cloud_image_bytes_desired`, `test_cloud_sync_metadata_only`, `test_image_tombstones`, gallery checkbox deletion tests, `test_cloud_sync_fast_path` | ledger key format, flush ordering |

**Invariants at risk:** 1, 2, 3, 10, 11.

**Sol audit (gate to Stage 2), independently:**
- run the verifier yourself; inspect every `adapt:` hunk;
- list every client method and confirm its pull-only classification against
  its body (does it write?);
- trace `_push_pending_image_tombstones` from `sync_all` and confirm it still
  runs before candidate pruning;
- grep production code for imports of moved names from `utils.cloud_sync`
  inside `cloud_sync_impl` (must be none beyond `_facade()`);
- sample 20 retargeted tests and confirm the owner-vs-helper rule (§5).

**Live canary:** no. **Rollback:** per commit; the stage merges as one merge commit.
**Acceptance:** broad gate green; verifier clean except listed `adapt:`s; no
facade redefinition of a moved name; every client method classified.

---

# Stage 2 — Reconciliation substrate and calibrations

**Objective:** pure reconciliation layer, baseline, conflict plan, calibrations;
no module cycle.
**Absorbs:** old 4a, 4b, 5a, taxonomy identity, conflict detail.
**Behavior:** preserving only. Typed plan design is **not** here (Stage 4).
**Prerequisites:** Stage 1 merged. **Review candidate:** one.

| Slice | Label(s) | Content | Pre / post tests |
|---|---|---|---|
| 2.1 | `move:`, `test:` | `reconciliation/sample_source.py` (L7519–7605), `reconciliation/compare.py` (`_observation_compare_payload`, image compare L4061–4441, measurement compare L9073–9373 incl. `_measurement_payloads_match` L9279, location precision L3792–3910). | `test_image_conflict_normalization`, location-precision tests, `test_cloud_sync_conflict_preflight` |
| 2.2 | `move:`, `test:` | `reconciliation/taxonomy_identity.py` (L10898–11240), `reconciliation/preflight.py` (`ObservationPushConflictReport`, `_analyze_observation_push_conflicts`). | `test_cloud_sync_no_baseline_identity_contradiction`, preflight suite |
| 2.3 | `move:`, `test:` | `reconciliation/accepted_asymmetry.py` (§1.1.4), then `baseline.py` (L3555 parse, L5585 build, L5716 load/store, L12483 `_store_remote_snapshot`). | snapshot persistence suite, `test_cloud_conflict_plan_execution` |
| 2.4 | `move:`, `test:` | `conflict_plan.py` (L12319–15811 minus 2.3), `conflict_detail.py` (L19593). | `test_cloud_conflict_plan_execution`, `test_cloud_conflict_dialog`, drift/partial-retry/no-media-deletion tests |
| 2.5 | `move:`, `test:` | `calibrations.py` (L650–1166, L7458–8225 incl. `_reconcile_local_image_calibration_links`; recovery cache L1167–1877 only if all its callers are calibration/original paths, else 3B). | `test_cloud_calibration_sync`, download-only, fast path |

Add to the layering test: `reconciliation/*` imports nothing from layers 1
(client), 4+, `database` writers, or `PySide6`. If a function moved in 2.1–2.2
turns out to read SQLite, it stays in its original slice as-is and is listed in
the handoff for Stage 4; do not split it here (that would be `adapt:` logic
work in a mechanical stage).

**Invariants at risk:** 5, 12, 13, 14. **Untouched:** snapshot schema/versions,
`_CONFLICT_PLAN_BASELINE_SCHEMA_VERSION` (L13067), fingerprint output.

**Sol audit (gate to Stage 3), independently:**
- confirm no import edge `baseline → conflict_plan` and none from
  `reconciliation/*` upward;
- inspect every remaining `_store_remote_snapshot` call site and confirm
  arguments and ordering unchanged;
- run conflict-plan fixtures (baseline drift, partial retry, accepted asymmetry
  carry) and compare persisted snapshot JSON byte-for-byte against base;
- spot-check that taxonomy-identity contradiction still fails closed on a
  no-baseline fixture.

**Live canary:** no. **Acceptance:** broad gate green; verifier clean; layering
test enforces the reconciliation purity rule.

---

# Stage 3 — Domain executors (two review candidates)

**Objective:** owners for image identity, anchors, images, measurements and
derived products, in dependency order.
**Absorbs:** old 5b, 6a, 6b, 6c; EXIF; image-size formatting; derived glue.
**Behavior:** preserving only. Deep `mark_observation_dirty` calls move unchanged.
**Prerequisites:** Stage 2 merged.

Two candidates because 3B (~6–7k lines across images, measurements, derived)
plus 3A would exceed what one audit can trace through identity and deletion
paths. Each leaves a coherent, green architecture.

### 3A — Identity and anchors

| Slice | Label(s) | Content | Pre / post tests |
|---|---|---|---|
| 3A.1 | `move:`, `adapt:`, `test:` | `image_identity.py`: `_reconcile_local_image_cloud_id` (L6242), lost-link helpers, `_finalize_portable_cloud_identity_guard` (L23640), identity-clear trio + `_verify_identity_clear_landed`; client methods `_resolve_existing_observation_for_push`, `_resolve_existing_image_for_push`, `_find_cloud_image` into an identity mixin (`move:` with verifier check 4). | `test_image_push_identity`, `test_cloud_identity_fail_closed`, `test_portable_cloud_identity_guard`, `test_cloud_sync_no_baseline_identity_contradiction` |
| 3A.2 | `move:`, `test:` | `anchors.py`: L9446–9566 predicates (`microscope_image_requires_*`, `measurement_qualifies_for_public_spore_anchor`, `_cloud_explicit_media_upload_selection`), `_ensure_local_metadata_only_microscope_anchor` (L11772), L22094–22160 promotion helpers, L22953–23640 ensure/owner-sync/retire. | `test_cloud_anchor_promotion`, `test_cloud_sync_metadata_only`, mosaic anchor tests, `test_cloud_media_measurement_mosaic_chain` |

**Invariants at risk:** 4, 6, 7, 8.
**Sol audit (3A), independently:** trace image push identity from the facade
`push_all` through `_resolve_existing_image_for_push` and confirm every
disagreement path raises and none reaches `_post`; trace owner-sync parent
creation and confirm the capability probe still gates it; confirm promotion
order (pending marker → reserve PATCH → upload → finalize / rollback) by
reading the moved code, not the tests.

### 3B — Images, measurements, derived

| Slice | Label(s) | Content | Pre / post tests |
|---|---|---|---|
| 3B.1 | `move:`, `test:` | `images.py`: `_push_images_for_observation` (L22186) and its helpers, `_promote_temp_imported_image_if_needed` (L11720), `_remote_images_missing_locally`, `_apply_remote_images_to_local` (L12106), materialization, media signatures incl. `_carry_forward_local_mosaic_signature`, EXIF (L11310, L26041), size-limit formatting (L2535–2790, L19174), recovery cache if not in 2.5, pending-image repair scan + `_CLOUD_PENDING_IMAGE_REPAIR_VERSION` (one commit). | `test_cloud_sync_image_upload_policy`, dirty-pending-image, media-pull retry, original sync/recovery, `test_cloud_spore_mosaic_signature`, `test_cloud_spore_mosaic_unchanged_sync`, fast path, dirty-loop |
| 3B.2 | `move:`, `test:` | `measurements.py`: lookups L9982–10050, push L23759, reconcile L23977, import L27355. | `test_cloud_measurement_sync_v1`, `test_sync_observation_dirty_propagation` |
| 3B.3 | `move:`, `test:` | `derived.py`: summary/mosaic glue L24217–25770 (callers of `spore_summary_sync`, `cloud_spore_mosaic`). | spore summary tests, mosaic tests, `test_cloud_media_measurement_mosaic_chain` |

**Invariants at risk:** 7, 9, 15, 16, 17; image push ordering (intent init →
identity/link → tombstone/protection filter → prep → metadata reserve/create →
byte upload → metadata finalize → local `cloud_id` bookkeeping);
`prepared_items` never becomes desired-state truth.
**Sol audit (3B), independently:** read the moved `_push_images_for_observation`
and confirm the ordering above; trace deletion intent from checkbox to
`_push_pending_image_tombstones` and confirm no new path can delete a cloud
image; confirm every `mark_observation_dirty` call site is unchanged in count
and condition (grep base vs candidate); confirm the repair version constant and
scan moved together.

**Live canary:** no, unless an `adapt:` touches an identity or anchor write path.
**Acceptance:** per candidate: broad gate green, verifier clean;
`SporelyCloudClient` keeps only mixin wiring, observation/calibration/reference
RPCs and capability probes; the facade's remaining bulk is `push_all`,
`pull_all`, `sync_all`.

---

# Stage 4 — Orchestration replacement

**Objective:** one owner of observation completion; final `synced` commit on
push and pull; shared typed reconciliation; structured issues.
**Absorbs:** old 6.5a–j, 7a–7e, 8a, 8d. **Prerequisites:** 3B merged.
**Review candidate:** one, with an internal Sol design gate.

Order is chosen so every commit is green and there is never more than one
writer of `sync_status='synced'` or of snapshots.

| Slice | Label | Content |
|---|---|---|
| 4.1 | `docs:` | Design note appended to this plan: state machine; `SyncIssue`, `OperationOutcome`, `ObservationSyncOutcome`, `ReconciliationPlan`; required-vs-best-effort table audited from tests/contract (incl. whether pull completion requires materialization under `sync_images=False`); snapshot/stamp owner API; classification of every test asserting call order/counts (start: `test_cloud_conflict_dialog.py:797,826`; `test_cloud_conflict_plan_execution.py:120,416,2950`; `test_cloud_sync_conflict_preflight.py` L747–1248; the 39 files with exact call/count asserts) as *contract* or *accidental*. **Sol design gate before 4.2.** |
| 4.2 | `adapt:` | `observation_coordinator.py` with `commit_synced(...)` and `store_baseline(...)`. Every existing stamp (push L21016; pull L26367–L27085) and every `_store_remote_snapshot` caller is routed through it **with unchanged timing**. Grep test: no other writer of `sync_status='synced'` or snapshot store. From here on there is exactly one writer. |
| 4.3 | `test:` | Desired-semantics tests (final commit after required work + snapshot; snapshot failure blocks synced; pull equivalents) as `xfail(strict=True)`. Capture reconciliation decision fixtures from current push and pull classifiers. |
| 4.4 | `behavior:` | Coordinator moves the commit point: stamp after required work and snapshot; remove compensations L21085/L21354/L21445/L21573; pull stamps likewise. Flip 4.3 xfails. Update the tests 4.1 classified as accidental for *this* change, in this commit. |
| 4.5 | `behavior:` | `reconciliation/types.py`; push and pull classification switched to it **in one commit**; 4.3 decision fixtures must match except listed, reviewed differences. `_mosaic_render_state_unverified` becomes a plan query. No period with two classifiers. |
| 4.6 | `behavior:` | `push_executor.py`, `pull_executor.py` return outcomes to the coordinator; deep `mark_observation_dirty`/`mark_observation_media_dirty` removed or documented as exceptions (grep test). Accidental-test updates for this change in this commit. |
| 4.7 | `behavior:` | Structured issue pipeline: executors emit `SyncIssue`; `summarize_sync_issues()` consumes them; legacy `result["errors"]` strings generated from issues. A golden test captured in 4.3 asserts legacy result dicts/strings unchanged except documented items. |
| 4.8 | `move:`/`adapt:` | Thin `orchestration.sync_all`; facade delegates. |

**Intentional behavior changes (exhaustive):** synced written once after
required work and snapshot persistence (push and pull); snapshot failure blocks
synced; push and pull share one classifier; issues typed. Everything in §2 holds.

**Why not split Stage 4:** 4.2 (single writer, timing unchanged) is the only
stable intermediate state worth merging on its own. If 4.4–4.8 run long or Sol
rejects the design, merge 4.1–4.3 as candidate 4a and continue as 4b; otherwise
one candidate.

**Invariants at risk:** 3, 4, 9, 12, 14, 15, 18, 20.
**Sol audit, independently (full candidate):**
- list every write to `sync_status`, snapshots and review-pending markers in
  the candidate; each must go through the coordinator or be a documented
  exception;
- run 4.3 decision fixtures against base and candidate; explain each difference;
- trace one failing-image observation and one failing-measurement observation
  end to end and confirm it stays dirty, visible, and keeps the old baseline;
- confirm pull-only still performs zero writes and fast no-op sync zero writes;
- diff legacy `sync_all` result dicts on the fixture matrix.

**Live canary:** yes, on the full candidate (§9).
**Acceptance:** grep tests (single writer; no deep dirty marks) green; no
early stamp; one classifier imported by both executors; legacy results
compatible; canary clean.

---

# Stage 5 — Retire transitional scaffolding

**Objective:** remove what the transition added; nothing optional.
**Prerequisites:** Stage 4 merged and canaried. **Review candidate:** one.

| Slice | Label | Content |
|---|---|---|
| 5.1 | `adapt:` | Remove the `_facade()` late binding; transport imports its four names from owners. |
| 5.2 | `test:` | Retarget helper uses to owners where every caller is in an owner module; reduce `patch_cloud_sync` to a documented list of imported dependencies or delete it. |
| 5.3 | `adapt:`/`docs:` | Facade reduced to re-exports + legacy adapters; architecture doc and contract navigation (both repo copies per contract rules) point to owners; inventory doc archived. |
| 5.4 | `behavior:` (conditional) | Only if Stage 4's no-op-write audit (4.1 table) found a write that changes remote `updated_at` without semantic change: suppress it, with a zero-write test. Otherwise skip. |

**Dropped from the old Stage 8–10:** client split (no maintenance problem once
identity moved to a mixin and callers construct only `SporelyCloudClient`);
diagnostics rework (typed outcomes in 4.6–4.7 already provide candidate → plan →
execution → final state); hidden-state audit (now a Stage 4 grep test); anchor
reservation risks (already tracked in `INBOX.md`; separate behavior work).

**Sol audit:** confirm no `cloud_sync_impl` module imports the facade; confirm
remaining helper uses are on the documented list; for 5.4, confirm the
suppressed write is semantically a no-op on all fixtures.
**Live canary:** only if 5.4 lands. **Acceptance:** Definition of done.

---

## 8. What we removed from the old plan and why

- **20 checkpoints → 5 stages / 6 review candidates.** Verifier (§6), layering
  test, commit labels and Sol audits carry the safety; review cycles per module
  do not add evidence.
- **Separate pre-stage** → slices 1.0–1.1.
- **Policy/tombstone split** → one slice; one-directional, small coupling.
- **4a/4b split** → the cycle is broken by moving pure asymmetry logic down.
- **Calibrations as a leaf stage** → last slice of Stage 2 (needs snapshot
  codec and sample-source helpers).
- **Measurements before images** → reversed.
- **6a/6b/6c as three stages** → 3A and 3B.
- **Stage 6.5 standalone** → Stage 4.1 with a Sol design gate.
- **7a–7e as five states** → one stage ordered so there is a single writer
  from 4.2 on.
- **6.5k (reference-use pending signal)** → product feature, moved to `INBOX.md`.
- **Stage 8 items** → 8a/8d into Stage 4; 8b conditional 5.4; 8c covered by
  typed outcomes; 8e to `INBOX.md`.
- **Stage 9 client split, Stage 10 facade removal** → dropped (no concrete
  benefit); facade kept by design.

## 9. Things that still deserve small commits even though they no longer deserve separate stages

- The red-baseline fix (1.0), alone.
- Each `move:` of one module, separate from its `adapt:` re-exports and its
  `test:` retargeting.
- The `reference_cloud_adapter` import switch.
- Every pull-only registry change.
- `accepted_asymmetry` before `baseline` before `conflict_plan`.
- The identity mixin leaving `SporelyCloudClient`.
- Repair-scan + version constant (together); `_carry_forward_local_mosaic_signature`.
- Stage 4: each of 4.1–4.8; accidental-test updates inside the behavior commit
  they belong to, never batched at the end.
- Stage 5: `_facade()` retirement; each no-op suppression.

## 10. Live-canary policy

Canary only where remote-state semantics can change: Stage 4 (required), 5.4,
any Stage 3 `adapt:` on an identity or anchor write path. Before: fresh SQLite
backup; read-only reconciliation report with C=D1=D2=E=H=0 (or a documented
exception). Deliberate Sync Now on a disposable or known account; record
account/DB/build; rerun the report and diff. Never combined with cleanup or GC.
Live Supabase writes are human-gated per `AGENTS.md`.

## 11. Execution estimate

| Candidate | Slices | Sol audits | Sparring cycles (expected) | Canary |
|---|---|---|---|---|
| 1 Boundaries | 5 | 1 | 1–2 | — |
| 2 Reconciliation + calibrations | 5 | 1 | 1–2 | — |
| 3A Identity + anchors | 2 | 1 | 1–2 | conditional |
| 3B Images, measurements, derived | 3 | 1 | 1–2 | — |
| 4 Orchestration | 8 | 2 | 2–3 | 1 |
| 5 Scaffolding retirement | 3–4 | 1 | 1 | conditional |
| **Total** | **26–27** | **7** | **7–12** | **1–2** |

Branch drift is the main schedule risk: keep each stage branch short-lived,
merge on acceptance, and do not run another `cloud_sync.py` feature branch in
parallel with a `move:` stage. A `cloud_sync.py` change landing on `main` during
a stage is rebased by redoing the affected `move:` commit, never by hand-merging
into moved code.

## 12. Out of scope

E3 R2 garbage collection; `G_conflicting_intent` repair; historical duplicate
cleanup; migration-tool hardening; cloud schema changes not required by the
Stage 4 design; spore orientation; UI redesign; account-link/reset; broad
lint/type migration; external publishing; 6.5k pending-change signalling;
anchor reservation risk fixes.

## 13. Definition of done

- **Compatibility:** `utils/cloud_sync.py` is re-exports plus thin legacy
  adapters; production imports unchanged or migrated; legacy result dicts
  supported; no mirror mutable state; no `_facade()` late binding.
- **Ownership:** every region in §3 has exactly one owner; layering test green;
  ownership tests assert facade identity; verifier history shows every move
  `identical` or a justified `adapt:`.
- **Reconciliation:** `reconciliation/*` is pure (enforced); push and pull share
  it; candidate selection stays separate; derived products query the plan.
- **State:** only the coordinator writes `sync_status='synced'` and snapshots
  (enforced), after required work and snapshot persistence; required failures
  stay retryable and visible; unresolved conflicts never advance the baseline.
- **Diagnostics:** issues are typed; each failed observation is explainable as
  candidate reason → plan → execution → final state from the result.
- **Safety:** every §2 invariant passes its suite; pull-only and fast no-op
  sync perform zero writes.
- **Tests:** `patch_cloud_sync` removed or limited to a documented list; all
  `cloud_sync`-importing tests green; final canary clean.

## 14. Anti-goals

A split that keeps the implicit state machine; competing classifiers in push
and pull; a multi-thousand-line `reconciliation` or `coordinator` module; domain
helpers that decide observation completion; `synced` meaning "started and
hoping"; removing the facade to reduce file count; pulling external publishing
into cloud sync.

> **Target:** a boring facade over focused owners, pure reconciliation, explicit
> executors, and one coordinator that commits `synced` only when the whole
> required sync transaction is actually complete.
