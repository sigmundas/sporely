# Cloud Sync Extraction and Orchestration Refactor Plan

Status: authoritative plan for decomposing and hardening `utils/cloud_sync.py`.
Rewritten 2026-10-03 against `main` @ `65f09d2` (plan `5a41cd1`); refined for
execution the same day (dependency corrections in §1.1), and revised after the
first faithful agent-sparring intake (Stage 4 split into 4A/4B, post-acceptance
canary gates, relocation-aware verifier, `sporely-web` contract sibling). The
2026-08-23 version (20 checkpoints) remains in git history.

## Agent handoff

- **Status:** Active, ready for execution. No stage accepted on `main`.
- **Donor evidence:** `feature/cloud-sync-transport-boundary` (`4002270`, merge
  base `7acaad1`, 274 commits behind `main` at `b39dfd1`) implemented old Stages 0–1 and
  passed review in September. It is a donor, never a merge source (§4).
- **Review candidates (7, fixed):** 1, 2, 3A, 3B, 4A, 4B, 5. Slices inside a
  candidate are implementation steps, not managed stages.
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

layer 0  common, errors, profiling, progress, summary;
         observation_payload (PURE observation-row → payload serializer, Stage 2)
layer 1  transport, pagination (client mixins); pull_only; remote_reads;
         capabilities; sync_state (settings keys, per-image local state,
         media/file signature stores)
layer 2  image_policy (desired bytes, ledger, anchor predicates); tombstones
layer 3  reconciliation/ (PURE: no SQLite/database reads or writes, no settings,
         no cloud I/O/client, no Qt)
           sample_source, compare (+ pure location-precision ranking),
           taxonomy_identity, preflight (report type + pure formatting),
           accepted_asymmetry, types (Stage 4B)
layer 4  baseline (snapshot codec + load/store; reads remote via remote_reads)
layer 4b stateful reconciliation adapters (not pure; outside reconciliation/),
         in this import order: taxonomy_identity_state (installed-taxonomy reads,
         ObservationDB identity writes); location_precision; observation_preflight
layer 5  domain owners, in dependency order:
           calibrations
           image_identity (+ client identity mixin) → anchors → images → measurements → derived
layer 6  conflict_plan, conflict_detail
layer 7  observation_coordinator (ONLY writer of sync_status='synced' and snapshots, Stage 4B)
         push_executor, pull_executor (Stage 4B)
layer 8  orchestration (thin sync_all, Stage 4B)
```

Sibling owners stay outside: `cloud_media_policy`, `original_sync_policy`,
`cloud_media_recovery`, `cloud_media_audit`, `cloud_spore_mosaic[_backfill]`,
`spore_summary_sync`, `r2_storage`, the reference-sync stack
(`reference_cloud_sync`, `reference_cloud_adapter`, `curated_reference_sync`,
`database/reference_sync_*`). External publishing is out of scope.

A layering test (added 1.1, extended each stage) parses imports of every
`cloud_sync_impl` module and fails on upward or facade imports.

Dependency direction for the stateful adapters is always toward pure code:
`taxonomy_identity_state → reconciliation/taxonomy_identity`;
`location_precision → reconciliation/compare, baseline`;
`observation_preflight → location_precision, baseline, tombstones,
taxonomy_identity_state, reconciliation/*`. Nothing in `reconciliation/` or
`observation_payload` imports a stateful adapter.

### 3.1 Shared sync contract and the `sporely-web` sibling

The shared contract exists in two repositories:
`sporely-py/docs/supabase-sync-contract.md` and
`sporely-web/docs/supabase-sync-contract.md`. A change to its wording must land
in both copies in the same work item.

- Primary implementation is always in `sporely-py`.
- Only a candidate that actually changes contract wording (expected: Stage 4B,
  if 4A design §7 lists changes; Stage 5 only if 5.4 lands and changes
  contract wording) declares `sporely-web` as a **sibling repository
  candidate** containing the matching contract-only change to
  `docs/supabase-sync-contract.md` — no other `sporely-web` files.
- Sibling membership is decided at **run approval**, not at intake: the
  Stage 4B run is approved with `sporely-web` declared iff accepted
  `docs/cloud-sync-stage4-design.md` section 7 lists wording changes; the
  Stage 5 run is approved with it iff 5.4 will run and changes contract
  wording. A person confirms this at each of those approvals.
- Agent-sparring acceptance of that candidate pins and verifies both commits
  (`sporely-py` candidate SHA and `sporely-web` sibling SHA); the two contract
  copies must be byte-identical in the changed sections.
- Navigation/line-reference refreshes that live only in `sporely-py` docs
  (`docs/cloud-sync-architecture.md`) never require a sibling. A stage whose
  contract wording does not change has no sibling candidate.

### 3.2 Symbol-to-owner inventory (overlapping and high-risk regions)

Line ranges in this plan are search hints; this table wins. Each symbol has one
final owner. Inspected at `b39dfd1`.

| Symbol(s) | Final owner | Slice | Why |
|---|---|---|---|
| `_safe_int` (L8414) | `common` | 1.2 | Pure leaf used by Stage 1 `sync_state` and Stage 2 compare. |
| Settings keys / per-image local state L5325–5584, L5737–5800 (file signatures, pending-promotion keys, metadata-only state) | `sync_state` | 1.4 | Per §1.1.7. |
| `_cloud_observation_snapshot` (L5585), `_normalize_accepted_asymmetry_for_snapshot` (L5661), `_normalize_accepted_asymmetry_entry` (L5702), `_load_cloud_observation_snapshot` (L5716), `_store_cloud_observation_snapshot` (L5726), `_parse_cloud_observation_snapshot` (L3555), `_store_remote_snapshot` (L12483) | `baseline` | 2.3 | Snapshot codec/persistence; excluded from Stage 1 `sync_state` although inside its line range. |
| `_RemoteIdentityClaim`, `_identity_sync_key`, `_remote_identity_claim`, `_withhold_identity_from_push`, `_remote_row_without_identity`, `_local_identity_sync_key`, `_baseline_identity_key`, `_remote_identity_changed_since`, `_local_identity_is_claim`, `_classify_identity_sync_change`, `_remote_name_snapshot` | `reconciliation/taxonomy_identity` | 2.1 | Operate only on supplied values. |
| `_installed_taxon_concept`, `_local_identity_columns_for_remote_claim`, `_apply_remote_identity_to_local` | `taxonomy_identity_state` | 2.1 | Read installed taxonomy DB / write `ObservationDB`. |
| `_apply_remote_observation_fields` (L11238) | not Stage 2 (pull apply; facade until Stage 4B `pull_executor`) | 4B | Edge of the taxonomy range only; observation pull application, not taxonomy identity. |
| `_observation_push_payload`, `_normalize_observation_*` (L3600–3681), `_sharing_scope_to_cloud_visibility`, `_parse_sync_timestamp` | `observation_payload` | 2.2 | Pure serializer; needed by compare; no push execution. |
| `_location_precision_rank`, `_precision_local_id` | `reconciliation/compare` | 2.2 | Pure. |
| `_confirmed_location_precision_key`, `record_confirmed_location_precision`, `_confirmed_location_precision`, `consume_confirmed_location_precision`, `_guard_local_location_precision` | `location_precision` | 2.2 | Read/write `SettingsDB`. |
| `_snapshot_baseline_for_cloud_id`, `repair_legacy_location_precision` | `location_precision` | 2.3 | Load snapshots (needs `baseline`); repair writes SQL/settings. Public name re-exported by facade. |
| `ObservationPushConflictReport`, `_format_push_conflict_review_reasons` | `reconciliation/preflight` | 2.4 | Pure data/formatting. |
| `_analyze_observation_push_conflicts`, `_observation_push_diff_fields`, `_local_has_real_changes_since_snapshot`, `_local_image_snapshot_payload`, `_image_calibration_uuid`, `_locally_tombstoned_snapshot_image_identity_keys` | `observation_preflight` | 2.4 | Read snapshots, tombstones, `CalibrationDB`, confirmed precision. |
| `_shape_geography_patch_payload` (L3950), `_set/_clear_observation_conflict_review_pending` (L4700/L4717) | not Stage 2 (push path / review markers; facade until Stage 4B push executor / coordinator) | 4B | Stateful push-execution and review-marker writers. |
| `_carry_forward_local_mosaic_signature` (L24828) | `images` | 3B.1 | Only caller is `_sync_existing_remote_image_to_local` (image pull working-file swap); not in `derived` despite the L24217–25770 range. |

## 4. Using the donor branch

- Start every stage from current `main`. Never merge, rebase or cherry-pick the
  donor wholesale.
- **Reuse:** module layout of `errors`, `profiling`, `progress`, `summary`,
  `common`, `pagination`; ownership tests `tests/test_cloud_sync_stage0_ownership.py`,
  `tests/test_cloud_sync_stage1_ownership.py`; the **test-only** parts of
  `ca16130` (stub signature + 3 materialization-state regressions in
  `tests/test_cloud_media_pull_retry.py`), each only after confirming it still
  applies on `main`.
- **Skipped donor fixes (checked against `main` @ `b39dfd1`):** the
  `reference_library_schema` index-idempotence fix is already on `main`
  (`_normalized_index_ddl`, `_existing_index_sql`) — skip. The `ca16130`
  `cloud_sync.py` materialization fix is already on `main` — skip. The
  migration-tool dry-run fixes (`ca16130` tool part, `7297bf9`) touch
  `tools/migrate_legacy_reference_values.py`; that is migration-tool hardening
  (§12, out of scope) — not reapplied by this plan.
- **Regenerate from `main`, never copy:** `pull_only.py` registries;
  `transport.py` (add `_patch_with_precondition` and the session-cache hook
  near `_cloud_timing_log`, L210).
- **Lessons applied up front:** (1) *shadowing* — delete the facade definition
  and add its re-export in the same commit as the move (§6.1); ownership tests
  assert `facade.X is owner.X`;
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

Retargeting (`monkeypatch.setattr(cloud_sync, "<name>", v)` → owner patch if
the rule above requires it, else `patch_cloud_sync(...)`) is placed per §6.1: in
the `move:` commit when the existing suite would otherwise go red, else in an
immediately following `test:` commit.
String targets (`mock.patch("utils.cloud_sync.<name>")`) are inventoried in
1.0 and treated the same way.

## 6. Mechanical-move verifier

`tools/verify_cloud_sync_move.py --base <sha> --candidate <sha>` (added 1.1).
For every top-level function, class, method and module-level statement removed
from `utils/cloud_sync.py` (or from `SporelyCloudClient`) it locates the new
definition and reports `identical`, `differs` (with a diff), `adapt-required`,
or `missing`. Every symbol in a `move:` commit must be `identical`. Anything
else must be in an `adapt:` or `behavior:` commit. The verifier is
relocation-aware but fails closed: when it cannot prove equivalence it reports
`differs` or `adapt-required`, never `identical`.

### 6.1 Smallest green extraction unit

A `move:` commit contains exactly:
- the definition(s) moved unchanged to the owner module;
- deletion of the old facade definition;
- the facade re-export/binding (`from ...owner import X`) and any owner-module
  imports needed so the moved code resolves the same objects (§6.3). These
  bindings make the relocation executable and must not alter behavior;
- test patch retargeting (§5) **only when** the existing suite would otherwise
  be red after the move.

Retargeting not needed for green goes in an immediately following `test:`
commit. Semantic changes (signature plumbing, logger-name changes, anything the
verifier reports `differs`/`adapt-required`) go in separate `adapt:` commits.
**Every committed slice is green** (focused tests pass, facade identity holds).
The only exception is a commit explicitly marked as a deliberate red-test design
step (Stage 4B slice 4B.2 `xfail(strict=True)` tests, which still pass as xfail).

### 6.2 Definition equivalence

Compared exactly, no normalization: the full AST of the definition, including
body, decorators, default values, annotations, docstrings, keyword arguments,
`global`/`nonlocal` statements, and nested functions/classes. Comments and
whitespace are not in the AST and are ignored. Any difference in decorators,
defaults or annotations is `differs` and requires `adapt:`.

### 6.3 Global/free-name binding equivalence

Base and candidate are imported in separate subprocesses, so equivalence is
established by **origin**, never by cross-process `is`. For every global/free
name the definition reads, the verifier records its origin in each process:
- function/class: `(__module__, __qualname__)` plus a source-AST hash of the
  defining code;
- module: its `__name__`;
- immutable literal (`int`, `str`, `bytes`, `float`, `bool`, `None`, tuples/
  frozensets of these): `repr`;
- anything else (instances, mutable containers, loggers, locks, `ContextVar`s,
  partials, bound methods): no origin — the name is `adapt-required` unless
  §6.5 proves it is the single shared instance.

A free name is equivalent when its candidate origin equals its base origin, or
when the base origin `utils.cloud_sync.X` maps through the **relocation map**
(generated from this and earlier verified `move:`s: `utils.cloud_sync.X →
owner.X`) to the candidate origin and the AST hashes match. Otherwise
`differs`. Within the **candidate** process alone, the verifier additionally
checks `utils.cloud_sync.X is owner.X` for every moved name (§6.1) and that the
owner module's binding of each free name `is` the facade's binding of the same
name where both exist. Acceptance tests for these rules are part of slice 1.1.

### 6.4 Relocation metadata

Differences caused solely by relocation — `__module__`, `__qualname__` prefix
(class → mixin), owner module path, `__globals__` identity — never by themselves
fail a valid move.

### 6.5 Module-level state

A moved assignment, registry, cache dict, `ContextVar`, lock or logger must
exist exactly once across facade + owners; the facade name must bind the same
object. Two instances = `differs`.

### 6.6 Automatic `adapt-required` (manual review, even if the AST is identical)

`global`; `globals()`; `sys.modules[__name__]`, `getattr(<module>, …)`,
`importlib` or other dynamic module lookup; late-bound facade resolution
(`_facade()`); descriptors, properties, `__init_subclass__`/metaclass-sensitive
code; `logging.getLogger(__name__)` or any logger whose name/identity changes;
string-based patch targets.

### 6.7 Client methods moved to mixins

For each method moved into a `SporelyCloudClient` mixin: the method's AST is
`identical` (§6.2); its free names satisfy §6.3 in the mixin module;
`SporelyCloudClient.<name>` resolves via the MRO to the moved function; no other
attribute of `SporelyCloudClient` changes its MRO resolution; `super()` /
`self.<attr>` references resolve to the same functions as at base. If the
verifier cannot prove this, it reports `adapt-required`.

**Never ignored:** renamed identifiers (local or global), reordered statements,
changed literals, changed imports *inside* function bodies, changed exception
types, added/removed `try`, changed default arguments. A false `differs` costs a
short `adapt:` review; a false `identical` is not acceptable.

Commit labels: `move:` (§6.1; all symbols `identical`), `adapt:` (import/
binding/logger-name/signature plumbing only, each listed with a one-line
reason), `test:` (retargeting, new tests), `behavior:` (intentional change with
its own tests), `docs:`.

## 7. Slice protocol (every implementation slice)

The implementer (GPT-5.5-low) receives one slice at a time and:

1. Confirms clean `git status`, records base SHA, reads `AGENTS.md`,
   `.claude/rules/cloud-sync.md`, this slice and §2.
2. Runs the slice's **pre** tests; stops and reports if not green.
   **Bootstrap exception (slice 1.0 only):** the one known red baseline test
   (Agent handoff) is expected to fail before 1.0; any *additional* failure is a
   stop. After 1.0 every `cloud_sync`-importing test file must be green.
3. Makes the commits in the listed order, one label per commit.
4. Before each commit runs the slice's focused tests on the working tree;
   commits only if green; after each `move:` commit runs the verifier on the
   commit and records its output (§6.1: every commit green).
5. Runs the slice's **post** tests, the layering test, ownership tests (each
   only once it exists — from slice 1.1 on),
   `py_compile` of touched files, and a subprocess import of the facade.
6. Does not touch anything listed under "Untouched".
7. Pushes; hands the reviewer: base/candidate SHAs, `git log --oneline`,
   verifier output, test commands + pass counts, list of `adapt:` reasons,
   patch-retargeting counts (owner vs helper).

**Stage-wide broad gate** (run once per review candidate, not per slice): all
test files importing `cloud_sync` (record count; ~1,593 at `5a41cd1`),
layering test, ownership tests, verifier over base..candidate.

**Candidate acceptance vs. human gates.** Verification of every candidate in
this plan is self-verifiable (automated tests, verifier, Sol audit); no
candidate's verification requires live Supabase writes, so the `AGENTS.md`
human-gated "do not commit" rule does not apply to candidate commits. The plan
authorizes creation of a human canary gate, not execution of live writes. When
a canary gate is reached, agent-sparring must pause and obtain the user's
explicit authorization and evidence before the live sync is performed or the
gate is satisfied. A review candidate is accepted by
automated implementation evidence plus the independent GPT-6.1-Sol audit
through agent-sparring; acceptance never waits on a live canary. Where §10
requires a live canary, it is a **post-acceptance human gate**: it runs on the
accepted, pushed candidate, and the next candidate may not start until the gate
has passed (or, for Stage 5, it is the final closeout gate). A failed canary is
handled as a new corrective candidate, never by amending the accepted one.

---

# Stage 1 — Boundaries

**Objective:** green baseline, tooling, and every layer 0–2 module.
**Absorbs:** old pre-stage, 0, 1, 2, 3. **Behavior:** preserving only.
**Prerequisites:** none. **Review candidate:** one.

| Slice | Label(s) | Content | Pre / post tests | Untouched |
|---|---|---|---|---|
| 1.0 | `test:`, `docs:` | Reapply the test-only parts of `ca16130` (stub fix + 3 regressions) if still applicable; donor production fixes are skipped (§4). Add `docs/cloud-sync-refactor-inventory.md`: production imports from `utils.cloud_sync`, tools/scripts using private helpers, string patch targets, patched-name counts. Refresh line refs in `docs/cloud-sync-architecture.md`; state the early-stamp model as current truth. | pre: `test_cloud_sync_progress_reset_and_prepare` (expect exactly the 1 known fail, §7 bootstrap exception); post: every `cloud_sync`-importing test file green (verifier/layering/ownership tests do not exist yet)  | all production code |
| 1.1 | `test:` | `tools/verify_cloud_sync_move.py` (§6) with its own unit tests on synthetic before/after modules (identical, valid relocation incl. mixin, renamed free name, rebound free name, duplicated ContextVar, `global`, logger name); `tests/cloud_sync_patching.py` (§5) with tests (same-object rule, missing-name failure); layering test (§3). | post: new tests green; verifier on `HEAD..HEAD` reports nothing | production code |
| 1.2 | `move:`, `adapt:`, `test:` | Layer 0: `errors` (+`is_identity_clear_verification_failed_error`), `profiling`, `progress`, `summary`, `common` (+`_safe_int`, §3.2), each `move:` carrying its facade re-export (§6.1). `adapt:` `reference_cloud_adapter` imports from `errors`; drop lazy imports in `sync_all` (L6877, L7214). Ownership test from donor. | pre/post: `test_cloud_sync_change_notification`, profiler/progress tests, `test_cloud_sync_stage0_ownership`, reference adapter tests | error text, summary keys, progress phases |
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
**Behavior:** preserving only. Typed plan design is **not** here (Stage 4A design, 4B implementation).
**Prerequisites:** Stage 1 merged. **Review candidate:** one.

Slice order follows real call dependencies (§3.2 is the symbol inventory).
Symbol ownership wins over line ranges.

| Slice | Label(s) | Content | Pre / post tests |
|---|---|---|---|
| 2.1 | `move:`, `test:` | Taxonomy identity foundations. Pure → `reconciliation/taxonomy_identity.py`: `_RemoteIdentityClaim`, `_identity_sync_key`, `_remote_identity_claim`, `_withhold_identity_from_push`, `_remote_row_without_identity`, `_local_identity_sync_key`, `_baseline_identity_key`, `_remote_identity_changed_since`, `_local_identity_is_claim`, `_classify_identity_sync_change`, `_remote_name_snapshot`. Stateful → `taxonomy_identity_state.py`: `_installed_taxon_concept`, `_local_identity_columns_for_remote_claim`, `_apply_remote_identity_to_local`. | `test_cloud_sync_no_baseline_identity_contradiction`, taxonomy identity sync tests |
| 2.2 | `move:`, `test:` | Observation payload, comparison, location precision. (a) `observation_payload.py`: `_observation_push_payload` and its pure helpers (`_normalize_observation_*` L3600–3681, `_sharing_scope_to_cloud_visibility`, `_parse_sync_timestamp`); inspected at `b39dfd1` it is a pure serializer, so this is `move:`. If at execution it is found to read state, split only the pure payload construction as a listed `adapt:` and stop-and-report anything larger. (b) `reconciliation/sample_source.py` (L7519–7605). (c) `reconciliation/compare.py`: `_observation_compare_payload`, `_baseline_observation_compare_payload`, `_SNAPSHOT_OBS_FIELDS`, `_observation_field_values_match`, `_location_precision_rank`, `_precision_local_id`, image compare L4061–4441 minus §3.2 stateful symbols, `_remote_image_payload` (L8939), measurement compare L9073–9373 incl. `_measurement_payloads_match`. (d) `location_precision.py` (settings-backed part): `_confirmed_location_precision_key`, `record_confirmed_location_precision`, `_confirmed_location_precision`, `consume_confirmed_location_precision`, `_guard_local_location_precision`. | `test_image_conflict_normalization`, location-precision tests, `test_cloud_sync_conflict_preflight` |
| 2.3 | `move:`, `test:` | `reconciliation/accepted_asymmetry.py` (§1.1.4), then `baseline.py` (§3.2 baseline symbols incl. L12483 `_store_remote_snapshot`), then into `location_precision.py`: `_snapshot_baseline_for_cloud_id`, `repair_legacy_location_precision` (need `baseline`). | snapshot persistence suite, location-precision repair tests, `test_cloud_conflict_plan_execution` |
| 2.4 | `move:`, `test:` | Stateful observation preflight → `observation_preflight.py`: `_analyze_observation_push_conflicts`, `_observation_push_diff_fields`, `_local_has_real_changes_since_snapshot`, `_local_image_snapshot_payload`, `_image_calibration_uuid`, `_locally_tombstoned_snapshot_image_identity_keys`. Pure parts → `reconciliation/preflight.py`: `ObservationPushConflictReport`, `_format_push_conflict_review_reasons`. Placed after 2.3 because the analyzer loads snapshots (`_load_cloud_observation_snapshot`). Shape in Stage 2 is "read local state → call pure helpers → return report"; Stage 4A/4B formalizes it into `ReconciliationPlan`. | preflight suite, `test_cloud_sync_conflict_preflight` |
| 2.5 | `move:`, `test:` | `conflict_plan.py` (L12319–15811 minus 2.3/2.4), `conflict_detail.py` (L19593). | `test_cloud_conflict_plan_execution`, `test_cloud_conflict_dialog`, drift/partial-retry/no-media-deletion tests |
| 2.6 | `move:`, `test:` | `calibrations.py` (L650–1166, L7458–8225 incl. `_reconcile_local_image_calibration_links`; recovery cache L1167–1877 only if all its callers are calibration/original paths, else 3B). | `test_cloud_calibration_sync`, download-only, fast path |

Layering/purity test additions (enforced, not advisory): `reconciliation/*`
and `observation_payload` import nothing from layers 1, 2 and 4+ (client,
settings, `sync_state`, `image_policy`, `tombstones`, `baseline`), no stateful
adapter (imports among `reconciliation/*` modules and from `observation_payload`
and layer 0 are allowed), no `PySide6`, and from `database` only the
static `ObservationDB._normalize_location_precision` (explicit allowlist); an AST
scan rejects any call to `get_connection`, `SettingsDB`, `get_app_settings`,
`update_app_settings`, `*DB.<method>` other than the allowlisted static, or
client methods inside those modules. Stateful adapters live only in
`taxonomy_identity_state`, `location_precision`, `observation_preflight`. If
a symbol assigned to a pure module is found stateful at execution, it moves to
the documented stateful owner for its responsibility (§3.2) as a listed
`adapt:`; never weaken the purity test.

**Invariants at risk:** 5, 12, 13, 14. **Untouched:** snapshot schema/versions,
`_CONFLICT_PLAN_BASELINE_SCHEMA_VERSION` (L13067), fingerprint output.

**Sol audit (gate to Stage 3), independently:**
- confirm no import edge `baseline → conflict_plan`, none from
  `reconciliation/*` upward, and none from `reconciliation/*` into a stateful
  adapter;
- inspect every remaining `_store_remote_snapshot` call site and confirm
  arguments and ordering unchanged;
- run conflict-plan fixtures (baseline drift, partial retry, accepted asymmetry
  carry) and compare persisted snapshot JSON byte-for-byte against base;
- spot-check that taxonomy-identity contradiction still fails closed on a
  no-baseline fixture.

**Live canary:** no. **Acceptance:** broad gate green; verifier clean; the
layering/purity test proves `reconciliation/` performs no SQLite/database
reads or writes, no settings reads/writes, no cloud I/O, and imports no Qt;
the stateful wrappers live in `taxonomy_identity_state`, `location_precision`,
`observation_preflight`; the facade only re-exports compatibility names for
moved Stage 2 symbols (incl. public `repair_legacy_location_precision`,
`record_confirmed_location_precision`, `consume_confirmed_location_precision`).

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
| 3B.1 | `move:`, `test:` | `images.py`: `_push_images_for_observation` (L22186) and its helpers, `_promote_temp_imported_image_if_needed` (L11720), `_remote_images_missing_locally`, `_apply_remote_images_to_local` (L12106), materialization, media signatures incl. `_carry_forward_local_mosaic_signature`, EXIF (L11310, L26041), size-limit formatting (L2535–2790, L19174), recovery cache if not in 2.6, pending-image repair scan + `_CLOUD_PENDING_IMAGE_REPAIR_VERSION` (one commit). | `test_cloud_sync_image_upload_policy`, dirty-pending-image, media-pull retry, original sync/recovery, `test_cloud_spore_mosaic_signature`, `test_cloud_spore_mosaic_unchanged_sync`, fast path, dirty-loop |
| 3B.2 | `move:`, `test:` | `measurements.py`: lookups L9982–10050, push L23759, reconcile L23977, import L27355. | `test_cloud_measurement_sync_v1`, `test_sync_observation_dirty_propagation` |
| 3B.3 | `move:`, `test:` | `derived.py`: summary/mosaic glue L24217–25770 except `_carry_forward_local_mosaic_signature` (owned by `images`, §3.2) (callers of `spore_summary_sync`, `cloud_spore_mosaic`). | spore summary tests, mosaic tests, `test_cloud_media_measurement_mosaic_chain` |

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

**Live canary (both candidates):** none, unless an `adapt:` in that candidate
touches an identity or anchor write path; then a post-acceptance human gate
(§7, §10) that must pass before the next candidate (3B after 3A; 4A after 3B)
starts.

**Acceptance shared by 3A and 3B:** broad gate green; verifier clean except
listed `adapt:`s; no facade redefinition of a moved name; the candidate's Sol
audit passed.

**Structural acceptance after 3A:** image-identity and anchor ownership live in
`image_identity.py` and `anchors.py`; the identity client methods resolve
through the identity mixin. Images, measurements and derived code (3B scope)
may still remain in `cloud_sync.py`.

**Structural acceptance after 3B:** images, measurements and derived
summary/mosaic ownership live in `images.py`, `measurements.py`, `derived.py`;
`SporelyCloudClient` keeps only mixin wiring, observation/calibration/reference
RPCs and capability probes; the facade's remaining bulk is orchestration
(`push_all`, `pull_all`, `sync_all` and their coordinator-scale helpers) plus
documented compatibility glue.

---

# Stage 4A — Orchestration design

**Objective:** the accepted design for Stage 4B. Design and evidence only.
**Behavior:** no production code changes (`utils/`, `database/`, `ui/`,
`tools/` untouched). **Prerequisites:** 3B merged and any 3B canary gate passed.
**Review candidate:** one. **Labels:** `docs:` (and `test:` only for read-only
evidence scripts or fixture capture that does not change any existing test).

Deliverable: `docs/cloud-sync-stage4-design.md` (a separate artifact; this plan
is not edited during execution). Required sections:

1. **State machine** — observation sync states and transitions for push and
   pull, including where `sync_status='synced'` and snapshots are written today
   (push L21016; pull L26367–L27085; compensations L21085/L21354/L21445/L21573)
   and where they will be written after 4B.
2. **Types** — `SyncIssue`, `OperationOutcome`, `ObservationSyncOutcome`,
   `ReconciliationPlan`: fields, producers, consumers.
3. **Required vs best-effort work by caller mode** — a table audited from tests
   and the contract, for every combination that matters of `sync_images`,
   `materialize_remote_images`, `full_pull`, `child_safety_pull`, `pull_only`,
   stating what counts as complete for an observation (including whether pull
   completion requires materialization under `sync_images=False`).
4. **Final snapshot/synced ownership** — the `observation_coordinator` API
   (`commit_synced(...)`, `store_baseline(...)`), its single-writer rule, and
   the ordering "required work → snapshot → synced".
5. **Test classification** — every test asserting call order/counts (start:
   `test_cloud_conflict_dialog.py:797,826`; `test_cloud_conflict_plan_execution.py:120,416,2950`;
   `test_cloud_sync_conflict_preflight.py` L747–1248; the 39 files with exact
   call/count asserts) classified *contract* or *accidental*, and for each
   accidental one the 4B slice that will update it.
6. **No-op remote writes** — table of every remote write on a fast no-op sync
   path, stating whether it changes remote `updated_at` without semantic change.
   This section decides whether Stage 5 slice 5.4 runs.
7. **Shared-contract impact** — the exact wording changes 4B makes to
   `docs/supabase-sync-contract.md` (or "none"), which determines whether 4B
   carries a `sporely-web` sibling candidate (§3.1).
8. **Golden/decision fixtures plan** — which fixtures 4B.2 captures.

**Sol audit (gate to 4B), independently:** check every required-vs-best-effort
row against the cited tests/contract lines; confirm the classification of each
*contract* test; confirm the design keeps every §2 invariant; confirm the
no-op-write table against the code. Stage 4B may not start until 4A is accepted.

**Live canary:** no. **Acceptance:** design artifact complete per the list
above; no production diff; Sol design audit passed.

---

# Stage 4B — Orchestration implementation

**Objective:** implement the accepted 4A design: one owner of observation
completion; final `synced` commit on push and pull; shared typed reconciliation;
structured issues.
**Absorbs:** old 6.5a–j, 7a–7e, 8a, 8d. **Prerequisites:** 4A accepted and
merged. **Review candidate:** one. Deviations from the accepted design are
stop-and-report, not implementer decisions.

Order is chosen so every commit is green and there is never more than one
writer of `sync_status='synced'` or of snapshots.

| Slice | Label | Content |
|---|---|---|
| 4B.1 | `adapt:` | `observation_coordinator.py` with `commit_synced(...)` and `store_baseline(...)` per design §4. Every existing stamp (push L21016; pull L26367–L27085) and every `_store_remote_snapshot` caller is routed through it **with unchanged timing**. Grep test: no other writer of `sync_status='synced'` or snapshot store. From here on there is exactly one writer. |
| 4B.2 | `test:` | Desired-semantics tests (final commit after required work + snapshot; snapshot failure blocks synced; pull equivalents) as `xfail(strict=True)` — the deliberate red-test design step (§6.1). Capture reconciliation decision fixtures from current push and pull classifiers and the legacy result golden (design §8). |
| 4B.3 | `behavior:` | Coordinator moves the commit point: stamp after required work and snapshot; remove compensations L21085/L21354/L21445/L21573; pull stamps likewise. Flip 4B.2 xfails. Update the tests design §5 classified as accidental for *this* change, in this commit. |
| 4B.4 | `behavior:` | `reconciliation/types.py`; push and pull classification switched to it **in one commit**; 4B.2 decision fixtures must match except listed, reviewed differences. `_mosaic_render_state_unverified` becomes a plan query. No period with two classifiers. |
| 4B.5 | `behavior:` | `push_executor.py`, `pull_executor.py` return outcomes to the coordinator; deep `mark_observation_dirty`/`mark_observation_media_dirty` removed or documented as exceptions (grep test). Accidental-test updates for this change in this commit. |
| 4B.6 | `behavior:` | Structured issue pipeline: executors emit `SyncIssue`; `summarize_sync_issues()` consumes them; legacy `result["errors"]` strings generated from issues. The 4B.2 golden asserts legacy result dicts/strings unchanged except documented items. |
| 4B.7 | `move:`/`adapt:` | Thin `orchestration.sync_all`; facade delegates. |
| 4B.8 | `docs:` | Shared contract update, only if design §7 lists wording changes: `sporely-py/docs/supabase-sync-contract.md` here, and the identical change in the `sporely-web` sibling candidate (§3.1). |

**Intentional behavior changes (exhaustive):** synced written once after
required work and snapshot persistence (push and pull); snapshot failure blocks
synced; push and pull share one classifier; issues typed. Everything in §2 holds.

**Invariants at risk:** 3, 4, 9, 12, 14, 15, 18, 20.
**Sol audit, independently (full candidate):**
- list every write to `sync_status`, snapshots and review-pending markers in
  the candidate; each must go through the coordinator or be a documented
  exception;
- run 4B.2 decision fixtures against base and candidate; explain each difference;
- trace one failing-image observation and one failing-measurement observation
  end to end and confirm it stays dirty, visible, and keeps the old baseline;
- confirm pull-only still performs zero writes and fast no-op sync zero writes;
- diff legacy `sync_all` result dicts on the fixture matrix;
- confirm the implementation matches the accepted 4A design;
- if a sibling candidate exists, confirm both contract copies are identical.

**Acceptance:** grep tests (single writer; no deep dirty marks) green; no
early stamp; one classifier imported by both executors; legacy results
compatible; if the contract changed, both pinned commits (`sporely-py` and
`sporely-web`) verified.
**Live canary:** required, as a post-acceptance human gate (§7, §10) on the
accepted candidate. Stage 5 may not start until it has passed.

---

# Stage 5 — Retire transitional scaffolding

**Objective:** remove what the transition added; nothing optional.
**Prerequisites:** Stage 4B merged and its post-acceptance canary gate passed.
**Review candidate:** one.

| Slice | Label | Content |
|---|---|---|
| 5.1 | `adapt:` | Remove the `_facade()` late binding; transport imports its four names from owners. |
| 5.2 | `test:` | Retarget helper uses to owners where every caller is in an owner module; reduce `patch_cloud_sync` to a documented list of imported dependencies or delete it. |
| 5.3 | `adapt:`/`docs:` | Facade reduced to re-exports + legacy adapters; `docs/cloud-sync-architecture.md` points to owners; inventory doc archived. Contract navigation is `sporely-py`-only unless contract wording changes (§3.1). |
| 5.4 | `behavior:` (conditional) | Scope: pre-existing writes on paths not covered by the invariant-18 zero-write tests (those tests stay green through 4B; 4B must not introduce any new write). Only if `docs/cloud-sync-stage4-design.md` section 6 "No-op remote writes" lists a write that changes remote `updated_at` without semantic change: suppress it, with a zero-write test. Otherwise skip and say so in the notes. If suppression changes contract wording, add the `sporely-web` sibling (§3.1). |

**Dropped from the old Stage 8–10:** client split (no maintenance problem once
identity moved to a mixin and callers construct only `SporelyCloudClient`);
diagnostics rework (typed outcomes in 4B.5–4B.6 already provide candidate → plan →
execution → final state); hidden-state audit (now a Stage 4B grep test); anchor
reservation risks (already tracked in `INBOX.md`; separate behavior work).

**Sol audit:** confirm no `cloud_sync_impl` module imports the facade; confirm
remaining helper uses are on the documented list; for 5.4, confirm the
suppressed write is semantically a no-op on all fixtures.
**Acceptance:** Definition of done (excluding the final canary).
**Live canary:** only if 5.4 lands; then it is the **final closeout gate** on
the accepted candidate (§7, §10), not part of candidate acceptance. The plan is
complete when Stage 5 is accepted and merged and, if 5.4 landed, the closeout
canary has passed.

---

## 8. What we removed from the old plan and why

- **20 checkpoints → 5 stages / 7 review candidates.** Verifier (§6), layering
  test, commit labels and Sol audits carry the safety; review cycles per module
  do not add evidence.
- **Separate pre-stage** → slices 1.0–1.1.
- **Policy/tombstone split** → one slice; one-directional, small coupling.
- **Old 4a/4b reconciliation split** → the cycle is broken by moving pure
  asymmetry logic down. (Unrelated to the current 4A/4B orchestration split.)
- **Calibrations as a leaf stage** → last slice of Stage 2 (needs snapshot
  codec and sample-source helpers).
- **Measurements before images** → reversed.
- **6a/6b/6c as three stages** → 3A and 3B.
- **Stage 6.5 standalone** → Stage 4A, a design-only review candidate.
- **7a–7e as five states** → Stage 4B, ordered so there is a single writer
  from 4B.1 on.
- **6.5k (reference-use pending signal)** → product feature, moved to `INBOX.md`.
- **Stage 8 items** → 8a/8d into Stage 4B; 8b conditional 5.4; 8c covered by
  typed outcomes; 8e to `INBOX.md`.
- **Stage 9 client split, Stage 10 facade removal** → dropped (no concrete
  benefit); facade kept by design.

## 9. Things that still deserve small commits even though they no longer deserve separate stages

- The red-baseline fix (1.0), alone.
- Each `move:` of one module (with its facade re-export, §6.1), separate from
  any `adapt:` and from retargeting not needed for green.
- The `reference_cloud_adapter` import switch.
- Every pull-only registry change.
- `accepted_asymmetry` before `baseline` before `conflict_plan`.
- The identity mixin leaving `SporelyCloudClient`.
- Repair-scan + version constant (together); `_carry_forward_local_mosaic_signature` (owner `images`, §3.2).
- Stage 4B: each of 4B.1–4B.8; accidental-test updates inside the behavior commit
  they belong to, never batched at the end.
- Stage 5: `_facade()` retirement; each no-op suppression.

## 10. Live-canary policy

Canary only where remote-state semantics can change: Stage 4B (required), 5.4,
any Stage 3A/3B `adapt:` on an identity or anchor write path. Every canary is a
post-acceptance human gate (§7): it runs on the accepted, pushed candidate and
blocks the next candidate (Stage 5's is the final closeout gate). Before: fresh SQLite
backup; read-only reconciliation report with C=D1=D2=E=H=0 (or a documented
exception). Deliberate Sync Now on a disposable or known account; record
account/DB/build; rerun the report and diff. Never combined with cleanup or GC.
Live Supabase writes are human-gated per `AGENTS.md`.

## 11. Execution estimate

| Candidate | Slices | Sol audits | Sparring cycles (expected) | Canary |
|---|---|---|---|---|
| 1 Boundaries | 5 | 1 | 1–2 | — |
| 2 Reconciliation + calibrations | 6 | 1 | 1–2 | — |
| 3A Identity + anchors | 2 | 1 | 1–2 | conditional |
| 3B Images, measurements, derived | 3 | 1 | 1–2 | conditional |
| 4A Orchestration design | 1 | 1 | 1–2 | — |
| 4B Orchestration implementation | 7–8 | 1 | 1–2 | 1 (post-acceptance) |
| 5 Scaffolding retirement | 3–4 | 1 | 1 | conditional (closeout) |
| **Total** | **27–28** | **7** | **7–12** | **1–4** |

Branch drift is the main schedule risk: keep each stage branch short-lived,
merge on acceptance, and do not run another `cloud_sync.py` feature branch in
parallel with a `move:` stage. A `cloud_sync.py` change landing on `main` during
a stage is rebased by redoing the affected `move:` commit, never by hand-merging
into moved code.

## 12. Out of scope

E3 R2 garbage collection; `G_conflicting_intent` repair; historical duplicate
cleanup; migration-tool hardening; cloud schema changes not required by the
Stage 4A design; spore orientation; UI redesign; account-link/reset; broad
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
  `cloud_sync`-importing tests green; every required canary gate passed
  (Stage 4B; Stage 5 closeout if 5.4 landed).

## 14. Anti-goals

A split that keeps the implicit state machine; competing classifiers in push
and pull; a multi-thousand-line `reconciliation` or `coordinator` module; domain
helpers that decide observation completion; `synced` meaning "started and
hoping"; removing the facade to reduce file count; pulling external publishing
into cloud sync.

> **Target:** a boring facade over focused owners, pure reconciliation, explicit
> executors, and one coordinator that commits `synced` only when the whole
> required sync transaction is actually complete.
