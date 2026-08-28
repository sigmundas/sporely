# Cloud Sync Extraction and Orchestration Refactor Plan

Status: authoritative planning document for the staged decomposition and hardening of `utils/cloud_sync.py`.

## Agent handoff

- **Status:** Active; no extraction stage is verified as implemented.
- **Last completed stage:** Pre-existing E1c dead-code cleanup, commit `919b3e7` (a prerequisite, not an extraction stage).
- **Current/next stage:** Pre-stage inventory and baseline, then Stage 0.
- **Relevant commits:** `919b3e7`, `de824a4`.
- **Primary design principle:** **Preserve contracts, not accidents.**
- **Compatibility decision:** Keep `utils/cloud_sync.py` as a stable public compatibility facade unless there is a concrete reason to remove it later.
- **Mechanical-extraction rule:** Mechanical movement commits do not intentionally change behavior.
- **Architecture/hardening rule:** Deliberate changes that improve correctness, resilience, debuggability, or performance are allowed only in explicitly scoped architecture/hardening stages with dedicated tests and validation.
- **Major intended hardening:** Replace the current early `synced` stamp + compensating re-dirty behavior with a single final synced commit after required work and snapshot persistence succeed.
- **Do not combine with this job:** E3 garbage collection, historical duplicate cleanup, unrelated schema work, UI redesign, account-link/reset work, broad lint/type migrations, or external-publishing refactors.
- **Remaining acceptance criteria:** The definition of done and validation matrix at the end of this document.

This document supersedes the older extraction-only plan and the older embedded proposal formerly in `docs/cloud-sync-architecture.md`.

The purpose is no longer merely to spread `utils/cloud_sync.py` across smaller files. The job has two goals:

1. **Create clear ownership boundaries** so the sync implementation is understandable, testable, and maintainable.
2. **Use those boundaries to replace accidental orchestration behavior with an explicit, typed, debuggable sync state model.**

The refactor is not complete merely because the monolith has been split.

---

## 1. Governing rules

### 1.1 Preserve contracts, not accidents

The following are not the same thing:

- public compatibility;
- safety contracts;
- implementation details that happen to exist today.

Public entry points and safety contracts are valuable. Accidental internal behavior is not automatically a compatibility requirement.

Examples of implementation behavior that should **not** be frozen simply because it exists today:

- stamping an observation `synced` before required child work is complete, then compensating by marking it dirty again;
- using formatted log/error strings as machine-readable state;
- performing avoidable no-op cloud writes that still trigger remote `updated_at`;
- having push and pull independently rediscover overlapping three-way reconciliation rules;
- deeply nested helpers mutating global observation sync state without one authoritative completion owner.

### 1.2 Mechanical extraction rule

For Stages 0-6:

> **Move ownership boundaries without intentional behavior changes.**

Within a mechanical extraction commit:

- preserve observable behavior;
- preserve error/result shapes;
- preserve ordering when ordering is load-bearing;
- preserve retry and conflict semantics;
- do not mix movement with cleanup merely because the old code looks ugly;
- do not redesign an API unless import correctness requires a narrow adapter.

### 1.3 Architecture and hardening rule

At a declared architecture/hardening checkpoint:

> **Existing internal behavior may be changed when there is a concrete correctness, resilience, debuggability, or performance benefit.**

Such a change must:

1. have a written intended contract;
2. have tests for the new behavior before or with implementation;
3. be reviewed as behavior work, not disguised as file movement;
4. have focused live validation when the risk justifies it;
5. become the new baseline for subsequent extraction once accepted.

### 1.4 Compatibility rule

Public entry points remain stable where doing so is inexpensive.

`utils/cloud_sync.py` should remain a compatibility facade used by production code, UI code, scripts, tools, and external tests while internals move.

The architectural success criterion is:

> **`cloud_sync.py` is boring.**

It is not necessary for the file to disappear.

### 1.5 Safety rule

The following remain strict unless separately and explicitly redesigned:

1. Local SQLite remains authoritative for whether individual image bytes are desired in Sporely Cloud.
2. Explicit checkbox/context-menu removal is the source of cloud image deletion intent.
3. Omission, filtering, preparation failure, missing files, or partial reads are never deletion intent.
4. Verified local `observations.cloud_id` and `images.cloud_id` are primary push identities.
5. Remote `desktop_id` is recovery identity only.
6. Identity disagreement or ambiguity fails closed; it never falls through to POST.
7. Metadata-only microscope anchors are valid cloud rows and must not be treated as broken uploads.
8. Pull-only mode performs zero cloud writes. A blocked write attempt is a bug, not a successful safety outcome.
9. Partial or bounded remote collections are never authoritative.
10. Paginated reads require deterministic ordering.
11. Required work failure leaves the observation retryable and visible in the sync result.
12. Real concurrent edits still trigger review; representation differences alone do not.
13. Tombstone flush ordering relative to dirty-observation pruning remains load-bearing.
14. Cloud recovery-cache files are remote-owned and their bytes are never re-uploaded.
15. Snapshot persistence means “known-good agreed baseline,” not “whatever state we happened to read.”

---

## 2. Intended end-state architecture

Do not treat this exact file tree as sacred; ownership boundaries matter more than file count.

```text
utils/
    cloud_sync.py                    # stable compatibility facade

    cloud_sync_impl/
        __init__.py

        # cross-cutting infrastructure
        errors.py
        profiling.py
        progress.py
        summary.py

        # remote boundary
        transport.py
        pagination.py
        pull_only.py

        # state / policy
        image_policy.py
        tombstones.py
        snapshots.py
        conflicts.py

        # domain owners
        calibrations.py
        measurements.py
        image_identity.py
        images.py
        anchors.py

        # reconciliation: pure classification, not side effects
        reconciliation/
            __init__.py
            types.py
            observation.py
            images.py
            measurements.py

        # execution / coordination
        observation_coordinator.py
        push_orchestration.py
        pull_orchestration.py
        orchestration.py
```

Existing sibling modules such as:

```text
utils/cloud_media_policy.py
utils/original_sync_policy.py
utils/cloud_media_recovery.py
utils/cloud_media_audit.py
utils/cloud_spore_mosaic.py
utils/cloud_spore_mosaic_backfill.py
utils/spore_summary_sync.py
utils/r2_storage.py
```

remain sibling owners unless a stage finds a concrete reason to move a narrow piece of sync glue. Do not pull already coherent subsystems into `cloud_sync_impl/` merely to make one tree look complete.

External publishing to Artsobservasjoner, Artportalen, iNaturalist, Mushroom Observer, etc. is a **parallel integration subsystem**, not part of this refactor.

---

## 3. Public API / facade strategy

Do **not** begin by replacing `utils/cloud_sync.py` with `utils/cloud_sync/__init__.py`.

During and after the refactor, prefer:

```text
utils/cloud_sync.py
```

as a stable public facade.

It may:

- re-export public exceptions and dataclasses;
- expose `sync_all`, `push_all`, `pull_all`, and other intentionally public helpers;
- contain thin compatibility wrappers;
- translate new internal structured results into legacy result dictionaries where required;
- preserve existing import paths while callers migrate gradually.

Do not keep duplicate mutable state or fake globals in the facade merely to satisfy old monkeypatch targets.

A likely acceptable final size is roughly 100-300 lines, but line count is not itself an acceptance criterion.

---

## 4. Test and monkeypatch strategy

A re-export is sufficient for ordinary imports but not necessarily for monkeypatching.

A test that patches:

```python
utils.cloud_sync.some_helper
```

will not affect implementation that has already imported `some_helper` into:

```python
utils.cloud_sync_impl.images
```

Therefore:

- preserve production imports from `utils.cloud_sync` throughout the refactor;
- inventory tests that monkeypatch `utils.cloud_sync` internals;
- as ownership moves, retarget tests to patch the owning module;
- retain explicit public API/import compatibility tests;
- do not maintain duplicate mutable globals solely for old monkeypatch behavior;
- prefer owner-module patching or dependency injection for new tests;
- when behavior is intentionally hardened, update tests that encode accidental old behavior only with explicit review.

---

# Pre-stage — Freeze the truth before moving code

**Risk:** low. Required before Stage 0.

## A. Documentation bookkeeping

- Treat E1c Stage 4 dead-code cleanup as completed historical work; commit `919b3e7` removed confirmed-dead helpers and the duplicate module-scope deleted-observation prompt.
- Remove stale references to the retired observation-level image-storage sentinel and sparse-default initialization model where they remain.
- Make `docs/cloud-sync-architecture.md` describe current implementation truth before the refactor begins.
- Explicitly document that the current implementation still stamps an observation `synced` early and compensates for required child failures by re-dirtying.
- Link `docs/cloud-sync-architecture.md` to this plan.
- Remove or clearly deprecate older embedded extraction-plan prose.

## B. Establish the test baseline

Before structural movement:

- run focused sync safety suites;
- run the broader cloud-sync suite;
- record exact failures;
- distinguish genuine baseline failures from regressions;
- fix or intentionally quarantine stale failures before extraction where practical.

A structural refactor must not begin from an ambiguous red baseline.

## C. Import and monkeypatch inventory

Create a simple inventory of:

- production imports from `utils.cloud_sync`;
- scripts/tools importing internal helpers;
- tests monkeypatching `utils.cloud_sync`;
- dynamic or string-based references;
- public symbols relied upon by external tooling.

This becomes the compatibility checklist for every later stage.

## D. Freeze known current debt

Record, but do not fix during the pre-stage:

- early synced stamping with re-dirty compensation;
- string-based issue categorization;
- duplicated push/pull reconciliation logic;
- unnecessary no-op remote writes where still present;
- deeply distributed ownership of sync-state mutation;
- any known anchor reservation/adoption risks already documented elsewhere.

These items feed Stage 6.5 and Stage 8.

---

# Stage 0 — Leaf infrastructure

**Risk:** very low.  
**Mode:** mechanical extraction.

Move only cross-cutting leaf infrastructure:

- `CloudSyncError` family and related classifiers/constants -> `errors.py`;
- `CloudSyncProfiler`, phase scopes, timing helpers -> `profiling.py`;
- progress phase/state helpers -> `progress.py`;
- sync summary/result bookkeeping with no entity policy -> `summary.py` if the boundary is clean.

Keep `utils/cloud_sync.py` as facade/re-export layer.

## Do not change

- error text;
- summary key names;
- progress phase names;
- exception hierarchy;
- logging semantics;
- UI-visible result behavior.

## Gate

- compile touched modules;
- import compatibility tests;
- profiler/progress tests;
- focused sync smoke suite;
- broader cloud-sync safety suite;
- no production behavior diff expected.

---

# Stage 1 — Transport, pagination, and pull-only boundary

**Risk:** low.  
**Mode:** mechanical extraction.

Move transport-only concerns:

- request/session refresh plumbing;
- `_get`, `_post`, `_patch`, `_delete`, `_rpc`, storage remove;
- `_get_paginated` and deterministic-pagination helpers;
- read-only/get helpers carrying no sync policy;
- `PullOnlyCloudClient`;
- `PullOnlyModeError`;
- blocked-write reporting.

Recommended modules:

```text
transport.py
pagination.py
pull_only.py
```

## Pull-only registry ownership

Keep writer/read classification in one explicit client-contract registry adjacent to the remote boundary.

Add a test that every public client method used by sync is classified as read or write where relevant.

New writer methods should fail a test until explicitly classified.

Do not duplicate allow/block registries across modules.

## Do not change

- HTTP headers / `Prefer` semantics;
- authentication refresh behavior;
- pagination ordering;
- retry behavior;
- pull-only allow/block behavior;
- request timeout semantics.

## Gate

- `tests/test_cloud_download_only.py` in full;
- pagination tests;
- fast-path tests;
- broader cloud-sync safety suite.

---

# Stage 2 — Image storage policy and per-image intent ledger

**Risk:** low-medium.  
**Mode:** mechanical extraction.

Move canonical current policy:

- `cloud_image_bytes_desired`;
- `should_push_local_image_to_cloud`;
- `should_pull_cloud_image_to_desktop`;
- pure metadata-only anchor predicates;
- storage excluded-set accessors;
- per-image storage-intent ledger accessors;
- `_ensure_cloud_image_storage_intent_initialized`;
- pure/default-selection helpers;
- compatibility alias `_initialize_cloud_image_storage_desired_state_for_observation` only if externally referenced.

Recommended module:

```text
image_policy.py
```

## Preserve exactly

- ledger key format `sporely_cloud_image_storage_intent_ids_<obs>`;
- only ledger membership proves initialized intent;
- tombstoned -> excluded;
- field -> desired;
- new member of an initialized microscope group -> local-only;
- genuinely new/legacy group -> deterministic keeper behavior;
- explicit checkbox decisions are never reseeded;
- no legacy Artsobservasjoner exclusion migration;
- initializer performs zero cloud I/O.

## Gate

- storage-intent suite;
- desired-byte tests;
- metadata-only tests;
- dirty-pending-image tests;
- checkbox-policy tests;
- broader cloud-sync safety suite.

---

# Stage 3 — Tombstone lifecycle

**Risk:** medium.  
**Mode:** mechanical extraction.

Move the complete tombstone lifecycle together:

- queue/cancel helpers;
- local tombstone lookup helpers;
- `_push_pending_image_tombstones`;
- `_record_remote_image_tombstones`;
- microscope-anchor tombstone cancellation;
- tombstone warning/state helpers;
- checkbox transition support that directly owns tombstone state, where the dependency boundary is clean.

Recommended module:

```text
tombstones.py
```

## Load-bearing ordering

Preserve:

```text
pending tombstone flush
    BEFORE
local dirty-observation pruning / candidate processing
```

Explicit checkbox deletion must still converge in the same sync invocation.

## Same-run tombstone behavior

A successful same-run tombstone may change the remote child list and parent `updated_at`.

That may legitimately open deeper comparison.

It must **not** be classified as an unrelated concurrent remote image edit merely because the child list now differs from the previous baseline.

Do not add baseline-pruning cross-store writes as part of this extraction.

## Gate

- `tests/test_image_tombstones.py`;
- gallery checkbox deletion tests;
- same-run tombstone + surviving-image conflict-normalization regression;
- fast-path tests;
- broader cloud-sync safety suite.

---

# Stage 4a — Snapshots and canonical comparison representation

**Risk:** medium-high.  
**Mode:** mechanical extraction.

This stage owns canonical comparison representation, not merely persistence.

Recommended modules:

```text
snapshots.py
conflicts.py
```

Optionally:

```text
image_snapshot.py
```

only if snapshot persistence and conflict analysis genuinely share enough canonical payload logic to justify it.

Move:

- snapshot load/store/parse/clear family;
- snapshot schema/version helpers;
- `_store_remote_snapshot`;
- known-good baseline helpers;
- `_local_image_snapshot_payload`;
- `_remote_image_payload`;
- `_image_metadata_payload`;
- canonical observation/image comparison payload helpers;
- `ObservationPushConflictReport`;
- `_analyze_observation_push_conflicts`;
- local/remote meaningful-change classifiers;
- review-pending markers;
- push-blocked review-origin reporting where ownership is coherent.

## Canonical normalization invariant

Local, baseline, and remote are compared in the same canonical form.

Preserve live-fixed equivalences such as:

- `sample_source` case/canonical form;
- naive/local vs UTC-normalized `captured_at`;
- local `calibration_id` vs cloud `calibration_uuid`;
- absent/missing keys vs `None` semantics where contractually equivalent.

Representation-only difference must not conflict.

Genuine three-way divergence must still conflict.

## Gate

- snapshot persistence suite;
- conflict-preflight suite;
- `tests/test_image_conflict_normalization.py`;
- genuine remote-edit conflict tests;
- push-skipped reporting tests;
- broader cloud-sync safety suite.

---

# Stage 4b — Existing interactive conflict-plan machinery

**Risk:** medium-high.  
**Mode:** mechanical extraction.

Only after 4a is stable, move:

- `build_conflict_plan_baseline`;
- `resolve_conflict_*`;
- `finalize_sync_candidates`;
- `PartialConflictPlanError`;
- conflict-plan execution bookkeeping.

Do not yet redesign the entire conflict UX or execution model.

The Stage 6.5 reconciliation design may later reuse or simplify this machinery, but first extract it cleanly.

## Preserve exactly

- reviewed baseline drift aborts apply;
- partial conflict-plan retries remain idempotent;
- media deletion APIs remain unreachable from conflict plans;
- snapshot-before-final-synced ordering remains strict;
- unresolved review remains dirty/review-pending.

## Gate

- conflict-plan execution suite;
- baseline drift tests;
- partial retry tests;
- no-media-deletion-plan tests;
- snapshot persistence tests;
- broader cloud-sync safety suite.

---

# Stage 5a — Calibrations

**Risk:** medium.  
**Mode:** mechanical extraction.

Move calibration-specific:

- payload logic;
- identity logic;
- push/pull behavior;
- linking;
- repair helpers.

Recommended module:

```text
calibrations.py
```

Do not move generic transport/client machinery with them.

Preserve no-op matching and identity semantics.

## Gate

- `tests/test_cloud_calibration_sync.py`;
- affected pull-only tests;
- affected fast-path tests;
- broader cloud-sync safety suite.

---

# Stage 5b — Measurements

**Risk:** medium.  
**Mode:** mechanical extraction.

Move measurement-specific:

- canonical payload and no-op equivalence helpers;
- identity cache/prefetch;
- per-observation push driver;
- pull/apply helpers;
- remote cleanup helpers;
- reconciliation helpers where measurement ownership is clear.

Recommended module:

```text
measurements.py
```

## Required failure behavior

Preserve current validated contract:

```text
required measurement failure
    -> observation remains/re-becomes retryable
    -> failure appears in result / issue summary
```

Do not reintroduce fire-and-forget measurement exceptions when moving closures/helpers.

## Derived scientific/public products

Spore summaries and mosaics already have sibling modules. Do not force them into `measurements.py`.

During this stage, identify the narrow glue that belongs to cloud-sync orchestration and classify each operation as:

- required for observation completion; or
- best effort / externally recoverable.

That classification becomes an input to Stage 6.5.

## Gate

- `tests/test_cloud_measurement_sync_v1.py`;
- `tests/test_sync_observation_dirty_propagation.py` measurement integration;
- dirty-loop tests;
- fast-path tests;
- spore summary/mosaic tests where affected;
- broader cloud-sync safety suite.

---

# Stage 6a — Image identity and metadata mechanics

**Risk:** medium-high.  
**Mode:** mechanical extraction.

Recommended module:

```text
image_identity.py
```

Move or wrap:

- `_resolve_existing_image_for_push`;
- `_find_cloud_image`;
- `ImageIdentityConflictError` if not already in `errors.py`;
- lost-link reconciliation helpers;
- canonical image metadata PATCH/POST identity decision support.

## Preserve exactly

- verified local `images.cloud_id` direct leg is primary;
- remote `desktop_id` is recovery only;
- observation ownership compares canonical ID values (`817` == `"817"`);
- direct/reverse disagreement fails closed;
- global same-user desktop-id collision guard before POST;
- soft-deleted rows do not become ordinary existing identities;
- 23505 race -> identity conflict;
- no silent reparenting;
- no POST fallback on identity conflict.

## Gate

`tests/test_image_push_identity.py` in full plus the broader sync safety suite.

---

# Stage 6b — Image push, pull, preparation, and materialization

**Risk:** high.  
**Mode:** mechanical extraction.

Move ordinary byte/metadata mechanics to:

```text
images.py
```

Likely scope:

- `_push_images_for_observation` core mechanics;
- preparation candidate handling;
- upload result handling;
- remote metadata application;
- download/materialization/localization helpers;
- media signatures closely tied to image synchronization;
- recovery-cache byte guards.

## Preserve ordering inside image push

At minimum:

```text
intent initialization
-> identity/link reconciliation
-> tombstone/protection filtering
-> preparation
-> metadata reserve/create as required
-> byte upload
-> metadata finalize
-> local cloud_id bookkeeping
```

Do not let `prepared_items` become the authoritative desired-state set.

## Preserve failure propagation

Per-image failures must:

- make the image phase unsuccessful;
- keep/re-mark the observation retryable under the current baseline;
- appear in summary/error output;
- remain retryable without duplicate creation.

Stage 6.5 will redesign **who owns the final observation state transition**; do not do that during this move.

## Gate

- image upload policy;
- dirty pending images;
- visibility phase7;
- media pull retry;
- original sync/recovery;
- dirty-loop;
- image identity;
- retry-propagation suites;
- broader cloud-sync safety suite.

---

# Stage 6c — Metadata-only anchors and promotion/rollback

**Risk:** high.  
**Mode:** mechanical extraction.

Recommended module:

```text
anchors.py
```

Move:

- metadata-only microscope anchor ensure helpers;
- public-spore anchor orchestration;
- reserve/release storage-path promotion methods or wrappers;
- local promotion pending-marker helpers;
- promotion rollback logic;
- protected-anchor decisions that are not pure image policy.

## Preserve exactly

- promotion uses the existing cloud image row; no replacement POST;
- pending marker is written before reserve PATCH;
- reserve is conditional on `storage_path IS NULL`;
- upload failure removes partial objects and conditionally releases only the exact reserved key;
- `None` upload return counts as failure;
- reserved-but-unconfirmed storage path is never trusted as proof of bytes;
- pull-only blocks reserve/release writers;
- anchor `ImageIdentityConflictError` propagates and is not swallowed into success.

## Known residual risks

Do not fix cross-device reservation-adoption or dangling-reservation risks while moving code unless separately approved as explicit behavior work.

Record them for Stage 8.

## Gate

- `tests/test_cloud_anchor_promotion.py`;
- metadata-only suites;
- spore-mosaic anchor tests;
- retry propagation;
- broader cloud-sync safety suite.

---

# Stage 6.5 — Orchestration architecture checkpoint

**Risk:** design-critical.  
**Mode:** explicit architecture design.  
**Rule:** **Do not move `push_all`, `pull_all`, or `sync_all` yet.**

Stages 0-6 create ownership boundaries. Stage 6.5 decides what the final state machine should actually be.

This checkpoint is mandatory.

## 6.5a — Define typed internal issues

Core sync logic should no longer need to infer machine state by parsing human-readable error strings.

Introduce an internal typed issue model, conceptually:

```python
@dataclass(frozen=True)
class SyncIssue:
    kind: SyncIssueKind
    phase: SyncPhase
    observation_id: int | None = None
    cloud_id: str | None = None
    reason: str = ""
    retryable: bool = False
    details: dict[str, object] | None = None
```

Possible `kind` values may include:

```text
conflict
blocked
retryable
error
warning
```

Exact names are design-time decisions.

Human-readable strings should be produced at the UI/report boundary.

Legacy `result["errors"]` output may remain for compatibility, generated from structured issues.

## 6.5b — Define typed operation outcomes

Domain executors should increasingly report facts rather than silently deciding global observation state.

Conceptually:

```python
@dataclass
class OperationOutcome:
    changed_local: bool = False
    changed_remote: bool = False
    retry_required: bool = False
    review_required: bool = False
    issues: list[SyncIssue] = field(default_factory=list)
```

And at observation scope:

```python
@dataclass
class ObservationSyncOutcome:
    changed_local: bool = False
    changed_remote: bool = False
    retry_required: bool = False
    review_required: bool = False
    snapshot_safe: bool = False
    issues: list[SyncIssue] = field(default_factory=list)
```

Do not over-design the type hierarchy. Prefer a few stable structures over dozens of tiny result classes.

## 6.5c — Define reconciliation as pure classification

Push and pull currently contain overlapping change-classification logic.

The new architecture should share a common reconciliation brain without creating a new god module.

Target:

```text
local state
remote state
last agreed snapshot
       │
       ▼
pure classification
       │
       ▼
ReconciliationPlan
       │
       ▼
side-effect executors
```

Conceptually:

```python
plan = reconcile(local, remote, snapshot)
```

returning something like:

```python
ReconciliationPlan(
    observation_action=...,
    image_actions=[...],
    measurement_actions=[...],
    conflicts=[...],
    warnings=[...],
)
```

The plan describes **what should happen**.

It does not:

- perform HTTP requests;
- write SQLite;
- stamp sync status;
- format UI messages;
- silently execute conflict decisions.

## 6.5d — Keep reconciliation modular

Do not create a giant `reconciliation.py`.

Prefer focused pure modules, for example:

```text
reconciliation/
    types.py
    observation.py
    images.py
    measurements.py
```

If a smaller flat module set is clearer, use that instead.

The important property is:

> reconciliation modules classify; executor modules mutate.

## 6.5e — Define one owner for observation completion

Introduce an `observation_coordinator.py` or equivalent ownership boundary.

It owns:

- the observation-level reconciliation plan;
- execution ordering;
- aggregation of required child outcomes;
- whether snapshot persistence is safe;
- the final observation completion decision;
- final `sync_status` transition.

Low-level image/measurement helpers should not be the final authority on whether the whole observation is synced.

## 6.5f — Make `synced` a final commit point

This is an explicit intended behavior change.

Current accidental model:

```text
dirty
  -> push observation
  -> mark synced
  -> child operation fails
  -> mark dirty again
```

Target model:

```text
dirty
  -> observation action succeeds
  -> required image / anchor work succeeds
  -> required measurement work succeeds
  -> required derived work succeeds
  -> final known-good remote state is established
  -> snapshot is stored successfully
  -> mark synced
```

Failure before the final point leaves the observation unfinished/retryable.

Do not add a larger public sync-state enum unless needed. The coordinator may hold internal in-progress state without persisting it.

## 6.5g — Define required versus best-effort work

For every child/derived operation, explicitly classify failure semantics.

Example categories:

### Required for observation completion

Potentially includes, according to the accepted existing contract:

- observation persistence;
- required image identity/metadata/byte state;
- anchor operations that are required for the requested sync;
- required measurement state;
- structured spore summary where the existing contract treats failure as retryable;
- successful known-good snapshot persistence.

### Best effort / non-blocking

Examples may include:

- public spore mosaic generation where existing fallback behavior makes it non-blocking;
- diagnostics;
- profiling;
- optional cache improvement.

Do not guess. Audit current tests and contract before finalizing this list.

## 6.5h — Define retry semantics centrally

Executor results report failure/retry state.

Prefer:

```python
result = image_executor.apply(plan.image_actions)
```

returning structured outcome.

Avoid adding new deep paths that do:

```python
mark_observation_dirty(...)
```

as an implicit side effect.

Existing deep state mutations can remain temporarily during transition, but the target architecture has one authoritative observation completion owner.

## 6.5i — Define snapshot ownership

A persisted snapshot means:

> this complete state is accepted as the new shared baseline.

Therefore snapshot storage must not occur after:

- truncated/partial reads;
- unresolved conflicts;
- incomplete required child work;
- failed materialization when materialization is required;
- ambiguous identity;
- partially applied conflict plans.

Snapshot failure must prevent the final synced commit.

## 6.5j — Define candidate selection versus reconciliation

Preserve an important distinction:

- **candidate selection** decides which observations need expensive inspection;
- **reconciliation** decides what the inspected state means.

Fast-path pruning, child-change cursors, and remote head comparisons may remain optimized candidate selectors.

Do not force every no-op observation through a full three-way deep fetch merely to make the design conceptually pure.

## Stage 6.5 deliverables

Before Stage 7 implementation:

1. documented state machine;
2. dependency diagram;
3. typed issue definitions;
4. typed outcome definitions;
5. reconciliation-plan definition;
6. explicit required/best-effort operation table;
7. owner for final `sync_status`;
8. owner for snapshot persistence;
9. test plan for new desired semantics;
10. explicit list of current tests that encode accidental behavior and therefore need reviewed updates.

## Gate

- architecture/design review completed;
- no new god module in the dependency graph;
- pure reconciliation tests written;
- final synced-commit tests written;
- typed issue/result compatibility strategy tested;
- implementation has not yet silently changed production behavior.

---

# Stage 7 — Replace the old orchestration behind the facade

**Risk:** high.  
**Mode:** explicit architecture + behavior hardening.

Stage 7 is not “move three giant functions to new files.”

It replaces the old orchestration with the accepted Stage 6.5 model while keeping the public API stable.

---

## Stage 7a — Observation coordinator and final commit semantics

Recommended module:

```text
observation_coordinator.py
```

Implement one observation-level coordinator that:

1. obtains normalized local / remote / baseline state;
2. requests a pure reconciliation plan;
3. dispatches side effects to domain owners;
4. aggregates typed outcomes;
5. decides whether review/retry remains;
6. persists the final known-good snapshot;
7. stamps `synced` only after successful completion.

### Required deliberate behavior hardening

Replace:

```text
dirty -> synced -> child failure -> dirty
```

with:

```text
dirty -> required work -> snapshot -> synced
```

This must land as an explicitly reviewed behavior change with dedicated tests.

### Gate

- new final-commit tests;
- `tests/test_sync_observation_dirty_propagation.py`;
- snapshot tests;
- conflict preflight/plan tests;
- image identity tests;
- measurement/calibration tests;
- retry tests;
- live canary if the test boundary cannot prove enough.

---

## Stage 7b — Push executor

Recommended module:

```text
push_orchestration.py
```

Its role is now to execute push actions that the reconciliation/coordinator layer has already classified.

Responsibilities:

- observation writes;
- image/anchor writes;
- measurement writes;
- required summary writes;
- collect typed outcomes;
- respect identity/deletion/storage policy;
- preserve special privacy/plan-specific error semantics where still part of the contract.

It should **not** own:

- an independent three-way conflict model;
- UI error formatting;
- final synced commit;
- snapshot acceptance policy.

### Preserve

- tombstone flush ordering;
- identity fail-closed behavior;
- image-storage intent;
- retryability;
- no-op write suppression where required by remote trigger semantics.

### Gate

- dirty propagation;
- fast path;
- dirty-loop;
- conflict tests;
- image identity;
- image upload policy;
- measurement/calibration;
- summary/mosaic tests where affected.

---

## Stage 7c — Pull executor

Recommended module:

```text
pull_orchestration.py
```

Its role is to execute remote-to-local actions from the reconciliation plan.

Preserve:

- fast-pull candidate pruning;
- periodic child-safety reconciliation;
- full-pull semantics;
- metadata-only apply independent of byte materialization;
- missing/failed work remains retryable;
- complete paginated collections before interpreting absence;
- pull-only source gating;
- protection of larger/better local originals;
- remote deletion never silently deletes local originals.

It should not maintain a second independent definition of conflict.

### Gate

- download-only;
- metadata-only;
- fast path;
- child-change probe/cursor;
- snapshot tests;
- media-pull retry;
- remote deletion review behavior;
- broader cloud-sync safety suite.

---

## Stage 7d — Structured issue pipeline

Replace internal string categorization with structured issues.

Target flow:

```text
domain executor
    -> SyncIssue / OperationOutcome
    -> observation coordinator
    -> sync result assembler
    -> UI compatibility formatter
```

For compatibility, `sync_all()` may still return:

```python
{
    "pushed": ...,
    "pulled": ...,
    "errors": [...],
    ...
}
```

while also carrying structured data internally or under a new additive field.

Do not break the UI merely to get typed internals.

Update `summarize_sync_issues()` so it consumes structured issue data where available rather than reparsing formatted strings.

Temporary legacy parsing may remain only as a compatibility bridge.

### Gate

- issue categorization tests;
- UI summary tests;
- privacy blocked tests;
- plan-limit retry tests;
- conflict-count tests;
- raw legacy error compatibility tests where still needed.

---

## Stage 7e — Top-level orchestration

Recommended module:

```text
orchestration.py
```

Only after 7a-7d are stable, extract/replace `sync_all`.

The final `sync_all` should remain thin:

```text
validate account binding
-> load required remote heads
-> push/pull calibrations as required
-> select observation candidates
-> reconcile + execute observations
-> handle child-change safety cursors
-> assemble result
```

Preserve caller-mode rules for:

- `sync_images`;
- `materialize_remote_images`;
- `full_pull`;
- `child_safety_pull`;
- `pull_only`.

Do not “turn everything on” to simplify orchestration.

### Gate

- full cloud-sync safety suite;
- caller-mode tests;
- pull-only tests;
- fast/no-op tests;
- startup/refresh tests;
- sync-now tests;
- child-change cursor tests;
- live canary.

---

# Stage 8 — Hardening and simplification pass

**Risk:** medium-high.  
**Mode:** explicit behavior/architecture hardening only.

This stage exists because some improvements are unsafe to mix into mechanical extraction and easier to evaluate after the new coordinator exists.

Each hardening item should land separately when practical.

## 8a — Eliminate duplicate reconciliation semantics

Audit push and pull for duplicated rules around:

```text
local-only change
remote-only change
both changed, disjoint
both changed, overlapping
representation-only difference
remote deletion
no meaningful change
```

Move classification into canonical pure reconciliation owners.

Do not create convenience shortcuts that bypass the canonical classifier.

## 8b — Remove avoidable no-op remote writes

Remote UPDATE triggers can make apparently idempotent writes observable.

Audit and suppress remote writes that make no semantic change where doing so is safe.

Especially protect:

- `updated_at`-driven child-change cursors;
- reverse-link healing;
- image metadata;
- measurement upserts;
- calibration metadata.

No-op suppression must not weaken required repair behavior.

## 8c — Improve diagnostics

An observation failure should be explainable without reconstructing behavior from scattered print statements.

Target diagnostic shape:

```text
observation 817
  candidate reason:
      local dirty + remote child changed

  reconciliation:
      local notes changed
      remote image added

  plan:
      push notes
      pull image

  execution:
      pull image: success
      push notes: timeout

  final:
      retryable
      snapshot not advanced
      sync_status remains dirty
```

Keep profiling/logging optional where appropriate, but make result state explicit enough for deterministic tests.

## 8d — Audit hidden state mutation

Inventory every call to:

- `mark_observation_dirty`;
- `mark_observation_media_dirty`;
- `_stamp_observation_synced`;
- `update_observation_sync_state`;
- snapshot write/clear helpers;
- conflict-review pending markers.

Expected end state:

- a small number of authoritative owners;
- domain executors report outcomes;
- coordinator owns final completion;
- direct SQL sync-status writes are exceptional and documented.

## 8e — Reassess known anchor reservation risks

Only now, if still justified, separately consider the documented:

- cross-device reservation-adoption risk;
- dangling reservation risk.

Do not fold such changes into unrelated cleanup.

## Gate

For each hardening patch:

- focused tests;
- broader safety suite;
- review for changed contract;
- live canary when remote-state semantics are affected.

---

# Stage 9 — Optional client split

**Risk:** high.  
**Mode:** optional.

Only after Stages 0-8 have landed and stabilized, reassess `SporelyCloudClient`.

Possible end state:

```text
authenticated transport client
observation remote service
image remote service
measurement remote service
calibration remote service
```

Compatibility methods may remain on `SporelyCloudClient` where tools rely on them.

Do not split client and orchestration in the same commit.

If the remaining client is understandable, stop.

A smaller file count is not a reason to keep refactoring.

---

# Stage 10 — Compatibility facade review

**Risk:** low-medium.  
**Mode:** optional cleanup.

Default decision:

> **Keep `utils/cloud_sync.py` permanently as a facade unless it causes a concrete maintenance problem.**

Possible final form:

```python
from utils.cloud_sync_impl.errors import ...
from utils.cloud_sync_impl.orchestration import sync_all
from utils.cloud_sync_impl.push_orchestration import push_all
from utils.cloud_sync_impl.pull_orchestration import pull_all
...
```

Plus narrow compatibility wrappers where justified.

Only consider replacing the file with `utils/cloud_sync/__init__.py` if:

- production imports no longer care;
- tooling/tests do not depend on module identity;
- there is a measurable maintenance benefit.

Removing the facade is not a project goal.

---

# Per-stage execution protocol

## Mechanical extraction stages (0-6)

1. Confirm clean git status and exact HEAD.
2. Read `AGENTS.md`, `docs/supabase-sync-contract.md`, `docs/cloud-sync-architecture.md`, and this plan.
3. Identify exact symbols to move and all import/monkeypatch consumers.
4. Run focused baseline tests **before** editing.
5. Move code mechanically.
6. Do not rename/rewrite unless required for import correctness.
7. Preserve facade exports.
8. Compile touched modules.
9. Run focused tests.
10. Run broader cloud-sync safety suite.
11. Compare logs/result structures if progress/errors/summaries are touched.
12. Review for accidental policy changes, especially deletion, identity, pull-only, snapshots, and retry state.
13. Update architecture ownership/navigation docs.
14. Commit one stage only.
15. Do not begin the next stage in the same context until the current stage is green and reviewed.

## Architecture/hardening stages (6.5-8)

1. Start from green baseline.
2. State the intended behavior change explicitly.
3. Define the new contract before implementation.
4. Add or update tests for the intended behavior.
5. Implement behind stable public entry points.
6. Do not bundle unrelated cleanup.
7. Review architecture and behavior separately.
8. Run focused tests.
9. Run full cloud-sync safety suite.
10. Compare reconciliation report before/after when remote state may change.
11. Perform live canary at meaningful risk boundaries.
12. Update architecture docs to describe the new accepted baseline.
13. Land one conceptual behavior change per commit when practical.

---

# Required validation matrix

At minimum, keep these areas green across the project:

- pull-only / zero write;
- pagination and partial-read safety;
- observation identity;
- image identity;
- storage-intent ledger;
- checkbox/tombstone lifecycle;
- metadata-only anchors;
- anchor promotion rollback;
- image conflict normalization;
- snapshot persistence;
- conflict preflight;
- conflict plan;
- final synced-commit semantics;
- image/measurement failure retry propagation;
- typed issue categorization;
- fast no-op path;
- dirty-loop steady state;
- child-change cursor;
- measurements;
- calibrations;
- image upload policy;
- media pull retry;
- original upload/recovery;
- spore mosaic / summary behavior where affected;
- remote deletion safety;
- UI sync summary compatibility;
- public import compatibility.

---

# Live-canary policy

Do not live-canary every mechanical file move.

Use live validation at meaningful boundaries, for example:

- behavior fix required before extraction;
- tombstone or identity ownership change if tests cannot fully prove the integration boundary;
- final synced-commit redesign;
- push/pull executor replacement;
- top-level orchestration replacement;
- client split;
- remote write-suppression changes.

Before a live canary:

- make a fresh SQLite backup;
- run the read-only reconciliation report;
- require `C=D1=D2=E=H=0` unless a known reviewed exception is explicitly documented;
- do not mix canary validation with cleanup or garbage collection;
- record the exact account/database/environment used;
- compare post-canary reconciliation results to baseline.

---

# Work explicitly out of scope

Do not combine this refactor with:

- E3 R2 garbage collection;
- repair of the one known `G_conflicting_intent` row;
- historical duplicate-observation cleanup;
- historical duplicate-image cleanup;
- standalone migration-tool hardening unless it blocks an extraction stage;
- cloud schema changes not required by an explicitly approved orchestration contract;
- orientation/analysis work;
- UI redesign;
- account-link/reset work;
- broad type-annotation or lint migrations;
- external publishing refactors;
- Artsobservasjoner / Artportalen / iNaturalist / Mushroom Observer uploader redesign.

---

# Definition of done

The refactor is complete when all of the following are true.

## Compatibility

- `utils/cloud_sync.py` is a small stable compatibility surface.
- Public production imports remain stable or have an explicit migration.
- Legacy result dictionaries remain supported where callers still need them.
- No fake duplicate mutable state exists solely for facade compatibility.

## Ownership

- transport contains transport, not sync policy;
- pagination has one authoritative implementation;
- pull-only enforcement has one authoritative writer/read contract;
- storage intent has a clear owner;
- tombstones have a clear owner;
- snapshots have a clear owner;
- conflicts/reconciliation have clear owners;
- calibrations have a clear owner;
- measurements have a clear owner;
- image identity has a clear owner;
- image mechanics have a clear owner;
- anchors have a clear owner;
- observation completion has one authoritative coordinator.

## Reconciliation architecture

- reconciliation is primarily pure classification;
- reconciliation is split into focused modules rather than becoming a new monolith;
- push and pull use the same canonical change/conflict semantics;
- candidate selection remains independently optimized;
- executors perform side effects from explicit decisions/plans.

## State and retry semantics

- `sync_status='synced'` is written only at the final successful observation commit point;
- required child failure leaves the observation retryable;
- snapshot failure prevents final synced commit;
- unresolved conflicts do not advance the accepted baseline;
- deep helpers do not independently decide final observation completion.

## Snapshot semantics

- snapshots are written only from complete, known-good remote state;
- truncated/partial reads never become baselines;
- representation-only differences do not create false conflicts;
- genuine concurrent edits remain reviewable.

## Structured diagnostics

- internal issue categorization is structured rather than dependent on reparsing UI/log strings;
- a failed observation can be explained deterministically in terms of candidate reason, reconciliation decision, execution result, and final state;
- legacy string output exists only as a compatibility/UI representation where still needed.

## Safety

- identity disagreement remains fail-closed;
- explicit deletion intent remains the only source of routine cloud image deletion;
- pull-only performs zero cloud writes;
- local originals remain protected from cloud-side disappearance or lower-quality recovery copies;
- cloud recovery-cache bytes are never re-uploaded;
- pagination/partial-read safeguards remain intact.

## Validation

- cloud-sync safety suite is green against the accepted final baseline;
- public import compatibility tests are green;
- final synced-commit tests are green;
- structured issue/result tests are green;
- no new reconciliation anomalies appear in the final live canary;
- architecture docs point to the new owning modules.

---

# Anti-goals

The following do **not** count as success:

- `cloud_sync.py` has merely been split into many files while preserving the same implicit state machine;
- push and pull still contain competing definitions of conflict;
- a new `reconciliation.py` becomes another several-thousand-line god module;
- every domain helper can still mutate observation sync state independently;
- `synced` still means “we started successfully and hope compensation catches later failures”;
- debugging still requires reconstructing state from print output;
- the facade is removed solely to reduce file count;
- external publishing is pulled into the cloud-sync subsystem merely because both use observation/images data.

---

# Final target in one sentence

> **A boring compatibility facade over focused domain owners, pure reconciliation logic, explicit side-effect executors, and a single observation coordinator that commits `synced` only when the whole required sync transaction is actually complete.**
