# Cloud Sync Extraction Plan

Status: authoritative planning document for the staged decomposition of `utils/cloud_sync.py`.

## Agent handoff

- Status: Active; no extraction stage is verified as implemented.
- Last completed stage: Pre-existing E1c dead-code cleanup, commit `919b3e7` (a prerequisite, not an extraction stage).
- Current/next stage: Pre-stage inventory and baseline, then Stage 0.
- Relevant commits: `919b3e7`, `de824a4`.
- Important decisions: Preserve `utils.cloud_sync` as the compatibility facade and move one ownership boundary at a time without behavior changes.
- Do not: Mix extraction with sync behavior redesign, E3 garbage collection, schema changes, or UI work.
- Remaining acceptance criteria: The definition of done and per-stage validation matrix below.

This document supersedes the older **“Proposed staged extraction plan”** formerly embedded in `docs/cloud-sync-architecture.md`. The architecture document remains the behavioral/navigation map and now links here for extraction work.

The goal is not to redesign sync. The goal is to split the current monolith into smaller ownership modules while preserving the public import surface, sync semantics, retry behavior, identity rules, deletion rules, pull-only guarantees, and current live-validated behavior.

## Why the plan was revised

The original extraction plan was sound in broad structure but became stale during the August 2026 sync-hardening work. In particular:

- the old observation-level image-storage initialization sentinel was replaced by a per-image storage-intent ledger;
- image push identity now has a canonical two-leg resolver (`cloud_id` primary, `desktop_id` recovery);
- image/anchor push failures and measurement push failures now propagate back to observation dirty state and the sync issue summary;
- image conflict analysis now depends on canonical normalization of local, baseline, and remote payloads;
- same-run tombstone changes are live-validated not to create false image conflicts after canonical normalization;
- the current implementation still stamps an observation `synced` early and relies on dirty-mark compensation for required child-operation failures. The architecture docs must not pretend this ordering has already been refactored away;
- the repository has many direct imports and monkeypatches of `utils.cloud_sync`, so converting the module into a package at the start would create unnecessary blast radius;
- the previous E1c dead-code cleanup is already landed and must not be repeated.

## Non-negotiable invariants

Every extraction stage must preserve the existing contract. In particular:

1. Local SQLite remains authoritative for whether individual image bytes are desired in Sporely Cloud.
2. Explicit checkbox/context-menu removal is the source of cloud image deletion intent. Omission, filtering, preparation failure, missing files, or partial reads are never deletion intent.
3. Verified local `observations.cloud_id` and `images.cloud_id` are primary push identities. Remote `desktop_id` is recovery identity only.
4. Identity disagreement or ambiguity must fail closed; it must not fall through to POST.
5. Metadata-only microscope anchors are valid cloud rows and must not be treated as broken uploads.
6. Pull-only mode performs zero cloud writes. A blocked write attempt is a bug, not a successful safety outcome.
7. Partial/bounded remote collections are never authoritative. Paginated reads require deterministic ordering.
8. Required child-operation failures must leave the observation retryable and visible in the sync result.
9. Real concurrent edits must still trigger conflict review. Representation differences alone must not.
10. Tombstone flush ordering relative to dirty-observation pruning is load-bearing and must not change accidentally.
11. Cloud recovery-cache files are remote-owned and their bytes must never be re-uploaded.
12. No extraction stage may mix mechanical movement with behavior cleanup, broad renaming, schema changes, or unrelated refactoring.

## Public API / compatibility strategy

Do **not** begin by replacing `utils/cloud_sync.py` with `utils/cloud_sync/__init__.py`.

During the extraction, keep:

```text
utils/cloud_sync.py
```

as the stable compatibility facade used by production code, tools, scripts, UI code, and older tests. Move implementation into a new internal package, for example:

```text
utils/cloud_sync_impl/
    __init__.py
    errors.py
    profiling.py
    progress.py
    transport.py
    pagination.py
    pull_only.py
    image_policy.py
    tombstones.py
    snapshots.py
    conflicts.py
    calibrations.py
    measurements.py
    image_identity.py
    images.py
    anchors.py
    push_orchestration.py
    pull_orchestration.py
    orchestration.py
```

`utils/cloud_sync.py` re-exports the stable public surface and may temporarily contain wrappers while call sites migrate internally.

Only after the implementation has been split and stabilized should we decide whether converting `utils.cloud_sync` itself into a package buys enough to justify the import churn. That conversion is optional, not a goal.

## Test / monkeypatch strategy

A re-export is sufficient for normal imports but **not necessarily for monkeypatching**. A test that patches `utils.cloud_sync.some_helper` will not affect code that has already imported `some_helper` into `utils.cloud_sync_impl.images`.

Therefore:

- preserve production imports from `utils.cloud_sync` throughout the refactor;
- as each owner moves, update tests that patch internals to patch the new owning module;
- keep public API/import compatibility tests so external call sites do not drift;
- do not maintain fake duplicate mutable globals in the facade solely to keep old monkeypatch targets alive;
- prefer dependency injection or module-owner patching for new tests.

## Pre-stage — Freeze the truth before moving code

Risk: low. Required before Stage 0.

### A. Documentation bookkeeping

- Treat E1c Stage 4 dead-code cleanup as completed historical work; commit `919b3e7` removed the confirmed-dead helpers and the duplicate module-scope deleted-observation prompt.
- Remove stale references to the retired observation-level image-storage sentinel and sparse-default initialization model where they survive in planning prose.
- Make `docs/cloud-sync-architecture.md` accurately describe the **current** retry ordering: the early `sync_status='synced'` stamp still exists and required child failures are currently compensated by re-dirtying. Do not claim that the final synced stamp has already been moved after all child work.
- Link the architecture document to this extraction plan and remove/deprecate its older embedded extraction plan.

### B. Establish the test baseline

Before structural movement:

- run the focused sync safety suites;
- run the broader cloud-sync suite;
- record exact failures and distinguish true baseline failures from regressions;
- preferably fix or intentionally quarantine the known stale failures in `test_cloud_visibility_phase7.py` / `test_cloud_anchor_promotion.py` before extraction so each movement stage can use a clean before/after gate.

A structural refactor should not start from an ambiguous red baseline.

### C. Import and monkeypatch inventory

Create a simple inventory of:

- production imports from `utils.cloud_sync`;
- scripts/tools importing internal helpers;
- tests monkeypatching `utils.cloud_sync` internals;
- dynamic/string-based references if any.

This inventory is the compatibility checklist for every later stage.

### D. Explicitly defer behavior redesign

The early-synced-stamp architecture is known debt. Do **not** silently fix it as part of extraction. Either:

- leave behavior exactly as-is during extraction and schedule a separate ordering hardening change after the split; or
- if it must change first for correctness, do it as a standalone behavior patch with dedicated tests and live canary validation before Stage 0.

Default: leave it unchanged during mechanical extraction.

## Stage 0 — Leaf infrastructure

Risk: very low.

Move only cross-cutting leaf infrastructure:

- `CloudSyncError` family and related error classifiers/constants -> `errors.py`;
- `CloudSyncProfiler`, phase scopes, timing helpers -> `profiling.py`;
- progress phase/state helpers -> `progress.py`;
- sync summary/result bookkeeping that has no entity policy -> `summary.py` if the boundary is clean.

Keep `utils/cloud_sync.py` as facade/re-export layer.

### Do not change

- error text;
- summary key names;
- progress phase names;
- exception hierarchy;
- logging semantics.

### Gate

- compile all touched modules;
- import compatibility test;
- profiler/progress tests;
- focused sync smoke suite;
- no production behavior diff expected.

## Stage 1 — Transport, pagination, and pull-only boundary

Risk: low.

Move transport-only concerns:

- request/session refresh plumbing;
- `_get`, `_post`, `_patch`, `_delete`, `_rpc`, storage remove;
- `_get_paginated` and deterministic-pagination helpers;
- read-only/get helpers that carry no sync policy;
- `PullOnlyCloudClient`, `PullOnlyModeError`, blocked-write reporting.

Recommended modules:

```text
transport.py
pagination.py
pull_only.py
```

### Pull-only registry ownership

The old plan was internally inconsistent about where writer classification should live. Pick one source of truth.

Preferred model:

- keep writer/read classification in one explicit client-contract registry adjacent to the client facade or transport boundary;
- add a test that every public client method used by sync is classified as read or write where relevant;
- new writer methods must fail a test until they are explicitly added to the pull-only blocked set.

Do not duplicate allow/block registries across modules.

### Do not change

- HTTP headers / Prefer semantics;
- authentication refresh behavior;
- pagination ordering;
- retry semantics;
- pull-only allow/block behavior.

### Gate

Run `tests/test_cloud_download_only.py` in full plus pagination and fast-path tests.

## Stage 2 — Image storage policy and per-image intent ledger

Risk: low-medium.

The original plan is stale here. Do **not** move retired sentinel/group-freeze logic as if it were current architecture.

Move the canonical current policy:

- `cloud_image_bytes_desired`;
- `should_push_local_image_to_cloud`;
- `should_pull_cloud_image_to_desktop`;
- metadata-only anchor predicates that are pure classification;
- storage excluded-set accessors;
- per-image storage-intent ledger accessors;
- `_ensure_cloud_image_storage_intent_initialized` and its pure/default-selection helpers;
- the thin compatibility alias `_initialize_cloud_image_storage_desired_state_for_observation` only if still externally referenced.

Recommended module:

```text
image_policy.py
```

### Preserve exactly

- ledger key format `sporely_cloud_image_storage_intent_ids_<obs>`;
- only ledger membership proves initialized intent;
- tombstoned -> excluded;
- field -> desired;
- new member of an already initialized microscope group -> local-only;
- genuinely new/legacy group -> deterministic keeper behavior;
- explicit checkbox decisions are never reseeded;
- no legacy Artsobservasjoner exclusion migration;
- initializer performs zero cloud I/O.

### Gate

Storage-intent, desired-byte, metadata-only, dirty-pending-image, and checkbox policy suites.

## Stage 3 — Tombstone lifecycle

Risk: medium.

Move the complete tombstone lifecycle together:

- queue/cancel helpers;
- local tombstone lookup helpers;
- `_push_pending_image_tombstones`;
- `_record_remote_image_tombstones`;
- microscope-anchor tombstone cancellation;
- tombstone warning/state helpers;
- checkbox transition support that directly owns tombstone state, if the dependency boundary stays clean.

Recommended module:

```text
tombstones.py
```

### Load-bearing ordering

Preserve:

```text
pending tombstone flush
    BEFORE
local dirty-observation pruning / candidate processing
```

The live canary proved that explicit checkbox deletions can converge in the same sync invocation. Do not delay them by a cycle.

### Same-run tombstone behavior

A successful same-run tombstone may change the remote observation's child list and `updated_at`. That is allowed to open the deeper comparison gate. It must **not** be treated as an independent remote edit merely because the baseline child list differs.

Do not add baseline-pruning cross-store writes as part of this extraction.

### Gate

- `tests/test_image_tombstones.py`;
- gallery checkbox deletion tests;
- same-run tombstone + surviving-image conflict-normalization regression;
- fast-path tests.

## Stage 4 — Snapshots, canonical payloads, and conflict analysis

Risk: medium-high.

This stage must own **canonical comparison representation**, not just snapshot storage.

Recommended modules:

```text
snapshots.py
conflicts.py
```

Optionally use a small shared module:

```text
image_snapshot.py
```

if both snapshot persistence and conflict analysis depend on the same canonical payload functions.

Move together:

- snapshot load/store/parse/clear family;
- snapshot schema/version helpers;
- `_store_remote_snapshot` and related known-good baseline logic;
- `_local_image_snapshot_payload`;
- `_remote_image_payload`;
- `_image_metadata_payload` / canonical metadata comparison helpers;
- `ObservationPushConflictReport`;
- `_analyze_observation_push_conflicts`;
- local/remote meaningful-change classifiers;
- review-pending markers;
- push-blocked review-origin reporting used by `summarize_sync_issues` if ownership is coherent.

### Critical normalization invariant

Local, baseline, and remote must be compared in the **same canonical form**.

Preserve the live-fixed equivalences:

- `sample_source` case/canonical form;
- naive/local vs UTC-normalized `captured_at`;
- local `calibration_id` resolving to cloud `calibration_uuid`;
- absent/missing keys and `None` semantics.

A representation-only difference must not conflict. A genuine three-way field divergence must still conflict.

### Stage 4b — Interactive conflict plan

Only after 4a is stable, move:

- `build_conflict_plan_baseline`;
- `resolve_conflict_*`;
- `finalize_sync_candidates`;
- `PartialConflictPlanError`;
- conflict-plan execution bookkeeping.

Do not combine 4a and 4b if the diff becomes hard to review.

### Gate

- snapshot persistence suite;
- conflict-preflight suite;
- conflict-plan execution suite;
- `tests/test_image_conflict_normalization.py`;
- genuine remote-edit conflict tests;
- push-skipped reporting tests.

## Stage 5a — Calibrations

Risk: medium.

Move calibration-specific payload, identity, push/pull, linking, and repair helpers to:

```text
calibrations.py
```

Do not move generic transport/client machinery with them.

Preserve no-op matching and calibration identity semantics.

Gate: `tests/test_cloud_calibration_sync.py` plus affected pull-only/fast-path tests.

## Stage 5b — Measurements

Risk: medium.

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

### Required failure behavior

The recent live-hardening work fixed a path where non-auth measurement push failures were printed but did not affect observation state.

Preserve:

```text
required measurement failure
    -> observation dirty/retryable
    -> failure present in push_all result / issue summary
```

Do not reintroduce fire-and-forget measurement exceptions when moving closures/helpers.

### Gate

- `tests/test_cloud_measurement_sync_v1.py`;
- `tests/test_sync_observation_dirty_propagation.py` measurement integration case;
- dirty-loop / fast-path suites.

## Stage 6a — Image identity and metadata mechanics

Risk: medium-high.

Move image identity as a coherent boundary before moving the entire image pipeline.

Recommended module:

```text
image_identity.py
```

Move or wrap:

- `_resolve_existing_image_for_push`;
- `_find_cloud_image` identity lookup behavior;
- `ImageIdentityConflictError` if not already in `errors.py`;
- lost-link reconciliation helpers;
- canonical image metadata PATCH/POST identity decision support.

### Preserve exactly

- verified local `images.cloud_id` direct leg is primary;
- remote `desktop_id` is recovery only;
- observation ownership must compare canonical ID values (`817` == `"817"`);
- direct/reverse disagreement fails closed;
- global same-user desktop-id collision guard before POST;
- soft-deleted rows do not become ordinary existing identities;
- 23505 race -> identity conflict;
- no silent reparenting;
- no POST fallback on identity conflict.

### Gate

`tests/test_image_push_identity.py` in full.

## Stage 6b — Image push, pull, preparation, and materialization

Risk: high.

Move the ordinary byte/metadata mechanics to:

```text
images.py
```

Likely scope:

- `_push_images_for_observation` core flow;
- preparation candidate handling;
- upload result handling;
- remote metadata application;
- download/materialization/localization helpers;
- media signatures closely tied to image synchronization;
- recovery-cache byte guards.

### Preserve ordering inside image push

At minimum:

```text
intent initialization
-> identity/link reconciliation
-> tombstone/protection filtering
-> preparation
-> metadata reserve/create as required
-> byte upload
-> metadata finalize
-> local cloud_id/sync bookkeeping
```

Do not let `prepared_items` become the authoritative desired-state set.

### Preserve failure propagation

Per-image/anchor failures must:

- make the image phase unsuccessful;
- keep/re-mark the observation dirty;
- appear in summary/error output;
- remain retryable without duplicate creation.

### Gate

Image upload policy, dirty pending images, visibility phase7, media pull retry, original sync, dirty-loop, identity, and retry-propagation suites.

## Stage 6c — Metadata-only anchors and promotion/rollback

Risk: high.

Do not bury anchor promotion in generic upload code. It now has enough state and failure semantics to justify explicit ownership.

Recommended module:

```text
anchors.py
```

Move:

- metadata-only microscope anchor ensure helpers;
- public-spore anchor orchestration;
- reserve/release storage-path promotion methods or their policy wrapper;
- local promotion pending-marker helpers;
- promotion rollback logic;
- protected-anchor decisions that are not pure image policy.

### Preserve exactly

- promotion uses the existing cloud image row; no replacement POST;
- pending marker written before reserve PATCH;
- reserve is conditional on `storage_path IS NULL`;
- upload failure removes partial objects and conditionally releases only the exact reserved key;
- `None` upload return counts as failure;
- reserved-but-unconfirmed storage path is never trusted as proof of bytes;
- pull-only blocks reserve/release writers;
- anchor `ImageIdentityConflictError` propagates; it must not be swallowed into success.

### Known residual risks — do not “fix while moving”

Keep the documented cross-device reservation-adoption and dangling-reservation risks unchanged unless separately approved as behavior work.

### Gate

`tests/test_cloud_anchor_promotion.py`, metadata-only suites, spore-mosaic anchor tests, and retry propagation.

## Stage 7a — Push orchestration

Risk: high.

Only after entity modules are stable, move `push_all` and its orchestration-only helpers to:

```text
push_orchestration.py
```

`push_all` should call stable owner modules rather than contain entity logic.

Do not redesign the early synced stamp during this movement. Preserve current behavior exactly unless a separate behavior patch has already changed it.

### Explicit push failure contract

Retain compensation semantics currently validated by integration tests:

- image/anchor failure -> dirty + error;
- measurement failure -> dirty + error;
- escaping generic CloudSyncError -> dirty + error;
- privacy/plan-specific branches retain their special states.

### Gate

- `tests/test_sync_observation_dirty_propagation.py`;
- fast path;
- dirty-loop steady state;
- conflict preflight;
- image identity;
- measurement/calibration suites.

## Stage 7b — Pull orchestration

Risk: high.

Move `pull_all` and pull candidate orchestration to:

```text
pull_orchestration.py
```

Keep entity application in owner modules.

Preserve:

- fast-pull candidate pruning;
- periodic child-safety reconciliation;
- full-pull semantics;
- metadata-only apply independent from byte materialization;
- missing/failed work remains retryable;
- complete paginated collections before interpreting absence;
- pull-only source gating.

### Gate

Download-only, metadata-only, fast-path, child-safety, snapshot, and media-pull retry suites.

## Stage 7c — Top-level orchestration

Risk: medium-high after 7a/7b are stable.

Move `sync_all` coordination to:

```text
orchestration.py
```

It should remain thin:

```text
account/link validation
-> load remote observation/calibration heads
-> calibration push
-> push orchestration
-> pull orchestration
-> calibration pull
-> summary/result assembly
```

Preserve caller-mode rules for `sync_images`, `materialize_remote_images`, `full_pull`, and `pull_only` exactly.

## Stage 8 — Optional client split

Risk: high; optional.

Only after all previous stages have landed and stabilized, reassess `SporelyCloudClient`.

Possible end state:

- thin authenticated transport client;
- entity service modules/functions receiving the transport client;
- compatibility methods left on `SporelyCloudClient` where external tools rely on them.

Do not split the client and orchestrators in the same commit.

If the remaining client is understandable after Stages 0–7, **stop**. A smaller file count is not itself a reason to keep refactoring.

## Stage 9 — Optional facade/package cleanup

Risk: medium; optional.

Only after production imports and tests no longer depend on implementation placement, decide whether to:

- keep `utils/cloud_sync.py` permanently as a facade; or
- convert to `utils/cloud_sync/__init__.py` and remove the facade file.

Default preference: keep the facade unless there is a concrete maintenance benefit to conversion.

## Per-stage execution protocol

Every extraction stage follows the same workflow:

1. Confirm clean git status and exact HEAD.
2. Read `AGENTS.md`, `docs/supabase-sync-contract.md`, `docs/cloud-sync-architecture.md`, and this plan.
3. Identify the exact symbols to move and all import/monkeypatch consumers.
4. Run the stage's focused baseline tests **before** editing.
5. Move code mechanically; do not rename/rewrite unless required for import correctness.
6. Preserve facade exports.
7. Compile touched modules.
8. Run focused tests.
9. Run the broader cloud-sync safety suite.
10. Compare logs/result structures if the stage touches progress/errors/summaries.
11. Review for accidental policy changes, especially deletion, identity, pull-only, snapshots, and retry state.
12. Update architecture ownership/navigation docs for new module homes.
13. Commit one stage only.
14. Do not begin the next stage in the same context until the current stage is green and reviewed.

## Required validation matrix

At minimum, keep these areas green across the extraction:

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
- conflict preflight + conflict plan;
- image/measurement failure dirty propagation;
- fast no-op path;
- dirty-loop steady state;
- measurements;
- calibrations;
- image upload policy;
- media pull retry;
- original upload/recovery;
- spore mosaic / summary sync where affected.

## Live-canary policy during refactor

Do not live-canary every mechanical file move.

Use production/live validation only at meaningful boundaries, for example:

- after a behavior fix required before extraction;
- after tombstone or identity ownership changes if tests cannot fully cover the integration boundary;
- after push/pull orchestration moves;
- after final facade/client changes.

Before any live canary:

- make a fresh SQLite backup;
- run the read-only reconciliation report;
- require `C=D1=D2=E=H=0` unless a known reviewed exception is explicitly documented;
- do not mix live validation with cleanup/GC operations.

## Work explicitly out of scope for extraction

Do not combine this refactor with:

- E3 R2 garbage collection;
- repair of the one known `G_conflicting_intent` row;
- historical duplicate-observation cleanup;
- historical duplicate-image cleanup;
- standalone migration-tool hardening unless that tool blocks an extraction stage;
- cloud schema changes;
- orientation/analysis work;
- UI redesign;
- account-link/reset work;
- broad type-annotation or lint migrations.

## Definition of done

The extraction is complete when:

- `utils/cloud_sync.py` is a small stable facade (or, only if separately justified, replaced by a package facade);
- orchestration contains orchestration rather than entity implementations;
- transport contains no sync policy;
- storage intent, tombstones, snapshots/conflicts, measurements, calibrations, image identity, image mechanics, and anchors each have clear owners;
- the ownership table in `docs/cloud-sync-architecture.md` points to the new modules;
- all public production imports remain stable or have an explicit migration;
- the cloud-sync safety suite is green against the agreed baseline;
- no new reconciliation anomalies appear in the post-refactor live canary;
- no stage introduced behavior changes merely to make extraction easier.
