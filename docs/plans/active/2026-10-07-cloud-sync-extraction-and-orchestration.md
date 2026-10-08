# Cloud sync extraction and orchestration refactor

Written 2026-10-07 against `main` @ `c9d655f`. Replaces, for execution, the
earlier plan `docs/plans/active/2026-08-23-cloud-sync-extraction.md` (on
`main`) and its revision at `927eb64` on `feature/cloud-sync-refactor`. Those
documents and their intake reports remain the record of how the requirements
below were reached. Nothing in this plan is approved or started by writing it.

## What this plan is for

`utils/cloud_sync.py` is 28,305 lines (712 top-level and client-method
definitions; `SporelyCloudClient` alone starts at L15867 and holds domain
identity logic as well as transport). The work has two parts:

1. **Extraction (behavior-preserving).** Move the module's responsibilities
   into focused owners under a new `utils/cloud_sync_impl/` package, keeping
   `utils/cloud_sync.py` as the compatibility facade, with machine-checked
   evidence that every relocation is unchanged.
2. **Orchestration (intentional behavior change).** Make one owner responsible
   for observation completion, so that `sync_status='synced'` and the
   snapshot baseline are written only after required work has succeeded, on
   push and on pull, with one shared reconciliation classifier and typed sync
   issues.

Part 2 has consequential open questions (what counts as required work per
caller mode, whether the shared contract wording changes and therefore
whether `sporely-web` is involved, which tests are contract versus
accidental, and what live verification is needed). This plan therefore
contains the **design stage that resolves them** and the extraction stages.
The orchestration implementation and the retirement of transitional
scaffolding are planned in a follow-up plan once Stage S1 is accepted and the
person has answered the decisions it lists.

The extraction stops at the observation completion boundary. The code that
writes observation completion state stays in the facade, because the
orchestration follow-up redesigns it. That code is `push_all`, `pull_all`,
`sync_all`, the `synced` and conflict-review writers, and conflict
*execution* (`resolve_conflict_*`, `finalize_sync_candidates`). Conflict
execution calls `_stamp_observation_synced`, `_apply_remote_observation_fields`,
`_clear_observation_conflict_review_pending`, `_set_observation_privacy_blocked`
and the AI-field merge helpers, so moving it now would need either an upward
facade import or relocating those writers ahead of the design.

## Decided requirements carried forward

From the earlier plan, verified against current code:

- The 20 sync invariants listed in Stage S1 hold through every stage; they
  derive from `docs/supabase-sync-contract.md`, `.claude/rules/cloud-sync.md`
  and their guarding tests. Each extraction stage restates the ones its
  candidate could plausibly violate.
- `utils/cloud_sync.py` stays as the public facade (removing it to reduce file
  count is an anti-goal). Every name importable from it today stays
  importable and is the same object as its owner's.
- Owners form an acyclic import graph, enforced by a test. Owners do not import
  the facade, except an enumerated transitional allowlist.
- Pure reconciliation code is isolated and its purity is enforced by test: no
  SQLite/database, settings, cloud client/I/O or Qt.
- Sibling owners stay outside the refactor: `cloud_media_policy`,
  `original_sync_policy`, `cloud_media_recovery`, `cloud_media_audit`,
  `cloud_spore_mosaic[_backfill]`, `spore_summary_sync`, `r2_storage`, the
  reference-sync stack. The client is not split into several classes.
- Orchestration outcomes the follow-up must deliver (Stage S1 designs them; it
  does not revisit them): one owner writes `synced` and snapshots; `synced`
  only after required work and snapshot persistence, on push and pull;
  snapshot failure blocks `synced`; push and pull share one classifier, with no
  period with two; sync issues are typed while legacy `sync_all` result dicts
  and strings stay compatible except documented items; domain helpers stop
  deciding observation completion; `sync_all` becomes thin orchestration; the
  behavior change gets a live canary.
- Live-canary policy: a canary only where remote-state semantics can change;
  before it, a fresh SQLite backup and a read-only
  `tools/cloud_reconciliation_report.py` run with categories C, D1, D2, E and
  H at zero (or a documented exception); a deliberate Sync Now on a disposable
  or known account; account, database and build recorded; the report rerun
  and diffed; never combined with cleanup or garbage collection. Live
  Supabase writes are performed by the person, never by an agent.

## Stages, route and gates

All stages run in `sporely-py`, in order, as one run slice. No stage in this
plan changes `docs/supabase-sync-contract.md`, so there is no `sporely-web`
candidate here.

1. S1: orchestration completion design, a decision document only.
2. S2: green baseline and relocation-safety checks, tests and tooling only.
3. S3: leaf and boundary owners.
4. S4: reconciliation substrate, baseline and calibrations.
5. S5: image identity and metadata-only anchors.
6. S6: images, measurements and derived products.
7. S7: conflict detail and the conflict-plan model.

The design comes first because it resolves the only consequential
uncertainty, and nothing in the extraction depends on its answers.
Extraction is independent of orchestration and is worth doing either way.
Each extraction stage moves code only onto owners that already exist below
it. Conflict detail and the conflict-plan model therefore come last, because
they call image, measurement and derived-product code.

**Execution route: plan intake (compile mode), not direct Markdown.** The
plan declares two gates that hold work for a person's live-canary record:
the section *Gate G1* and the section *Completion gate G2*. Direct Markdown
cannot declare gates. Compile intake carries them into a version 2 manifest,
G1 as `gates_before` on Stage S6 and G2 as `completion_gates`. The stage
labels `S1`–`S7` are deliberately not the direct `## Stage <n>` convention,
so `sparring check-plan` reports that this plan does not parse as direct
Markdown. That result is expected.

Live-canary lifecycle: each of Stages S5, S6 and S7 is implemented, reviewed
and accepted on automated evidence alone. Its review records whether the
live-canary condition applies. The person reads that record at the gate. If
the condition applies, the person runs the canary there. A gate is released
only by the person's `pass`.

## Open questions (none blocks Stages S1–S7)

- The orchestration implementation plan needs the accepted Stage S1 design
  and the person's answers to the decisions in its section 8. It extracts
  conflict execution and the observation completion writers that this plan
  leaves in the facade.
- Scheduling: item 2 of `docs/plans/active/2026-09-25-sync-integrity-follow-ups.md`
  (measurement deletions) still changes `utils/cloud_sync.py`. Each extraction
  stage already redoes relocations when its base moves. Whether to land that
  fix before, between or after the extraction stages is a scheduling choice.
- Housekeeping after this plan is approved, for the person to decide: mark the
  2026-08-23 plan superseded; repoint the "Extraction plan" link in
  `docs/cloud-sync-architecture.md`; retire the `feature/cloud-sync-transport-boundary`
  and `review/cloud-sync-prestage-2026-09-08` branches; remove the untracked
  `utils/cloud_sync_impl/__pycache__` on `main`.

## Out of scope

R2 garbage collection, `G_conflicting_intent` repair, historical duplicate
cleanup, migration-tool hardening (including the donor's
`tools/migrate_legacy_reference_values.py` fixes), cloud schema changes, spore
orientation, UI redesign, account link/reset, broad lint/type migration,
external publishing, reference-use pending-change signalling, anchor
reservation risk fixes (tracked in `INBOX.md`). Also out of scope:
extracting `push_all`, `pull_all`, `sync_all`, conflict execution and the
observation completion writers (the orchestration follow-up does that), and
moving `SporelyCloudClient`'s own class definition.

## Stage S1 — Orchestration completion design

**Outcome:** a new, evidence-backed decision document,
`docs/cloud-sync-orchestration-design.md`. It establishes how observation sync
completion works in the current code and proposes a target completion model
consistent with the decided outcomes below. The person can then answer the
open decisions and plan the implementation. The document must contain:

1. **Current completion state machine.** Every code path that writes
   `observations.sync_status`, an observation snapshot or a conflict-review
   marker, inside and outside `utils/cloud_sync.py`. For each path: its
   trigger, its ordering relative to required child work and to snapshot
   storage, and the state that each failure leaves. Verified starting points
   at `c9d655f`:
   - `push_all`: stamps `synced` through `update_observation_sync_state`
     (~L21016) before image and measurement work, compensates with
     `mark_observation_dirty` on child failure (~L21085, L21354, L21445,
     L21573), and stores the snapshot after the stamp (~L21493).
   - `pull_all` and `_create_local_from_remote`: `_stamp_observation_synced`
     call sites, ~L26367–L27085.
   - `_set_observation_sync_state`, `_clear_observation_dirty_if_no_real_changes`,
     `_set_observation_conflict_review_pending` and
     `_set_observation_privacy_blocked` (both write `sync_status='dirty'`),
     `mark_observation_dirty`, `mark_observation_media_dirty` and the direct
     `mark_observation_sync_dirty` calls (for example in the pending-image
     repair scan).
   - Conflict execution: `resolve_conflict_keep_local`,
     `resolve_conflict_keep_cloud`, `resolve_conflict_merge`,
     `resolve_conflict_plan` and `finalize_sync_candidates`.
   - `materialize_cloud_media_for_observation`, which stores a snapshot.
   - `ui/observations_tab.py::_mark_cloud_observation_imported`, which writes
     `synced` directly.

   Record where `docs/cloud-sync-architecture.md` (sections C and I) states an
   ordering the code does not follow, and correct those statements in that
   document so they describe current behavior and point to the design.
2. **Required versus best-effort work by caller mode.** Cover every
   combination that matters of `sync_images`, `materialize_remote_images`,
   `full_pull`, `child_safety_pull` and `pull_only`, including the caller
   presets in `.claude/rules/cloud-sync.md`. State what work occurs, whether
   its failure currently leaves the observation dirty, and what would count as
   complete. Cite tests and contract sections, and keep current behavior
   separate from proposals.
3. **Target completion model.** The completion owner's API and its
   single-writer rule; the ordering required work → snapshot → `synced` for
   push and pull; the outcome and issue types (fields, producers,
   consumers); the shared push/pull reconciliation classification; what
   happens to deep `mark_observation_dirty`/`mark_observation_media_dirty`
   calls; how legacy `sync_all` result dicts and `result["errors"]` strings stay
   compatible; how conflict execution comes under the same owner (the
   extraction stages of this plan leave it in the facade); and how the UI
   import path above is brought under the same owner or documented as an
   exception.
4. **Test classification.** Classify each test asserting call order, call
   counts or intermediate sync state as *contract* or *accidental*. Tie each
   accidental test to the change that will alter it.
5. **No-op remote writes.** List every remote write reachable on a fast no-op
   sync. For each, state whether it changes remote `updated_at` without a
   semantic change and whether the invariant 18 tests cover it.
6. **Shared-contract impact.** Give the exact wording changes the target model
   requires in `docs/supabase-sync-contract.md`, or "none". Any change must
   land identically in `sporely-web/docs/supabase-sync-contract.md` in the same
   work item, so this answer decides whether the follow-up needs a
   `sporely-web` candidate.
7. **Verification strategy.** Name the fixtures to capture before the behavior
   change: reconciliation decisions from the current push and pull
   classifiers, and the legacy result golden. Assess whether the opt-in local
   Supabase harness (`tests/local_supabase/`, `tools/run_local_sync_harness.sh`)
   can exercise observation push/pull completion. State what the live canary
   must still cover.
8. **Decisions for the person.** List each product or scope question the code
   does not settle. Give at least two options and their consequences, and say
   which follow-up work depends on the answer. At minimum, cover whether
   observation completion on pull requires remote media materialization in
   each caller mode.

**Decided outcomes the design must conform to (not open for redesign):**
- exactly one owner writes `synced` and observation snapshots; any other
  remaining writer is a documented, justified exception;
- `synced` is written only after required work and snapshot persistence, on
  push and pull, and a snapshot failure blocks `synced`;
- a required child failure leaves the observation dirty, retryable and visible,
  and keeps the old baseline;
- push and pull use one reconciliation classifier;
- issues are typed, and legacy result dicts and strings stay compatible except
  documented items;
- domain helpers do not decide observation completion;
- `utils/cloud_sync.py` remains the facade;
- no new remote writes: pull-only and fast no-op sync perform zero writes.

**Invariants the target model must keep (all stages):**
1. Local SQLite decides which image bytes are desired. Only ledger membership
   (`sporely_cloud_image_storage_intent_ids_<obs>`) proves initialized intent,
   and the initializer performs no cloud I/O.
2. Explicit user removal is the only source of routine cloud image deletion.
   Omission, filtering, prep failure, missing files and partial reads never are.
3. The pending tombstone flush runs before dirty-observation pruning, so an
   explicit deletion converges in the same sync.
4. A verified local `cloud_id` is the primary push identity, and remote
   `desktop_id` is recovery only. Disagreement, ambiguity, soft-deleted matches
   and unique-violation races fail closed: never POST, never reparent.
5. Structured taxonomy identity is authoritative: display, scientific and
   vernacular text never substitutes for it, and `sporely_taxon_id` with the
   existing source and namespace semantics is preserved. It participates in
   change and conflict detection, a no-baseline local/remote contradiction
   fails closed, and its snapshot and reconciliation representation is
   semantically unchanged by extraction.
6. A stale cloud identity is cleared explicitly and its landing is verified
   (`_verify_identity_clear_landed`).
7. Metadata-only microscope anchors are valid rows. Owner-sync metadata
   parents are created only when `_owner_sync_parents_supported` confirms
   support (fail closed). Byte selection is independent of measurement and
   mosaic participation.
8. Anchor promotion reuses the existing row. The pending marker comes before
   the reserve PATCH, and the reserve is conditional on `storage_path IS NULL`.
   A failure releases only the exact reserved key, a `None` upload return is a
   failure, and a reserved path never proves bytes.
9. A local render signature never proves remote upload completeness.
   `_CLOUD_PENDING_IMAGE_REPAIR_VERSION` changes only when the repair scan's
   selection changes.
10. Pull-only performs zero cloud writes, and every client method is classified
    as a read or a write.
11. A partial or bounded remote collection is never authoritative, and
    paginated reads use deterministic ordering.
12. The snapshot is the accepted shared baseline, accepted asymmetry included.
    It is never written after truncated reads, unresolved conflicts,
    incomplete required work or ambiguous identity.
13. Representation-only differences never conflict, and genuine three-way
    divergence always does.
14. In conflict plans, reviewed-baseline drift aborts the apply, partial
    retries are idempotent, and media deletion is unreachable from a plan.
15. A required child failure leaves the observation retryable and visible.
16. Cloud recovery-cache bytes are never re-uploaded. Local originals are
    never deleted or downgraded by cloud-side disappearance.
17. The mosaic signature survives the sync's own working-file swap, and
    publication selection is not part of the mosaic key.
18. A fast no-op sync performs zero remote writes, and child-change cursor
    semantics are unchanged.
19. The Red List follows the identification, the location-precision guard
    holds, and cloud-only field edits are persisted locally during push.
20. The `sync_all` caller-mode flags keep their meaning.

**Boundaries / non-goals:** documentation only. The only files that change are
the new design document and the corrections to
`docs/cloud-sync-architecture.md`. Nothing under `utils/`, `database/`, `ui/`,
`tools/` or `tests/` changes, and neither does
`docs/supabase-sync-contract.md` (section 6 only proposes wording). Do not
edit this plan. Refer to code by symbol and file; line numbers are hints
only, because the extraction stages relocate the code. The design does not
answer the section 8 decisions; it presents them. Its acceptance does not
wait for the person's answers.

**Acceptance evidence:**
- A reviewer can confirm section 1 is complete. A repository-wide search for
  `sync_status` writes, `update_observation_sync_state` and
  `mark_observation_sync_dirty` callers, and snapshot store callers
  (`_store_remote_snapshot`, `_store_cloud_observation_snapshot`) finds no
  writer missing from the inventory.
- Every row of the caller-mode table cites a test or a contract section. Rows
  without one are marked as unverified.
- The target model is checked against each decided outcome and each invariant
  above, and any tension is stated rather than resolved silently.
- The no-op write table matches the code paths it names.
- `git diff --stat` against the base shows only the two documentation files.

## Stage S2 — Green baseline and relocation-safety checks

**Outcome:** the cloud-sync test baseline is green, and the repository has the
automated checks that the extraction stages (S3–S7) and their reviewers use as
evidence. Production code does not change.

1. **Red baseline repaired (test-only).** At `c9d655f`,
   `tests/test_cloud_sync_progress_reset_and_prepare.py::test_reconcile_metadata_only_linked_images_skips_unchanged_siblings`
   fails because its stub `push_image_metadata` lacks the `remote_row` keyword
   that production now passes (1 failed, 6 passed in that file). The donor
   commit `ca16130` on `feature/cloud-sync-transport-boundary` contains this
   stub fix and three materialization-state regressions for
   `tests/test_cloud_media_pull_retry.py`. The regressions are absent on
   `main`, but the production fix they cover is present. Reapply what still
   applies after checking it against current code. The donor's production and
   migration-tool changes are out of scope.
2. **Relocation-equivalence check (new tool, runnable by reviewers over any
   `base..candidate`).** For every top-level function, class, method
   (including `SporelyCloudClient` methods) and module-level statement removed
   from `utils/cloud_sync.py`, the check locates the new definition and reports
   one of *identical*, *differs* (with a diff), *needs review* or *missing*. It
   must fail closed: when it cannot prove equivalence it never reports
   *identical*. Equivalence means all of the following:
   - **Definition:** the full AST is equal, including decorators, defaults,
     annotations, docstrings, keyword arguments, `global`/`nonlocal` statements
     and nested definitions. Comments and whitespace are ignored. Renamed
     identifiers, reordered statements, changed literals, imports inside
     bodies, exception types, `try` blocks and defaults are never ignored.
   - **Free names:** every global or free name the definition reads resolves
     to the same origin at base and at candidate. Base and candidate run in
     separate processes, so equivalence is by origin, never by cross-process
     identity. The origin of a function or class is its module, its qualname
     and a source hash. A name relocated by an earlier verified move maps
     through that relocation. A module's origin is its name, and an immutable
     literal's is its `repr`. Any other object needs review unless it is shown
     to be the single shared instance.
   - **Module-level state:** a moved cache, registry, `ContextVar`, lock or
     logger exists exactly once across facade and owners, and the facade binds
     the same object.
   - **Dynamic lookup:** `global`, `globals()`, `sys.modules`, `getattr` on a
     module, `importlib`, late-bound facade access, loggers named from
     `__name__`, and descriptor- or metaclass-sensitive code always report
     *needs review*.
   - **Client methods moved off `SporelyCloudClient`:** the method resolves
     through the MRO to the moved function, and no other attribute changes its
     resolution.
   - **Annotation-only names:** `utils/cloud_sync.py` uses
     `from __future__ import annotations`, and many definitions name
     `SporelyCloudClient` only in annotations. Such a name is not a runtime
     read when the owner module has the same future import, which the check
     confirms. The annotation itself must still be AST-equal.
   - **Relocation metadata alone** (`__module__`, the qualname prefix, the
     owner path, `__globals__` identity) is never a difference.
   - **Candidate-side identity:** within the candidate process, the check
     confirms `utils.cloud_sync.X is <owner>.X` for every moved name.
3. **Import-direction test (new)** over `utils/cloud_sync_impl/`. The package
   does not exist on `main` yet; only an untracked `__pycache__` is there. The
   test fails on an owner importing `utils.cloud_sync` at any nesting level,
   function-local and lazy imports included, except an explicit enumerated
   allowlist (empty unless a later stage justifies an entry). An import under
   `if TYPE_CHECKING:` that binds names used only in annotations is permitted,
   because it never executes; the test confirms the guard and the
   annotation-only use. It also fails on a dependency-direction violation
   between owners. Later stages extend it as owners appear.
4. **Taxonomy identity golden (new test, captured at the base).** A committed
   fixture records, for a fixed set of rows, the base outputs of
   `_identity_sync_key`, `_local_identity_sync_key`, `_remote_identity_claim`,
   `_baseline_identity_key` (including `_IDENTITY_BASELINE_UNKNOWN` for a
   snapshot stored before identity joined change detection),
   `_classify_identity_sync_change`, `_remote_identity_changed_since`, the
   identity part of `_observation_compare_payload`, and the persisted
   observation snapshot JSON carrying identity. A test asserts the current code
   reproduces the fixture exactly. The rows cover at least:
   - a proven Sporely identity (`sporely_taxon_id` with its source and
     namespace);
   - a cloud-selected unverified identity;
   - external-only evidence, unresolved external evidence, and an external
     integer that collides numerically with a Sporely id;
   - no identity;
   - an id absent from the installed taxonomy;
   - a pre-identity snapshot;
   - local and remote rows that differ only in display, scientific or
     vernacular text, or only in case and whitespace.

   The fixture is evidence for the extraction stages and must not change while
   they run.
5. **Discoverability.** `docs/cloud-sync-architecture.md` (test map, section K)
   records how to run the equivalence check, the import-direction test and the
   taxonomy identity golden. The extraction stages are briefed only to use "the
   checks accepted in Stage S2", so they find them there.

**Hard constraints:** nothing under `utils/`, `database/` or `ui/` changes.
The checks are committed so that the extraction stages and reviewers run the
same code. A later stage may extend the checks but may not weaken their
fail-closed rules.

**Implementation freedom:** tool location, CLI and output format; how the
import-direction rules are expressed; whether to add a test-only helper for
redirecting facade monkeypatches to relocated code. If such a helper is added,
it patches only bindings that are the *same object* as the facade's binding,
fails when the name is missing, and is never imported by production code. It
also comes with its own tests.

**Acceptance evidence:**
- Unit tests on synthetic before/after modules produce the expected verdict
  for each case: identical, a valid relocation (including method-to-mixin), a
  renamed free name, a rebound free name, duplicated module-level state, a
  `global` statement, a logger-name change, a changed default or
  decorator, and an annotation-only name with and without the owner's
  `from __future__ import annotations`.
- The check over an empty range reports nothing.
- The import-direction test has positive and negative synthetic cases,
  including a function-local import of the facade and a `TYPE_CHECKING`
  import used outside annotations.
- The formerly failing test passes.
- The taxonomy identity golden passes at the base and covers every row class
  above.
- Broad gate: every test file importing cloud sync passes:
  `/Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/pytest -q $(grep -rlE "utils\.cloud_sync|from utils import cloud_sync" tests --include='test_*.py')`.
  At `c9d655f` this is 84 files and 1,830 collected tests. The full pass/fail
  count was not measured when the plan was written, so record it before and
  after the stage. If the base has failures besides the known one, report them
  and stop. Do not fix production code in this stage.

## Stage S3 — Leaf and boundary owners

**Outcome:** the infrastructure and boundary layer moves out of
`utils/cloud_sync.py` into owners under `utils/cloud_sync_impl/`, with no
behavior change. The layer is everything later owners depend on that depends
on no reconciliation, domain or orchestration code. Responsibilities, with
verified starting points:
- **Errors and issue classification:** the error classes,
  `is_identity_clear_verification_failed_error` (L3026),
  `_collect_sync_error_details`, and the image-too-large and privacy-slot
  classification and formatting (`is_image_too_large_for_plan_error`,
  `summarize_image_too_large_for_plan_error`,
  `infer_image_too_large_for_plan_reason`, `is_privacy_slot_limit_error`,
  `privacy_slot_limit_user_message` and their parsers), which
  `summarize_sync_issues` uses.
- **Profiling, progress and sync summary:** `summarize_sync_issues` (L3464),
  the summary `ContextVar` accessors (`_cloud_sync_current_summary`,
  `_cloud_sync_current_profiler`) and progress emitters.
- **Pure common helpers:** for example `_safe_int` (L8414) and
  `_normalize_cloud_media_key`.
- **HTTP transport and pagination** for `SporelyCloudClient`: `_get_paginated`,
  `_patch_with_precondition`. `SporelyCloudClient` remains the class callers
  construct, and its class definition stays in the facade.
- **Pull-only boundary:** `_PULL_ONLY_BLOCKED_CLIENT_METHODS`,
  `_PULL_ONLY_ALLOWED_READ_METHODS` and the pull-only client wrapper.
- **Shared remote reads:** `_pull_remote_images_for_sync`,
  `_pull_remote_measurements_for_images`,
  `_group_remote_measurements_by_observation`.
- **Capability probes:** `_owner_sync_parents_supported`,
  `_remote_metadata_purpose` and the `METADATA_PURPOSE_*` constants.
- **Local sync-state helpers:** settings keys and per-image local state
  (`_cloud_metadata_only_image_ids`, `_set_cloud_image_metadata_only_state`,
  the pending promotion key store `_store_pending_image_promotion_key`,
  `_load_pending_image_promotion_key` and `_clear_pending_image_promotion_key`,
  `_explicit_image_restore_source` and `remember_explicit_image_restore_source`);
  the file and media signature *stores*, which load, store and clear by
  settings key (`_clear_cloud_image_file_signature`,
  `_load_local_cloud_media_signature`, `_store_local_cloud_media_signature`);
  and the dirty markers `mark_observation_dirty` and
  `mark_observation_media_dirty`. No `synced` writer moves.
- **Image byte policy:** the canonical owner of desired bytes, explicit
  selection and anchor predicates:
  - `cloud_image_bytes_desired` and `should_pull_cloud_image_to_desktop`;
  - the storage-intent ledger (`_cloud_image_storage_intent_initialized_ids`,
    `_ensure_cloud_image_storage_intent_initialized`,
    `cloud_image_storage_intent_initialized`);
  - explicit selection (`set_image_cloud_selected`,
    `_cloud_explicit_media_upload_selection`);
  - the anchor predicates `microscope_image_requires_public_spore_anchor`,
    `microscope_image_requires_owner_sync_anchor`,
    `measurement_qualifies_for_public_spore_anchor`,
    `_is_metadata_only_microscope_cloud_image` and
    `_is_local_metadata_only_microscope_anchor`.

  No later stage moves these again; the anchors owner in Stage S5 imports
  them from here.
- **Tombstones:** `_local_tombstoned_cloud_image_ids`,
  `_local_tombstoned_local_image_ids`, `_record_remote_image_tombstones`,
  `_tombstoned_cloud_image_warning`, `_push_pending_image_tombstones`.
- **Reference adapter:** `utils/reference_cloud_adapter.py` imports its error
  classes from the errors owner instead of the facade.

**Verified dependency facts:**
- `_push_pending_image_tombstones` calls `_owner_sync_parents_supported`,
  `microscope_image_requires_public_spore_anchor` and
  `microscope_image_requires_owner_sync_anchor`. The capability probe and the
  anchor predicates therefore have their owner no later than tombstones.
- `set_image_cloud_selected` (the gallery checkbox's entry point, and the
  source of explicit removal intent) calls `mark_observation_media_dirty`,
  which calls `mark_observation_dirty`. The dirty markers therefore move with
  it.
- `summarize_sync_issues` calls the image-too-large and privacy-slot helpers.
- The tombstone line region (~L6287–6570) is mixed. `_reconcile_local_image_cloud_id`
  is image identity (Stage S5) and `_remote_images_missing_locally` is images
  (Stage S6); both stay in the facade here.
- Snapshot codec and persistence (`_cloud_observation_snapshot`,
  `_load_cloud_observation_snapshot`, `_store_cloud_observation_snapshot`,
  `_parse_cloud_observation_snapshot`) sit inside the sync-state line range but
  belong to the baseline owner (Stage S4).

**Hard constraints:**
- **Behavior-preserving.** Remote and local writes, their ordering, error
  text, result-dict keys, progress phases, ledger key format, headers,
  refresh, retry, timeouts and page ordering are unchanged.
- **Facade compatibility.** Every name importable from `utils.cloud_sync` at
  the base stays importable and is the same object as its owner's binding. The
  facade never redefines a moved name.
- **Import direction.** No owner imports the facade except entries added to
  the Stage S2 allowlist, each with a reason and a retirement condition. The
  import graph is acyclic.
- **Dependency closure.** A definition moves only when every module-level
  name it uses at runtime is owned by this stage or comes from outside
  `utils.cloud_sync`. A helper this stage's moved code needs moves with it, to
  the lowest owner that fits its responsibility, even when it sits in a later
  stage's line range; the stage notes record it. A definition that still
  needs facade-resident code at runtime stays in the facade, and the stage
  notes name the blocking dependency. Ownership is by responsibility and
  dependency, never by line range.
- **Dirty and sync-status writes.** Every call of `mark_observation_dirty`,
  `mark_observation_media_dirty` or `mark_observation_sync_dirty`, and every
  `sync_status` write, is unchanged in count and condition across the facade
  and owners together.
- **Pull-only registries** are those of the current base. The donor branch's
  `utils/cloud_sync_impl/` modules are 274+ commits stale and are reference
  only, never merged or copied wholesale.
- **Patched tests keep working.** A test that patched a relocated name must
  still affect the code it exercises.
- **Base drift.** If `utils/cloud_sync.py` changes on the base during the
  stage, redo the affected relocations against the new base instead of
  hand-merging into moved code. The equivalence evidence is computed against
  the candidate's actual base.
- **Documentation.** `docs/cloud-sync-architecture.md` ownership and
  navigation entries for moved symbols point to their owners.

**Implementation freedom:** module names and boundaries within the package,
mixin structure for client methods, commit structure, whether to retarget a
patch to the owner or use a test-only same-object patch helper, and removing
the lazy imports in `sync_all` once the import graph shows they are no longer
needed. Stage S2 may have added such a helper. If it did not, one may be added
here under the same rules: it patches only bindings that are the same object as
the facade's, fails on a missing name, and is never imported by production
code.

**Invariants at risk:**
- Local SQLite decides which image bytes are desired; only ledger membership
  proves initialized intent, and the initializer performs no cloud I/O.
- Explicit user removal (`set_image_cloud_selected`) is the only source of
  routine cloud image deletion. Omission, filtering, prep failure, missing
  files and partial reads never are.
- The pending tombstone flush runs before dirty-observation pruning.
- Owner-sync parents are gated by `_owner_sync_parents_supported` (fail
  closed), and byte selection stays independent of measurement and mosaic
  participation.
- Pull-only performs zero cloud writes, and every client method is classified
  as a read or a write.
- Partial or bounded remote collections are never authoritative, and
  paginated reads keep deterministic ordering.
- A fast no-op sync performs zero remote writes, and child-change cursor
  semantics are unchanged.
- The `sync_all` caller-mode flags keep their meaning, including when the lazy
  imports in `sync_all` are removed.

**Acceptance evidence:**
- The relocation-equivalence check accepted in Stage S2 (its invocation is
  recorded in `docs/cloud-sync-architecture.md`) runs over `base..candidate`
  and reports every relocated item *identical*, except listed items. Each listed item has a
  one-line reason, and none is a semantic change.
- The import-direction test covers the new owners, and the allowlist is
  empty or each entry carries a reason and a retirement condition.
- A facade-identity test covers every moved name.
- For every relocated name, a search of `tests/` for patches of it (attribute
  and string targets such as `"utils.cloud_sync.<name>"`) shows each one
  retargeted to the owner or routed through the same-object helper. No patch
  silently stops reaching the code under test.
- An enforcing test shows that every method reachable on `SporelyCloudClient`,
  including methods from new bases, is in exactly one pull-only list.
- A base-versus-candidate search shows the dirty-marking calls and
  `sync_status` writes unchanged in count and condition.
- The stage notes list each definition kept in the facade under the
  dependency-closure rule, with its blocking dependency.
- The stage notes trace that `_push_pending_image_tombstones` still runs before
  dirty-observation pruning in `push_all`.
- These pass: `tests/test_cloud_download_only.py`,
  `tests/test_cloud_storage_intent_ledger.py`,
  `tests/test_cloud_image_bytes_desired.py`,
  `tests/test_cloud_sync_metadata_only.py`, `tests/test_image_tombstones.py`,
  `tests/test_cloud_sync_fast_path.py`,
  `tests/test_cloud_sync_dirty_loop_steady_state.py`,
  `tests/test_child_change_probe.py`,
  `tests/test_cloud_sync_change_notification.py`,
  `tests/test_reference_cloud_adapter.py`,
  `tests/test_reference_cloud_sync_coordinator.py`, and the broad gate. The broad gate command is
  `/Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/pytest -q $(grep -rlE "utils\.cloud_sync|from utils import cloud_sync" tests --include='test_*.py')`,
  and its pass count must not fall below the count recorded when Stage S2 was
  accepted.

## Stage S4 — Reconciliation substrate, baseline and calibrations

**Prerequisite:** the Stage S3 owners are accepted. Names an accepted earlier
stage already moved stay where they are.

**Outcome:** the code that decides what changed and what the accepted baseline
is moves into owners, with no behavior change and no import cycle:
- **Pure reconciliation code** (a dedicated package, planned as
  `utils/cloud_sync_impl/reconciliation/`):
  - taxonomy identity classification (`_RemoteIdentityClaim`,
    `_identity_sync_key`, `_remote_identity_claim`, `_local_identity_sync_key`,
    `_classify_identity_sync_change`, `_baseline_identity_key` with the
    `_IDENTITY_BASELINE_UNKNOWN` sentinel, `_local_identity_is_claim`,
    `_remote_identity_changed_since`, `_identification_key`,
    `_identification_contradicts_remote`, `_withhold_identity_from_push`,
    `_remote_row_without_identity` and `_remote_name_snapshot`);
  - observation, image and measurement compare payloads and change analysis
    (`_observation_compare_payload`, `_analyze_image_changes`,
    `_analyze_measurement_changes`, `_remote_image_payload`, the measurement
    snapshot and compare helpers, and related value-only helpers);
  - sample-source representation helpers (`_desktop_to_cloud_sample_source`,
    `_cloud_to_desktop_sample_source`, `_split_legacy_sample_type_into_source`,
    `_apply_image_sample_fields_to_push_payload`);
  - location-precision ranking;
  - the preflight report type and its formatting
    (`ObservationPushConflictReport`, `_format_push_conflict_review_reasons`);
  - accepted-asymmetry helpers: `_reconcile_accepted_asymmetry` (with its
    nested `_still_*` predicates), `_accepted_asymmetry_key`,
    `_merge_accepted_asymmetry`, `_identities_referenced_by_plan`, the
    `_asymmetry_fingerprint_*` helpers and
    `_filter_accepted_one_sided_images` /
    `_filter_accepted_one_sided_measurements`.
- **The observation push payload serializer** (`_observation_push_payload` and
  its pure normalizers). It contains no push execution.
- **The baseline owner:** snapshot codec, load and store, including
  `_CLOUD_OBSERVATION_SNAPSHOT_SCHEMA_VERSION`, `_local_observation_id_by_cloud_id`
  and `_store_remote_snapshot`, which reads the remote state through the
  Stage S3 remote-read owner when not given rows.
- **Stateful reconciliation adapters**, outside the pure package:
  - taxonomy identity state (`_installed_taxon_concept`,
    `_local_identity_columns_for_remote_claim`,
    `_apply_remote_identity_to_local`);
  - taxonomy identity push and explicit clear, as client methods moved to a
    mixin of `SporelyCloudClient`: `_sync_observation_selected_taxon`,
    `_maybe_clear_stale_cloud_identity` and `_verify_identity_clear_landed`;
  - location precision (`record_confirmed_location_precision`,
    `consume_confirmed_location_precision`, `_guard_local_location_precision`,
    `_snapshot_baseline_for_cloud_id`, `repair_legacy_location_precision`);
  - observation preflight (`_analyze_observation_push_conflicts`,
    `_observation_push_diff_fields`, `_local_has_real_changes_since_snapshot`,
    `_local_image_snapshot_payload`, `_image_calibration_uuid`,
    `_locally_tombstoned_snapshot_image_identity_keys`,
    `_is_spore_measurement_source_image`);
  - the observation local media signature: computation, comparison and
    refresh (`_local_cloud_media_signature`, `_local_cloud_image_media_signature`,
    `_local_media_signatures_match`, `_store_local_media_signature_if_equivalent`,
    `_refresh_local_cloud_media_signature`). It builds on the Stage S3
    signature stores;
  - the local measurement lookup `_load_local_measurement_lookup`.
- **Calibrations**: the calibration normalizers and payloads,
  `push_calibrations`, `pull_calibrations`, `list_calibration_conflicts`,
  `repair_calibrations_local_wins`, `_reconcile_local_image_calibration_links`
  and `_local_calibration_id_for_image`.

**Verified dependency facts:**
- The current code is cyclic. `_store_remote_snapshot` calls
  `_reconcile_accepted_asymmetry`, and conflict resolution calls
  `_store_remote_snapshot`. The accepted-asymmetry helpers are pure dict
  functions, so placing them below the baseline breaks the cycle without
  behavior change.
- `_observation_compare_payload` calls `_local_identity_sync_key` and
  `_remote_identity_claim`.
- The taxonomy identity helpers above depend only on each other and on
  value normalizers. `_identification_contradicts_remote`,
  `_withhold_identity_from_push` and `_remote_row_without_identity` are called
  only from `push_all` and `pull_all`, which import them downward.
- `_sync_observation_selected_taxon` (called from the client's
  `push_observation`) pushes taxonomy identity. When the local identity is
  none and the baseline proves a previously synced Sporely identity, it calls
  `_maybe_clear_stale_cloud_identity`, which issues the explicit clear RPC and
  confirms it through `_verify_identity_clear_landed`. These clear the
  observation's *taxonomy* identity; they are not image identity. Their other
  dependencies are client RPC methods reached through `self`
  (`set_observation_selected_taxon`, `clear_observation_selected_taxon`,
  `get_observation`), which stay on the client.
- Red List application stays with observation field apply in the facade:
  `_remote_observation_extra_values` (pull) and
  `_merge_cloud_selected_ai_fields` / `_adopt_merge_filled_ai_fields_locally`
  (push merge). This stage must not change how they read identity.
- `_analyze_observation_push_conflicts` loads snapshots, reads tombstones and
  reads calibrations, and calls `_is_spore_measurement_source_image`,
  `_analyze_image_changes` and `_remote_image_payload`.
- `_local_has_real_changes_since_snapshot` calls the media-signature
  helpers above and `_load_local_measurement_lookup`. Those therefore live
  here, not with images and measurements in Stage S6.
- `pull_calibrations` reaches snapshot parsing through
  `_reconcile_local_image_calibration_links`, and calibration payloads use the
  sample-source helpers.
- These stay in the facade (pull and push execution, observation completion
  writers, review markers; orchestration follow-up):
  `_apply_remote_observation_fields`, `_shape_geography_patch_payload`,
  `_stamp_observation_synced`, `_set_observation_sync_state`,
  `_clear_observation_dirty_if_no_real_changes`,
  `_set_observation_privacy_blocked`,
  `_set_observation_conflict_review_pending` and
  `_clear_observation_conflict_review_pending`.
- Conflict detail and the conflict-plan model move in Stage S7; conflict
  execution stays in the facade.

**Hard constraints:**
- **Purity, enforced by test.** Pure modules perform no SQLite or database
  reads or writes, no settings reads or writes, no cloud client or I/O, and
  import no Qt.
  - They import only other pure modules and leaf owners from Stage S3 that are
    themselves pure. From `database`, only the static
    `ObservationDB._normalize_location_precision` is allowed, as an explicit
    allowlist entry.
  - An AST scan rejects calls in them to `get_connection`, `SettingsDB`,
    `get_app_settings`, `update_app_settings`, any other `*DB` method, and
    client methods.
  - If a symbol assigned to pure code turns out to be stateful, it goes to the
    stateful owner for its responsibility. The purity test is never weakened.
- **No cycle.** The baseline does not import conflict code, and nothing pure
  imports a stateful adapter.
- **Unchanged:** snapshot schema and versions, persisted snapshot JSON and
  the observation local media signature format.
- **Behavior-preserving.** Remote and local writes, their ordering, error
  text, result-dict keys and progress phases are unchanged.
- **Facade compatibility.** Every name importable from `utils.cloud_sync` at
  the base stays importable as the same object. This includes the public
  `repair_legacy_location_precision`, `record_confirmed_location_precision`
  and `consume_confirmed_location_precision`. The facade never redefines a
  moved name.
- **Import direction.** No owner imports the facade except entries on the
  enumerated allowlist, each with a reason and a retirement condition.
- **Dependency closure.** A definition moves only when every module-level
  name it uses at runtime is owned by this stage or an accepted earlier one,
  or comes from outside `utils.cloud_sync`. A helper this stage's moved code
  needs moves with it, to the lowest owner that fits its responsibility, even
  when it sits in a later stage's line range; the stage notes record it. A
  definition that still needs facade-resident code at runtime stays in the
  facade, and the stage notes name the blocking dependency.
- **Dirty and sync-status writes.** Every call of `mark_observation_dirty`,
  `mark_observation_media_dirty` or `mark_observation_sync_dirty`, and every
  `sync_status` write, is unchanged in count and condition across the facade
  and owners together.
- **Patched tests keep working.** A test that patched a relocated name must
  still affect the code it exercises.
- **Base drift.** If `utils/cloud_sync.py` changes on the base, redo the
  affected relocations instead of hand-merging. The equivalence evidence is
  computed against the actual base.
- **Documentation.** `docs/cloud-sync-architecture.md` ownership entries point
  to the new owners.

**Implementation freedom:** module names other than the pure package's role,
helper placement within a responsibility (including which value-only compare
helpers are pure), commit structure, and patch retargeting strategy.

**Invariants at risk:**
- Taxonomy identity contract:
  - structured taxonomy identity is authoritative; display, scientific and
    vernacular text never substitutes for it, and identity is never inferred
    from name text;
  - `sporely_taxon_id` and the existing source and namespace identity
    semantics are preserved, including unverified, unresolved-external and
    not-installed identities that are kept but never bound;
  - taxonomy identity participates in change and conflict detection;
  - a stale identity is cleared only explicitly, and the clear is verified to
    have landed;
  - a no-baseline local/remote taxonomy contradiction fails closed;
  - the Red List follows the identification;
  - the snapshot and reconciliation representation of taxonomy identity is
    semantically unchanged, including the pre-identity baseline sentinel.
- The snapshot is the accepted shared baseline, accepted asymmetry included.
  It is never written after truncated reads, unresolved conflicts, incomplete
  required work or ambiguous identity. `_store_remote_snapshot` never treats a
  partial or bounded remote read as authoritative.
- Representation-only differences never conflict, and genuine three-way
  divergence always does.
- Conflict plans still abort on reviewed-baseline drift, and partial retries
  stay idempotent. The accepted-asymmetry carry they rely on moves here.
- A local render or media signature never proves remote upload completeness.
- Pull-only performs zero cloud writes: `pull_calibrations` stays a pure read
  on pull-only paths, and every client method stays classified.
- A fast no-op sync performs zero remote writes, including calibration push
  and snapshot storage.
- The Red List follows the identification, the location-precision guard
  holds, and cloud-only field edits are persisted locally during push.

**Acceptance evidence:**
- The relocation-equivalence check accepted in Stage S2 (invocation in
  `docs/cloud-sync-architecture.md`) is clean over `base..candidate` except
  listed, justified items.
- The purity and import-direction tests cover the new modules.
- A facade-identity test covers every moved name.
- For every relocated name, a search of `tests/` for patches of it (attribute
  and string targets such as `"utils.cloud_sync.<name>"`) shows each one
  retargeted to the owner or routed through the same-object helper. No patch
  silently stops reaching the code under test.
- The stage notes show that every remaining `_store_remote_snapshot` call
  site is unchanged in arguments and ordering.
- Conflict-plan fixtures (baseline drift, partial retry, accepted-asymmetry
  carry), still run through the facade's conflict execution, produce
  byte-identical persisted snapshot JSON at base and candidate.
- A no-baseline taxonomy contradiction still fails closed.
- The Stage S2 taxonomy identity golden passes, and its fixture is unchanged
  in `git diff base..candidate`.
- The equivalence check confirms `_IDENTITY_BASELINE_UNKNOWN` exists exactly
  once and the facade binds the same object, and that
  `_sync_observation_selected_taxon`, `_maybe_clear_stale_cloud_identity` and
  `_verify_identity_clear_landed` resolve through the MRO to the moved
  functions and stay classified in the pull-only lists.
- A base-versus-candidate search shows the dirty-marking calls and
  `sync_status` writes unchanged in count and condition.
- The stage notes list each definition kept in the facade under the
  dependency-closure rule, with its blocking dependency.
- These pass: `tests/test_cloud_sync_no_baseline_identity_contradiction.py`,
  `tests/test_cloud_taxonomy_identity_sync.py`,
  `tests/test_cloud_identity_change_detection.py`,
  `tests/test_cloud_identity_pull.py`,
  `tests/test_cloud_identity_fail_closed.py`,
  `tests/test_cloud_selected_unverified_identity.py`,
  `tests/test_cloud_sync_identity_clear.py`,
  `tests/test_red_list_sync.py`,
  `tests/test_red_list_push_merge_follows_identification.py`,
  `tests/test_red_list_identity_invariants.py`,
  `tests/test_taxonomy_identity_boundary.py`,
  `tests/test_image_conflict_normalization.py`,
  `tests/test_cloud_sync_conflict_preflight.py`,
  `tests/test_cloud_conflict_plan_execution.py`,
  `tests/test_cloud_conflict_dialog.py`,
  `tests/test_cloud_calibration_sync.py`,
  `tests/test_location_precision_sync_guard.py`,
  `tests/test_cloud_download_only.py`, `tests/test_cloud_sync_fast_path.py`,
  `tests/test_cloud_sync_dirty_loop_steady_state.py`,
  and the broad gate. The broad gate command is
  `/Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/pytest -q $(grep -rlE "utils\.cloud_sync|from utils import cloud_sync" tests --include='test_*.py')`,
  and its pass count must not fall below the previous stage's.
  `tests/test_red_list_identity_invariants.py` and
  `tests/test_taxonomy_identity_boundary.py` do not import cloud sync, so the
  broad gate does not run them; they are named here for that reason.

## Stage S5 — Image identity and metadata-only anchors

**Prerequisite:** the Stage S3 and Stage S4 owners are accepted. Names an
accepted earlier stage already moved stay where they are. In particular the
anchor predicates are owned by the Stage S3 image byte policy owner, and this
stage imports them from there.

**Outcome:** image identity and the remote-write side of metadata-only
microscope anchors have their own owners, with no behavior change:
- **Image identity:** `_reconcile_local_image_cloud_id`,
  `_portable_cloud_identity_pending_for_observation`,
  `_finalize_portable_cloud_identity_guard`, and the client methods
  `_resolve_existing_observation_for_push`, `_resolve_existing_image_for_push`
  and `_find_cloud_image`. The taxonomy identity clear
  (`_maybe_clear_stale_cloud_identity`, `_verify_identity_clear_landed`) was
  moved by Stage S4 and is not image identity.
- **Anchors (remote ensure, owner-sync and retire):**
  - `_ensure_metadata_anchors_for_public_spore_observation`,
    `_ensure_metadata_only_microscope_images_for_observation`,
    `_ensure_metadata_only_microscope_image_for_public_spores`,
    `_observation_has_owner_sync_candidates`,
    `_retire_unneeded_owner_sync_parent`,
    `_cancel_microscope_anchor_tombstones`,
    `_remote_image_row_matches_anchor_payload` and
    `_metadata_only_microscope_image_payload`;
  - the promotion helpers `_reserve_anchor_promotion_key` and
    `_rollback_anchor_promotion`.

Images, measurements and derived products may still be in the facade after
this stage. The local, pull-side anchor insert
`_ensure_local_metadata_only_microscope_anchor` is not part of this stage. Its
only callers are image pull and materialization, and it uses image-apply
helpers, so it moves with images in Stage S6.

**Verified dependency facts:** anchor-ensure code uses the Stage S3 policy,
probe and state helpers, identity, transport, the Stage S4
`_image_calibration_uuid`, and `_metadata_only_microscope_image_payload`, and
does not call image upload. The promotion helpers and
`_ensure_metadata_anchors_for_public_spore_observation` are called *from* image
and measurement push (`_push_images_for_observation`,
`_push_measurements_for_observation`), so anchors must have their owner before
images move. `_retire_unneeded_owner_sync_parent` queues a tombstone for an
owner-sync parent; `_push_pending_image_tombstones` performs the soft delete.

**Hard constraints:**
- **Canonical names.** The canonical policy functions named in
  `.claude/rules/cloud-sync.md` (`_reconcile_local_image_cloud_id`,
  `_resolve_existing_observation_for_push`, `_resolve_existing_image_for_push`)
  keep their names. They stay reachable from the facade and the client.
- **Client resolution.** `SporelyCloudClient` remains the class callers
  construct. Its method resolution is unchanged, and every method stays
  classified in the pull-only lists.
- **Behavior-preserving.** Remote and local writes, their ordering, error
  text, result-dict keys and progress phases are unchanged. This holds
  especially for identity fail-closed paths and anchor promotion order.
- **Facade compatibility.** Every base name stays importable as the same
  object, and the facade never redefines a moved name.
- **Import direction.** No owner imports the facade except entries on the
  enumerated allowlist, each with a reason and a retirement condition. The
  import graph is acyclic.
- **Dependency closure.** A definition moves only when every module-level
  name it uses at runtime is owned by this stage or an accepted earlier one,
  or comes from outside `utils.cloud_sync`. A helper this stage's moved code
  needs moves with it, to the lowest owner that fits its responsibility; the
  stage notes record it. A definition that still needs facade-resident code at
  runtime stays in the facade, and the stage notes name the blocking
  dependency.
- **Dirty and sync-status writes.** Every call of `mark_observation_dirty`,
  `mark_observation_media_dirty` or `mark_observation_sync_dirty`, and every
  `sync_status` write, is unchanged in count and condition across the facade
  and owners together.
- **Patched tests keep working.** A test that patched a relocated name must
  still affect the code it exercises.
- **Base drift.** Redo relocations rather than hand-merging. The equivalence
  evidence is computed against the actual base.
- **Documentation.** `docs/cloud-sync-architecture.md` ownership entries point
  to the new owners.

**Implementation freedom:** module names, mixin structure, commit structure,
and patch retargeting strategy.

**Invariants at risk:**
- A verified local `cloud_id` is the primary push identity, and remote
  `desktop_id` is recovery only. Disagreement, ambiguity, soft-deleted matches
  and unique-violation races fail closed: never POST, never reparent.
- Taxonomy identity is untouched by this stage: no moved image-identity or
  anchor code changes how observation taxonomy identity is read or written.
- Metadata-only microscope anchors are valid rows. Owner-sync parents exist
  only when `_owner_sync_parents_supported` confirms support (fail closed).
  Byte selection stays independent of measurement and mosaic participation.
- Anchor promotion reuses the existing row and keeps its order: pending
  marker, then the reserve PATCH conditional on `storage_path IS NULL`, then
  upload, then finalize or a rollback of the exact reserved key. A `None`
  upload return is a failure, and a reserved path never proves bytes.
- Explicit user removal stays the only source of routine cloud image
  deletion. Retiring an owner-sync parent keeps its current conditions, and
  no new path can queue a cloud image tombstone.
- Pull-only performs zero cloud writes, and every moved client method stays
  classified as a read or a write.
- A fast no-op sync performs zero remote writes; anchor ensure and owner-sync
  code issue no write when nothing changed.

**Live-canary determination.** The canary is not part of this stage's
acceptance. The review records whether the live-canary condition applies,
with the evidence. The condition applies when the candidate changes code on an
identity or anchor remote-write path in any way other than a relocation the
equivalence check reports *identical*. The person acts on that record at
Gate G1, and an agent never performs the canary or any live Supabase write.

**Acceptance evidence:**
- The relocation-equivalence check accepted in Stage S2 (invocation in
  `docs/cloud-sync-architecture.md`) is clean over `base..candidate` except
  listed, justified items,
  including MRO resolution for every moved client method.
- The import-direction and facade-identity tests are extended to the new
  owners.
- For every relocated name, a search of `tests/` for patches of it (attribute
  and string targets) shows each one retargeted to the owner or routed through
  the same-object helper.
- The stage notes trace, from the moved code itself, image push identity from
  `push_all` through `_resolve_existing_image_for_push`. Every disagreement
  path raises, and none reaches a POST.
- They trace owner-sync parent creation and show it still depends on
  `_owner_sync_parents_supported`.
- They confirm the anchor promotion order above.
- A base-versus-candidate search shows the dirty-marking calls and
  `sync_status` writes unchanged in count and condition.
- The stage notes list each definition kept in the facade under the
  dependency-closure rule, with its blocking dependency.
- The review records the live-canary determination above, with evidence.
- The Stage S2 taxonomy identity golden passes, and its fixture is unchanged.
- These pass: `tests/test_image_push_identity.py`,
  `tests/test_observation_push_identity.py`,
  `tests/test_cloud_identity_fail_closed.py`,
  `tests/test_portable_cloud_identity_guard.py`,
  `tests/test_cloud_sync_no_baseline_identity_contradiction.py`,
  `tests/test_cloud_anchor_promotion.py`,
  `tests/test_cloud_sync_metadata_only.py`,
  `tests/test_cloud_media_measurement_mosaic_chain.py`,
  `tests/test_image_tombstones.py`,
  `tests/test_cloud_download_only.py`, `tests/test_cloud_sync_fast_path.py`,
  and the broad gate. The broad gate
  command is
  `/Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/pytest -q $(grep -rlE "utils\.cloud_sync|from utils import cloud_sync" tests --include='test_*.py')`,
  and its pass count must not fall below the previous stage's.

## Gate G1 — Identity and anchor live canary

**Kind:** a manual canary check that only the person performs. Agents never
perform it and never perform live Supabase writes.

**Position:** this canary gate follows Stage S5 and holds Stage S6: after
Stage S5 is accepted, do not start Stage S6 until this gate records `pass`.
Stage S6 moves image push, which calls the image identity resolution and the
anchor promotion helpers that Stage S5 relocated.

**Check `identity-anchor-canary`:**
1. Read the live-canary determination in Stage S5's accepted review and stage
   notes.
2. If it states, with evidence, that the condition does not apply, record
   `pass` and cite that evidence. Nothing else is needed.
3. If the condition applies, or the record does not settle it, run the canary:
   1. Make a fresh SQLite backup.
   2. Run `tools/cloud_reconciliation_report.py` (read-only). Categories C,
      D1, D2, E and H should be at zero, or have a documented exception.
   3. Run a deliberate Sync Now with the Stage S5 candidate build on a
      disposable or known account. Record the account, database and build.
   4. Rerun the report and diff it against the first run.
   5. Do not combine the canary with cleanup or garbage collection.

**Pass criteria:** either step 2 applies, or the sync completed without new
errors and the report diff shows no new discrepancy in categories C, D1, D2,
E or H.

**Fail or blocked:** either result keeps Stage S6 from starting. A failure is
repaired as renewed Stage S5 candidate work with a fresh review, and this
check is then answered for the repaired candidate. Nobody records `pass` over
failed canary evidence.

## Stage S6 — Images, measurements and derived products

**Prerequisite:** the Stage S3, S4 and S5 owners are accepted, and Gate G1
has recorded `pass`. Names an accepted earlier stage already moved stay where
they are.

**Outcome:** the remaining image, measurement and derived-product owners leave
`utils/cloud_sync.py`, with no behavior change:
- **Images:**
  - `_push_images_for_observation` and its helpers;
  - `_promote_temp_imported_image_if_needed`, `_remote_images_missing_locally`,
    `_apply_remote_images_to_local`, `_import_remote_images`,
    `_sync_existing_remote_image_to_local` and
    `_apply_remote_image_metadata_only_to_local`;
  - the local, pull-side anchor insert
    `_ensure_local_metadata_only_microscope_anchor`, with the image-apply
    helpers it shares with image pull (`_remote_ai_crop_box`,
    `_remote_ai_crop_is_custom`, `_remote_ai_crop_source_size`,
    `_cloud_image_captured_at_to_local`, `_remote_image_desktop_id_current`,
    `_update_image_columns_without_touching_observation`);
  - materialization state (`cloud_media_materialization_state_for_observation`)
    and the mosaic signature carry-forward (`_carry_forward_local_mosaic_signature`,
    whose only caller is the image pull working-file swap);
  - EXIF inject and backfill;
  - original recovery: `recover_full_original_for_image` and the original
    recovery cache helpers (~L1167–1400). Every caller of the cache is that
    recovery path;
  - the pending-image repair scan
    (`_mark_cloud_observations_dirty_for_pending_local_images`,
    `_pending_cloud_pushable_image_ids`) together with
    `_CLOUD_PENDING_IMAGE_REPAIR_VERSION`.
- **Measurements:** the remote measurement identity cache,
  `_push_measurements_for_observation`, and the measurement reconcile and
  import helpers.
- **Derived products:** the summary and mosaic glue that calls
  `spore_summary_sync` and `cloud_spore_mosaic`, including
  `_push_spore_mosaic_for_observation`.

**Verified dependency facts:**
- Measurement push needs image `cloud_id`s, metadata anchors, tombstones and
  the portable identity guard. Measurement import calls
  `_apply_remote_images_to_local`. Images therefore get their owner before
  measurements.
- `_import_remote_measurements_for_observation` and
  `materialize_cloud_media_for_observation` construct
  `SporelyCloudClient.from_stored_credentials()` at runtime when no client is
  passed. The class stays in the facade, so these two entry points stay in
  the facade under the dependency-closure rule. Their helpers move.
- The observation local media signature and `_load_local_measurement_lookup`
  were moved by Stage S4. The image-too-large helpers were moved by Stage S3.

**Hard constraints:**
- **Repair constant.** `_CLOUD_PENDING_IMAGE_REPAIR_VERSION` keeps its value
  and lives with the repair scan.
- **Image push order.** Intent initialization → identity and link →
  tombstone/protection filter → prep → metadata reserve or create → byte
  upload → metadata finalize → local `cloud_id` bookkeeping. `prepared_items`
  never becomes desired-state truth.
- **Dirty and sync-status writes.** Every call of `mark_observation_dirty`,
  `mark_observation_media_dirty` or `mark_observation_sync_dirty` (the repair
  scan and derived-product code call the last directly), and every
  `sync_status` write, is unchanged in count and condition across the facade
  and owners together. Removing deep dirty marks is follow-up work.
- **Behavior-preserving.** Remote and local writes, their ordering, error
  text, result-dict keys and progress phases are unchanged.
- **Facade compatibility.** Every base name stays importable as the same
  object, and the facade never redefines a moved name.
- **Import direction.** No owner imports the facade except entries on the
  enumerated allowlist, each with a reason and a retirement condition. The
  import graph is acyclic.
- **Dependency closure.** A definition moves only when every module-level
  name it uses at runtime is owned by this stage or an accepted earlier one,
  or comes from outside `utils.cloud_sync`. A helper this stage's moved code
  needs moves with it, to the lowest owner that fits its responsibility; the
  stage notes record it. A definition that still needs facade-resident code at
  runtime stays in the facade, and the stage notes name the blocking
  dependency.
- **Patched tests keep working.** A test that patched a relocated name must
  still affect the code it exercises.
- **Base drift.** Redo relocations rather than hand-merging. The equivalence
  evidence is computed against the actual base.
- **Documentation.** `docs/cloud-sync-architecture.md` ownership and
  navigation entries point to the new owners.

**Implementation freedom:** module names, split between image sub-owners,
commit structure, and patch retargeting strategy.

**Invariants at risk:**
- Local SQLite decides which image bytes are desired; only ledger membership
  proves initialized intent.
- Explicit user removal is the only source of routine cloud image deletion.
  Omission, filtering, prep failure, missing files and partial reads never
  are; image push and pull add no deletion path.
- A verified local `cloud_id` stays the primary identity in image and
  measurement push; disagreement fails closed (never POST, never reparent).
- Metadata-only anchors are valid rows, owner-sync anchors keep their parents,
  and byte selection stays independent of measurement and mosaic
  participation.
- Anchor promotion keeps its order from image push: pending marker, then
  the conditional reserve, then upload, then finalize or exact-key rollback.
- A render signature is not upload completeness, and the repair version is
  unchanged.
- Pull-only performs zero cloud writes: image pull, materialization and
  measurement import keep their read-only behavior on pull-only paths, and
  every moved client method stays classified.
- Partial or bounded remote collections are never authoritative.
- The snapshot is never written after truncated reads or incomplete required
  work (materialization stores a snapshot).
- A required child failure stays retryable and visible.
- Recovery-cache bytes are never re-uploaded, and originals are never deleted
  or downgraded by cloud-side disappearance.
- The mosaic signature survives the working-file swap, and publication
  selection is not part of the mosaic key.
- A fast no-op sync performs zero remote writes.
- The `sync_all` caller-mode flags (`sync_images`, `materialize_remote_images`,
  `full_pull`, `child_safety_pull`, `pull_only`) keep their meaning for the
  moved image and measurement code.

**Live-canary determination.** The canary is not part of this stage's
acceptance. The review records whether the live-canary condition applies,
with the evidence. The condition applies when the candidate changes code on an
identity or anchor remote-write path in any way other than a relocation the
equivalence check reports *identical*. Image push calls the anchor promotion
helpers, so that call path counts. The person acts on that record at
Completion gate G2, and an agent never performs the canary or any live
Supabase write.

**Acceptance evidence:**
- The relocation-equivalence check accepted in Stage S2 (invocation in
  `docs/cloud-sync-architecture.md`) is clean over `base..candidate` except
  listed, justified items.
- The import-direction and facade-identity tests are extended to the new
  owners.
- For every relocated name, a search of `tests/` for patches of it (attribute
  and string targets) shows each one retargeted to the owner or routed through
  the same-object helper.
- A base-versus-candidate search shows the dirty-marking calls and
  `sync_status` writes unchanged in count and condition.
- The stage notes list each definition kept in the facade under the
  dependency-closure rule, with its blocking dependency.
- The stage notes read the moved `_push_images_for_observation` and confirm
  the order above.
- They trace deletion intent from the gallery checkbox to
  `_push_pending_image_tombstones` and show that no new path can delete a
  cloud image.
- The review records the live-canary determination above, with evidence.
- These pass:
  - `tests/test_cloud_sync_image_upload_policy.py`,
    `tests/test_cloud_media_pull_retry.py`,
    `tests/test_cloud_spore_mosaic_signature.py`,
    `tests/test_cloud_spore_mosaic_unchanged_sync.py`,
    `tests/test_cloud_measurement_sync_v1.py`,
    `tests/test_sync_observation_dirty_propagation.py`,
    `tests/test_cloud_media_measurement_mosaic_chain.py`,
    `tests/test_cloud_anchor_promotion.py`,
    `tests/test_image_push_identity.py`,
    `tests/test_image_tombstones.py`;
  - `tests/test_cloud_original_sync_recovery.py`,
    `tests/test_cloud_original_sync_upload.py`,
    `tests/test_cloud_media_recovery.py`, `tests/test_spore_summary_sync.py`;
  - `tests/test_cloud_sync_fast_path.py`,
    `tests/test_cloud_sync_dirty_loop_steady_state.py`,
    `tests/test_cloud_download_only.py`;
  - the broad gate. Its command is
    `/Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/pytest -q $(grep -rlE "utils\.cloud_sync|from utils import cloud_sync" tests --include='test_*.py')`,
    and its pass count must not fall below the previous stage's.

## Stage S7 — Conflict detail and conflict-plan model

**Prerequisite:** the Stage S3–S6 owners are accepted. Names an accepted
earlier stage already moved stay where they are.

**Outcome:** read-only conflict detail and the conflict-plan model and
validation get their own owner, with no behavior change. This scope is
settled and narrow. Conflict *execution* stays in the facade, together with
every observation-completion writer, every decision to store a snapshot, and
every conflict-review marker writer. The orchestration implementation moves
those under the Stage S1 design. This stage moves no writer.
- **Conflict detail:** `get_conflict_detail` and `_observation_display_name`.
- **Conflict-plan model:** the baseline (`build_conflict_plan_baseline`,
  `_CONFLICT_PLAN_BASELINE_SCHEMA_VERSION`, the
  `_conflict_plan_*_fingerprint` and `_find_*_fingerprint` helpers), drift and
  shape checks (`_plan_drift_message`, `_validate_plan_baseline_shape`,
  `_verify_plan_baseline`, `_validate_plan_identity_state`), operation
  building (`_build_plan_operations`, `_build_plan_from_automatic_decisions`,
  `_stable_op_key`, `_plan_dispatch_keys`, `_plan_item_matches_completed_op`),
  retry-state validation (`_malformed_retry_state`,
  `_validate_prior_op_expected_after`, `_require_full_material_expected`,
  `_reconcile_verification_pending_op`, `_verify_completed_ops_and_rebase`),
  expected and intended material state (`_material_*_expected_state`,
  `_material_*_current_state`, `_intended_after_*`,
  `_normalize_observation_field_for_baseline`, `_mosaic_render_state_unverified`),
  and `_build_accepted_asymmetry_from_plan` with `_iso_timestamp_now`.
- **Stays in the facade** (conflict execution; the orchestration follow-up
  extracts it under the Stage S1 design): `resolve_conflict_keep_local`,
  `resolve_conflict_keep_cloud`, `resolve_conflict_merge`,
  `resolve_conflict_plan`, `finalize_sync_candidates`, and the helpers only
  they use that write local state or format their results
  (`_capture_local_presentation`, `_restore_local_presentation`,
  `_assign_downloaded_image_order`, `_format_recomputed_spore_statistics`).
- **Writers that also stay in the facade:** observation completion
  (`_stamp_observation_synced`, `_set_observation_sync_state`,
  `_clear_observation_dirty_if_no_real_changes`,
  `_set_observation_privacy_blocked`, `_apply_remote_observation_fields`,
  `_merge_cloud_selected_ai_fields`, `_adopt_merge_filled_ai_fields_locally`
  and the direct `update_observation_sync_state` stamps in `push_all`);
  conflict-review markers (`_set_observation_conflict_review_pending`,
  `_clear_observation_conflict_review_pending`). Snapshot storage functions
  are owned by the Stage S4 baseline owner, but every call that decides to
  store a snapshot stays in the facade's push, pull, conflict-execution and
  materialization code.

After this stage the facade holds orchestration (`push_all`, `pull_all`,
`sync_all` and their coordinator-scale helpers), the observation completion
writers, conflict execution, the default-client entry points kept by
Stage S6, `SporelyCloudClient`'s class definition, and documented
compatibility glue.

**Verified dependency facts:**
- `get_conflict_detail` calls `build_conflict_plan_baseline`, the Stage S4
  identity classification, compare, snapshot, accepted-asymmetry filter and
  `_load_local_measurement_lookup` helpers, the Stage S3 remote measurement
  read, and `client.pull_image_metadata`. It performs no write.
- The plan-model helpers call only Stage S3 and S4 code and each other; none
  calls a completion writer, the cloud client or image/measurement push.
- Conflict execution calls `_push_images_for_observation`,
  `_push_measurements_for_observation`, `_push_spore_mosaic_for_observation`,
  `_apply_remote_images_to_local` and `_import_remote_measurements_for_observation`,
  and also the completion writers that stay in the facade:
  `_stamp_observation_synced`, `_apply_remote_observation_fields`,
  `_clear_observation_conflict_review_pending`,
  `_set_observation_privacy_blocked`, `_merge_cloud_selected_ai_fields` and
  `_adopt_merge_filled_ai_fields_locally`. Moving it would need an upward
  facade import, so it stays; it imports the plan model downward.
- `ui/cloud_conflict_dialog.py` imports `get_conflict_detail` and the
  `resolve_conflict_*` functions from the facade.

**Hard constraints:**
- **Unchanged:** `_CONFLICT_PLAN_BASELINE_SCHEMA_VERSION`, fingerprint output,
  operation keys, persisted plan and snapshot JSON, and the order in which
  conflict execution calls the moved model.
- **Behavior-preserving.** Remote and local writes, their ordering, error
  text, result-dict keys and progress phases are unchanged.
  `get_conflict_detail` stays read-only.
- **Facade compatibility.** Every base name stays importable as the same
  object, including the public `get_conflict_detail` and
  `build_conflict_plan_baseline`, and the facade never redefines a moved name.
- **Import direction.** No owner imports the facade except entries on the
  enumerated allowlist, each with a reason and a retirement condition. The
  import graph is acyclic, and the baseline owner does not import the
  conflict-plan owner.
- **Dependency closure.** A definition moves only when every module-level
  name it uses at runtime is owned by this stage or an accepted earlier one,
  or comes from outside `utils.cloud_sync`. A definition that still needs
  facade-resident code at runtime stays in the facade, and the stage notes
  name the blocking dependency.
- **Dirty and sync-status writes.** Every call of `mark_observation_dirty`,
  `mark_observation_media_dirty` or `mark_observation_sync_dirty`, and every
  `sync_status` write, is unchanged in count and condition across the facade
  and owners together.
- **Patched tests keep working.** A test that patched a relocated name must
  still affect the code it exercises.
- **Base drift.** Redo relocations rather than hand-merging. The equivalence
  evidence is computed against the actual base.
- **Documentation.** `docs/cloud-sync-architecture.md` ownership entries point
  to the new owner and state that conflict execution remains in the facade.

**Implementation freedom:** module names, whether detail and plan model share
one module, commit structure, and patch retargeting strategy.

**Invariants at risk:**
- In conflict plans, reviewed-baseline drift aborts the apply, partial retries
  are idempotent, and media deletion is unreachable from a plan.
- Plan identity validation fails closed: disagreement or ambiguity never leads
  to a POST or a reparent.
- Taxonomy identity contract in conflict detail and plans: structured taxonomy
  identity (`sporely_taxon_id` with its source and namespace semantics) is
  authoritative, and display, scientific and vernacular text never substitutes
  for it. Identity participates in conflict detection, and a no-baseline
  local/remote taxonomy contradiction fails closed. The plan baseline and
  snapshot represent identity exactly as before.
- Representation-only differences never conflict, and genuine three-way
  divergence always does.
- The snapshot and the plan baseline are never accepted from truncated reads,
  unresolved conflicts or ambiguous identity, and accepted asymmetry carries
  through unchanged.
- Partial or bounded remote collections read for detail are never
  authoritative.
- Conflict detail performs no cloud or local write, so pull-only and fast
  no-op syncs that reach it stay write-free.

**Live-canary determination.** The canary is not part of this stage's
acceptance. The review records whether the live-canary condition applies,
with the evidence. The condition applies when the candidate changes code on an
identity or anchor remote-write path, or code that conflict execution uses to
decide its remote writes, in any way other than a relocation the equivalence
check reports *identical*. The person acts on that record at Completion
gate G2, and an agent never performs the canary or any live Supabase write.

**Acceptance evidence:**
- The relocation-equivalence check accepted in Stage S2 (invocation in
  `docs/cloud-sync-architecture.md`) is clean over `base..candidate` except
  listed, justified items.
- The import-direction and facade-identity tests are extended to the new
  owner.
- For every relocated name, a search of `tests/` for patches of it (attribute
  and string targets) shows each one retargeted to the owner or routed through
  the same-object helper.
- Conflict-plan fixtures (baseline drift, partial retry, accepted-asymmetry
  carry) produce byte-identical persisted snapshot and plan JSON at base and
  candidate, and drift still aborts the apply.
- A no-baseline taxonomy contradiction still fails closed in conflict detail.
- The Stage S2 taxonomy identity golden passes, and its fixture is unchanged.
- A base-versus-candidate search shows no observation-completion writer,
  conflict-review marker writer or snapshot-store call site moved out of the
  facade.
- The stage notes trace each `resolve_conflict_*` function and show that its
  calls into the moved model are unchanged in arguments and order.
- A base-versus-candidate search shows the dirty-marking calls and
  `sync_status` writes unchanged in count and condition.
- The stage notes list each definition kept in the facade under the
  dependency-closure rule, with its blocking dependency.
- The review records the live-canary determination above, with evidence.
- These pass: `tests/test_cloud_conflict_plan_execution.py`,
  `tests/test_cloud_conflict_dialog.py`,
  `tests/test_cloud_sync_conflict_preflight.py`,
  `tests/test_image_conflict_normalization.py`,
  `tests/test_cloud_sync_no_baseline_identity_contradiction.py`,
  `tests/test_cloud_identity_fail_closed.py`,
  `tests/test_cloud_identity_change_detection.py`,
  `tests/test_cloud_download_only.py`, `tests/test_cloud_sync_fast_path.py`,
  and the broad gate. Its command is
  `/Users/sigmundas/Documents/Code/sporely/sporely-py/.venv/bin/pytest -q $(grep -rlE "utils\.cloud_sync|from utils import cloud_sync" tests --include='test_*.py')`,
  and its pass count must not fall below the previous stage's.

## Completion gate G2 — Final extraction live canary

**Kind:** a manual canary check that only the person performs. Agents never
perform it and never perform live Supabase writes.

**Position:** this canary gate follows Stage S7 and holds plan completion:
after Stage S7 is accepted, the plan does not complete until this gate
records `pass`. The orchestration implementation plan starts only from a
completed run of this plan, so this gate also holds it.

**Check `final-extraction-canary`:**
1. Read the live-canary determinations in the accepted reviews and stage
   notes of Stage S6 and Stage S7.
2. If both state, with evidence, that the condition does not apply, record
   `pass` and cite that evidence. Nothing else is needed.
3. Otherwise, or if either record does not settle it, run the canary with the
   final candidate build, which is Stage S7's accepted candidate and contains
   Stage S6:
   1. Make a fresh SQLite backup.
   2. Run `tools/cloud_reconciliation_report.py` (read-only). Categories C,
      D1, D2, E and H should be at zero, or have a documented exception.
   3. Run a deliberate Sync Now with that build on a disposable or known
      account. Record the account, database and build.
   4. Rerun the report and diff it against the first run.
   5. Do not combine the canary with cleanup or garbage collection.

**Pass criteria:** either step 2 applies, or the sync completed without new
errors and the report diff shows no new discrepancy in categories C, D1, D2,
E or H.

**Fail or blocked:** either result keeps this plan from completing, and with
it the orchestration implementation plan. A failure is repaired as new
reviewed candidate work on the code responsible, and this check is then
answered for the repaired build. Nobody records `pass` over failed canary
evidence.
