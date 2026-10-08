# Cloud sync orchestration: observation completion design

Status: **decision document (Stage S1 of
`docs/plans/active/2026-10-07-cloud-sync-extraction-and-orchestration.md`).**
It changes no code. Section 1 to section 7 record current behaviour and a
proposed target. Section 8 lists the decisions that the person must make
before the orchestration follow-up plan is written.

Evidence base: `feature/cloud-sync-extraction-and-orchestration` at `861e19d`
(`utils/cloud_sync.py` is the same as at `c9d655f`). Code is cited by symbol
and file. Line numbers (`~L…`) are hints only, because the extraction stages
move code.

Vocabulary used throughout:

- **Completion state:** `observations.sync_status` (`synced`, `dirty`,
  `blocked`, NULL), `synced_at`, the error columns (`sync_error_code`,
  `sync_error_message`, `sync_blocked_reason`, `sync_blocked_at`) and the
  observation snapshot (settings key from `_cloud_observation_snapshot_key`).
- **Review marker:** `sync_blocked_reason = CONFLICT_REVIEW_PENDING_MARKER`
  with `sync_status='dirty'`.
- **Required work** and **best-effort work** are defined per caller mode in
  section 2.

---

## 1. Current completion state machine

### 1.1 Low-level writers (the primitives)

Every write to `observations.sync_status` goes through one of these
primitives. A repository-wide search (`sync_status` writes,
`update_observation_sync_state(`, `mark_observation_sync_dirty(`,
`_store_remote_snapshot(`, `_store_cloud_observation_snapshot(`, raw
`UPDATE observations SET … sync_status`) finds no other writer. The reference
library (`database/reference_*`, `utils/reference_cloud_sync.py`,
`utils/curated_reference_sync.py`) has its own `sync_status` columns on other
tables. Those are out of scope.

| Primitive | File | Writes | Notes |
| --- | --- | --- | --- |
| `update_observation_sync_state` | `database/models.py` | any of `cloud_id`, `sync_status`, `synced_at`, the error columns | Generic column setter. No policy. |
| `mark_observation_sync_dirty(cursor, id)` | `database/models.py` | `sync_status='dirty'` and clears error columns, but **only** if `cloud_id IS NOT NULL OR sync_status='blocked'` | Local-edit signal. Needs a cursor (two arguments). |
| `reset_cloud_sync_state` | `database/models.py` | every observation: `cloud_id=NULL`, `sync_status='dirty'`, `synced_at=NULL` | Account reset. Out of scope for this plan. |
| `recalculate_measurements_for_calibration` | `database/models.py` (~L5105) | raw `UPDATE … sync_status='dirty' WHERE cloud_id IS NOT NULL` | Local edit (calibration rescale). Bypasses `mark_observation_sync_dirty`, so it does not clear the error columns. |
| `unlink_local_observation_from_cloud` | `utils/cloud_sync.py` | raw `UPDATE`: `cloud_id`, `sync_status`, `synced_at` = NULL; clears the snapshot | Explicit unlink. Out of scope. |
| `_store_cloud_observation_snapshot` | `utils/cloud_sync.py` | snapshot setting | Only caller: `_store_remote_snapshot`. |
| `_store_remote_snapshot` | `utils/cloud_sync.py` | snapshot setting | Re-reads `get_observation`, image metadata and measurements when not supplied. It reconciles `accepted_asymmetry`. It can raise. |
| `_clear_cloud_observation_snapshot` | `utils/cloud_sync.py` | removes the snapshot | Only caller: `unlink_local_observation_from_cloud`. |

Observation-level helpers in `utils/cloud_sync.py` that wrap these primitives:

| Helper | Effect |
| --- | --- |
| `_set_observation_sync_state(id, cloud_id, dirty=…, synced_at=…)` | `synced` or `dirty`, sets `cloud_id` and `synced_at`, **clears all error columns**, including the review marker. |
| `_stamp_observation_synced` | `_set_observation_sync_state(dirty=False)`. |
| `_clear_observation_dirty_if_no_real_changes` | Writes `synced` and clears errors when `_local_has_real_changes_since_snapshot` is false. **Does not store a snapshot.** |
| `_set_observation_conflict_review_pending` | `dirty` + review marker. |
| `_clear_observation_conflict_review_pending` | Clears the marker only. |
| `_set_observation_sync_blocked` / `_set_observation_privacy_blocked` | **`sync_status='blocked'`** with error code and reason. (The stage brief lists the privacy writer as writing `dirty`. The code writes `blocked`.) |
| `_set_observation_plan_image_retryable` | `dirty` + `image_too_large_for_plan` error, clears blocked reason. |
| `_set_observation_sync_error_detail_only` | Error code and message only. Status and marker are unchanged. |
| `mark_observation_dirty` | Opens a connection and calls `mark_observation_sync_dirty`. |
| `mark_observation_media_dirty` | Alias of `mark_observation_dirty` (checkbox lifecycle). |

### 1.2 Local-edit dirty signals (not completion writers)

These writers record that local data changed. They do not decide sync
completion, and the target model keeps them as they are (section 3.5):

- `database/models.py::_touch_observation(mark_dirty=True)`. It has 20 call
  sites in `database/models.py`: image, measurement and annotation
  create/update/delete, observation merges and image insert
  (`add_image(mark_observation_dirty=True)`).
- `database/models.py::update_observation` → `mark_observation_sync_dirty`.
- `database/models.py::reconcile_legacy_publish_exclusion_tombstones` →
  `mark_observation_sync_dirty`.
- `database/models.py` calibration rescale (raw `UPDATE`, above).
- `ui/main_window.py` (~L8604) → `mark_observation_dirty`.
- `utils/cloud_sync.py::set_image_cloud_selected` →
  `mark_observation_media_dirty` (restore, upload queued, anchor promotion).

### 1.3 Sync-time dirty scans (preflight of `push_all`)

| Writer | Trigger | Effect |
| --- | --- | --- |
| `_mark_cloud_observations_dirty_for_media_changes` | every `push_all` | No-op (kept as a hook; see its comment). |
| `_mark_cloud_observations_dirty_for_image_capture_time_changes` | `push_all(sync_images=True)` | `mark_observation_dirty` for linked observations whose image `captured_at` differs from the stored media signature. |
| `_mark_cloud_observations_dirty_for_pending_local_images` | `push_all(sync_images=True)` when `_cloud_pending_image_repair_scan_due` | Clears the local media signature, then `mark_observation_sync_dirty(cursor, id)`. |

### 1.4 Push: `push_all`

Per dirty or unlinked observation (`cloud_id IS NULL OR sync_status='dirty'`),
in this order:

1. **Preflight read** (with a stored snapshot and a remote row). It reads image
   metadata and measurements and builds the remote snapshot.
   - If remote differs from the snapshot and `_clear_observation_dirty_if_no_real_changes`
     succeeds, the observation is written `synced`, **no new snapshot is
     stored**, and it is skipped. The stale baseline stays. The next pull sees
     remote ≠ snapshot and handles the change.
   - If the three-way preflight (`_analyze_observation_push_conflicts`) reports a
     conflict, the result is `_set_observation_conflict_review_pending` and an
     error string (`_format_review_needed_error`). Nothing is pushed.
   - Ordinary remote-only fields are adopted locally
     (`_apply_remote_observation_fields`) before the push (contract, "One side
     changed").
2. **Case F, no-baseline identity contradiction:** review marker plus error.
   Only a narrowing visibility PATCH (`_push_narrower_visibility_while_blocked`)
   may go out. Its failure is recorded through `_set_observation_sync_error_detail_only`.
3. `client.push_observation`.
4. **Stamp.** `update_observation_sync_state(sync_status='synced', synced_at=now,
   clear_sync_error_state=True)` (~L21012). This happens **before any child
   work**. If `identity_review_pending` (no baseline, identity conflict), the
   row is immediately re-set to `dirty` with the review marker.
5. `_finalize_portable_cloud_identity_guard` (portable imports only).
6. **Child work**, when `sync_images=True`:
   - image upload (`_push_images_for_observation`, or a fast path gated by
     upload completeness). On failure: `mark_observation_dirty` plus an error.
   - measurement push (only when images succeeded). On failure:
     `mark_observation_dirty` plus an error.
   - spore mosaic. **Best-effort.** A failure is only logged; it is not added
     to `errors` and does not make the observation dirty.
   - on success, `_refresh_local_cloud_media_signature`.
7. **Child work**, when `sync_images=False`: a metadata-only image PATCH for
   drifted linked images. On a PATCH failure: `mark_observation_dirty`. Then the
   media signature is refreshed unconditionally.
8. **Spore summary** (`_push_summary_for_current_observation`, every mode). On
   failure, the error is added to `errors` and the code *attempts*
   `mark_observation_sync_dirty(id)`. **Defect:** the call passes one argument,
   but the function needs `(cursor, id)`. The resulting `TypeError` is
   swallowed by `except Exception: pass`. **A summary failure therefore leaves
   the observation `synced`.** `tests/test_spore_summary_sync.py::test_call_site_unexpected_error_recorded_in_errors_list`
   monkeypatches a one-argument fake, which hides the defect.
9. **Snapshot.** `_store_remote_snapshot(client, cloud_id)` (~L21493), or a
   snapshot without identity when `identity_review_pending`. This runs
   **even after a child failure marked the row dirty**: the baseline advances
   past incomplete required work. If the snapshot store raises a
   `CloudSyncError`, the outer handler (item 10) marks the row dirty in its
   generic branch. Any other exception aborts `push_all`, and the row keeps
   the `synced` stamp from step 4 with the old snapshot.
10. **Exception handler** (`CloudSyncError`): privacy slot limit →
    `blocked`; image too large for plan → `dirty` + error; WebP required →
    **no state write** (the row stays as the stamp left it, normally
    `synced`); identity-clear verification failed → review marker; anything
    else → `mark_observation_dirty`. Exceptions other than `CloudSyncError`
    propagate and abort the whole push.

After the loop come the backfill passes:

- `_reconcile_missing_spore_measurements`. On a per-observation failure it
  makes the same broken one-argument `mark_observation_sync_dirty(local_id)`
  call, so the failure is only recorded in `errors`. The masking test is
  `tests/test_spore_summary_sync.py::test_measurement_reconcile_records_per_observation_errors`.
- `_reconcile_missing_spore_summaries`. It only appends errors.

### 1.5 Pull: `pull_all` and `_create_local_from_remote`

Fast-path candidate pruning (`full_pull=False`):

- **Cheap convergence.** The remote `updated_at` is newer, the observation
  fields match the snapshot, identity is unchanged, and the local status is
  not `dirty`. The code calls `_stamp_observation_synced` and then
  `_store_remote_snapshot` with the previous images and measurements. **The
  stamp comes before the snapshot.** A snapshot failure is logged and
  swallowed. A `blocked` row passes the `!= 'dirty'` test and is
  re-stamped `synced`, which clears its blocked state. This is unverified by
  any test.

Per candidate (existing local row):

- `client.set_desktop_id` is called when the remote `desktop_id` differs and
  the run is not pull-only (a remote write; see section 5).
- If the row is dirty and `_clear_observation_dirty_if_no_real_changes`
  succeeds, the row is written `synced` with no snapshot. Processing then
  continues as clean.
- **Case F (pull side).** Review marker, error, no snapshot.
- **No baseline, remote changed.** Apply fields (identity fail-closed),
  images and measurements. The row becomes `dirty` on a measurement conflict,
  an identity conflict, or (when `materialize_remote_images`) missing media.
  Otherwise it is stamped `synced`. Then a snapshot is stored, **with images
  and measurements omitted** when media is pending or a measurement conflict
  exists.
- **Baseline, remote changed.**
  - When `removed_keys` is set ("cloud removed local image files"), the code
    adds an error and `continue`s. **No state write:** a clean row stays
    `synced`.
  - Otherwise it applies remote-only fields and remote images, and imports
    measurements. Conflict fields produce an error string. **The pull path never
    sets the review marker** for ordinary field conflicts.
  - The row becomes `dirty` when local changes remain, on a measurement
    conflict, on media still pending (only when materializing), or when a
    review marker is already present. Otherwise it is stamped `synced`.
  - A snapshot is stored unless there are conflict fields or a review marker.
    It is a full snapshot only when nothing remains.
- **Remote unchanged and local clean:** the row is re-stamped `synced`
  (which clears the error columns).
- **Missing-media retry** (stored snapshot, `materialize_remote_images`). It
  re-applies images and measurements. The row becomes `dirty` when media is
  still pending or a measurement conflict exists, otherwise `synced`.
- **Snapshot write.** It comes **after** all of the stamps above, and its
  exceptions go to the per-observation `except`. That `except` records the
  error. **The stamp is not undone**, so the row stays `synced` with the old
  baseline.

`_create_local_from_remote` (new remote observation):

1. Insert the row and write `dirty` with `synced_at=NULL`.
2. Run `_import_remote_images` and `_import_remote_measurements_for_observation`.
3. Stamp `synced` only when both are `complete` with no errors and no
   conflict. Otherwise the row stays `dirty`. When
   `materialize_remote_images=False`, every non-anchor image counts as
   `skipped_materialization` and makes the result incomplete.
4. Store the snapshot **after** the stamp: a partial snapshot (observation
   only) when incomplete. Snapshot exceptions are **swallowed**
   (`except Exception: pass`).

### 1.6 Conflict execution

| Function | Order | Failure states |
| --- | --- | --- |
| `resolve_conflict_keep_local` | push obs → **stamp `synced`** → images (when `prepare_images_cb`) → measurements → mosaic (best-effort) → snapshot → signature | Image failure: `mark_observation_dirty` + raise. A **measurement push failure raises after the stamp. The row stays `synced` with the old snapshot.** A privacy limit gives `blocked`. |
| `resolve_conflict_keep_cloud` | apply fields, images and measurements locally → stamp `synced` → signature → snapshot | A measurement `failed` raises before the stamp. Image application warnings do **not** block the stamp. A snapshot failure after the stamp leaves the row `synced` with the old baseline. |
| `resolve_conflict_merge` | apply remote images → push obs → **stamp `synced`** → images → snapshot | Image failure: dirty + raise. No measurement push. A privacy limit gives `blocked`. |
| `resolve_conflict_plan` | drift check → operations → mosaic (best-effort, recorded) → **snapshot → signature → stamp** | A snapshot failure raises `PartialConflictPlanError` before the stamp, so the conflict stays unsealed. **This is the only path that already has the target order.** |
| `finalize_sync_candidates` | runs `resolve_conflict_plan` for automatic decisions, rereads, and calls `_clear_observation_conflict_review_pending` when nothing manual remains | It does not write `sync_status` or the snapshot itself. |

### 1.7 Other writers

- **`materialize_cloud_media_for_observation`** (`ui/observations_tab.py`,
  on-demand media download). It never writes `sync_status`. It stores a
  **full** snapshot after the downloads **even when downloads failed or
  measurements conflict**, and it reports a snapshot failure as a warning.
  This replaces the baseline outside the completion owner.
- **`ui/observations_tab.py::_mark_cloud_observation_imported`** (manual
  "import cloud observation into a new local row"). It writes
  `sync_status='synced'`, `synced_at=datetime.now()` (local naive time; every
  other writer uses UTC), and clears errors **before** downloading images. It
  pairs local and cloud images by list position (`zip`), then calls
  `set_image_desktop_id` and `set_desktop_id` unconditionally. Exceptions are
  swallowed. **It stores no snapshot**, so the next pull classifies the row as
  "no baseline". This path does not use the stable-ID or no-op-write rules
  that the sync paths follow.

### 1.8 Where `docs/cloud-sync-architecture.md` disagreed with the code

Corrected in this stage. The corrected text points here.

| Section | Statement | Code |
| --- | --- | --- |
| C, "Remote snapshot storage" | Snapshot stored only "after required child work succeeds". | `push_all` stores it after child failure. `materialize_cloud_media_for_observation` stores it after download failure. |
| C, "sync_status transitions" | Four named helpers are "the only paths that flip dirty/synced"; "stamp only after ALL required child ops succeeded and the snapshot stored". | Many writers (1.1–1.7). Push, pull, keep-local, keep-cloud and merge stamp **before** the snapshot. Push stamps before child work. |
| H, "Written" | `finalize_sync_candidates` stores the snapshot before stamping. | That ordering is in `resolve_conflict_plan`. `finalize_sync_candidates` stores nothing. |
| I, bullets 1–2 | Stamp only after children and snapshot; per-observation push "skips the synced stamp" on child failure. | Push stamps first and compensates with `mark_observation_dirty`. Summary and measurement-reconcile compensation is broken (1.4). Mosaic failure is silent. |
| I, ordering item 2 | `finalize_sync_candidates` stores the snapshot before stamping. | That ordering is in `resolve_conflict_plan`. |

---

## 2. Required versus best-effort work by caller mode

### 2.1 Caller presets as they are wired today

| Caller | `sync_images` | `materialize_remote_images` | `full_pull` | `child_safety_pull` | `pull_only` |
| --- | --- | --- | --- | --- | --- |
| Observations tab Refresh (`_on_refresh_clicked` → `_start_cloud_sync`) | **True** | True | False | True (`_CloudAutoSyncWorker` default) | False |
| Profile & Cloud "Sync now" (`_request_settings_cloud_sync`) | True | True | False | True | False |
| Download from Cloud (button, `start_cloud_download`) | False (forced) | True / caller | True (forced) | False | True |
| Cloud sync dialog `_SyncWorker` | checkbox (default True) | checkbox | **True** (`sync_all` default) | False | False |
| Cloud sync dialog, pull-only | False | checkbox | True (forced in `sync_all`) | False | True |
| `_start_cloud_sync` defaults (no current caller passes them) | False | False | False | True | False |
| Local harness `tests/local_supabase/device.py::_sync` | False | False | True | False | either |

**Disagreement with the rules:** `.claude/rules/cloud-sync.md` says the
Observations-tab refresh uses `sync_images=False`. The code passes `True`,
so Refresh and "Sync now" are the same preset. Resolving this is decision D6
in section 8. `tests/test_cloud_sync_fast_path.py::test_start_cloud_sync_defaults_to_fast_path`
checks the defaults, not the Refresh preset.

### 2.2 Work, its current failure state and a proposed completion rule

"Dirty today?" describes current behaviour. "Proposed" is the target rule;
the bracketed options refer to section 8. Citations name a test or a
contract section. **Unverified** means no test pins the behaviour.

| # | Mode / flags | Work | Dirty today on failure? | Proposed: required for `synced`? | Evidence |
| --- | --- | --- | --- | --- | --- |
| P1 | push, any | observation PATCH/POST | yes (`mark_observation_dirty` in generic handler; WebP branch: **no**) | required | `test_sync_observation_dirty_propagation.py::test_gap_c_…`; WebP branch **unverified** |
| P2 | push, `sync_images=True` | selected image byte upload / anchor | yes | required | `test_gap_d_image_anchor_failure_obs_stays_dirty`; contract rule 8 |
| P3 | push, `sync_images=True` | measurement push | yes | required | `test_gap_e_measurement_push_failure_obs_stays_dirty`; contract rule 8 |
| P4 | push, `sync_images=True` | spore mosaic | no (logged only) | best-effort (D4) | contract "Public spore mosaic after conflict resolution"; plan-path `best_effort_failed` in `test_cloud_conflict_plan_execution.py`; ordinary push **unverified** |
| P5 | push, `sync_images=False` | metadata-only image PATCH | yes | required | `test_cloud_sync_dirty_loop_steady_state.py::test_metadata_only_refresh_patches_image_metadata_on_existing_cloud_rows` (success path only; failure **unverified**) |
| P6 | push, any | spore summary | **no** (broken call, 1.4) | required (contract rule 8 names "summary") | rule 8; masking test `test_spore_summary_sync.py::test_call_site_unexpected_error_recorded_in_errors_list` |
| P7 | push, any | pending tombstone flush | error only; tombstone stays unsynced and retries; the observation's status is not touched | **proposal, in tension with contract rule 8** (which names "deletion" work): keep the durable unsynced tombstone as the retry record, so a failure does not block the observation's `synced` (decision D9) | `test_cloud_sync_fast_path.py::test_push_all_surfaces_image_tombstone_failures`; contract rule 8, "Retry-safe sequencing" |
| P8 | push, any | measurement / summary backfill | error only (broken call) | required for the affected observation (D5) | `test_spore_summary_sync.py::test_measurement_reconcile_records_per_observation_errors` (fake) |
| P9 | push, any | calibration push | error only | not observation work. Separate issue type. | `test_cloud_calibration_sync.py`; rule 8 names "calibration" **unverified** per observation |
| P10 | push, any | snapshot store | `CloudSyncError`: dirty; other exceptions: `synced` and the push aborts | required; failure blocks `synced` (decided) | **unverified** for push |
| L1 | pull, any | field apply | n/a | required | contract "One side changed" |
| L2a | pull, `materialize=True`, existing row, **no baseline** | remote image bytes | yes: `remote_media_pending` includes `_remote_images_missing_locally`, so a failed download makes the row dirty | required (D1) | contract rule 7; failure path **unverified** |
| L2b | pull, `materialize=True`, existing row, **baseline, remote changed** | remote image bytes | not checked directly: here `remote_media_pending` counts missing local images only when `materialize=False`. A failed download is caught afterwards by the missing-media retry block (stored snapshot + `materialize`), which re-attempts and makes the row dirty if images are still missing | required (D1) | **unverified** |
| L2c | pull, `materialize=True`, `_create_local_from_remote` | remote image bytes | yes: an incomplete `_import_remote_images` result keeps the row dirty | required (D1) | **unverified** |
| L3a | pull, `materialize=False`, existing row | remote image bytes | not attempted. Missing bytes do not block `synced`; the row is stamped `synced` if nothing else remains (field, media or measurement conflict, review marker) | D1 | `test_cloud_download_only.py` (materialize toggles); per-branch status **unverified** |
| L3b | pull, `materialize=False`, `_create_local_from_remote` | remote image bytes | every non-anchor image counts as `skipped_materialization`, so a new row with such images stays **dirty**; a row with only anchors and no failures is stamped `synced` | D1 | **unverified** |
| L4a | pull, existing row (both branches) | measurement import | a **conflict** always makes the row dirty. A **failure or skipped materialization** only feeds `remote_media_pending`, which blocks completion only when `materialize=True`. With `materialize=False` the row can be stamped `synced` after a measurement failure | required (proposal: in every mode, independent of D1) | contract rule 8; **unverified** |
| L4b | pull, `_create_local_from_remote` | measurement import | any incomplete or failed result, error or conflict keeps the row dirty, in every mode | required | **unverified** |
| L5 | pull, any | snapshot store | no: row stays `synced` (push-side ordering) | required; failure blocks `synced` | `_create_local_from_remote` swallows failure, **unverified** |
| L6 | pull, `full_pull=False` | cheap convergence | stamps before snapshot; snapshot failure swallowed | required order: snapshot → `synced` | `test_cloud_sync_fast_path.py::test_fast_pull_converges_when_remote_updated_at_bumped_but_fields_unchanged` |
| L7 | pull, `child_safety_pull` due | metadata-only deep pull | as above | same as `full_pull` pull | `test_cloud_sync_fast_path.py::test_child_safety_pull_selects_deep_metadata_reconciliation_without_media` |
| L8 | `pull_only=True` | everything above, no writes | as above | completion must be reachable with zero writes (D2) | contract rule 18; `test_cloud_download_only.py` |

---

## 3. Target completion model (proposal)

### 3.1 Owner and single-writer rule

A new completion owner, `utils/cloud_sync_impl/observation_completion.py`
(the name is proposed), re-exported through the `utils/cloud_sync.py` facade.
It is the **only** code that writes `sync_status ∈ {synced, dirty-with-
outcome}`, `synced_at`, the review marker and the observation snapshot during
sync. Proposed API:

```python
@dataclass(frozen=True)
class SyncIssue:
    kind: SyncIssueKind        # enum, see 3.3
    severity: Literal['required', 'best_effort', 'review', 'blocked']
    local_id: int | None
    cloud_id: str | None
    stage: str                 # 'observation', 'images', 'measurements', 'summary', 'mosaic', 'snapshot', ...
    message: str               # the exact legacy string (3.6)
    error_code: str | None

@dataclass
class ObservationOutcome:
    local_id: int
    cloud_id: str | None
    direction: Literal['push', 'pull', 'create', 'conflict_resolution']
    issues: list[SyncIssue]
    snapshot: SnapshotIntent | None   # what to persist if complete; None = keep old baseline

def complete_observation(outcome: ObservationOutcome, client) -> CompletionResult
```

Rules applied by `complete_observation`:

1. Any `required` issue → `dirty`, old baseline kept, `synced_at` unchanged.
2. Any `review` issue → `dirty` plus the review marker. The old baseline is
   kept, **except** for `identity_review_no_baseline` (rule 6).
3. Any `blocked` issue → `blocked` (privacy slot limit), old baseline kept.
4. Otherwise: persist the snapshot first. If that raises, the outcome becomes
   `dirty` with a `snapshot_failed` issue. If it succeeds, stamp `synced`.
5. `best_effort` issues never change the status. They are reported.
6. **Identity-less snapshot exception.** When there is no baseline and the
   local and remote taxonomy identities disagree (`identity_review_pending`
   on push; `IDENTITY_APPLY_CONFLICT` in the no-baseline pull branch), the
   owner persists a `SnapshotIntent(without_identity=True)` *and* sets the
   review marker. This is required by `docs/supabase-sync-contract.md`,
   "Identity in change detection": the stored baseline must leave the
   identity "unknown", so that later pulls and pushes classify the same
   disagreement as a conflict and not as a one-sided change. It does not write
   `synced`. **Tension with invariant 12** ("never written after unresolved
   conflicts"): the snapshot does record the other fields that both sides
   agreed on while one conflict is unresolved. This is resolved by treating
   the excluded identity as "baseline unknown" rather than as a written
   baseline for the conflicting field. The follow-up must keep this exception
   explicit and tested
   (`test_cloud_identity_fail_closed.py::test_snapshot_after_a_no_baseline_conflict_keeps_the_disagreement_detectable`).
   No other `review` issue writes a snapshot.

Precedence: rule 3 (`blocked`), then rules 2 and 6 (review), then rule 1
(required), then rule 4.

Domain helpers (`_push_images_for_observation`, measurement push/import,
summary, mosaic, `_apply_remote_*`) **return issues**. They no longer call
`mark_observation_dirty`, `_stamp_observation_synced` or the snapshot store.

### 3.2 Ordering (push and pull)

`classify → required work → (snapshot persisted) → synced`. On push, the
observation PATCH/POST is required work. The current early stamp
(~L21012) and the compensating `mark_observation_dirty` calls are removed. The
`identity_review_pending` stamp-then-unstamp becomes a single `review` issue.
On pull, every `_stamp_observation_synced` in `pull_all` and
`_create_local_from_remote` becomes an outcome. The cheap-convergence path
supplies a `SnapshotIntent` that reuses the previous images and measurements.
Rows that need no work (remote unchanged, local clean) are **not re-stamped**,
which also stops the clearing of `blocked` state noted in 1.5.

Interrupted-sync consequence: the push currently writes `synced` right after
the cloud PATCH. Under the target model, an interruption between the PATCH and
completion leaves `dirty`, so the next sync re-pushes. This is safe, because
the identity rules (invariants 4 and 8) and the no-op PATCH skip
(`test_cloud_visibility_phase7.py::test_push_observation_skips_noop_patch_when_remote_matches_after_normalization`)
make the re-push idempotent. The extra cost is bounded by one more preflight
read.

### 3.3 Issue types

`SyncIssueKind` (initial set, one per existing error-producing branch):
`observation_push_failed`, `privacy_slot_limit`, `image_too_large_for_plan`,
`webp_required`, `identity_clear_unverified`, `conflict_review` (with
`reasons`), `identity_review_no_baseline`, `cloud_removed_local_images`,
`image_upload_failed`, `image_metadata_patch_failed`,
`measurement_push_failed`, `measurement_import_failed`,
`measurement_conflict`, `remote_media_pending`, `summary_failed`,
`mosaic_failed`, `snapshot_failed`, `tombstone_failed`,
`calibration_failed`, `reconcile_failed`, `privacy_narrowing_failed`.

| Kind group | Producers | Consumers |
| --- | --- | --- |
| review | push preflight, Case F, pull conflict fields, `removed_keys` | completion owner (marker), `summarize_sync_issues`, `partition_download_from_cloud_issues`, conflict dialog candidates |
| blocked | privacy slot limit | completion owner, main-window blocked list (`sync_status='blocked'` query) |
| required | image, measurement, summary, snapshot and observation-push failures | completion owner (dirty), result `errors` |
| best_effort | mosaic, privacy narrowing detail | result `errors` or log only (decision D4) |
| non-observation | tombstones, calibrations, reconcile passes | result `errors` only |

### 3.4 Shared reconciliation classifier

Today the push and pull sides each have their own classification:

- push: `_analyze_observation_push_conflicts` plus Case F plus
  `identity_review_pending`;
- pull: `_remote_snapshot_has_meaningful_changes`,
  `_analyze_observation_field_changes`, `_analyze_image_changes`,
  `_remaining_local_changes_after_remote_merge` plus Case F (pull).

Both also use `_local_has_real_changes_since_snapshot`. The target is one pure
function, `classify_observation(local, remote, baseline) →
ReconciliationDecision`. Its fields are local-only, remote-only and
conflict sets for fields, identity, images and measurements, plus flags for
`no_baseline_contradiction` and `remote_removed_images`. It lives in the pure
reconciliation owner from Stage S4 (purity enforced by test). Push and pull
switch to it in the same commit. There is no period with two classifiers. The
fixtures in section 7 prove that the decisions are unchanged.

### 3.5 Deep `mark_observation_dirty` / `mark_observation_media_dirty` calls

- **Local-edit signals (1.2):** kept unchanged. They express "local data
  changed", not completion.
- **Sync-time scans (1.3):** kept. They run before classification, and their
  result (`dirty`) is an input to classification.
- **Compensating calls inside sync (1.4 items 6–8 and 10, conflict
  execution):** removed. Their failures become `required` issues. This also
  fixes the two broken one-argument calls, because those call sites
  disappear.
- **`recalculate_measurements_for_calibration` raw UPDATE:** routed through
  `mark_observation_sync_dirty`. Optional. Recorded as a follow-up item.

### 3.6 Legacy result compatibility

`sync_all` keeps every key it returns today: `pushed`, `pulled`,
`calibrations_*`, `errors`, `deleted_remote`, `sync_summary`, `original_sync`,
`pull_only`, `images_downloaded`, `observations_updated`,
`cloud_writes_completed`, `blocked_write_attempts` and the reference keys.
`result['errors']` is rebuilt as `[issue.message for issue in issues]` in
production order. Each `message` is the exact legacy string, because
`summarize_sync_issues`, `partition_download_from_cloud_issues` and the UI
parse those strings. A new `result['issues']` holds the typed list. Documented
exceptions:

- (a) A summary failure now also leaves the observation dirty. The string is
  unchanged.
- (b) A snapshot failure adds a new `snapshot_failed` string. Today it is
  either swallowed or reported as `cloud <id>: <exc>`.
- (c) A mosaic failure adds a string **only if** decision D4 chooses
  visibility.

### 3.7 Conflict execution

`resolve_conflict_keep_local`, `keep_cloud`, `merge` and `resolve_conflict_plan`
build an `ObservationOutcome(direction='conflict_resolution')` and call
`complete_observation`. `resolve_conflict_plan` already uses the right order.
Its `PartialConflictPlanError` contract (retry with `prior_result`, drift
abort, no media deletion) is unchanged: the owner raises the existing
`_partial_error` on `snapshot_failed`. In `finalize_sync_candidates`, the
marker clear becomes a completion-owner call. The extraction stages leave these
functions in the facade. The follow-up moves them with the owner.

### 3.8 Other writers

- `materialize_cloud_media_for_observation`: proposed to persist the snapshot
  only through the owner, and only when the downloads and measurement link-up
  are complete. Otherwise the old baseline stays (decision D3).
- `_mark_cloud_observation_imported`: proposed to go through the owner as a
  `create` outcome (snapshot plus `synced` only after the image downloads),
  using UTC and dropping the unconditional `set_*desktop_id` writes in favour of
  the guarded helpers. The alternative is a documented exception (decision D7).
- `unlink_local_observation_from_cloud`, `reset_cloud_sync_state`, local-edit
  signals and the sync-time scans: documented exceptions. They do not write
  `synced` or a snapshot, except for clearing the snapshot on unlink.

### 3.9 Check against the decided outcomes and invariants

| Item | Target model | Tension |
| --- | --- | --- |
| Identity-less snapshot on no-baseline identity review | 3.1 rule 6 | A snapshot is written while a review is open. This is required by the contract and stated as a deliberate exception to "old baseline kept" and to invariant 12. |
| Contract rule 8 "deletion" work | Tombstone failures stay non-observation issues (P7) | They do not block `synced`. Decision D9. |
| One owner writes `synced` and snapshots | 3.1, 3.8 | The UI import is an exception unless D7 chooses to absorb it. |
| `synced` only after required work and snapshot; snapshot failure blocks | 3.1 rule 4, 3.2 | Re-push after an interrupted sync (3.2), which is acceptable. |
| Required child failure → dirty, retryable, old baseline | 3.1 rules 1–2 | Current push stores the baseline after failure. That changes, and the push-side `accepted_asymmetry` pruning then happens one sync later. |
| One classifier | 3.4 | Must land atomically. The pull side has more categories (`removed_keys`) than the push side. |
| Typed issues, legacy strings compatible | 3.3, 3.6 | Documented exceptions (a)–(c). |
| Domain helpers do not decide completion | 3.1, 3.5 | — |
| Facade stays | 3.1 | — |
| No new remote writes; pull-only and no-op sync write nothing | completion is local-only; `_store_remote_snapshot` only reads | Snapshot re-read cost is unchanged. D2 must not add writes. |
| Inv. 1–2, 7–9, 16–17 (media) | Not touched. Selection and anchor logic stay in their owners. | — |
| Inv. 3 tombstone-before-prune | Unchanged. Tombstones are non-observation issues. | — |
| Inv. 4, 6 identity | Identity failures become `review` / `identity_clear_unverified` issues. Verification stays. | The interrupted-push re-push relies on inv. 4. |
| Inv. 5 taxonomy | The classifier must keep `taxon_identity` as a virtual field, and keep "snapshot without identity" as a `SnapshotIntent` option. | The snapshot representation must be byte-identical (fixtures, section 7). |
| Inv. 10 pull-only | Owner performs zero remote writes. | — |
| Inv. 11 partial reads | `SnapshotIntent` is only built from complete reads (unchanged). | — |
| Inv. 12 snapshot | Strengthened. This is the central fix. | Exception: 3.1 rule 6 (identity-less snapshot). |
| Inv. 13 | Classifier fixtures. | — |
| Inv. 14 plans | 3.7. | — |
| Inv. 15 | 3.1 rule 1. | — |
| Inv. 18 no-op | Rows needing no work are not re-stamped (local write only). The remote write list is unchanged (section 5). | — |
| Inv. 19 | Remote-only field adoption stays before the push. | — |
| Inv. 20 flags | Meanings unchanged. D1 and D6 decide which work each flag makes required. | The Refresh preset disagrees with the rules (2.1). |

---

## 4. Test classification

**Contract** tests assert behaviour the target model keeps. **Accidental**
tests assert current ordering or mechanics that the follow-up will change.
They are tied to the change that alters them.

| Test | Asserts | Class | Changed by |
| --- | --- | --- | --- |
| `test_cloud_conflict_plan_execution.py::test_finalization_order_snapshot_before_stamp` | snapshot → signature → stamp | contract | — (target order) |
| `…::test_snapshot_failure_leaves_conflict_unsealed` | no stamp after snapshot failure | contract | — |
| `…::test_reviewed_measurement_upload_generates_mosaic_before_finalization` | mosaic before finalization | contract | — (fakes the stamp; rewire to the owner) |
| `…::_patch_common` fixture users | stamp/snapshot recorders | accidental (mechanics) | 3.7 owner adoption: recorders move to the owner |
| `…::test_ordinary_snapshot_write_prunes_accepted_asymmetry_…`, `test_sqlite_snapshot_round_trip_*` | snapshot content | contract | — |
| `test_cloud_conflict_dialog.py::test_resolution_plan_applies_mixed_cloud_field_local_measurement_and_recomputes` | field application, measurement upload, statistics recompute, no deletion (stamp and snapshot are faked, not asserted) | contract (outcome) | — (fakes are retargeted in 3.7) |
| `test_cloud_conflict_dialog.py::test_keep_cloud_disables_deletion_preserves_local_file_and_overwrites_remote_measurements` | stamp + snapshot recorded | contract (outcome), accidental (call names) | 3.7 |
| `test_cloud_visibility_phase7.py::test_mark_observation_dirty_clears_blocked_sync_state` | blocked → dirty on local edit | contract | — |
| `test_cloud_visibility_phase7.py::test_resolve_conflict_keep_local_records_deleted_cloud_images_before_push` | tombstone record before push | contract | — |
| `test_sync_observation_dirty_propagation.py::test_gap_c_mark_obs_dirty_called_in_generic_cloud_sync_error_branch` | `mark_observation_dirty` called | accidental | 3.5 (compensating calls removed). Assert `dirty` state instead. |
| `…::test_gap_d_…`, `…::test_gap_e_…` | `dirty` after child failure (real SQLite) | contract | Strengthen: they stub `_store_remote_snapshot`, so they cannot see the baseline advancing. Add "old baseline kept". |
| `test_cloud_sync_dirty_loop_steady_state.py::test_push_all_metadata_only_refreshes_signature_after_stamp` | signature refreshed after stamp | accidental (order), contract (signature matches) | 3.2 |
| `…::test_push_all_metadata_only_logs_dirty_to_synced_transition` | `dirty→synced caller=push_all` log line | accidental | 3.1 (the owner emits the log; keep the text or update) |
| `…::test_signature_refresh_makes_current_and_stored_match`, `…::test_metadata_only_refresh_patches_image_metadata_on_existing_cloud_rows` | end state `synced` | contract | — |
| `test_cloud_sync_upload_completeness.py::test_storage_intent_is_initialized_before_pending_state_is_evaluated` | initializer before pending | contract (inv. 1) | — |
| `…::test_missing_file_and_duplicate_rows_do_not_create_a_dirty_loop`, `…::test_linked_cache_row_retains_the_fast_path_shortcut` | end state `synced` | contract | — |
| `test_cloud_media_measurement_mosaic_chain.py` (3 tests) | end state `synced` | contract | — |
| `test_cloud_sync_dirty_pending_images.py`, `test_cloud_sync_pending_image_repair.py::test_dirty_scan_redirties_genuinely_pending_image` | scan re-dirties | contract | — |
| `test_cloud_sync_pending_image_repair.py::test_explicit_checkbox_change_marks_dirty_without_invalidating_signature` | `mark_observation_dirty` fake called | contract (local-edit signal, 3.5) | — |
| `test_spore_summary_sync.py::test_call_site_unexpected_error_recorded_in_errors_list` | one-argument fake `mark_observation_sync_dirty` called | **accidental, masks a defect** | 3.5 → assert real `dirty` with SQLite |
| `test_spore_summary_sync.py::test_measurement_reconcile_records_per_observation_errors` | same | **accidental, masks a defect** | 3.5 / D5 |
| `test_spore_summary_sync.py::test_measurement_reconcile_rebuilds_mosaic_after_raw_repair` | call order: measurements, then mosaic, in the backfill pass | contract (the mosaic follows its measurements) | — |
| `test_spore_summary_sync.py::test_measurement_reconcile_is_idempotent_after_successful_push` | push call count on a second pass | contract (inv. 18) | — |
| `test_reference_client_capability_stage_m.py::test_device_report_once_per_session_before_pushes` | one report per session, before reference pushes | contract (Stage M), in tension with inv. 18 (section 5) | D10 |
| `test_reference_client_capability_stage_m.py::test_device_report_failure_is_non_fatal_and_retried_next_sync` | report count 2 after a failure | contract | D10 |
| `test_reference_client_capability_stage_m.py::test_pull_only_sync_never_reports`, `…::test_pull_only_allows_the_feed_and_blocks_the_device_report` | zero reports in pull-only | contract (inv. 10) | — |
| `tests/local_supabase/test_reference_sync_local.py::test_2_no_change_sync_keeps_one_device` (opt-in) | exactly one `record_reference_client_capabilities` RPC on a no-change sync in a fresh process | contract (Stage M), in tension with inv. 18 | D10 |
| `test_cloud_sync_image_captured_at.py::test_old_signature_capture_time_gap_marks_observation_dirty` | scan calls `mark_observation_dirty` | contract (scan, 1.3) | — |
| `test_observations_tab_cloud_sync.py::test_gallery_publish_uncheck_routes_through_cloud_lifecycle`, `test_main_window_background_activity_badge.py::test_measure_gallery_publish_uncheck_routes_through_cloud_lifecycle` | checkbox → `mark_observation_dirty` | contract (local edit) | — |
| `test_cloud_identity_fail_closed.py::test_snapshot_after_a_no_baseline_conflict_keeps_the_disagreement_detectable` | identity-less snapshot after a no-baseline conflict | contract (inv. 5) | — (becomes a `SnapshotIntent` option) |
| `test_cloud_sync_fast_path.py::test_fast_pull_converges_when_remote_updated_at_bumped_but_fields_unchanged` | stamp + snapshot on convergence | contract (outcome) | 3.2 order |

Note: about 25 test files stub `_store_remote_snapshot` to a no-op. That is
suppression, not an assertion. Those stubs must be retargeted when the owner
takes over snapshot persistence. They are not classified individually here.

---

## 5. No-op remote writes

Fast no-op sync means `full_pull=False`, nothing dirty, and no remote change.
Remote writes reachable on that path:

| Write | Path / guard | Changes `updated_at` without a semantic change? | Invariant 18 test |
| --- | --- | --- | --- |
| Calibration PATCH/POST, reference image | `push_calibrations`; semantic no-op detection | no (guarded) | `test_cloud_calibration_sync.py::test_push_calibrations_treats_semantically_equivalent_remote_rows_as_no_op`, `…::test_push_calibration_reference_image_quiet_for_fast_no_op` |
| Image tombstone soft-delete | `_push_pending_image_tombstones`; only pending tombstones | no (only explicit intent) | `test_cloud_sync_fast_path.py::test_push_all_fast_path_flushes_image_tombstones_without_dirty_observations` |
| Observation PATCH / image metadata PATCH | only for dirty rows; the pending-image scan can re-dirty when `sync_images=True` and the scan is due | no: `push_observation` skips normalized-equal PATCHes | `test_cloud_metadata_sync.py::test_push_all_skips_noop_patch_and_clears_dirty_state_after_normalized_match`, `test_cloud_visibility_phase7.py::test_push_observation_skips_noop_patch_when_remote_matches_after_normalization`. Image-metadata PATCH skip **unverified** |
| Measurement restore `deleted_at=NULL`, measurement push, mosaic | `_reconcile_missing_spore_measurements`; local candidate scan, only when candidates exist | no | `test_spore_summary_sync.py::test_measurement_reconcile_is_noop_when_all_measurements_have_cloud_ids`; `test_cloud_sync_fast_path.py::test_push_all_fast_path_runs_lightweight_spore_reconciliation` |
| Spore summary upsert | `_reconcile_missing_spore_summaries`; only for missing/stale context hashes | no | `test_cloud_sync_fast_path.py::test_push_all_fast_path_runs_lightweight_spore_reconciliation` (call, not writes); **write-free assertion unverified** |
| `set_desktop_id` | `pull_all` candidate loop; only when the remote `desktop_id` differs and not pull-only; candidates are pruned on the fast path | no (inequality guard) | `test_cloud_sync_fast_path.py::test_pull_all_fast_path_returns_early_when_nothing_changed` (no candidates) |
| Image `desktop_id` relink | `_remote_image_desktop_id_current` guard | no | `test_child_change_probe.py::test_second_noop_sync_zero_child_candidates` |
| Reference library pushes (`sync_reference_*`, `sync_observation_reference_use`) | `sync_reference_library` → `_push_reference_library` | no | `test_reference_use_no_change_resync.py::test_no_change_sync_leaves_every_status_row_byte_identical`; `tests/local_supabase/test_reference_sync_local.py::test_2b_no_change_sync_sends_no_reference_writes` (opt-in; it filters its write count to these RPC names) |
| **Device capability report** (`record_reference_client_capabilities` RPC) | `sync_reference_library` → `utils/reference_client_capabilities.py::report_reference_client_capabilities`; runs in every non-pull-only sync whose reference pull had no errors; deduplicated in memory **once per app process and account**, and retried at the next sync after a failure | **yes, a mutation with no observation or reference change**: the first no-op sync of every app session writes it. Its effect on any `updated_at` is defined server-side in `sporely-web` and is not verified here | Not covered by an inv. 18 test. `tests/local_supabase/test_reference_sync_local.py::test_2_no_change_sync_keeps_one_device` **expects** exactly one call on a no-change sync, and `test_reference_client_capability_stage_m.py::test_device_report_once_per_session_before_pushes` pins the once-per-session rule. **Existing tension with invariant 18**, recorded and not resolved here (decision D10) |

Apart from the device capability report above, no other remote write is reachable on a fast no-op sync. No observation completion write is remote: `_store_remote_snapshot` and the
status writers are local. The target model adds no remote writes.
`_mark_cloud_observation_imported` performs unconditional `set_*desktop_id`
writes, but it is a manual UI action, not a sync path.

---

## 6. Shared-contract impact

**None required.** `docs/supabase-sync-contract.md` already states the
target: rule 8 ("do not mark an observation fully synced if required … work
failed"), rule 16 (incomplete remote data is never a snapshot), "First
upload" ("save the known-good baseline after all required child work
succeeds") and "Retry-safe sequencing" (snapshot last). `sync_status` and the
snapshot are desktop-local state that other clients cannot see. The follow-up
therefore needs **no `sporely-web` candidate**, unless the person chooses one
of these optional clarifications, which would then have to land identically in
both copies:

- Optional, rule 8 append: "This applies to pull as well as push: a pull
  marks an observation synced only after the work its caller mode requires
  (see the desktop orchestration design) and after the baseline is stored."
- Optional, if D4 makes the mosaic best-effort explicit: in "Public spore
  mosaic after conflict resolution", add "Mosaic generation is best-effort and
  never blocks observation sync completion."

The desktop-only rules (caller presets, D1, D6) belong in
`.claude/rules/cloud-sync.md` and `docs/cloud-sync-architecture.md`, not in
the shared contract.

---

## 7. Verification strategy

**Fixtures to capture before the behaviour change** (in a tests-only stage,
on the pre-change code):

1. **Classifier decision corpus.** For a table of (local, remote, baseline)
   triples, record the push decision (`_analyze_observation_push_conflicts`,
   Case F, `identity_review_pending`) and the pull decision
   (`_remote_snapshot_has_meaningful_changes`, `_analyze_observation_field_changes`,
   `_analyze_image_changes`, `_remaining_local_changes_after_remote_merge`,
   Case F). The table must cover representation-only drift, three-way
   divergence, taxonomy identity (no baseline, unknown baseline,
   claim/no-claim), accepted asymmetry, and removed remote images. Store it as
   JSON. The shared classifier must reproduce it, and any differences between
   the push and pull decisions must be listed and decided explicitly.
2. **Snapshot byte golden.** Serialized `_cloud_observation_snapshot` output
   for the same corpus, including the variants without identity and without
   images or measurements (inv. 5 and 12).
3. **Legacy result golden.** Run `sync_all` with fake clients for each caller
   preset in 2.1 and for each failure branch in 1.4–1.5. Record the result
   keys and the `errors` strings in order, plus
   `summarize_sync_issues(errors)` and `partition_download_from_cloud_issues(errors)`.
   The follow-up must match this golden except for the documented items in 3.6.
4. **State-transition table** (real SQLite, fake client): the final
   `sync_status`, review marker, snapshot presence and snapshot hash per
   failure branch. The follow-up updates it deliberately row by row.

**Local Supabase harness.** `tools/run_local_sync_harness.sh` runs the real
`sync_all` against a local stack in per-device processes. Today
`device.py::_sync` uses `sync_images=False, materialize_remote_images=False,
full_pull=True`, and only reference-library scenarios exist. It *can*
exercise observation metadata push/pull completion if new device actions are
added (create, edit, sync, inspect `sync_status` and snapshot, two devices).
Its script does not show R2 or the media Worker in the stack, so image byte
upload and materialization are unverified there. Treat media paths as
out-of-reach for the harness until shown otherwise.

**Live canary (still needed after the above).** On a disposable or known
account, follow the plan's canary policy. It must cover:

- a "Sync now" that uploads one new selected image plus measurements;
- a Refresh that pulls a web-side edit with new media;
- a Download from Cloud (zero writes; completion state as D2 decides);
- one forced child failure, if a safe trigger exists (for example an image over
  the plan limit), showing `dirty` with the old baseline;
- a second Sync now with zero remote writes and the reconciliation report
  unchanged.

---

## 8. Decisions for the person

Each decision affects the follow-up plan. None of them blocks the extraction
stages S2–S7.

**D1. Does pull completion require remote media materialization, per caller
mode?**

- (a) **Required only when `materialize_remote_images=True`** (current
  behaviour for existing rows). With `materialize=False`, metadata, anchors
  and measurements suffice. *Consequence:* a metadata-only pull can mark rows
  `synced` without bytes. A new row created with `materialize=False` would also
  become `synced`, which is a change: today it stays dirty.
- (b) **Never required.** Media download becomes best-effort and is tracked
  separately (for example a "media pending" flag). *Consequence:* `synced`
  means "metadata agrees". This needs a new local state, and possibly a schema
  change.
- (c) **Always required.** *Consequence:* every Refresh with
  `materialize=False` leaves rows dirty and pushes them again, which conflicts
  with invariant 18.

Depends on it: the outcome rules for L2a–L3b and L8, the `create` outcome, and
the canary scope.

**D2. What does Download from Cloud (`pull_only`) leave behind?**

- (a) The same completion rules as pull. `synced` is written locally (a local
  write is allowed; only cloud writes are forbidden).
- (b) Rows keep their pre-download status. *Consequence:* the next normal sync
  re-pushes.

Depends on it: L8 and the pull-only tests.

**D3. Should `materialize_cloud_media_for_observation` write the baseline?**

- (a) Only through the owner, and only when the download is complete.
- (b) Never. On-demand download does not touch the baseline. *Consequence:* the
  next pull re-evaluates and may re-download.

**D4. Spore mosaic: best-effort or required?**

- (a) Best-effort and visible (an issue in `errors`).
- (b) Best-effort and silent (current behaviour).
- (c) Required. *Consequence:* a public-mosaic outage leaves observations
  dirty.

This decides legacy exception 3.6(c) and the optional contract wording.

**D5. Backfill passes (`_reconcile_missing_spore_measurements` /
summaries): should a failure dirty the observation?**

- (a) Yes, as a `required` issue on that observation. This fixes the
  defect's intent.
- (b) No. Report only, and remove the dead `mark_observation_sync_dirty`
  calls. *Consequence:* rule 8 ("summary") holds only for the per-observation
  summary.

**D6. Observations-tab Refresh preset.**

- (a) Change the code to `sync_images=False`, as the rules say.
  *Consequence:* Refresh no longer uploads pending media.
- (b) Change the rules to match the code. Refresh equals Sync now.

The follow-up's caller-mode tests depend on this.

**D7. Manual "import cloud observation" UI path.**

- (a) Bring it under the owner as a `create` outcome (3.8).
- (b) A documented exception. *Consequence:* rows imported this way have no
  baseline, and a non-UTC `synced_at`.

**D8. Defect timing.** The broken one-argument `mark_observation_sync_dirty`
calls (1.4) can be:

- (a) fixed in a small standalone change before the follow-up, with tests
  that do not use a one-argument fake;
- (b) left for the follow-up, which removes those call sites.

The orchestration plan's Stage 1 scope depends on this choice.

**D9. Do tombstone (deletion) failures block observation completion?**
Contract rule 8 lists "deletion" among the work whose failure must not leave
an observation fully synced.

- (a) No (proposal, P7). The unsynced tombstone row is the durable,
  retryable and visible record, and the flush runs before pruning
  (invariant 3). *Consequence:* rule 8 is read as "the deletion stays
  retryable", not as "the observation stays dirty". This may need an optional
  contract clarification, which would bring in a `sporely-web` candidate.
- (b) Yes. A failed tombstone for an image makes its observation dirty.
  *Consequence:* that observation is pushed again on every sync until the
  deletion lands. This adds re-push cost but no new remote writes, because the
  no-op PATCH skip applies.

**D10. Device capability report versus invariant 18.**
The once-per-session `record_reference_client_capabilities` RPC is a remote
write on a no-op sync (section 5).

- (a) Accept it as a documented exception to invariant 18 (a session-level
  registration, not a sync data write). Record this in
  `.claude/rules/cloud-sync.md` and the tests.
- (b) Make it change-driven, for example by persisting the last reported
  capabilities locally and reporting only when they change. *Consequence:*
  this is a reference-sync change and is outside this plan. The local harness
  test `test_2_no_change_sync_keeps_one_device` would change.

This does not block the orchestration follow-up, but its no-op test must
either exclude or assert this RPC explicitly.
