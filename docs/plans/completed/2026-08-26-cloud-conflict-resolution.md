# Cloud conflict resolution

Status: Completed

Completed: 2026-08-26

Relevant commits: `6e32e99`, `f1e4a80`, `6099514`, `c2df6d9`, `1e28efd`, `d01edfb`, `e0a5c12`, `8fb1360`, `7cd7f37`, `f274fc0`, `fd5439a`

## Objective

Make cloud conflict detection, user review, and conflict-plan execution explicit and safe across the desktop sync workflow.

## Implemented design

- Detect meaningful local/remote divergence before push and preserve a known-good baseline.
- Present field, identity, geometry, image-order, and local/cloud image choices in the conflict dialog.
- Execute a complete conflict plan without exposing media-deletion operations through that plan.
- Abort safely when the baseline drifts or the plan is incomplete.
- Keep unresolved or failed work retryable rather than falsely marking it synchronized.
- Normalize comparison payloads so representation-only differences do not create false image conflicts.

## Validation record

- Preflight and execution coverage lives in `tests/test_cloud_sync_conflict_preflight.py` and `tests/test_cloud_conflict_plan_execution.py`.
- Dialog behavior is covered by `tests/test_cloud_conflict_dialog.py`.
- Deterministic UI review scenarios and their manifest were finalized in `fd5439a`.

The stage numbers used during this work were local to this conflict-resolution effort and are not repository-global identifiers.
