# W2D-R integration proof + source-recovery status

**Status:** PostgreSQL proof complete; real audit blocked pending authorized snapshot.

## PostgreSQL integration proof

Executed in the sibling web repo against a local disposable Supabase stack.
See `sporely-web/docs/evidence/taxonomy-v2/w2dr-postgres-integration.{json,md}`.

* 21 tests, 21 passed, 0 skipped.
* 11 previously-skipped integration tests all executed against real
  PostgreSQL rows and constraints.
* Lifecycle proofs verified: exact resolution materialization, legacy
  chain preservation, unresolved-without-registry, conflicting-without-
  registry, namespace uniqueness, namespace isolation, out-of-cache
  materialization, later exact resolution, snapshot immutability trigger,
  idempotency, rollback.

## Source-recovery tooling (this repo)

New package `database/taxonomy/reconciliation/snapshot/`:

| module | purpose |
|---|---|
| `pseudonym.py` | HMAC-keyed deterministic observation pseudonymiser (`obs_<24 hex>`); key never leaves the environment; key ≥ 32 bytes required |
| `validator.py` | Schema + privacy validator over a JSONL snapshot; rejects prohibited fields, media URLs, coordinate-shaped keys, raw UUIDs under `observation_id`, duplicates, missing header |
| `transformer.py` | Offline transformation from an authorised raw JSONL export to an anonymised snapshot JSONL; emits SHA-256 sidecar and deterministic stats sidecar; refuses `--production` |

Documentation:

* `database/taxonomy/docs/w2d-input-snapshot-contract.md` (existing, unchanged)
* `database/taxonomy/docs/w2d-source-recovery-runbook.md` (new) — the
  operator runbook covering key generation, raw-export projection, offline
  transform + validate, reconciliation, disposable web simulation, and
  commit boundary.
* `database/taxonomy/scripts/export_observations_snapshot.py` (existing,
  specification-only, refuses `--production`)

Tests: 16 new pytest cases in
`tests/taxonomy/test_w2dr_source_recovery.py` cover:

* pseudonym determinism under one key; divergence across keys; refusal
  on short / missing key
* validator: minimal-valid, prohibited-field, raw UUID, email/media URL
  in values, non-string external_id, snapshot header + duplicate
  detection
* transformer: end-to-end determinism, prohibited-field stripping into
  `stats.json`, refuse-to-overwrite, CLI refuses `--production`
* export spec refuses `--production` (subprocess)
* real-vs-fixture manifest separation is documented in the runbook

Full desktop suite (`python -m pytest tests/taxonomy/ -q`): 42 passed.

## Snapshot availability

**No.** No human-authorized anonymized snapshot has been supplied. The
tooling above is ready to consume one; running the runbook is the
operator's next step.

## Real reconciliation

**0 real records audited.** Reason: no human-authorized anonymized
snapshot supplied. Fixture-based reconciliation evidence remains under
`reconciliation-manifest.json` / `reconciliation-report.md`; the real
manifest, when produced, must land in a **distinct** output directory
labelled `historical anonymized manifest` per the runbook — the
synthetic fixture evidence must not be overwritten.

## Remaining blocker

**Human-authorized read-only export required.** Production access is not
authorised and the tooling here does not request it. The operator's next
step is documented in the runbook.

## Safety

* production Supabase accessed: **no**
* production taxonomy writes: **no**
* production observation writes: **no**
* production migrations created: **no**
* production migrations applied: **no**
* client cutover: **no**
* desktop taxonomy activated: **no**
* legacy taxonomy removed: **no**
* name-only automatic resolutions: **no**
* production observation rows modified: **no**
