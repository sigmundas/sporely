# W2D historical reconciliation report

**SYNTHETIC FIXTURES ONLY — REAL 337 AUDIT BLOCKED.**

The 337 / 227 / 87 / 23 Phase-A audit counts referenced by the Stage W2D brief
originate in the sibling repo as hard-coded `generate_series(...)` constants in
`sporely-web/scripts/taxonomy-v2/run-sparse-registry-experiment.mjs`. They are
not backed by any local snapshot of real observation rows in either repository.

Per the W2D "no usable local snapshot" branch, this evidence entry documents:

* the engine + policy + manifest generator behaviour under exhaustive
  synthetic fixtures, and
* the required input format for an anonymised historical snapshot
  (`database/taxonomy/docs/w2d-input-snapshot-contract.md`) that would
  unblock the real audit.

The **real 337-observation reconciliation and its resulting migration manifest
remain blocked** pending an anonymised export conforming to that input contract.

## Run provenance

* `manifest_version`: `reconciliation-manifest-v1`
* `policy_version`: `w2d-1.0.0`
* `policy_sha256`: `c408601f71b7d89de0283c307220b06876d80a7418bbb9089337b6d3941c43d6`
* `taxonomy_release_id`: `tax-2026.08.01-01`
* `taxonomy_scope_manifest_sha256`: `72758b2c574e8aea27432b6b55c62dfb6ad87f3fadc11ad1c892a61abf23ac4e`
* Input fixtures: `database/taxonomy/reconciliation/fixtures/all_states.jsonl`
  — **synthetic, non-production** (13 records)
* `input_source_hash`: `71344ade56597930e2c3057f7ef0a4174bbcca4e88414342518919ac17cf8d0a`
* Manifest semantic SHA-256:
  `c4785a25c8690144abd64a75ff369292aaf139dc4030c2ce2ce3df413462d72c`
* Determinism: two consecutive runs produce byte-identical manifest and
  matching semantic SHA (verified against `/tmp/w2d-run-1` and `/tmp/w2d-run-2`).

## Aggregate counts (SYNTHETIC FIXTURES)

| primary state | count |
|---|---:|
| resolved_exact | 3 |
| resolved_exact_via_legacy_mapping | 1 |
| resolved_exact_via_synonym_relationship | 0 (fixture skipped — pinned release lacks a suitable synonym relationship; see fixture README) |
| ambiguous_multiple_candidates | 1 |
| conflicting_exact_evidence | 1 |
| unresolved_external_identifier | 1 |
| unresolved_legacy_identifier | 1 |
| manual_unresolved | 2 |
| no_identity_evidence | 1 |
| source_record_missing | 1 |
| invalid_or_unnamespaced_identifier | 1 |
| **total** | **13** |

Migration-action distribution:

| migration_action | count |
|---|---:|
| materialize_existing_taxonomy_v2_concept | 4 |
| manual_review_required | 2 |
| retain_unresolved_without_registry_concept | 7 |
| reuse_existing_registry_concept | 0 (no pre-registered anchor in this fixture set) |

Resolution-method distribution (resolved records only):

| resolution_method | count |
|---|---:|
| direct_taxonomy_v2_mapping | 3 |
| legacy_lookup_chain | 1 |
| pinned_synonym_relationship | 0 (see above) |
| trusted_secondary_provider_mapping | 0 |

**Explicit statement:** candidate matching did NOT create identity in any
record. Candidate concepts appear only under the observation's
`candidate_concepts` field and are labelled non-authoritative.

## Registry impact (synthetic)

* Unique resolved concepts: 4
* Existing registry concepts reusable: 0
* Concepts requiring materialisation: 4
* Resolved concepts outside macrofungi cache: at least 1 (out-of-cache fixture)
* External mappings required: aligned with resolved concept count
* Registered names required: 0 (the release supplies canonical names)

## Real-data audit

**Blocked.** No local snapshot of the 337 observations exists. Reproducing
the audit against real data requires either:

1. an anonymised export produced by
   `database/taxonomy/scripts/export_observations_snapshot.py` running against
   a local user `observations.sqlite3` (never production), or
2. a Supabase-side read-only export conforming to the same contract, run
   outside this stage's authorisation.

Do not treat this evidence file as a completed real-data audit.

## Determinism verification

```
python -m database.taxonomy.reconciliation.cli \
  --input database/taxonomy/reconciliation/fixtures/all_states.jsonl \
  --output /tmp/w2d-run-1 \
  --release-dir database/reference_data/generated/taxonomy_v2/global_macrofungi_tax-2026.08.01-01 \
  --policy database/taxonomy/policies/w2d-reconciliation-policy.json

# repeat with /tmp/w2d-run-2

diff /tmp/w2d-run-1/reconciliation-manifest.json /tmp/w2d-run-2/reconciliation-manifest.json
# (no output — byte-identical)

sha256 c4785a25c8690144abd64a75ff369292aaf139dc4030c2ce2ce3df413462d72c
```

Tests: `python -m pytest tests/taxonomy/test_w2d_reconciliation.py -x -q` —
26 passed, 0 failed.

## W3 readiness verdict

`legacy-source recovery required` — until an anonymised snapshot of the
audited historical observations is provided, no production-schema design
stage may begin against real reconciliation output. The engine, policy,
manifest generator, disposable migration simulation (see sibling repo), and
input contract are ready to consume such a snapshot deterministically.

**W3 production integration authorised: no.**
