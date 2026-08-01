# W2D real historical reconciliation — aggregate evidence

**Label:** `historical anonymized manifest` (per contract §14). Kept
distinct from the synthetic-fixture manifest under
`reconciliation-manifest.json`.

Only aggregate outcomes and hashes are committed. The anonymised
snapshot, the raw export, the pseudonymisation key, and the real
reconciliation manifest itself all live outside the repository at the
operator's paths.

## Input

* Snapshot SHA-256:
  `a3d0fdb534fce5d9020600767356e18082d6279495bb12bd08e573a0307defb0`
* Raw-export SHA-256 (from the snapshot header):
  `8efbedfaedb3fea94c6d5ebcc4b80eb65c04e535f8b27649dfdcf353e40b17e4`
* Snapshot schema version: `w2d-input-1.0.0`
* Snapshot record count: **369**
* Validation: `ok=true`, no prohibited fields, no duplicates
* Committed to Git: **no**

## Reconciliation

Ran twice, into distinct output directories, with the canonical registry
loaded (`--canonical-registry database/taxonomy/registry/canonical`).

* Policy version / SHA-256: `w2d-1.0.0` /
  `c408601f71b7d89de0283c307220b06876d80a7418bbb9089337b6d3941c43d6`
* Taxonomy release: `tax-2026.08.01-01`, scope-manifest SHA-256
  `72758b2c574e8aea27432b6b55c62dfb6ad87f3fadc11ad1c892a61abf23ac4e`
* Manifest semantic SHA-256:
  `8087d4398311268a4fe049435de61ec16edff7f3c52baf169310807c77f77448`
* Determinism: **byte-identical** across the two runs

## Aggregate counts (real, 369 records)

| primary state | count |
|---|---:|
| resolved_exact | 54 |
| resolved_exact_via_legacy_mapping | 0 |
| resolved_exact_via_synonym_relationship | 0 |
| ambiguous_multiple_candidates | 0 |
| conflicting_exact_evidence | 0 |
| unresolved_external_identifier | 200 |
| unresolved_legacy_identifier | 0 |
| manual_unresolved | 85 |
| no_identity_evidence | 30 |
| source_record_missing | 0 |
| invalid_or_unnamespaced_identifier | 0 |
| **total** | **369** |

Resolution methods (resolved records only):

| method | count |
|---|---:|
| direct_taxonomy_v2_mapping | 54 |
| legacy_lookup_chain | 0 |
| pinned_synonym_relationship | 0 |
| trusted_secondary_provider_mapping | 0 |

Migration actions:

| action | count |
|---|---:|
| materialize_existing_taxonomy_v2_concept | 54 |
| retain_unresolved_without_registry_concept | 315 |

## Registry impact

* Unique resolved concepts: **39** (54 resolved records collapse to 39
  distinct sporely_taxon_ids — the runner deduplicates repeated
  external-id triples).
* External mappings created: **39**.
* Existing registry concepts reused: 0 (disposable sim starts empty).
* Concepts requiring materialisation: 39.

## Why 200 unresolved_external_identifier?

The pinned macrofungi release currently exposes only
`col_xr:col_usage_id` in `taxon_external_id.jsonl` and an empty
`taxon_external_id_legacy_integer.jsonl`. Observation-side signals in
this dataset are Norwegian (`nortaxa:nortaxa_taxon_id`) and iNaturalist
(`inaturalist:inaturalist_taxon_id`) — namespaces the release does not
yet map. The 54 resolutions that did land came through the canonical
registry shards. Back-filling NorTaxa / iNaturalist mappings into the
release (or the canonical registry) is the next legacy-source-recovery
step.

## Disposable simulation

Executed against the local disposable Supabase stack.

| proof | verdict | detail |
|---|---|---|
| Idempotency | ✓ | applying the real 369-record manifest twice: identification_snapshot 369↔369, registry_concept 39↔39, external_mapping 39↔39, snapshot fingerprint `9f7107e948babb356140247802b4b7aa` stable |
| Rollback | ✓ | induced failure (SQLSTATE P0001) left zero rows across identification_snapshot, resolution_link, registry_concept, external_mapping, reconciliation_result |

Sim runner logs are archived at the operator's paths
(`/tmp/w2dr-real-1/sim-twice.json`, `/tmp/w2dr-real-1/sim-rollback.json`).

## Remaining blocker

**Legacy-source recovery required.** The engine and disposable pipeline
are proven against real data. Automatic resolution of the 200
unresolved_external_identifier records requires NorTaxa / iNaturalist
cross-mappings to be added to the pinned macrofungi release or the
canonical registry. That is a separate stage — not W3.

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
* raw export committed: **no**
* anonymised snapshot committed: **no**
* real reconciliation manifest committed: **no**
