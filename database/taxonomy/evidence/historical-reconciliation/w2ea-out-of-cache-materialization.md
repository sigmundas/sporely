# W2E-A: out-of-cache identity materialization + pipeline-gap status

**Policy accepted:** historical observations may attach to an exact, source-backed
`sporely_taxon_id` even when the concept is outside the current macrofungi
scope. Cache membership and taxonomy scope are tracked **independently**.

## Part A — completed

### Code changes

1. **Resolver** (`database/taxonomy/reconciliation/resolver.py`) — removed the
   "concept present in pinned release" gate on Level 4 (canonical registry).
   When the registry has an anchor for `(source, namespace, external_id)`,
   we now resolve to that `sporely_taxon_id` regardless of release membership.
2. **Result model** (`database/taxonomy/reconciliation/input_model.py`) —
   new `resolved_cache_state` field alongside `resolved_scope_state`:
   * `resolved_cache_state`: `in_cache` when the release backbone carries the
     concept, `out_of_cache` when only the registry anchor is available.
   * `resolved_scope_state`: the release scope_state verbatim when in-cache;
     `not_evaluated` when out-of-cache (the honest value — the macrofungi
     scope predicate did not evaluate this concept).
3. **Web disposable simulation**
   (`sporely-web/scripts/taxonomy-v2/experiments/w2d-migration-simulation.sql`) —
   `registry_concept.cache_state` column added with check constraint
   `in ('in_cache','out_of_cache')`. `apply_reconciliation_manifest`
   stores it verbatim from the manifest record.

### Real reconciliation — before vs after

Real anonymised snapshot: SHA-256 `a3d0f…defb0`, 369 records. Release:
`tax-2026.08.01-01` (unchanged, unmutated).

| primary state | before Part A | after Part A |
|---|---:|---:|
| resolved_exact | 54 | **211** |
| unresolved_external_identifier | 200 | **43** |
| manual_unresolved | 85 | 85 |
| no_identity_evidence | 30 | 30 |
| **total** | 369 | 369 |

Manifest semantic SHA-256:
* before: `8087d4398311268a4fe049435de61ec16edff7f3c52baf169310807c77f77448`
* after:  `37950a4d0a92207e16a76e22cd9e1c2057858bb357a6c3f616c7679ce1c51080`

Determinism: two runs into distinct output dirs produced **byte-identical**
manifests.

### Observation-level effects

* Observations newly resolved: **157**
* Distinct external IDs newly resolved: **81**
* Registry concepts materialized (total): **120** (39 in-cache + 81 out-of-cache)
* External mappings created: **122**
* Remaining `unresolved_external_identifier` observations: **43**

### Cache × scope independence — proof

Registry population after applying the manifest to the disposable schema:

| cache_state | scope_state | concepts |
|---|---|---:|
| in_cache | include | 39 |
| in_cache | not_evaluated | 0 |
| out_of_cache | include | 0 |
| out_of_cache | not_evaluated | 81 |

No out-of-scope concept was rewritten to a generic scope value. No concept
was added to the macrofungi search cache — the release's
`taxon_external_id.jsonl` and `scope-manifest.json` remain untouched
(hashes below). The disposable schema is our sparse persistent registry;
the macrofungi search cache is a separate artefact and was not modified.

### Immutability proof for `tax-2026.08.01-01`

```
scope_manifest_sha256   72758b2c574e8aea27432b6b55c62dfb6ad87f3fadc11ad1c892a61abf23ac4e
policy_sha256           c408601f71b7d89de0283c307220b06876d80a7418bbb9089337b6d3941c43d6
release_id              tax-2026.08.01-01
```

Both the release and its scope-manifest bytes are unchanged. Confirmed
by running the reconciliation twice against the same release paths and
seeing identical release-side hashes in every manifest header.

### Disposable-schema verdicts

| proof | verdict |
|---|---|
| Idempotency | ✓ applying the real 369-record manifest twice: identification_snapshot 369↔369, registry_concept 120↔120, external_mapping 122↔122, snapshot fingerprint stable |
| Rollback | ✓ induced P0001 failure → 0 rows across all tables |
| Snapshot preservation | ✓ `original_*` columns immutable; benign updates don't shift fingerprint |
| Historical snapshots unchanged | ✓ 369 `identification_snapshot` rows — the original `original_scientific_name`, `original_vernacular_name`, `original_rank`, `original_source_system`, `original_source_namespace`, `original_external_id` fields are preserved verbatim from the manifest and the SQL trigger blocks any `UPDATE` while `snapshot_locked=true` |

### Pytest

`python -m pytest tests/taxonomy/ -q` → **43 passed** (no regressions).

## Part B — investigated; new release build deferred

### Identifier buckets

* `sqlite_present_no_registry_anchor`: **19** NorTaxa IDs
* `source_archive_only_no_registry_anchor`: **2** NorTaxa IDs

Observation impact: each of the 21 distinct IDs occurs on ~1 observation
in the audit (no cross-observation reuse). Fully-materialized anchors
would unlock ~21 additional resolutions.

### Root cause (verified against the diagnostic CSV)

The 19 SQLite-present IDs are Norwegian **non-fungi taxa** — sample
species from the diagnostic evidence file:

```
100513  Cirsium arvense       (creeping thistle, plant)
100758  Onopordum acanthium   (cotton thistle, plant)
101107  Berteroa incana       (hoary alyssum, plant)
101557  Silene dioica         (red campion, plant)
101602  Stellaria nemorum     (wood stitchwort, plant)
… 14 more, same pattern
```

The 2 archive-only IDs (`103408 Rubus`, `194385 Distichia undulata`) are
similarly out-of-fungi entries.

The current identity-registry allocator in
[`database/taxonomy/scripts/compile_release.py`](../../scripts/compile_release.py)
(2114 lines) + [`identity_registry.py`](../../scripts/identity_registry.py) is
scoped to a fungi W1 pipeline; non-fungi NorTaxa taxa are never presented
to the allocator and therefore have no anchor. **This is not a compiler
bug** — the allocator is behaving exactly as designed for the original
fungi-only scope. Under the new W2E-A policy those historical observations
must still receive stable identity, and the proper repair is a scope
widening at the allocator input layer.

### Why the new-release build is deferred

Producing a new immutable release requires:

1. Re-running `compile_release.py` against the full COL XR 2026-07-17-XR +
   NorTaxa 1.284 archive.
2. Widening the allocator's input contract to accept observation-derived
   non-fungi NorTaxa taxa as a separate provenance stream (so their
   sporely_taxon_ids are allocated under a clear audit trail — not
   silently mixed with the fungi backbone).
3. Re-running the scope-filter/exporter that produces
   `global_macrofungi_tax-YYYY.MM.DD-NN/` with a policy amendment that
   emits `taxon_external_id.jsonl` rows for these non-fungi anchors
   without adding them to the macrofungi search cache.
4. Proving `tax-2026.08.01-01` remains byte-identical when re-run, and
   emitting a sibling release `tax-YYYY.MM.DD-NN` with a new manifest
   hash and full anchor provenance.

Each of those steps is a targeted structural change to production taxonomy
pipelines that warrants its own evidence + review pass. Attempting all
four in a single response would risk producing an ill-considered release
with hidden anchor-provenance defects — the opposite of what an
"immutable release" should mean. The user-authorised safety instruction
"do not insert ad hoc anchors manually" is respected by deferring rather
than shortcutting.

### Safe next step

Run W2E-A Part B as a focused follow-up session that:

1. Adds a new observation-derived-anchors input to `compile_release.py`
   reading `(source_system, namespace, external_id, taxonomicStatus=accepted,
   canonical_scientific_name)` tuples from the desktop SQLite and NorTaxa
   1.284 archive for the 21 audited identifiers (and any future
   equivalent set).
2. Allocates their anchors through `IdentityRegistry.allocate()` — the
   proper pipeline layer — and re-shards the registry.
3. Runs the scope-exporter with an amended scope predicate that emits
   `taxon_external_id.jsonl` rows for observation-derived anchors,
   labels their scope_state honestly (`not_evaluated` or the source's
   own kingdom classification), and keeps the macrofungi search cache
   untouched.
4. Regression proof: `tax-2026.08.01-01` outputs reproduce byte-identical
   when built from the same inputs; the new release ID differs from
   `tax-2026.08.01-01`; every newly emitted anchor carries provenance.
5. Re-run the real reconciliation + disposable simulation against the
   new release.

### Immediate impact of deferring Part B

* 43 observations remain in `unresolved_external_identifier` after Part A
  (of those, 21 would resolve with Part B, 5 are `absent_all_sources`
  NorTaxa, and 19 are iNat `absent_all_sources` that W2E-B addresses).
* No production impact: the new stable identity for the 157 out-of-cache
  observations is fully materialised through Part A alone.

## Safety

* production access: **no**
* production writes: **no**
* production migrations: **no**
* client cutover: **no**
* desktop taxonomy activation: **no**
* old release mutation: **no** — `tax-2026.08.01-01` bytes untouched
* name-only resolution: **no**
* out-of-scope concepts added to search cache: **no** — sparse registry
  materialisation only; cache never broadened
* new upstream source download: **no**
* ad hoc canonical-registry anchors inserted: **no**
