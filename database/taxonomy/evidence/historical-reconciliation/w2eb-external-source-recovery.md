# W2E-B external-source recovery

**Scope**: 22 identifiers unresolved after W2E-A2 (`absent_all_sources` in the
W2D-R diagnostic). Deferred and untouched: 85 `manual_unresolved`, 30
`no_identity_evidence`.

## Count reconciliation

The prior diagnostic reported 24 distinct IDs (5 NorTaxa + 19 iNat) as
`absent_all_sources`. My W2E-A2 wrap-up prose then said 22 observations =
5 NorTaxa + 17 iNat — using "IDs" and "observations" as if they were the
same. They are not; here are the current numbers, measured separately:

| measurement | value |
|---|---:|
| unresolved observations | **22** |
| external-ID occurrences within them | **22** |
| distinct external IDs | **22** |
| distinct IDs by provider — NorTaxa | **3** (`39825472`, `40992445`, `48041`) |
| distinct IDs by provider — iNaturalist | **19** |
| observations carrying more than one distinct ID | **0** |
| observations carrying duplicated signals for the same ID | **0** |

**Delta explained**: the W2D-R diagnostic classifier only probed
`taxon_external_id_min` (`source_system='artsdatabanken'`) and
`taxon_min.inaturalist_taxon_id` — it did **not** probe the canonical
registry directly for `outside_release_scope` vs `absent_all_sources`.
Two of the 5 NorTaxa IDs previously classified as absent (`41020336`,
`41096500`) are in fact anchored in the base registry under
`(nortaxa, nortaxa_taxon_id)`. Before W2E-A those anchors did not resolve
because the resolver required release-backbone membership at Level 4.
Once W2E-A Part A lifted that gate, they became `resolved_exact` via
canonical registry, leaving 22 truly unresolved identifiers (not 24).

## Supplement contract

`tax-2026.08.02-01` release JSON was extended to declare the formal
supplement contract; its **identity records are byte-identical** to the
W2E-A2 output. The additions:

```
artifact_kind                        registry_supplement
supplement_contract_version          supplement-contract-1.0.0
supplement_release_id                tax-2026.08.02-01
base_release_id                      tax-2026.08.01-01
base_release_dependency              {export_manifest_sha256, scope_manifest_sha256}
depends_on                           []
required_application_order           4 explicit steps
compatibility_rules                  6 explicit rules
```

Key compatibility rule: **a supplement must not be loaded as a standalone
taxonomy/search release** — it lacks scope, name and vernacular exports.
Consumers must apply it strictly on top of the declared base and any
`depends_on` entries.

### W2E-A2 shard-order defect (fixed)

The W2E-A2 shard emitted the synonym alias line **before** its accepted-
target anchor line because the writer sorted by `(sporely_taxon_id, kind)`
where `'alias' < 'anchor'` lexicographically. The reconciliation CLI's
shard indexer loaded it fine (no ordering enforcement), but
`IdentityRegistry.load()` — which the W2E-B allocator uses to absorb
depends-on supplements — rejected the file with
`registry line 22 alias references unknown anchor sporely_taxon_id 634916`.

The fix is a one-line sort-key change:

```python
sorted(newly_created, key=lambda a: (a.sporely_taxon_id, 0 if a.kind == "anchor" else 1))
```

Regenerated shard SHA-256:

| stage | supplement shard sha256 |
|---|---|
| before fix | `a7cf966b71530b56e7b9cd544d45c2959c28752b13c22d623c34fbf7f8cb158a` |
| after fix  | `06c0ee6bf17340e2cea0a502d14a26ac8ab9f6dda9da14fe73de7dfae7e18914` |

**No identity records were renumbered or reassigned** — every one of the
22 (sporely_taxon_id, source, namespace, identifier, kind) tuples is
present in both versions; only the emit order changed.

## Source recovery

### NorTaxa (3 IDs)

Endpoint: `https://artsdatabanken.no/api/Taxon/{id}`.

| taxonID | HTTP | classification | evidence |
|---|---|---|---|
| `39825472` | 404 | `deleted_or_unavailable` | "Taxon with id39825472 was not found." |
| `40992445` | 404 | `deleted_or_unavailable` | "Taxon with id40992445 was not found." |
| `48041` | 200 | `exact_synonym_replacement_mapping` | `taxonID=48041` returned as accepted alternate form of `scientificNameID=72068` (`Neuropogon antarctica`); 72068 already anchored at `sporely_taxon_id=630103`, so W2E-B binds an alias — no new anchor allocation for that concept. |

### iNaturalist (19 IDs)

Endpoint: `https://api.inaturalist.org/v1/taxa/{id}` with a labelled
user agent. Every response had `is_active=true` and
`current_synonymous_taxon_ids=null`. Since iNat does not expose a COL
usage ID cross-reference in the endpoint, allocations are emitted as
**iNat-native anchors** (source=`inaturalist`, namespace=
`inaturalist_taxon_id`) without a COL binding. This matches the identity
model — the sparse registry accepts source-native anchors and
`cache_state=out_of_cache` is preserved.

| classification | count |
|---|---:|
| `exact_accepted_mapping` | 19 |

Names span macrofungi and non-fungi, e.g.
`Trametes ochracea`, `Lactarius rufus`, `Marasmius oreades`,
`Cortinarius sanguineus`, `Inonotus obliquus` (fungi);
`Eriophorum angustifolium`, `Typha laxmannii`, `Homo sapiens`,
`Austrocactus coxii` (non-fungi). Every allocation went to
`cache_state=out_of_cache`; the macrofungi search cache was not touched.

### Raw responses

Per-entry raw-response SHA-256s are embedded in
[`database/taxonomy/policies/recovered-external-sources-w2eb.json`](../../policies/recovered-external-sources-w2eb.json).
The raw response bodies themselves are **not committed** — they were
retained locally under `/tmp/w2eb-recovery/` during acquisition. Any
future rerun of the policy must reproduce the same raw-response SHAs to
be considered equivalent evidence.

## Supplement release `tax-2026.08.03-01`

* `supplement_shard_sha256`: `bdfdb8c08bf48e9c457d31c009f6ee9446d6f485e010a35d90f221f3a281e947`
* `depends_on`: `["tax-2026.08.02-01"]`
* `sporely_taxon_id_range`: 634917 – 634935 (**19 new anchors** allocated;
  20 total records emitted = 19 anchors + 1 alias binding for 48041 →
  existing 630103)
* build byte-identical across two runs into distinct output directories

## `tax-2026.08.01-01` immutability

Verified: every file in `database/reference_data/generated/taxonomy_v2/global_macrofungi_tax-2026.08.01-01/`
matches the SHAs recorded in `taxonomy_export_manifest.json`. Scope-manifest
SHA `72758b2c…ac4e` unchanged.

## Reconciliation before / after

| primary state | after W2E-A2 | after W2E-B |
|---|---:|---:|
| resolved_exact | 232 | **252** |
| unresolved_external_identifier | 22 | **2** |
| manual_unresolved | 85 | 85 |
| no_identity_evidence | 30 | 30 |

* observations newly resolved: **20**
* distinct IDs newly resolved: **20**
* remaining unresolved observations: **2** (39825472, 40992445 — both
  404 from the current NorTaxa API)
* new manifest semantic SHA-256:
  `054a0de78c354ab9fc2e0978b3be53fe7bd1adb2361ea1e167e0846ffd8de8c6`
* byte-identical across two reconciliation runs

Resolution-method distribution after W2E-B:

| method | resolutions |
|---|---:|
| direct_taxonomy_v2_mapping (release backbone) | 54 |
| trusted_secondary_provider_mapping (canonical registry) | 198 |
| legacy_lookup_chain | 0 |
| pinned_synonym_relationship | 0 |

## Disposable Postgres simulation

Applied against the local disposable schema `w2d_migration_simulation`.

| dimension | count |
|---|---:|
| `identification_snapshot` | 369 |
| `resolution_link` | 369 |
| `registry_concept` | **161** (39 `in_cache` + 122 `out_of_cache`) |
| `external_mapping` | **163** |

Cache × scope independence still holds:

| cache_state | scope_state | count |
|---|---|---:|
| in_cache | include | 39 |
| out_of_cache | not_evaluated | 122 |

### Invariants proven

| proof | verdict |
|---|---|
| Idempotency | ✓ apply twice → identification_snapshot 369↔369, registry_concept 161↔161, external_mapping 163↔163, snapshot fingerprint `9f7107e948babb356140247802b4b7aa` stable |
| Rollback | ✓ induced P0001 → zero orphan rows across all tables |
| Snapshot immutability | ✓ trigger still blocks UPDATE of `original_*` while `snapshot_locked=true` |
| Mapping-conflict rollback | ✓ carried over from W2E-A2 (`W2E-A2 external_mapping conflict` raises inside `simulate_migration`, subtransaction rolls back with zero partial changes) |
| Unchanged search-cache contents | ✓ release `taxon_external_id.jsonl` still 52 881 rows keyed on `col_xr:col_usage_id` |
| Correct supplement application order | ✓ engine consumes `--canonical-registry` flags in argv order (base, W2E-A2, W2E-B). The W2E-B allocator refuses to run without `--depends-on-supplement` pointing at tax-2026.08.02-01 and absorbs it into the working registry before allocation — so 634917 is deterministically the next available ID, and reordering the CLI flags produces a different manifest that would violate the contract |

## Safety

* production access: **no**
* production writes: **no**
* production migrations: **no**
* client cutover: **no**
* desktop taxonomy activation: **no**
* existing release mutation: **no** (`tax-2026.08.01-01` bytes unchanged; `tax-2026.08.02-01` identity records unchanged)
* name-only resolution: **no**
* search-cache broadening: **no**
* manual/ad-hoc registry anchors: **no**
* new upstream download: **yes** — controlled read-only GETs to
  `artsdatabanken.no/api/Taxon/{id}` (3 requests) and
  `api.inaturalist.org/v1/taxa/{id}` (19 requests) for the 22
  unresolved identifiers only. No auth tokens sent. Per-request
  raw-response SHA-256s recorded in the policy file. Response bodies
  retained locally under `/tmp/w2eb-recovery/` and NOT committed.

## Remaining unresolved

Two NorTaxa IDs (`39825472`, `40992445`) return 404 from the current
Artsdatabanken API. They are obsolete or purged and have no exact
source-backed identity available. Their observations remain
`unresolved_external_identifier`; the snapshot preserves the original
scientific-name text and the exact identifier verbatim per contract §8.
