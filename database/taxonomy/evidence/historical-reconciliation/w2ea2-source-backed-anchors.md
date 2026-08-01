# W2E-A2 source-backed registry-anchor completion

**Scope**: the 21 diagnostic IDs classified as
`sqlite_present_no_registry_anchor` (19) or
`source_archive_only_no_registry_anchor` (2). Deferred: 5 NorTaxa +
19 iNaturalist `absent_all_sources`, 85 `manual_unresolved`, 30
`no_identity_evidence`.

## Design proof

1. **`sporely_taxon_id` allocation point**:
   `database/taxonomy/scripts/identity_registry.py`
   `IdentityRegistry.allocate()` and `bind_alias()`.
2. **Stability**: the registry is append-only; `allocate()` looks up
   first and returns any existing key unchanged; new anchors get
   strictly-increasing IDs. Existing IDs are never renumbered.
3. **Source evidence per ID**: every one of the 21 taxonIDs is present
   in `database/taxonomy/sources/nortaxa/1.284/archive.zip` `taxon.txt`
   with `taxonomicStatus=valid` (20 IDs, self-accepted) or `synonym`
   (1 ID, `194385 Distichia undulata` with `acceptedNameUsageID=189757`
   `Neckeropsis undulata`, itself valid).
4. **Unique COL target**: none of the 21 IDs have a COL cross-reference
   in the pinned source data. That is expected — plants and animals are
   outside the COL XR macrofungi backbone. The identity model does not
   require a COL binding: `IdentityRegistry.allocate()` accepts any
   `(source, namespace, identifier)` tuple.
5. **Pipeline layer responsible**:
   * `sqlite_present_no_registry_anchor` (19): the desktop SQLite
     carries the mapping `taxon_external_id_min(artsdatabanken, id)` →
     `taxon_min` row, but the fungi-scoped W1 pipeline never presented
     the row to the registry allocator. The gap is at the
     **canonical registry allocation** layer.
   * `source_archive_only_no_registry_anchor` (2): the SQLite
     compilation legitimately dropped these because they fall outside
     the fungi kingdom scope. The correct repair is *not* to broaden
     the fungi-scoped SQLite (that would pollute product taxonomy),
     but to feed the archive rows directly into the registry allocator
     via a curated input.
6. **Smallest correct change**: a single YAML policy file
   ([`database/taxonomy/policies/observation-derived-anchors.yml`](../../policies/observation-derived-anchors.yml))
   listing the 22 IDs with their NorTaxa provenance, and one dedicated
   allocator script
   ([`database/taxonomy/scripts/allocate_observation_derived_anchors.py`](../../scripts/allocate_observation_derived_anchors.py))
   that re-verifies every entry against the pinned NorTaxa archive and
   then calls `IdentityRegistry.allocate()` / `bind_alias()` unchanged.
   The output is an **append-only supplement directory** — the base
   registry shard bytes and the macrofungi release bytes are untouched.

**Stop-conditions checked, none tripped**: no ID lacks source evidence;
no ID maps to multiple COL concepts (none map to any COL concept, which
is fine); no ID conflicts with an existing registry anchor; no ID
requires scientific-name matching.

## Repair executed

### Part A — 19 SQLite-present identifiers

Allocated as NorTaxa anchors via `IdentityRegistry.allocate()`:
```
1600, 3026, 99419, 99439, 100513, 100758, 101107, 101557, 101602,
101969, 102317, 102346, 102409, 102774, 103138, 103316, 103560,
103958, 143556
```

### Part B — 2 archive-only identifiers

* `103408 Rubus` (genus) — allocated as anchor via the same path.
* `194385 Distichia undulata` — allocated as an **alias** to
  `189757 Neckeropsis undulata`. The accepted target is also allocated
  under the same policy (the allocator refuses aliases whose accepted
  target is not declared alongside them). Result: `194385` and `189757`
  share `sporely_taxon_id=634916`.

### Allocator invariants

* Refuses `--production`.
* Refuses to overwrite an existing output directory.
* Re-verifies every declared `(taxonID, scientificName, kingdom, rank)`
  against the archive.
* Uses `IdentityRegistry.allocate()` / `bind_alias()` verbatim — no
  ad-hoc `sporely_taxon_id` construction.
* Byte-identical output across two runs.

## Immutable output

**Supplement release `tax-2026.08.02-01`**:

| artefact | SHA-256 |
|---|---|
| supplement shard `part-0001.jsonl` | `a7cf966b71530b56e7b9cd544d45c2959c28752b13c22d623c34fbf7f8cb158a` |
| supplement `manifest.json` `concatenated_sha256` | `a7cf966b71530b56e7b9cd544d45c2959c28752b13c22d623c34fbf7f8cb158a` |
| `sporely_taxon_id` range | 634896 – 634916 (22 anchors + aliases) |
| NorTaxa archive SHA-256 | recorded in the release provenance JSON |
| policy SHA-256 | `b34a29c06260d6827773f0ffaca5c4c705f4e41da24b8214333be6a45bf6d396` |

**`tax-2026.08.01-01` immutability**: verified by re-hashing every file
in `database/reference_data/generated/taxonomy_v2/global_macrofungi_tax-2026.08.01-01/`
and comparing to the SHA-256s embedded in that release's
`taxonomy_export_manifest.json`. All 7 files match; scope-manifest
SHA `72758b2c574e8aea27432b6b55c62dfb6ad87f3fadc11ad1c892a61abf23ac4e`
unchanged.

## Reconciliation before / after

Real 369-record anonymised snapshot. Base registry + W2E-A2 supplement
stacked via repeated `--canonical-registry` flag.

| primary state | W2E-A after Part A | W2E-A2 after |
|---|---:|---:|
| resolved_exact | 211 | **232** |
| unresolved_external_identifier | 43 | **22** |
| manual_unresolved | 85 | 85 |
| no_identity_evidence | 30 | 30 |
| **total** | 369 | 369 |

Manifest semantic SHA-256: `884782d120ca446974d392f1118721c46c1592aac7dc67dff314f3dae37a0f9f`
Byte-identical across two runs into distinct output directories.

**Observation-level effects**

* Observations represented by the 19 SQLite-present IDs: **19** — all resolved
* Observations represented by the 2 archive-only IDs: **2** — all resolved
* Observations newly resolved (this stage alone): **21**
* Distinct external IDs newly resolved: **21**
* Remaining `unresolved_external_identifier` observations: **22**
  * (5 NorTaxa `absent_all_sources` + 17 iNaturalist `absent_all_sources`;
    total drops by 2 from 24 because two iNat IDs each appeared on two
    observations in the audit — the diagnostic distinct-ID count is 24,
    the observation count is 22.)

## Disposable Postgres simulation

Applied against the local disposable schema
`w2d_migration_simulation` (never a Supabase migration).

| dimension | count |
|---|---:|
| `identification_snapshot` | 369 |
| `resolution_link` | 369 |
| `registry_concept` | **141** (39 in_cache + 102 out_of_cache) |
| `external_mapping` | **143** |

Cache × scope (independence proof):

| cache_state | scope_state | count |
|---|---|---:|
| in_cache | include | 39 |
| in_cache | not_evaluated | 0 |
| out_of_cache | include | 0 |
| out_of_cache | not_evaluated | 102 |

**Search-cache membership unchanged**: the pinned macrofungi release's
`taxon_external_id.jsonl` still holds 52 881 `col_xr:col_usage_id`
rows. The 22 new registry entries live in the sparse registry only.

### Invariants proven

| proof | verdict |
|---|---|
| Idempotency (apply twice) | ✓ identification_snapshot 369↔369, registry_concept 141↔141, external_mapping 143↔143, snapshot fingerprint stable |
| Rollback (induced P0001) | ✓ 0 orphan rows across all tables |
| Immutable identification snapshots | ✓ trigger blocks any UPDATE to `original_*` while `snapshot_locked=true` |
| Unchanged search cache | ✓ release bytes untouched |
| Out-of-cache materialization | ✓ 102 out_of_cache concepts recorded in sparse registry |
| Correct external mapping counts | ✓ 143 = 122 (Part A) + 21 (Part B), matches manifest exact-signal count |

### Conflict invariant (new)

`external_mapping` insert now uses:

```
ON CONFLICT (source_system, namespace, external_id) DO UPDATE
  SET sporely_taxon_id = external_mapping.sporely_taxon_id
  WHERE external_mapping.sporely_taxon_id = excluded.sporely_taxon_id;
```

with a follow-up `RAISE EXCEPTION 'W2E-A2 external_mapping conflict …'`
when the update matches zero rows (different target). The enclosing
`simulate_migration` subtransaction catches the exception and rolls
back with zero partial changes.

**Integration test** (`W2D_INTEGRATION=1`):
`integration: W2E-A2 conflict invariant — reallocating an external_id
to a different sporely_taxon_id rolls back with zero partial rows` — pass.
Full web suite: **22 tests, 22 pass, 0 skipped**.

## Safety

* production access: **no**
* production writes: **no**
* production migrations: **no**
* client cutover: **no**
* desktop taxonomy activation: **no**
* old release mutation: **no** (`tax-2026.08.01-01` files byte-identical)
* name-only resolution: **no**
* search-cache broadening: **no**
* new upstream download: **no**
* manual / ad-hoc registry anchors: **no** — every anchor was allocated
  through `IdentityRegistry.allocate()` from a re-verified source row
