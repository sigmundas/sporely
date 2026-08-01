# W2E-B corrections and release-lineage freeze

Two blocking issues in the accepted-part W2E-B were corrected here.

## Correction 1 — revert the 19 iNaturalist-native anchors

**Accepted architecture:** Catalogue of Life XR is the canonical identity
backbone; iNaturalist is a discovery/reference provider, not a canonical
identity authority. An active iNaturalist record without an exact COL
binding is insufficient evidence to allocate a `sporely_taxon_id`.

The corrected W2E-B policy
([`recovered-external-sources-w2eb-v2.json`](../../policies/recovered-external-sources-w2eb-v2.json))
reclassifies every one of the 19 iNat entries from
`exact_accepted_mapping` to
`source_record_exists_but_no_exact_canonical_connection`. The allocator's
`ALLOCATING_CLASSIFICATIONS` set does not include that classification, so
no iNat anchor is emitted.

Provider snapshots are preserved verbatim in the policy JSON: acquired
scientific name, rank, iconic taxon, active/deleted status, endpoint,
acquisition date, per-request raw-response SHA-256. The original
iNaturalist identifiers remain in the observation snapshots and in the
policy record — they are never rewritten. No scientific-name equality
matching was attempted.

## Correction 2 — release-immutability + lineage freeze

Two supplements had their bytes redefined under an already-published
release ID:

| release ID | prior status | resolution |
|---|---|---|
| `tax-2026.08.02-01` | shard `a7cf966b…` published, then silently redefined as `06c0ee6b…` when the shard-order bug was fixed | **SUPERSEDED**. The `a7cf966b…` bytes are the historical record; W2E-B evidence retains that hash. The corrected supplement is re-emitted under a NEW ID `tax-2026.08.02-02` with shard `6c95612b83fb…`. |
| `tax-2026.08.03-01` | contained 19 iNat-native anchors (rejected by architecture) | **SUPERSEDED**. Retained hash `bdfdb8c0…`. Corrected supplement re-emitted under `tax-2026.08.03-02` with shard `dedf0fbc1d69…` containing only the NorTaxa 48041 alias. |

The `/tmp/`-only supplement bytes were never committed to Git; only the
recorded SHAs live in the prior evidence files
(`w2ea2-source-backed-anchors.json`, `w2eb-external-source-recovery.json`),
which remain untouched. Those files ARE the historical record. Any
future artefact bearing those hashes is identifiable as the superseded
variant.

Full lineage documented in
[`release-lineage.json`](release-lineage.json).

Invariants enforced:

* no previously published release identifier refers to two byte-distinct artefacts;
* supersession does not delete historical evidence — every prior evidence file remains committed;
* corrected releases carry NEW release IDs;
* the `depends_on` graph never points at a superseded ID
  (`tax-2026.08.03-02.depends_on = ["tax-2026.08.02-02"]`).

## Correction 3 — formal dependency validation

Argument-order trust is replaced with a validating loader
[`database/taxonomy/reconciliation/supplement_loader.py`](../../reconciliation/supplement_loader.py).
`load_supplement_chain()` fails closed on:

| defect | test |
|---|---|
| standalone supplement (no base) | `test_reject_standalone_supplement_load` |
| unknown `artifact_kind` | `test_reject_unknown_artifact_kind` |
| `base_release_id` mismatch | `test_reject_base_release_id_mismatch` |
| base export/scope hash mismatch | `test_reject_base_hash_mismatch` |
| depends_on release missing | `test_reject_missing_dependency` |
| supplements out of order | `test_reject_out_of_order_supplements` |
| depends_on hash mismatch | `test_reject_dependency_hash_mismatch` |
| release-ID reuse with different hashes | `test_reject_release_id_reuse_with_different_hashes` |
| self-dependency cycle | `test_reject_self_dependency_cycle` |
| shard SHA-256 disk mismatch | `test_reject_shard_sha_mismatch_on_disk` |
| declared shard SHA-256 ≠ manifest | `test_reject_declared_shard_sha_disagrees_with_manifest` |
| empty chain → valid shell | `test_load_empty_chain_returns_valid_shell` |

`python -m pytest tests/taxonomy/test_supplement_loader.py -q` → **12 passed**.
Full desktop suite (`tests/taxonomy/`) → **55 passed** (43 existing + 12 new).

The CLI now takes `--release-dir` + `--canonical-registry` (base) +
repeatable `--supplement <dir>`. The loader validates the whole chain
before any shard byte is consumed; only when validation passes does
`PinnedRelease.load()` see the supplement paths.

## Corrected release chain and dependency order

```
base:           tax-2026.08.01-01
                    (scope_manifest 72758b2c…ac4e; export_manifest sha256 verified)
supplement 1:   tax-2026.08.02-02
                    depends_on: []
                    shard: 6c95612b83fbf684d9db7c66fe515b2225e57c8b3b6ceb03e001867fad41067b
                    allocates: 21 nortaxa anchors + 1 alias binding
supplement 2:   tax-2026.08.03-02
                    depends_on: [tax-2026.08.02-02]
                    shard: dedf0fbc1d691b8f716e8ece3a365d1ab3835474912bef5fc9a527fec784eadd
                    allocates: 1 alias binding (nortaxa 48041 -> existing 630103)
                                — no fresh sporely_taxon_id
```

Superseded and retained:

```
tax-2026.08.02-01   shard a7cf966b71530b56e7b9cd544d45c2959c28752b13c22d623c34fbf7f8cb158a
tax-2026.08.03-01   shard bdfdb8c08bf48e9c457d31c009f6ee9446d6f485e010a35d90f221f3a281e947
```

## Reconciliation proof

Real 369-record snapshot, corrected chain loaded via `SupplementLoader`.

| primary state | count |
|---|---:|
| resolved_exact | **233** |
| unresolved_external_identifier | **21** |
| manual_unresolved | **85** |
| no_identity_evidence | **30** |
| **total** | **369** |

Matches the expected corrected counts exactly.

Composition of the 21 remaining `unresolved_external_identifier`:

* **19** iNaturalist records with source_record_exists_but_no_exact_canonical_connection — provider snapshots, IDs, acquisition hashes preserved, no scientific-name equality attempted
* **2** NorTaxa records `deleted_or_unavailable` (`39825472`, `40992445`; both 404 from the current API)

**NorTaxa 48041 alias resolves exactly.**
Evidence chain from the manifest: `nortaxa:nortaxa_taxon_id:48041` →
canonical registry alias (tax-2026.08.03-02) → `sporely_taxon_id=630103`
(Neuropogon antarctica, existing base-registry anchor).

Manifest semantic SHA-256:
`1beaa33f3891b216d3bc7c6d34cd96df1a936627c5a6f749a515cc75d51c094e`
Byte-identical across two reconciliation runs into distinct output
directories.

## Disposable Postgres simulation

Applied against the isolated schema `w2d_migration_simulation` on the
local disposable Supabase stack.

| dimension | count |
|---|---:|
| `identification_snapshot` | **369** (immutable) |
| `resolution_link` | **369** |
| `registry_concept` | **142** (`in_cache/include=39`, `out_of_cache/not_evaluated=103`) |
| `external_mapping` | **144** |

Cache × scope stays independent:

| cache_state | scope_state | count |
|---|---|---:|
| in_cache | include | 39 |
| out_of_cache | not_evaluated | 103 |

Search-cache membership unchanged: the pinned macrofungi release's
`taxon_external_id.jsonl` still holds 52 881 rows keyed on
`col_xr:col_usage_id`. The two supplements added only sparse-registry
entries, not searchable cache entries.

### Invariants proven against the corrected manifest

| proof | verdict |
|---|---|
| corrected counts 233 / 21 / 85 / 30 | ✓ |
| NorTaxa 48041 alias resolves exactly | ✓ (evidence chain in manifest: canonical registry alias → 630103) |
| 19 iNaturalist IDs preserved but unresolved | ✓ (`unresolved_external_identifier`; provider snapshots, IDs, and per-request hashes recorded in policy file, never rewritten) |
| 2 NorTaxa 404 IDs preserved but unresolved | ✓ (`unresolved_external_identifier`; original identifiers preserved verbatim in snapshot) |
| search-cache contents unchanged | ✓ (release bytes untouched) |
| identification snapshots immutable | ✓ (trigger blocks UPDATE of `original_*` while `snapshot_locked=true`) |
| idempotency | ✓ (twice apply → row counts and snapshot fingerprint stable) |
| rollback | ✓ (induced P0001 → zero orphan rows) |
| external-mapping conflicts cause full rollback | ✓ (W2E-A2 hardened invariant carries; integration test `W2E-A2 conflict invariant` still passes) |
| dependency/hash/order validation fails closed | ✓ (12/12 SupplementLoader tests) |

## Safety

* production access: **no**
* production writes: **no**
* production migrations: **no**
* client cutover: **no**
* desktop taxonomy activation: **no**
* name-only resolution: **no**
* search-cache broadening: **no**
* provider-native canonical anchors: **no** (iNat allocations reverted)
* release-ID reuse: **no** (tax-2026.08.02-01 and tax-2026.08.03-01 marked SUPERSEDED; corrections issued under NEW IDs)
* existing artifact deletion: **no** (prior evidence files retained)
