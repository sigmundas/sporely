# Stage 3 candidate `tax-2026.09.23-01` — build and verification record

Non-active production-scale candidate, built 2026-09-23 under the authorization
recorded in the closeout plan's `authorize-stage3-candidate-build` check. The
active release `tax-2026.08.01-01` was read only and never modified; the
candidate was never activated.

Build output lives outside the repository at
`/Users/sigmundas/Documents/Code/sporely/.stage3-build/2026-09-23-taxonomy-v2`
(4.5 GB). It is not committed. This document plus
`stage3-candidate-build-report.json` are the committed evidence; the machine
report carries the full per-artifact hash table.

## Reviewed relationships applied

Both approved 2026-09-23 by **Sigmund Ås** against gate
`f0c45157058d4fedab2c13719c6cf548`.

| Record | Kind | Evidence class |
|---|---|---|
| `nortaxa-53482-entoloma-conferendum-to-col-39ZCL` | `manual_approved_exact` | `shared_synonymy` — 7 of NorTaxa's 7 synonyms appear under COL `39ZCL` with byte-identical authorship |
| `nortaxa-52369-pholiotina-rugosa-superseded-by-col-5ZT3G` | `reviewed_supersession` | `reciprocal_accepted_synonymy` — each source publishes the other's accepted name as its own synonym; the only reciprocal case in 19,809 associations |

The reviewer's retention condition for 624680 is satisfied: the append-only
registry still records `(nortaxa, nortaxa_taxon_id, 52369) -> 624680` as an
anchor allocated in `tax-2026.07.29-01`, and every re-keyed binding carries
`superseded_from_sporely_taxon_id: 624680`.

## Inputs, all hash-pinned

| Input | Measured | Pinned |
|---|---|---|
| COL XR `archive.zip` | sha256 `397d701c…7814f9`, 620,976 normalized records, 0 orphan parents | matches `sources/col_xr/2026-07-17-XR/manifest.json` |
| NorTaxa `archive.zip` | sha256 `29c11c54…2fe22`, 229,018 taxa, 58,773 vernaculars | matches `sources/nortaxa/1.284/manifest.json` |
| `redlist-2021.xlsx` | 10,890,135 bytes, sha256 `c2fb6a5f…8bef04`, 34,171 assessments | matches `docs/redlist-overlay.md` lines 312-313 |
| Identity registry | 682,560 lines, concatenated sha256 `21b5d39d…1d3077` | matches `registry/canonical/manifest.json` |

The COL normalized record count equals the registry's 620,976 `col_xr` anchors
exactly.

## Determinism

Two compiles from identical inputs under one release identity, against separate
copies of the same base registry. **Every artifact byte-identical; zero
mismatches.**

| Artifact | sha256 | A == B |
|---|---|---|
| `taxa.jsonl` | `aa2a0d12…56bcf9` | yes |
| `source_usages.jsonl` | `9f92f500…075255` | yes |
| `vernacular.jsonl` | `8d8ab697…38ff5a2` | yes |
| `mappings.jsonl` | `5d041d63…1838c2` | yes |
| `redlist_no.jsonl` | `43ff7ed1…d62ca9` | yes |
| registry after compile | — | yes |
| SQLite candidate | `2d128d08…1b2a8a7` | yes |

`desktop-tax-2026.09.23-01.sqlite3` sha256 `6a1aff46…1170eb` (19,001,344 bytes).

## Coverage audit under the reviewed standard

Every cross-source association is emitted or refused with a recorded reason.
Counts from the candidate's own projection:

| Disposition | All bindings | On retained concepts | The plan's 2,041 vernacular-joined taxa |
|---|---:|---:|---:|
| emitted as a reviewed relationship | 1 | 1 | 1 |
| reviewable — published cross-reference exists, no decision yet | 4,862 | 1,888 | 849 |
| rejected — no published cross-reference | 14,945 | 5,193 | 1,191 |

Nothing unaccounted for. The candidate's own emission census records 3 emitted
identifier rows against 47,662 refused bindings, each refusal carrying the
policy reason: 17,674 `cross_source_automatic_exact_strict`, 2,133
`cross_source_automatic_exact_missing_authorship`, 27,855
`intra_source_synonym`.

Three rows for two relationships because 624680 held two NorTaxa usages — the
accepted `52369` and its intra-source synonym `58722` (`Conocybe rugosa`) —
both of which follow the supersession onto 83668.

## Verified behaviour

Against the candidate SQLite, the scoped projection and the desktop pack:

| Requirement | Result |
|---|---|
| resolve `nortaxa/nortaxa_taxon_id/52369` | **83668** |
| resolve `nortaxa/nortaxa_taxon_id/53482` | **7821** |
| search `Pholiotina rugosa` | 83668, `source=nortaxa`, `note=reviewed_supersession`, non-preferred |
| search `Conocybe rugosa` | 83668, `source=col_xr`, **preferred** — COL presentation unchanged |
| search `Entoloma conferendum` | 7821, preferred, `source=col_xr` |
| metadata explains the match | `source_system`, `namespace`, `external_name`, `note=authoritative_bridge:<class>` |
| vernaculars on 83668 | `slank ringkjeglesopp` (nb, nn) — carried across the supersession |
| vernaculars on 7821 | all four NorTaxa names, preferred flags intact |
| no duplicate concept | one `taxon_min` row for the rugosa pair; 624680 absent from `taxon_min` and from the desktop pack |
| ambiguity controls | 0 tuples resolving to more than one concept, across the whole table |
| no unrelated same-name merge | no automatic class is eligible; 17,674 strict name matches refused |

## Row and namespace change, scoped release

Active `tax-2026.08.01-01` → candidate `tax-2026.09.23-01`:

| Dataset | Active | Candidate | Δ |
|---|---:|---:|---:|
| `taxon.jsonl` | 52,917 | 52,917 | 0 |
| `scientific_name.jsonl` | 57,769 | 57,770 | +1 |
| `vernacular.jsonl` | 3,923 | 3,925 | +2 |
| `taxon_external_id.jsonl` | 52,881 | 52,884 | +3 |
| `taxon_external_id_legacy_integer.jsonl` | 0 | 0 | 0 |
| `taxon_redlist.jsonl` | 2,262 | 2,262 | 0 |

Namespace census for `taxon_external_id.jsonl`: active is 100%
`col_xr/col_usage_id` (52,881); the candidate adds a second namespace,
`nortaxa/nortaxa_taxon_id` (3).

**Sparse mappings, and it does change desktop search-pack content.**
`macrofungi_scope.build_desktop` loads `taxon_external_id.jsonl` straight into
`external_mapping`, which grows 52,881 → 52,884 and gains the
`nortaxa/nortaxa_taxon_id` namespace. `scientific_name` gains one row
(`Pholiotina rugosa`) and `vernacular_name` two; `taxon` is unchanged. The
desktop pack resolves both regressions directly.

Unscoped W1 export `taxon.jsonl` is 634,893 against the 634,894 recorded for
the earlier full build — one fewer concept, which is 624680's suppressed
canonical row.
