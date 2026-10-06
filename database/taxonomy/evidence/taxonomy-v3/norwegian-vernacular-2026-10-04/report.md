# Norwegian vernacular coverage audit and enrichment simulation

Date: 2026-10-04. Current bundle and active production release: `tax-2026.09.30-01`.

## Result

**1,483 new unique (target, language, name) rows on 691 canonical COL concepts** pass the conservative vernacular-only rule. They represent 1,485 source rows (two duplicate source records collapse), 1,478 distinct language/name pairs and 691 source usages. Affected concepts: **567 species, 120 genera, 3 varieties and 1 subspecies**.

**127 source usages remain exceptions: 82 species and 45 genera**, representing 257 source rows. 104 have only higher-classification disagreement; 23 have other or combined blockers. Family disagreement is not proof of different species: many may reflect revised placement. This audit does not automatically waive it. Grouped review of specific classification transitions could reduce the queue, without weakening identity reconciliation or treating all family mismatches as harmless.

Safe to automate as a **vernacular metadata association** under the simulated guards and explicit reviewed precedence. This is not evidence sufficient for identity reconciliation. Current matches are candidates for later implementation, not published names. Species-complex detection is limited to qualifiers and explicit annotations in the pinned sources; an unannotated scientific dispute cannot be discovered by exact matching alone.

## Coverage and counting units

NorTaxa 1.284 contains **55,909 Norwegian source vernacular rows**, **55,230 distinct (language, name) pairs**, on **32,980 source usages**. Languages: nb 36,243, nn 19,666, no 0. This includes all kingdoms.

Fungal subset: **10,229 source rows**, **10,093 distinct language/name pairs**, **5,700 source usages**.

- Already present on bundled canonical COL concepts: **6,454 source rows**, 6,401 distinct language/name pairs, 3,684 source usages.
- Already reaching selectable current cloud canonical concepts: **4,622 source rows**, 4,586 distinct language/name pairs, 2,357 source usages.
- Present on their existing source-bound bundled concept but not reaching selectable cloud canonical concepts (“stranded”): **5,607 source rows**, 5,541 distinct language/name pairs, 3,343 source usages. This includes fungi outside the product scope and is not entirely recoverable by this rule.
- Fungal names not reaching selectable cloud canonical concepts: **5,607 source rows**. Of these, 1,485 pass automatic enrichment, 255 are fungal exception rows and 3,867 are outside scope or lack an exact accepted target. The exception ledger additionally retains two non-fungal homonym rows.
- Not reaching selectable cloud canonical concepts across all kingdoms: **51,287 source rows**. Most are outside the fungal product scope; this is not the exception queue.
- Within current selectable scope: 4,622 unchanged + 1,485 automatic + 257 exception = **6,364 source rows**.
- Outside scope or lacking an exact eligible accepted target: **49,545 source rows**. Reasons overlap: no exact accepted canonical name 47,151; outside current cloud scope 2,387; rank mismatch 7; non-fungal source 45,678; qualified source 142; nomenclatural warning 257.

A source row is an entry in the pinned source extension, not a unique string or target row. Existing source aliases/duplicate rows can account for multiple source rows reaching one exported row. Production currently has **4,616 Norwegian exported rows**. Simulated additions would make **6,099**, about **32.1% more**. None were written to production.

## Rank breakdown

| Source rank | All source rows | Already cloud | New unique target rows | Affected concepts | Exception usages | Outside scope/no target rows |
|---|---:|---:|---:|---:|---:|---:|
| class | 200 | 10 | 0 | 0 | 0 | 190 |
| cultivar | 8 | 0 | 0 | 0 | 0 | 8 |
| family | 2,078 | 0 | 0 | 0 | 0 | 2,078 |
| form | 55 | 3 | 0 | 0 | 0 | 52 |
| genus | 4,319 | 276 | 241 | 120 | 45 | 3,713 |
| infraclass | 3 | 0 | 0 | 0 | 0 | 3 |
| infraorder | 4 | 0 | 0 | 0 | 0 | 4 |
| kingdom | 16 | 0 | 0 | 0 | 0 | 16 |
| order | 611 | 42 | 0 | 0 | 0 | 569 |
| phylum | 154 | 0 | 0 | 0 | 0 | 154 |
| section | 70 | 0 | 0 | 0 | 0 | 70 |
| species | 45,192 | 4,276 | 1,236 | 567 | 82 | 39,510 |
| subclass | 19 | 0 | 0 | 0 | 0 | 19 |
| subfamily | 475 | 0 | 0 | 0 | 0 | 475 |
| subgenus | 13 | 0 | 0 | 0 | 0 | 13 |
| suborder | 42 | 0 | 0 | 0 | 0 | 42 |
| subphylum | 33 | 0 | 0 | 0 | 0 | 33 |
| subspecies | 2,062 | 0 | 2 | 1 | 0 | 2,060 |
| superclass | 4 | 0 | 0 | 0 | 0 | 4 |
| superfamily | 11 | 0 | 0 | 0 | 0 | 11 |
| superorder | 2 | 0 | 0 | 0 | 0 | 2 |
| tribe | 18 | 0 | 0 | 0 | 0 | 18 |
| variety | 520 | 15 | 4 | 3 | 0 | 501 |

## Exception reasons

Counts overlap when one source usage has several blockers. `exception-source-usages.json` is the deduplicated 127-entry queue; `exceptions.json` retains every name/language record and evidence.

| Reason | Source vernacular rows | Source usages |
|---|---:|---:|
| existing_same_language_vernacular_differs | 6 | 2 |
| higher_classification_disagreement | 221 | 108 |
| multiple_canonical_targets_in_scope | 15 | 8 |
| qualified_or_aggregate_source | 19 | 10 |
| source_nomenclatural_status_ambiguous | 7 | 4 |
| source_not_fungi | 2 | 1 |

Exact mutually exclusive reason combinations are recorded in `summary.json`. They are: classification only 104; classification plus multiple targets 3; multiple targets only 5; qualified only 9; nomenclatural warning only 4; qualified plus vernacular conflict 1; non-fungal plus classification plus vernacular conflict 1.

Classification disagreements by source usages (overlapping): family 98, order 23, class 8, kingdom 1, phylum 1, genus 0. `classification-disagreements.json` names each source/target clade. An `Incertae sedis`/unknown/unclassified placement is treated as missing information and retained as a warning, not a material contradiction.

Multiple in-scope targets occur for **Coprotus, Hydnellum, Hydnum, Lepiota, Lycoperdon, Odontia, Sarcodon and Helvella crispa**. Candidate IDs are in `multiple-targets.json`. A duplicate outside the current scope is recorded as a diagnostic but does not violate uniqueness inside the eligible scope.

Qualified source usages: Ramaria rubrievanescens, Cortinarius milvinicolor, Ramaria strasseri, Ramaria conjunctipes, Inocybe praetervisa, Entoloma sericellum, Mycena pearsoniana, Ramaria aurea, Ramaria rubripermanens, Lentaria micheneri. Examples include `s. auct. scand.`, `sensu`, and `agg.`. Four further source usages have the raw nomenclatural status `illegitimate`: Hypsizygus tessulatus, Pseudoboletus parasiticus, Rhizopogon luteolus, Pseudoinonotus dryadeus. These are conservatively withheld, not claimed to be biological conflicts. No additional target qualifier/split-annotation or target nomenclatural-warning blockers were found in the scoped exception population.

Existing same-language name conflicts:

- Animal Melanogaster supplies `engblomsterfluer`/`engblomsterfluger`, while the fungal target has `slimknoller`. Kingdom/classification guards reject the association.
- Qualified Entoloma sericellum supplies `silkerødskivesopp`, `silkerødspore-gruppen`, `silkeraudskivesopp`, `silkeraudspore-gruppa`, while the target has `silkerødspore`/`silkeraudspore`. Existing names stay unchanged; the additional qualified names require review.

**Zero existing reviewed decisions contradict the scoped proposed automatic associations.** Approved exact mappings/supersessions are followed for existing coverage; rejected/non-exact/differing reviewed decisions block automation. All manual mappings and supersessions were read, not modified.

## Named regressions

| Case | Result | Evidence |
|---|---|---|
| Russula aeruginea / grønnkremle | Automatic | COL 61673 / 4TRHW; exact name/rank; unique scoped target; compatible classification. Lindblad vs Lindblad ex Fr. is diagnostic only. Nynorsk grønkremle also passes. |
| Henningsomyces puber / dunpipe | Exception | Full bundle has 14834 / 3KV7Z and 165605 / PLPT2. Only 14834 is in current scope, so current scoped ambiguity is gone. NorTaxa family Schizophyllaceae vs COL Marasmiaceae remains a blocker. Authorship agrees. |
| Helvella crispa / lys høstmorkel | Exception | Both 14541 / 3KJX6, authored (Scop.) Fr., and 165172 / PFDFL, authored Sowerby, are in scope. Uniqueness fails; authorship is not used to pick a winner. |
| Arrhenia chlorocyanea / grønn navlesopp | Unchanged | Already on COL 79753 / 5VSVR in bundle and cloud. No automatic addition or preferred-name replacement. |

Machine-readable regression evidence is in `named-regressions.json`.

## Machinery audit and implementation points

1. `scripts/national_source.py:_normalize_into` (around lines 690–870) already preserves separate scientific name/authorship, rank, current status, namespaced core/taxon IDs, higher classification, and extension/core row provenance. The audit calls its existing normalization unchanged, into /tmp. Raw NorTaxa `nomenclaturalStatus` is not currently in the normalized record; future normalization should preserve this existing source field additively so the simulation's raw-source status check is available to the builder. No new identifier semantics are needed.
2. `scripts/cross_source_mapping.py:_canonical_name` supplies existing NFC/casefold/whitespace normalization. Scientific name and authorship are separate upstream fields. No author-stripping heuristic, fuzzy synonym lookup or cross-source identity promotion was used. Existing accepted-status equivalence includes NorTaxa `valid` and COL `accepted`/`provisionally accepted`.
3. `scripts/compile_release.py:compile_release`, vernacular pipeline around **1212–1289**, is the implementation point for a metadata-only association after normal source core linkage. Reuse source record lookup, canonical target index, reviewed decisions and existing compiled-vernacular format. Keep normal original linkage; add only target vernacular metadata after eligibility. Record target/rule/classification/authorship diagnostics in the existing `vernacular.jsonl` provenance object. No allocation, registry alias, supersession, preferred national scientific name, or `source_usages` identity binding is created by this rule.
4. `source_usages.jsonl` remains the identity/bridge facts artifact (compiler around **1043–1119**). Existing reviewed mapping/supersession facts take precedence. The current local compiler source-usage artifacts were not retained; the audit reconstructs existing binding from the pinned registry plus unchanged reviewed ledgers and checks against bundled names. Stage 2 manifests are historical candidate evidence, not the candidate universe or current authority. The audit loads the no-cross-reference and non-one-to-one lists as annotations and recomputes targets from the current bundle/scope. **1,203 automatic source rows** carry annotations from these Stage 2 populations.
5. `scripts/bridge_emission.py` explicitly separates metadata carriage from authoritative external-ID emission (module docstring, lines **4–21**). Preserve this distinction: the vernacular-only evidence class must remain **ineligible** for identifier emission. Do not add it to `authoritative_bridge_emission.eligible`. Do not route a metadata association through an alias allocation to obtain a name.
6. `scripts/build_sqlite_candidate.py`, **Pass 4 around 882–913**, already consumes compiler vernacular rows, projects `source_code` to `source`, and deduplicates on target/language/name. It can consume additive metadata rows without database schema changes. Preserve existing preferred names; the simulation withholds a different existing name in the same language rather than displacing it.
7. `cloud_export.py:emit_vernacular` around **721–739**, and `macrofungi_scope.py:build_export` around **536–575**, already carry projected vernacular rows and restrict them to selectable scope. They need coverage/regression verification, not a parallel identity mechanism. Classification-only ancestors do not receive search vernaculars.
8. Release coverage/reporting in `scripts/build_release.py` and compiler manifest should account for automatic enrichment, unchanged names, exceptions and regressions, with the enrichment policy digest. Add targeted fixtures for qualifiers, cross-kingdom homonyms, contradictory reviewed decisions, same-language conflicts and duplicate scoped targets, plus identity/external-ID unchanged invariants.

## Provenance and schema

Normalized national `vernacular.jsonl` already has source code, source release, namespaced core record, preferred flag and provenance (archive member and row index). Compiler `vernacular.jsonl` retains them with `sporely_taxon_id`; its existing provenance object can additionally retain source usage, source/target names and authorships, classification comparison, rule/evidence class and association decision. The compiler manifest fingerprints this artifact.

`build_sqlite_candidate.py` discards this detailed provenance when writing `vernacular_min`; only `source` survives. `cloud_export.py` exports that compact structure. Detailed durable release provenance **does not require a database/cloud schema change** if the fingerprinted compiler vernacular/evidence artifact is archived with the release. Current release metadata stores its compiler-manifest digest, not the artifact content; keeping only the SQLite/gzip cannot recover all per-vernacular provenance later. Artifact retention therefore needs to be part of the owning release workflow. If runtime/cloud queries must retrieve full per-vernacular provenance without those artifacts, that would require a separate schema/API change; not implemented here.

This can be additive without changing the identity contract: declare a metadata-only rule, enrich existing compiler vernacular provenance and add source status preservation and coverage checks. Stable IDs, identity aliases, supersessions, bridges and national scientific-name authority remain untouched.

## Verification and reproducibility

Inputs are fingerprinted in `summary.json`: both pinned source archives, current gzip/SQLite bundle, registry manifest, manual mappings, supersessions, mapping/scope policies, compiler digest and the read-only live snapshot. The archive fingerprints agree with bundle metadata. Production snapshot contains 52,917 concepts. All selectable cloud Norwegian rows agree with the bundle. The 28 bundle-only names on 13 required ancestors are intentional export omissions, not missing species names.

Syntax verification passed. A repeat run produced byte-identical association/exception manifests; summaries agree except the hash of differently formatted input snapshot JSON. Named regression and automatic-row invariant assertions passed.

The audit opens SQLite read-only with query_only enabled, performs only SELECT on production, creates no registry/release/identity artifacts and asserts the named regressions and no contradictions in automatic rows. The automatic set has no competing NorTaxa source usages per target. Evidence manifests are simulation outputs, not taxonomy release datasets. No production, builder, policy, manual mapping or supersession edits were made.

Reproduce from repository root using the project Python:

```sh
.venv/bin/python database/taxonomy/scripts/national_source.py normalize --profile database/taxonomy/national_sources/nortaxa/1.284/source.json --archive database/taxonomy/sources/nortaxa/1.284/archive.zip --output /tmp/sporely-no-coverage-normalized
# Decompress the pinned bundle into /tmp/sporely-no-coverage-bundle.sqlite3; do not install it.
.venv/bin/python database/taxonomy/evidence/taxonomy-v3/audit_norwegian_vernacular_enrichment.py --normalized /tmp/sporely-no-coverage-normalized --bundle /tmp/sporely-no-coverage-bundle.sqlite3 --live-snapshot database/taxonomy/evidence/taxonomy-v3/norwegian-vernacular-2026-10-04/live-taxonomy-snapshot.json --output /tmp/sporely-no-coverage-repeat
```

`automatic-associations.json` is the proposed association manifest; `exceptions.json` has full per-name exception evidence; `exception-source-usages.json` is the concise review list. `all-source-associations.jsonl.gz` is the exhaustive all-kingdom coverage ledger. Separate conflict, multiple-target, classification and qualified-case manifests make individual questions inspectable.
