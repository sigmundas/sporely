# Relationship-based pass over the species pairs not in S1 (2026-10-21)

This pass is read-only. It approves nothing and does not change the S1
package. It covers the 2,805 species pairs in `../data/species-not-merged.csv`
(B 1,948, A1 766, A2 86, C 3, D 2).

The pass grades each pair on source relationships before authorship strings:
1. accepted names;
2. basionyms;
3. synonymy;
4. nomenclatural status;
5. external identifiers;
6. vernacular names, as support only;
7. authorship strings, last.

**Inputs.** All local and pinned; nothing was downloaded.
- COL XR `397d701c…`;
- NorTaxa 1.284 `29c11c54…`;
- Dyntaxa 2026-09-30, as a third witness;
- release `e349e8b1…`;
- the release evidence `manual_mappings.json`;
- the audit work DB.

## 1. Field inventory (`data/field_inventory.csv`)

The relationship fields exist, but the compiler drops them:

| Source | Field | Coverage | Compiler |
|---|---|---|---|
| COL | `basionymID` | 14.9% of fungal rows; 1,301 of 2,805 pairs | discarded |
| COL | `NameRelation` | 119,821 basionym, 9,850 spelling-correction, 1,216 homotypic and 424 based-on rows | discarded (file not read) |
| COL | `TypeMaterial` | 3,348 rows | discarded |
| COL | `link` (Index Fungorum `RecordID`) | any link 93.6%; Index Fungorum `RecordID` 38.9% (corrected in pass 2); 2,039 pairs | discarded |
| COL | `alternativeID` (iNaturalist, Dyntaxa, Wikidata) | 36.6% | the normaliser sets `external_ids={}` (`normalize_col_xr.py:208`) |
| COL | `combinationAuthorship` / `basionymAuthorship` | 45.4% / 17.0% (corrected in pass 2) | discarded |
| COL | `nameStatus` | 1.5% | imported to raw, used only by vernacular projection; identity mapping never reads it |
| COL | `combinationAuthorshipID` / `basionymAuthorshipID` | 0% | – |
| COL | `nameAlternativeID` | 647 IRMNG and 601 WoRMS LSIDs (corrected in pass 2) | discarded |
| COL | `TaxonConceptRelation` | empty | – |
| NorTaxa | `acceptedNameUsageID` | – | imported, to bind synonyms |
| NorTaxa | `nomenclaturalStatus` | 37.3% of fungal rows | vernacular projection only |
| NorTaxa | `namePublishedIn` | 21.9% | discarded (`national_source.py:824`) |
| NorTaxa | basionym field (`originalNameUsageID`) | does not exist in the archive | – |
| Dyntaxa | homotypic / heterotypic synonym status | – | collapsed to `synonym` (`compile_release.py:2162-2167`) |

COL's Norwegian vernaculars come mostly from COL source 2030, which is NorTaxa
itself (6,032 of 6,691 rows). They are therefore copies, not independent
evidence.

## 2. Method (`scripts/relate.py`)

Each pair is resolved in both sources:
- NorTaxa's name and authorship are looked up in COL, and COL's are looked up in NorTaxa;
- the COL concept's homotypic group is built from its `basionymID` and the `NameRelation` basionym, homotypic, based-on and spelling-correction links;
- the synonyms each source publishes are intersected;
- Dyntaxa is reached through COL's own `dyntaxa:` alternative id.

Authorship is compared last. "Same author" means one of three things:
- comparator-compatible;
- equal after documented folding (diacritics, ð/ø/æ/ß, punctuation, particles);
- equal on either side of the ICN sanctioning colon.

Abbreviation-prefix, spelling-distance, ordering and partial-list relations
are labelled **candidates**. No local author standard exists. A 4-letter
overlap is only a discovery flag (54 pairs).

**Buckets.** The buckets are disjoint, assigned in this precedence:
X1 > X2 > B1 > B2 > B3 > B4 > B5 > B6 > R.

| Bucket | Rule | Pairs |
|---|---|---|
| X1 conflict | Contradicting evidence: a different basionym author (139), class or phylum disagreement (176), an iNaturalist id conflict (25), or one source placing the other's name+authorship under a different concept (COL 20, NorTaxa 9) | 356 |
| X2 ambiguity | Qualified or unpublished authorship (76), a blocked NorTaxa status (49), a provisional COL concept (19), not one-to-one (18), a possible homonym with disjoint authors and no relationship (23), COL nameStatus (3) | 145 |
| B1 explicit cross-reference | COL lists NorTaxa's exact name+authorship as a synonym of the COL concept (88), or NorTaxa lists COL's under the NorTaxa concept (1). No approved manual mapping matched. | 89 |
| B2 shared homotypic | Both sources publish a synonym that is homotypic with the COL concept: through a COL basionym link (456 pairs) or by basionym author = accepted author | 477 |
| B3 basionym inferred | Same epithet and the same parenthetical basionym author. COL's side is a source row in 374 pairs; NorTaxa's side is always inferred from its authorship string | 460 |
| B4 heterotypic or uncertain | Only synonymy not shown to be homotypic | 109 |
| B5 accepted name + vernacular | Same accepted name plus an **independent** shared Norwegian vernacular (COL source ≠ 2030), not reused, genus-level or aggregate | 10 |
| B6 normalised-authorship candidate | Folded-equal, abbreviation prefix, spelling distance ≤ 2, ordering, or partial or ex-author list. Not evidence | 797 |
| R residual | Name equality only | 362 |

High-confidence association candidates are **B1 + B2 = 566**
(`data/promoted_pairs.csv`). Of these, 194 carry an order or family
placement difference, so the placement decision still applies to them.
Everything else is review material.

**Pairs with any evidence** (`data/summary.json`). 1,647 pairs have at least
one positive relationship item:

| Evidence | Pairs |
|---|---|
| Same basionym author | 1,099 |
| iNaturalist id agrees (inferred: NorTaxa's id is `legacy_compat`) | 592 |
| Shared homotypic synonym, through a COL link | 565 |
| Shared non-homotypic synonym | 421 |
| Dyntaxa (linked by COL) uses NorTaxa's authorship | 171 |
| COL lists the NorTaxa string | 100 |
| Dyntaxa homotypic synonym matches a NorTaxa synonym | 51 |
| Basionym author = accepted author | 47 |
| Shared independent vernacular | 42 |

A further 478 pairs share a vernacular only through COL's copy of NorTaxa,
and that is not counted.

**Transitions** (rows: current category; columns: new bucket):

| Current | X1 | X2 | B1 | B2 | B3 | B4 | B5 | B6 | R |
|---|---|---|---|---|---|---|---|---|---|
| A1 (766) | 0 | 0 | 3 | 184 | 171 | 33 | 6 | 356 | 13 |
| A2 (86) | 0 | 3 | 0 | 47 | 31 | 1 | 0 | 4 | 0 |
| B placement_only (504) | 0 | 0 | 1 | 147 | 129 | 16 | 0 | 211 | 0 |
| B authorship_conflicting (447) | 92 | 18 | 70 | 49 | 84 | 12 | 2 | 90 | 30 |
| B multiple_or_other (693) | 250 | 40 | 11 | 35 | 29 | 27 | 0 | 56 | 245 |
| B basionym_omitted (88) | 0 | 0 | 0 | 7 | 0 | 10 | 0 | 0 | 71 |
| B ex_author_differs (82) | 0 | 0 | 4 | 6 | 12 | 7 | 2 | 51 | 0 |
| B partial_list (38) | 0 | 0 | 0 | 2 | 4 | 3 | 0 | 29 | 0 |
| B qualified / unpublished / malformed / status / provisional (84) | 1 | 83 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| B col_places_nortaxa_under_other (11) | 11 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| B missing_without_support (1) / D (2) | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 3 |
| C (3) | 2 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

## 3. Worked examples

- **Cladonia pocillum.** NorTaxa 69234 (628910) `(Ach.) Grognot`; COL VPKC (613472) `(Ach.) O.J. Rich.`, Index Fungorum 121177.
  - COL's `basionymID` is KCNH, *Baeomyces pocillum* Ach. (Index Fungorum 477398), which carries 11 homotypic recombinations.
  - NorTaxa does not list *Baeomyces pocillum*. Its only synonym is *C. pyxidata* var. *pocillum* (Ach.) Flot.; COL has that name as (Ach.) Fr. in the same homotypic group, so the authors differ.
  - Both basionym authors are Ach.; the iNaturalist id 217136 agrees; *kalkbeger* is an independent COL vernacular.
  - **B3, basionym inferred.** It is the same combination, so only the combining author's attribution differs. Index Fungorum's record for "(Ach.) Grognot" would make it verified.
- **Cladonia stereoclada.** NorTaxa 69299 `des Abb.`; COL VPLV `Abbayes`, Index Fungorum 365712. Neither side lists a synonym or basionym.
  - **B6 candidate.** The particle plus abbreviation prefix is plausible (des Abbayes), but no local author list can confirm it.
- **Dermatocarpon taminium.** NorTaxa 81992 `Heidmarsson`; COL 34WYJ `Heiðm.`, Index Fungorum 395212. Folded (ð→d), Heiðm. is a prefix of Heidmarsson; it is also a 4-letter discovery hit. There is no relationship row.
  - **B6 candidate.** It needs the Authors of Fungal Names list.
- **Entoloma sphagneti.** NorTaxa 53677 `Navaeu`; COL 6FDPL `Naveau`, Index Fungorum 254784.
  - Both sources publish *Rhodophyllus sphagneti* (Naveau) Kühner & Romagn.: NorTaxa 59943 and COL SKHS5.
  - COL links SKHS5 homotypically, and NorTaxa's own synonym spells the basionym author "Naveau". Dyntaxa also lists it as a homotypic synonym, and also repeats the typo.
  - **B2, high confidence.** The accepted authorship "Navaeu" is a NorTaxa misspelling.
- **Clavaria spathula.** NorTaxa 52406 `Dicks.`; COL 5Z9BM `O.F. Müll.`, sourced from UKSI, so there is no Index Fungorum link. There is no synonym, basionym or shared vernacular, and the author sets are disjoint.
  - **X2, possible homonym.** These may be two different names; the later one would be illegitimate. Not associable without Index Fungorum.

## 4. Missing evidence (not fetched)

- **Index Fungorum name records.** Fields needed: authors, year, basionym record, current-name record, nomenclatural comment and legitimacy.
  - COL already holds the Index Fungorum `RecordID` for 2,039 COL concepts, so a lookup by id is possible for them.
  - The remaining 766 COL concepts need a lookup by name; 229 of those are in R.
  - Most valuable for: B3 (460), whose NorTaxa side would become verified; X1's different basionym author (139); X2's possible homonyms (23); and the R pairs (133 with an Index Fungorum id).
- **Authors of Fungal Names** (the Index Fungorum author list). It would turn the B6 candidates (797) and the 54 4-letter discovery hits into evidence or rejections.
- **A NorTaxa basionym link.** NorTaxa publishes no `originalNameUsageID`. Artsdatabanken would have to supply it; it is not in DwC-A 1.284.
- **Wikidata's Artsdatabanken taxon-id statements.** COL gives a Wikidata id for 2,369 of the pairs' COL concepts; these statements would give true identifier cross-references, which B1 lacks today.
- **Provenance of NorTaxa-side iNaturalist ids.** They are `legacy_compat`, so the 592 agreements stay inferred.

## 5. Caveats

- Every bucket is a candidate class, not an approval. NorTaxa's side of B3 and of any authorship relation is inferred.
- Homotypy "by basionym author = accepted author" (47 pairs) is inferred from ICN authorship notation.
- The X1 class or phylum disagreements (176) include higher-classification revisions, not only different taxa.
- B2 and B3 apply COL's synonymy view. A shared synonym does not prove identical circumscription.
- Vernacular names are never sole evidence. Only 42 pairs have an independent shared Norwegian name.
- The pass reads `cross-source-pairs.all.jsonl.gz` and re-uses the rev-4 one-to-one, iNaturalist and Dyntaxa conflict fields from it.

## Files and reproduction

- **`data/`:**
  - `field_inventory.csv` and `field_coverage_raw.json`;
  - `pairs_relationship.jsonl`: all 2,805 pairs, each with both source ids, both Sporely ids, its evidence (source file and row id, `source_verified`), contradictions and flags;
  - `evidence_gained.jsonl`: 1,647 pairs;
  - `promoted_pairs.csv`: B1 + B2;
  - `summary.json`.
- **`scripts/`:** `extract_relations.py`, `relate.py`, `field_inventory.py`.

```
PY=<sporely-py>/.venv/bin/python; S=<sporely-py>/database/taxonomy/sources; W=<scratch>
$PY -B scripts/extract_relations.py --workdb $W/workdb.sqlite3 --col $S/col_xr/2026-07-17-XR/archive.zip \
  --nortaxa $S/nortaxa/1.284/archive.zip --dyntaxa $S/dyntaxa/2026-09-30/archive.zip \
  --out $W/rel/relations.sqlite3 --coverage data/field_coverage_raw.json
$PY -B scripts/relate.py --csv ../data/species-not-merged.csv --pairs ../manifests/cross-source-pairs.all.jsonl.gz \
  --workdb $W/workdb.sqlite3 --relations $W/rel/relations.sqlite3 --release $W/release.sqlite3 \
  --manual-mappings $W/evidence/inputs/manual_mappings.json --out-dir data
$PY -B scripts/field_inventory.py --coverage data/field_coverage_raw.json --summary data/summary.json --out data/field_inventory.csv
```

## Pass 2 (correction after `review-codex.md`)

[v2/README.md](v2/README.md) supersedes this pass's buckets. Pass 1's data
files above are kept unchanged.

Pass 2 changes the rules:
- it tracks provenance, treating COL source 2030 as copied from NorTaxa;
- it requires documented homotypy;
- it excludes qualified, ambiguous and unacceptable witnesses;
- it compares author identities properly;
- it renames "explicit cross-reference" to "synonym-listing match" (identifier cross-references: 0).

Results: **verified 158**, pragmatic 1,671 and unresolved 976. Of pass 1's
566 promoted pairs, 158 are verified, 329 pragmatic, 65 sent to review and 14
in conflict.

## Pass 3 (correction after `v2/review-codex.md`, plus ChecklistBank provenance)

[v3/README.md](v3/README.md) supersedes pass 2's groups. Pass 1 and pass 2
files are kept unchanged.

Pass 3 changes the following:
- it corrects the author comparator: initials, particles, `in`, `f.`, roles moving between citation parts, and fail-closed handling of missing or unparsed authorship;
- it checks homotypic original-author consistency;
- it limits verified's witness-author exception;
- it splits pragmatic into compatible, missing authorship and contradictory;
- it adds the `checklistbank_source_record` evidence class, which records origin and is never corroboration;
- it assesses COL id churn;
- it regenerates the manifests in validator format.

**Results** (after the 2026-10-22 correction): verified **147**, a strict
subset of pass 2's 158. Pragmatic splits into compatible **1,169** and missing
authorship **234**. Unresolved **1,255**.
