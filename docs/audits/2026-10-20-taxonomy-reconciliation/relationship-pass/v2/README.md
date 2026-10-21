# Relationship pass 2: focused correction (2026-10-21)

Pass 2 corrects pass 1 after the Codex review (`../review-codex.md`). It
covers the same 2,805 pairs and uses the same pinned inputs. Pass 1 and its
outputs are unchanged; pass 2 approves nothing. Every CSV is written as UTF-8
with a BOM.

## Rules changed

1. **Provenance.** Each COL row's `sourceID` is recorded. Source 2030 is
   NorTaxa (Artsnavnebasen) itself, so its rows are `copied_from_nortaxa`:
   they are kept as single-source relationships but are never independent.
   This applies to COL synonym and basionym rows, `NameRelation` rows and
   vernacular rows. A blank vernacular `sourceID` is
   `unverified_blank_source`.
2. **Verified homotypy needs a documented link.** That is a COL
   `basionymID`, a COL `NameRelation` row of type basionym, homotypic or
   based-on, or a Dyntaxa `homotypicSynonym` status (Dyntaxa is reached
   through COL's own `dyntaxa:` id). The link must come from a non-2030 COL
   row or from Dyntaxa. Matching basionym authors alone never qualify.
3. **Some witnesses never verify.** These are synonyms that are ambiguous or
   misapplied; those with COL name status unacceptable, doubtful, manuscript,
   not established or rejected; and those with a qualifier or unpublished
   marker. A witness also needs the same author identities. A witness that
   differs only by an abbreviation in the combining author is allowed, but only
   if both sides cite a combining author and the basionym authors agree.
4. **Author identities** (`scripts/authors_v2.py`, 25 tests in
   `scripts/test_authors_v2.py`):
   - initials matter;
   - sanctioning (`:`), validating (`ex`) and basionym roles are kept apart;
   - J.E. Lange ≠ M. Lange, and `Sowerby: Fr.` ≠ `Vahl: Fr.`;
   - `Vahl: Fr.` vs `Fr.`, `(Nees: Fr.)` vs `(Nees ex Fr.)`, and `(Sowerby: Fr.)` vs `(Fr.)` are `role_mismatch`, never equal.
5. **Renamed category.** "Explicit cross-reference" is now
   `synonym_listing_match`, which is a name lookup. An **identifier
   cross-reference**, meaning a shared identifier published by both sources,
   occurs **0** times. NorTaxa publishes no external identifiers, COL's
   source-2030 rows carry no NorTaxa taxonID, and no approved manual mapping
   matches.
6. **Vernaculars** are support only and carry their provenance. Only 15
   pairs share a Norwegian vernacular whose COL row comes from an independent
   source (Dyntaxa 2041, 2011 or 53131). Another 27 shared names have a blank
   source and 642 are copies of NorTaxa.

**Field coverage corrected** (`data/field_inventory.csv`):
- Index Fungorum `RecordID` links: 314,076 of 806,448 rows, 38.9%. The 93.6% figure counts any populated link.
- `combinationAuthorship`: 45.4%; `basionymAuthorship`: 17.0%.
- `nameAlternativeID`: 647 IRMNG and 601 WoRMS LSIDs.

## Results

| Output | Pairs | Breakdown |
|---|---|---|
| `verified_candidates` | **158** | 139 by COL `basionymID` + `NameRelation`; 18 with Dyntaxa `homotypicSynonym` as well; 1 by Dyntaxa only |
| `pragmatic_candidates` | **1,671** | authorship identical 181; compatible (an omitted sanctioning citation) 86; same basionym author, combining author attributed differently 146; documented homotypy, combining author attributed differently 51; authorship candidate needing an author standard (abbreviation, initials on one side only, `et al.`, ex-author) 1,207 |
| `unresolved_pairs` | **976** | conflict 263 (different basionym authors 216, iNaturalist id conflict 25, one source places the other's name elsewhere 26); placement review (class or phylum differ) 163; ambiguity 466 (reasons, which can overlap: possible homonym 264, author role mismatch 120, qualified accepted name 65, blocked status 39 + 1, provisional 13, not one-to-one 7); accepted authors differ 84 |

The 1,207 "authorship candidate" pairs are the weakest pragmatic tier. Treat
them separately: without an author list they are only plausible.

**Pass 1's 566 promoted pairs** (`data/transition_pairs_from_pass1_566.csv`,
one row per pair, with reasons):

| Pass 1 | Verified | Pragmatic | Review | Conflict |
|---|---|---|---|---|
| B1 "explicit" (89) | 2 | 55 | 31 | 1 |
| B2 shared homotypic (477) | 156 | 274 | 34 | 13 |
| **566** | **158** | **329** | **65** | **14** |

**Why pairs left the verified set:**
- the only witness was a source-2030 copy;
- an ambiguous, qualified or unacceptable witness;
- witness authors did not match, or the witness had no combining author;
- homotypy inferred from authors only;
- the combining author is attributed differently (pragmatic);
- an author-role mismatch or a possible homonym (review).

## Examples

- **Verified:**
  - **Entoloma sphagneti**, NorTaxa 53677 / 625274 ↔ COL 6FDPL / 96338. The NorTaxa synonym 59943 *Rhodophyllus sphagneti* (Naveau) Kühner & Romagn. matches COL SKHS5 (source 7888) through `basionymID` and `NameRelation`, and Dyntaxa lists it as a `homotypicSynonym`. NorTaxa's "Navaeu" is a misspelling, explained by its own synonym.
  - **Diaporthe carpini**, 66424 / 627673 ↔ 35CR6 / 2754. Through *Valsa carpini*: COL basionym and Dyntaxa homotypic.
  - **Dasyscyphella nivea**, 67749 ↔ 349ZZ. *Dasyscyphus niveus* (R. Hedw.: Fr.) Sacc., via COL `basionymID`. Order and family differ, so the placement decision still applies.
- **Pragmatic:**
  - **Lactarius trivialis**, 57654 / 626808 ↔ 3RSC5 / 22221. Its only homotypic witness is *L. fascinans* "ss. M.M. Moser" (source 2030, unacceptable), which is excluded. The accepted authors `(Fr. : Fr.) Fr.` and `(Fr.) Fr.` are compatible: the sanctioning citation is omitted.
  - **Cortinarius brunneus**, 52596 ↔ YK96. Its witnesses are either 2030 copies or lack a combining author.
  - **Bulbothrix suffixa**, 71552 ↔ MFLCD. Its only witness is a 2030 copy.
  - **Cladonia pocillum**, 69234 ↔ VPKC. Same basionym (Ach.), but the combining authors differ: Grognot vs O.J. Rich. The *kalkbeger* vernacular has a blank COL source, so it is unverified.
  - **Cladonia bellidiflora** and **Anamylopsora pulcherrima**: their witnesses are ambiguous or have unmatched authors.
- **Unresolved:**
  - **Clavaria spathula**, 52406 ↔ 5Z9BM: possible homonym (Dicks. vs O.F. Müll.).
  - **Cucurbitaria spartii** and **Hymenoscyphus calyculus**: author role mismatch (`Nees: Fr.` / `Nees ex Fr.`; `Sowerby: Fr.` / `Fr.`).
  - **Gyalideopsis graminicola**: class differs; it goes to placement review and is not taken as a different taxon.

## Caveats

- NorTaxa's "illegitimate" status on synonyms (very common) is recorded but does not exclude a witness.
- Dyntaxa's independence from NorTaxa is not proven. For example, Dyntaxa repeats "Navaeu".
- "Pragmatic" is not an approval. It inherits the placement and A2 structural questions.
- No external nomenclatural adjudication was made, and nothing proves equal circumscription.

## Files and reproduction

- **`scripts/`:** `authors_v2.py`, `test_authors_v2.py`, `relate_v2.py`, `field_inventory_v2.py`.
- **`data/`:**
  - `verified_candidates`, `pragmatic_candidates` and `unresolved_pairs`, each as `.csv` and `.jsonl`. Each row holds:
    - both source ids and both Sporely ids;
    - evidence with source file and row id;
    - provenance and the `source_verified` / `verified_eligible` flags;
    - the reasons a witness was excluded;
    - Norwegian vernaculars from NorTaxa and from COL, with provenance.
  - `transition_from_pass1_566.csv` and `transition_pairs_from_pass1_566.csv`;
  - `field_inventory.csv` and `summary.json`.

```
PY=<sporely-py>/.venv/bin/python; W=<scratch>
$PY -B -m pytest -q -p no:cacheprovider scripts/test_authors_v2.py
$PY -B scripts/relate_v2.py --csv ../../data/species-not-merged.csv --pairs ../../manifests/cross-source-pairs.all.jsonl.gz \
  --pass1 ../data/pairs_relationship.jsonl --workdb $W/workdb.sqlite3 --relations $W/rel/relations.sqlite3 \
  --release $W/release.sqlite3 --manual-mappings $W/evidence/inputs/manual_mappings.json --out-dir data
$PY -B scripts/field_inventory_v2.py --pass1 ../data/field_inventory.csv --relations $W/rel/relations.sqlite3 --out data/field_inventory.csv
```
