# Relationship pass 3 (2026-10-21; corrected 2026-10-22)

The figures below include the narrow correction made after the Codex review of
pass 3 (`review-codex.md`). See section 0 and `data/changes_from_pass3.csv`.

Pass 3 corrects pass 2 after its Codex review (`../v2/review-codex.md`,
findings 1–6). It also adds the closed GBIF/ChecklistBank research
(`../../../2026-10-21-gbif-nortaxa-col/`) as a named evidence class. It covers
the same 2,805 species pairs and uses the same pinned inputs.

This pass is read-only and approves nothing. It does not merge identities,
compile a release or change the S1 package or plan. Pass 1 and pass 2 files are
unchanged. Every CSV is UTF-8 with a BOM.

## Headline

| Group | Pairs | Sub-groups |
|---|---|---|
| **verified** (smallest defensible) | **147** | COL `basionymID` + `NameRelation` 129; with Dyntaxa `homotypicSynonym` as well 17; Dyntaxa only 1 |
| **pragmatic_compatible** | **1,169** | identical 185; ICN-compatible 87 (an omitted sanctioning citation 86, an omitted `in` host 1); author-standard candidate 895; held new entrants 2 |
| **pragmatic_missing_authorship** | **234** | both sides blank 202; one side blank 32 |
| **unresolved** | **1,255** | ambiguity 498; contradictory authorship 286; conflict 247; placement review 164; accepted authors differ 60 |

- **Verified is a strict subset of pass 2's 158.** 85 of the 147 differ in order or family placement, so the placement decision still applies to them.
- **Verified authorship exceptions:**
  - 7 verified pairs rely on the limited witness exception: a surname abbreviation in the combining author.
  - 49 verified pairs have accepted authors that are an abbreviation-type candidate, such as initials on one side only or `et al.`.
  - 1 verified pair rests on the misspelling rule: Entoloma sphagneti, "Navaeu".
- **No independent identifier cross-reference exists.** Count: 0.

## 0. Correction after the pass-3 review (2026-10-22)

The correction is narrow. The classifier was not rewritten.

**Parser fixes (`authors_v3.py`; 111 tests, including 28 new negatives).**
- **Role moves are now checked even when the preliminary result is identical or compatible.**
  - `Pike in Hale` vs `Pike: Hale` and `(A in B) C` vs `(A: B) C` are now `role_mismatch`. Before, they came out compatible.
  - The same check now runs in `basionym_cmp`.
  - An `unparsed` person comparison no longer counts as "the same person" when detecting role moves.
- **Unicode letters are kept.**
  - `fold()` lower-cases first, then applies the documented letter map (ð, ø, æ, ß, þ, ł, capitals included). Every other letter is kept.
  - `Łind.` ≠ `Ind.` and `Þor` ≠ `Or`.
  - `Halıcı` ≠ `Halici`: 173281/75SJH stays `candidate:et_al`.
  - 172709/3ST32 (`Łubek`) stays `different`.
- **`et al.` must be at the end of the list.** Anywhere else (`Smith et al. Jones`) the result is `unparsed`, even when both strings are equal.
- **Multiword surname tokens get their own rule.** Examples: `Smith John`, `Willi Krieg.`, `San Martín`, `Müll. Arg.`, `Parg.-Leduc`. Such a person compares as follows:
  - `identical` only on an exact person key (folded tokens without spaces or full stops, so `Müll. Arg.` = `Müll.Arg`);
  - `different` when no word of one person equals, or is a prefix of, a word of the other (`Müll. Arg.` / `Hale`);
  - otherwise `unparsed`;
  - never candidate or compatible.
  - An `unparsed` person can no longer be turned into an `et al.` candidate.

**Effect: 27 pairs change subclass, all to more conservative subclasses** (`data/changes_from_pass3.csv`).

| From | To | Pairs |
|---|---|---|
| unresolved: contradictory authorship | unresolved: ambiguity | 9 |
| unresolved: accepted authors differ | unresolved: ambiguity | 8 |
| unresolved: conflict | unresolved: ambiguity | 6 |
| pragmatic_compatible: author-standard candidate | unresolved: ambiguity | 3 |
| unresolved: conflict | unresolved: placement review | 1 |

- **The 3 pragmatic pairs that left:** Biscogniauxia fuscella 82829/LWJW (`San Martin` / `F. San Martín`), Daldinia lloydii 82858/33ZDZ and Pronectria pilosa 299895/4MT2G (`López Silanes`).
- **Other examples:**
  - Xanthoparmelia amplexula 72997/5C6JQ: `Jen Johnston` / `J. Johnst.`;
  - Urocystis violae 123288/7DT7D: `E. Fisch.` / `A.A. Fisch. Waldh.`;
  - Antealophiotrema brunneosporum 173930/679RB: `Yin. Zhang` / `Y. Zhang bis`, which was a conflict.
- **Verified is unchanged.** The same 147 pairs remain, and the `rel3-verified` `members_sha256` is unchanged. The `members_sha256` of the identical, ICN, held and missing-authorship manifests is also unchanged. Only the author-standard manifest changes, 898 → 895. Every `file_sha256` changes, because the pinned script hashes change.

**ChecklistBank labels now carry the COL status.**
- Exact copies among the pairs: 240 accepted and 11 provisionally accepted, 251 in total. All 11 provisionally accepted pairs are unresolved.
- Population: 309 accepted and 20 provisionally accepted, 329 in total.

**The churn forecast is withdrawn.** See section 6.

**ChecklistBank requests (owner-authorised, cap 25): 6 used.** They are cached
as `data/clb_cache/*.json.gz` and fetched with `scripts/clb_fetch.py`.

| # | Request | Result |
|---|---|---|
| 1 | `/dataset/315834/nameusage/search?SECTOR_DATASET_KEY=2030&TAXON_ID=F&limit=3` | 200. **Total 12,102**, exactly the local count of fungal source-2030 usages. Each usage carries `verbatimSourceKey` (e.g. TZ76P → 336906909). |
| 2 | `/dataset/315834/nameusage/VKR4C` | 404. *Clavaria spathula* Dicks. (2030:52406, the 26.6 id) is absent from 26.7. |
| 3–5 | `/dataset/315834/verbatimsource?…` (with `sourceDatasetKey=2030&sectorKey=2231&sourceEntity=NAME_USAGE`; with `sectorKey=2231&limit=5`; with `sourceDatasetKey=2030`) | **HTTP 500 every time.** The bulk listing is not usable. |
| 6 | `/dataset/315834/verbatimsource/336906909` | 200. `sourceId` 295832 = NorTaxa *Crassistomataceae*, the same record as COL TZ76P. The usage → `verbatimSourceKey` → NorTaxa `taxonID` join works **per record**. |

**What the requests establish.**
- A complete offline join is not available, by export or by bulk API.
- Paging the search (13 requests of 1,000) would yield the 12,102 `verbatimSourceKey` values, but resolving them still takes one request per record: about 11,470 requests beyond the 629 already cached.
- So no further requests were made, and no classification uses these responses.


**Author comparator.** The comparator is `scripts/authors_v3.py`, tested by 111
cases in `scripts/test_authors_v3.py`.

- **Initials are kept (finding 3).**
  - `D.` is an initial, never a particle: `D. Hawksw.` ≠ `J. Hawksw.`, and `D. Hawksw.` vs `Hawksw.` is now only a candidate.
  - Particles are kept: `de Vries` ≠ `van Vries`. A particle on one side only, as in `des Abb.` vs `Abbayes`, is a candidate.
  - Effect: 12 pass-2 "identical" pairs, including Mycomicrothelia inaequalis 79691/44WWF, are now `initials_one_side` candidates. Pyrgillus javanicus (`Van den Bosch`) is now `particle_one_side`.
- **Roles are kept apart, including across citation parts (finding 2).**
  - A person who holds a sanctioning, pre-`ex` or `in`-host role on one side and a different role on the other is a `role_mismatch`. Examples: `(Pers.: Fr.) Fr.` vs `Pers. ex Fr.`, and `(Bull.: Fr.) Lam.` vs `Bull. ex Lam.`
  - **13 pass-2 pragmatic pairs** move to review for this reason:
    - Gyromitra esculenta 79004/3HZQ4;
    - Amanita verna 52179/5TYR8;
    - Cheilymenia rubra, Didymella rehmii, Fenestella betulae, Flammulaster ferrugineus;
    - Hygrophorus atramentosus, Lahmia kunzei, Peccania arizonica, Pezicula carpinea;
    - Phlyctis ludoviciensis, Polycoccum squamarioides, Septoria malagutii.
- **`in` and `f.` are parsed properly (finding 4).**
  - `in`: `A in B` vs `A` is ICN-compatible, because the host is omitted. `A in B` vs `B` is a role mismatch. So Hypogymnia rugosa 71787/PLQNZ, `Pike in Hale` / `Pike`, is now ICN-compatible.
  - `f.` (filius) is a flag on the person: `Hook. f.` ≠ `Hook.`
- **What is tested and guaranteed** (not a blanket "fail closed"):
  - An empty authorship returns `missing`.
  - These tested constructs return `unparsed`:
    - qualifiers: `sensu`, `auct.`, `ss.`, `s. str.`, `s. lat.`, `lato`, `stricto`, `non`, `nec`, `emend.`, `pro parte`, `nom.`, `comb.`, `inval.`, `illeg.`, `nud.`, `ined.`, `prov.`, `sp.`, `nov.`, `stat.`, `orth.`, `var.`, `cf.`, `aff.`, `fide`;
    - digits other than a strict trailing year;
    - `?`, brackets and other symbols;
    - unbalanced, nested, repeated or non-leading parentheses;
    - empty people or list members;
    - repeated `:`, `ex` or `in`;
    - an initial after a surname word;
    - a misplaced `et al.`;
    - a multiword person whose exact key differs, unless it is `different` as defined in section 0.
  - Neither `missing` nor `unparsed` is ever evidence. An unparsed accepted or basionym authorship is an ambiguity: 166 pairs carry this flag, 22 of them pragmatic in pass 2.
  - Typography only is tidied first: whitespace, doubled full stops, a trailing comma, a serial comma before `&`, and a strict trailing year (pass 1 dropped it the same way).
  - **Not guaranteed:**
    - other malformed citations that happen to fit the grammar (for example, a missing `&` between two unabbreviated surnames is read as one multiword person);
    - author identity beyond surname, initials, particles and filius (no author standard is used);
    - the correctness of the source strings themselves.

**Homotypic consistency (finding 1).** Every documented homotypic witness must
share the author of the original combination:
- with NorTaxa's accepted name;
- with COL's accepted name;
- and COL's accepted name must share it with its own `basionymID` record.

A `different` or `role_mismatch` result is a contradiction. The pair then goes
to review, but only when the witness is otherwise acceptable. For witnesses that
are already excluded, the checks are recorded only.
- Contradictions found: 15 pairs. Four of them were verified in pass 2:
  - **Sebacina grisea** 57952/4W79Z: `(Pers.)` vs witness *Exidiopsis grisea* (Bres.: Fr.);
  - **Cryptodiaporthe populea** 129389/ZX7T: `(Sacc.)` vs basionym *Dothichiza populea* Sacc. & Briard;
  - Cladonia ecmocyna 69108/5YW5W;
  - Xanthoparmelia versicolor 73319/5C723.
- None of these proves different taxa.
- Exception: a NorTaxa accepted original author that is within 2 edits of NorTaxa's own homotypic synonym's author, which in turn agrees with COL, is a misspelling, not a contradiction. Only Entoloma sphagneti uses this.

**Verified's author exception is limited (finding 5).**
- **Witness authorship.** The witness must be identical or ICN-compatible. The only exception is a surname abbreviation in the combining author, and only when both witnesses cite a combining author and equal basionym parts.
  - `Hepp` vs `Nägeli ex Hepp` (pre-`ex` omitted), initials on one side only, and a particle on one side only no longer qualify.
  - This removes Caloplaca cerina 81483/PZSY (witness 97742/4JDVH), Amundsenia approximata, Pyrgillus javanicus and Pyrenula anomala from verified. They move to pragmatic_compatible.
- **Accepted authorship.** For verified, accepted authors may differ only as follows:
  - by the ICN-compatible omissions;
  - or by `surname_abbreviation`, `initials_one_side`, `initials_prefix`, `particle_one_side`, `et_al` or `pre_ex_omitted`;
  - but never by a basionym or combination part cited on one side only.

**Precision (finding 6).**
- Vernacular provenance:
  - Copies of NorTaxa: 642 occurrences across 481 pairs.
  - Blank COL source: 27 occurrences.
  - Independently sourced: 15 pairs (COL sources 2041 Dyntaxa, 2011 WoRMS and 53131 IUCN).
- Dasyscyphella nivea 67749/349ZZ is flagged `placement_differs:family` only. Both orders are Helotiales.

## 2. Why 11 pairs left verified (158 → 147)

| Cause | Pairs | Pairs (NorTaxa / COL) | Now |
|---|---|---|---|
| Homotypic original-author contradiction | 4 | Sebacina grisea 57952/4W79Z, Cryptodiaporthe populea 129389/ZX7T, Cladonia ecmocyna 69108/5YW5W, Xanthoparmelia versicolor 73319/5C723 | unresolved (ambiguity) |
| Witness exception limited: initials, particle or pre-`ex` | 4 | Caloplaca cerina 81483/PZSY, Amundsenia approximata 187252/D8G2, Pyrgillus javanicus 80822/4QSV2, Pyrenula anomala 80740/4QS7B | pragmatic_compatible |
| Accepted citation omits the basionym or combining part on one side | 3 | Physcidia wrightii 74859/4HGQ6 (`Tuck.` vs `(Tuck.) Tuck.`), Cladonia halei 69141/VPG9 (`(Ahti)`), Porpidia circumnigrata 74181/B22YS (`(Vezda)`) | unresolved (accepted authors differ) |

**Two held entrants.**
- **Which pairs:** Paraxylaria xylostei 227405/B2N4Z and Pseudocyphellaria intricata 78297/4NVJN meet the pass-3 verified rules.
- **Why they meet them now:** comparator corrections. A trailing year is now tidied, and `Del.` is no longer read as a particle.
- **Why they are held:** they were not verified in pass 2 and have not been reviewed.
- **Where they are:** `pragmatic_compatible`, sub-group `verified_rules_new_entrant_held`.

## 3. Transition from pass 2 (`data/transition_from_pass2.csv`, per pair in `transition_pairs_from_pass2.csv`)

| Pass 2 | verified | prag. compatible | prag. missing auth. | unres.: contradictory | unres.: ambiguity | unres.: conflict | unres.: placement | unres.: authors differ | Total |
|---|---|---|---|---|---|---|---|---|---|
| verified | 147 | 4 | 0 | 0 | 4 | 0 | 0 | 3 | 158 |
| pragmatic | 0 | 1,118 | 234 | 281 | 38 | 0 | 0 | 0 | 1,671 |
| unresolved | 0 | 47 | 0 | 5 | 456 | 247 | 164 | 57 | 976 |

**Main movements:**
- **Pragmatic → contradictory (281 pairs).**
  - The combining author differs: 171 pairs. Pass 2 called this "attribution differs"; 50 of them have documented homotypy, and Cladonia pocillum is an example.
  - A basionym or combining part is cited on one side only: 109 pairs, such as `(Fr.) Fr.` vs `Fr.`
  - The pre-`ex` authors differ: 1 pair.
- **Pragmatic → missing authorship (234 pairs).** Lecidea lanuginosa 70713/DSLY3 is blank on both sides.
- **Unresolved → pragmatic_compatible (47 pairs).** These were pass-2 "different" results caused only by a trailing year (`Filson` vs `Filson, 1966`) or by initials (`Lamb` vs `I.M. Lamb, 1954`). 42 were pass-2 ambiguities, mostly `possible_homonym`. They now sit in the author-standard or identical tier.

## 4. Evidence class `checklistbank_source_record`

The source is the cached ChecklistBank `VerbatimSource` responses on the pinned
COL26.7 XR (`/dataset/315834/nameusage/{id}/source`). They are extracted to
`data/checklistbank_source_records.csv`: 846 records on 26.7 and 245 on 26.6. All
605 rows of `population_provenance.csv` match them. No new requests were made per
usage.

**What the evidence is.**
- It is traceable origin evidence: `provenance_class=copied_source_provenance`, `verified_eligible=false` and `independent_corroboration=false`, always.
- It can label a pragmatic pair. It can never make a pair verified, and `test_copied_source_provenance_never_alone_verifies` asserts this.
- A COL usage copied from this NorTaxa record that COL treats as a different concept is a conflict.

| Sub-class (pairs; 2,805 total) | Basis | Pairs |
|---|---|---|
| `exact_copy:col_accepted_usage_is_this_nortaxa_record` | source record id | 240 |
| `exact_copy:col_provisionally_accepted_usage_is_this_nortaxa_record` | source record id | 11 (e.g. 78771/RKVKH, 65684/DSTSD; all unresolved) |
| `exact_copy:other_accepted_col_concept` (conflict) | source record id | 1 (Alectoria asiatica 71450/V7T37, whose copy is L8R6K) |
| `exact_copy:col_synonym_of_this_concept` / `…_of_other_concept` | source record id | 0 / 0 |
| `name_match:col_usage_from_other_source` (queried) | name | 63 |
| `name_match:…(local_sourceID_only)` (not queried; the archive `sourceID` is not 2030) | name | 2,491 |

**Coverage.** All 251 pair concepts whose COL source is 2030 are in the cache.

**Where the 251 exact-copy pairs fall:**

| Group | Pairs |
|---|---|
| Missing authorship, both sides blank | 201 |
| Missing authorship, one side blank | 2 |
| Placement review | 35 (provisionally accepted: 3) |
| Ambiguity | 12 (provisionally accepted: 8) |
| Identical | 1 |

So the missing-authorship group is almost entirely one NorTaxa record imported twice: once directly and once through COL.

**Population** (`data/checklistbank_source_record_population.csv`, 605 rows):
- 329 exact copies at the same rank: 309 `exact_copy:col_accepted` and 20 `exact_copy:col_provisionally_accepted`. 251 of the 329 are among the pairs; 78 are outside the 2,805.
- 267 `exact_copy:col_synonym`. COL made NorTaxa's accepted record a synonym of another accepted usage, from Index Fungorum (2073) in 207 cases, ITIS 22, Dyntaxa 9 and others. None of these is among the 2,805 pairs.
- 9 `name_match:copy_of_other_record`.

## 5. Bulk export with source-record ids? **No.**

| What was checked | Finding |
|---|---|
| Pinned archive, which is the `extended=true` ColDP export (1.4 GB; `request.json`) | `NameUsage.tsv` has 73 columns: `sourceID` (the dataset key) and `clb:merged`, but no source-record id. The rows for QHK7J, PMLHM, MD47W, RBVV2 and LXP7S (source 2030) carry no NorTaxa id; 3NB6F only has its Index Fungorum link. |
| Exports listed for 315834 (`/export?datasetKey=315834`, 15 jobs) | ColDP (129 MB to 1.5 GB), DwC-A (143 MB; 823 MB extended), text tree, newick, dot. There is no other option (`ExportRequest`: format, extended, synonyms, bareNames, …). |
| CLB exporter source, `DwcaExtendedExport.java` | Taxon `datasetID` = sector source dataset key; `scientificNameID` = the release's name id. No `sourceId`. |
| API (`openapi.json`) | `VerbatimSource` (`id`, `sourceId`, `sourceDatasetKey`, `sectorKey`, …) has no usage id. It joins to a usage only through the usage JSON's `verbatimSourceKey`, which no export carries. Verified: `/dataset/315834/nameusage/QHK7J` gives `verbatimSourceKey` 336896097, the same as the `/source` id for QHK7J (→ 2030:296793). |

**Nothing was downloaded.**

**Remaining routes (cost and benefit only; not run):**
- **(a) Bulk listing plus paged search.**
  - `GET /dataset/315834/verbatimsource?sourceDatasetKey=2030&sourceEntity=NAME_USAGE` takes 1 request. It is unpaged per the spec and would return about 22,015 records.
  - It needs the usage → `verbatimSourceKey` map from a paged name-usage search over sector 2231: about 23 requests of 1,000 each.
  - In total about 25 requests and minutes of time.
  - **Tested 2026-10-22 (section 0).** The search does carry `verbatimSourceKey`, but the `verbatimsource` listing returns HTTP 500 in all three forms tried. Route (a) is not usable.
- **(b) The per-usage `/source` endpoint for all 12,102 fungal source-2030 usages.**
  - Cost: about 1.5–3.5 h at a 0.4–1 s spacing; latencies of up to 96 s were observed.
  - Gain: exact-copy links for every source-2030 anchor (5,024) and synonym (7,078), against today's 629.
- Both routes need the owner's approval. Route (a) is far cheaper; check one search page first.

**Network use in pass 3:** 13 requests. 12 are cached as JSON in `<scratch>/pass3/http/` and 1 raw file is in `http/raw/`:
- the OpenAPI spec (2);
- usage QHK7J;
- sector 2231;
- the source-2030 metrics for 26.7 and 26.6;
- the export list;
- the GitHub tree and the exporter sources (5, two of them 404);
- one full fetch of `DwcaExtendedExport.java`.

## 6. COL id churn and Sporely identity (`data/id_churn.csv`, `data/id_churn_summary.json`)

These are **observed counts on a selected sample**: the ids the GBIF research
happened to query. They mix accepted names and synonyms, and they cover several
event types. **No rate, interval or forecast is claimed.** The earlier
projection (693 anchors, 276–1,535) and the Wilson-interval framing are
**withdrawn**.

- **Backward: ids in the pinned 26.7, looked up in 26.6.**
  - **Involving NorTaxa in either release:** 6 of 31 are not continuous.
    - 4 are absent from 26.6: LXP7S, RBVV2, MKBXV and PQQ7F (PQQ7F is a synonym).
    - 2 were re-sourced away from 2030. Their COL ids survived: V2MTQ (now Dyntaxa) and PPY6B.
  - **Never NorTaxa:** all 208 are continuous.
- **Forward: ids in 26.6, looked up in the pinned 26.7.**
  - NorTaxa in 26.6: 4 of 31 absent. These are VKW8F, VKXJ3, VKPDV and VKR4C; VKR4C is absent per the local pinned archive and the 404 in request 2.
  - Other sources: 2 of 210 absent (VJLMB and VJP5T).
- **Re-identified records:** 3 of 3 kept their `(sourceDatasetKey, sourceId)`:
  - VKW8F→LXP7S (2030:75310);
  - VKXJ3→RBVV2 (2030:82116);
  - VJP5T→V2MTQ (Dyntaxa 6332087).
- **Vanished:** VKR4C (*Clavaria spathula* Dicks., 2030:52406) and VKPDV.

**Release metadata (cached).** NorTaxa's contribution to COL XR fell between 26.6 and 26.7:
- names: 36,896 → 22,015;
- taxa: 12,842 → 7,564;
- vernaculars: 8,838 → 1,561.

The source-2030 sector is volatile.

**Sporely exposure (work DB):**
- 5,024 of 620,976 COL-anchored Sporely concepts (0.8%) are anchored on a source-2030 `col_usage_id`.
- 5,884 NorTaxa usages are bridged onto those anchors.
- 0 observations in the audit snapshot reference them.
- **Continuity risk (documented, not quantified).** When a source-2030 usage is re-identified, `same_col_usage_id` (`mapping_policy.yml`) cannot fire. The concept falls to `source_record_disappears` plus a new concept, which is a silent duplicate, unless lineage is supplied.

**Recommendation (no policy edit made):**
1. **Record lineage at pin time.** For each pinned COL usage, record `(sourceDatasetKey, sourceId, verbatimSourceKey)`. Add a continuity event `same_source_record_new_col_usage_id` → keep the Sporely id, with review.
   - The bulk route (a) fails with HTTP 500 (section 0). Today this needs per-record requests (about 12,100 for fungal source-2030), or COL/ChecklistBank adding the source record to an export.
2. **Anchor NorTaxa copies on NorTaxa.** For source-2030 copies the source record *is* a NorTaxa `taxonID`, which Sporely already anchors (`nortaxa_taxon_id`). Anchor or bridge such concepts on the NorTaxa id, not on the COL copy's id, and treat the COL copy as an alias. This also resolves most of the 234 missing-authorship pairs as one record imported twice.
3. **Use the names-index id only as a name key, never as identity.** It is a fallback for discovery only.
4. **Before the next XR refresh, diff the source-record keys.** Compare the pinned and the new release for every Sporely anchor on a source-2030 or merged usage before applying the continuity rules.

## 7. Manifests and validator (`manifests/`, `scripts/test_manifests_validator_v3.py`)

| Partition | Members |
|---|---|
| `rel3-verified` | 147 |
| `rel3-pragmatic-compatible-identical` | 185 |
| `rel3-pragmatic-compatible-icn` | 87 |
| `rel3-pragmatic-compatible-author-standard` | 895 |
| `rel3-pragmatic-compatible-held-new-entrants` | 2 |
| `rel3-pragmatic-missing-authorship` | 234 |

**Format.** Same validator format as S1 (`batch_by_file_sha256`, `needs_review`).
`index.json` carries the pins: the release, the archives, the policies, and
`authors_v3.py`, `relate_v3.py` and the ChecklistBank CSV by sha256.

**Harness.** Modelled on `../../scripts/test_manifests_validator.py`, with the
unmodified `bridge_emission.verify_supersession_manifest_approvals` and synthetic
"TEST — NOT AN APPROVAL" ledgers. **All 13 tests pass** (rerun after the correction,
together with the 111 comparator tests and the 18 S1 harness tests: 142 passed):
- each of the 6 manifests is accepted;
- the partitions are disjoint and one-to-one, by both Sporely ids;
- no member is in S1 A0;
- copied-source evidence never verifies;
- a tampered manifest is refused, as is a missing member record;
- the pins match their files;
- **the 18 not-one-to-one pairs are refused** when inserted honestly. All 18 are in unresolved.

**Excluded from candidate manifests:** 0 (`data/manifest_exclusions.csv`).

**Overlap with S1.** The members also appear in the S1 package's A1, A2 and B
candidate partitions (column `s1_package_partition`). Never approve both
groupings for the same member.

## 8. Caveats

- Pragmatic is not an approval. The author-standard tier (895) needs an author standard, the Authors of Fungal Names. The missing-authorship tier rests on name and rank only, plus copy provenance where it exists.
- The comparator is still a heuristic grammar. Section 1 lists what is tested and what is not guaranteed. Written-out given names and compound surnames (`Willi Krieg.`, `San Martín`) are `identical` only on an exact person key; otherwise they are `unparsed`, or `different` when no word is shared.
- Contradictions are flags for review, not proof of different taxa. Placement differences are not identity conflicts.
- The churn figures are observed counts on ids that the GBIF sample touched. They are not a random sample, and no forecast is made.
- The source-metrics vernacular count (1,561) does not match the 6,032 local Norwegian vernacular rows from source 2030. CLB metrics may count only the primary sector; this was not resolved.

## Files and reproduction

- **`scripts/`:**
  - `authors_v3.py` and `test_authors_v3.py`;
  - `relate_v3.py`;
  - `clb_provenance.py` and `clb_population.py`;
  - `id_churn.py`;
  - `export_manifests_v3.py` and `test_manifests_validator_v3.py`;
  - `clb_fetch.py`, a capped and cached fetcher;
  - `compare_pass3.py`.
- **`data/`:**
  - `verified`, `pragmatic_compatible`, `pragmatic_missing_authorship` and `unresolved`, each as `.csv` and `.jsonl`. Each row has:
    - both source ids and both Sporely ids;
    - the evidence, with source file and row id, `provenance_class` and `source_verified` / `verified_eligible`;
    - `checklistbank_source_record`;
    - Norwegian vernaculars from NorTaxa and COL, with provenance.
  - `transition_from_pass2.csv` and `transition_pairs_from_pass2.csv`;
  - `checklistbank_source_records.csv` and `checklistbank_source_record_population.csv` (with `.summary.json`);
  - `id_churn.csv` and `id_churn_summary.json`;
  - `changes_from_pass3.csv`: the 27 pairs changed by the 2026-10-22 correction;
  - `clb_cache/`: 6 cached ChecklistBank responses.
  - `manifest_exclusions.csv` and `summary.json`.
- **`manifests/`:** `index.json` and 6 `*.manifest.json` files.

```
PY=<sporely-py>/.venv/bin/python; S=<scratch>; W=$S/rev5
$PY -B -m pytest -q -p no:cacheprovider --noconftest scripts/test_authors_v3.py
$PY -B scripts/clb_provenance.py --cache $S/gbif/cache --population ../../../2026-10-21-gbif-nortaxa-col/data/population_provenance.csv --out data/checklistbank_source_records.csv
$PY -B scripts/relate_v3.py --csv ../../data/species-not-merged.csv --pairs ../../manifests/cross-source-pairs.all.jsonl.gz \
  --pass2-dir ../v2/data --workdb $W/workdb.sqlite3 --relations $W/rel/relations.sqlite3 --release $W/release.sqlite3 \
  --manual-mappings $W/evidence/inputs/manual_mappings.json --clb data/checklistbank_source_records.csv --out-dir data
$PY -B scripts/clb_population.py --population ../../../2026-10-21-gbif-nortaxa-col/data/population_provenance.csv --workdb $W/workdb.sqlite3 --data data --out data/checklistbank_source_record_population.csv
$PY -B scripts/id_churn.py --clb data/checklistbank_source_records.csv --http $S/pass3/http --workdb $W/workdb.sqlite3 --out-csv data/id_churn.csv --out-json data/id_churn_summary.json
$PY -B scripts/export_manifests_v3.py --data data --out manifests --s1-index ../../manifests/index.json
$PY -B scripts/compare_pass3.py --old <copy of reviewed pass-3 data> --new data --out data/changes_from_pass3.csv
# from the worktree root:
PYTHONDONTWRITEBYTECODE=1 $PY -B -m pytest -q -p no:cacheprovider --noconftest <this dir>/scripts/test_manifests_validator_v3.py
```

A rerun of the generators into scratch reproduced every data file byte for byte.
