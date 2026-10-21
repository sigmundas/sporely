# GBIF as a NorTaxa → COL mapping source (research, 2026-10-21)

**Question:** can GBIF provide reliable identity mappings between NorTaxa and COL
for Sporely? This was a read-only study. It made no production queries and wrote
nothing to any database, and it changes no taxonomy. Public GBIF and
ChecklistBank APIs were called sequentially with a delay, and every response was
cached.

**Answer:** No. GBIF's NorTaxa-to-COL path is name matching at every hop. On the
sample it reproduces what Sporely's own name matching and the 2026-10-20 audit
already found, and it does so with weaker homonym handling, against a different
COL release. One adjacent source is new and worth using for **candidate
discovery only**: ChecklistBank's per-usage provenance for the pinned COL XR
(`VerbatimSource`). It ties 596 of Sporely's NorTaxa-only concepts to the COL XR
usage that COL copied from that same NorTaxa `taxonID`.

## 1. What Sporely does today

This section restates the existing state; nothing here is new. Sources:
`database/taxonomy/docs/identity-contract.md`, `policies/mapping_policy.yml`,
the installed `tax-2026.10.07-01` and the rev5 work DB.

- **Pinned sources:**
  - COL XR `2026-07-17 XR`: ChecklistBank dataset **315834** (alias COL26.7 XR), DOI 10.48580/dgykv.
  - NorTaxa **1.284** from the IPT `artsnavnebase` resource. Its `taxonID` is an Artsnavnebase scientific-name id.
- **How NorTaxa accepted usages bind (61,583 fungal NorTaxa usages):**

| Binding | Count | Emitted as a bridge? |
|---|---|---|
| `manual_approved_exact` | 3,384 | yes (`nortaxa_taxon_id` on the COL concept) |
| `reviewed_supersession` | 1,353 (+4,779 synonym bridges) | yes |
| `cross_source_automatic_exact_strict` / `_missing_authorship` | 14,291 / 2,133 | no. Name binding only; policy rejects it as identity |
| NorTaxa-only anchors | 12,566 | n/a: 5,874 same-name review pairs, 81 ambiguous, 5,895 whose name exists only as a COL synonym, 716 with no same-name COL usage |

- **Policy** (`mapping_policy.yml`): automatic identity needs either a continuous source id or an *authoritative explicit cross-reference*. Canonical-name equality alone is forbidden.
- **The 2026-10-20 audit** (relationship pass v2, 2,805 pairs) found 0 identifier cross-references. It reports 158 verified, 1,671 pragmatic and 976 unresolved pairs. It flagged COL rows from source 2030 as `copied_from_nortaxa`, which makes them non-independent.

## 2. How NorTaxa → GBIF → COL actually works

| Hop | Mechanism | Identifier or name? |
|---|---|---|
| NorTaxa IPT → GBIF checklist `a6c6cead-b5ce-4a4e-8cf5-1542ba708dec` | GBIF crawls the same IPT DwC-A. It is version **1.292** (pubDate 2026-09-11), not 1.284. `taxonID` is kept verbatim: `/v1/species?datasetKey=…&sourceId=294404` → key 238836234. | **identifier** (same NorTaxa `taxonID`). 182/182 sample ids were found. |
| GBIF checklist usage → `nubKey` | Checklist indexing matches each usage to the **legacy GBIF Backbone** (`d7dddbf4…`, frozen 2023). Dataset metrics: nubMatching 222,221 / 229,125. | **name**. In 2 of 178 sample cases the nub usage has a different spelling (*Pannaria mosenii* → *moseni*; *Phellodon brunneo-olivaceus* → *brunneoolivaceus*). It is not COL. |
| Occurrence → COL XR | Since June 2026, GBIF matches every occurrence to COL XR (`checklistKey=7ddf754f-d193-4cc9-b351-99906754a03b`) and to the legacy Backbone. The result goes in `occurrence.classifications[checklistKey]`. Matching-ws metadata gives the main index as **CLB 315557 = COL26.6 XR (2026-06-19)**, built 2026-07-18. | **name** (name, rank, classification). See the identifier row below. |
| Identifier matching in matching-ws (`/v2/species/match?taxonID=…`) | Identifier indexes exist only for WoRMS 2011, **Dyntaxa 2041**, ITIS 2144, Wikidata, UKSI 7888, GBIF 53147 and IPNI 2006. `taxonID=urn:lsid:dyntaxa.se:Taxon:4379` → 3NB6F. `taxonID=294404` or `NBIC:294404` → `TAXON_ID_NOT_FOUND`. Norwegian occurrences carry no `taxonID` anyway: Artsobservasjoner `b124e1e0…` occurrence 6419667337 has only `scientificName`, `scientificNameAuthorship` and `kingdom`. | NorTaxa: **none** |
| ChecklistBank NorTaxa (dataset **2030**, imported 2026-10-12, version 2026-09-11) | Usage ids are the NorTaxa `taxonID`s (`/dataset/2030/nameusage/294404`). Each name carries a `namesIndexId` (names index: canonical name plus normalized authorship). | identifier within NorTaxa. The `namesIndexId` is a **name** key. |
| COL XR usage → source record (`/dataset/315834/nameusage/{id}/source`) | `VerbatimSource`: `sourceDatasetKey` and `sourceId`. For usages that COL XR merged from NorTaxa (sector 2231), `sourceId` is the NorTaxa `taxonID`. Example: QHK7J ← 2030:296793 (*Mallocybe tomentella*). Usages from other sources point elsewhere, such as 3NB6F ← Index Fungorum 2073:356885. The COL `identifier` list (3NB6F has dyntaxa, inat, worms, wikidata) never carries NorTaxa. | **identifier provenance**, but only for COL usages that were copied from NorTaxa |
| ChecklistBank match against the pinned XR (`/dataset/315834/match/nameusage`) | Names-index match, typed `exact`, `variant`, `canonical`, `ambiguous` or `higherrank`. | **name** |

**Release difference.** GBIF serves COL26.6 XR (315557). Sporely pins COL26.7 XR
(315834). Of the 239 COL ids involved in the sample, 208 of 210 usages from
non-NorTaxa sources keep the same id and the same source record (2 are
re-sourced). Usages from NorTaxa are not stable:
- 4 of 29 pinned ids do not exist in 26.6.
  - *Bunodophoron whakapapaensi* (NorTaxa 75310) is **VKW8F** in 26.6 but **LXP7S** in 26.7.
  - *Parmentaria operta* (82116) is VKXJ3 in 26.6 but RBVV2 in 26.7.
- Some 26.6 usages copied from NorTaxa disappear or are re-sourced in 26.7:
  - VKR4C, *Clavaria spathula* Dicks. ← 2030:52406, is gone.
  - V2MTQ changed from NorTaxa 296816 (synonym) to Dyntaxa (accepted).

So a COL id that GBIF returns must be re-resolved against Sporely's pinned
release. For NorTaxa-derived usages, the stable key is
(`2030`, `sourceId`), not the XR id. This also matters for Sporely's own
`same_col_usage_id` continuity rule at the next XR refresh.

## 3. Experiment

**Sample.** 182 NorTaxa concepts, all fungi, with 87 lichen-proxy taxa (class
Lecanoromycetes, Arthoniomycetes or Lichinomycetes). The draw is
stratified with seed 20261021 (`scripts/build_sample.py`):

| Stratum | Size | Population |
|---|---|---|
| A: approved bridges (manual 20, supersession 20) | 40 | 4,737 |
| B: automatic name binds (strict 20, missing-authorship 10) | 30 | 16,424 |
| C: audit pairs (verified 15, pragmatic 20, unresolved 20) | 55 | 2,799 anchors |
| D: national-only | 25 | 6,610 |
| E: compiler-ambiguous | 10 | 81 |
| F: NorTaxa synonyms | 15 | 23,077 |
| X: worked examples | 7 | — |

**Queries per concept:**
- G1, GBIF by `sourceId`;
- G2, the `nubKey` usage;
- G3, matching-ws against COL XR with name, authorship, rank and full classification, `verbose=true`;
- C1, CLB 2030 usage;
- C2, CLB match against 315834;
- C3, provenance of every COL id involved.

**Population pass.** C3 for all **605** pinned COL usages with source 2030 that
share a canonical name with one of Sporely's NorTaxa-only anchors (604 anchors).

**GBIF matching-ws categories:**
- *ambiguous* means 2 or more COL usages with the same canonical name and rank among GBIF's candidates. GBIF still returns one of them, with EXACT and confidence 97–100.
- *name_author_differs* means GBIF EXACT or VARIANT while CLB says the authorship differs.

| GBIF category (n=182) | Same COL usage as Sporely | Different COL usage | Different id, absent from pinned 26.7 | Sporely has no COL identity | Total |
|---|---|---|---|---|---|
| name_exact | 85 | 0 | 0 | 2 | 87 |
| name_author_differs | 5 | 0 | 2 | 1 | 8 |
| synonym_match (COL synonym → accepted) | 6 | 3 | 1 | 20 | 30 |
| ambiguous (homonym or duplicate present) | 39 | 0 | 2 | 15 | 56 |
| missing (HIGHERRANK) | 0 | 1 (GBIF none) | 0 | 0 | 1 |
| **Total** | **135** | **4** | **5** | **38** | 182 |

**By Sporely reference:**

| Reference | Same | Different / none | Different id, absent from 26.7 |
|---|---|---|---|
| approved bridges | 39/40 | 0 | 1 |
| automatic binds | 28/30 | 0 | 2 |
| audit verified | 16/16 | 0 | 0 |
| audit pragmatic | 21/22 | 0 | 1 |
| audit unresolved | 23/24 | 1 | 0 |

"Same" here only means that two name matchers agree. For C (audit pairs),
GBIF's choice equals the audit's own name-derived candidate and adds no
concept evidence.

GBIF and CLB return the same target in 158 of 174 cases where CLB returns a
usage at the NorTaxa rank. CLB returns `higherrank` (no answer) in 8 cases where
GBIF still picks a species.

**Identifier provenance (C3).**

- **Sample:** for 23 concepts, a pinned COL usage is a copy of the same NorTaxa `taxonID`:
  - 16 of the 30 automatic binds (B): Sporely's name bind lands on COL's copy of the same record;
  - 3 C pairs, 1 E concept and 2 D concepts.
- **Population (all 604 NorTaxa-only anchors with a same-name usage from source 2030):** 596 copies carry the anchor's own `taxonID`. The other 9 COL usages point to a different NorTaxa record (sibling or orthographic variant).
  - **329** are accepted or provisionally accepted at the same rank. All 329 are already in the audit's 6,072 same-name pairs, flagged `col_record_from_nortaxa_source`. Audit v2 placed 204 as pragmatic, 47 as unresolved, 0 as verified and 78 outside v2 (A category).
  - **267** are COL **synonyms**. XR merged the NorTaxa accepted record and made it a synonym of a usage from another source. Examples:
    - *Uredo arctica* (123132) → *Puccinia arctica* 6WMP9 (Index Fungorum);
    - *Metasphaeria tritirulosa* → *tritorulosa*;
    - *Venturia petasitidis* → *Coleroa petasitidis* (2010).
- **Locally, without GBIF:** about 4,690 of Sporely's 16,424 automatic binds point at a COL usage with source 2030. Given B's 16/30, most of those are COL's copy of the same NorTaxa record.

## 4. Examples

**Agreement (name only, no new evidence):**
- *Hygrocybe conica*, NorTaxa 294404, Sporely 624178: GBIF → 3NB6F, EXACT 100; CLB `variant`, same `namesIndexId` 4429309. COL's record comes from Index Fungorum (2073:356885), not NorTaxa, so nothing beyond the name links them.
- *Entoloma sphagneti*, 53677: GBIF → 6FDPL, with "Navaeu" matched to "Naveau". This is the audit's verified pair, but GBIF supplies none of the basionym evidence.
- *Lactarius trivialis*, 57654: GBIF → 3RSC5.

**Identifier provenance (new evidence):**
- *Mallocybe tomentella*: COL QHK7J ← 2030:296793.
- *Alectoria asiatica* (71450, compiler-ambiguous): L8R6K ← 2030:71450. The twin V7T37 comes from source 312616.
- *Rehmia furfurosa* (298219, national-only): V4GRJ ← 2030:298219, a COL synonym of 4SC52.

**Ambiguous; GBIF silently picks one:**
- *Clavaria spathula* Dicks. (52406): GBIF → **5Z9BM *C. spathula* O.F. Müll.**, EXACT 99, with note `authorship=0`. In 26.6 the Dicks. usage VKR4C (copy of 52406) was an alternative. CLB on the pinned release returns `higherrank`. The audit's homonym concern stands.
- **Stereum durum**: Burt (172237) and "LLoyd" (172927) both go to **6ZR96 *S. durum* Lloyd**, EXACT. Burt is a different author, a possible homonym. CLB correctly gives `higherrank` for Burt and `variant` for LLoyd.
- *Cladonia pocillum* (Ach.) Grognot (69234): GBIF → VPKC (Ach.) O.J. Rich., EXACT. CLB gives `higherrank`.
- Compiler-ambiguous genera (E, 10 of 10): GBIF picks one twin each time. For example, *Epicoccum* Link → NQ2ZS (UKSI, provisional) rather than 62S9T (Index Fungorum). For *Kukwaea* GBIF and CLB disagree.

**Incorrect or unsafe:**
- *Buellia grisea* Dodge & Baker (73438): GBIF VARIANT → *Buellia grossa* (Pers. ex Nyl.) Jatta, a synonym → 3YTVH. That is a different epithet. The pinned release has MD4G9 *B. grisea*.
- *Rinodina hallii* Tuck. (73949): GBIF VARIANT → *Rinodina halei* H.Magn., a different name.
- *Polyporus fractipes* (172009, NorTaxa synonym): GBIF HIGHERRANK → order *Polyporales*, while CLB has the exact usage 786NY.
- NorTaxa synonyms placed differently from NorTaxa (2 of 15): *Pertusaria coudercii* → 76V6T, but Sporely's concept is 776RV. *Melampsora populina* → 3Z54C vs 3Z538.
- Ids absent from the pinned release (5): VKW8F, VKXJ3, VKPDV, VJP5T and VJLMB. GBIF returns these 26.6 ids where Sporely holds LXP7S, RBVV2, 34QLV, V2MTQ and Q3QM.

## 5. Does GBIF improve bridge coverage?

**Approved bridges** (4,737): GBIF agrees on 39 of 40 sampled; the remaining
one is a 26.6 id difference. It adds no evidence.

**NorTaxa-only anchors** (12,566):

- **5,874 same-name pairs:** GBIF returns the same candidate Sporely already has (53 of 55 agree in C). It adds no concept evidence and is less cautious on homonyms than the audit or CLB.
- **5,895 whose name is only a COL synonym:** GBIF reproduces COL's own synonymy (D: 18 of 25 synonym matches). Sporely holds that synonymy locally in the pinned XR, so this is not a gain.
- **716 with no same-name COL usage:** in the sample, GBIF offered 2 variant spellings and 1 wrong name. The gain is marginal and error-prone.
- **Identifier provenance (ChecklistBank, not GBIF):** this adds the per-record link "COL usage X is COL's copy of NorTaxa `taxonID` Y" for 596 anchors: 329 accepted and 267 as COL synonyms. These are not *new* pairs; all 329 accepted ones are in the audit's duplicate list. It does upgrade their evidence from "same name, COL row from source 2030" to an identifier link that COL itself published. For the 329, the two Sporely concepts are the same NorTaxa record imported twice, once directly and once through COL. For the 267, it gives COL's synonym target for review.

**Net reliable gain from GBIF:** 0. **Net gain from ChecklistBank provenance:**
identifier-level evidence for 329 existing duplicate pairs and 267 synonym
candidates. All of them still need review under `mapping_policy.yml`. A copy
proves record lineage. It does not prove that COL's placement or circumscription
matches, and the copies are release-specific.

## 6. Recommendation

**Do not integrate GBIF matching-ws, `nubKey` or GBIF checklist data as a
mapping source.** Every hop to COL is name-based. GBIF runs on a different XR
release (26.6) whose ids for usages from NorTaxa differ from Sporely's pins.
It resolves homonyms and ambiguities silently, and it adds no evidence beyond
Sporely's own local name matching and the audit.

**Use ChecklistBank `VerbatimSource` provenance on the pinned XR for candidate
discovery only.** That means `/dataset/315834/nameusage/{id}/source`, read for
usages whose `sourceID` is 2030. Concretely:

- feed the 329 accepted copy pairs into the existing review track as a named evidence class, such as `col_copy_of_nortaxa_record`; and
- feed the 267 synonym copies in as synonym candidates.

Both need review; neither is automatic identity. Before relying on it:

- check whether the pinned ColDP export can supply the same `sourceId` offline. The audit found it does not. The API needs about 12,100 calls for every fungal source-2030 usage.
- record the ChecklistBank dataset key with the evidence, because the provenance changes between XR releases.

## 7. Caveats

- **Sample size and coverage:** 182 concepts. The lichen class proxy is approximate. The population pass covers only anchors that share a name with a COL usage from source 2030.
- **Version mismatch:** GBIF's NorTaxa (1.292) and CLB 2030 (2026-09-11) are newer than 1.284. All 182 sample ids still resolved.
- **Ambiguity rule:** "ambiguous" uses GBIF's verbose alternatives within a rank. It is a flag, not proof of a homonym.
- **Pinned-release presence:** checked through `/source` 404s and the local fungal `col_raw`.
- **Network:** responses were slow at times (up to 96 s); one run was restarted after a timeout, and retries were added. No production or Supabase access was made.

## 8. Endpoints, keys and reproduction

**Datasets:**
- GBIF NorTaxa `a6c6cead-b5ce-4a4e-8cf5-1542ba708dec` (DOI 10.15468/4dd3tf, CLB 2030).
- GBIF COL `7ddf754f-d193-4cc9-b351-99906754a03b` (matching index CLB 315557, COL26.6 XR).
- Legacy Backbone `d7dddbf4-2cf0-4f39-9b2a-bb099caae36c`.
- Sporely's pinned XR is CLB 315834.

**Requests:** 2,034 cached GET requests (11 returned 404), plus 4 uncached requests for timing, dataset metrics and the matching-ws README. By endpoint:

| Endpoint | Requests |
|---|---|
| CLB `/source` on 315834 | 846 |
| CLB `/source` on 315557 | 245 |
| GBIF `/v2/species/match` | 188 |
| CLB 2030 nameusage | 187 |
| CLB 315834 match | 184 |
| GBIF `/v1/species?sourceId` | 182 |
| GBIF `/v1/species/{key}` | 178 |
| Registry and metadata | about 25 |

The index is in `data/response_index.jsonl`. Raw responses (16 MB) stay in the scratch cache (`GBIF_CACHE`).

**Files:**
- `scripts/`:
  - `fetch.py`: polite cached GET; `GBIF_OFFLINE=1` replays from the cache;
  - `build_sample.py`;
  - `run_queries.py`, modes `sample` and `population`;
  - `classify.py`;
  - `id_stability.py`.
- `data/` (CSV files are UTF-8 with BOM):
  - `sample.csv`;
  - `results.csv`: one row per concept, with all route outputs and categories;
  - `population_provenance.csv`: 605 rows;
  - `id_stability.csv`;
  - `summary.json`.

```
PY=<sporely-py>/.venv/bin/python; W=<scratch>/rev5; export GBIF_CACHE=<scratch>/gbif/cache
$PY -B scripts/build_sample.py --workdb $W/workdb.sqlite3 --audit ../2026-10-20-taxonomy-reconciliation --out data/sample.csv
$PY -B scripts/run_queries.py sample --sample data/sample.csv --out $GBIF_CACHE/../raw_sample.jsonl
$PY -B scripts/run_queries.py population --workdb $W/workdb.sqlite3 --out $GBIF_CACHE/../raw_population.jsonl
$PY -B scripts/classify.py --raw-sample $GBIF_CACHE/../raw_sample.jsonl --raw-population $GBIF_CACHE/../raw_population.jsonl --workdb $W/workdb.sqlite3 --out-dir data
$PY -B scripts/id_stability.py
```

**Documentation consulted:**
- GBIF techdocs "Taxonomy interpretation" (https://techdocs.gbif.org/en/data-processing/taxonomy-interpretation);
- the GBIF data blog post on migrating to COL XR (https://discourse.gbif.org/t/6398);
- the `gbif/matching-ws` README.
