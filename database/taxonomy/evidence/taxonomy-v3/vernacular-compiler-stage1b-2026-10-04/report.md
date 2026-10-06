# Stage 1B — current COL vernacular recovery

Completed the bounded implementation. All **10,331** audited missing rows on **3,579 concepts** now have current pinned COL coverage. The full removal audit has **zero** `current_evidence_exists_but_compiler_does_not_recover_it` cases and no policy rejections among these recoveries. The **8,453** unsupported historical associations remain withheld.

## Candidate delta

| Comparison | Rows |
|---|---:|
| Old bundled vernacular rows | 31,140 |
| Stage 1 candidate rows | 13,271 |
| Stage 1B candidate rows | 96,144 |
| Exact old rows retained | 19,418 |
| Exact old rows removed | 11,722 |
| Remaining removals with current capitalization/spacing replacement | 3,269 |
| Remaining unsupported associations | 8,453 |
| Exact additions versus old bundle | 76,726 |
| Exact additions versus Stage 1 | 82,873 |
| Removed versus Stage 1 | 0 |

The full source-native extension contains 89,926 normalized rows, representing 89,917 unique exact concept/language/name keys in 133 language codes. Existing compiled/SQLite deduplication accounts for copies. The larger candidate reflects current direct COL metadata across its canonical scope, beyond the historical recovery cohort; no name is associated across concepts or inferred by scientific-name equality.

Exact remaining removals by language: Swedish 4,619; English 1,706; French 1,315; Finnish 1,178; German 1,058; Danish 912; Polish 627; Spanish 194; Portuguese 59; Italian 54. The nested removal-audit summary gives the complete historical source/language/reason breakdown and its manifest classifies every remaining removed row.

## Implementation and provenance

- `normalize_col_xr.py:_normalize_vernaculars` reads the recipe-pinned `VernacularName.tsv` additively. Normalized rows retain the COL usage ID, source release, unmodified name, ISO language conversion, preferred flag, archive member/line/row and all raw fields (including source/reference/status-like annotations). Other source language codes remain intact; three missing-language fungal rows explicitly use `und` with a diagnostic flag.
- Scope follows the existing normalized COL usage set. Of 1,996,915 archive vernacular records, 89,926 are emitted; 1,894,695 reference usages outside that normalized scope; 12,294 reference usages absent from current `NameUsage.tsv` and are explicitly recorded in `vernacular_rejections.jsonl`. No dangling usage is reassigned. These full-archive rejections are not rejections of the 10,331 recovery cohort.
- `compile_release.py` reuses the existing `core_row_id_to_sporely` vernacular join and additionally asserts that the resolved target's canonical COL usage is exactly the incoming usage. Unknown references fail; scientific-name equality never supplies an association.
- `vernacular_projection.py` preserves independently derivable Norwegian automatic/reviewed associations when a source-native COL copy exists. Original source-native COL evidence remains in `vernacular_evidence.jsonl`, including archive-row provenance and existing canonical target evidence.
- `build_sqlite_candidate.py` retains exact-key deduplication and gives current national-source copies priority over duplicate COL copies, preserving their compact source/preferred metadata. Compiler evidence retains all supporting COL copies, even where SQLite deduplicates.

No aliases, supersessions, identity bindings, registry allocation, bridge eligibility, authoritative external-ID emission, schema or production changes. No historic-only restoration. Release evidence archiving/promotion remains the next separate stage.

## Preferred names

No existing Stage 1 preferred flag changes (**zero**). Relative to old bundled rows that survive exactly, 6,897 flags differ: the prior 19 Swedish changes (15 false→true, 4 true→false) and 6,878 recovered rows that no longer inherit a historical preferred flag. Their current COL preferred field is empty; it is normalized as false, with the raw value preserved. These flags do not replace any existing current preferred vernacular. The complete exact-row changes are in `preferred-name-delta.json`.

## Verification

- 187 compiler/source/bridge/build/review tests pass. Focused fixtures cover distinct usage IDs with identical scientific names, dangling COL usage rejection, raw provenance, missing language, unchanged taxonomy normalization, duplicate COL/national preferred handling and independent Norwegian associations.
- Two independent normalized COL artifact sets are byte-identical, including taxonomy, vernaculars, report and rejection evidence.
- Two compiler sets and two SQLite candidates are byte-identical; manifests validate output fingerprints.
- Every one of the 10,331 recovery cases is linked in `recovered-current-col-associations.json` to current COL usage/member/row provenance; all match the same canonical usage. All 89,926 normalized current-canonical COL records have exact names in the SQLite projection. No silent omissions.
- Compiler `taxa.jsonl`, `source_usages.jsonl`, `mappings.jsonl` and `legacy_external_ids.jsonl` are byte-identical to Stage 1. Registry is byte-identical. SQLite canonical/scientific-name/authoritative external-ID tables are identical; integrity and foreign-key checks pass.
- Norwegian association evidence is unchanged: 1,619 automatic + 17 reviewed, all automatic targets full-universe unique, all ten concept reviews reused. Grønnkremle remains automatic; grønn navlesopp's compact row is unchanged; the five open species remain withheld by unchanged association evidence. Entoloma strict/collective concepts remain separate.
- Full removal-audit repeat is byte-identical and exhaustively classifies all 11,722 exact removals.
- Syntax and diff checks pass. `verification.json` records fingerprints, counts and protected-input checks. `verify_stage1b.py` captures the candidate verification procedure; input/candidate artifacts are under `/tmp/sporely-vernacular-stage1b`.

Stop at Stage 1B. All changes remain uncommitted. No bundled release replacement, promotion or production changes.
