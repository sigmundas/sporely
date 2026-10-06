# Audit of 19,505 removed vernacular rows

The removals are not all intentional evidence cleanup. **10,331 rows have reconstructable current pinned COL evidence that the pipeline omits.** Another **721** are capitalization/spacing replacements already present on the same current concept. **8,453** lack a current pinned source-to-target association and should remain withheld until that association is established; this does not prove the historical names obsolete.

No rows were restored. This pass writes audit evidence only; compiler, policy, identities, database candidates, publication and production are unchanged.

## Source and language census

“Missing recovery” means current pinned source evidence is already linked to the same stable target. “No target evidence” means no established current source-to-that-target association, even where the spelling occurs elsewhere.

| Historical source | Language | Removed | Missing recovery | Current spelling retained | No target evidence |
|---|---|---:|---:|---:|---:|
| Artportalen | Swedish | 4,337 | 1,994 | 721 | 1,622 |
| iNaturalist CSV | Danish | 1,786 | 1,010 | 0 | 776 |
| iNaturalist CSV | German | 2,198 | 1,148 | 0 | 1,050 |
| iNaturalist CSV | English | 3,028 | 1,692 | 0 | 1,336 |
| iNaturalist CSV | Spanish | 329 | 135 | 0 | 194 |
| iNaturalist CSV | Finnish | 3,116 | 1,964 | 0 | 1,152 |
| iNaturalist CSV | French | 2,796 | 1,490 | 0 | 1,306 |
| iNaturalist CSV | Italian | 89 | 36 | 0 | 53 |
| iNaturalist CSV | Polish | 1,304 | 685 | 0 | 619 |
| iNaturalist CSV | Portuguese | 90 | 31 | 0 | 59 |
| iNaturalist CSV | Swedish | 432 | 146 | 0 | 286 |
| **Total** | | **19,505** | **10,331** | **721** | **8,453** |

Historical `source` describes the old row, not necessarily the current provider that supports it. All 10,331 missing recoveries have COL evidence on 3,579 concepts; current projection should retain current provenance rather than perpetuate the old provider label.

## Reasons and evidence strength

- **10,302:** `col_vernacular_member_not_normalized`. The pinned COL archive contains `VernacularName.tsv`, linked by `col:taxonID` to an existing current COL usage/Sporely target. `normalize_col_xr.py` does not emit these names, so the compiler cannot consume them. This requires source-native vernacular normalization/projection, not a scientific-name association or identity change. Across the missing-recovery cohort, 10,330 COL usages are accepted and one is provisionally accepted; all are the target’s existing canonical COL usage. All 19,505 target concepts remain in the candidate.
- **29:** the same COL omission, plus `national_language_policy_exclusion` on the supporting Dyntaxa copy: 15 Danish, 1 English, 13 Finnish. Dyntaxa deliberately projects Swedish only. That policy explains its copies being absent, but does not negate independent current COL evidence.
- **721:** `historical_spelling_replaced_by_current_source_spelling`. The candidate already has a Swedish name on the same target differing only in Unicode/capitalization/spacing normalization. These are exact-string removals, not lost name coverage. All have current Dyntaxa support; 715 also have COL support.
- **8,453:** `no_current_pinned_source_to_target_association`, with `provider_snapshot_available_but_not_current_pinned_source`. Local provider snapshots are historical candidates, not release-authoritative evidence. Do not project them simply because they reproduce the old database.

Within the 8,453 withheld associations:

| Current archive evidence | Rows | Interpretation |
|---|---:|---|
| Matching vernacular and matching scientific usage name, but no target association | 5,616 | Potential metadata reconciliation investigation; name equality is diagnostic only |
| Matching vernacular only on other scientific concepts | 1,116 | Homonymous vernacular/other concepts; no inferred transfer |
| No matching vernacular in the three pinned archives | 1,721 | Only local historical provider evidence found |
| **Total** | **8,453** | |

Thus **6,732** withheld rows have current name records somewhere, but insufficient evidence for the historical target. This is kept separate from the **1,721** with no matching pinned name. Within the withheld set, 7,981 local snapshot matches have a provider-ID or existing national-source-binding match; 472 have only scientific-name diagnostics. Neither makes an unpinned snapshot a current release source.

“No current evidence” in the machine manifest is explicitly scoped to **the association to this target**, not a claim that the name is absent from live provider databases. Each row separately records whether a current pinned name exists anywhere and whether its target association exists. Qualified/native concepts and names on other concepts are not reassigned.

## Method and artifacts

Compared the old read-only bundle with the already-built stage-1 candidate using exact `(taxon_id, language_code, vernacular_name)` keys. Audited every removed key against recipe-pinned NorTaxa 1.284, Dyntaxa 2026-09-30 and COL XR 2026-07-17 archives, existing compiled source usages, and local provider snapshots. Evidence matching uses NFC/case/whitespace normalization and language-code equivalence; it never creates a concept association. Only an existing current usage binding to the same target establishes reconstructability. Scientific-name comparisons and records bound elsewhere are diagnostic.

The legacy SQLite input is pinned as compatibility data, but historical presence alone is not current vernacular evidence. Local multilingual provider CSVs are not pinned source acquisitions in the release recipe. The current iNaturalist refresh concerns external-ID decisions, not vernacular records.

- `removed-row-classification.jsonl.gz`: all 19,505 rows, exact old key/source/preferred status, target, reasons, current supporting/other-concept records, source usage and archive row index, and unpinned snapshot provenance.
- `summary.json`: full source/language/reason census and input fingerprints.
- `current-reconstructable.json`: 10,331 omitted-current-evidence candidates.
- `unpinned-snapshot-candidates.json`: 8,453 associations requiring acquisition/revalidation.
- `verification.json`: exhaustive/disjoint classification, repeat-byte verification and protected-input hashes.

The audit fingerprints its input archives and artifacts. Rich source metadata is retained in the audit manifest; this pass does not implement release provenance archiving.

## Consequence for the staged work

The prior description of all 19,505 removals as unsupported cleanup was too broad. Before freezing/promotion, address the omitted source-native COL vernacular extension and re-evaluate the resulting change report. Preserve current source spellings and preferred-name policy; do not insert historical rows wholesale. Review/acquire/pin the remaining provider evidence separately, validating concept associations and withholding unresolved cases.

The Norwegian enrichment policy and its previously verified 1,619 automatic + 17 reviewed additions are unchanged. No automatic guard was weakened, no identities were reconciled, and no compiler or release was rerun for this audit. Stop after this evidence census.
