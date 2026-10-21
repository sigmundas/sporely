# Review of relationship pass 3 by Codex (gpt-6.1-sol, high effort), 2026-10-21

This read-only review ran through `codex exec` in a read-only sandbox. The text below is its output, unedited.

| Claim | Verdict |
|---|---|
| VERIFIED (147) | **Sound** for the stated documentary tier; no sampled false positives |
| PRAGMATIC-COMPATIBLE (1,172) | **Sound with corrections** to parser guarantees |
| MISSING-AUTHORSHIP (234) | **Sound** |
| PROVENANCE CLASS | **Sound with corrections** to accepted-status labels |
| CHURN ASSESSMENT | **Sound with corrections**; the quantified future-loss projection is unsupported |

1. **CORRECTNESS — the churn projection conflates direction, population and endpoint.**  
   [id_churn.py:119](/Users/sigmundas/Documents/Code/sporely/sporely-worktrees/py-taxonomy-audit/docs/audits/2026-10-20-taxonomy-reconciliation/relationship-pass/v3/scripts/id_churn.py:119) computes **693 (276–1,535)** from **4/29**, newer-release source-2030 ids absent from the older release. The headline **6/31** includes re-sourcing of **V2MTQ and PPY6B**, whose COL ids survived. Scaling 6/31 would instead give approximately **972 (462–1,823)**. Neither calculation estimates future anchor disappearance: the sample is selected, includes synonyms such as **PQQ7F**, and measures several different events. Forward inspection finds **4/31** old NorTaxa ids absent versus **2/210** other-source ids absent. Wilson arithmetic is correct; its range is not an empirical 95% confidence interval for future losses. Keep the volatility/lineage recommendation; withdraw the numerical forecast.

2. **CORRECTNESS — compatible comparisons bypass role-change detection.**  
   [authors_v3.py:321](/Users/sigmundas/Documents/Code/sporely/sporely-worktrees/py-taxonomy-audit/docs/audits/2026-10-20-taxonomy-reconciliation/relationship-pass/v3/scripts/authors_v3.py:321) only checks role moves when the preliminary result is outside `OK`. Executed probes **`Pike in Hale` / `Pike: Hale`** and **`(A in B) C` / `(A: B) C`** return `compatible`, although host becomes sanctioning author. **No affected accepted comparison was found in the 2,805-pair census.** The 13 reported pass-2 pragmatic role conflicts are correctly unresolved.

3. **RISK — “fail closed” and folding guarantees are broader than the grammar.**  
   [authors_v3.py:106](/Users/sigmundas/Documents/Code/sporely/sporely-worktrees/py-taxonomy-audit/docs/audits/2026-10-20-taxonomy-reconciliation/relationship-pass/v3/scripts/authors_v3.py:106) deletes residual non-ASCII surname letters: **`Łind.` / `Ind.` → identical**, **`Þor` / `Or` → identical**. Multiword tokens and malformed `et al.` can also pass: **`Smith John` / itself**, **`Willi Krieg.` / itself**, **`Smith et al. Jones` / itself → identical**. No sampled verified promotion depended on these failures. Actual residual-letter cases are **173281/75SJH** (`Halıcı`, author-standard candidate) and **172709/3ST32** (`Łubek`, unresolved). Initials, particles, `f./fil.`, ordinary `in`, `ex/:`, `&/et`, brackets and tested diacritics otherwise behaved as claimed.

4. **CLEANUP — exact-copy “accepted” counts include provisional acceptance.**  
   [relate_v3.py:323](/Users/sigmundas/Documents/Code/sporely/sporely-worktrees/py-taxonomy-audit/docs/audits/2026-10-20-taxonomy-reconciliation/relationship-pass/v3/scripts/relate_v3.py:323) assigns the accepted-copy subclass without checking status. Of **251**, **240 are accepted and 11 provisionally accepted**, including **78771/RKVKH** and **65684/DSTSD**; all 11 remain unresolved. Similarly, population “329 accepted copies” comprises **309 accepted + 20 provisional**. Provenance derivation itself is correct.

**Sampling and classifications:** seed **20261022**; **36 stratified verified pairs**, including **18 placement-differing**, plus **8 targeted additions**; **28 pragmatic**, **12 missing-authorship**, and **174 unresolved**. All 11 removals and both held entrants were checked. Source checks covered **650 NorTaxa and 824 COL archive rows**, plus all **49 documented Dyntaxa rows’ essential fields**.  
**Verified false-positive estimate:** **0/36 (0%)**, **0/44** including targeted additions; descriptive sample results, not a population estimate. Confirmed examples include **54853/3KV7B**, **71266/3SY98** and the explicitly documented misspelling exception **53677/6FDPL**. No source-proven distinct-taxon false match observed.

**Provenance:** cache extraction reproduced **846 + 245** records; all **605** population links matched. **201/202 blank/blank pairs** identify the same NorTaxa source record imported through both paths. **71450/V7T37** correctly conflicts with accepted copy **L8R6K**. Every verified pair has an eligible non-ChecklistBank witness; copied provenance never qualifies alone.

**Bulk export:** confirmed extended ColDP request, **73 columns with no source-record key**, and DwC-A exporter assignments to dataset/name ids. Cache inventory supports **13 requests**. Approximately **25 requests versus 12,102 per-usage requests** remains a conditional route estimate; search-page filtering and returned linkage were not exercised.

**Validation/S1:** **83 parser tests passed**; **13 validator harness checks passed**, with temporary manifests supplied in memory and validator code unchanged. Six partitions **147/185/87/898/2/234** are disjoint; all **18 non-one-to-one pairs** remain unresolved and are refused. All **12 pinned S1 manifest files** match their hashes, including **62790e59…/6706b6a6…**; independently intersecting actual members gives **zero S1 A0 overlap**. Installed release hash matches its pin.

**Pass-2 findings:** **1, 3, 4, 5 and 6 fixed for their cited cases**; **2 fixed for all 13 reported pairs**, with the additional parser gap in finding 2 above still open.  
**Ran:** complete relationship, provenance, population and churn regeneration into memory; outputs matched exactly. **Inferred:** biological identity, future churn and bulk-route runtime behavior. No writes, commits or network.