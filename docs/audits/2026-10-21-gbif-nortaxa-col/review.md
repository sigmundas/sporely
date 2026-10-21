# Review of the GBIF research report (2026-10-21)

This review is by the orchestrating Claude session. It spot-checked the
report's claims against the cached API responses (`data/response_index.jsonl`
and the cache in the scratchpad) and the locally extracted source archives. It
modified no files except this one.

**Verdict: the conclusion holds.** GBIF does not provide identity mappings
between NorTaxa and COL. ChecklistBank's provenance for COL usages copied from
NorTaxa (source 2030) is a real identifier link, but it is only useful for
finding candidates.

## Confirmed

- **GBIF's matcher cannot look up a NorTaxa id.**
  `v2/species/match?taxonID=294404&checklistKey=7ddf754f-…` returns
  `TAXON_ID_NOT_FOUND`, `matchType: NONE`.
- **The provenance link is an identifier link, not a name match.** All 629
  cached `/dataset/315834/nameusage/{id}/source` responses for usages from
  source 2030 return a `sourceId` that is an existing NorTaxa taxonID in the
  pinned NorTaxa 1.284 archive. Examples: QHK7J → 296793, PMLHM → 173254,
  MD47W → 73429. In all 217 non-2030 responses, `sourceDatasetKey` equals the
  local archive's `sourceID`.
- **The pinned local COL archive does not carry this link.** For source-2030
  rows, `link`, `nameAlternativeID` and the source record id are all empty
  (for example QHK7J, PW2RW and MNTV8). Using the link today therefore needs
  the ChecklistBank API, or a ColDP export option that has not been verified.
- **The worked examples match the report:**
  - Hygrocybe conica gives an EXACT name match to 3NB6F, whose source is Index
    Fungorum (`sourceDatasetKey` 2073, `sourceId` 356885). No NorTaxa link
    exists.
  - Buellia grisea is matched as a VARIANT to Buellia grossa (NNFJ), the wrong
    taxon.
  - Clavaria spathula Dicks. is matched EXACT to 5Z9BM, which is O.F. Müll.
  - Stereum durum Burt is matched EXACT to 6ZR96, which is Lloyd.

## Caveats on the report

1. **`agreement_with_sporely = same` overstates agreement.** The column
   compares GBIF with the audit's candidate COL id
   (`sporely_reference_col_id`), not with an approved Sporely identity. The
   audit's own homonym cases (Stereum durum Burt, Clavaria spathula) are
   counted as "same", although GBIF picked the wrong homonym in both. "Same"
   means only that two name matchers agree, as the report's own footnote says.
   The table should not be read as agreement on identity.
2. **The provenance link proves where a COL usage was copied from, not that
   the two are the same taxon.** It shows that COL's usage was copied from
   that NorTaxa record. That makes the 329 accepted copies strong candidates,
   but COL may have re-classified or synonymised them since. They still need
   review under `mapping_policy.yml`, as the report recommends.
3. **The 182-concept sample is small and stratified, so its counts are not
   population rates.** The population checks (596/604, about 4,690 of 16,424)
   rest on the provenance responses and were not independently rerun here.
4. **COL ids copied from NorTaxa are unstable across releases.** The report
   shows 4 of 29 missing from 26.6. Treat this as a risk to Sporely's own
   identity-continuity rule at the next COL refresh, and investigate it
   separately.

## Recommendation

Agree: make no GBIF integration. If anything is adopted, record ChecklistBank
provenance from the pinned COL XR release as a named evidence class for finding
candidates, and first check whether a ColDP export can carry `sourceId`
offline.
