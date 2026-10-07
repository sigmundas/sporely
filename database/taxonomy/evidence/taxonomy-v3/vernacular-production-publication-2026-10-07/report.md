# Production publication of tax-2026.10.07-01 (Stage 4C)

**Verdict: PRODUCTION_RELEASE_VERIFIED_WITH_NONBLOCKING_FINDINGS** (2026-10-07).

Production project `zkpjklzfwzefhjluvhfw`. Previous active release `tax-2026.09.30-01` (now retired); new sole active release `tax-2026.10.07-01`.

## Artifacts

- Freeze SHA-256: `a86e35854fd4d01984d8b8fe121d8fc318e75a01876d243975847127462fd14e`. All frozen-file hashes and sizes re-verified.
- Import SQL: the exact Stage 3 `run1/import.sql`, 76,193,662 bytes, SHA-256 `cc9a1ddfc4c5f56fa553935b79fb40a2eda01588f0c6d2781e243cddda84852e` (run2 identical). Unchanged before and after execution; nothing regenerated.

## Sequence

1. **Read-only preflight** (`preflight.json`): sole active `tax-2026.09.30-01`; 52,917 concepts / 13,760 vernaculars; no target release, run or rows; validator source equals migration `20261007123912`, with owner/ACL/settings unchanged since Stage 4B; `statement_timeout` 2min. The 16-table production fingerprint equals the Stage 4B post-deploy fingerprint. Previous-release taxa, scientific names, external IDs and red-list rows equal the frozen candidate row for row and equal the Stage 4 baseline digests. All seven taxonomy_v3 tables equal the Stage 4 baselines.
2. **Import** (`import-execution.json`): the same containerised psql command as Stage 4, from 13:34:31 to 13:35:34 UTC, exit 0, `COMMIT` reached. The artifact loads, runs `taxonomy_v2_validate_release` and activates in one transaction, so validation precedes activation atomically.
3. **Post-commit verification** (`production-verification.json`, read-only, 13:46–13:47 UTC):
   - Release state: 2026.10.07-01 active, 2026.09.30-01 retired, exactly one active release; one succeeded import run.
   - Counts: concepts 52,917 (unchanged); new-release vernaculars 59,884 (frozen metadata); previous release 13,760 retained.
   - Validator ok with no errors; national-name errors empty. Timing: full validator 840 ms on a warm rerun (2,207 ms on the first run); parent check about 170 ms; timeout 2min unchanged.
   - Fingerprint: every pre-existing row is unchanged. Only `taxonomy_v2_releases` (the new row plus the status transition) and `taxonomy_v2_import_runs` (+1) differ.
   - New-release taxa, scientific names, external IDs, red list and vernacular names equal the frozen scoped export exactly (0 unexpected, 0 missing, 0 duplicates). Non-vernacular tables equal the pre-publication baseline digests. taxonomy_v3 identity, registry and mapping tables are unchanged.
   - No `[GAMMELT]` / `[UTGÅTT]`. 357 spelling replacements checked: all current names present, no historical spellings.
   - Isolation: 96276 has only `silkerødspore`/`silkeraudspore` (no -gruppen name); 58814 has only `taigarødtuppsopp`; 19080, 58651, 58832 and 58815 have no Norwegian names.
   - All 33 Stage 3 search probes pass. The 22 expected-target probes return the intended concept first with a result list identical to Stage 3; the 11 negative/withheld probes are identical to Stage 3.

## Findings

- **Nonblocking, owner-accepted:** `dangling_parent_count` is 1, as declared in the release metadata. The sole row is the kingdom root Fungi, 152331, whose parent 150361 is outside the release scope. All prior releases contain the identical row. The verification asserts exactly this row and fails on any other.
- Verification script fix: production `search_taxa_v2` exposes the displayed scientific name as `display_scientific_name`, which the Stage 3 probe file records as `scientific_name`. The comparison maps the field name; the values compared are unchanged.

## Contents

- `scripts/`: the Stage 4C preflight, import and verification scripts (paths refer to the operator's scratch directory; the credentials file is referenced by path only).
- `logs/`: their console output.
- `stage4-attempt1/`: the first attempt, which timed out in the validator and rolled back cleanly, plus the timeout investigation.
- `stage4b-deploy/`: the validator-fix deployment verification (before/after fingerprints; the baseline for the Stage 4C preflight).
