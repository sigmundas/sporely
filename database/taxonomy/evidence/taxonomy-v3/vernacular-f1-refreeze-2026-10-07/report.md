# Stage 3D — F1 refreeze: reject source-marked obsolete COL vernaculars (2026-10-07)

**Verdict: READY_FOR_PRODUCTION_PROMOTION.** There are no new findings. Nothing was committed, promoted or published, and production was not touched.

## Change

Owner decision: fix Stage 3C finding F1. A COL vernacular whose name contains an Artsdatabanken status marker `[GAMMELT]` ("old") or `[UTGÅTT]` ("retired") is rejected, not stripped and kept. The source itself labels these names obsolete. Stripping the marker would duplicate current spellings on 1041 and 60670, and would present an obsolete name as the current Norwegian name on 58651, 58722 and 65750.

- `scripts/normalize_col_xr.py` adds `OBSOLETE_VERNACULAR_MARKER`, matching `\[\s*(GAMMELT|UTGÅTT)\s*\]` case-insensitively. A matching row is written to `vernacular_rejections.jsonl` with reason `source_marked_obsolete_vernacular` and its raw row provenance, and is counted as `source_marked_obsolete`.
- `tests/test_stage_3a_1.py` adds a test for case and whitespace variants and a leading marker. It also checks that non-markers stay: `[dau ddot]`, `[2 ddot]`, `Lungwort [lichen]`, bare words and `[GAMMELTX]`.
- `README.md` lists the COL rejection reasons.

**Full marker scan** of the pinned COL VernacularName (all rows, all languages): exactly 5 hits. All are `nob`/NO rows contributed via iNaturalist.

| Concept | COL ID | Rejected name |
|---:|---|---|
| 1041 | 33PRB | Rustoker grynhatt [UTGÅTT] |
| 58651 | 4RBLM | gullkorallsopp [GAMMELT] |
| 58722 | 4RBQ5 | Gul korallsopp [UTGÅTT] |
| 60670 | 4SF4S | Rødbrun flathatt [UTGÅTT] |
| 65750 | 4XRP7 | strøkjuke [GAMMELT] |

## Frozen candidate

- **Release:** `tax-2026.10.07-01`
- **Freeze digest:** `a86e35854fd4d01984d8b8fe121d8fc318e75a01876d243975847127462fd14e`
- **Location:** `~/sporely-scratch/vernacular-2026-10-07-f1/final` (durable, read-only)
- **File hashes:** sqlite.gz `bff40f0d…`, evidence archive `e2d1dfca…`, manifest `83686295…`, compatibility `abfb70df…`
- **Decompressed SQLite:** `e349e8b1…`
- **Build:** produced by the documented `build_release.py`. buildA and buildB are byte-identical, and a third build in the promotion checks gave the same freeze.

This candidate supersedes `fda89f37…` (`tax-2026.10.06-01`).

## Content delta vs fda89f37

- **Vernacular rows:** exactly −5 `vernacular_min` rows, the five above (96,141 → 96,136). No preferred flag changed.
- **Everything else** is metadata: `compiler_manifest_sha256`, vernacular index statistics, evidence counts and hashes (nb rows 12,535 → 12,530, COL rejections +5), and the release ID in `mappings.jsonl`.

Norwegian names on the affected concepts now equal bundled `2026.09.30-01`:

| Concept | Norwegian name now |
|---:|---|
| 1041 | rustoker grynhatt (preferred, NorTaxa) |
| 58651 | none |
| 58722 | none |
| 60670 | rødbrun flathatt / raudbrun flathatt |
| 65750 | taggkroneskinn |

No recipe pins changed. The projection pins hold: 1,619 automatic + 18 reviewed = 1,637 on 767 concepts. None of the five rows was a Stage 1B recovery, so recoveries stay at 10,331 (7,176 selectable, all surviving, 0 gaps).

## Stage 3C checks rerun (A–G, twice)

All pass. The local import ran in `supabase_db_zkpjklzfwzefhjluvhfw` over a unix socket with no `DATABASE_URL`, and ended in `ROLLBACK`. Counts before and after are identical.

- **Export:** scoped and imported vernaculars are 59,884 (previously 59,889). Scoping drops 36,252 rows, 0 unexplained. The imported DB fingerprints equal the scoped export.
- **Enrichments and guard:** all 1,637 enrichments survive. The guard withholds exactly 58815 nb/nn and 19080 nb.
- **Probes:** all 29 previous probes pass, with 0 qualified-concept leaks and 0 identity differences against bundled.
- **Markers:** none in SQLite, W1, the scoped export or the imported DB. This was checked with a full-table regex, with a positive control.

Searches for the former marked names:

| Search | Returns |
|---|---|
| gullkorallsopp | 58668 only |
| strøkjuke | 65711 "strøkjuke" |
| Gul korallsopp | 58818 "gul korallsopp" |
| Rustoker grynhatt | 1041 and the existing NorTaxa variety 163350 |
| Rødbrun flathatt | 60670 |

- **Promotion:**
  - A scratch promotion with `--expect-freeze-sha256` produced the exact frozen bytes, and repeating it was idempotent.
  - A wrong hash and all 5 tamper cases were refused with no writes.
  - `build_release --promote` was refused when given no hash and when given a wrong one.
  - The real repository bundle is unchanged.
- **Determinism:** `import.sql` (`cc9a1ddf…`) and the validation log are identical across runs. Only `build_seconds` in `desktop.json` varies.
- **Tests:** `database/taxonomy/tests` has 1,076 passed and 1 skipped.

## Findings

- **INFO — discarded run.** The first validation pair was thrown away because a report-only probe reused the query "gullkorallsopp" and overwrote the positive probe. Both runs were redone from scratch.
- **INFO — disk space.** The disk filled up (ENOSPC) during the first promotion check. Space was freed only in this stage's scratch folder, and the check was rerun.
- **Accepted by the owner (2026-10-07):** F2 LOW (ligature duplicate on 122442 sv), F3 MEDIUM (NorTaxa-native collective and qualified concepts are outside cloud scope), F4 INFO (*Eukaryota* root missing from W1).

## Reproduce and publish

Build: `.venv/bin/python database/taxonomy/scripts/build_release.py --release-id tax-2026.10.07-01 --build-dir <new dir>`, from HEAD `d2a0516` plus this stage's change.

Owner-authorized publication only:

1. `promote_desktop_bundle.py --frozen-dir ~/sporely-scratch/vernacular-2026-10-07-f1/final --expect-freeze-sha256 a86e35854fd4d01984d8b8fe121d8fc318e75a01876d243975847127462fd14e`
2. Then the cloud import.

## Evidence

- `verification.json`
- `content-delta.json`
- `search-probes.json`
- `export-verification.json`
- `promotion-checks.json`
- `determinism.json`
- `logs/`
- `scripts/`
