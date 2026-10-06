# Stage 3B refreeze — frozen candidate tax-2026.10.06-01 (2026-10-06)

**Verdict: READY_WITH_RECOMMENDATIONS.** Owner decisions D1 and D2 are implemented. The new set was produced entirely by `build_release.py` and frozen twice; the two freezes are byte-identical. Every Stage 3 check passes on the frozen set. Nothing was committed or published, and production was not touched.

## Frozen candidate (durable, read-only)

The candidate is at `~/sporely-scratch/vernacular-2026-10-06-r2/final`. Its freeze descriptor `freeze.json` has SHA-256 **`fda89f3744975dfb296336b73256c33df1b278efe092eca336557d067d07148e`**.

| File | SHA-256 | Bytes |
|---|---|---:|
| `freeze.json` | `fda89f37…7148e` | 895 |
| `tax-2026.10.06-01.sqlite3.gz` | `9e6690d5…a58d3` | 73,281,566 |
| `tax-2026.10.06-01.evidence.tar.gz` | `97b4a896…feadd` | 53,373,707 |
| `manifest.json` | `6c9d4919…0379e` | 4,235 |
| `compatibility.json` | `747f32d2…feccd` | 3,214 |

- Decompressed SQLite: `3f9037c0…bb73e`. Full hashes are in `verification.json`.
- The set was built twice independently (`buildA`, `buildB`) with `build_release.py --release-id tax-2026.10.06-01`, and all 5 files are byte-identical.
- A third build, the wrong-hash `--promote` refusal run, produced the same freeze hash.
- The superseded set `d0e5de92…` is untouched and was used only for comparison.

Publish with:

```
promote_desktop_bundle.py --frozen-dir ~/sporely-scratch/vernacular-2026-10-06-r2/final --expect-freeze-sha256 fda89f37…7148e
```

## Code and policy changes

- **`vernacular_projection.py`**
  - Added an automatic COL echo guard. A `col_xr` row is withheld from a target when the same (target, language, normalized name) is a NorTaxa association withheld with reason `qualified_or_aggregate_source`. The withheld row is recorded as `withheld_col_vernacular_association` evidence, together with the COL and national source evidence. The guard applies whatever the COL row's provenance is.
  - A valid owner review lifts the guard, but only for the languages listed in its `vernacular_names`. Reviews are still concept decisions, so current names stay derivable.
  - Fixed the qualifier regex: `s.l.` and `p.p.` followed by a capital letter are now treated as author initials, not qualifiers.
- **`build_release.py`**: `--promote` now requires `--expect-freeze-sha256`.
  - Without it, the run is refused before anything is built.
  - With a hash that does not match the fresh freeze, the run is refused before the promote step.
  - The reviewed hash is what gets passed to `promote_desktop_bundle`.
- **Owner reviews.** Two reviews were added to `vernacular_associations.json`, both with `identity_effect` none and `external_identifier_emission` false:
  - `owner-2026-10-06-vernacular-129065`: taigarødtuppsopp (nb) → 58814.
  - `owner-2026-10-06-vernacular-56148`: rosenekornnøtt (nb) → 60590.
  - The nn variant **rosenikornnøtt** exists. It was not approved. It is already present on 60590 through the existing identity-bound NorTaxa projection, unchanged from both bundled and d0e5de92, so its withheld additional-association evidence stays.
- **Recipe pins**: reviewed 17→18, total 1,636→1,637, affected concepts 766→767. The +1 is taigarødtuppsopp on 58814, which is new.
  - rosenekornnøtt, furustokklav and blekgul køllesopp add no rows. Each name already sits on its target through the identity-bound NorTaxa projection, so the exact key is already present.
- **Docs**: README and `docs/vernacular-enrichment.md` now document the single reviewed-publication command and the guard.
- **Tests**: 12 new tests (guard, regex, language-limited review, promote hash). 200 focused Python tests pass, as do the 29 web client contract tests.

## D2: does build_release need the extra evidence that `freeze-final.py` added?

No.
- The `build_release` archive holds:
  - all 13 compiler outputs;
  - a receipt embedding the full build report (input hashes, determinism, coverage);
  - the recipe, source acquisition manifests and normalization reports;
  - COL vernacular rejections, policies, review ledger and scope policy.
- The `stage1b-*` members that `freeze-final.py` added were Stage 1B review documentation: report, tests log, verify script, removal audit, preferred delta and recovery list. None of them is a compile input, and each derives from outputs the archive already contains.
- They belong in the repository evidence folder, which must be committed.
- The member names also differ (for example `owner-reviews.json` → `review_ledger.json`). The Phase B scripts were adjusted for this.

## Content delta against d0e5de92

Full detail is in `content-delta.json`.

- **SQLite vernacular rows**: 96,144 → 96,141.
  - Removed: 19080 nb knolltrevlesopp, 58815 nb sørlig rødtuppsopp and 58815 nn sørleg rødtuppsopp, all `col_xr`.
  - Changed: 58814 nb taigarødtuppsopp moves from `col_xr` non-preferred to `nortaxa` preferred. This is the reviewed addition taking precedence.
- **No other changes.** All other SQLite tables are identical. `taxon_min` differs only in its release id column. `taxonomy_meta` differs only in release id and compiler manifest hash.
- **Compiler `vernacular.jsonl`**: 103,211 → 103,209 (−3 COL rows, +1 NorTaxa reviewed row). No field other than `association_evidence_sha256` changed. Hash churn is evidence-only:
  - 1,341 Dyntaxa sv rows: the D2 normalizer change.
  - 38 col/nortaxa rows: the regex fix. It alters `concept_annotation` for 7 author strings with "S.L."/"P.P." initials.
  - `mappings.jsonl` differs only in `applied_in_release`.
- **Evidence classes**: owner-reviewed additions 17→18; `withheld_col_vernacular_association` 0→3; `withheld_vernacular_association` 1,810→1,807.
  - The 3 rows no longer withheld are 71888 nb and 227151 nb (regex fix) and 56148 nb (review).
  - The regex fix changed qualified status, among NorTaxa fungi carrying nb/nn/no names, only for 71888 and 227151.
  - Automatic additions are unchanged at 1,619.
- **Guard effect**: exactly 3 rows, as the owner expected.

## Stage 3 on the frozen set (two runs, identical)

**Cloud import** ran in the local container `supabase_db_zkpjklzfwzefhjluvhfw` over a unix socket, with no `DATABASE_URL`, inside a transaction ending in `ROLLBACK`. Counts before and after were identical (releases 0, taxa 0, observations 6).

| Check | Result |
|---|---|
| W1 taxa / vernaculars | 633,541 / 96,141 |
| Scoped taxa | 52,917 |
| Scientific names | 60,697 |
| Vernacular names | 59,889 (−3, the D1 rows) |
| Authoritative external IDs | 56,959 |
| Legacy IDs | 0 |
| Red-list rows | 2,600 |
| W1 and scoped exports vs frozen SQLite | Exactly equal |

- All 1,637 additions are in both W1 and the scoped export.
- All 10,331 COL recoveries (on 3,579 concepts) are present. None of the three withheld rows was a recovery: they were new exposure, so the count is unchanged.
- All 13,271 Stage 1 rows are retained.
- Preferred-name drift: 0.
- The 8,453 unsupported historical associations are absent everywhere.
- Entoloma: strict 96276 and collective 625268 are still separate, and no collective name sits on 96276.

**`search_taxa_v2`** ran as `authenticated` with language `no`:
- All 12 previous probes pass.
- taigarødtuppsopp, rosenekornnøtt and blekgul køllesopp each return their target at rank 1, `vernacular_exact`.
- sørlig rødtuppsopp and sørleg rødtuppsopp return nothing.
- knolltrevlesopp returns only 18575 (*Inocybe asterospora*), not 19080.

**Invariants**: all unchanged.
- Unchanged against HEAD: registry, supersessions, manual mappings, mapping policy, `bridge_emission.py`, overlays, repository bundle manifest `051464bb…` and `desktop-compatibility.json` `7afbe071…`.
- Compared with bundled `2026.09.30-01`: 0 differences in external IDs (both tables), scientific names, red-list and taxon identity columns.

**Promotion**, run into a scratch copy of the bundle with a guard in place:
- The output equals the frozen bytes exactly, and a repeat is idempotent.
- A wrong hash is refused, and so is a one-byte tamper in each of the 5 files; nothing is written in any case.
- `build_release --promote` without a hash exits 2 and creates no build directory.
- `build_release --promote` with a wrong hash exits 2 after the freeze; the promote step is not run and the real bundle is unchanged.

**Determinism**: both Phase B runs produced identical analysis, import SQL `4465a8ba…`, rollback SQL `c62f5c4e…` and probe results.

## Discrepancies

- **LOW** — the furustokklav probe fails in the cloud. 103256 *Imshaugia aleurites* is a lichen, and the macrofungi cloud scope (policy unchanged) excludes it, as it did for d0e5de92. The name is still on 103256 in the frozen SQLite and in W1, identical to bundled. The probe expectation was wrong; the data is not.
- **MEDIUM (carried)** — D3: the collective Entoloma 625268 is outside cloud scope.
- **LOW** — the review approval now honours the languages in `vernacular_names`. All existing reviews keep their 17 rows.
- **Recommendation** — commit the evidence folders together with the code. Publish only with the command above.
