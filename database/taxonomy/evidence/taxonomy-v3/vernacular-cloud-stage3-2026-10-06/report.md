# Stage 3 rerun — frozen cloud export, search and publication checks (2026-10-06)

**Verdict: NOT_READY.** This is blocked on an owner decision (D1). If the owner accepts the D1 rows, the frozen set is READY_WITH_RECOMMENDATIONS and needs no rebuild. If the owner rejects them, Stage 1B projection policy changes and a new freeze plus review are required. Production was not touched, nothing was published, and nothing was committed.

## Re-materialization exception

A reboot on 2026-10-06 wiped `/tmp`. That removed the accepted Stage 2 set (`/tmp/sporely-vernacular-stage2/finalA`/`finalB`), the Stage 1B `frozenA`, and the 2026-10-04 Stage 3 scratch. No other copy existed. The owner authorized one exception to "do not rebuild": a deterministic re-materialization from the pinned sources, accepted only if it matched every accepted fingerprint.

- `build_release.py` with the current working-tree code is deterministic but **does not** reproduce the accepted set. The first divergence is the compiler `vernacular.jsonl` (accepted `be7e0d24…`, rebuilt `676489eb…`). The SQLite (`aedf2319…` vs `69a34da8…`) and the freeze (`4c66c7b8…` vs `d0e5de92…`) then diverge in turn.
- Cause: the accepted Stage 1B was built by ad-hoc `/tmp` scripts that reused Dyntaxa normalized by the **HEAD** `national_source.py`. The working-tree version adds `nomenclatural_status`/`concept_annotation` to Dyntaxa evidence.
- The divergence is evidence-only. 1,341 Swedish Dyntaxa rows differ, and only in `association_evidence_sha256`. Dropping that field, both outputs hash to the same value (`85994c31…`) with the same 103,211 rows. Names, languages, preferred flags and targets are identical.
- Gate: re-running with HEAD `national_source.py` reproduces all 13 compiler files and SQLite `69a34da8…`. Re-running the reconstructed Stage 2 `freeze-final.py` twice gives byte-identical output, and all 5 files match the accepted hashes and sizes: freeze `d0e5de92…`, gzip `56020d1f…`, archive `25625ec3…`, manifest `a9ee2bb7…`, compatibility `56258f22…`. All 42 archive members verify. **GATE PASS.**
- Durable frozen set (read-only): `~/sporely-scratch/vernacular-2026-10-06/stage2/finalA`. The reconstructed scripts are in `scripts/` (`rematerialize.sh`, `freeze-final.py`).

## Cloud export and import (local container, rolled back)

The approved sporely-web importer ran against local container `supabase_db_zkpjklzfwzefhjluvhfw` over a unix socket, with no `DATABASE_URL`, inside a transaction that ended in `ROLLBACK`. Counts before and after: releases 0, taxa 0, observations 6.

| Dataset | Rows |
|---|---:|
| W1 taxa / vernaculars | 633,541 / 96,144 |
| Scoped and imported taxa | 52,917 |
| Scientific names | 60,697 |
| Vernacular names | 59,892 |
| Authoritative external IDs | 56,959 |
| Legacy integer IDs | 0 |
| Red-list | 2,600 |

- All 1,636 additions (1,619 automatic + 17 reviewed, on 766 concepts) are exported.
- All 10,331 Stage 1B COL recoveries are in W1.
- All 13,271 Stage 1 rows are retained.
- Preferred-name drift: 0.
- The 8,453 unsupported historical associations are absent from SQLite, W1, the scoped export and the imported DB.

## Search probes

All probes ran through `public.search_taxa_v2` as `authenticated` with language `no`. Every target ranked #1 and was the only result.

| Query | Target | ID | Match |
|---|---|---:|---|
| grønnkremle | Russula aeruginea / 4TRHW | 61673 | vernacular_exact |
| lys høstmorkel | Helvella crispa / 3KJX6 | 14541 | vernacular_exact |
| dunpipe | Henningsomyces puber / 3KV7Z | 14834 | vernacular_exact |
| dråpeknippesopp | Hypsizygus tessulatus / 6N4RM | 103130 | vernacular_exact |
| snylterørsopp | Pseudoboletus parasiticus / 4NQ37 | 52936 | vernacular_exact |
| tårekjuke | Pseudoinonotus dryadeus / 4NYKZ | 54615 | vernacular_exact |
| gul ekornnøtt | Rhizopogon luteolus / 4SD8Z | 60559 | vernacular_exact |
| rognerust | Gymnosporangium cornutum | 100280 | vernacular_exact |
| gråfiolett køllesopp | Alloclavaria purpurea | 146835 | vernacular_exact |
| grønn navlesopp | Arrhenia chlorocyanea / 5VSVR | 79753 | vernacular_exact |
| silkerødspore | Entoloma sericellum (strict) / 6FDM9 | 96276 | vernacular_exact |
| Russula aeruginea | Russula aeruginea | 61673 | canonical_exact |

The targets for rognerust and gråfiolett køllesopp were established from frozen evidence before searching. Full results are in `search-probes.json`.

## Concept-sensitive checks

- The strict *Entoloma sericellum* 96276 and the collective 625268 (NorTaxa 53666) remain separate. `silkerødspore-gruppen` is never on 96276 in SQLite, W1, the scoped export or the imported DB. It returns 0 cloud results because the collective is out of cloud scope (D3).
- Qualified/aggregate Ramaria and Inocybe cases: 9 of the 13 withheld rows are absent. 4 reach COL targets through COL-native rows (D1).

## Invariants

The following are unchanged against HEAD and the accepted Stage 2 protected hashes:

- registry
- `concept_supersessions.yml`
- `manual_mappings.yml`
- `bridge_emission.py`
- overlays
- repository bundle manifest `051464bb…`
- `desktop-compatibility.json` `7afbe071…`

Compared with the bundled release `2026.09.30-01`, external IDs, identity columns, scientific names and red-list show 0 differences.

## Publication mechanics

Promotion ran into a scratch copy of the bundle directory only, with compiler imports, subprocesses and gzip/tar writes guarded to fail.

- `promote_desktop_bundle.py --frozen-dir … --expect-freeze-sha256 d0e5de92…` outputs exactly the frozen bytes.
- A repeat promotion is idempotent.
- A wrong freeze hash is refused, and so is a one-byte tamper in each of the 5 files. Nothing is written in either case.
- The frozen set and the real repository bundle are unchanged.

## Determinism

Run 1 and run 2 of Phase B are identical. They also match the 2026-10-04 Stage 3 run: SQL payload `49f01824…`, every scoped file hash, and the probe results. See `determinism.json`.

Tests: 87 focused Python tests and 29 web client contract tests pass.

## Discrepancies

- **D1 — HIGH (owner decision).** Four NorTaxa names withheld as qualified/aggregate concepts still reach COL strict-species targets through COL's own vernacular rows (`source=col_xr`, non-preferred). Three are ChecklistBank-merged (`clb:merged`, sourceID 2030), which means likely an echo of the same national source rather than independent evidence. NorTaxa keeps each name preferred on its own native concept (621458, 626348, 625094). None of these rows exist in the bundled `2026.09.30-01`, so this is new cloud exposure.
  - 58814 *Ramaria rubrievanescens* — nb taigarødtuppsopp
  - 58815 *Ramaria rubripermanens* — nb sørlig rødtuppsopp, nn sørleg rødtuppsopp
  - 19080 *Inocybe praetervisa* — nb knolltrevlesopp

  The same mechanism, a withheld NorTaxa association whose name is present on the target from another source, applies to about 288 further rows. Those carry the reasons `source_nomenclatural_status_ambiguous`, `higher_classification_disagreement` and `multiple_canonical_targets_in_full_release`, which fall within accepted Stage 1B COL recovery. The full list is in `withheld-associations-present-via-other-sources.json`. D1 is specifically the qualified/aggregate subset that this stage required to stay withheld.
- **D2 — HIGH.** The reviewed set cannot be reproduced by the uncommitted code, because the working-tree `national_source.py` changes the Dyntaxa evidence hashes. `build_release.py --promote` binds to the freeze it has just built, not to the reviewed hash. Run with the current code, it would publish the unreviewed freeze `4c66c7b8…`. Publish only with `promote_desktop_bundle.py --frozen-dir <finalA> --expect-freeze-sha256 d0e5de92…`.
- **D3 — MEDIUM (carried over).** The collective Entoloma 625268 is outside cloud scope, because `macrofungi_scope.load_taxa` loads only `col_xr`.
- **D4 — LOW.** rognerust and gråfiolett køllesopp also sit on the NorTaxa-native duplicate concepts 621074 and 621443 in the desktop SQLite. Those concepts are not in the cloud.
- **D5 — LOW.** The Stage 1B and Stage 2 build scripts existed only in `/tmp`. Reconstructions are archived in `scripts/`.

## Evidence

- `verification.json`
- `rematerialization.json`
- `export-verification.json`
- `search-probes.json`
- `promotion-checks.json`
- `determinism.json`
- `withheld-associations-present-via-other-sources.json`
- `logs/`
- `scripts/`

Larger outputs stay in `~/sporely-scratch/vernacular-2026-10-06/`, referenced by hash.
