# Stage 3C — frozen cloud export, search and publication validation (2026-10-07)

**Verdict: READY_WITH_NONBLOCKING_FINDINGS.** There are no blockers. Checks A–G pass in two independent from-scratch runs. Production was not touched, nothing was committed, and no source, policy, test, recipe or script file changed.

## Input

The owner's prompt named `/tmp/sporely-vernacular-stage2/finalA` (freeze `d0e5de92…`). That set was wiped by a reboot on 2026-10-06, and the owner's D1/D2 decisions superseded it. This run validates only the owner-reviewed set `tax-2026.10.06-01`:

- **Freeze digest:** `fda89f3744975dfb296336b73256c33df1b278efe092eca336557d067d07148e`
- **Location:** `~/sporely-scratch/vernacular-2026-10-06-r2/final` (durable, read-only)
- **Reproducibility:** a fresh build from branch commit `d2a0516` reproduced all five files byte-for-byte.

Nothing was regenerated, and there was no fallback to other artifacts.

## A. Artifact integrity

- All 5 file hashes match `freeze.json` and `vernacular-refreeze-stage3-2026-10-06/verification.json`, checked before and after each run.
- `verify_frozen` passes, and the archive has 32 members.
- The decompressed SQLite hash is `3f9037c0…` in both runs.

## B. Cloud export

The export used the production export path (`cloud_export` / `macrofungi_scope`), then the approved sporely-web importer. The import ran in local container `supabase_db_zkpjklzfwzefhjluvhfw` over a unix socket with no `DATABASE_URL`, inside a transaction ending in `ROLLBACK`. Counts before and after were identical: releases 0, taxa 0, vernacular 0, external IDs 0, observations 6.

| Layer | Taxa | Vernacular | nb/nn/no |
|---|---:|---:|---:|
| Frozen SQLite | 633,542 | 96,141 | 12,719 |
| W1 export | 633,541 | 96,141 | 12,719 |
| Scoped and imported | 52,917 | 59,889 | 6,618 |

- **W1 vs SQLite:** the W1 vernaculars equal the SQLite exactly. Breakdowns by language × source × preferred are in `export-verification.json`.
- **Scoped vs SQLite:** the scoped export equals the SQLite restricted to included concepts. The imported DB fingerprints (vernacular, taxa, external IDs) equal the scoped export.
- **Scope drops:** scoping removed 36,252 rows, and **0 are unexplained**.

  | Scope rule | Rows dropped |
  |---|---:|
  | Excluded concept | 25,023 |
  | Review-only concept | 7,310 |
  | NorTaxa-native concept (never scope-evaluated) | 3,800 |
  | Classification-only ancestor | 119 |
- **Norwegian enrichments:** 1,619 automatic + 18 reviewed = 1,637 on 767 concepts. All targets are selectable, and all 1,637 survive into the scoped export and the DB.
- **COL-echo guard:** it withholds exactly 58815 nb sørlig rødtuppsopp, 58815 nn sørleg rødtuppsopp and 19080 nb knolltrevlesopp.
- **Stage 1B COL recoveries:** all 10,331 are in SQLite and W1. None is affected by the guard. All 7,176 with in-scope targets survive, and the other 3,155 are each explained by a scope rule.

## C. Search probes

Probes ran through `public.search_taxa_v2` as role `authenticated`, with language `no` and limit 50. All 29 pass: 22 positive and 7 negative.

| Query | Concept | Scientific name | Lang |
|---|---:|---|---|
| grønnkremle | 61673 | Russula aeruginea | nb |
| grønkremle | 61673 | Russula aeruginea | nn |
| lys høstmorkel | 14541 | Helvella crispa | nb |
| lys haustmorkel | 14541 | Helvella crispa | nn |
| dunpipe | 14834 | Henningsomyces puber | nb |
| dråpeknippesopp | 103130 | Hypsizygus tessulatus | nb |
| snylterørsopp | 52936 | Pseudoboletus parasiticus | nb |
| tårekjuke | 54615 | Pseudoinonotus dryadeus | nb |
| gul ekornnøtt | 60559 | Rhizopogon luteolus | nb |
| rognerust | 100280 | Gymnosporangium cornutum | nb |
| gråfiolett køllesopp | 146835 | Alloclavaria purpurea | nb |
| grønn navlesopp / grøn navlesopp | 79753 | Arrhenia chlorocyanea | nb / nn |
| taigarødtuppsopp | 58814 | Ramaria rubrievanescens | nb (reviewed) |
| rosenekornnøtt | 60590 | Rhizopogon roseolus | nb (reviewed) |

What every positive probe has in common:

- The target is returned at rank 1 as `vernacular_exact`, matched on a preferred NorTaxa row.
- No competing concept appears in the results.
- The preferred and non-preferred flags do not affect ranking for these probes.

Full per-probe records are in `search-probes.json`. That includes the remaining positives: oliven rådyrslørsopp, slank vedkorallsopp, norsk flammekorallsopp, sumpreddikhette and blekgul køllesopp.

`grønkremle`, `lys haustmorkel` and `grøn navlesopp` are real Nynorsk (nn) names, not misspellings, which shows that `no` covers both nb and nn. Search has no fuzzy matching.

The 7 negative probes:

- sørlig rødtuppsopp and sørleg rødtuppsopp return nothing.
- silkerødspore-gruppen and silkeraudspore-gruppa return nothing.
- falsk lindekorallsopp and blek rødtuppsopp return nothing.
- knolltrevlesopp returns only 18575 *Inocybe asterospora*, its legitimate existing target, and never 19080.

## D. Qualified and collective concepts

Checked by concept ID, there are 0 leaks in SQLite, W1, the scoped export and the imported DB.

- **Strict *Entoloma sericellum* 96276** carries only silkerødspore and silkeraudspore. The `-gruppen`/`-gruppa` names stay on collective 625268, which is outside cloud scope.
- ***Inocybe praetervisa* 19080:** knolltrevlesopp is withheld.
- ***Ramaria aurea*:** strict COL concept 58651 (4RBLM). The qualified NorTaxa usage 626338 ("s. auct. scand.", source 56485) carries falsk lindekorallsopp, which is withheld from the strict concept.
- ***Ramaria strasseri*:** strict COL concept 58832 (4RBVY). The qualified usage 621902 (source 136341) carries blek rødtuppsopp, which is withheld.
- ***Ramaria rubripermanens* 58815:** sørlig rødtuppsopp and sørleg rødtuppsopp are withheld.
- ***Ramaria rubrievanescens* 58814:** only nb taigarødtuppsopp is present, through the owner-reviewed path.

## E. Historical-name delta

- Current-source recovery gaps: 0.
- All 8,453 unsupported historical associations are absent.
- 721 spelling-replacement pairs: the current spelling is present in all 721 and the historical variant in none. There is one ligature duplicate (F2).
- **Preferred-name changes, compiler level** (frozen SQLite vs bundled `2026.09.30-01`): 6,897 flag changes. All are explained by the Stage 1B preferred-name delta, and none is Norwegian.
- **Preferred-name changes, export level** (export vs frozen SQLite): 0.

## F. Identity invariants

Against bundled `2026.09.30-01`, there are 0 differences in:

- taxa identity
- both external-ID tables
- scientific names
- red-list rows

Registry, supersessions, manual mappings and mapping policy equal the repository at HEAD. `source_usages` has 692,068 rows (633,542 anchors and 58,526 aliases), with 0 anchor/canonical mismatches. Canonical concept IDs and authoritative external-ID emission are unchanged through export and import.

## G. Determinism

Between the two runs, these are byte-identical:

- all W1 and scoped export files
- `import.sql` (`4465a8ba…`)
- the rollback SQL
- the probe log
- the analysis

The only nondeterministic field is `build_seconds` in `desktop.json`, which is timing metadata. Every W1/scoped file and `import.sql` also equals the 2026-10-06 refreeze run.

## Findings

- **F1 — MEDIUM (owner decision; recommended fix before production).** Five nb COL vernacular rows carry literal Artsdatabanken status markers in the name. All are `col_xr` and non-preferred, and none is in bundled `2026.09.30-01`:
  - 1041 Rustoker grynhatt [UTGÅTT]
  - 58651 gullkorallsopp [GAMMELT]
  - 58722 Gul korallsopp [UTGÅTT]
  - 60670 Rødbrun flathatt [UTGÅTT]
  - 65750 strøkjuke [GAMMELT]

  They are prefix-searchable. On *Ramaria aurea* 58651 the marked name is the only Norwegian name, so the app would display "gullkorallsopp [GAMMELT]". This is not a qualified-concept leak. The fix belongs in COL normalization (reject or strip bracketed status markers), and it requires a refreeze. The analysis agent reported six marked rows; a direct query of the frozen SQLite finds five Norwegian rows (plus 3 bracketed rows in other languages).
- **F2 — LOW.** On 122442 (sv), two current spellings differ only by the "ﬁ" ligature character.
- **F3 — MEDIUM (by policy, carried over).** NorTaxa-native collective and qualified concepts, such as Entoloma 625268, are outside cloud scope, so their names are not searchable in the cloud.
- **F4 — INFO (pre-existing).** W1 omits the domain root 150361 *Eukaryota*, so the scoped export has 52,917 taxa where 52,918 might be expected.

## Evidence

- `verification.json`
- `export-verification.json`
- `search-probes.json`
- `determinism.json`
- `logs/`
- `scripts/` (`stage3c_prepare.py`, `stage3c_analyze.py`, `stage3c-local-validation.mjs`)

Scratch outputs are in `~/sporely-scratch/vernacular-2026-10-07-stage3c/run{1,2}`. The plan entry is "Stage 3C validation (2026-10-07)".

Stop at the Stage 3 boundary. Production promotion needs explicit owner authorization, through `promote_desktop_bundle.py --frozen-dir <final> --expect-freeze-sha256 fda89f37…` followed by the cloud import.
