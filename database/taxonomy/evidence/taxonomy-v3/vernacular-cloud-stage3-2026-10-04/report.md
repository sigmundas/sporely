# Stage 3 — frozen cloud export and publication checks

Validated accepted `finalA` as exported, without recompiling taxonomy or revisiting builder logic. The current cloud importer and actual `public.search_taxa_v2` pass all requested Norwegian probes. Production and the repository bundle remain unchanged. Changes remain uncommitted.

**Publication caveat:** strict Entoloma and the collective concept remain separate in frozen SQLite, but the current cloud projection excludes the source-native collective concept entirely. `macrofungi_scope.load_taxa` loads only `source_system='col_xr'`; NorTaxa-native 625268 is outside its universe. Cloud search for `silkerødspore-gruppen` returns no results. This is not a merge or a failed enrichment, and the collective name is never placed on strict 96276. Broadening cloud scope was outside this validation stage and has not been implemented. If cloud availability of the group concept is required for this release, resolve this scope limitation in a separate bounded stage before publication.

## Export/import counts

| Dataset | Cloud-scoped rows |
|---|---:|
| Taxa (including classification ancestors) | 52,917 |
| Scientific names | 60,697 |
| Vernacular names | 59,892 |
| Authoritative external IDs | 56,959 |
| Legacy namespace-lost IDs | 0 |
| Red-list assessments | 2,600 |
| Release metadata | 1 |

The full W1 export has 633,541 taxa and 96,144 vernaculars. The cloud scope is the existing archived global-macrofungi policy. All scoped current SQLite names export exactly, including their preferred flag/source. All 6,948 existing Stage 1 names in scope remain; 52,944 additional scoped names are present after COL normalization. All 4,616 old-bundle Norwegian rows in scope remain. All 1,619 automatic and 17 reviewed Norwegian additions are exported on 766 concepts. No scientific-name matching or identity changes occur during export.

## End-to-end probes

The approved web importer generated a fingerprinted SQL payload from the scoped files. It was executed only in the independently verified local Supabase container, under a transaction terminated with `ROLLBACK`. Importer's row/hash/release validation and internal activation/search/protected-state checks passed. Named probes ran under role `authenticated` through the actual SQL function used by Android/web search (`public.search_taxa_v2`, language `no`, limit 50).

| Query | Canonical target | Sporely ID |
|---|---|---:|
| grønnkremle | Russula aeruginea / 4TRHW | 61673 |
| lys høstmorkel | Helvella crispa / 3KJX6 | 14541 |
| dunpipe | Henningsomyces puber / 3KV7Z | 14834 |
| dråpeknippesopp | Hypsizygus tessulatus / 6N4RM | 103130 |
| snylterørsopp | Pseudoboletus parasiticus / 4NQ37 | 52936 |
| tårekjuke | Pseudoinonotus dryadeus / 4NYKZ | 54615 |
| gul ekornnøtt | Rhizopogon luteolus / 4SD8Z | 60559 |
| grønn navlesopp | Arrhenia chlorocyanea / 5VSVR | 79753 |
| silkerødspore | strict Entoloma sericellum / 6FDM9 | 96276 |

Each query returns its target as `vernacular_exact`. `Russula aeruginea` also returns 61673 with grønnkremle as its vernacular label. Detailed results, including IDs/metadata, are saved in `search-probes.json`. These prove importer→database→RPC search behavior, not a live device/browser UI session. The web client contract suite passes all 29 tests and confirms its RPC dispatch/language/normalization behavior. No client code changed.

Collective Entoloma 625268 / NorTaxa 53666 still has `silkerødspore-gruppen` in frozen SQLite, while strict 96276 has `silkerødspore`. The scoped files and imported database contain no collective name on the strict species. The collective is unavailable in cloud search under current scope, as noted above.

## Exact publication-byte confirmation

Accepted freeze descriptor SHA-256 remains `d0e5de92cd2e4e2222f77818e5b0425360d37596df2cc9b3d3fbd9567d6870ad`. Every `finalA` file matches accepted Stage 2 fingerprints; full archive members and decompressed SQLite verify. Promotion's frozen hash requirement refers to exactly this set. It would copy its prepared gzip, archive, manifest and compatibility bytes, plus the same freeze descriptor; no new compiler/evidence generation. Stage 2 copy-only scratch proof remains applicable. Stage 3 export and import SQL are separately fingerprinted validation outputs and do not alter the accepted frozen set.

The local import was rolled back, with release/taxa/observation counts identical before and after. Protected repository bundle/compatibility, reviewed mappings/supersessions, bridge policy and accepted SQLite fingerprints remain unchanged. Production was neither connected to nor written. No migration or schema changes, no builder modifications, no publication.

Evidence: `export-verification.json` (counts/all-row preservation), `prepared.json` (SQL hash/import expectations), `search-probes.json`, `local-validation.log`, `rollback-verification.json`, `promotion-checks.json`, `verification.json`, and client test log. Scratch exports/payload are under `/tmp/sporely-vernacular-stage3`.

Stop at Stage 3 validation boundary. Publication readiness has the explicit collective-concept scope caveat; no changes were made to bypass it.
