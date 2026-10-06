# Revised Norwegian vernacular policy simulation — before compiler implementation

Date: 2026-10-04. Current canonical universe: the COL concepts in `tax-2026.09.30-01` bundled SQLite, including canonical concepts outside the current cloud scope. Cloud reach counts use the saved read-only snapshot from the previous audit; no production operation was performed in this pass.

## Changed counts

| Metric | Previous policy | Revised policy | Change |
|---|---:|---:|---:|
| New unique target/language/name rows | 1,483 | **1,619** | +136 |
| Canonical concepts affected | 691 | **756** | +65 |
| Source vernacular rows accepted | 1,485 | **1,621** | +136 |
| Source usages needing review | 127 | **62** | −65 |
| Exception vernacular rows | 257 | **121** | −136 |

Affected automatic concepts: **633 species, 119 genera, 3 varieties, 1 subspecies**. Exceptions: **16 species and 46 genera**. Existing cloud coverage (4,622 source rows), all-source coverage census and taxonomy scope remain unchanged. Source duplicates collapse two accepted source rows, hence 1,621 proposed source rows become 1,619 unique projected rows.

The change admits **138 source vernacular rows on 66 species** with below-kingdom classification differences, retained as provenance warnings. It withdraws the two `mosehatter` language rows on **Gerronema**, because its full canonical name/rank bucket has two targets. `policy-change.json` records every newly admitted and withdrawn association.

## Revised rules

- Exact normalized scientific name and rank; author equality is not required.
- Accepted/current source and target status. Explicit source accepted-reference to a different usage is blocked.
- **Exactly one accepted canonical name/rank target across the full canonical release**, before considering cloud scope or using classification to narrow targets. Eligibility evidence checks all full-release candidates. Scope controls reach/output counting only; it cannot make an ambiguous bucket unique.
- Explicit fungal kingdom compatibility. Missing target kingdom confirmation or kingdom disagreement blocks.
- Source/target qualifiers, aggregates, nomenclatural warnings and explicit split/merge annotations block. Raw source warning tokens other than empty, conserved or sanctioned are withheld, including dubious and orthographic. Target status/remarks checks remain separate from author differences.
- Existing reviewed mappings, rejections and supersessions take precedence. No contradictory decision is bypassed.
- For **species**, family/order/class/phylum/genus disagreements are warnings; they do not block an otherwise exact unique match. For other ranks, those disagreements still require review. An unresolved/incertae-sedis placement is missing information, retained diagnostically.
- A different existing same-language vernacular is retained. It no longer blocks adding an alias; the proposed row is marked non-preferred if the target already has a preferred name in that language. Existing preferred rows are never altered by the simulation.
- No identity/alias/source-usage/registry/external-ID operations are performed. The rule is `vernacular_only_exact_name_rank_full_universe_unique_current_v2`, not an authoritative bridge class.

## Exception counts

Categories overlap; `exception-source-usages.json` is the exact 62-entry deduplicated queue.

| Reason | Vernacular source rows | Source usages |
|---|---:|---:|
| higher_classification_disagreement | 83 | 42 |
| kingdom_disagreement | 2 | 1 |
| multiple_canonical_targets_in_full_release | 23 | 12 |
| multiple_canonical_targets_in_scope | 15 | 8 |
| qualified_or_aggregate_source | 19 | 10 |
| source_nomenclatural_status_ambiguous | 7 | 4 |
| source_not_fungi | 2 | 1 |

Full-release ambiguity affects 12 source usages, of which 8 also have more than one target in current cloud scope. Names: Coprotus, Gerronema, Helvella crispa, Henningsomyces, Henningsomyces puber, Hydnellum, Hydnum, Lachnella, Lepiota, Lycoperdon, Odontia, Sarcodon.

The remaining 16 species comprise 10 qualified usages, 4 nomenclatural warnings, and the two full-release ambiguities dunpipe/lys høstmorkel. Below-kingdom disagreement is not a species blocker. The non-fungal Melanogaster homonym remains rejected. No contradictory reviewed decision was found among automatic candidates. No automatic name is accepted on a target with an unresolved kingdom or qualified/split/nomenclatural blocker.

## Regressions

| Case | Revised result |
|---|---|
| grønnkremle / Russula aeruginea | **Automatic**, unique full target 61673 / 4TRHW; author `Lindblad` vs `Lindblad ex Fr.` diagnostic only. |
| Gerronema / mosehatter | **Exception**, full targets 85259 and 611731. Cloud filtering cannot select a winner. |
| dunpipe / Henningsomyces puber | **Exception**, full targets 14834 and 165605; family difference remains provenance, not a species blocker. |
| lys høstmorkel / Helvella crispa | **Exception**, full targets 14541 and 165172; still ambiguous in cloud scope too. |
| rognerust / Gymnosporangium cornutum | **Automatic**, unique target 100280; Pucciniaceae vs Gymnosporangiaceae recorded as warning. |
| gråfiolett køllesopp / Alloclavaria purpurea | **Automatic**, unique target 146835; Repetobasidiaceae vs Rickenellaceae recorded as warning. |
| grønn navlesopp / Arrhenia chlorocyanea | **Unchanged**, already reaching 79753 in bundle/cloud. |

The audit explicitly asserts `all(len(full_bundle_target_ids) == 1 for automatic rows)`, fungal compatibility, no contradictory reviewed decision, and these named regressions. Authors are preserved diagnostically. No automatic candidate uses authorship/classification to choose among full-release alternatives.

## Implemented in this first bounded pass

`national_source.py:_normalize_into` now preserves raw DwC `nomenclaturalStatus` as `nomenclatural_status`, independent of profile optional-term mappings. No status interpretation, identity-field changes or profile rewriting is involved. Missing fields are empty strings; raw spelling/whitespace survives. Fixture coverage proves verbatim warnings and independent taxonomic status. The actual pinned NorTaxa archive was re-normalized into `/tmp`, and the simulation asserts the normalized warning equals the raw archive value.

The audit program is revised; compiler enrichment, bridge policy, aliases, source usages, supersessions, registry, release manifest and production are untouched. This report is the requested checkpoint **before compiler enrichment implementation**.

## Required next implementation points

1. Add the revised metadata-only association rule inside `compile_release.py`'s vernacular projection. Use existing normalized record `.raw['nomenclatural_status']`; do not extend or write identity bindings in `source_usages`, nor modify authoritative bridge eligibility. Retain original source linkage and attach only additional vernacular rows with target/rule/source/classification/author provenance. Preserve existing preferred rows. Reuse the audit's criteria, with fixtures for full-universe ambiguity (including Gerronema), nonfungal homonyms, qualified names, nomenclatural warnings, reviewed conflicts and species classification warnings.
2. Archive the fingerprinted compiler `manifest.json`, `vernacular.jsonl` and enrichment evidence output when the bundle is promoted/published. The owning local promotion path is `scripts/promote_desktop_bundle.py:promote`, called by `scripts/build_release.py --promote`; it currently writes gzip and compact manifest and retains only the compiler manifest digest, not its source artifacts. Add a deterministic evidence archive, verify each member against compiler output hashes before copying, and add archive name/hash/size to the publication manifest. The archive must be retained with the release, including later releases; do not rely on temporary build directories. The compiler's enrichment evidence output must be hashed in its existing manifest.
3. `.github/workflows/release.yml` currently uploads only platform installers. Ensure the evidence archive is included in the relevant release-publication assets and/or the committed taxonomy release artifact set, with a guard that the archived compiler digest equals the bundle digest. Keep this lifecycle work separate from vernacular/identity behavior. No installer, publication or release build was run in this pass.

A compact DB/cloud schema change remains unnecessary: the detailed association is durable in the release evidence archive, while runtime rows keep the existing `source` projection. Evidence archiving has been located/designed but not implemented at this pre-enrichment checkpoint.

## Verification

- **46 national-source adapter tests passed**, including raw warning preservation and missing-term behavior.
- **21 NorTaxa adapter/compiler tests passed**; compiler is unchanged and consumes the additive raw normalized record.
- Python syntax checks and `git diff --check` passed.
- Revised simulation completed against pinned archives/bundle and saved live snapshot, asserting full-release uniqueness and all named regressions. Repeat execution produced byte-identical summary and every generated association/exception manifest.
- Source/compiler/simulation/normalized-input fingerprints and the projected evidence are in `summary.json` and adjacent manifests.

No release artifact was created and no production was touched. Current stage is self-verified normalization plus revised simulation; compiler enrichment and publication evidence retention are the next bounded work.
