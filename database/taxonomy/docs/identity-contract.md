# Taxonomy identity and identifier contract

## Internal identity

`sporely_taxon_id` is an immutable positive integer allocated only by the
stable Sporely registry. It identifies a Sporely concept, is never derived from
an external identifier, and is never recycled. Name equality does not establish
identity.

Catalogue of Life XR remains the global identity and reconciliation backbone:
accepted concepts, synonym relationships, higher classification, stable source
identifiers, and pinned-release provenance. The complete source may remain an
immutable build artifact, but neither it nor the rejected W1 full-Fungi union is
loaded wholesale into production Supabase. NorTaxa, iNaturalist, Artsorakel,
and national Red Lists retain their source-specific roles and never replace COL
or determine global macrofungi scope.

For the pinned 2026-07-17 XR release, the biological labels
`Pucciniomycotina` and `Ustilaginomycotina` are not exposed as stable COL
concepts. Executable scope policy uses `Pucciniomycetes` (`H7`) and
`Ustilaginomycetes` (`K9`) as the approved class-level product boundaries.
`Gymnosporangium` (`4RXL`) and `Mycosarcoma maydis` (`B24TM`) are explicit
lower-rank exceptions. `Ustilago maydis` is retained as a pinned COL synonym of
accepted `B24TM`; it is not a separate identity and is not resolved by name
equality.

Every external identifier is stored as text with both `source` and `namespace`.
Mappings may be current, preferred, historical, superseded, rejected, or
unresolved. Historical usages remain traceable.

## Identifier namespaces

| Identifier | Source | Namespace | Object | Automatic identity |
|---|---|---|---|---|
| Sporely taxon ID | `sporely` | `sporely_taxon_id` | internal concept | authoritative |
| COL usage ID | `col_xr` | `col_usage_id` | source usage | only continuous pinned COL lineage |
| Darwin Core `id` | `nortaxa` | `nortaxa_dwc_id` | archive row | no |
| `taxonID` | `nortaxa` | `nortaxa_taxon_id` | source-defined taxon | no |
| `acceptedNameUsageID` | `nortaxa` | `nortaxa_accepted_name_usage_id` | accepted-usage reference | no |
| `parentNameUsageID` | `nortaxa` | `nortaxa_parent_name_usage_id` | parent-usage reference | no |
| Artsorakel `NBIC:` ID | `artsorakel` | `nbic_scientific_name_id` | scientific name | no |
| Artsnavnebase scientific-name ID | `artsdatabanken` | `artsnavnebase_scientific_name_id` | scientific name | no |
| Artsdatabanken taxon-concept ID | `artsdatabanken` | `artsdatabanken_taxon_concept_id` | taxon concept | no |
| Artportalen ID | `artportalen` | `artportalen_taxon_id` | source taxon; concept precision not yet assumed | no |
| iNaturalist ID | `inaturalist` | `inaturalist_taxon_id` | source taxon | no |
| legacy GBIF ID | `gbif` | `gbif_taxon_key` | source usage/concept | no |
| MycoBank ID | `mycobank` | `mycobank_name_id` | scientific name | no |
| Index Fungorum ID | `index_fungorum` | `index_fungorum_name_id` | scientific name | no |

The NorTaxa fields are distinct even when a particular row repeats the same
digits. Stripping `NBIC:` does not convert its scientific-name identifier into
a NorTaxa row ID, NorTaxa `taxonID`, or Sporely ID. `NBIC:54995` must be retained
as the raw external value; any match on its numeric component is valid only
under an explicit, evidenced namespace bridge.

### Artsnavnebase name-ID vs Artsdatabanken taxon-concept-ID

Artsdatabanken maintains two independent identifier registries and Sporely
must never conflate them:

* `artsnavnebase_scientific_name_id` — the Artsnavnebase name registry.
  Every distinct scientific name (with its authorship) has one. This is what
  the Norwegian Red List workbook publishes under the column
  `Vitenskapelig navn id`, and what Artsorakel returns under its `NBIC:`
  prefix.
* `artsdatabanken_taxon_concept_id` — Artsdatabanken's internal concept
  registry. Distinct from the name id even for a single scientific name
  (e.g. *Vulpes vulpes* has name-id `48034` and concept-id `31176`;
  *Cladonia chlorophaea* has name-id `69071` and concept-id `45044`).

The NorTaxa Darwin Core archive (dataset `artsnavnebase`) publishes the
Artsnavnebase name registry: its `dwc:taxonID` column values ARE
Artsnavnebase scientific-name IDs. See
`national_sources/nortaxa/<release>/source.json.identifier_namespace_semantics`
for the machine-readable declaration. Because of this, values under the
DwC-technical namespace `nortaxa_taxon_id` may be bridged to the
semantically-correct namespace `artsnavnebase_scientific_name_id` — but
never to `artsdatabanken_taxon_concept_id`. Numeric equality across the
two Artsdatabanken registries is coincidence, not identity.

## Mapping and continuity

Only `exact` mappings satisfying `mapping_policy.yml` may automatically share
a Sporely taxon ID. `likely_exact`, broader/narrower/overlapping, synonym, name-
only, fuzzy, homonym, split, and merge evidence requires the action and review
defined there. Approved manual mappings run before automation. Rejected
mappings remain stored so the same proposal is not repeatedly raised.

Confidence is a decimal from zero through one describing evidence strength. It
never changes a non-exact relationship into identity.
