# Taxonomy identity and identifier contract

## Internal identity

`sporely_taxon_id` is an immutable positive integer allocated only by the
stable Sporely registry. It identifies a Sporely concept, is never derived from
an external identifier, and is never recycled. Name equality does not establish
identity.

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

## Mapping and continuity

Only `exact` mappings satisfying `mapping_policy.yml` may automatically share
a Sporely taxon ID. `likely_exact`, broader/narrower/overlapping, synonym, name-
only, fuzzy, homonym, split, and merge evidence requires the action and review
defined there. Approved manual mappings run before automation. Rejected
mappings remain stored so the same proposal is not repeatedly raised.

Confidence is a decimal from zero through one describing evidence strength. It
never changes a non-exact relationship into identity.
