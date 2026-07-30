# Consumer compatibility contract and current audit

## Required compatibility metadata

All consumers declare minimum and maximum supported taxonomy schema versions
and the content release used for testing. `sporely-py` also declares the bundled
SQLite SHA-256. `sporely-web` records the active cloud release tested.
`sporely-landing` must declare whether it consumes taxonomy; if it begins doing
so, it adopts the same schema-range and tested-release metadata.

No runtime compatibility implementation is part of Stage 1.

## Current `sporely-py` assumptions

- The bundled SQLite uses integer `taxon_min.taxon_id`, currently derived from
  NorTaxa Darwin Core `id`.
- Runtime lookup expects `taxon_min`, `vernacular_min`,
  `scientific_name_min`, and optionally `taxon_external_id_min`.
- Models prefer `norwegian_taxon_id` and integer external-ID columns where
  present. Existing observations store genus/species/name snapshots rather than
  a taxonomy-v2 usage identity.
- Future migration must preserve historical selected names and cannot reinterpret
  existing integers as new Sporely IDs without an explicit mapping.

## Current `sporely-web` assumptions

- `search_taxa(q, lang, lim)` returns integer `taxon_id`, genus,
  `specific_epithet`, `canonical_scientific_name`, family, one vernacular name,
  integer Norwegian/Swedish/iNaturalist/Artportalen IDs, and `match_type`.
- `artsorakel.js` maps the RPC integer `taxon_id` to `taxonId`, while AI results
  use the same property for strings such as `NBIC:54995`. Persisted
  `ai_selected_taxon_id` is text and therefore currently mixes namespaces.
- Prediction caches/deduplication use `taxonId` or scientific name. Stored AI
  results and observation rows preserve `ai_selected_scientific_name`; these
  fields are compatibility-sensitive identification snapshots.
- Stored-identification normalization can strip namespace prefixes from
  auxiliary external-ID fields. For Artsorakel, the raw `NBIC:` value is
  currently retained in the mixed-purpose text `taxonId`/`ai_selected_taxon_id`
  field and is deliberately not duplicated into the auxiliary `nbic` field.
  Taxonomy v2 must migrate this to explicit source/namespace storage without
  interpreting the numeric suffix as NorTaxa or Sporely identity.
- The stable RPC must retain existing fields for old clients while later adding
  explicit internal ID, match type/name, and namespaced external identifiers.
  Cache invalidation will require taxonomy content release ID.
- Stage 1 makes no consumer code or RPC change.

## Current `sporely-landing` assumptions

The inspected landing code does not currently call `search_taxa`. Its public
explorer audit derives scientific identity from observation genus/species and
notes that such slugs break on renames. If it consumes taxonomy later, it must
use stable Sporely identity rather than a genus/species slug. Its existing
`taxonId?: string` planning type is not a defined taxonomy namespace.

## Backward-compatibility floor

Before a breaking activation, old clients must still receive the current RPC
fields and observations must retain their stored source identifier/name
snapshots. A future field named `taxon_id` means Sporely internal identity only;
source identifiers require explicit source and namespace fields.
