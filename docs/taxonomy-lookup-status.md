# Taxonomy Lookup Status

Audit date: 2026-05-28

Scope: the local taxonomy SQLite DB, `TaxonLookupService`, and `VernacularDB`.

## Short Summary

Sporely now has a rebuilt local taxonomy database that uses Artsdatabanken/Nortaxa taxon IDs as the local canonical anchor and layers in iNaturalist and Artportalen metadata on top. The lookup layer already supports genus suggestions, species suggestions, vernacular suggestions, and scientific-name/synonym resolution, but it still does not expose external IDs as first-class lookup results.

The current lookup stack is intentionally local-first:

- `taxon_id` is the local anchor and is the Artsdatabanken/Nortaxa taxon ID for accepted Norwegian taxa.
- iNaturalist and Artportalen IDs are stored in SQLite, but they are not yet surfaced through `TaxonChoice`.
- red-list data is not bulk-loaded into the generated DB; it should stay as on-demand metadata fetched after taxon selection and cached later.
- Artsdatabanken `/Api/Taxon/ScientificName/Suggest` and `/Api/Taxon/ScientificName?scientificname=...` should stay reserved for "did you mean", synonym resolution, and accepted-name resolution, not normal local autocomplete.

## Current Generated Inputs

The final DB is rebuilt from these already-downloaded files:

- `database/reference_data/sources/taxon.txt`
- `database/reference_data/sources/vernacularname.txt`
- `database/reference_data/generated/vernacular_inat_11lang.csv`
- `database/reference_data/generated/artportalen_taxon_ids_by_genus.csv`
- `database/reference_data/generated/artportalen_taxon_ids_swedish_only_reconciled.csv`

The only live network step in the taxonomy pipeline is the iNaturalist refresh script:

- `database/update_inat_common_names.py`

The rebuild itself is otherwise local/offline.

## Generated Schema

The generated DB at `database/reference_data/generated/vernacular_multilanguage.sqlite3` currently contains these tables:

- `taxon_min`
- `vernacular_min`
- `scientific_name_min`
- `taxon_external_id_min`
- `sqlite_sequence` from SQLite

Key columns:

- `taxon_min`: `taxon_id`, `parent_taxon_id`, `genus`, `specific_epithet`, `family`, `norwegian_taxon_id`, `swedish_taxon_id`, `inaturalist_taxon_id`, `canonical_scientific_name`, `taxon_rank`, `taxonomic_status`, `source_system`, `preferred_scientific_name_no`, `preferred_scientific_name_sv`
- `vernacular_min`: `vernacular_id`, `taxon_id`, `language_code`, `vernacular_name`, `is_preferred_name`, `source`
- `scientific_name_min`: `scientific_name_id`, `taxon_id`, `language_code`, `scientific_name`, `is_preferred_name`, `source`, `note`
- `taxon_external_id_min`: `external_id_row_id`, `taxon_id`, `source_system`, `external_id`, `id_role`, `is_preferred`, `external_name`, `note`

Important index behavior:

- `taxon_min.inaturalist_taxon_id` has a non-unique partial index.
- `taxon_min.swedish_taxon_id` and `taxon_min.norwegian_taxon_id` are unique where present.
- `scientific_name_min` is unique only on `(taxon_id, language_code, scientific_name)`.
- `taxon_external_id_min` is unique only on `(source_system, external_id, taxon_id)`.

Audit snapshot from the current generated DB:

| Metric | Value |
| --- | ---: |
| `taxon_min` rows | 112,777 |
| `vernacular_min` rows | 98,485 |
| `scientific_name_min` rows | 293,458 |
| `taxon_external_id_min` rows | 140,378 |
| `taxon_min` rows with iNaturalist ID | 13,044 |
| `taxon_min` rows with Swedish ID | 19,011 |
| duplicate iNaturalist ID groups | 191 |
| case-insensitive duplicates within the same taxon/language/source | 0 |

Cross-taxon vernacular collisions still exist, which is expected. The builder removes same-taxon case-insensitive duplicates, but it does not try to eliminate legitimate same-name collisions across different taxa.

## What Already Works

### Genus suggestions

`TaxonLookupService.suggest_genera()` already works and merges:

- local genus values from `taxon_min`
- genus names inferred from `scientific_name_min`
- optional reference-data genus suggestions when a reference provider is available

### Species suggestions

`TaxonLookupService.suggest_species()` already works and returns `TaxonChoice` rows.

It merges local and reference species lists, then resolves local taxa through the scientific-name lookup path. The `source` field can currently be `taxonomy`, `reference`, or `both`.

### Vernacular suggestions

`TaxonLookupService.suggest_common_names()` and the `VernacularDB` common-name helpers (`suggest_vernacular()`, `suggest_vernacular_entries()`, and `suggest_vernacular_for_taxon()`) already work for common-name lookup. They are language-aware when `language_code` is set.

### Scientific-name resolution

`TaxonLookupService.resolve_scientific()` and `VernacularDB.taxon_from_scientific()` already support accepted-name lookup and synonym/alias lookup through `scientific_name_min`.

### Synonym and alias lookup

The rebuilt DB already stores scientific aliases and synonyms in `scientific_name_min`, and the lookup code consults that table for both genus/species suggestion and scientific resolution.

Concrete probe result:

- `Boletus pini` resolved to accepted `Phellinus pini` through the synonym table.

### Language filtering

`VernacularDB` has language filtering and `TaxonLookupService.language_code` forwards to it. The current code path respects the selected vernacular language when querying common names.

### TaxonChoice fields

`TaxonChoice` currently exposes:

- `genus`
- `species`
- `common_name`
- `family`
- `source`
- `taxon_id`
- `language_code`
- `red_list_category`
- `red_list_source`

The red-list fields are placeholders today and are always `None` in the current lookup code.

## Probe Highlights

These are the most useful runtime observations from the current DB:

- `Amanita muscaria` exists both as the accepted Nortaxa row and as Artportalen-only split concepts (`s.str.` and `s.lat.`).
- `VernacularDB.taxon_from_scientific("Amanita", "muscaria")` returns the accepted Norwegian row (`taxon_id=52147`).
- `TaxonLookupService.resolve_scientific("Amanita", "muscaria")` currently returns a negative Artportalen-only local row (`taxon_id=-236537`) because the lookup query does not yet prefer the accepted backbone row when duplicate binomials exist.
- Swedish exact vernacular lookup is case-sensitive. `Röd flugsvamp` resolves, but `röd flugsvamp` and `RÖD FLUGSVAMP` do not when the service is queried fresh.
- `Betula`, `Picea`, and `Sphagnum` species suggestions all work and return local taxon matches.
- A taxon with no iNaturalist ID still resolves normally, and a taxon with no Swedish ID still resolves normally. The lookup result just lacks those external IDs because they are not yet exposed in `TaxonChoice`.

## Known Gaps

- `TaxonChoice` does not yet carry external IDs.
- There is no lookup method yet for `inaturalist_taxon_id` or `swedish_taxon_id` / `artportalen_taxon_id`.
- `resolve_scientific()` and `suggest_species()` can surface a negative Artportalen-only row when the same binomial exists in both the accepted backbone and an Artportalen-only split concept.
- `best_common_name_for_taxon()` can return `None` when more than one preferred name is present.
- Exact vernacular resolution is still literal/case-sensitive.
- Red-list status is not yet fetched on demand or cached after taxon selection.
- The service does not yet expose a rich external-link bundle for the selected taxon.

## Known Risks

- iNaturalist taxon IDs are not globally unique in the source data. One iNat ID can map to zero, one, or many local taxa, so any future API must return lists.
- Swedish Artportalen IDs can also map to more than one local concept, especially when an accepted taxon and an Artportalen-only split concept share the same scientific name.
- The current lookup code does not yet have an explicit accepted-vs-duplicate tie-breaker for shared binomials.
- If we rely on literal vernacular equality too early in the UI, we will miss case variants that users reasonably expect to resolve.

## Recommended Source Priority

- Norwegian: Artsdatabanken / Nortaxa > iNaturalist
- Swedish: Artportalen > iNaturalist
- Other languages: iNaturalist

This priority should stay encoded in the builder and in any future lookup tie-breakers.

## Future API / Service Shape

The next service layer should probably expose explicit list-returning lookup helpers:

```python
find_by_inaturalist_id(inat_id) -> list[TaxonChoice]
find_by_artportalen_id(artportalen_id) -> list[TaxonChoice]
resolve_scientific_name(name)
resolve_vernacular_name(name, language_code)
get_best_display_name(taxon_id, language_code)
get_external_links(taxon_id)
get_redlist_badge_async(taxon_id or scientific_name)
```

Notes:

- `find_by_inaturalist_id()` must return a list because one iNat ID can map to several local taxa.
- `find_by_artportalen_id()` should also return a list for the same reason.
- `get_redlist_badge_async()` should stay out of the generated DB for now and fetch live metadata on demand, with caching layered later.

## Open Decisions

- Should the lookup service expose external IDs directly in `TaxonChoice`, or should it return a separate richer match object?
- Should scientific-name resolution prefer accepted backbone rows over Artportalen-only split concepts when both share the same binomial?
- Should vernacular exact lookup stay literal, or should it normalize case and/or punctuation before matching?
- Where should red-list metadata be cached, and for how long?
- Should desktop and web share the same taxonomy lookup abstraction, or should the web side keep a narrower view model?
- Should the future `get_external_links()` method bundle Artsdatabanken, Artportalen, and iNaturalist URLs in a single structure or return source-specific entries?
