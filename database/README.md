# Database and taxonomy reference-data workflow

This folder contains both the application database code and the reference-data build scripts used to create the local multilingual taxonomy database.

The taxonomy pipeline is deliberately split into two kinds of work:

- **offline rebuilds**, which combine already-downloaded source files into the SQLite database used by the app
- **explicit refresh/enrichment steps**, which contact external services and update cached CSV files

A normal rebuild must not call iNaturalist, Artportalen, GBIF, Artsdatabanken APIs, or Supabase. This keeps the generated database reproducible and prevents slow accidental network refreshes.

## Active taxonomy scripts

### `update_inat_common_names.py`

Refreshes the cached iNaturalist common-name CSV.

Default input:

```text
reference_data/sources/taxon.txt
```

Default output:

```text
reference_data/generated/vernacular_inat_11lang.csv
```

Output columns:

```text
scientificName,inaturalist_taxon_id,en,de,fr,es,da,sv,no,fi,pl,pt,it
```

By default this processes all accepted species in Nortaxa, not only fungi. This is intentional because desktop fields such as “Grows on” may need plant, animal, moss, lichen, and other species names.

Use `--fungi-only` or specific filters when you want a smaller cache for testing.

Examples:

```bash
# Resume from the existing CSV and fill missing rows.
python database/update_inat_common_names.py

# Start over and rebuild the full iNaturalist cache.
python database/update_inat_common_names.py --overwrite --request-delay 0.2

# Quick test run.
python database/update_inat_common_names.py --limit 100

# Fungal subset only.
python database/update_inat_common_names.py --fungi-only --overwrite

# Plants only, useful for testing “Grows on” data.
python database/update_inat_common_names.py --kingdom Plantae --limit 500
```

The script uses conservative matching. It stores an iNaturalist taxon ID only when it finds an exact scientific-name match or an exact matched term. It does not silently accept the first fuzzy result unless `--allow-fuzzy-match` is passed.

### `build_unified_multilang_taxonomy_db.py`

Builds the final multilingual taxonomy SQLite database.

Default inputs:

```text
reference_data/sources/taxon.txt
reference_data/sources/vernacularname.txt
reference_data/generated/vernacular_inat_11lang.csv
reference_data/generated/artportalen_taxon_ids_by_genus.csv
reference_data/generated/artportalen_taxon_ids_swedish_only_reconciled.csv
```

Default output:

```text
reference_data/generated/vernacular_multilanguage.sqlite3
```

This script is the final DB builder. It no longer depends on the older `build_multilang_vernacular_db.py` helper.

It stores:

- Artsdatabanken/Nortaxa taxon IDs as the local Norwegian backbone
- Norwegian and Sámi vernacular names from `vernacularname.txt`
- non-Norwegian common names from the cached iNaturalist CSV
- iNaturalist taxon IDs from `inaturalist_taxon_id`
- Artportalen Swedish taxon IDs where overlay CSVs are available
- scientific-name aliases and synonym mappings
- external IDs in `taxon_external_id_min`

Useful tables:

```text
taxon_min
vernacular_min
scientific_name_min
taxon_external_id_min
```

`taxon_min.inaturalist_taxon_id` is kept for fast app lookup. The same ID is also stored in `taxon_external_id_min` with `source_system = 'inaturalist'`.

### `rebuild_taxonomy_db.py`

Offline orchestrator for the normal rebuild.

Typical command:

```bash
python database/rebuild_taxonomy_db.py
```

This validates paths, calls `build_unified_multilang_taxonomy_db.py`, and runs sanity checks afterwards.

Use this when Artportalen overlay files are not available yet:

```bash
python database/rebuild_taxonomy_db.py --without-artportalen
```

Use this only if the iNaturalist CSV is intentionally absent:

```bash
python database/rebuild_taxonomy_db.py --allow-missing-inat
```

Preview the builder command without writing the DB:

```bash
python database/rebuild_taxonomy_db.py --dry-run
```

The rebuild script prints counts for key tables, language codes, Norwegian/Sámi vernacular rows, iNaturalist IDs, Swedish Artportalen IDs, and duplicate scientific names in the iNaturalist CSV.

## Source files

`reference_data/sources/` contains source files downloaded from Artsdatabanken/Nortaxa.

Expected files:

```text
reference_data/sources/taxon.txt
reference_data/sources/vernacularname.txt
```

`taxon.txt` is the source backbone for accepted names, synonyms, ranks, and classification.

`vernacularname.txt` is the authoritative source for Norwegian, Nynorsk, and Sámi names where present.

## Generated files

`reference_data/generated/` contains generated cache/build products.

Important generated files:

```text
reference_data/generated/vernacular_inat_11lang.csv
reference_data/generated/artportalen_taxon_ids_by_genus.csv
reference_data/generated/artportalen_taxon_ids_swedish_only.csv
reference_data/generated/artportalen_taxon_ids_swedish_only_reconciled.csv
reference_data/generated/vernacular_multilanguage.sqlite3
```

Do not edit generated files by hand unless you are deliberately debugging a one-off data issue.

## Artportalen Swedish ID overlay

The Artportalen scripts are optional enrichment scripts. They are not part of the normal offline rebuild unless their generated CSVs already exist.

Active scripts:

```text
fetch_artportalen_taxon_ids_by_genus.py
reconcile_artportalen_swedish_only.py
```

The genus-based fetcher is preferred because it queries Artportalen by genus and reconciles returned species back to the local Nortaxa backbone. The older one-taxon-at-a-time fetcher is superseded.

Generated outputs:

```text
reference_data/generated/artportalen_taxon_ids_by_genus.csv
reference_data/generated/artportalen_taxon_ids_swedish_only.csv
reference_data/generated/artportalen_taxon_ids_swedish_only_reconciled.csv
```

Swedish IDs are stored in `taxon_min.swedish_taxon_id` where there is a clean preferred match, and in `taxon_external_id_min` for external-ID tracking.

## iNaturalist refresh policy

The iNaturalist refresh is intentionally separate from the DB rebuild.

Run it only when you want to update cached multilingual names or fill missing iNaturalist IDs:

```bash
python database/update_inat_common_names.py
python database/rebuild_taxonomy_db.py
```

The CSV is designed to be long-lived. A full refresh can take a long time and should be treated as a cached source update, not as a normal build step.

## Name-source precedence

The final DB should treat source authority roughly like this:

- Artsdatabanken/Nortaxa is the canonical Norwegian taxonomy backbone.
- Artsdatabanken `vernacularname.txt` wins for Norwegian, Nynorsk, and Sámi vernacular names.
- iNaturalist supplies multilingual common names and iNaturalist taxon IDs.
- Artportalen supplies Swedish taxon IDs and Swedish overlay names when explicitly fetched.
- GBIF, if added later, should be external linking/search enrichment, not the canonical local taxonomy.

## Red-list and name-resolution note

Red-list categories should not be stored as a single column on `taxon_min`. The same species/name can have several red-list entries by year and area, for example Norway versus Svalbard.

If red-list extraction is added later, use a separate table such as `taxon_redlist_min` with one row per source/name/year/area/category.

Artsdatabanken API name resolution is also a separate runtime or enrichment feature, not part of the normal offline rebuild. It is useful for “Did you mean…” and synonym resolution, for example resolving a misspelled or old scientific name to an accepted name.

## Supabase upload

`import_taxa_to_supabase.py` is separate from the local rebuild flow.

Use it only after the local SQLite DB has been built and inspected. It requires a service-role key and should not be called by the normal taxonomy rebuild.

## Suggested active file set

For the taxonomy-name pipeline, the active files are:

```text
README.md
__init__.py
reference_data_paths.py
reference_data/
update_inat_common_names.py
rebuild_taxonomy_db.py
build_unified_multilang_taxonomy_db.py
fetch_artportalen_taxon_ids_by_genus.py
reconcile_artportalen_swedish_only.py
import_taxa_to_supabase.py        # optional, Supabase-only
taxon_lookup.py                   # runtime lookup code
vernacular_db.py                  # runtime DB access code
```

Other database infrastructure files such as `models.py`, `schema.py`, `migrate.py`, `sqlite_migrations/`, and app maintenance scripts are not part of the taxonomy-refresh pipeline but may still be needed by the app.

## Safe cleanup checklist

Before deleting old scripts:

```bash
python database/rebuild_taxonomy_db.py --without-artportalen
python database/rebuild_taxonomy_db.py --dry-run
python -m py_compile \
  database/update_inat_common_names.py \
  database/build_unified_multilang_taxonomy_db.py \
  database/rebuild_taxonomy_db.py
```

After the generated DB looks correct, archive superseded scripts first. Delete them only after a successful rebuild and a quick app smoke test.
