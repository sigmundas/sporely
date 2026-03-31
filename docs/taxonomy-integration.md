# Taxonomy integration

## Overview

Sporely uses a local multilingual taxonomy SQLite database for:

- species and common-name lookup in the observation dialog
- synonym-aware scientific-name resolution
- Norwegian and Swedish common-name support
- external taxon ID lookup for upload targets such as Artsobservasjoner and Artportalen

The main database file is:

- `database/vernacular_multilanguage.sqlite3`

## Current implementation

The local taxonomy database combines several sources:

- Norwegian backbone taxonomy from `taxon.txt`
- Norwegian vernacular names from `vernacularname.txt`
- multilingual common names from iNaturalist in `vernacular_inat_11lang.csv`
- Swedish scientific names and Swedish vernacular names from Artportalen CSVs

The unified DB keeps the core lookup tables:

- `taxon_min`
- `vernacular_min`

and adds:

- `scientific_name_min` for scientific aliases and synonyms
- `taxon_external_id_min` for external IDs by source system
- extra taxon columns such as Norwegian, Swedish, and iNaturalist taxon IDs

In practice this means one accepted local taxon can keep:

- the accepted Norwegian taxon row
- alternate scientific names and synonyms
- Norwegian and Swedish common names
- Artportalen taxon IDs
- iNaturalist taxon IDs

## How the app uses it

### Observation dialog

The observation taxonomy UI uses one visible mixed lookup field for species entry.

It can match:

- scientific names
- scientific synonyms
- common names in the selected vernacular language

`Genus` works as an optional filter. If genus is empty and the selected match is unambiguous, Sporely fills genus automatically.

Internally, the app still stores structured taxonomy fields separately:

- common name
- genus
- species

This keeps upload/export logic stable while making lookup easier for the user.

### Vernacular languages

The common-name language is chosen globally in app settings. The UI currently supports whatever languages are present in `vernacular_min`, with Norwegian and Swedish being the main Nordic targets.

### Upload targets

When an observation is uploaded, Sporely resolves external taxon IDs from the local taxonomy DB rather than from ad hoc web lookups.

Current target-specific resolution includes:

- Artsobservasjoner / Artsdatabanken taxon IDs
- Artportalen taxon IDs

## Rebuilding from scratch

For a full local rebuild, use:

```bash
python database/rebuild_taxonomy.py --cookie-json path/to/artportalen_cookies.json --overwrite
```

This pipeline now runs in order:

1. build the iNaturalist multilingual common-name CSV
2. build a temporary base taxonomy DB from Norwegian sources plus iNaturalist names
3. fetch Swedish Artportalen taxonomy matches by genus
4. reconcile Swedish-only taxa against `taxon.txt`
5. build the final unified taxonomy DB

That makes it possible to rebuild cleanly when:

- new names are added
- accepted names change
- synonyms change
- Swedish mappings need refreshing
- iNaturalist common names change

## Key scripts

- `database/inat_common_names_from_taxon.py`
  Builds `vernacular_inat_11lang.csv` and now also includes `inaturalist_taxon_id`.
- `database/build_multilang_vernacular_db.py`
  Builds a base accepted-species DB from Norwegian taxonomy plus multilingual vernacular names.
- `database/fetch_artportalen_taxon_ids_by_genus.py`
  Fetches Swedish Artportalen taxa using a genus-first strategy.
- `database/reconcile_artportalen_swedish_only.py`
  Reconciles Swedish-only rows against the Norwegian backbone and synonym relationships.
- `database/build_unified_multilang_taxonomy_db.py`
  Builds the final unified taxonomy DB used by the app.
- `database/rebuild_taxonomy.py`
  Runs the full pipeline above in one command.

## Updating existing taxonomy data

If you already have the intermediate CSVs and only want to rebuild the final DB, you can skip parts of the pipeline:

```bash
python database/rebuild_taxonomy.py \
  --skip-inat \
  --skip-artportalen-fetch \
  --skip-artportalen-reconcile
```

If you want to refresh only the Swedish Artportalen part, reuse the iNaturalist CSV but rerun the fetch/reconcile/build steps:

```bash
python database/rebuild_taxonomy.py --skip-inat --cookie-json path/to/artportalen_cookies.json --overwrite
```

## See also

- [Artsobservasjoner login and upload](./artsobservasjoner.md)
- [Spore measurements](./spore-measurements.md)
- [Database structure](./database-structure.md)
