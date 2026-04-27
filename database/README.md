# Database Notes

## Taxonomy rebuild pipeline

The main taxonomy DB used by Sporely is:

- `vernacular_multilanguage.sqlite3`

The full rebuild can now be run with:

```bash
python database/rebuild_taxonomy.py --cookie-json path/to/artportalen_cookies.json --overwrite
```

This pipeline runs:

1. `inat_common_names_from_taxon.py`
   Builds `vernacular_inat_11lang.csv` with multilingual iNaturalist common names and `inaturalist_taxon_id`.
2. `build_multilang_vernacular_db.py`
   Builds a temporary base DB from `taxon.txt`, `vernacularname.txt`, and the iNaturalist CSV.
3. `fetch_artportalen_taxon_ids_by_genus.py`
   Fetches Swedish Artportalen taxa and writes matched plus Swedish-only CSVs.
4. `reconcile_artportalen_swedish_only.py`
   Resolves Swedish-only rows against `taxon.txt`, including synonym cases.
5. `build_unified_multilang_taxonomy_db.py`
   Builds the final unified taxonomy DB used by the app.

## Unified taxonomy DB

The unified output keeps:

- `taxon_min`
- `vernacular_min`

and adds:

- `norwegian_taxon_id`
- `swedish_taxon_id`
- `inaturalist_taxon_id`
- `scientific_name_min`
- `taxon_external_id_min`

This lets one local accepted taxon row keep:

- Norwegian backbone identity
- Norwegian and Swedish common names
- scientific aliases and synonyms
- Artportalen taxon IDs
- iNaturalist taxon IDs

Swedish names from Artportalen are treated as preferred where appropriate, while older names can remain as secondary rows.

## Partial rebuilds

Reuse existing intermediate files:

```bash
python database/rebuild_taxonomy.py \
  --skip-inat \
  --skip-artportalen-fetch \
  --skip-artportalen-reconcile
```

Refresh only the Swedish Artportalen side:

```bash
python database/rebuild_taxonomy.py --skip-inat --cookie-json path/to/artportalen_cookies.json --overwrite
```

## Reporting targets

Observation records carry an explicit `publish_target`, currently:

- `artsobs_no`
- `artportalen_se`

Target-specific upload IDs are stored separately from the local taxonomy DB.

## Supabase Cloud Migrations

This folder also contains `.sql` migration files for the Sporely Cloud (Supabase) PostgreSQL database. These are meant to be executed manually in the Supabase SQL Editor to keep the cloud schema in sync with the desktop application:

- `supabase_r2_media_migration.sql` — Adds `image_key` and `thumb_key` columns to support Cloudflare R2 media hosting, and normalizes legacy storage paths.
- `supabase_observation_images_ai_crop.sql` — Adds AI crop geometry columns (`ai_crop_x1`, etc.) to the `observation_images` table so crops sync across platforms.
- `supabase_spore_measurements_sync.sql` — Prepares the `spore_measurements` table to receive synced measurements from the desktop.
- `supabase_people_directory.sql` — Adds the `search_people_directory` RPC used by the web People screen for privacy-aware public contributor stats.
- `supabase_unique_constraints.sql` — Adds `UNIQUE (desktop_id, user_id)` constraints to ensure high-performance upserts during desktop-to-cloud sync.
