# Database code and reference data

This folder holds the application database code (`models.py`, `schema.py`,
`migrate.py`, `sqlite_migrations/`, reference-library modules) and the build
scripts for the reference data the app ships with.

- **Species database (taxonomy v2)** — the current one. Plain-language
  overview: [docs/taxonomy-database.md](../docs/taxonomy-database.md).
  Build and release: [taxonomy/README.md](taxonomy/README.md).
- **Legacy names database** — described below. Still shipped, still needed.
- **Other reference data** — described at the end.

Before changing the SQLite schema, read
[sqlite_migrations/README.md](sqlite_migrations/README.md).

## Reference data layout

```
reference_data/
  sources/     inputs (gitignored): taxon.txt, vernacularname.txt, Parmasto tables, …
  generated/   build outputs; only the files the app bundles are tracked
```

Tracked, bundled files in `reference_data/generated/`:

| File | Used for |
|---|---|
| `taxonomy_v2/manifest.json`, `taxonomy_v2/<release>.sqlite3.gz` | Species database (taxonomy v2) |
| `vernacular_multilanguage.sqlite3` | Legacy names database |
| `reference_values.db` | Bundled spore reference values |
| `artportalen_biotopes_tree.json`, `artportalen_substrate_tree.json`, `nin2_biotopes_tree.json`, `substrate_tree.json` | Habitat and substrate pickers |

Everything else under `generated/` is a local intermediate and is gitignored.
Do not edit generated files by hand.

## Legacy names database

`reference_data/generated/vernacular_multilanguage.sqlite3` is the database
Sporely used before taxonomy v2. It is keyed by NorTaxa IDs and holds
Norwegian and Sámi names, Swedish names and Artportalen IDs, and
iNaturalist names in 11 languages plus iNaturalist IDs.

It is still needed for three reasons:

1. It is the fallback when taxonomy v2 is switched off or fails to install.
2. The vernacular-language list is still read from it.
3. It is the only source of Swedish and iNaturalist-language names and of
   Artportalen and iNaturalist IDs. Current v2 releases do not carry them; see
   [Legacy enrichment](taxonomy/README.md#legacy-enrichment).

Retire it only after a v2 release carries that data and the fallback is
removed (plan: `docs/plans/completed/2026-07-23-taxonomy-v2-integration.md`,
section 18).

### Rebuilding it

Rebuilds are offline. Network refreshes are separate, explicit steps.

```bash
# Optional network refresh: iNaturalist names and IDs (long-running, resumable)
python database/update_inat_common_names.py

# Optional network refresh: Swedish Artportalen IDs (needs Artportalen cookies)
python database/fetch_artportalen_taxon_ids_by_genus.py --cookie-json path/to/artportalen_cookies.json
python database/reconcile_artportalen_swedish_only.py

# Offline rebuild of vernacular_multilanguage.sqlite3 with sanity checks
python database/rebuild_taxonomy_db.py            # --without-artportalen, --allow-missing-inat, --dry-run
```

| Script | Role |
|---|---|
| `update_inat_common_names.py` | Refreshes `generated/vernacular_inat_11lang.csv` from iNaturalist. Exact matches only unless `--allow-fuzzy-match`. |
| `fetch_artportalen_taxon_ids_by_genus.py`, `reconcile_artportalen_swedish_only.py` | Fetch Swedish Artportalen taxa by genus and reconcile them to the NorTaxa backbone. |
| `fetch_artportalen_taxon_ids.py` | Older per-taxon fetcher; its picker parser is still used by the Artsobservasjoner upload. |
| `build_unified_multilang_taxonomy_db.py` | Builds the database from the sources and cached CSVs. |
| `rebuild_taxonomy_db.py` | Orchestrates the offline rebuild and prints counts. |
| `import_taxa_to_supabase.py` | Uploads the legacy cloud `taxa` table (service-role key). Legacy only; the cloud now uses taxonomy v2. |

## Other reference data

```bash
python database/merge_parmasto_reference_data.py   # Parmasto tables -> reference_values.db
python database/build_artportalen_habitat_trees.py # habitat/substrate trees
python database/fetch_artportalen_habitat_trees.py
python database/artsobs_get_naturtyper.py
python database/artsobs_get_livsmedium.py
```
