# Reference Data

This folder separates the source inputs for taxonomy and lookup generation from the generated artifacts consumed by the desktop app.

## Layout

- `sources/`
  - Raw or curated inputs such as `taxon.txt`, `vernacularname.txt`, `parmasto_table35.csv`, `parmasto_table36.csv`, `livsmedium.txt`, `natursystem.txt`, and `vernacular_scientific.csv`
- `generated/`
  - Build outputs such as `reference_values.db`, `vernacular_multilanguage.sqlite3`, `vernacular_multilanguage_unified.sqlite3`, `artportalen_*.csv`, and the habitat tree JSON files

## Rebuild

- Full taxonomy rebuild:

```bash
python database/rebuild_taxonomy.py --cookie-json path/to/artportalen_cookies.json --overwrite
```

- Parmasto reference rebuild:

```bash
python database/merge_parmasto_reference_data.py
```

- Habitat tree rebuilds:
  - `python database/build_artportalen_habitat_trees.py`
  - `python database/fetch_artportalen_habitat_trees.py`
  - `python database/artsobs_get_naturtyper.py`
  - `python database/artsobs_get_livsmedium.py`

## Notes

- `generated/` files are build artifacts unless the app explicitly bundles them.
- `source/` files should be treated as inputs, not as mutable runtime state.

