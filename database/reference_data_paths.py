from __future__ import annotations

from pathlib import Path


REFERENCE_DATA_DIR = Path(__file__).resolve().with_name("reference_data")
REFERENCE_DATA_SOURCES_DIR = REFERENCE_DATA_DIR / "sources"
REFERENCE_DATA_GENERATED_DIR = REFERENCE_DATA_DIR / "generated"

BUNDLED_REFERENCE_DATABASE_PATH = REFERENCE_DATA_GENERATED_DIR / "reference_values.db"
BUNDLED_TAXON_FILE_PATH = REFERENCE_DATA_SOURCES_DIR / "taxon.txt"
BUNDLED_VERNACULAR_FILE_PATH = REFERENCE_DATA_SOURCES_DIR / "vernacularname.txt"
BUNDLED_INAT_VERNACULAR_CSV_PATH = REFERENCE_DATA_GENERATED_DIR / "vernacular_inat_11lang.csv"
BUNDLED_VERNACULAR_DB_PATH = REFERENCE_DATA_GENERATED_DIR / "vernacular_multilanguage.sqlite3"

# Taxonomy v2 (Stage 3B.2 dev-activation) — the raw SQLite is ~310 MB; the
# committed artifact is the deterministic gzip (~63 MB). The runtime installer
# extracts it into user-data on first activation.
TAXONOMY_V2_DIR = REFERENCE_DATA_GENERATED_DIR / "taxonomy_v2"
TAXONOMY_V2_GZ_PATH = TAXONOMY_V2_DIR / "tax-2026.07.29-01.sqlite3.gz"
TAXONOMY_V2_MANIFEST_PATH = TAXONOMY_V2_DIR / "manifest.json"
BUNDLED_ARTPORTALEN_BIOTOPES_PATH = REFERENCE_DATA_GENERATED_DIR / "artportalen_biotopes_tree.json"
BUNDLED_ARTPORTALEN_SUBSTRATE_PATH = REFERENCE_DATA_GENERATED_DIR / "artportalen_substrate_tree.json"
BUNDLED_NIN2_BIOTOPES_PATH = REFERENCE_DATA_GENERATED_DIR / "nin2_biotopes_tree.json"
BUNDLED_SUBSTRATE_TREE_PATH = REFERENCE_DATA_GENERATED_DIR / "substrate_tree.json"
