"""A small, real taxonomy sample for taxon-picker tests and review screenshots.

``taxon_picker_sample.json`` holds rows copied verbatim from the bundled
release (``tax-2026.10.23-11``): the concepts, scientific names, Norwegian
and Swedish vernaculars and Artsdatabanken ids of a few genera that show
every picker case (COL/NorTaxa pairs, a pair whose families disagree,
same-source homonyms, synonyms, an aggregate, a large genus). It is a
deterministic, offline fixture: :func:`build_sample_db` writes it into a
throwaway SQLite file with the release's table layout.

Regenerate from a release database with::

    python -m tools.review_ui.taxon_sample <release.sqlite3>
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

SAMPLE_PATH = Path(__file__).with_name("taxon_picker_sample.json")
RELEASE = "tax-2026.10.23-11"

_TAXON_COLUMNS = (
    "taxon_id", "genus", "specific_epithet", "family", "canonical_scientific_name",
    "taxon_rank", "taxonomic_status", "canonical_source_system", "canonical_external_id",
)

# Whole genera, plus single names that the search examples reach.
_GENERA = ("Flammulina", "Cantharellus", "Aschersonia")
_NAMES = (
    "Russula", "Amanita muscaria", "Arrhenia velutipes", "Aureoboletus velutipes",
    "Conocybe velutipes", "Conocybe velutipes var. nitrophila", "Conocybe velutipes var. velutipes",
    "Cordyceps velutipes", "Cortinarius albo-ochraceus", "Hygrophorus russula",
    "Craterellus tubaeformis", "Cortinarius pulchripes", "Cortinarius albonigrellus",
)
# Russula: every species with a Norwegian name, and the first names in
# order, so a search for the genus needs "Show more".
_RUSSULA_FIRST = 60


def extract(release_db: Path) -> dict:
    conn = sqlite3.connect(f"file:{release_db}?mode=ro", uri=True)
    try:
        ids: set[int] = set()
        for genus in _GENERA:
            ids |= {r[0] for r in conn.execute("SELECT taxon_id FROM taxon_min WHERE genus = ?", (genus,))}
        for name in _NAMES:
            ids |= {r[0] for r in conn.execute(
                "SELECT taxon_id FROM taxon_min WHERE canonical_scientific_name = ?", (name,))}
        ids |= {r[0] for r in conn.execute(
            "SELECT DISTINCT t.taxon_id FROM taxon_min t JOIN vernacular_min v USING (taxon_id) "
            "WHERE t.genus = 'Russula' AND v.language_code = 'nb'")}
        ids |= {r[0] for r in conn.execute(
            "SELECT taxon_id FROM taxon_min WHERE genus = 'Russula' AND taxon_rank = 'species' "
            "ORDER BY canonical_scientific_name LIMIT ?", (_RUSSULA_FIRST,))}
        marks = ",".join("?" for _ in ids)
        order = sorted(ids)
        taxa = [list(r) for r in conn.execute(
            f"SELECT {', '.join(_TAXON_COLUMNS)} FROM taxon_min WHERE taxon_id IN ({marks}) ORDER BY taxon_id", order)]
        names = [list(r) for r in conn.execute(
            "SELECT taxon_id, scientific_name, is_preferred_name FROM scientific_name_min "
            f"WHERE language_code = 'sci' AND taxon_id IN ({marks}) ORDER BY taxon_id, scientific_name", order)]
        vernaculars = [list(r) for r in conn.execute(
            "SELECT taxon_id, language_code, vernacular_name, is_preferred_name FROM vernacular_min "
            f"WHERE language_code IN ('nb', 'nn', 'sv') AND taxon_id IN ({marks}) "
            "ORDER BY taxon_id, language_code, vernacular_name", order)]
        external = [list(r) for r in conn.execute(
            "SELECT taxon_id, external_id FROM taxon_external_id_min "
            f"WHERE source_system = 'artsdatabanken' AND id_role = 'accepted' AND taxon_id IN ({marks}) "
            "ORDER BY taxon_id, external_id", order)]
        release = conn.execute(
            "SELECT value FROM taxonomy_meta WHERE key = 'content_release_id'").fetchone()
    finally:
        conn.close()
    return {
        "release": release[0] if release else None,
        "taxon_columns": list(_TAXON_COLUMNS),
        "taxa": taxa,
        "scientific_names": names,
        "vernaculars": vernaculars,
        "artsdatabanken_ids": external,
    }


def build_sample_db(path: Path, sample_path: Path = SAMPLE_PATH) -> Path:
    """Write the sample into a SQLite file with the release's table layout."""
    sample = json.loads(sample_path.read_text(encoding="utf-8"))
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE taxonomy_meta (key TEXT PRIMARY KEY, value TEXT);
            CREATE TABLE taxon_min (
                taxon_id INTEGER PRIMARY KEY, parent_taxon_id INTEGER,
                genus TEXT NOT NULL, specific_epithet TEXT NOT NULL, family TEXT,
                canonical_scientific_name TEXT, taxon_rank TEXT, taxonomic_status TEXT,
                canonical_source_system TEXT NOT NULL, canonical_external_id TEXT NOT NULL,
                preferred_scientific_name_no TEXT, preferred_scientific_name_sv TEXT
            );
            CREATE INDEX idx_taxon_canonical_name ON taxon_min(canonical_scientific_name);
            CREATE TABLE scientific_name_min (
                scientific_name_id INTEGER PRIMARY KEY, taxon_id INTEGER NOT NULL,
                language_code TEXT NOT NULL, scientific_name TEXT NOT NULL,
                is_preferred_name INTEGER NOT NULL DEFAULT 0, source TEXT, note TEXT
            );
            CREATE INDEX idx_scientific_name_lookup ON scientific_name_min(language_code, scientific_name);
            CREATE TABLE vernacular_min (
                vernacular_id INTEGER PRIMARY KEY, taxon_id INTEGER NOT NULL,
                language_code TEXT NOT NULL, vernacular_name TEXT NOT NULL,
                is_preferred_name INTEGER NOT NULL DEFAULT 0, source TEXT
            );
            CREATE INDEX idx_vern_lang_name ON vernacular_min(language_code, vernacular_name);
            CREATE TABLE taxon_external_id_min (
                external_id_row_id INTEGER PRIMARY KEY, taxon_id INTEGER NOT NULL,
                source_system TEXT NOT NULL, external_id INTEGER NOT NULL, id_role TEXT NOT NULL,
                is_preferred INTEGER NOT NULL DEFAULT 0, external_name TEXT, note TEXT
            );
            """
        )
        conn.execute("INSERT INTO taxonomy_meta VALUES ('content_release_id', ?)", (sample["release"],))
        columns = sample["taxon_columns"]
        conn.executemany(
            f"INSERT INTO taxon_min ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})",
            [[v if v is not None or c not in ("genus", "specific_epithet", "canonical_external_id") else ""
              for c, v in zip(columns, row)] for row in sample["taxa"]],
        )
        conn.executemany(
            "INSERT INTO scientific_name_min (taxon_id, language_code, scientific_name, is_preferred_name) "
            "VALUES (?, 'sci', ?, ?)", sample["scientific_names"])
        conn.executemany(
            "INSERT INTO vernacular_min (taxon_id, language_code, vernacular_name, is_preferred_name) "
            "VALUES (?, ?, ?, ?)", sample["vernaculars"])
        conn.executemany(
            "INSERT INTO taxon_external_id_min (taxon_id, source_system, external_id, id_role, is_preferred) "
            "VALUES (?, 'artsdatabanken', ?, 'accepted', 1)", sample["artsdatabanken_ids"])
    return path


def sample_lookup(path: Path, language: str = "nb"):
    """A :class:`TaxonLookupService` over a freshly built sample database."""
    from database.taxon_lookup import TaxonLookupService
    from database.vernacular_db import VernacularDB

    build_sample_db(path)
    db = VernacularDB(path, language_code=language, display_language_code="en")
    return TaxonLookupService(db, language_code=language, include_reference_data=False)


if __name__ == "__main__":
    data = extract(Path(sys.argv[1]))
    if data["release"] != RELEASE:
        raise SystemExit(f"expected {RELEASE}, got {data['release']}")
    SAMPLE_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=0) + "\n", encoding="utf-8")
    print(f"{len(data['taxa'])} taxa, {len(data['scientific_names'])} names -> {SAMPLE_PATH}")
