from __future__ import annotations

import sqlite3
import subprocess
import sys
from pathlib import Path

from database.vernacular_db import VernacularDB


def _seed_vernacular_db(
    db_path: Path,
    *,
    with_language_code: bool,
    with_scientific_name_table: bool,
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE taxon_min (
                taxon_id INTEGER PRIMARY KEY,
                genus TEXT,
                specific_epithet TEXT,
                family TEXT,
                canonical_scientific_name TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE vernacular_min (
                taxon_id INTEGER,
                vernacular_name TEXT,
                is_preferred_name INTEGER
                {language_column}
            )
            """.format(
                language_column=", language_code TEXT" if with_language_code else "",
            )
        )
        if with_scientific_name_table:
            conn.execute(
                """
                CREATE TABLE scientific_name_min (
                    taxon_id INTEGER,
                    scientific_name TEXT,
                    is_preferred_name INTEGER
                )
                """
            )

        conn.executemany(
            """
            INSERT INTO taxon_min (taxon_id, genus, specific_epithet, family, canonical_scientific_name)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (1, "Agaricus", "bisporus", "Agaricaceae", "Agaricus bisporus"),
                (2, "Amanita", "muscaria", "Amanitaceae", "Amanita muscaria"),
            ],
        )
        conn.executemany(
            """
            INSERT INTO vernacular_min (taxon_id, vernacular_name, is_preferred_name{language_column})
            VALUES (?, ?, ?{language_values})
            """.format(
                language_column=", language_code" if with_language_code else "",
                language_values=", ?" if with_language_code else "",
            ),
            [
                (1, "Button mushroom", 1, "en") if with_language_code else (1, "Button mushroom", 1),
                (1, "Sjampinjong", 1, "no") if with_language_code else (1, "Sjampinjong", 1),
                (2, "Fly agaric", 1, "en") if with_language_code else (2, "Fly agaric", 1),
                (2, "Rød fluesopp", 1, "no") if with_language_code else (2, "Rød fluesopp", 1),
            ],
        )
        if with_scientific_name_table:
            conn.executemany(
                """
                INSERT INTO scientific_name_min (taxon_id, scientific_name, is_preferred_name)
                VALUES (?, ?, ?)
                """,
                [
                    (1, "Agaricus bisporus", 1),
                    (2, "Amanita muscaria", 1),
                ],
            )


def test_importing_vernacular_db_does_not_import_ui_modules() -> None:
    script = (
        "import sys\n"
        "from database.vernacular_db import VernacularDB\n"
        "assert 'ui.observations_tab' not in sys.modules\n"
        "assert 'ui.cloud_reference_dialog' not in sys.modules\n"
        "print(VernacularDB.__module__)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == "database.vernacular_db"


def test_lookup_helpers_work_without_language_column_or_scientific_table(tmp_path: Path) -> None:
    db_path = tmp_path / "vernacular-basic.sqlite"
    _seed_vernacular_db(
        db_path,
        with_language_code=False,
        with_scientific_name_table=False,
    )

    helper = VernacularDB(db_path, language_code="en")

    assert helper.list_languages() == []
    assert helper.suggest_genus("ag") == ["Agaricus"]
    assert helper.suggest_species("Agaricus", "bi") == ["bisporus"]
    assert helper.taxon_from_scientific("AGARICUS", "BISPORUS") == (
        "Agaricus",
        "bisporus",
        "Agaricaceae",
    )
    assert helper.taxon_from_vernacular("Button mushroom") == (
        "Agaricus",
        "bisporus",
        "Agaricaceae",
    )
    assert helper.vernacular_from_taxon("Agaricus", "bisporus") == "Button mushroom"


def test_language_code_filtering_uses_language_column_when_present(tmp_path: Path) -> None:
    db_path = tmp_path / "vernacular-language.sqlite"
    _seed_vernacular_db(
        db_path,
        with_language_code=True,
        with_scientific_name_table=True,
    )

    helper = VernacularDB(db_path, language_code="nb")

    assert helper.list_languages() == ["en", "no"]
    assert helper.suggest_genus("ag") == ["Agaricus"]
    assert helper.suggest_species("Agaricus", "bi") == ["bisporus"]
    assert helper.suggest_vernacular("sja", genus="Agaricus", species="bisporus") == ["Sjampinjong"]
    assert helper.suggest_vernacular_entries("sja", genus="Agaricus", species="bisporus") == [
        {
            "vernacular_name": "Sjampinjong",
            "genus": "Agaricus",
            "species": "bisporus",
            "family": "Agaricaceae",
            "is_preferred_name": True,
        }
    ]
    assert helper.taxon_from_scientific("agaricus", "bisporus") == (
        "Agaricus",
        "bisporus",
        "Agaricaceae",
    )
    assert helper.taxon_from_vernacular("Sjampinjong") == (
        "Agaricus",
        "bisporus",
        "Agaricaceae",
    )
    assert helper.taxon_from_vernacular("Button mushroom") is None
    assert helper.vernacular_from_taxon("Agaricus", "bisporus") == "Sjampinjong"
