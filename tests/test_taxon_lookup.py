from __future__ import annotations

import sqlite3
import subprocess
import sys
from pathlib import Path

import database.models as models
from database.taxon_lookup import TaxonChoice, TaxonLookupService
from database.vernacular_db import VernacularDB


def _seed_local_taxonomy_db(db_path: Path) -> VernacularDB:
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
                is_preferred_name INTEGER,
                language_code TEXT
            )
            """
        )
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
                (3, "Entoloma", "sericeum", "Entolomataceae", "Entoloma sericeum"),
            ],
        )
        conn.executemany(
            """
            INSERT INTO vernacular_min (taxon_id, vernacular_name, is_preferred_name, language_code)
            VALUES (?, ?, ?, ?)
            """,
            [
                (1, "Button mushroom", 1, "en"),
                (1, "Cultivated mushroom", 0, "en"),
                (1, "Sjampinjong", 1, "no"),
                (2, "Fly agaric", 1, "en"),
                (2, "Rød fluesopp", 1, "no"),
                (3, "Silky entoloma", 1, "en"),
            ],
        )
        conn.executemany(
            """
            INSERT INTO scientific_name_min (taxon_id, scientific_name, is_preferred_name)
            VALUES (?, ?, ?)
            """,
            [
                (1, "Agaricus bisporus", 1),
                (2, "Amanita muscaria", 1),
                (3, "Entoloma sericeum", 1),
            ],
        )

    return VernacularDB(db_path, language_code="en")


def _seed_reference_db(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE reference_values (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                genus TEXT NOT NULL,
                species TEXT NOT NULL
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO reference_values (genus, species)
            VALUES (?, ?)
            """,
            [
                ("Agaricus", "bisporus"),
                ("Coprinus", "comatus"),
            ],
        )


def _patch_reference_connection(monkeypatch, ref_path: Path) -> None:
    monkeypatch.setattr(models, "get_reference_connection", lambda: sqlite3.connect(ref_path))


def _make_service(tmp_path: Path, monkeypatch) -> TaxonLookupService:
    local_db = tmp_path / "local-taxonomy.sqlite"
    ref_db = tmp_path / "reference-values.sqlite"
    vernacular_db = _seed_local_taxonomy_db(local_db)
    _seed_reference_db(ref_db)
    _patch_reference_connection(monkeypatch, ref_db)
    return TaxonLookupService(vernacular_db=vernacular_db, language_code="en")


def _assert_no_redlist(choice: TaxonChoice) -> None:
    assert choice.red_list_category is None
    assert choice.red_list_source is None


def test_importing_taxon_lookup_does_not_import_ui_modules() -> None:
    script = (
        "import sys\n"
        "from database.taxon_lookup import TaxonChoice, TaxonLookupService\n"
        "assert 'PySide6' not in sys.modules\n"
        "assert 'ui.main_window' not in sys.modules\n"
        "assert 'ui.observations_tab' not in sys.modules\n"
        "print(TaxonChoice.__module__)\n"
        "print(TaxonLookupService.__module__)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.splitlines() == ["database.taxon_lookup", "database.taxon_lookup"]


def test_suggest_genera_returns_local_genera(tmp_path: Path, monkeypatch) -> None:
    service = _make_service(tmp_path, monkeypatch)

    assert service.suggest_genera("en") == ["Entoloma"]


def test_suggest_species_returns_species_constrained_by_genus(tmp_path: Path, monkeypatch) -> None:
    service = _make_service(tmp_path, monkeypatch)

    values = service.suggest_species("Entoloma", "se")

    assert [choice.genus for choice in values] == ["Entoloma"]
    assert [choice.species for choice in values] == ["sericeum"]
    assert values[0].source == "taxonomy"
    assert values[0].family == "Entolomataceae"
    _assert_no_redlist(values[0])


def test_suggest_species_includes_reference_only_species(tmp_path: Path, monkeypatch) -> None:
    service = _make_service(tmp_path, monkeypatch)

    values = service.suggest_species("Coprinus", "co")

    assert len(values) == 1
    assert values[0].genus == "Coprinus"
    assert values[0].species == "comatus"
    assert values[0].source == "reference"
    assert values[0].common_name is None
    _assert_no_redlist(values[0])


def test_duplicate_species_from_taxonomy_and_reference_are_merged(tmp_path: Path, monkeypatch) -> None:
    service = _make_service(tmp_path, monkeypatch)

    values = service.suggest_species("Agaricus", "bi")

    assert len(values) == 1
    assert values[0].genus == "Agaricus"
    assert values[0].species == "bisporus"
    assert values[0].source == "both"
    assert values[0].taxon_id == 1
    assert values[0].common_name == "Button mushroom"
    assert values[0].family == "Agaricaceae"
    _assert_no_redlist(values[0])


def test_suggest_common_names_can_be_constrained_by_genus(tmp_path: Path, monkeypatch) -> None:
    service = _make_service(tmp_path, monkeypatch)

    values = service.suggest_common_names(prefix="but", genus="agaricus", species="bisporus")

    assert len(values) == 1
    assert values[0].genus == "Agaricus"
    assert values[0].species == "bisporus"
    assert values[0].common_name == "Button mushroom"
    assert values[0].source == "taxonomy"
    _assert_no_redlist(values[0])


def test_resolve_scientific_returns_taxonchoice_with_common_name_and_family(
    tmp_path: Path,
    monkeypatch,
) -> None:
    service = _make_service(tmp_path, monkeypatch)

    choice = service.resolve_scientific("AGARICUS", "BISPORUS")

    assert choice is not None
    assert choice.genus == "Agaricus"
    assert choice.species == "bisporus"
    assert choice.common_name == "Button mushroom"
    assert choice.family == "Agaricaceae"
    assert choice.source == "taxonomy"
    assert choice.taxon_id == 1
    assert choice.language_code == "en"
    _assert_no_redlist(choice)


def test_resolve_common_name_returns_choices(tmp_path: Path, monkeypatch) -> None:
    service = _make_service(tmp_path, monkeypatch)

    values = service.resolve_common_name("Button mushroom")

    assert len(values) == 1
    assert values[0].genus == "Agaricus"
    assert values[0].species == "bisporus"
    assert values[0].common_name == "Button mushroom"
    assert values[0].source == "taxonomy"
    _assert_no_redlist(values[0])


def test_best_common_name_for_taxon_prefers_preferred_name(tmp_path: Path, monkeypatch) -> None:
    service = _make_service(tmp_path, monkeypatch)

    choice = service.best_common_name_for_taxon("Agaricus", "bisporus")

    assert choice is not None
    assert choice.common_name == "Button mushroom"
    assert choice.language_code == "en"
    assert choice.source == "taxonomy"
    _assert_no_redlist(choice)


def test_service_is_safe_without_vernacular_db(tmp_path: Path, monkeypatch) -> None:
    ref_db = tmp_path / "reference-only.sqlite"
    _seed_reference_db(ref_db)
    _patch_reference_connection(monkeypatch, ref_db)

    service = TaxonLookupService(vernacular_db=None, language_code="en")

    assert service.suggest_genera("co") == ["Coprinus"]
    species = service.suggest_species("Coprinus", "co")
    assert len(species) == 1
    assert species[0].source == "reference"
    assert species[0].genus == "Coprinus"
    assert species[0].species == "comatus"
    assert service.resolve_scientific("Agaricus", "bisporus") is None
    assert service.resolve_common_name("Button mushroom") == []
    assert service.common_names_for_taxon("Agaricus", "bisporus") == []
    assert service.best_common_name_for_taxon("Agaricus", "bisporus") is None


def test_red_list_fields_remain_none_in_local_lookup_results(tmp_path: Path, monkeypatch) -> None:
    service = _make_service(tmp_path, monkeypatch)

    scientific = service.resolve_scientific("Agaricus", "bisporus")
    common = service.resolve_common_name("Button mushroom")
    suggestions = service.suggest_species("Agaricus", "bi")

    assert scientific is not None
    _assert_no_redlist(scientific)
    assert common
    _assert_no_redlist(common[0])
    assert suggestions
    _assert_no_redlist(suggestions[0])
