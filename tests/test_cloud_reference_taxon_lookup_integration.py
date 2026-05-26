from __future__ import annotations

import os
import sqlite3
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt, QEvent
from PySide6.QtGui import QStandardItem
from PySide6.QtWidgets import QApplication, QWidget

import database.models as models
import ui.cloud_reference_dialog as cloud_reference_dialog
from database.taxon_lookup import TaxonChoice
from ui.cloud_reference_dialog import CloudReferenceDialog


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def _seed_local_taxonomy_db(db_path: Path) -> None:
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
                (2, "Fly agaric", 1, "en"),
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


def _patch_dialog_dependencies(monkeypatch, ref_db_path: Path, *, vernacular_db_path: Path | None) -> None:
    monkeypatch.setattr(
        cloud_reference_dialog.SettingsDB,
        "get_setting",
        lambda key, default=None: "en" if key == "vernacular_language" else default,
    )
    monkeypatch.setattr(cloud_reference_dialog, "resolve_vernacular_db_path", lambda _lang: vernacular_db_path)
    monkeypatch.setattr(models, "get_reference_connection", lambda: sqlite3.connect(ref_db_path))


def _build_dialog(
    tmp_path: Path,
    monkeypatch,
    *,
    vernacular_db_path: Path | None,
    genus: str = "Agaricus",
    species: str = "bisporus",
) -> tuple[CloudReferenceDialog, QWidget]:
    ref_db_path = tmp_path / "reference-values.sqlite"
    _seed_reference_db(ref_db_path)
    if vernacular_db_path is not None:
        _seed_local_taxonomy_db(vernacular_db_path)
    _patch_dialog_dependencies(monkeypatch, ref_db_path, vernacular_db_path=vernacular_db_path)
    parent = QWidget()
    dialog = CloudReferenceDialog(parent, genus=genus, species=species)
    app = QApplication.instance()
    if app is not None:
        app.processEvents()
    return dialog, parent


def test_cloud_reference_dialog_species_rows_use_taxon_lookup_service(tmp_path: Path, monkeypatch, qapp) -> None:
    vernacular_db_path = tmp_path / "vernacular.sqlite"
    dialog, parent = _build_dialog(tmp_path, monkeypatch, vernacular_db_path=vernacular_db_path)

    values = dialog._update_species_suggestions("Agaricus", "bi")

    assert values == ["bisporus"]
    item = dialog._species_model.item(0)
    assert item is not None
    choice = item.data(dialog._ROLE_TAXON_CHOICE)
    assert isinstance(choice, TaxonChoice)
    assert choice.genus == "Agaricus"
    assert choice.species == "bisporus"
    assert choice.common_name == "Button mushroom"
    assert choice.family == "Agaricaceae"
    assert item.text() == "Agaricus bisporus - Button mushroom - Agaricaceae"
    assert item.data(Qt.UserRole) == "bisporus"
    assert item.data(Qt.UserRole + 1) == "Agaricus"
    assert item.data(Qt.UserRole + 2) == "bisporus"

    dialog.deleteLater()
    parent.deleteLater()


def test_cloud_reference_dialog_focus_populates_blank_prefix_species_and_common_name_models(
    tmp_path: Path,
    monkeypatch,
    qapp,
) -> None:
    vernacular_db_path = tmp_path / "vernacular.sqlite"
    dialog, parent = _build_dialog(
        tmp_path,
        monkeypatch,
        vernacular_db_path=vernacular_db_path,
        genus="Agaricus",
        species="",
    )

    species_calls: list[tuple[str, str, int]] = []
    common_calls: list[tuple[str, str | None, str | None, int]] = []

    def _suggest_species(genus: str, prefix: str, limit: int = 50) -> list[TaxonChoice]:
        species_calls.append((genus, prefix, limit))
        assert prefix in ("", "bi")
        return [TaxonChoice(genus="Agaricus", species="bisporus", common_name="Button mushroom")]

    def _suggest_common_names(
        prefix: str = "",
        genus: str | None = None,
        species: str | None = None,
        limit: int = 50,
    ) -> list[TaxonChoice]:
        common_calls.append((prefix, genus, species, limit))
        assert prefix == ""
        return [
            TaxonChoice(genus="Agaricus", species="bisporus", common_name="Cultivated mushroom"),
            TaxonChoice(genus="Agaricus", species="bisporus", common_name="Button mushroom"),
        ]

    monkeypatch.setattr(dialog._taxon_lookup, "suggest_species", _suggest_species)
    monkeypatch.setattr(dialog._taxon_lookup, "suggest_common_names", _suggest_common_names)
    monkeypatch.setattr(dialog._species_completer, "complete", lambda *args, **kwargs: None)
    monkeypatch.setattr(dialog._vernacular_completer, "complete", lambda *args, **kwargs: None)

    dialog.genus_input.blockSignals(True)
    dialog.genus_input.setText("Agaricus")
    dialog.genus_input.blockSignals(False)
    dialog.genus_input.setFocus()
    qapp.processEvents()
    dialog.eventFilter(dialog.genus_input, QEvent(QEvent.FocusIn))
    qapp.processEvents()
    assert dialog.genus_input.selectedText() == "Agaricus"

    dialog.species_input.blockSignals(True)
    dialog.species_input.setText("bi")
    dialog.species_input.blockSignals(False)
    dialog.species_input.setFocus()
    qapp.processEvents()
    dialog.eventFilter(dialog.species_input, QEvent(QEvent.FocusIn))
    qapp.processEvents()
    assert species_calls == [("Agaricus", "bi", 50)]
    assert dialog._species_model.rowCount() == 1
    assert dialog._species_model.item(0).data(Qt.UserRole) == "bisporus"
    assert dialog.species_input.selectedText() == "bi"

    dialog.eventFilter(dialog.vernacular_input, QEvent(QEvent.FocusIn))
    qapp.processEvents()
    assert common_calls == [("", "Agaricus", "bi", 50)]
    assert dialog._vernacular_model.rowCount() == 2
    assert dialog._vernacular_model.item(0).data(dialog._ROLE_TAXON_CHOICE).common_name == "Cultivated mushroom"
    assert dialog._vernacular_model.item(1).data(dialog._ROLE_TAXON_CHOICE).common_name == "Button mushroom"

    dialog.vernacular_input.blockSignals(True)
    dialog.vernacular_input.setText("Button mushroom")
    dialog.vernacular_input.blockSignals(False)
    dialog.vernacular_input.setFocus()
    qapp.processEvents()
    dialog.eventFilter(dialog.vernacular_input, QEvent(QEvent.FocusIn))
    qapp.processEvents()

    dialog.deleteLater()
    parent.deleteLater()


def test_cloud_reference_dialog_common_name_selection_uses_hidden_choice_payload(
    tmp_path: Path,
    monkeypatch,
    qapp,
) -> None:
    vernacular_db_path = tmp_path / "vernacular.sqlite"
    dialog, parent = _build_dialog(tmp_path, monkeypatch, vernacular_db_path=vernacular_db_path)

    dialog._update_vernacular_suggestions_for_taxon()
    index = dialog._vernacular_model.index(0, 0)
    choice = index.data(dialog._ROLE_TAXON_CHOICE)
    assert isinstance(choice, TaxonChoice)
    assert choice.genus == "Agaricus"
    assert choice.species == "bisporus"

    def fail_if_resolved(*_args, **_kwargs):
        raise AssertionError("selection should use the hidden TaxonChoice payload")

    monkeypatch.setattr(dialog._taxon_lookup, "resolve_common_name", fail_if_resolved)

    dialog._on_vernacular_selected(index)

    assert dialog.vernacular_input.text() == "Button mushroom"
    assert dialog.genus_input.text() == "Agaricus"
    assert dialog.species_input.text() == "bisporus"

    dialog.deleteLater()
    parent.deleteLater()


def test_cloud_reference_dialog_handles_missing_vernacular_db(tmp_path: Path, monkeypatch, qapp) -> None:
    dialog, parent = _build_dialog(tmp_path, monkeypatch, vernacular_db_path=None)

    assert dialog._vernacular_db is None
    assert dialog._taxon_lookup is not None
    assert dialog._taxon_lookup.vernacular_db is None

    dialog._update_vernacular_suggestions_for_taxon()
    assert dialog._vernacular_model.rowCount() == 0

    dialog.deleteLater()
    parent.deleteLater()


def test_cloud_reference_dialog_handles_choice_without_species(tmp_path: Path, monkeypatch, qapp) -> None:
    vernacular_db_path = tmp_path / "vernacular.sqlite"
    dialog, parent = _build_dialog(tmp_path, monkeypatch, vernacular_db_path=vernacular_db_path)

    choice = TaxonChoice(genus="Agaricus", species=None, common_name="Button mushroom")
    dialog._species_model.appendRow(QStandardItem("Button mushroom"))
    index = dialog._species_model.index(0, 0)
    dialog.species_input.setText("preserve me")
    monkeypatch.setattr(dialog, "_choice_from_index", lambda _index: choice)

    dialog._on_species_selected(index)

    assert dialog.species_input.text() == "preserve me"
    assert dialog.vernacular_input.text() == "Button mushroom"

    dialog.vernacular_input.setText("Button mushroom")
    dialog.genus_input.setText("")
    dialog.species_input.setText("preserve me")
    monkeypatch.setattr(
        dialog._taxon_lookup,
        "resolve_common_name",
        lambda _name, *_args, **_kwargs: [choice],
    )

    dialog._on_vernacular_editing_finished()

    assert dialog.genus_input.text() == "Agaricus"
    assert dialog.species_input.text() == "preserve me"

    dialog.deleteLater()
    parent.deleteLater()
