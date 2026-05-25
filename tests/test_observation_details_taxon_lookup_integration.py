from __future__ import annotations

import os
import sqlite3
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QDialog, QLineEdit

from database.taxon_lookup import TaxonChoice
import ui.observations_tab as observations_tab
from ui.observations_tab import ObservationDetailsDialog


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


class _MinimalObservationDetailsDialog(ObservationDetailsDialog):
    def __init__(self):
        QDialog.__init__(self)
        self._style_dropdown_popup_readability = lambda *args, **kwargs: None
        self._suppress_taxon_autofill = False
        self._host_suppress_taxon_autofill = False
        self._last_genus = ""
        self._last_species = ""


def _seed_taxonomy_db(db_path: Path) -> None:
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
                (2, "Entoloma", "sericeum", "Entolomataceae", "Entoloma sericeum"),
                (3, "Amanita", "muscaria", "Amanitaceae", "Amanita muscaria"),
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
                (2, "Silky entoloma", 1, "en"),
                (3, "Fly agaric", 1, "en"),
            ],
        )
        conn.executemany(
            """
            INSERT INTO scientific_name_min (taxon_id, scientific_name, is_preferred_name)
            VALUES (?, ?, ?)
            """,
            [
                (1, "Agaricus bisporus", 1),
                (2, "Entoloma sericeum", 1),
                (3, "Amanita muscaria", 1),
            ],
        )


def _configure_dialog_environment(monkeypatch, db_path: Path | None) -> None:
    monkeypatch.setattr(
        observations_tab.SettingsDB,
        "get_setting",
        lambda key, default=None: "en" if key == "vernacular_language" else default,
    )
    monkeypatch.setattr(observations_tab, "resolve_vernacular_db_path", lambda _lang: db_path)


def _build_dialog() -> _MinimalObservationDetailsDialog:
    dialog = _MinimalObservationDetailsDialog()
    dialog.vernacular_input = QLineEdit(dialog)
    dialog.genus_input = QLineEdit(dialog)
    dialog.species_input = QLineEdit(dialog)
    return dialog


def _seeded_dialog(tmp_path: Path, monkeypatch) -> _MinimalObservationDetailsDialog:
    db_path = tmp_path / "vernacular.sqlite"
    _seed_taxonomy_db(db_path)
    _configure_dialog_environment(monkeypatch, db_path)
    dialog = _build_dialog()
    dialog._setup_vernacular_autocomplete()
    return dialog


def test_main_taxon_lookup_wires_service_and_constrains_species_and_common_names(
    tmp_path: Path,
    monkeypatch,
    qapp,
) -> None:
    dialog = _seeded_dialog(tmp_path, monkeypatch)

    assert dialog._taxon_lookup is not None
    assert dialog._taxon_lookup.vernacular_db is not None

    dialog.genus_input.setText("Entoloma")
    dialog.species_input.setText("se")
    ObservationDetailsDialog._on_species_text_changed(dialog, "se")
    assert dialog._species_model.stringList() == ["sericeum"]

    dialog.species_input.setText("sericeum")
    dialog.vernacular_input.setText("")
    ObservationDetailsDialog._on_vernacular_text_changed(dialog, "sil")
    assert dialog._vernacular_model.stringList() == ["Silky entoloma (Entoloma sericeum)"]

    dialog.deleteLater()


def test_common_name_selection_uses_hidden_choice_payload(tmp_path: Path, monkeypatch, qapp) -> None:
    dialog = _seeded_dialog(tmp_path, monkeypatch)

    dialog.genus_input.setText("")
    dialog.species_input.setText("")
    dialog.vernacular_input.setText("")
    ObservationDetailsDialog._on_vernacular_text_changed(dialog, "But")

    assert dialog._vernacular_model.stringList() == ["Button mushroom (Agaricus bisporus)"]
    label = dialog._vernacular_model.stringList()[0]

    ObservationDetailsDialog._on_vernacular_selected(dialog, label)

    assert dialog.vernacular_input.text() == "Button mushroom"
    assert dialog.genus_input.text() == "Agaricus"
    assert dialog.species_input.text() == "bisporus"

    dialog.deleteLater()


def test_common_name_selection_handles_choice_without_species(tmp_path: Path, monkeypatch, qapp) -> None:
    dialog = _seeded_dialog(tmp_path, monkeypatch)

    dialog.vernacular_input.blockSignals(True)
    dialog.genus_input.blockSignals(True)
    dialog.species_input.blockSignals(True)
    dialog.vernacular_input.setText("preserve me")
    dialog.genus_input.setText("")
    dialog.species_input.setText("preserve me")
    dialog.vernacular_input.blockSignals(False)
    dialog.genus_input.blockSignals(False)
    dialog.species_input.blockSignals(False)
    dialog._vernacular_entry_map = {
        "button mushroom": TaxonChoice(genus="Agaricus", species=None, common_name="Button mushroom")
    }

    ObservationDetailsDialog._on_vernacular_selected(dialog, "Button mushroom")

    assert dialog.vernacular_input.text() == "Button mushroom"
    assert dialog.genus_input.text() == "Agaricus"
    assert dialog.species_input.text() == "preserve me"

    dialog.deleteLater()


def test_common_name_autofills_and_refreshes_when_taxon_changes(
    tmp_path: Path,
    monkeypatch,
    qapp,
) -> None:
    dialog = _seeded_dialog(tmp_path, monkeypatch)

    dialog.genus_input.setText("Agaricus")
    dialog.species_input.setText("bisporus")
    dialog.vernacular_input.setText("")
    ObservationDetailsDialog._maybe_set_vernacular_from_taxon(dialog)
    assert dialog.vernacular_input.text() == "Button mushroom"

    dialog.genus_input.blockSignals(True)
    dialog.genus_input.setText("Entoloma")
    dialog.genus_input.blockSignals(False)
    dialog.species_input.blockSignals(True)
    dialog.species_input.setText("sericeum")
    dialog.species_input.blockSignals(False)

    ObservationDetailsDialog._on_species_editing_finished(dialog)

    assert dialog.vernacular_input.text() == "Silky entoloma"
    assert dialog._vernacular_model.stringList() == ["Silky entoloma (Entoloma sericeum)"]

    dialog.deleteLater()


def test_setup_is_safe_without_vernacular_db(tmp_path: Path, monkeypatch, qapp) -> None:
    _configure_dialog_environment(monkeypatch, None)
    dialog = _build_dialog()

    dialog._setup_vernacular_autocomplete()

    assert dialog._taxon_lookup is not None
    assert dialog._taxon_lookup.vernacular_db is None

    dialog.deleteLater()
