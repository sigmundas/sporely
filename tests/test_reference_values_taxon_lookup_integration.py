from __future__ import annotations

import os
import sqlite3
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QEvent
from PySide6.QtWidgets import QApplication, QWidget

import database.models as models
import ui.main_window as main_window
from database.taxon_lookup import TaxonChoice
from ui.main_window import ReferenceValuesDialog


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


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
                (4, "Coprinus", "comatus", "Agaricaceae", "Coprinus comatus"),
                (5, "Laccaria", "laccata", "Hydnangiaceae", "Laccaria laccata"),
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
                (4, "Shaggy mane", 1, "en"),
                (5, "Shared name", 1, "en"),
                (3, "Shared name", 1, "en"),
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
                (4, "Coprinus comatus", 1),
                (5, "Laccaria laccata", 1),
            ],
        )


def _seed_reference_db(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE reference_values (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                genus TEXT NOT NULL,
                species TEXT NOT NULL,
                source TEXT,
                mount_medium TEXT,
                stain TEXT,
                length_min REAL,
                length_p05 REAL,
                length_p50 REAL,
                length_p95 REAL,
                length_max REAL,
                width_min REAL,
                width_p05 REAL,
                width_p50 REAL,
                width_p95 REAL,
                width_max REAL,
                q_min REAL,
                q_p50 REAL,
                q_max REAL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO reference_values (
                genus, species, source, mount_medium, stain,
                length_min, length_p05, length_p50, length_p95, length_max,
                width_min, width_p05, width_p50, width_p95, width_max,
                q_min, q_p50, q_max, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "Agaricus",
                    "bisporus",
                    "Reference sheet",
                    "water",
                    "cotton blue",
                    4.0,
                    4.5,
                    5.0,
                    5.5,
                    6.0,
                    3.0,
                    3.2,
                    3.4,
                    3.6,
                    3.8,
                    1.1,
                    1.2,
                    1.3,
                    "2026-05-25 12:00:00",
                ),
                (
                    "Coprinus",
                    "comatus",
                    "Other sheet",
                    "water",
                    None,
                    8.0,
                    8.5,
                    9.0,
                    9.5,
                    10.0,
                    4.0,
                    4.2,
                    4.4,
                    4.6,
                    4.8,
                    1.8,
                    2.0,
                    2.2,
                    "2026-05-25 12:10:00",
                ),
            ],
        )


def _configure_dialog_environment(monkeypatch, vernacular_db_path: Path, reference_db_path: Path) -> None:
    monkeypatch.setattr(
        main_window.SettingsDB,
        "get_setting",
        lambda key, default=None: "en" if key == "vernacular_language" else default,
    )
    monkeypatch.setattr(main_window, "resolve_vernacular_db_path", lambda _lang: vernacular_db_path)
    monkeypatch.setattr(models, "get_reference_connection", lambda: sqlite3.connect(reference_db_path))


def _build_dialog(
    tmp_path: Path,
    monkeypatch,
    *,
    genus: str = "",
    species: str = "",
    ref_values: dict | None = None,
) -> tuple[ReferenceValuesDialog, QWidget]:
    vernacular_db_path = tmp_path / "vernacular.sqlite"
    reference_db_path = tmp_path / "reference.sqlite"
    _seed_taxonomy_db(vernacular_db_path)
    _seed_reference_db(reference_db_path)
    _configure_dialog_environment(monkeypatch, vernacular_db_path, reference_db_path)
    parent = QWidget()
    dialog = ReferenceValuesDialog(genus, species, ref_values=ref_values, parent=parent)
    app = QApplication.instance()
    if app is not None:
        app.processEvents()
    return dialog, parent


class _FailingVernacularDB:
    def __getattr__(self, name: str):
        raise AssertionError(f"ReferenceValuesDialog should not call VernacularDB.{name}")


def test_reference_values_dialog_uses_taxon_lookup_for_suggestions_and_hidden_common_name_selection(
    tmp_path: Path,
    monkeypatch,
    qapp,
) -> None:
    dialog, parent = _build_dialog(tmp_path, monkeypatch)

    assert dialog._taxon_lookup is not None
    assert dialog._taxon_lookup.vernacular_db is not None

    dialog.vernacular_db = _FailingVernacularDB()
    complete_calls: list[str] = []
    monkeypatch.setattr(dialog._genus_completer, "complete", lambda *args, **kwargs: complete_calls.append("genus"))
    monkeypatch.setattr(dialog._species_completer, "complete", lambda *args, **kwargs: complete_calls.append("species"))
    monkeypatch.setattr(dialog._vernacular_completer, "complete", lambda *args, **kwargs: complete_calls.append("vernacular"))

    dialog.show()
    qapp.processEvents()

    dialog.genus_input.blockSignals(True)
    dialog.genus_input.setText("Aga")
    dialog.genus_input.blockSignals(False)
    dialog.genus_input.setFocus()
    qapp.processEvents()
    dialog.eventFilter(dialog.genus_input, QEvent(QEvent.FocusIn))
    qapp.processEvents()
    assert "genus" in complete_calls
    assert dialog.genus_input.selectedText() == "Aga"

    dialog.genus_input.blockSignals(True)
    dialog.genus_input.setText("Agaricus")
    dialog.genus_input.blockSignals(False)

    dialog.species_input.blockSignals(True)
    dialog.species_input.setText("bi")
    dialog.species_input.blockSignals(False)
    dialog.species_input.setFocus()
    qapp.processEvents()
    dialog.eventFilter(dialog.species_input, QEvent(QEvent.FocusIn))
    qapp.processEvents()
    assert dialog.species_input.selectedText() == "bi"

    dialog.species_input.blockSignals(True)
    dialog.species_input.setText("bisporus")
    dialog.species_input.blockSignals(False)

    dialog._maybe_set_vernacular_from_taxon()
    assert dialog.vernacular_input.text() == "Button mushroom"
    assert "species" in complete_calls

    dialog.vernacular_input.blockSignals(True)
    dialog.vernacular_input.setText("Button mushroom")
    dialog.vernacular_input.blockSignals(False)
    dialog.vernacular_input.setFocus()
    qapp.processEvents()
    dialog.eventFilter(dialog.vernacular_input, QEvent(QEvent.FocusIn))
    qapp.processEvents()
    assert dialog.vernacular_input.selectedText() == "Button mushroom"

    dialog.vernacular_input.blockSignals(True)
    dialog.vernacular_input.clear()
    dialog.vernacular_input.blockSignals(False)
    dialog.genus_input.blockSignals(True)
    dialog.genus_input.clear()
    dialog.genus_input.blockSignals(False)
    dialog.species_input.blockSignals(True)
    dialog.species_input.setText("bisporus")
    dialog.species_input.blockSignals(False)
    dialog._clear_vernacular_suggestions()

    dialog._update_genus_suggestions("Ent")
    assert dialog._genus_model.stringList() == ["Entoloma"]

    dialog._update_species_suggestions("Entoloma", "se")
    assert dialog._species_model.stringList() == ["sericeum"]

    dialog.genus_input.blockSignals(True)
    dialog.genus_input.clear()
    dialog.genus_input.blockSignals(False)
    dialog.species_input.blockSignals(True)
    dialog.species_input.clear()
    dialog.species_input.blockSignals(False)

    suggestions = dialog._taxon_lookup.suggest_common_names(prefix="", genus="Agaricus", species="bisporus")
    dialog._populate_vernacular_model(suggestions)
    assert dialog._vernacular_model.rowCount() == 2
    index = dialog._vernacular_model.index(0, 0)
    choice = index.data(dialog._ROLE_TAXON_CHOICE)
    assert isinstance(choice, TaxonChoice)

    monkeypatch.setattr(
        dialog._taxon_lookup,
        "resolve_common_name",
        lambda *_args, **_kwargs: [choice],
    )

    dialog._on_vernacular_selected(index)

    assert dialog.vernacular_input.text() == choice.common_name
    assert dialog.genus_input.text() == choice.genus
    assert dialog.species_input.text() == choice.species

    dialog.deleteLater()
    parent.deleteLater()


def test_reference_values_dialog_ambiguous_common_name_does_not_autofill(
    tmp_path: Path,
    monkeypatch,
    qapp,
) -> None:
    dialog, parent = _build_dialog(tmp_path, monkeypatch)

    dialog.vernacular_db = _FailingVernacularDB()

    dialog.vernacular_input.blockSignals(True)
    dialog.vernacular_input.setText("Shared name")
    dialog.vernacular_input.blockSignals(False)

    dialog._on_vernacular_editing_finished()

    assert dialog.vernacular_input.text() == "Shared name"
    assert dialog.genus_input.text() == ""
    assert dialog.species_input.text() == ""

    dialog.deleteLater()
    parent.deleteLater()


def test_reference_values_dialog_still_loads_reference_via_reference_db(
    tmp_path: Path,
    monkeypatch,
    qapp,
) -> None:
    calls: list[tuple[tuple, dict]] = []
    original_get_reference = main_window.ReferenceDB.get_reference

    def _tracking_get_reference(*args, **kwargs):
        calls.append((args, kwargs))
        return original_get_reference(*args, **kwargs)

    monkeypatch.setattr(main_window.ReferenceDB, "get_reference", _tracking_get_reference)

    dialog, parent = _build_dialog(
        tmp_path,
        monkeypatch,
        genus="Agaricus",
        species="bisporus",
        ref_values={"source": "Reference sheet"},
    )

    assert calls
    assert any(call_args[:3] == ("Agaricus", "bisporus", "Reference sheet") for call_args, _kwargs in calls)
    assert dialog.mount_input.text() == "water"
    assert dialog.stain_input.text() == "cotton blue"
    assert dialog.table.item(0, 0).text() == "4"
    assert dialog.table.item(1, 4).text() == "3.8"

    dialog.deleteLater()
    parent.deleteLater()
