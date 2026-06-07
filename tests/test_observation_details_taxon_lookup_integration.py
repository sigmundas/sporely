from __future__ import annotations

import os
import sqlite3
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QEvent, Qt
from PySide6.QtGui import QFocusEvent
from PySide6.QtWidgets import QApplication, QDialog, QLineEdit, QLabel

import database.models as models
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
                (4, "Coprinus", "comatus", "Agaricaceae", "Coprinus comatus"),
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
                (4, "Lawyer's wig", 1, "en"),
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
                ("Entoloma", "sericeum"),
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
    dialog.host_genus_input = QLineEdit(dialog)
    dialog.host_species_input = QLineEdit(dialog)
    dialog.host_vernacular_input = QLineEdit(dialog)
    dialog.host_vernacular_label = QLabel(dialog)
    return dialog


def _seeded_dialog(tmp_path: Path, monkeypatch) -> _MinimalObservationDetailsDialog:
    db_path = tmp_path / "vernacular.sqlite"
    _seed_taxonomy_db(db_path)
    _configure_dialog_environment(monkeypatch, db_path)
    dialog = _build_dialog()
    dialog._setup_vernacular_autocomplete()
    dialog._setup_host_autocomplete()
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
    assert dialog._vernacular_model.stringList() == ["Silky entoloma"]

    dialog.deleteLater()


def test_popup_focus_does_not_reselect_text_while_typing(tmp_path: Path, monkeypatch, qapp) -> None:
    dialog = _seeded_dialog(tmp_path, monkeypatch)

    calls: list[str] = []
    monkeypatch.setattr(dialog._genus_completer, "complete", lambda: calls.append("complete"))
    monkeypatch.setattr(dialog.genus_input, "selectAll", lambda: calls.append("select_all"))

    dialog.genus_input.setText("Ent")
    dialog.eventFilter(dialog.genus_input, QFocusEvent(QEvent.FocusIn, Qt.PopupFocusReason))
    qapp.processEvents()

    assert "complete" in calls
    assert "select_all" not in calls

    dialog.deleteLater()


def test_main_taxon_focus_populates_blank_prefix_species_and_common_name_models(
    tmp_path: Path,
    monkeypatch,
    qapp,
) -> None:
    dialog = _seeded_dialog(tmp_path, monkeypatch)

    species_calls: list[tuple[str, str, int]] = []
    common_calls: list[tuple[str, str | None, str | None, int]] = []
    complete_calls: list[str] = []

    def _suggest_species(genus: str, prefix: str, limit: int = 200) -> list[TaxonChoice]:
        species_calls.append((genus, prefix, limit))
        assert prefix in ("", "bi")
        return [TaxonChoice(genus="Agaricus", species="bisporus", common_name="Button mushroom")]

    def _suggest_common_names(
        prefix: str = "",
        genus: str | None = None,
        species: str | None = None,
        limit: int = 200,
    ) -> list[TaxonChoice]:
        common_calls.append((prefix, genus, species, limit))
        assert prefix in ("", "Button mushroom")
        return [
            TaxonChoice(genus="Agaricus", species="bisporus", common_name="Cultivated mushroom"),
            TaxonChoice(genus="Agaricus", species="bisporus", common_name="Button mushroom"),
        ]

    monkeypatch.setattr(dialog._taxon_lookup, "suggest_species", _suggest_species)
    monkeypatch.setattr(dialog._taxon_lookup, "suggest_common_names", _suggest_common_names)
    monkeypatch.setattr(
        dialog._taxon_lookup,
        "best_common_name_for_taxon",
        lambda genus, species: TaxonChoice(genus=genus, species=species, common_name="Button mushroom"),
    )
    monkeypatch.setattr(dialog._genus_completer, "complete", lambda: complete_calls.append("genus"))
    monkeypatch.setattr(dialog._species_completer, "complete", lambda: complete_calls.append("species"))
    monkeypatch.setattr(dialog._vernacular_completer, "complete", lambda: complete_calls.append("vernacular"))

    dialog.show()
    qapp.processEvents()

    dialog.genus_input.blockSignals(True)
    dialog.genus_input.setText("Agaricus")
    dialog.genus_input.blockSignals(False)
    dialog.genus_input.setFocus()
    qapp.processEvents()
    ObservationDetailsDialog.eventFilter(dialog, dialog.genus_input, QEvent(QEvent.FocusIn))
    qapp.processEvents()
    assert dialog.genus_input.selectedText() == "Agaricus"

    dialog.species_input.blockSignals(True)
    dialog.species_input.setText("bi")
    dialog.species_input.blockSignals(False)
    ObservationDetailsDialog.eventFilter(dialog, dialog.species_input, QEvent(QEvent.FocusIn))
    qapp.processEvents()
    assert species_calls == [("Agaricus", "bi", 200)]
    assert dialog._species_model.stringList() == ["bisporus"]
    assert "species" in complete_calls
    assert dialog.species_input.selectedText() == "bi"

    dialog.species_input.blockSignals(True)
    dialog.species_input.setText("bisporus")
    dialog.species_input.blockSignals(False)

    ObservationDetailsDialog.eventFilter(dialog, dialog.vernacular_input, QEvent(QEvent.FocusIn))
    assert ("", "Agaricus", "bisporus", 200) in common_calls
    assert dialog._vernacular_model.stringList() == [
        "Cultivated mushroom",
        "Button mushroom",
    ]
    assert "vernacular" in complete_calls

    dialog.vernacular_input.blockSignals(True)
    dialog.vernacular_input.setText("Button mushroom")
    dialog.vernacular_input.blockSignals(False)
    dialog.vernacular_input.setFocus()
    qapp.processEvents()
    ObservationDetailsDialog.eventFilter(dialog, dialog.vernacular_input, QEvent(QEvent.FocusIn))
    qapp.processEvents()
    assert dialog.vernacular_input.selectedText() == "Button mushroom"

    common_calls.clear()
    dialog.vernacular_input.blockSignals(True)
    dialog.vernacular_input.setText("")
    dialog.vernacular_input.blockSignals(False)
    ObservationDetailsDialog._maybe_set_vernacular_from_taxon(dialog)

    assert common_calls == [("", "Agaricus", "bisporus", 200)]
    assert dialog.vernacular_input.text() == "Button mushroom"

    dialog.deleteLater()


def test_host_taxon_lookup_wires_service_and_constrains_species_and_common_names(
    tmp_path: Path,
    monkeypatch,
    qapp,
) -> None:
    dialog = _seeded_dialog(tmp_path, monkeypatch)

    assert dialog._taxon_lookup is not None
    assert dialog._host_genus_model is not None

    monkeypatch.setattr(dialog.vernacular_db, "suggest_genus", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("host genus should use TaxonLookupService")))
    monkeypatch.setattr(dialog.vernacular_db, "suggest_species", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("host species should use TaxonLookupService")))
    monkeypatch.setattr(dialog.vernacular_db, "suggest_vernacular_entries", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("host common-name suggestions should use TaxonLookupService")))
    monkeypatch.setattr(dialog.vernacular_db, "taxon_from_vernacular", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("host common-name resolution should use TaxonLookupService")))
    monkeypatch.setattr(dialog.vernacular_db, "taxon_from_scientific", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("host scientific resolution should use TaxonLookupService")))

    monkeypatch.setattr(dialog._taxon_lookup, "suggest_genera", lambda prefix, limit=200: ["Entoloma"])
    monkeypatch.setattr(
        dialog._taxon_lookup,
        "suggest_species",
        lambda genus, prefix, limit=200: [TaxonChoice(genus="Entoloma", species="sericeum", common_name="Silky entoloma")],
    )
    monkeypatch.setattr(
        dialog._taxon_lookup,
        "suggest_common_names",
        lambda prefix="", genus=None, species=None, limit=200: [
            TaxonChoice(genus="Agaricus", species="bisporus", common_name="Button mushroom")
        ],
    )
    monkeypatch.setattr(
        dialog._taxon_lookup,
        "resolve_common_name",
        lambda *_args, **_kwargs: [TaxonChoice(genus="Agaricus", species="bisporus", common_name="Button mushroom")],
    )

    dialog.host_genus_input.blockSignals(True)
    dialog.host_genus_input.setText("Ent")
    dialog.host_genus_input.blockSignals(False)
    ObservationDetailsDialog._on_host_genus_text_changed(dialog, "Ent")
    assert dialog._host_genus_model.stringList() == ["Entoloma"]

    ObservationDetailsDialog._on_host_genus_selected(dialog, "Entoloma")
    assert dialog._host_species_model.stringList() == ["sericeum"]

    dialog.host_genus_input.blockSignals(True)
    dialog.host_genus_input.setText("Agaricus")
    dialog.host_genus_input.blockSignals(False)
    dialog.host_species_input.blockSignals(True)
    dialog.host_species_input.setText("")
    dialog.host_species_input.blockSignals(False)
    dialog.host_vernacular_input.setText("")
    ObservationDetailsDialog._on_host_vernacular_text_changed(dialog, "But")
    assert dialog._host_vernacular_model.stringList() == ["Button mushroom"]

    label = dialog._host_vernacular_model.stringList()[0]
    monkeypatch.setattr(dialog, "_refresh_host_vernacular_for_current_taxon", lambda: None)
    ObservationDetailsDialog._on_host_vernacular_selected(dialog, label)

    assert dialog.host_vernacular_input.text() == "Button mushroom"
    assert dialog.host_genus_input.text() == "Agaricus"
    assert dialog.host_species_input.text() == "bisporus"

    dialog.deleteLater()


def test_host_taxon_focus_populates_blank_prefix_species_and_common_name_models(
    tmp_path: Path,
    monkeypatch,
    qapp,
) -> None:
    dialog = _seeded_dialog(tmp_path, monkeypatch)

    species_calls: list[tuple[str, str, int]] = []
    common_calls: list[tuple[str, str | None, str | None, int]] = []
    complete_calls: list[str] = []

    def _suggest_species(genus: str, prefix: str, limit: int = 200) -> list[TaxonChoice]:
        species_calls.append((genus, prefix, limit))
        assert prefix in ("", "bi")
        return [TaxonChoice(genus="Agaricus", species="bisporus", common_name="Button mushroom")]

    def _suggest_common_names(
        prefix: str = "",
        genus: str | None = None,
        species: str | None = None,
        limit: int = 200,
    ) -> list[TaxonChoice]:
        common_calls.append((prefix, genus, species, limit))
        assert prefix in ("", "Button mushroom")
        return [
            TaxonChoice(genus="Agaricus", species="bisporus", common_name="Cultivated mushroom"),
            TaxonChoice(genus="Agaricus", species="bisporus", common_name="Button mushroom"),
        ]

    monkeypatch.setattr(dialog._taxon_lookup, "suggest_species", _suggest_species)
    monkeypatch.setattr(dialog._taxon_lookup, "suggest_common_names", _suggest_common_names)
    monkeypatch.setattr(
        dialog._taxon_lookup,
        "best_common_name_for_taxon",
        lambda genus, species: TaxonChoice(genus=genus, species=species, common_name="Button mushroom"),
    )
    monkeypatch.setattr(dialog._host_genus_completer, "complete", lambda: complete_calls.append("host_genus"))
    monkeypatch.setattr(dialog._host_species_completer, "complete", lambda: complete_calls.append("host_species"))
    monkeypatch.setattr(dialog._host_vernacular_completer, "complete", lambda: complete_calls.append("host_vernacular"))

    dialog.show()
    qapp.processEvents()

    dialog.host_genus_input.blockSignals(True)
    dialog.host_genus_input.setText("Agaricus")
    dialog.host_genus_input.blockSignals(False)
    dialog.host_genus_input.setFocus()
    qapp.processEvents()
    qapp.processEvents()
    ObservationDetailsDialog.eventFilter(dialog, dialog.host_genus_input, QEvent(QEvent.FocusIn))
    qapp.processEvents()

    dialog.host_species_input.blockSignals(True)
    dialog.host_species_input.setText("bi")
    dialog.host_species_input.blockSignals(False)
    dialog.host_species_input.setFocus()
    qapp.processEvents()
    ObservationDetailsDialog.eventFilter(dialog, dialog.host_species_input, QEvent(QEvent.FocusIn))
    qapp.processEvents()
    assert species_calls == [("Agaricus", "bi", 200)]
    assert dialog._host_species_model.stringList() == ["bisporus"]
    assert "host_species" in complete_calls
    assert dialog.host_species_input.selectedText() == "bi"

    common_calls.clear()
    dialog.host_species_input.blockSignals(True)
    dialog.host_species_input.setText("bisporus")
    dialog.host_species_input.blockSignals(False)
    dialog.host_vernacular_input.blockSignals(True)
    dialog.host_vernacular_input.setText("")
    dialog.host_vernacular_input.blockSignals(False)
    dialog.host_vernacular_input.setFocus()
    qapp.processEvents()

    ObservationDetailsDialog.eventFilter(dialog, dialog.host_vernacular_input, QEvent(QEvent.FocusIn))
    assert common_calls == [("", "Agaricus", "bisporus", 200)]
    assert dialog._host_vernacular_model.stringList() == [
        "Cultivated mushroom",
        "Button mushroom",
    ]
    dialog.host_vernacular_input.blockSignals(True)
    dialog.host_vernacular_input.setText("Button mushroom")
    dialog.host_vernacular_input.blockSignals(False)
    dialog.host_vernacular_input.setFocus()
    qapp.processEvents()
    ObservationDetailsDialog.eventFilter(dialog, dialog.host_vernacular_input, QEvent(QEvent.FocusIn))
    qapp.processEvents()
    assert dialog.host_vernacular_input.selectedText() == "Button mushroom"

    common_calls.clear()
    dialog.host_vernacular_input.blockSignals(True)
    dialog.host_vernacular_input.setText("")
    dialog.host_vernacular_input.blockSignals(False)
    ObservationDetailsDialog._maybe_set_host_vernacular_from_taxon(dialog)

    assert common_calls == [("", "Agaricus", "bisporus", 200)]
    assert dialog.host_vernacular_input.text() == "Button mushroom"

    dialog.deleteLater()


def test_common_name_selection_uses_hidden_choice_payload(tmp_path: Path, monkeypatch, qapp) -> None:
    dialog = _seeded_dialog(tmp_path, monkeypatch)

    dialog.genus_input.setText("")
    dialog.species_input.setText("")
    dialog.vernacular_input.setText("")
    ObservationDetailsDialog._on_vernacular_text_changed(dialog, "But")

    assert dialog._vernacular_model.stringList() == ["Button mushroom"]
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

    assert dialog.vernacular_input.text() == ""
    assert dialog.genus_input.text() == "Agaricus"
    assert dialog.species_input.text() == "preserve me"

    dialog.deleteLater()


def test_host_common_name_selection_handles_choice_without_species(
    tmp_path: Path,
    monkeypatch,
    qapp,
) -> None:
    dialog = _seeded_dialog(tmp_path, monkeypatch)

    choice = TaxonChoice(genus="Agaricus", species=None, common_name="Button mushroom")
    monkeypatch.setattr(
        dialog._host_taxon_controller.lookup,
        "resolve_common_name",
        lambda _name, *_args, **_kwargs: [choice],
    )
    dialog.host_genus_input.blockSignals(True)
    dialog.host_genus_input.setText("")
    dialog.host_genus_input.blockSignals(False)
    dialog.host_species_input.blockSignals(True)
    dialog.host_species_input.setText("preserve me")
    dialog.host_species_input.blockSignals(False)

    ObservationDetailsDialog._set_host_taxon_from_vernacular(dialog, "Button mushroom")

    assert dialog.host_vernacular_input.text() == "Button mushroom"
    assert dialog.host_genus_input.text() == ""
    assert dialog.host_species_input.text() == "preserve me"

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
    assert dialog._vernacular_model.stringList() == ["Silky entoloma"]

    dialog.deleteLater()


def test_host_common_name_autofill_is_safe_when_multiple_names(
    tmp_path: Path,
    monkeypatch,
    qapp,
) -> None:
    dialog = _seeded_dialog(tmp_path, monkeypatch)

    dialog.host_genus_input.blockSignals(True)
    dialog.host_genus_input.setText("Coprinus")
    dialog.host_genus_input.blockSignals(False)
    dialog.host_species_input.blockSignals(True)
    dialog.host_species_input.setText("comatus")
    dialog.host_species_input.blockSignals(False)
    dialog.host_vernacular_input.setText("")

    ObservationDetailsDialog._refresh_host_vernacular_for_current_taxon(dialog)

    assert dialog.host_vernacular_input.text() == ""
    assert dialog._host_vernacular_model.stringList() == [
        "Lawyer's wig",
        "Shaggy mane",
    ]

    dialog.deleteLater()


def test_setup_is_safe_without_vernacular_db(tmp_path: Path, monkeypatch, qapp) -> None:
    reference_db_path = tmp_path / "reference.sqlite"
    _seed_reference_db(reference_db_path)
    _configure_dialog_environment(monkeypatch, None)
    monkeypatch.setattr(observations_tab, "list_available_vernacular_languages", lambda: ["no"])
    monkeypatch.setattr(observations_tab, "resolve_vernacular_db_path", lambda _lang: None)
    monkeypatch.setattr(models, "get_reference_connection", lambda: sqlite3.connect(reference_db_path))
    dialog = _build_dialog()

    dialog._setup_vernacular_autocomplete()
    dialog._setup_host_autocomplete()

    assert dialog._taxon_lookup is not None
    assert dialog._taxon_lookup.vernacular_db is None
    assert dialog._genus_completer is not None
    assert dialog._species_completer is not None
    assert dialog._vernacular_completer is not None
    assert dialog._host_genus_completer is not None
    assert dialog._host_species_completer is not None
    assert dialog._host_vernacular_completer is not None

    genus_complete_calls: list[str] = []
    species_complete_calls: list[str] = []
    host_species_complete_calls: list[str] = []
    host_genus_complete_calls: list[str] = []
    monkeypatch.setattr(dialog._genus_completer, "complete", lambda: genus_complete_calls.append("genus"))
    monkeypatch.setattr(dialog._species_completer, "complete", lambda: species_complete_calls.append("species"))
    monkeypatch.setattr(dialog._host_genus_completer, "complete", lambda: host_genus_complete_calls.append("host_genus"))
    monkeypatch.setattr(dialog._host_species_completer, "complete", lambda: host_species_complete_calls.append("host_species"))

    dialog.show()
    qapp.processEvents()

    dialog.genus_input.setFocus()
    qapp.processEvents()
    ObservationDetailsDialog.eventFilter(dialog, dialog.genus_input, QEvent(QEvent.FocusIn))
    assert dialog._genus_model.stringList()
    assert genus_complete_calls

    dialog.genus_input.setText("Agaricus")
    dialog.species_input.setFocus()
    qapp.processEvents()
    ObservationDetailsDialog.eventFilter(dialog, dialog.species_input, QEvent(QEvent.FocusIn))
    assert dialog._species_model.stringList() == ["bisporus"]
    assert species_complete_calls

    dialog.host_genus_input.setText("Agaricus")
    dialog.host_species_input.setFocus()
    qapp.processEvents()
    ObservationDetailsDialog.eventFilter(dialog, dialog.host_species_input, QEvent(QEvent.FocusIn))
    assert dialog._host_species_model.stringList() == ["bisporus"]
    assert host_species_complete_calls

    dialog.host_genus_input.setText("")
    dialog.host_genus_input.setFocus()
    qapp.processEvents()
    ObservationDetailsDialog.eventFilter(dialog, dialog.host_genus_input, QEvent(QEvent.FocusIn))
    assert dialog._host_genus_model.stringList()
    assert host_genus_complete_calls

    dialog.vernacular_input.setFocus()
    qapp.processEvents()
    ObservationDetailsDialog.eventFilter(dialog, dialog.vernacular_input, QEvent(QEvent.FocusIn))
    assert dialog._vernacular_model.rowCount() == 0

    dialog.deleteLater()
