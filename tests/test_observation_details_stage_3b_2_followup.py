"""Stage 3B.2 follow-up UI-integration tests.

Scientific/vernacular independence, free-text safeguards, chooser
opening, ambiguous-canonical-name identity preservation.
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QEvent, Qt, QPoint
from PySide6.QtGui import QFocusEvent, QMouseEvent
from PySide6.QtWidgets import QApplication, QDialog, QLineEdit, QLabel

import database.models as models
from database.taxon_lookup import TaxonChoice
import ui.observations_tab as observations_tab
from ui.observations_tab import (
    ObservationDetailsDialog,
    _format_observation_display_label,
)


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


# -------------------------- helpers ---------------------------------------


class _Dlg(ObservationDetailsDialog):
    def __init__(self):
        QDialog.__init__(self)
        self._style_dropdown_popup_readability = lambda *args, **kwargs: None
        self._suppress_taxon_autofill = False
        self._host_suppress_taxon_autofill = False
        self._last_genus = ""
        self._last_species = ""


def _seed(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE taxon_min (
            taxon_id INTEGER PRIMARY KEY,
            genus TEXT, specific_epithet TEXT, family TEXT,
            canonical_scientific_name TEXT
        );
        CREATE TABLE scientific_name_min (
            scientific_name_id INTEGER PRIMARY KEY AUTOINCREMENT,
            taxon_id INTEGER NOT NULL,
            language_code TEXT NOT NULL,
            scientific_name TEXT NOT NULL,
            is_preferred_name INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE vernacular_min (
            vernacular_id INTEGER PRIMARY KEY AUTOINCREMENT,
            taxon_id INTEGER NOT NULL,
            language_code TEXT NOT NULL,
            vernacular_name TEXT NOT NULL,
            is_preferred_name INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE taxonomy_meta (key TEXT PRIMARY KEY, value TEXT);
        """
    )
    conn.executemany("INSERT INTO taxon_min VALUES (?, ?, ?, ?, ?)", [
        (133345, "Candolleomyces", "candolleanus", "Psathyrellaceae",
         "Candolleomyces candolleanus"),
        # Two Sporely IDs share "Laccaria laccata" — Stage 3A conservative rule.
        (103805, "Laccaria", "laccata", "Hydnangiaceae", "Laccaria laccata"),
        (625355, "Laccaria", "laccata", "Hydnangiaceae", "Laccaria laccata"),
    ])
    conn.executemany(
        "INSERT INTO scientific_name_min (taxon_id, language_code, "
        "scientific_name, is_preferred_name) VALUES (?, ?, ?, ?)", [
            (133345, "sci", "Candolleomyces candolleanus", 1),
            (103805, "sci", "Laccaria laccata", 1),
            (625355, "sci", "Laccaria laccata", 1),
        ])
    conn.executemany(
        "INSERT INTO vernacular_min (taxon_id, language_code, "
        "vernacular_name, is_preferred_name) VALUES (?, ?, ?, ?)", [
            (133345, "nb", "hvit sprøsopp", 1),
            (133345, "nn", "kvit sprøsopp", 1),
            (625355, "nb", "lakssopp", 1),
            (625355, "nn", "lakssopp", 1),
            (625355, "en", "deceiver", 1),
        ])
    conn.executemany("INSERT INTO taxonomy_meta VALUES (?, ?)", [
        ("taxonomy_schema_version", "2"),
        ("state", "candidate"),
    ])
    conn.commit(); conn.close()


def _configure(monkeypatch, db_path: Path, language: str = "no") -> None:
    monkeypatch.setattr(
        observations_tab.SettingsDB, "get_setting",
        lambda key, default=None: language if key == "vernacular_language" else default,
    )
    monkeypatch.setattr(observations_tab, "resolve_vernacular_db_path",
                        lambda _lang: db_path)


def _build_dlg() -> _Dlg:
    d = _Dlg()
    d.vernacular_input = QLineEdit(d)
    d.genus_input = QLineEdit(d)
    d.species_input = QLineEdit(d)
    d.host_genus_input = QLineEdit(d)
    d.host_species_input = QLineEdit(d)
    d.host_vernacular_input = QLineEdit(d)
    d.host_vernacular_label = QLabel(d)
    return d


def _dialog(tmp_path: Path, monkeypatch, *, language: str = "no") -> _Dlg:
    db = tmp_path / "v2.sqlite3"
    _seed(db)
    _configure(monkeypatch, db, language)
    d = _build_dlg()
    d._setup_vernacular_autocomplete()
    d._setup_host_autocomplete()
    return d


# ---------------------- 1. scientific + vernacular independent -----------


def test_display_label_shows_both_common_and_scientific(qapp) -> None:
    assert _format_observation_display_label(
        "hvit sprøsopp", "Candolleomyces", "candolleanus"
    ) == "Hvit sprøsopp\nCandolleomyces candolleanus"


def test_display_label_scientific_only_when_common_missing(qapp) -> None:
    assert _format_observation_display_label(
        "", "Candolleomyces", "candolleanus"
    ) == "Candolleomyces candolleanus"


def test_display_label_common_only_when_scientific_missing(qapp) -> None:
    assert _format_observation_display_label("hvit sprøsopp", "", "") == "Hvit sprøsopp"


def test_display_label_capitalizes_first_letter_but_preserves_rest(qapp) -> None:
    # A word with internal capitals or spaces stays intact.
    assert _format_observation_display_label(
        "čáhppesguoppar", "Genus", "sp"
    ) == "Čáhppesguoppar\nGenus sp"
    # Existing capitalization isn't downcased.
    assert _format_observation_display_label(
        "SPRØSOPP", "Genus", "sp"
    ) == "SPRØSOPP\nGenus sp"


def test_display_label_dash_when_nothing_known(qapp) -> None:
    assert _format_observation_display_label("", "", "") == "-"


# ---------------------- 2. free-text vernacular preserves identity --------


def test_typing_custom_vernacular_does_not_clear_taxon(tmp_path, monkeypatch, qapp) -> None:
    d = _dialog(tmp_path, monkeypatch)
    d.genus_input.setText("Candolleomyces")
    d.species_input.setText("candolleanus")
    d.vernacular_input.setText("")
    # Simulate user typing free-text that has no matches for this taxon.
    ObservationDetailsDialog._on_vernacular_text_changed(d, "custom-nickname-xyz")
    # Genus and species must remain intact — the previous
    # `_clear_scientific_taxon_for_vernacular_search` side effect is gone.
    assert d.genus_input.text() == "Candolleomyces"
    assert d.species_input.text() == "candolleanus"
    d.deleteLater()


def test_clearing_vernacular_does_not_clear_taxon(tmp_path, monkeypatch, qapp) -> None:
    d = _dialog(tmp_path, monkeypatch)
    d.genus_input.setText("Candolleomyces")
    d.species_input.setText("candolleanus")
    d.vernacular_input.setText("hvit sprøsopp")
    # Actually clear the vernacular field and fire the changed handler.
    d.vernacular_input.setText("")
    ObservationDetailsDialog._on_vernacular_text_changed(d, "")
    assert d.genus_input.text() == "Candolleomyces"
    assert d.species_input.text() == "candolleanus"
    assert d.vernacular_input.text() == ""
    d.deleteLater()


# ---------------------- 3. chooser opens ---------------------------------


def test_focus_opens_vernacular_chooser_with_empty_prefix(tmp_path, monkeypatch, qapp) -> None:
    d = _dialog(tmp_path, monkeypatch)
    d.genus_input.setText("Candolleomyces")
    d.species_input.setText("candolleanus")
    d._taxon_controller.sync_vernacular_after_taxon_change()

    complete_calls: list[str] = []
    monkeypatch.setattr(
        d._vernacular_completer, "complete",
        lambda: complete_calls.append(d._vernacular_completer.completionPrefix()),
    )
    event = QFocusEvent(QEvent.FocusIn, Qt.MouseFocusReason)
    d._taxon_controller.eventFilter(d.vernacular_input, event)
    assert complete_calls, "focus-in must open the chooser"
    # Chooser opens with an EMPTY prefix so all alternatives show.
    assert complete_calls[0] == ""
    d.deleteLater()


def test_mouseclick_reopens_chooser_when_already_focused(tmp_path, monkeypatch, qapp) -> None:
    d = _dialog(tmp_path, monkeypatch)
    d.genus_input.setText("Candolleomyces")
    d.species_input.setText("candolleanus")
    d._taxon_controller.sync_vernacular_after_taxon_change()
    calls: list[int] = []
    monkeypatch.setattr(
        d._vernacular_completer, "complete", lambda: calls.append(1))
    press = QMouseEvent(
        QEvent.MouseButtonPress, QPoint(0, 0), Qt.LeftButton,
        Qt.LeftButton, Qt.NoModifier,
    )
    d._taxon_controller.eventFilter(d.vernacular_input, press)
    assert calls, "mouse press must reopen the chooser"
    d.deleteLater()


# --------- 4. selecting an alternative changes ONLY common_name -----------


def test_selecting_alternative_preserves_taxon(tmp_path, monkeypatch, qapp) -> None:
    d = _dialog(tmp_path, monkeypatch)
    d.genus_input.setText("Candolleomyces")
    d.species_input.setText("candolleanus")
    d.vernacular_input.setText("hvit sprøsopp")
    d._taxon_controller.sync_vernacular_after_taxon_change()
    # Snapshot identity BEFORE the alternative selection.
    before = (d.genus_input.text(), d.species_input.text())

    # Simulate picking "kvit sprøsopp (nn)" from the chooser: on_vernacular_selected
    # is called by the completer's activated signal with the row's index.
    # Instead of driving Qt fully we invoke the controller method with a
    # crafted TaxonChoice — same effect.
    from PySide6.QtCore import QModelIndex
    from PySide6.QtGui import QStandardItem
    from ui.taxon_input_controller import ROLE_TAXON_CHOICE
    choice = TaxonChoice(
        genus="Candolleomyces", species="candolleanus",
        common_name="kvit sprøsopp", language_code="nn",
    )
    # Locate the row in the vernacular model that corresponds to the choice
    # so we can pass a real QModelIndex.
    model = d._vernacular_model
    idx = None
    for row in range(model.rowCount()):
        item = model.item(row, 0)
        payload = item.data(ROLE_TAXON_CHOICE) if item else None
        if isinstance(payload, TaxonChoice) and payload.common_name == "kvit sprøsopp":
            idx = model.indexFromItem(item)
            break
    assert idx is not None, "kvit sprøsopp must be present in the chooser"
    d._taxon_controller.on_vernacular_selected(idx)
    # Taxon fields UNCHANGED. Only the vernacular snapshot rotates.
    assert (d.genus_input.text(), d.species_input.text()) == before
    assert d.vernacular_input.text() == "kvit sprøsopp"
    d.deleteLater()


# --------- 5. two Laccaria laccata identities not collapsed ---------------


def test_ambiguous_scientific_name_does_not_silently_bind(tmp_path, monkeypatch, qapp) -> None:
    from database.vernacular_db import VernacularDB
    db_path = tmp_path / "v2.sqlite3"
    _seed(db_path)
    db = VernacularDB(db_path, language_code="no")
    # Every entrypoint that a UI callsite might use returns either both
    # candidates (so a chooser can be presented) or None (so the caller
    # refuses to bind).
    assert db.taxon_ids_from_scientific("Laccaria", "laccata") == [103805, 625355]
    assert db.taxon_id_from_scientific("Laccaria", "laccata") is None
    # Loading alternatives strictly by ID keeps the two partitions separate.
    assert db.list_vernacular_alternatives(103805) == []
    names_625355 = {r["vernacular_name"] for r in db.list_vernacular_alternatives(625355)}
    assert names_625355 == {"lakssopp", "deceiver"}


# --------- 6. capitalization visible in editor, storage unchanged ---------


def test_display_capitalization_does_not_mutate_storage(tmp_path, monkeypatch, qapp) -> None:
    from utils.vernacular_utils import display_vernacular_name
    db_path = tmp_path / "v2.sqlite3"
    _seed(db_path)
    # Editor-facing display capitalizes.
    assert display_vernacular_name("hvit sprøsopp") == "Hvit sprøsopp"
    # Storage row unchanged.
    with sqlite3.connect(str(db_path)) as conn:
        rows = list(conn.execute(
            "SELECT vernacular_name FROM vernacular_min WHERE taxon_id=133345"))
    stored_names = {r[0] for r in rows}
    assert "hvit sprøsopp" in stored_names
    assert "Hvit sprøsopp" not in stored_names
