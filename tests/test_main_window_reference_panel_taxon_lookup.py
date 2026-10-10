from __future__ import annotations

import os
import sqlite3
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import Qt, QEvent, QStringListModel
from PySide6.QtGui import QStandardItemModel
from PySide6.QtWidgets import (
    QApplication,
    QAbstractItemView,
    QComboBox,
    QCompleter,
    QHeaderView,
    QLabel,
    QLineEdit,
    QSizePolicy,
    QTableWidget,
    QToolButton,
)

import ui.main_window as main_window
from database.taxon_lookup import TaxonChoice
from database.vernacular_db import VernacularDB
from ui.comparison_panel import ComparisonListWidget
from ui.taxon_input_controller import TaxonInputController


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


class _EmptySpeciesAvailability:
    def get_cache(self, force_refresh: bool = False):
        return {}

    def get_detailed_info(
        self,
        genus: str,
        species: str,
        exclude_observation_id: int | None = None,
    ) -> dict:
        return {}

    def get_species_display_name(
        self,
        genus: str,
        species: str,
        exclude_observation_id: int | None = None,
    ) -> tuple[str, bool]:
        return (f"{genus} {species}".strip(), False)


class _FailingVernacularDB:
    def __init__(self, db_path: Path | None = None):
        self.db_path = db_path
        self.language_code = "en"

    def __getattr__(self, name: str):
        raise AssertionError(f"MainWindow should not call VernacularDB.{name}")


class _ReferenceLookupStub:
    def __init__(
        self,
        *,
        genera: list[str],
        species: list[TaxonChoice],
        common_names: list[TaxonChoice],
        resolve_results: list[TaxonChoice] | None = None,
    ):
        self.genera = list(genera)
        self.species = list(species)
        self.common_names = list(common_names)
        self.resolve_results = list(resolve_results or [])
        self.calls: list[tuple] = []
        self.language_code = "en"
        self.vernacular_db = object()

    def suggest_genera(self, prefix: str = "", limit: int = 200) -> list[str]:
        self.calls.append(("suggest_genera", prefix, limit))
        return list(self.genera)

    def suggest_species(self, genus: str, prefix: str = "", limit: int = 200) -> list[TaxonChoice]:
        self.calls.append(("suggest_species", genus, prefix, limit))
        return list(self.species)

    def suggest_common_names(
        self,
        prefix: str = "",
        genus: str | None = None,
        species: str | None = None,
        limit: int = 200,
    ) -> list[TaxonChoice]:
        self.calls.append(("suggest_common_names", prefix, genus, species, limit))
        return list(self.common_names)

    def resolve_common_name(
        self,
        name: str,
        genus: str | None = None,
        species: str | None = None,
    ) -> list[TaxonChoice]:
        self.calls.append(("resolve_common_name", name, genus, species))
        return list(self.resolve_results)


def _build_minimal_window(
    monkeypatch,
    qapp,
    *,
    vernacular_language: str = "no",
) -> main_window.MainWindow:
    monkeypatch.setattr(main_window, "SpeciesDataAvailability", _EmptySpeciesAvailability)
    monkeypatch.setattr(
        main_window.SettingsDB,
        "get_setting",
        lambda key, default=None: vernacular_language if key == "vernacular_language" else default,
    )
    monkeypatch.setattr(main_window.MainWindow, "init_ui", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_populate_scale_combo", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "load_default_objective", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_restore_geometry", lambda self: None)
    window = main_window.MainWindow()
    window.ref_vernacular_label = QLabel()
    window.ref_vernacular_input = QLineEdit()
    window.ref_genus_input = QLineEdit()
    window.ref_species_input = QLineEdit()
    window.ref_source_input = QComboBox()
    window.ref_source_input.setEditable(True)
    window.ref_source_input.setInsertPolicy(QComboBox.NoInsert)
    window.ref_source_input.addItem("")
    window.ref_source_input.setCurrentIndex(0)
    window.table = QTableWidget(3, 5)
    window.ref_table = window.table
    window._ref_genus_model = QStringListModel()
    window._ref_species_model = QStandardItemModel()
    window._ref_vernacular_model = QStandardItemModel()
    window._ref_genus_completer = QCompleter(window._ref_genus_model, window)
    window._ref_genus_completer.setCaseSensitivity(Qt.CaseInsensitive)
    window._ref_species_completer = QCompleter(window._ref_species_model, window)
    window._ref_species_completer.setCaseSensitivity(Qt.CaseInsensitive)
    window._ref_species_completer.setCompletionRole(Qt.UserRole)
    window._ref_vernacular_completer = QCompleter(window._ref_vernacular_model, window)
    window._ref_vernacular_completer.setCaseSensitivity(Qt.CaseInsensitive)
    window._ref_vernacular_completer.setCompletionRole(Qt.UserRole)
    window._ref_completer_suppress = False
    window._ref_taxon_fill_from_vernacular = False
    window._reference_taxon_lookup = None
    window._ref_genus_summary_cache_key = None
    window._ref_genus_summary_cache = {}
    window._gallery_hint_controller = None
    window._pending_gallery_hint_widgets = []
    window.reference_series = []
    window.reference_values = {}
    window.gallery_plot_settings = {}
    window.species_availability = _EmptySpeciesAvailability()
    window.active_observation_id = None
    window._populate_reference_panel_sources = lambda auto_select_single=True: None
    window._update_reference_add_state = lambda: None
    return window


def _seed_reference_lookup_db(db_path: Path) -> None:
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
        conn.execute(
            """
            INSERT INTO taxon_min (taxon_id, genus, specific_epithet, family, canonical_scientific_name)
            VALUES (1, 'Agaricus', 'bisporus', 'Agaricaceae', 'Agaricus bisporus')
            """
        )
        conn.execute(
            """
            INSERT INTO vernacular_min (taxon_id, vernacular_name, is_preferred_name, language_code)
            VALUES (1, 'Button mushroom', 1, 'en')
            """
        )
        conn.execute(
            """
            INSERT INTO scientific_name_min (taxon_id, scientific_name, is_preferred_name)
            VALUES (1, 'Agaricus bisporus', 1)
            """
        )


def test_reference_panel_ensure_lookup_reuses_cached_service(tmp_path: Path, monkeypatch, qapp) -> None:
    lookup_db = tmp_path / "lookup.sqlite"
    _seed_reference_lookup_db(lookup_db)

    monkeypatch.setattr(
        main_window.SettingsDB,
        "get_setting",
        lambda key, default=None: "en" if key == "vernacular_language" else default,
    )

    window = _build_minimal_window(monkeypatch, qapp, vernacular_language="en")
    window.ref_vernacular_db = VernacularDB(lookup_db, language_code="en")

    lookup1 = window._ensure_reference_taxon_lookup()
    lookup2 = window._ensure_reference_taxon_lookup()

    assert lookup1 is lookup2
    assert lookup1.vernacular_db is window.ref_vernacular_db
    assert lookup1.language_code == "en"

    monkeypatch.setattr(
        main_window.SettingsDB,
        "get_setting",
        lambda key, default=None: "no" if key == "vernacular_language" else default,
    )

    lookup3 = window._ensure_reference_taxon_lookup()

    assert lookup3 is lookup1
    assert lookup3.language_code == "no"
    assert window.ref_vernacular_db.language_code == "no"


def test_comparison_list_shows_row_and_marks_it_editable(
    monkeypatch,
    qapp,
) -> None:
    """Stage 6: the comparison list replaced ``ref_series_table`` as the
    sole reference-plot list surface; this row-level data (not a removed
    table's per-column widgets) is now what the overflow menu's actions
    key off of (see ui.comparison_panel._open_overflow_menu)."""
    window = _build_minimal_window(monkeypatch, qapp)
    window.comparison_list = ComparisonListWidget()

    window.reference_series = [
        {
            "data": {
                "genus": "Agaricus",
                "species": "bisporus",
                "source": "Reference sheet",
                "source_kind": "reference",
            },
            "enabled": True,
        }
    ]

    window._refresh_reference_series_table()

    rows = window.comparison_list.rows()
    assert len(rows) == 1
    row = rows[0]
    assert row.title == "A. bisporus (Reference sheet)"
    assert row.visible is True
    # A plain ReferenceDB-backed row (no observation_reference_use_id) is
    # the "supported legacy row" _edit_reference_series_row can open.
    assert row.can_edit is True
    assert row.library_update_available is False
    assert row.library_successor_available is False


def test_comparison_list_offers_explicit_update_only_for_stale_attachment(
    monkeypatch, qapp
) -> None:
    window = _build_minimal_window(monkeypatch, qapp)
    window.comparison_list = ComparisonListWidget()
    window.comparison_list.library_update_requested.connect(
        window._on_comparison_library_update_requested
    )
    window.active_observation_id = 42
    window.reference_series = [
        {
            "key": "use-1",
            "data": {
                "source_kind": "reference",
                "observation_reference_use_id": "use-1",
                "reference_measurement_set_id": "set-1",
                "short_label": "Author 2000",
                "library_snapshot_state": "update_available",
                "library_update_available": True,
            },
            "label": "Author 2000",
            "enabled": True,
        }
    ]
    calls: list[tuple[str, int]] = []
    window._update_reference_use_from_library = (
        lambda use_id, observation_id: calls.append((use_id, observation_id))
    )

    window._refresh_reference_series_table()

    row = next(r for r in window.comparison_list.rows() if r.dataset_id == "use-1")
    assert row.library_update_available is True
    assert row.library_successor_available is False
    # Normalized library attachments are not editable via the row-driven
    # legacy-form adapter; they go through library management instead.
    assert row.can_edit is False

    window.comparison_list.library_update_requested.emit(row.observation_reference_use_id)
    assert calls == [("use-1", 42)]


def test_comparison_list_offers_distinct_successor_review_action(
    monkeypatch, qapp
) -> None:
    window = _build_minimal_window(monkeypatch, qapp)
    window.comparison_list = ComparisonListWidget()
    window.comparison_list.library_successor_requested.connect(
        window._on_comparison_library_successor_requested
    )
    window.active_observation_id = 42
    window.reference_series = [
        {
            "key": "use-1",
            "data": {
                "source_kind": "reference",
                "observation_reference_use_id": "use-1",
                "reference_measurement_set_id": "set-old",
                "short_label": "Author 2000",
                "library_update_available": False,
                "library_successor_available": True,
                "library_successor_id": "set-new",
            },
            "label": "Author 2000",
            "enabled": True,
        }
    ]
    calls: list[tuple[str, int]] = []
    window._review_reference_successor = (
        lambda use_id, observation_id: calls.append((use_id, observation_id))
    )

    window._refresh_reference_series_table()

    row = next(r for r in window.comparison_list.rows() if r.dataset_id == "use-1")
    assert row.library_update_available is False
    assert row.library_successor_available is True

    window.comparison_list.library_successor_requested.emit(row.observation_reference_use_id)
    assert calls == [("use-1", 42)]


def test_reference_panel_taxon_lookup_suggestions_and_hidden_choice_selection(
    monkeypatch,
    qapp,
) -> None:
    window = _build_minimal_window(monkeypatch, qapp)
    window.ref_vernacular_db = _FailingVernacularDB()

    lookup = _ReferenceLookupStub(
        genera=["Agaricus", "Entoloma"],
        species=[
            TaxonChoice(
                genus="Agaricus",
                species="bisporus",
                common_name="Button mushroom",
                family="Agaricaceae",
            )
        ],
        common_names=[
            TaxonChoice(
                genus="Agaricus",
                species="bisporus",
                common_name="Button mushroom",
                family="Agaricaceae",
            ),
            TaxonChoice(
                genus="Agaricus",
                species="bisporus",
                common_name="Cultivated mushroom",
                family="Agaricaceae",
            ),
        ],
    )
    window._ensure_reference_taxon_lookup = lambda: lookup

    monkeypatch.setattr(
        main_window.ReferenceDB,
        "list_genera",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("genus suggestions should use TaxonLookupService")),
    )
    monkeypatch.setattr(
        main_window.ReferenceDB,
        "list_species",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("species suggestions should use TaxonLookupService")),
    )

    genus_values = window._update_ref_genus_suggestions("Ag")
    species_values = window._update_ref_species_suggestions("Agaricus", "bi")

    assert genus_values == ["Agaricus", "Entoloma"]
    assert window._ref_genus_model.stringList() == ["Agaricus", "Entoloma"]
    assert species_values == ["bisporus"]
    assert window._ref_species_model.rowCount() == 1
    assert window._ref_species_model.item(0).text() == "bisporus"
    assert window._ref_species_model.item(0).data(Qt.UserRole) == "bisporus"

    window.ref_genus_input.setText("Agaricus")
    window.ref_species_input.setText("bisporus")
    window._update_ref_vernacular_suggestions_for_taxon()

    assert window._ref_vernacular_model.rowCount() == 2
    choice = window._ref_vernacular_model.item(0).data(window._ROLE_TAXON_CHOICE)
    assert isinstance(choice, TaxonChoice)
    assert choice.common_name == "Button mushroom"

    monkeypatch.setattr(
        lookup,
        "resolve_common_name",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("selection should use hidden TaxonChoice data")
        ),
    )

    window.ref_genus_input.clear()
    window.ref_species_input.clear()
    window._on_ref_vernacular_selected(window._ref_vernacular_model.index(0, 0))

    assert window.ref_vernacular_input.text() == "Button mushroom"
    assert window.ref_genus_input.text() == "Agaricus"
    assert window.ref_species_input.text() == "bisporus"


def test_reference_panel_focus_populates_blank_prefix_species_and_common_name_models(
    monkeypatch,
    qapp,
) -> None:
    window = _build_minimal_window(monkeypatch, qapp)
    window.ref_vernacular_db = _FailingVernacularDB()

    lookup = _ReferenceLookupStub(
        genera=["Agaricus"],
        species=[
            TaxonChoice(
                genus="Agaricus",
                species="bisporus",
                common_name="Button mushroom",
                family="Agaricaceae",
            )
        ],
        common_names=[
            TaxonChoice(
                genus="Agaricus",
                species="bisporus",
                common_name="Cultivated mushroom",
                family="Agaricaceae",
            ),
            TaxonChoice(
                genus="Agaricus",
                species="bisporus",
                common_name="Button mushroom",
                family="Agaricaceae",
            ),
        ],
    )
    lookup.best_common_name_for_taxon = lambda genus, species: TaxonChoice(
        genus=genus,
        species=species,
        common_name="Button mushroom",
        family="Agaricaceae",
    )
    window._ensure_reference_taxon_lookup = lambda: lookup
    window._reference_taxon_controller = TaxonInputController(
        lookup,
        window.ref_genus_input,
        window.ref_species_input,
        window.ref_vernacular_input,
        window,
    )
    window._ref_genus_model = window._reference_taxon_controller.genus_model
    window._ref_species_model = window._reference_taxon_controller.species_model
    window._ref_vernacular_model = window._reference_taxon_controller.vernacular_model
    window._ref_genus_completer = window._reference_taxon_controller.genus_completer
    window._ref_species_completer = window._reference_taxon_controller.species_completer
    window._ref_vernacular_completer = window._reference_taxon_controller.vernacular_completer
    complete_calls: list[str] = []
    monkeypatch.setattr(window._ref_genus_completer, "complete", lambda *args, **kwargs: complete_calls.append("genus"))
    monkeypatch.setattr(window._ref_species_completer, "complete", lambda *args, **kwargs: complete_calls.append("species"))
    monkeypatch.setattr(window._ref_vernacular_completer, "complete", lambda *args, **kwargs: complete_calls.append("vernacular"))

    window.show()
    qapp.processEvents()

    window.ref_genus_input.blockSignals(True)
    window.ref_genus_input.setText("Aga")
    window.ref_genus_input.blockSignals(False)
    window.ref_genus_input.setFocus()
    qapp.processEvents()
    window.eventFilter(window.ref_genus_input, QEvent(QEvent.FocusIn))
    qapp.processEvents()
    assert ("suggest_genera", "Aga", 200) in lookup.calls
    assert "genus" in complete_calls
    assert window.ref_genus_input.selectedText() == "Aga"

    window.ref_genus_input.blockSignals(True)
    window.ref_genus_input.setText("Agaricus")
    window.ref_genus_input.blockSignals(False)
    window.ref_species_input.blockSignals(True)
    window.ref_species_input.setText("bi")
    window.ref_species_input.blockSignals(False)
    window.ref_species_input.setFocus()
    qapp.processEvents()
    window.eventFilter(window.ref_species_input, QEvent(QEvent.FocusIn))
    qapp.processEvents()

    assert ("suggest_species", "Agaricus", "bi", 200) in lookup.calls
    assert window._ref_species_model.rowCount() == 1
    assert window._ref_species_model.item(0).data(Qt.UserRole) == "bisporus"
    assert "species" in complete_calls
    assert window.ref_species_input.selectedText() == "bi"

    window.ref_species_input.blockSignals(True)
    window.ref_species_input.setText("bisporus")
    window.ref_species_input.blockSignals(False)
    window.eventFilter(window.ref_vernacular_input, QEvent(QEvent.FocusIn))

    assert ("suggest_common_names", "", "Agaricus", "bisporus", 200) in lookup.calls
    assert window._ref_vernacular_model.rowCount() == 2
    assert window._ref_vernacular_model.item(0).data(Qt.UserRole) == "Cultivated mushroom"
    assert window._ref_vernacular_model.item(1).data(Qt.UserRole) == "Button mushroom"
    assert "vernacular" in complete_calls

    window.ref_vernacular_input.blockSignals(True)
    window.ref_vernacular_input.setText("Button mushroom")
    window.ref_vernacular_input.blockSignals(False)
    window.ref_vernacular_input.setFocus()
    qapp.processEvents()
    window.eventFilter(window.ref_vernacular_input, QEvent(QEvent.FocusIn))
    qapp.processEvents()
    assert window.ref_vernacular_input.selectedText() == "Button mushroom"

    window.ref_vernacular_input.blockSignals(True)
    window.ref_vernacular_input.setText("")
    window.ref_vernacular_input.blockSignals(False)
    window._sync_ref_vernacular_from_taxon()

    assert window.ref_vernacular_input.text() == "Button mushroom"


def test_language_settings_dialog_lists_available_languages_and_falls_back(
    monkeypatch,
    qapp,
) -> None:
    monkeypatch.setattr(
        main_window.SettingsDB,
        "get_setting",
        lambda key, default=None: "en" if key == "vernacular_language" else default,
    )
    monkeypatch.setattr(
        main_window,
        "list_available_vernacular_languages",
        lambda: ["en", "de", "no"],
    )

    dialog = main_window.LanguageSettingsDialog()
    dialog.show()
    qapp.processEvents()

    assert [dialog.vernacular_combo.itemData(i) for i in range(dialog.vernacular_combo.count())] == ["en", "de", "no"]
    assert dialog.vernacular_combo.currentData() == "en"

    dialog.deleteLater()


def test_language_settings_dialog_falls_back_to_norwegian_when_only_that_language_is_available(
    monkeypatch,
    qapp,
) -> None:
    monkeypatch.setattr(
        main_window.SettingsDB,
        "get_setting",
        lambda key, default=None: "en" if key == "vernacular_language" else default,
    )
    monkeypatch.setattr(
        main_window,
        "list_available_vernacular_languages",
        lambda: ["no"],
    )

    dialog = main_window.LanguageSettingsDialog()
    dialog.show()
    qapp.processEvents()

    assert dialog.vernacular_combo.count() == 1
    assert dialog.vernacular_combo.itemData(0) == "no"
    assert dialog.vernacular_combo.currentData() == "no"

    dialog.deleteLater()


def test_reference_panel_ambiguous_common_name_does_not_autofill(
    monkeypatch,
    qapp,
) -> None:
    window = _build_minimal_window(monkeypatch, qapp)
    window.ref_vernacular_db = _FailingVernacularDB()

    lookup = _ReferenceLookupStub(
        genera=[],
        species=[],
        common_names=[],
        resolve_results=[
            TaxonChoice(genus="Agaricus", species="bisporus", common_name="Shared name"),
            TaxonChoice(genus="Amanita", species="muscaria", common_name="Shared name"),
        ],
    )
    window._ensure_reference_taxon_lookup = lambda: lookup

    window.ref_vernacular_input.setText("Shared name")
    window.ref_genus_input.clear()
    window.ref_species_input.clear()

    window._on_ref_vernacular_editing_finished()

    assert window.ref_vernacular_input.text() == "Shared name"
    assert window.ref_genus_input.text() == ""
    assert window.ref_species_input.text() == ""


def test_reference_panel_still_uses_reference_db_for_loading(
    tmp_path: Path,
    monkeypatch,
    qapp,
) -> None:
    window = _build_minimal_window(monkeypatch, qapp)
    window.ref_genus_input.setText("Agaricus")
    window.ref_species_input.setText("bisporus")
    window.ref_source_input.setCurrentText("Reference sheet")
    window.mount_input = QLineEdit("water")
    window.stain_input = QLineEdit("cotton blue")

    calls: list[tuple[tuple, dict]] = []
    original_get_reference = main_window.ReferenceDB.get_reference

    def _tracking_get_reference(*args, **kwargs):
        calls.append((args, kwargs))
        return {
            "genus": "Agaricus",
            "species": "bisporus",
            "source": "Reference sheet",
            "mount_medium": "water",
            "stain": "cotton blue",
            "length_min": 4.0,
            "length_p05": 4.5,
            "length_p50": 5.0,
            "length_p95": 5.5,
            "length_max": 6.0,
            "width_min": 3.0,
            "width_p05": 3.2,
            "width_p50": 3.4,
            "width_p95": 3.6,
            "width_max": 3.8,
            "q_min": 1.1,
            "q_p50": 1.2,
            "q_max": 1.3,
        }

    monkeypatch.setattr(main_window.ReferenceDB, "get_reference", _tracking_get_reference)

    window._maybe_load_reference_panel_reference()

    assert calls
    assert calls[0][0][:3] == ("Agaricus", "bisporus", "Reference sheet")
    assert window.mount_input.text() == "water"
    assert window.stain_input.text() == "cotton blue"
    assert window.table.item(0, 0).text() == "4"
    assert window.table.item(1, 4).text() == "3.8"

    monkeypatch.setattr(main_window.ReferenceDB, "get_reference", original_get_reference)


def test_edit_reference_series_row_replaces_row_when_source_changes(monkeypatch) -> None:
    """Editing a legacy row's source (e.g. adding a missing book title)
    changes its identity key; the edit must replace the old ReferenceDB row
    and comparison row rather than add a second one beside it."""
    mw_cls = main_window.MainWindow

    class _Fake:
        species_availability = None
        active_observation_id = None
        reference_values = {}
        tr = staticmethod(lambda text: text)
        _active_sporely_taxon_id = lambda self: None
        _refresh_reference_species_availability = lambda self: None
        _refresh_reference_series_table = lambda self: None
        _update_reference_add_state = lambda self: None
        update_graph_plots_only = lambda self: None
        _save_gallery_settings = lambda self: None
        for _name in (
            "_edit_reference_series_row",
            "_find_reference_series_entry",
            "_reference_series_row_is_editable",
            "_clean_ref_genus_text",
            "_clean_ref_species_text",
            "_reference_series_key",
            "_format_reference_series_label",
            "_normalize_reference_series_entry",
            "_sync_reference_values_from_series_data",
            "_add_reference_series_entry",
        ):
            locals()[_name] = getattr(mw_cls, _name)
        del _name

    original = {
        "genus": "Pholiota", "species": "gummosa", "source": "P. gummosa",
        "mount_medium": None, "stain": None, "source_kind": "reference",
        "length_min": 5.5, "length_max": 8.0,
    }
    updated = dict(original, source="P. gummosa (Funga Nordica)")

    class _Dialog:
        def __init__(self, *args, **kwargs):
            pass

        def exec(self):
            return main_window.QDialog.Accepted

        def delete_requested(self):
            return False

        def result_data(self):
            return dict(updated)

    calls: list[tuple] = []

    class _DB:
        @staticmethod
        def set_reference(values):
            calls.append(("set", values.get("source")))

        @staticmethod
        def delete_reference(genus, species, source=None, mount_medium=None, stain=None):
            calls.append(("delete", genus, species, source, mount_medium, stain))

    monkeypatch.setattr(main_window, "ReferenceAddDialog", _Dialog)
    monkeypatch.setattr(main_window, "ReferenceDB", _DB)

    fake = _Fake()
    other = {"key": ("reference", "Pholiota", "squarrosa", "FN", "", ""),
             "data": {"genus": "Pholiota", "species": "squarrosa", "source": "FN"},
             "label": "other", "enabled": True}
    old_key = fake._reference_series_key(original)
    fake.reference_series = [
        {"key": old_key, "data": dict(original), "label": "old", "enabled": False},
        other,
    ]

    fake._edit_reference_series_row(old_key)

    assert calls == [
        ("set", "P. gummosa (Funga Nordica)"),
        ("delete", "Pholiota", "gummosa", "P. gummosa", None, None),
    ]
    assert len(fake.reference_series) == 2
    edited = fake.reference_series[0]
    assert edited["key"] == fake._reference_series_key(updated)
    assert edited["data"]["source"] == "P. gummosa (Funga Nordica)"
    assert edited["enabled"] is False
    assert fake.reference_series[1] is other
