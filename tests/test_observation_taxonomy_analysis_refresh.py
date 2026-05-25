from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QComboBox, QLabel, QLineEdit, QTableWidget

import ui.main_window as main_window
import ui.observations_tab as observations_tab
from ui.observations_tab import ObservationsTab


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


class _EmptySpeciesAvailability:
    DATA_POINT_EMOJI = "🔹"
    MINMAX_EMOJI = "📏"

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


class _NoVernacularLookup:
    vernacular_db = None
    language_code = "en"


class _FakeSelectionIndex:
    def __init__(self, row: int):
        self._row = row

    def row(self) -> int:
        return self._row


class _FakeSelectionModel:
    def __init__(self, table: "_FakeTable"):
        self._table = table

    def selectedRows(self):
        if self._table.selected_row is None:
            return []
        return [_FakeSelectionIndex(self._table.selected_row)]


class _FakeTable:
    def __init__(self, selected_row: int | None = 0):
        self.selected_row = selected_row

    def selectionModel(self):
        return _FakeSelectionModel(self)

    def selectRow(self, row: int) -> None:
        self.selected_row = row


class _FakeObservationDetailsDialog:
    def __init__(self, *args, **kwargs):
        self.image_results = []

    def exec(self) -> bool:
        return True

    def get_ai_state(self) -> dict:
        return {}

    def get_data(self) -> dict:
        return {
            "genus": "Entoloma",
            "species": "sericeum",
            "common_name": None,
            "publish_target": None,
            "is_draft": None,
            "sharing_scope": None,
            "location_public": None,
            "location_precision": None,
            "species_guess": None,
            "uncertain": False,
            "unspontaneous": False,
            "determination_method": None,
            "date": "2026-05-25",
            "location": None,
            "habitat": None,
            "habitat_nin2_path": None,
            "habitat_substrate_path": None,
            "habitat_host_genus": None,
            "habitat_host_species": None,
            "habitat_host_common_name": None,
            "habitat_nin2_note": None,
            "habitat_substrate_note": None,
            "habitat_grows_on_note": None,
            "open_comment": None,
            "private_comment": None,
            "interesting_comment": False,
            "gps_latitude": None,
            "gps_longitude": None,
        }


def _build_minimal_window(monkeypatch) -> main_window.MainWindow:
    monkeypatch.setattr(
        main_window.SettingsDB,
        "get_setting",
        lambda key, default=None: "en" if key == "vernacular_language" else default,
    )
    monkeypatch.setattr(main_window.MainWindow, "init_ui", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_populate_scale_combo", lambda self, selected_key=None: None)
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
    window.ref_table = QTableWidget(3, 5)
    window.reference_values = {}
    window.reference_series = []
    window.species_availability = _EmptySpeciesAvailability()
    window._reference_taxon_lookup = _NoVernacularLookup()
    window._update_reference_add_state = lambda: None
    window.update_graph_plots_only = lambda: None
    window.active_observation_id = 1
    return window


def test_edit_observation_refreshes_active_state_and_reference_cache(monkeypatch, qapp) -> None:
    tab = ObservationsTab.__new__(ObservationsTab)
    tab.table = _FakeTable(selected_row=0)
    tab.selected_observation_id = None
    tab._observation_edit_draft_cache = {}
    tab._ai_suggestions_cache = {}
    tab._merge_observation_edit_draft = lambda observation, draft: dict(observation)
    tab._build_import_results_from_images = lambda images: []
    tab._remap_ai_state_to_images = lambda ai_state, image_results: ai_state
    tab._load_observation_ai_state = lambda observation: {}
    tab._apply_import_results_to_observation = lambda *args, **kwargs: None
    tab.refresh_observations = lambda: None
    tab.on_selection_changed = lambda: None
    tab._upload_pending_artsobs_web_images = lambda: "none"
    tab.set_status_message = lambda *args, **kwargs: None
    tab._observation_id_for_row = lambda row: 1

    observation = {
        "id": 1,
        "genus": "Tricholoma",
        "species": "inamoenum",
        "date": "2026-05-25",
        "gps_latitude": None,
        "gps_longitude": None,
    }
    update_calls: list[tuple[int, dict]] = []
    call_order: list[object] = []
    host_calls: list[bool] = []

    class _Host:
        def _refresh_reference_species_availability(self, force_refresh: bool = False):
            host_calls.append(force_refresh)
            call_order.append(("cache_refresh", force_refresh))

    host = _Host()
    tab.window = lambda: host
    tab.parent = lambda: None
    tab.set_selected_as_active = lambda switch_tab=True: call_order.append(("active", switch_tab))

    monkeypatch.setattr(observations_tab.ObservationDB, "get_observation", lambda obs_id: dict(observation))
    monkeypatch.setattr(observations_tab.ObservationDB, "get_all_observations", lambda: [{"id": 1}])
    monkeypatch.setattr(observations_tab.ObservationDB, "update_observation", lambda observation_id, **kwargs: update_calls.append((observation_id, kwargs)))
    monkeypatch.setattr(observations_tab.ImageDB, "get_images_for_observation", lambda obs_id: [])
    monkeypatch.setattr(observations_tab, "ObservationDetailsDialog", _FakeObservationDetailsDialog)

    ObservationsTab.edit_observation(tab)

    assert update_calls
    assert update_calls[0][0] == 1
    assert update_calls[0][1]["genus"] == "Entoloma"
    assert update_calls[0][1]["species"] == "sericeum"
    assert host_calls == [True]
    assert call_order == [("cache_refresh", True), ("active", False)]


def test_load_reference_values_replaces_stale_fields_when_new_taxon_has_no_reference(
    monkeypatch,
    qapp,
) -> None:
    window = _build_minimal_window(monkeypatch)
    window.ref_vernacular_input.setText("Old common name")
    window.ref_source_input.setCurrentText("Old source")
    window.reference_values = {
        "genus": "Tricholoma",
        "species": "inamoenum",
        "source": "Old source",
        "length_min": 7.0,
        "width_min": 3.0,
    }
    window.reference_series = [dict(window.reference_values)]
    window._apply_reference_panel_values(window.reference_values)

    monkeypatch.setattr(
        main_window.ObservationDB,
        "get_observation",
        lambda observation_id: {
            "id": observation_id,
            "genus": "Entoloma",
            "species": "sericeum",
        },
    )
    monkeypatch.setattr(main_window.ReferenceDB, "get_reference", lambda *args, **kwargs: None)
    monkeypatch.setattr(main_window.ObservationDB, "get_personal_observations_for_species", lambda *args, **kwargs: [])
    monkeypatch.setattr(main_window.ReferenceDB, "list_sources", lambda *args, **kwargs: [])

    window.load_reference_values()

    assert window.ref_genus_input.text() == "Entoloma"
    assert window.ref_species_input.text() == "sericeum"
    assert window.ref_vernacular_input.text() == ""
    assert window.ref_source_input.currentText() == ""
    assert window.reference_values == {}
    assert window.reference_series == []
    assert window.ref_table.item(0, 0) is None


def test_load_reference_values_reloads_new_reference_data_for_changed_taxon(
    monkeypatch,
    qapp,
) -> None:
    window = _build_minimal_window(monkeypatch)
    window.ref_vernacular_input.setText("Old common name")
    window.ref_source_input.setCurrentText("Old source")
    window.reference_values = {
        "genus": "Tricholoma",
        "species": "inamoenum",
        "source": "Old source",
        "length_min": 7.0,
        "width_min": 3.0,
    }
    window.reference_series = [dict(window.reference_values)]
    window._apply_reference_panel_values(window.reference_values)

    new_reference = {
        "genus": "Entoloma",
        "species": "sericeum",
        "source": "Paper A",
        "length_min": 3.1,
        "width_min": 1.2,
        "q_min": 2.4,
    }

    monkeypatch.setattr(
        main_window.ObservationDB,
        "get_observation",
        lambda observation_id: {
            "id": observation_id,
            "genus": "Entoloma",
            "species": "sericeum",
        },
    )
    monkeypatch.setattr(main_window.ReferenceDB, "get_reference", lambda *args, **kwargs: dict(new_reference))
    monkeypatch.setattr(main_window.ObservationDB, "get_personal_observations_for_species", lambda *args, **kwargs: [])
    monkeypatch.setattr(main_window.ReferenceDB, "list_sources", lambda *args, **kwargs: ["Paper A"])

    window.load_reference_values()

    assert window.ref_genus_input.text() == "Entoloma"
    assert window.ref_species_input.text() == "sericeum"
    assert window.ref_source_input.currentText() == "Paper A"
    assert window.reference_values["genus"] == "Entoloma"
    assert window.reference_values["species"] == "sericeum"
    assert window.reference_series and window.reference_series[0]["data"]["genus"] == "Entoloma"
    assert window.ref_table.item(0, 0).text() == "3.1"
