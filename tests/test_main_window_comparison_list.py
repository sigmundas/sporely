"""The main window's comparison list and its legacy-row edit (redesign stage 6).

What remains of the old reference panel's tests after stage 10a4 removed the
panel's own fields, completers and taxon lookup.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QTableWidget

import ui.main_window as main_window
from ui.comparison_panel import ComparisonListWidget


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def _build_minimal_window(monkeypatch, qapp) -> main_window.MainWindow:
    monkeypatch.setattr(main_window.SettingsDB, "get_setting", lambda key, default=None: default)
    monkeypatch.setattr(main_window.MainWindow, "init_ui", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_populate_scale_combo", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "load_default_objective", lambda self: None)
    monkeypatch.setattr(main_window.MainWindow, "_restore_geometry", lambda self: None)
    window = main_window.MainWindow()
    window.table = QTableWidget(3, 5)
    window._gallery_hint_controller = None
    window._pending_gallery_hint_widgets = []
    window.reference_series = []
    window.reference_values = {}
    window.gallery_plot_settings = {}
    window.active_observation_id = None
    return window


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


def test_edit_reference_series_row_replaces_row_when_source_changes(monkeypatch) -> None:
    """Editing a legacy row's source (e.g. adding a missing book title)
    changes its identity key; the edit must replace the old ReferenceDB row
    and comparison row rather than add a second one beside it."""
    mw_cls = main_window.MainWindow

    class _Fake:
        active_observation_id = None
        reference_values = {}
        tr = staticmethod(lambda text: text)
        _active_sporely_taxon_id = lambda self: None
        _refresh_reference_series_table = lambda self: None
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
