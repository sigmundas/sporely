from __future__ import annotations

import json
import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QComboBox

import ui.main_window as main_window
from ui.observations_tab import ObservationsTab


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


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
    window.reference_values = {}
    window.reference_series = []
    window.update_graph_plots_only = lambda: None
    window.active_observation_id = 1
    return window


def test_rediger_save_refreshes_the_table_then_the_active_state(qapp) -> None:
    """A save in Rediger (stage 9g2) refreshes the table, then the active observation.

    Rediger replaced the Edit observation dialog (stage 9g4c), whose save did
    the same: ``refresh_after_observation_edits`` → ``_refresh_after_observation_edits``
    → ``ObservationsTab._refresh_active_observation_after_edit``.
    """
    call_order: list[object] = []
    tab = ObservationsTab.__new__(ObservationsTab)
    tab.refresh_observations = lambda: call_order.append("table")
    tab.get_selected_observation = lambda: (1, "Tricholoma inamoenum")
    tab.set_selected_as_active = lambda switch_tab=True: call_order.append(("active", switch_tab))
    tab.parent = lambda: None

    class _Host:
        active_observation_id = 1
        observations_tab = tab

    host = _Host()
    tab.window = lambda: host

    main_window.MainWindow._refresh_after_observation_edits(host, 1)
    assert call_order == ["table", ("active", False)]

    # Another observation shown meanwhile: no switch back.
    call_order.clear()
    host.active_observation_id = 2
    main_window.MainWindow._refresh_after_observation_edits(host, 1)
    assert call_order == ["table"]


def test_load_reference_values_clears_the_old_reference_when_the_new_taxon_has_none(
    monkeypatch,
    qapp,
) -> None:
    window = _build_minimal_window(monkeypatch)
    window.reference_values = {
        "genus": "Tricholoma",
        "species": "inamoenum",
        "source": "Old source",
        "length_min": 7.0,
        "width_min": 3.0,
    }
    window.reference_series = [dict(window.reference_values)]

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

    assert window.reference_values == {}
    assert window.reference_series == []


def test_load_reference_values_reloads_new_reference_data_for_changed_taxon(
    monkeypatch,
    qapp,
) -> None:
    window = _build_minimal_window(monkeypatch)
    window.reference_values = {
        "genus": "Tricholoma",
        "species": "inamoenum",
        "source": "Old source",
        "length_min": 7.0,
        "width_min": 3.0,
    }
    window.reference_series = [dict(window.reference_values)]

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

    assert window.reference_values["genus"] == "Entoloma"
    assert window.reference_values["species"] == "sericeum"
    assert window.reference_values["source"] == "Paper A"
    assert window.reference_series and window.reference_series[0]["data"]["genus"] == "Entoloma"


def test_removing_last_reference_persists_and_restores_empty_series(
    monkeypatch,
    qapp,
) -> None:
    window = _build_minimal_window(monkeypatch)
    window.active_observation_id = 682
    reference_data = {
        "source_kind": "observation",
        "observation_id": 443,
        "date": "2026-06-23",
        "author": "Sigmund Ås",
        "genus": "Agrocybe",
        "species": "praecox",
        "length_min": 12.3,
        "width_min": 7.9,
    }
    normalized = window._normalize_reference_series_entry(reference_data)
    assert normalized is not None
    window.reference_values = dict(reference_data)
    window.reference_series = [normalized]

    saved: dict[str, str] = {}
    monkeypatch.setattr(
        main_window.SettingsDB,
        "set_setting",
        lambda key, value: saved.update({key: value}),
    )

    window._remove_reference_series_key(normalized["key"])

    assert window.reference_values == {}
    assert window.reference_series == []
    assert "gallery_settings_682" in saved
    persisted = json.loads(saved["gallery_settings_682"])
    assert persisted["reference_values"] == {}
    assert persisted["reference_series"] == []

    # Simulate load_reference_values() auto-loading a species reference before
    # the per-observation Analysis settings are reapplied.
    window.reference_values = dict(reference_data)
    window.reference_series = [normalized]
    window._apply_saved_reference_state(persisted)

    assert window.reference_values == {}
    assert window.reference_series == []


def test_update_graph_plots_hides_spore_reference_overlay_for_pleurocystidia(
    monkeypatch,
    qapp,
) -> None:
    window = _build_minimal_window(monkeypatch)
    window.gallery_filter_combo = QComboBox()
    window.gallery_filter_combo.addItem("Pleurocystidia", "pleurocystidia")
    window.gallery_filter_combo.setCurrentIndex(0)
    window.gallery_plot_figure = main_window.Figure(figsize=(4.0, 4.0))
    window.gallery_plot_canvas = main_window.FigureCanvas(window.gallery_plot_figure)
    window.gallery_plot_settings = {
        "histogram": False,
        "legend": False,
        "reference_minmax": True,
    }
    window.gallery_image_labels = {}
    window.reference_values = {
        "genus": "Entoloma",
        "species": "sericeum",
        "source": "Paper A",
        "length_min": 3.1,
        "length_p05": 3.3,
        "length_p50": 3.7,
        "length_p95": 4.2,
        "length_max": 4.5,
        "width_min": 1.1,
        "width_p05": 1.2,
        "width_p50": 1.4,
        "width_p95": 1.6,
        "width_max": 1.8,
        "q_p05": 2.1,
        "q_p50": 2.6,
        "q_p95": 3.0,
    }
    window.reference_series = []
    window._gallery_highlighted_measurement_ids = lambda measurements: set()
    window._format_observation_legend_label = lambda: "Pleurocystidia"
    window._gallery_plot_style = lambda settings=None: "ellipse"
    window._apply_plot_light_theme = lambda *args, **kwargs: None
    window._apply_plot_dark_theme = lambda *args, **kwargs: None
    window._is_dark_theme = lambda: False
    window._update_gallery_stats_preview = lambda: None

    measurements = [
        {"id": 1, "length_um": 11.8, "width_um": 4.1, "measurement_type": "pleurocystidia"},
        {"id": 2, "length_um": 12.3, "width_um": 4.4, "measurement_type": "pleurocystidia"},
        {"id": 3, "length_um": 13.1, "width_um": 4.8, "measurement_type": "pleurocystidia"},
    ]

    window.update_graph_plots(measurements)

    ax_scatter = window.gallery_plot_figure.axes[0]
    assert len(ax_scatter.lines) == 1
    assert len(ax_scatter.patches) == 0


# Sanitized stored shape of the manual-test row (gallery_settings_958): a
# legacy literature row with explicit L/W bounds, no library use, no points.
LEGACY_958_ROW = {
    "id": None, "genus": "Panaeolus", "species": "fimicola",
    "source": "Funga Nordica (2008)", "source_kind": "reference",
    "length_min": 9.0, "length_p05": 11.0, "length_p95": 15.0,
    "width_p05": 7.0, "width_p95": 9.0,
}


def test_legacy_literature_row_translates_to_normalized_range():
    from references.reference_plotting import translate_legacy_literature_range

    stored = dict(LEGACY_958_ROW)
    out = translate_legacy_literature_range(stored)
    assert stored == LEGACY_958_ROW  # stored row untouched
    assert out["reference_data_kind"] == "range" and out["legacy_literature_range"]
    assert out["raw_text"] == "(9–)11–15 × 7–9"
    # Not literature ranges: point datasets, library uses, mean-only rows.
    assert translate_legacy_literature_range({"source_kind": "points", "points": [(1, 2)]}) is None
    assert translate_legacy_literature_range(
        {"source_kind": "reference", "points": [(9.0, 5.0)] * 5,
         "length_p05": 1, "length_p95": 2, "width_p05": 1, "width_p95": 2}) is None
    assert translate_legacy_literature_range(dict(LEGACY_958_ROW, observation_reference_use_id="u")) is None
    assert translate_legacy_literature_range(
        {"source_kind": "reference", "length_p50": 12.0, "width_p50": 8.0}) is None
    # Extreme-only pair: the drawn range itself.
    only_ext = translate_legacy_literature_range(
        {"length_min": 8, "length_max": 10, "width_min": 4, "width_max": 5})
    assert (only_ext["length_p05"], only_ext["length_p95"], only_ext["length_min"]) == (8.0, 10.0, None)


def test_comparison_row_subtitle_shows_range_expression():
    from references.reference_plotting import translate_legacy_literature_range
    from ui.comparison_panel import _format_detail

    assert _format_detail(translate_legacy_literature_range(LEGACY_958_ROW)) == "range · (9–)11–15 × 7–9"


def _plot_window(monkeypatch, shape, row, *, histogram=False):
    window = _build_minimal_window(monkeypatch)
    window.gallery_filter_combo = QComboBox()
    window.gallery_filter_combo.addItem("Spores", "spores")
    window.gallery_filter_combo.setCurrentIndex(0)
    window.gallery_plot_figure = main_window.Figure(figsize=(4.0, 4.0))
    window.gallery_plot_canvas = main_window.FigureCanvas(window.gallery_plot_figure)
    window.gallery_plot_settings = {"histogram": histogram, "legend": True, "avg_q": True,
                                    "reference_minmax": True, "reference_shape": shape}
    window.gallery_image_labels = {}
    window.reference_values = {}
    window.reference_series = [{"data": dict(row), "enabled": True}]
    window._gallery_highlighted_measurement_ids = lambda measurements: set()
    window._format_observation_legend_label = lambda: "Spores"
    window._gallery_plot_style = lambda settings=None: "ellipse"
    window._apply_plot_light_theme = lambda *args, **kwargs: None
    window._apply_plot_dark_theme = lambda *args, **kwargs: None
    window._is_dark_theme = lambda: False
    window._update_gallery_stats_preview = lambda: None
    measurements = [{"id": i, "length_um": 12.0 + i * 0.3, "width_um": 7.5 + i * 0.1,
                     "measurement_type": "spores"} for i in range(1, 6)]
    window.update_graph_plots(measurements)
    return window.gallery_plot_figure


def _signature(fig):
    out = []
    for ax in fig.axes:
        out.append((
            sorted((tuple(map(float, l.get_xdata())), tuple(map(float, l.get_ydata())), l.get_linestyle())
                   for l in ax.lines if len(l.get_xdata())),
            len(ax.collections),
            sorted(type(p).__name__ for p in ax.patches),
            [t.get_text() for t in (ax.get_legend().get_texts() if ax.get_legend() else [])],
        ))
    return out


@pytest.mark.parametrize("shape", ["ellipse", "square"])
def test_legacy_literature_row_plots_boxes_whatever_the_shape_setting(monkeypatch, qapp, shape):
    from matplotlib.patches import Ellipse, Polygon

    ax = _plot_window(monkeypatch, shape, LEGACY_958_ROW).axes[0]
    assert not any(isinstance(p, Ellipse) for p in ax.patches)
    fills = [p for p in ax.patches if isinstance(p, Polygon)]
    assert any(sorted(map(tuple, f.get_xy().round(3).tolist()))[0] == (11.0, 7.0) for f in fills)
    styles = {(min(l.get_xdata()), max(l.get_xdata()), l.get_linestyle()) for l in ax.lines if len(l.get_xdata())}
    assert (11.0, 15.0, "-") in styles   # core box, solid edge
    assert (9.0, 15.0, ":") in styles    # exceptional min -> dotted outer box


def test_translated_legacy_row_keeps_q_histogram_and_parmasto_overlays(monkeypatch, qapp):
    """Translation only changes the box drawing: every other overlay the
    legacy row produced (Q guidelines, mean-Q label, histogram overlay,
    Parmasto marker/range) is still there."""
    import references.reference_plotting as rp

    row = dict(LEGACY_958_ROW, q_p05=1.4, q_p50=1.6, q_p95=1.8, q_min=1.3, q_max=2.0,
               length_p50=13.0, width_p50=8.0,
               parmasto_length_mean=13.1, parmasto_width_mean=8.1, parmasto_q_mean=1.62,
               parmasto_v_sp_length=6.0, parmasto_v_sp_width=5.0, parmasto_v_sp_q=4.0)
    translated = _signature(_plot_window(monkeypatch, "square", row, histogram=True))
    monkeypatch.setattr(main_window, "translate_legacy_literature_range", lambda data: None)
    legacy = _signature(_plot_window(monkeypatch, "square", row, histogram=True))
    # Identical lines, collections and legend; only the translucent core fill is added.
    for t_ax, l_ax in zip(translated, legacy):
        assert t_ax[0] == l_ax[0]
        assert t_ax[1] == l_ax[1]
        assert t_ax[3] == l_ax[3]
    assert translated[0][2].count("Polygon") == legacy[0][2].count("Polygon") + 1
    assert len(translated) == len(legacy) and len(translated) > 1  # histogram axes present
    assert any("Q" in text for text in translated[0][3])  # mean-Q legend label kept


def test_confidence_ellipse_of_collinear_measurements_is_finite(monkeypatch):
    """Collinear spores have a singular covariance. Its smallest eigenvalue
    can come out a rounding error below zero (OpenBLAS does), which must
    give a flat ellipse, not NaN."""
    import numpy as np

    eigh = np.linalg.eigh

    def slightly_negative(cov):
        values, vectors = eigh(cov)
        return np.array([-1e-17, values[1]]), vectors

    monkeypatch.setattr(main_window.np.linalg, "eigh", slightly_negative)
    x = [12.0 + i * 0.3 for i in range(1, 6)]
    y = [7.5 + i * 0.1 for i in range(1, 6)]
    xs, ys = main_window.MainWindow._confidence_ellipse_points(None, x, y)
    assert np.isfinite(xs).all() and np.isfinite(ys).all()


def test_normalized_reference_draws_q_typical_and_extreme_guidelines(monkeypatch, qapp):
    window = _build_minimal_window(monkeypatch)
    window.gallery_filter_combo = QComboBox()
    window.gallery_filter_combo.addItem("Spores", "spores")
    window.gallery_plot_figure = main_window.Figure(figsize=(4, 4))
    window.gallery_plot_canvas = main_window.FigureCanvas(window.gallery_plot_figure)
    window.gallery_plot_settings = {"histogram": False, "legend": False,
        "q_minmax": True, "q_extreme_minmax": True, "avg_q": False}
    window.gallery_image_labels = {}
    window.reference_values = {}
    window.reference_series = [{"key": "q-use", "label": "Q source", "enabled": True,
        "color": "#ff00ff", "data": {"source_kind": "reference", "reference_data_kind": "range",
        "observation_reference_use_id": "q-use", "plot_color": "#ff00ff", "length_p05": 6, "length_p95": 10,
        "width_p05": 2, "width_p95": 5, "length_min": 5, "length_max": 11,
        "width_min": 1.5, "width_max": 5.5, "q_core_min": 2, "q_core_max": 3,
        "q_min": 1.5, "q_max": 3.5}}]
    window._gallery_highlighted_measurement_ids = lambda measurements: set()
    window._format_observation_legend_label = lambda: "Spores"
    window._gallery_plot_style = lambda settings=None: "ellipse"
    window._apply_plot_light_theme = lambda *args, **kwargs: None
    window._apply_plot_dark_theme = lambda *args, **kwargs: None
    window._is_dark_theme = lambda: False
    window._update_gallery_stats_preview = lambda: None
    window.update_graph_plots([{"id": i, "length_um": 8 + i * .2, "width_um": 3 + i * .1,
                               "measurement_type": "spores"} for i in range(1, 6)])
    lines = window.gallery_plot_figure.axes[0].lines
    drawn = {}
    for line in lines:
        xs, ys = line.get_xdata(), line.get_ydata()
        if len(xs) == 2 and len(ys) == 2 and line.get_color() == "#ff00ff":
            drawn[round(float(xs[0]) / float(ys[0]), 2)] = line.get_linestyle()
    assert set(drawn) == {1.5, 2.0, 3.0, 3.5}
    assert drawn[1.5] == drawn[3.5] == ":"
    assert drawn[2.0] == drawn[3.0] != ":"
    window.close()
