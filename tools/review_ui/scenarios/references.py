"""Reference Library scenarios and isolated SQLite fixture construction."""
from __future__ import annotations

import json
from unittest.mock import patch

from PySide6.QtCore import QSettings
from PySide6.QtWidgets import QTableWidgetItem

from database import schema as db_schema

from ..context import ReviewContext
from ..registry import ReviewScenario, ScenarioRegistry


def _fixture(context: ReviewContext):
    # Each builder starts with empty geometry/splitter settings, including
    # the legacy geometry lookup. Never consult the user's application state.
    import ui.window_state as geometry

    index = context.state.get("reference.settings_index", 0) + 1
    context.state["reference.settings_index"] = index
    settings_path = context.temporary_root / f"reference-settings-{index}.ini"
    factory = lambda *_args: QSettings(str(settings_path), QSettings.IniFormat)
    context.enter_fixture(patch.object(geometry, "QSettings", factory))
    cached = context.state.get("reference.fixture")
    if cached is not None:
        return cached
    assert context.temporary_root is not None
    fixture_dir = context.temporary_root / "reference-library"
    fixture_dir.mkdir()
    main_db = fixture_dir / "mushrooms.db"
    reference_db = fixture_dir / "reference_values.db"
    context.enter_fixture(patch.object(db_schema, "get_database_path", lambda: main_db))
    context.enter_fixture(
        patch.object(db_schema, "get_reference_database_path", lambda: reference_db)
    )
    context.enter_fixture(
        patch.object(
            db_schema,
            "get_bundled_reference_database_path",
            lambda: fixture_dir / "no-bundled-reference.db",
        )
    )
    db_schema.init_database()
    work, sets = _seed_library()
    cached = {"work": work, "sets": sets, "fixture_dir": fixture_dir}
    context.state["reference.fixture"] = cached
    return cached


def _select_work(dialog, work_id: str) -> None:
    for row in range(dialog.publication_combo.count()):
        if str(dialog.publication_combo.itemData(row) or "") == work_id:
            dialog.publication_combo.setCurrentIndex(row)
            return
    raise RuntimeError(f"publication {work_id} is missing from the picker")


def _populate_range(dialog) -> None:
    expression = "(8.1–)8.5–10.8(–11.4) × (4.2–)4.5–5.8(–6.1) µm, Q = 1.7–2.1, Qm = 1.89, n = 36"
    dialog.measurement_paste_input.setText(expression)
    dialog._parse_measurement_btn.click()


def _populate_reported_statistics(dialog) -> None:
    """A headed literature table that reports meanings and statistics.

    The Hebeloma layout names its inner range a 5%–95% percentile interval,
    prints the mean as an interval rather than a scalar, and adds a median
    and a standard deviation. It exercises the compact tag strip, the
    interval mean cells, the reported median / S.D. line and the
    reader-version notice in one shot.
    """
    dialog.measurement_paste_input.setText(
        "\n".join(
            [
                "Spore\t(min) 5%-95% (max)\tmean\tmedian\tS.D.",
                "Length\t(7.5) 8.4-13.0 (13.2)\t9.2-11.7\t9.2-11.7\t0.600",
                "Width\t(5.0) 5.1-7.2 (7.6)\t5.6-6.7\t5.6-6.7\t0.280",
                "Q\t(1.30) 1.42-1.96 (2.07)\t1.55-1.78\t1.54-1.79\t0.095",
            ]
        )
    )
    dialog._parse_measurement_btn.click()


def _populate_raw_points(dialog) -> None:
    dialog._spore_section.set_expanded(True)
    points = (
        (8.4, 4.6),
        (8.8, 4.7),
        (9.1, 4.9),
        (9.4, 5.0),
        (9.7, 5.1),
        (10.0, 5.3),
        (10.4, 5.5),
        (10.8, 5.7),
    )
    dialog.spore_table._ensure_rows(len(points))
    for row, (length, width) in enumerate(points):
        dialog.spore_table.setItem(row, 0, QTableWidgetItem(f"{length:.1f}"))
        dialog.spore_table.setItem(row, 1, QTableWidgetItem(f"{width:.1f}"))


def _make_add_dialog(context: ReviewContext, *, taxon_id: int | None = 7):
    from ui.main_window import ReferenceAddDialog

    fixture = _fixture(context)
    dialog = ReferenceAddDialog(
        context.host,
        "Cortinarius",
        "limonius",
        "Gulbelteslørsopp",
        observation_id=42,
        sporely_taxon_id=taxon_id,
    )
    _select_work(dialog, fixture["work"].id)
    return dialog


def _seed_library():
    from database.reference_library import (
        MeasurementSet,
        MeasurementSetRepository,
        ReferenceWork,
        ReferenceWorkRepository,
        TaxonTreatment,
        TaxonTreatmentRepository,
    )

    work = ReferenceWorkRepository.create(
        ReferenceWork(
            id="",
            type="article",
            title="A comprehensive revision of northern European Cortinarius species",
            short_label="Niskanen, Liimatainen & Kytövuori 2018",
            authors_json=json.dumps(
                [
                    {"family": "Niskanen", "given": "Tuula"},
                    {"family": "Liimatainen", "given": "Kare"},
                    {"family": "Kytövuori", "given": "Ilkka"},
                ]
            ),
            container_title="Fungal Diversity and Systematics of Northern Europe",
            year=2018,
            volume="42",
            pages="115–198",
            doi="10.1000/cortinarius.2018.42",
        )
    )
    treatment = TaxonTreatmentRepository.create(
        TaxonTreatment(
            id="",
            reference_work_id=work.id,
            taxon_id="7",
            name_as_published="Cortinarius limonius (Fr.) Fr.",
            locator_text="pp. 146–148, fig. 32",
        )
    )
    sets = [
        MeasurementSetRepository.create(
            MeasurementSet(
                id="",
                taxon_treatment_id=treatment.id,
                character="spore_size",
                data_kind="range",
                raw_text="(8.1–)8.5–10.8(–11.4) × (4.2–)4.5–5.8(–6.1) µm",
                length_min=8.1,
                length_core_min=8.5,
                length_core_max=10.8,
                length_max=11.4,
                width_min=4.2,
                width_core_min=4.5,
                width_core_max=5.8,
                width_max=6.1,
                q_min=1.7,
                q_max=2.1,
                q_mean=1.89,
                sample_size=36,
            )
        ),
        MeasurementSetRepository.create(
            MeasurementSet(
                id="",
                taxon_treatment_id=treatment.id,
                character="spore_size",
                data_kind="raw_points",
                raw_text="8 paired measurements from the holotype collection",
                raw_points_json=json.dumps(
                    [
                        {"length": 8.4, "width": 4.6},
                        {"length": 9.1, "width": 4.9},
                        {"length": 10.0, "width": 5.3},
                        {"length": 10.8, "width": 5.7},
                    ]
                ),
                sample_size=4,
            )
        ),
        MeasurementSetRepository.create(
            MeasurementSet(
                id="",
                taxon_treatment_id=treatment.id,
                character="spore_size",
                data_kind="summary",
                raw_text="L = 9.6 ± 0.7 µm; W = 5.1 ± 0.4 µm; n = 36",
                length_mean=9.6,
                width_mean=5.1,
                sample_size=36,
            )
        ),
    ]

    other_work = ReferenceWorkRepository.create(
        ReferenceWork(
            id="",
            type="book",
            title="The genus Cortinarius in temperate and boreal forests",
            short_label="Brandrud et al. 2020",
            authors_json=json.dumps([{"family": "Brandrud", "given": "T. E."}]),
            year=2020,
            publisher="Nordic Mycological Press",
            place="Oslo",
        )
    )
    other_treatment = TaxonTreatmentRepository.create(
        TaxonTreatment(
            id="",
            reference_work_id=other_work.id,
            taxon_id="99",
            name_as_published="Cortinarius rubellus Cooke",
            locator_text="Vol. 2, p. 311",
        )
    )
    MeasurementSetRepository.create(
        MeasurementSet(
            id="",
            taxon_treatment_id=other_treatment.id,
            character="spore_size",
            data_kind="range",
            raw_text="8.0–9.5 × 5.5–6.5 µm",
            length_core_min=8.0,
            length_core_max=9.5,
            width_core_min=5.5,
            width_core_max=6.5,
        )
    )
    return work, sets


def _range(context: ReviewContext):
    dialog = _make_add_dialog(context)
    _populate_range(dialog)
    return dialog


def _reported_statistics(context: ReviewContext):
    dialog = _make_add_dialog(context)
    _populate_reported_statistics(dialog)
    return dialog


def _raw_points(context: ReviewContext):
    dialog = _make_add_dialog(context)
    _populate_raw_points(dialog)
    return dialog


def _existing_set(context: ReviewContext):
    dialog = _make_add_dialog(context)
    dialog.use_existing_radio.setChecked(True)
    dialog._existing_sets_table.selectRow(0)
    return dialog


def _new_publication(context: ReviewContext):
    from ui.reference_library_manager_dialog import ReferenceWorkEditor

    _fixture(context)
    dialog = ReferenceWorkEditor(context.host)
    article_index = dialog.type_combo.findData("article")
    dialog.type_combo.setCurrentIndex(article_index)
    dialog.title_input.setText(
        "Morphological variation and species limits in northern European Cortinarius"
    )
    dialog.authors_editor.add_row(family="Niskanen", given="Tuula")
    dialog.authors_editor.add_row(family="Liimatainen", given="Kare")
    dialog.year_input.setText("2024")
    dialog.container_input.setText("Studies in Mycology and Boreal Fungal Diversity")
    dialog.volume_input.setText("108")
    dialog.issue_input.setText("2")
    dialog.pages_input.setText("145–189")
    dialog.doi_input.setText("10.1000/sim.2024.108.2")
    return dialog


def _library_manager(context: ReviewContext):
    from ui.reference_library_manager_dialog import ReferenceLibraryManagerDialog

    fixture = _fixture(context)
    dialog = ReferenceLibraryManagerDialog(
        context.host,
        active_observation_id=42,
        cloud_client=object(),
        sporely_taxon_id=7,
    )
    dialog.refresh_works(select_id=fixture["work"].id)
    dialog._refresh_hierarchy_for_current_work(
        select_set_id=fixture["sets"][0].id
    )
    return dialog


def _library_technical_details(context: ReviewContext):
    """Referansebibliotek: a publication, its identifiers under Technical details, open (stage 10e1)."""
    from ui.reference_library_manager_dialog import ReferenceLibraryManagerDialog

    fixture = _fixture(context)
    dialog = ReferenceLibraryManagerDialog(
        context.host, active_observation_id=42, cloud_client=object(), sporely_taxon_id=7,
    )
    dialog.refresh_works(select_id=fixture["work"].id)
    dialog.technical_section.set_expanded(True)
    return dialog


def _library_delete_failed(context: ReviewContext):
    """Referansebibliotek: deleting a measurement set in use failed; Details open (stage 10e1).

    The real delete path: the confirmation answers Yes and the repository
    refuses, as it does for a set attached to an observation.
    """
    from PySide6.QtWidgets import QMessageBox

    from database.reference_library import MeasurementSetRepository, ReferenceLibraryError
    from ui.reference_library_manager_dialog import ReferenceLibraryManagerDialog

    fixture = _fixture(context)

    def refuse(_set_id):
        raise ReferenceLibraryError("FOREIGN KEY constraint failed (observation_reference_uses)")

    context.enter_fixture(patch.object(MeasurementSetRepository, "delete", refuse))
    context.enter_fixture(patch.object(QMessageBox, "question", lambda *_a, **_k: QMessageBox.Yes))
    dialog = ReferenceLibraryManagerDialog(
        context.host, active_observation_id=42, cloud_client=object(), sporely_taxon_id=7,
    )
    dialog.refresh_works(select_id=fixture["work"].id)
    dialog._refresh_hierarchy_for_current_work(select_set_id=fixture["sets"][0].id)
    dialog._on_delete_selected_clicked()
    dialog.hint_bar._details_btn.click()
    return dialog


class _ReviewSharingClient:
    """Two shared reference sets; stopping one fails (no network)."""

    def list_my_reference_sharing(self):
        def row(set_id, label, status):
            return {
                "source_measurement_set_id": set_id, "status": status, "stopped_at": None,
                "hidden_by_moderation": False, "public_observation_count": 2,
                "species_page_contributions": [{
                    "contribution_id": f"c-{set_id}", "sporely_taxon_id": 1,
                    "canonical_scientific_name": "Cortinarius caperatus",
                    "current_revision": 3, "shared_at": None, "hidden_at": None,
                }],
                "source_short_label": label, "source_raw_text": "11–14 × 7–9 µm",
            }

        return {"status": "ok", "sets": [
            row("set-a", "Niskanen et al. 2024", "shared"),
            row("set-b", "Petersen 1990", "shared"),
        ]}

    def stop_sharing_reference_set(self, _set_id):
        from utils.cloud_sync import CloudSyncError

        raise CloudSyncError("HTTPSConnectionPool: Max retries exceeded (timed out)")


def _my_shared_failed(context: ReviewContext):
    """Mine delte referanser: stopping sharing failed; Details open (stage 10e1)."""
    from ui.reference_sharing_dialogs import MySharedReferencesDialog

    dialog = MySharedReferencesDialog(_ReviewSharingClient(), parent=context.host)
    dialog.confirm_stop_sharing = lambda: True
    dialog.select_row(0)
    dialog.stop_btn.click()
    dialog.hint_bar._details_btn.click()
    return dialog


def _measurement_set_form_enhanced(context: ReviewContext):
    """The library-manager form with enhanced editing switched on.

    Both rollout gates ship closed, so this is the only way to see the form
    the parser → repository wiring actually fills: the Q core pair visible
    beside its extremes, interval means in the mean fields, and the reported
    median / S.D. line. The gate is patched for this scenario alone.
    """
    from references import measurement_content_gates as gates
    from ui.reference_library_manager_dialog import _MeasurementSetForm

    _fixture(context)
    context.enter_fixture(
        patch.object(gates, "enhanced_attachments_enabled", lambda: True)
    )
    from database.reference_library import TaxonTreatmentRepository

    treatment = TaxonTreatmentRepository.list_for_work(
        context.state["reference.fixture"]["work"].id
    )[0]
    form = _MeasurementSetForm(context.host, taxon_treatment_id=treatment.id)
    form.raw_text_input.setText(
        "\n".join(
            [
                "Spore\t(min) 5%-95% (max)\tmean\tmedian\tS.D.",
                "Length\t(7.5) 8.4-13.0 (13.2)\t9.2-11.7\t9.2-11.7\t0.600",
                "Width\t(5.0) 5.1-7.2 (7.6)\t5.6-6.7\t5.6-6.7\t0.280",
                "Q\t(1.30) 1.42-1.96 (2.07)\t1.55-1.78\t1.54-1.79\t0.095",
            ]
        )
    )
    form.parse_btn.click()
    return form


def _no_taxon(context: ReviewContext):
    dialog = _make_add_dialog(context, taxon_id=None)
    _populate_range(dialog)
    return dialog


def _parmasto(context: ReviewContext):
    dialog = _make_add_dialog(context)
    dialog._parmasto_section.set_expanded(True)
    values = {
        "parmasto_length_mean": "9.62",
        "parmasto_width_mean": "5.08",
        "parmasto_q_mean": "1.89",
        "parmasto_v_sp_length": "7.4",
        "parmasto_v_sp_width": "8.1",
        "parmasto_v_sp_q": "6.3",
        "parmasto_v_ind_length": "5.8",
        "parmasto_v_ind_width": "6.6",
        "parmasto_v_ind_q": "4.9",
    }
    for key, value in values.items():
        dialog.parmasto_inputs[key].setText(value)
    return dialog


def _comparison_row(
    dataset_id,
    title,
    source_kind,
    detail,
    color,
    *,
    visible=True,
    is_observation=False,
    provenance="",
):
    from ui.comparison_panel import ComparisonRow, SourceKind

    return ComparisonRow(
        dataset_id=dataset_id,
        title=title,
        source_kind=SourceKind(source_kind) if isinstance(source_kind, str) else source_kind,
        detail=detail,
        color=color,
        visible=visible,
        is_observation=is_observation,
        provenance=provenance,
    )


def _comparison_list_basic(context: ReviewContext):
    from ui.comparison_panel import RESERVED_OBSERVATION_COLOR, ComparisonListWidget

    widget = ComparisonListWidget(context.host)
    widget.set_rows(
        [
            _comparison_row(
                "obs-1", "This observation", "observation", "n = 20",
                RESERVED_OBSERVATION_COLOR, is_observation=True,
            ),
            _comparison_row(
                "lib-1", "Funga Nordica", "library", "8.5–10.8 × 4.5–5.8 µm", "#e67e22",
                provenance=(
                    "Method: not reported\n"
                    "Mount medium: KOH\n"
                    "Stain: Congo red\n"
                    "Sample size: not reported\n"
                    "Specimen count: not reported"
                ),
            ),
            _comparison_row(
                "myobs-1", "Sigmund Ås 2026-08-02", "my_obs", "n = 17", "#2ecc71",
            ),
        ]
    )
    return widget


def _comparison_list_overflow(context: ReviewContext):
    from ui.comparison_panel import RESERVED_OBSERVATION_COLOR, ComparisonListWidget

    widget = ComparisonListWidget(context.host)
    rows = [
        _comparison_row(
            "obs-1", "This observation", "observation", "n = 20",
            RESERVED_OBSERVATION_COLOR, is_observation=True,
        )
    ]
    palette = ["#e67e22", "#8e44ad", "#2ecc71", "#e74c3c", "#1abc9c", "#f1c40f", "#34495e", "#c0392b"]
    kinds = ["library", "community", "my_obs"]
    for index in range(8):
        rows.append(
            _comparison_row(
                f"row-{index}",
                f"Reference set {index + 1}",
                kinds[index % len(kinds)],
                f"n = {10 + index}",
                palette[index % len(palette)],
            )
        )
    widget.set_rows(rows)
    return widget


def _comparison_list_longnames(context: ReviewContext):
    from ui.comparison_panel import RESERVED_OBSERVATION_COLOR, ComparisonListWidget

    widget = ComparisonListWidget(context.host)
    widget.set_rows(
        [
            _comparison_row(
                "obs-1", "This observation", "observation", "n = 20",
                RESERVED_OBSERVATION_COLOR, is_observation=True,
            ),
            _comparison_row(
                "lib-long",
                "A comprehensive revision of northern European Cortinarius species with extensive morphological and molecular notes",
                "library",
                "8.5–10.8 × 4.5–5.8 µm",
                "#e67e22",
            ),
            _comparison_row(
                "com-nordic",
                "Kantarell og trakttrompetsopp fra Ørsta og Ålesund — Blåbærgrøtsopp",
                "community",
                "n = 12",
                "#8e44ad",
            ),
        ]
    )
    return widget


def _comparison_list_suppressed(context: ReviewContext):
    """Category switched away from Spores: reference rows dim, the plot's
    ``n`` on the current-observation row reflects the cystidium population
    instead, and the explanatory hint line appears (stage-4b-fix Part 1)."""
    from ui.comparison_panel import RESERVED_OBSERVATION_COLOR, ComparisonListWidget

    widget = ComparisonListWidget(context.host)
    rows = [
        _comparison_row(
            "obs-1", "This observation", "observation", "n = 6 (cystidia)",
            RESERVED_OBSERVATION_COLOR, is_observation=True,
        ),
        _comparison_row(
            "lib-1", "Funga Nordica", "library", "range · 8.5–10.8 µm", "#d95319",
        ),
        _comparison_row(
            "myobs-1", "Sigmund Ås 2026-08-02", "my_obs", "n = 17", "#2ecc71",
        ),
    ]
    for row in rows[1:]:
        row.dimmed = True
    widget.set_rows(rows, references_suppressed=True)
    return widget


def _comparison_list_colors(context: ReviewContext):
    """The current observation and two references must be three visibly
    distinct colours (stage-4b-fix Part 2.3): the observation keeps the
    reserved blue, and references start at palette index 1, not 0."""
    from ui.comparison_panel import RESERVED_OBSERVATION_COLOR, ComparisonListWidget
    from ui.main_window import reference_plot_palette

    palette = reference_plot_palette(False)
    widget = ComparisonListWidget(context.host)
    widget.set_rows(
        [
            _comparison_row(
                "obs-1", "This observation", "observation", "n = 20",
                RESERVED_OBSERVATION_COLOR, is_observation=True,
            ),
            _comparison_row(
                "lib-1", "Funga Nordica", "library", "range · 8.5–10.8 µm", palette[1],
            ),
            _comparison_row(
                "com-1", "sporely_community_user_42", "community", "n = 24", palette[2],
            ),
        ]
    )
    return widget


def _analysis_panel_window(context: ReviewContext):
    """A real (but init-stubbed) ``MainWindow`` instance, isolated from any
    developer database, for rendering the actual Analysis-tab gallery panel
    (stage 6 removed the legacy duplicate reference form/table from this
    panel; a real ``create_gallery_panel()`` call is the only way to show
    that removal plus the Shape/Min-Max controls' new home in Plot settings,
    rather than an isolated ``ComparisonListWidget``).
    """
    import ui.main_window as main_window

    # Reuse the shared reference-library fixture database (see ``_fixture``)
    # instead of patching ``db_schema`` a second time: ``context.enter_fixture``
    # pushes onto one process-lifetime ExitStack that is never unwound between
    # scenarios, so a second, different db_schema patch here would silently
    # redirect every reference-library scenario registered after this one
    # (including the add-dialog-manual-* group) to an empty database.
    _fixture(context)

    with patch.object(main_window.MainWindow, "init_ui", lambda self: None), \
         patch.object(main_window.MainWindow, "_populate_scale_combo", lambda self: None), \
         patch.object(main_window.MainWindow, "load_default_objective", lambda self: None), \
         patch.object(main_window.MainWindow, "_restore_geometry", lambda self: None):
        window = main_window.MainWindow()
    # Keep the window alive for the scenario's lifetime -- the returned
    # panel's child widgets hold bound-method signal connections back to it,
    # but an explicit reference avoids relying on that alone.
    context.state.setdefault("reference.analysis_windows", []).append(window)
    return window


def _analysis_series_mixed() -> list[dict]:
    """A legacy ReferenceDB row, a community raw-points set, and a
    previously-recorded personal observation -- the three source kinds the
    legacy table's rows could show, exercised together."""
    return [
        {
            "data": {
                "genus": "Agaricus",
                "species": "bisporus",
                "source": "Funga Nordica",
                "source_kind": "reference",
                "length_min": 8.5,
                "length_max": 10.8,
                "width_min": 4.5,
                "width_max": 5.8,
                "mount_medium": "KOH",
                "stain": "Congo red",
            },
            "enabled": True,
        },
        {
            "data": {
                "source_kind": "points",
                "points": [(9.0 + index * 0.1, 5.0) for index in range(12)],
                "points_label": "sporely_community_user_42",
                "source_type": "community",
                "genus": "Agaricus",
                "species": "bisporus",
            },
            "enabled": True,
        },
        {
            "data": {
                "source_kind": "observation",
                "points": [(9.2, 5.1)] * 17,
                "genus": "Agaricus",
                "species": "bisporus",
                "date": "2026-08-02",
                "author": "Sigmund Ås",
                "observation_id": 99,
            },
            "enabled": True,
        },
    ]


def _analysis_series_longnames() -> list[dict]:
    return [
        {
            "key": "use-long",
            "data": {
                "source_kind": "reference",
                "observation_reference_use_id": "use-long",
                "short_label": (
                    "A comprehensive revision of northern European "
                    "Cortinarius species with extensive morphological and "
                    "molecular notes"
                ),
                "name_as_published": "Cortinarius diasemospermus",
                "locator_text": "p. 142",
                "reference_data_kind": "range",
                "raw_text": "8.5-10.8 × 4.5-5.8",
            },
            "enabled": True,
        },
        {
            "data": {
                "source_kind": "points",
                "points": [(9.0, 5.0)] * 12,
                "points_label": "Kantarell og trakttrompetsopp fra Ørsta og Ålesund — Blåbærgrøtsopp",
                "source_type": "community",
                "genus": "Cantharellus",
                "species": "cibarius",
            },
            "enabled": True,
        },
    ]


def _expand_plot_settings(panel) -> None:
    """Expand the "Plot settings" CollapsibleSection (collapsed by default)
    so its relocated Shape/Min-Max controls (stage 6) are visible in the
    screenshot; "Reference values" already starts expanded."""
    from PySide6.QtWidgets import QToolButton

    for toggle in panel.findChildren(QToolButton, "collapsibleToggle"):
        if not toggle.isChecked():
            toggle.click()


def _analysis_panel_empty(context: ReviewContext):
    window = _analysis_panel_window(context)
    window.active_observation_id = None
    window.reference_series = []
    panel = window.create_gallery_panel()
    _expand_plot_settings(panel)
    return panel


def _analysis_panel_populated(context: ReviewContext):
    window = _analysis_panel_window(context)
    window.active_observation_id = None
    window.reference_series = _analysis_series_mixed()
    panel = window.create_gallery_panel()
    _expand_plot_settings(panel)
    return panel


def _analysis_panel_longnames(context: ReviewContext):
    window = _analysis_panel_window(context)
    window.active_observation_id = None
    window.reference_series = _analysis_series_longnames()
    panel = window.create_gallery_panel()
    _expand_plot_settings(panel)
    return panel


def _analysis_series_five_ranges() -> list[dict]:
    rows = []
    for index, (name, raw) in enumerate((
        ("Psathyrella candolleana", "6.5-9 × 4-5"),
        ("Psathyrella piluliformis", "4.5-6 × 3-3.5"),
        ("Psathyrella spadiceogrisea", "7-9.5 × 4-5"),
        ("Panaeolus fimicola", "11-14 × 7.5-9.5"),
        ("Psathyrella corrugis", "11-14 × 6-7.5"),
    )):
        length, width = raw.split(" × ")
        lmin, lmax = (float(v) for v in length.split("-"))
        wmin, wmax = (float(v) for v in width.split("-"))
        rows.append({
            "key": f"use-{index}",
            "data": {
                "source_kind": "reference",
                "observation_reference_use_id": f"use-{index}",
                "short_label": "Funga Nordica (2008)",
                "name_as_published": name,
                "reference_data_kind": "range",
                "raw_text": raw,
                "length_p05": lmin, "length_p95": lmax,
                "width_p05": wmin, "width_p95": wmax,
            },
            "enabled": True,
        })
    return rows


def _analysis_panel_five_rows(context: ReviewContext):
    """Five references at a tall window: the list uses the column's spare
    height, so all five rows show without scrolling."""
    window = _analysis_panel_window(context)
    window.active_observation_id = None
    window.reference_series = _analysis_series_five_ranges()
    return window.create_gallery_panel()


def _analysis_panel_suppressed(context: ReviewContext):
    """Category switched away from Spores: the comparison list must dim its
    reference rows and show the explanatory hint without the legacy table
    guards this stage removed (see MainWindow._refresh_reference_series_table)."""
    window = _analysis_panel_window(context)
    window.active_observation_id = None
    window.reference_series = _analysis_series_mixed()
    panel = window.create_gallery_panel()
    window.gallery_filter_combo.addItem("Cystidia", "cystidia")
    window.gallery_filter_combo.setCurrentIndex(window.gallery_filter_combo.count() - 1)
    window._refresh_reference_series_table()
    _expand_plot_settings(panel)
    return panel


def register_reference_scenarios(registry: ScenarioRegistry) -> None:
    scenarios = (
        ReviewScenario(
            id="reference.add-range",
            group="reference-library",
            title="Add reference — normalized range data",
            description="An existing publication and realistic parsed literature range exercise the normalized table.",
            viewport=(900, 720),
            build=_range,
        ),
        ReviewScenario(
            id="reference.reported-statistics",
            group="reference-library",
            title="Add reference — reported statistics from a headed table",
            description=(
                "A 5%–95% headed table with interval means, medians and "
                "standard deviations exercises the tag strip, the "
                "scalar-or-interval mean column and the reported-statistics "
                "line."
            ),
            viewport=(900, 780),
            build=_reported_statistics,
        ),
        ReviewScenario(
            id="reference.raw-points",
            group="reference-library",
            title="Add reference — raw measurement points",
            description="Multiple paired length and width observations exercise the editable raw-data branch.",
            viewport=(900, 720),
            build=_raw_points,
        ),
        ReviewScenario(
            id="reference.existing-measurement-set",
            group="reference-library",
            title="Attach an existing measurement set while adding a reference",
            description="An existing publication set is selected while new-data tabs remain disabled.",
            viewport=(900, 720),
            build=_existing_set,
        ),
        ReviewScenario(
            id="reference.new-publication",
            group="reference-library",
            title="Create a new publication",
            description="A populated bibliography editor exercises field grouping, citation preview, and scrolling.",
            viewport=(720, 640),
            build=_new_publication,
        ),
        ReviewScenario(
            id="reference.library-manager",
            group="reference-library",
            title="Reference Library hierarchy",
            description="A selected publication, taxon treatment, and measurement set exercise the three-pane CRUD manager.",
            viewport=(1100, 700),
            build=_library_manager,
        ),
        ReviewScenario(
            id="reference.library-technical-details",
            group="reference-library",
            title="Reference Library: identifiers under Technical details, open (nb)",
            description="UUID, revision and citation key are behind a closed section under the details (stage 10e1).",
            viewport=(1100, 700),
            build=_library_technical_details,
            locale="nb_NO",
        ),
        ReviewScenario(
            id="reference.library-delete-failed",
            group="reference-library",
            title="Reference Library: a failed delete in the bottom bar, Details open (nb)",
            description="Failures are notices beside Close, not message boxes (stage 10e1).",
            viewport=(1100, 700),
            build=_library_delete_failed,
            locale="nb_NO",
        ),
        ReviewScenario(
            id="reference.library-delete-failed-dark",
            group="reference-library",
            title="Reference Library: a failed delete, Details open (nb, dark)",
            description="The notice in the dark theme (stage 10e1).",
            viewport=(1100, 700),
            build=_library_delete_failed,
            locale="nb_NO",
            theme="dark",
        ),
        ReviewScenario(
            id="reference.library-delete-failed-small-de",
            group="reference-library",
            title="Reference Library: a failed delete in a small window (de)",
            description="The long German notice beside Close (stage 10e1).",
            viewport=(860, 600),
            build=_library_delete_failed,
            locale="de_DE",
        ),
        ReviewScenario(
            id="reference.my-shared-failed",
            group="reference-library",
            title="My shared references: stopping sharing failed, Details open (nb)",
            description="Results and failures are notices left of the buttons (stage 10e1).",
            viewport=(900, 460),
            build=_my_shared_failed,
            locale="nb_NO",
        ),
        ReviewScenario(
            id="reference.my-shared-failed-de",
            group="reference-library",
            title="My shared references: stopping sharing failed (de)",
            description="The German notice and buttons on one row (stage 10e1).",
            viewport=(900, 460),
            build=_my_shared_failed,
            locale="de_DE",
        ),
        ReviewScenario(
            id="reference.measurement-set-form-enhanced",
            group="reference-library",
            title="Library manager measurement-set form with enhanced editing on",
            description=(
                "The gate-open form: the Q core pair beside its extremes, "
                "interval means in the mean fields, and the reported "
                "median / S.D. line the save path now persists."
            ),
            viewport=(760, 720),
            build=_measurement_set_form_enhanced,
        ),
        ReviewScenario(
            id="reference.no-taxon",
            group="reference-library",
            title="No-taxon legacy fallback",
            description="The fallback notice explains that range data will be saved only to the legacy list.",
            viewport=(900, 720),
            build=_no_taxon,
        ),
        ReviewScenario(
            id="reference.parmasto",
            group="reference-library",
            title="Parmasto biometrics legacy path",
            description="Realistic Parmasto values exercise the retained legacy-only biometric branch.",
            # Taller than the other Quick-add scenarios: the one-column
            # editor puts the publication block and the measurement grid
            # above this section, so 720px would capture the scroll area
            # rather than the biometric fields the scenario is about.
            viewport=(900, 1080),
            build=_parmasto,
        ),
        ReviewScenario(
            id="reference.nb-no",
            group="reference-library",
            title="Reference range workflow in Norwegian Bokmål",
            description="The real Norwegian translator exercises labels, tabs, headers, and button placement.",
            viewport=(900, 720),
            build=_range,
            locale="nb_NO",
        ),
        ReviewScenario(
            id="reference.comparison-list",
            group="reference-library",
            title="Comparison list — observation, library, and my_obs rows",
            description="Three plotted datasets exercise checkbox, color chip, badge, and detail line rendering.",
            viewport=(420, 260),
            build=_comparison_list_basic,
        ),
        ReviewScenario(
            id="reference.comparison-list-dark",
            group="reference-library",
            title="Comparison list — dark theme",
            description="Same three-row state in dark theme to verify contrast.",
            viewport=(420, 260),
            build=_comparison_list_basic,
            theme="dark",
        ),
        ReviewScenario(
            id="reference.comparison-list-overflow",
            group="reference-library",
            title="Comparison list — nine rows, scrolling",
            description="Nine rows must scroll within a fixed-height panel while staying usable.",
            viewport=(420, 320),
            build=_comparison_list_overflow,
        ),
        ReviewScenario(
            id="reference.comparison-list-longnames",
            group="reference-library",
            title="Comparison list — long titles and æøå",
            description="A publication title over 60 characters must elide; Norwegian names with æ, ø, å must render correctly.",
            viewport=(420, 260),
            build=_comparison_list_longnames,
        ),
        ReviewScenario(
            id="reference.comparison-list-suppressed",
            group="reference-library",
            title="Comparison list — Category outside Spores",
            description="Reference rows render dimmed and unchecked, with the explanatory hint line, while the current-observation row's n tracks the plotted (cystidia) population.",
            viewport=(420, 280),
            build=_comparison_list_suppressed,
        ),
        ReviewScenario(
            id="reference.comparison-list-colors",
            group="reference-library",
            title="Comparison list — three distinct colours",
            description="The current observation and two references must be visibly distinct: references now start at palette index 1, not the observation's reserved index 0.",
            viewport=(420, 260),
            build=_comparison_list_colors,
        ),
        ReviewScenario(
            id="reference.analysis-panel-empty",
            group="reference-library",
            title="Analysis tab reference panel — empty, no references",
            description="The real Analysis-tab gallery panel with no observation loaded: the legacy form/table/Attach-library button are gone, only Add reference and Manage reference library remain, and Plot settings carries Shape/Min-Max.",
            viewport=(440, 780),
            build=_analysis_panel_empty,
        ),
        ReviewScenario(
            id="reference.analysis-panel-five-rows",
            group="reference-library",
            title="Analysis tab reference panel — five references, tall window",
            description="Five literature ranges with Plot settings collapsed at a tall viewport: the Reference values list takes the spare height so all five rows are visible without scrolling, and the gallery card stays below it.",
            viewport=(440, 1000),
            build=_analysis_panel_five_rows,
        ),
        ReviewScenario(
            id="reference.analysis-panel-populated",
            group="reference-library",
            title="Analysis tab reference panel — populated, mixed sources",
            description="A legacy ReferenceDB row, a community raw-points set, and a previously-recorded personal observation together in the real comparison list, with Add reference and Manage reference library reachable below it.",
            viewport=(440, 780),
            build=_analysis_panel_populated,
        ),
        ReviewScenario(
            id="reference.analysis-panel-suppressed",
            group="reference-library",
            title="Analysis tab reference panel — non-spore category",
            description="Category switched away from Spores: reference rows dim and the suppressed-category hint appears, without any legacy table guard.",
            viewport=(440, 780),
            build=_analysis_panel_suppressed,
        ),
        ReviewScenario(
            id="reference.analysis-panel-longnames",
            group="reference-library",
            title="Analysis tab reference panel — long taxon/source names",
            description="A long publication title and a long æøå community label exercise row elision at a constrained width.",
            viewport=(440, 780),
            build=_analysis_panel_longnames,
        ),
        ReviewScenario(
            id="reference.analysis-panel-dark",
            group="reference-library",
            title="Analysis tab reference panel — dark theme",
            description="The populated mixed-source panel in the application's real dark palette.",
            viewport=(440, 780),
            build=_analysis_panel_populated,
            theme="dark",
        ),
        ReviewScenario(
            id="reference.analysis-panel-nb-no",
            group="reference-library",
            title="Analysis tab reference panel — Norwegian Bokmål",
            description="The real Norwegian translator exercises Add reference, Manage reference library, and the relocated Plot settings labels (Reference shape, Min/Max).",
            viewport=(440, 780),
            build=_analysis_panel_populated,
            locale="nb_NO",
        ),
    )
    for scenario in scenarios:
        registry.register(scenario)

    for theme in ("light", "dark"):
        for suffix, builder, viewport, natural, capture in (
            ("suppressed", _comparison_list_suppressed, (420, 260), False, None),
            ("colors", _comparison_list_colors, (420, 260), False, None),
        ):
            registry.register(ReviewScenario(
                id=f"reference.fix2-{suffix}-{theme}", group="reference-library",
                title=f"Reference correction — {suffix} ({theme})",
                description=f"Deterministic {suffix} evidence with isolated settings.",
                build=builder, viewport=viewport, theme=theme,
                natural_size=natural, capture_target=capture,
            ))
    for suffix, builder, viewport, capture in (
        ("hint", _comparison_list_suppressed, (620, 260), None),
    ):
        registry.register(ReviewScenario(
            id=f"reference.fix2-{suffix}-nb-no", group="reference-library",
            title=f"Norwegian reference {suffix}",
            description="Scoped Norwegian catalogue loaded by the production Qt translator.",
            build=builder, viewport=viewport, locale="nb_NO", capture_target=capture,
        ))
