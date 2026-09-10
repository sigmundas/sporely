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
    import ui.add_reference_dialog as picker
    import ui.window_state as geometry

    index = context.state.get("reference.settings_index", 0) + 1
    context.state["reference.settings_index"] = index
    settings_path = context.temporary_root / f"reference-settings-{index}.ini"
    factory = lambda *_args: QSettings(str(settings_path), QSettings.IniFormat)
    context.enter_fixture(patch.object(picker, "QSettings", factory))
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


def _populate_raw_points(dialog) -> None:
    dialog.tabs.setCurrentIndex(1)
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


def _attach_candidates():
    from database.reference_library import MeasurementSetCandidate

    return [
        MeasurementSetCandidate(
            measurement_set_id="candidate-range",
            short_label="Niskanen, Liimatainen & Kytövuori 2018",
            name_as_published="Cortinarius limonius (Fr.) Fr.",
            locator_text="pp. 146–148, fig. 32",
            data_kind="range",
            raw_text="(8.1–)8.5–10.8(–11.4) × (4.2–)4.5–5.8(–6.1) µm",
            revision=1,
            reference_work_id="work-main",
            reference_treatment_id="treatment-main",
            taxon_id="7",
        ),
        MeasurementSetCandidate(
            measurement_set_id="candidate-points",
            short_label="Niskanen, Liimatainen & Kytövuori 2018",
            name_as_published="Cortinarius limonius (Fr.) Fr.",
            locator_text="supplementary dataset S4",
            data_kind="raw_points",
            raw_text="8 paired holotype measurements",
            revision=1,
            reference_work_id="work-main",
            reference_treatment_id="treatment-main",
            taxon_id="7",
        ),
        MeasurementSetCandidate(
            measurement_set_id="candidate-other",
            short_label="Brandrud et al. 2020",
            name_as_published="Cortinarius rubellus Cooke",
            locator_text="Vol. 2, p. 311",
            data_kind="range",
            raw_text="8.0–9.5 × 5.5–6.5 µm",
            revision=1,
            reference_work_id="work-other",
            reference_treatment_id="treatment-other",
            taxon_id="99",
        ),
    ]


def _make_attach_dialog(context: ReviewContext):
    from ui.reference_library_attach_dialog import ReferenceLibraryAttachDialog

    _fixture(context)
    dialog = ReferenceLibraryAttachDialog(
        context.host,
        candidates=_attach_candidates(),
        taxon_id=7,
    )
    dialog.table.selectRow(0)
    return dialog


def _range(context: ReviewContext):
    dialog = _make_add_dialog(context)
    _populate_range(dialog)
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


def _no_taxon(context: ReviewContext):
    dialog = _make_add_dialog(context, taxon_id=None)
    _populate_range(dialog)
    return dialog


def _parmasto(context: ReviewContext):
    dialog = _make_add_dialog(context)
    dialog.tabs.setCurrentIndex(2)
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


def _community_preview(context: ReviewContext):
    from ui.reference_preview_pane import ReferencePreviewPane

    pane = ReferencePreviewPane(context.host)
    pane.set_summary(
        title="Cantharellus cibarius Fr. (Observation dataset) — Niskanen, Liimatainen & Kytövuori 2018, a comprehensive revision of northern European Cantharellus species with extensive morphological notes",
        meta="Contributor: sporely_community_user_42  •  Date: 2024-07-15  •  n=48",
        rows=[
            ("Length", "7.50", "9.20", "11.40"),
            ("Width", "4.10", "5.30", "6.60"),
            ("Q", "1.65", "1.73", "1.95"),
        ],
        note="QC signals: Mount recorded, Stain recorded, Scale recorded, Measurement points recorded",
    )
    pane.set_raw_spores(
        "L=7.50  W=4.10  Q=1.83\n"
        "L=8.40  W=4.60  Q=1.83\n"
        "L=9.10  W=4.90  Q=1.86\n"
        "L=9.40  W=5.00  Q=1.88\n"
        "L=9.70  W=5.10  Q=1.90\n"
        "L=10.00  W=5.30  Q=1.89\n"
        "L=10.40  W=5.50  Q=1.89\n"
        "L=11.40  W=6.60  Q=1.73"
    )
    pane.set_method(
        {
            "mount": "Melzer's reagent",
            "stain": "Congo red",
            "sample_type": "Fresh fruitbody",
            "contrast": "Phase contrast",
            "objective": "100× oil immersion",
            "scale": "0.08 µm/px",
        }
    )
    pane.set_calibration("Scale: 0.08 µm/px\nCalibration details come from image/objective metadata in the synced observation dataset.")
    pane.set_provenance(
        "Kind: observation\n"
        "Contributor: sporely_community_user_42\n"
        "Date: 2024-07-15\n"
        "Observation id: obs-00042\n"
        "Location and private observation content are intentionally excluded from this review flow."
    )
    return pane


def _reference_provenance_preview(context: ReviewContext):
    """Stage 5: reworded Method tab (reported-source wording, incl. a "not
    reported" case) + a Summary table with derived-value markers visible."""
    from ui.reference_preview_pane import ReferencePreviewPane

    pane = ReferencePreviewPane(context.host)
    pane.set_summary(
        title="Amanita muscaria (L.) Lam.",
        meta="Funga Nordica — pp. 145",
        rows=[
            ("Length", "8.00", "9.20", "12.00"),
            ("Width", "6.00", "7.10", "9.00"),
            ("Q", "1.20", "1.30", "1.40"),
        ],
        note="Range summary",
        # Length: min directly reported, mean/max derived from the typical
        # range. Width/Q: all three directly reported (no marker).
        derived=[(False, True, True), (False, False, False), (False, False, False)],
    )
    pane.set_raw_spores("This is a range summary; no raw spore points are stored.")
    pane.set_method(
        {
            "mount": "",
            "stain": "KOH",
            "sample_type": "",
            "contrast": "",
            "objective": "",
            "scale": "",
        }
    )
    pane.set_calibration("No calibration details recorded.")
    pane.set_provenance("Source notes: not reported")
    pane.set_provenance_summary(
        "Reported by: Funga Nordica (2012) · sample size: not reported · method recorded: yes"
    )
    return pane


def _reference_provenance_preview_method_tab(context: ReviewContext):
    pane = _reference_provenance_preview(context)
    pane.review_tabs.setCurrentIndex(2)  # Method tab: reworded reported-source labels
    return pane


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


def _add_dialog_candidates():
    from database.reference_library import MeasurementSetCandidate

    # Reuses the long-names data (60+ char title, æøå) already exercised by
    # reference.comparison-list-longnames rather than inventing a new fixture.
    return [
        MeasurementSetCandidate(
            measurement_set_id="add-dialog-1",
            short_label="Niskanen, Liimatainen & Kytövuori 2018",
            name_as_published="Cortinarius limonius (Fr.) Fr.",
            locator_text="pp. 146–148, fig. 32",
            data_kind="range",
            raw_text="(8.1–)8.5–10.8(–11.4) × (4.2–)4.5–5.8(–6.1) µm, Q = 1.7–2.1, n = 36",
            revision=1,
            reference_work_id="work-main",
            reference_treatment_id="treatment-main",
            work_title=(
                "A comprehensive revision of northern European Cortinarius "
                "species with extensive morphological and molecular notes"
            ),
            year=2018,
            taxon_id="7",
        ),
        MeasurementSetCandidate(
            measurement_set_id="add-dialog-2",
            short_label="Niskanen, Liimatainen & Kytövuori 2018",
            name_as_published="Cortinarius limonius (Fr.) Fr.",
            locator_text="supplementary dataset S4",
            data_kind="raw_points",
            raw_text="8 paired holotype measurements",
            revision=1,
            reference_work_id="work-main",
            reference_treatment_id="treatment-main",
            year=2018,
            taxon_id="7",
        ),
        MeasurementSetCandidate(
            measurement_set_id="add-dialog-3",
            short_label="Kantarell og trakttrompetsopp fra Ørsta og Ålesund",
            name_as_published="Cortinarius rubellus Cooke — Blåbærgrøtsopp",
            locator_text="Vol. 2, p. 311",
            data_kind="range",
            raw_text="8.0–9.5 × 5.5–6.5 µm",
            revision=1,
            reference_work_id="work-other",
            reference_treatment_id="treatment-other",
            year=2020,
            taxon_id="99",
        ),
    ]


def _add_dialog_library(context: ReviewContext):
    from ui.add_reference_dialog import AddReferenceDialog

    _fixture(context)
    dialog = AddReferenceDialog(
        context.host,
        taxon_label="Cortinarius limonius",
        taxon_id="7",
        candidates=_add_dialog_candidates(),
        attach_callback=lambda *_args: None,
    )
    dialog.only_this_taxon_checkbox.setChecked(False)
    dialog.results_list.setCurrentRow(0)
    return dialog


def _add_dialog_library_empty(context: ReviewContext):
    from ui.add_reference_dialog import AddReferenceDialog

    _fixture(context)
    dialog = AddReferenceDialog(
        context.host,
        taxon_label="Cortinarius limonius",
        taxon_id="not-present-in-any-candidate",
        candidates=_add_dialog_candidates(),
        attach_callback=lambda *_args: None,
    )
    # "Only this taxon" defaults to checked; no candidate matches this
    # taxon id, so the results pane must show the honest empty state.
    return dialog


def _add_dialog_my_observations_candidates():
    from ui.add_reference_dialog import PersonalObservationCandidate

    # Reuses the long-locality/æøå convention from _add_dialog_candidates
    # rather than inventing a new fixture.
    return [
        PersonalObservationCandidate(
            observation_id=501,
            date="2024-06-12",
            author="Åse Øyen",
            location="Ørsta og Ålesund, ved gammel bjørkeskog med et forbausende langt stedsnavn",
            points=[
                {"length_um": 8.4, "width_um": 5.1},
                {"length_um": 8.9, "width_um": 5.4},
                {"length_um": 9.2, "width_um": 5.6},
            ],
        ),
        PersonalObservationCandidate(
            observation_id=502,
            date="2023-09-03",
            author="",
            location="",
            points=[{"length_um": 8.0, "width_um": 5.0}],
        ),
        PersonalObservationCandidate(
            observation_id=503,
            date="2022-10-21",
            author="Kari Nordmann",
            location="Trøndelag",
            points=[
                {"length_um": 7.8, "width_um": 4.9},
                {"length_um": 8.3, "width_um": 5.2},
            ],
        ),
    ]


def _add_dialog_my_observations(context: ReviewContext):
    from ui.add_reference_dialog import AddReferenceDialog

    _fixture(context)
    dialog = AddReferenceDialog(
        context.host,
        taxon_label="Cortinarius limonius",
        taxon_id="7",
        candidates=_add_dialog_candidates(),
        my_observations=_add_dialog_my_observations_candidates(),
        attach_callback=lambda *_args: None,
    )
    dialog.tabs.setCurrentIndex(dialog._my_observations_tab_index)
    dialog.my_observations_list.setCurrentRow(0)
    return dialog


def _add_dialog_community_results():
    # A long source label (independent elision on the title line) paired
    # with a long, æøå-bearing contributor (independent elision on the
    # detail line too) -- both rows use the same TwoLineRow widget as
    # Library/My-observations, so both lines must elide against their own
    # width, per _add_dialog_candidates' convention.
    return [
        {
            "_kind": "reference",
            "genus": "Cortinarius",
            "species": "limonius",
            "source": (
                "Niskanen, Liimatainen & Kytövuori's comprehensive northern "
                "European community reference submission"
            ),
            "contributor_label": (
                "Åse Øyen — Ørsta og Ålesund lokalt observasjonsnettverk for sjeldne slørsopper"
            ),
            "measurement_count": 0,
            "q_min": 1.6,
            "q_p50": 1.85,
            "q_max": 2.1,
            "length_min": 8.1,
            "length_p50": 9.3,
            "length_max": 10.8,
            "width_min": 4.5,
            "width_p50": 5.1,
            "width_max": 5.8,
        },
        {
            "_kind": "observation",
            "genus": "Cortinarius",
            "species": "limonius",
            "contributor_label": "sporely_community_user_42",
            "observed_on": "2025-04-18",
            "measurement_count": 24,
            "q_min": 1.5,
            "q_p50": 1.8,
            "q_max": 2.0,
            "measurements_json": [
                {"length_um": 8.0 + 0.1 * i, "width_um": 4.5 + 0.05 * i} for i in range(24)
            ],
            "length_min": 8.0,
            "length_p50": 9.0,
            "length_max": 10.5,
            "width_min": 4.5,
            "width_p50": 5.0,
            "width_max": 5.8,
        },
    ]


def _add_dialog_community(context: ReviewContext):
    from ui.add_reference_dialog import AddReferenceDialog

    _fixture(context)
    dialog = AddReferenceDialog(
        context.host,
        taxon_label="Cortinarius limonius",
        taxon_id="7",
        genus="Cortinarius",
        species="limonius",
        candidates=_add_dialog_candidates(),
        community_results=_add_dialog_community_results(),
        attach_callback=lambda *_args: None,
        cloud_attach_callback=lambda *_args: None,
    )
    dialog.tabs.setCurrentIndex(dialog._community_tab_index)
    dialog._community_pane.results_list.setCurrentRow(0)
    return dialog


def _add_dialog_community_points(context: ReviewContext):
    dialog = _add_dialog_community(context)
    dialog._community_pane.results_list.setCurrentRow(1)
    dialog._community_pane.raw_points_radio.setChecked(True)
    return dialog


def _add_dialog_community_empty(context: ReviewContext):
    from ui.add_reference_dialog import AddReferenceDialog

    _fixture(context)
    dialog = AddReferenceDialog(
        context.host,
        taxon_label="Cortinarius limonius",
        taxon_id="7",
        genus="Cortinarius",
        species="limonius",
        candidates=_add_dialog_candidates(),
        community_results=[],
        attach_callback=lambda *_args: None,
        cloud_attach_callback=lambda *_args: None,
    )
    dialog.tabs.setCurrentIndex(dialog._community_tab_index)
    return dialog


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
    rather than an isolated ``ComparisonListWidget``/``AddReferenceDialog``).
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


def _add_dialog_taxon_selector_candidates() -> list[dict]:
    return [
        {
            "source": "arts",
            "scientific_name": "Cortinarius rubellus",
            "vernacular": "Svært langt norsk navn — bittersnerlerørsopp fra blåbærskog æøå",
            "genus": "Cortinarius",
            "species": "rubellus",
            "score": 0.82,
        },
        {
            "source": "inat",
            "scientific_name": "Cortinarius orellanus",
            "vernacular": "",
            "genus": "Cortinarius",
            "species": "orellanus",
            "score": 0.41,
        },
    ]


def _add_dialog_taxon_selector(context: ReviewContext):
    """The taxon target selector with an AI candidate active, showing its
    match percentage and the re-filtered Library tab (stage-4b-fix Part 3).

    The base scenario captures the selected dialog. Open-popup scenarios
    use the shared post-show capture hook to grab the actual popup window.
    """
    from ui.add_reference_dialog import AddReferenceDialog

    _fixture(context)
    dialog = AddReferenceDialog(
        context.host,
        taxon_label="Cortinarius limonius",
        taxon_id="7",
        genus="Cortinarius",
        species="limonius",
        candidates=_add_dialog_candidates(),
        community_results=[],
        ai_candidates=_add_dialog_taxon_selector_candidates(),
        attach_callback=lambda *_args: None,
        cloud_attach_callback=lambda *_args: None,
    )
    # setCurrentIndex() alone does not emit activated() -- a real click
    # does both together, so the handler is invoked explicitly here too.
    dialog.taxon_target_combo.setCurrentIndex(1)
    dialog._on_taxon_target_activated(1)
    return dialog


def _add_dialog_default_size(context: ReviewContext):
    """Initial production size with empty isolated geometry/splitter settings."""
    from ui.add_reference_dialog import AddReferenceDialog

    _fixture(context)
    dialog = AddReferenceDialog(
        context.host,
        taxon_label="Cortinarius limonius",
        taxon_id="7",
        candidates=_add_dialog_candidates(),
        community_results=[],
        attach_callback=lambda *_args: None,
        cloud_attach_callback=lambda *_args: None,
    )
    dialog.results_list.setCurrentRow(0)
    return dialog


def _add_dialog_manual(context: ReviewContext, *, taxon_id: str | None = "7"):
    """Build the picker on its Enter-manually tab, with a real publication
    seeded in the isolated library so the publication combo has a
    realistic non-empty option list.
    """
    from ui.add_reference_dialog import AddReferenceDialog

    fixture = _fixture(context)
    dialog = AddReferenceDialog(
        context.host,
        taxon_label="Cortinarius limonius",
        taxon_id=taxon_id,
        genus="Cortinarius",
        species="limonius",
        candidates=_add_dialog_candidates(),
        community_results=[],
        attach_callback=lambda *_args: None,
        cloud_attach_callback=lambda *_args: None,
        manual_attach_callback=lambda *_args: True,
        manual_save_callback=lambda *_args: "saved-preview-set",
    )
    dialog.tabs.setCurrentIndex(dialog._manual_tab_index)
    return dialog, fixture


def _add_dialog_manual_range(context: ReviewContext):
    dialog, fixture = _add_dialog_manual(context)
    _select_work(dialog.manual_editor, fixture["work"].id)
    _populate_range(dialog.manual_editor)
    return dialog


def _add_dialog_manual_saved(context: ReviewContext):
    dialog = _add_dialog_manual_range(context)
    dialog._on_save_to_library_clicked()
    return dialog


def _add_dialog_manual_species_mean(context: ReviewContext):
    """Stage 5 follow-up: exercises ``ReferenceEntryEditor._refresh_preview``
    on the real embedded editor with only extreme bounds entered plus a
    directly reported Parmasto species mean (no min/max-table mean, no
    typical range) -- the mean must render with no derived marker, while a
    genuine typical-range fallback (Q row) still shows one, so both states
    are visible side by side from real widget state, not a hand-set
    fixture."""
    dialog, fixture = _add_dialog_manual(context)
    _select_work(dialog.manual_editor, fixture["work"].id)
    editor = dialog.manual_editor
    editor.minmax_table.setItem(0, 0, QTableWidgetItem("8.00"))
    editor.minmax_table.setItem(0, 4, QTableWidgetItem("11.00"))
    editor.parmasto_inputs["parmasto_length_mean"].setText("9.50")
    editor.minmax_table.setItem(1, 0, QTableWidgetItem("6.00"))
    editor.minmax_table.setItem(1, 4, QTableWidgetItem("8.00"))
    editor.minmax_table.setItem(1, 2, QTableWidgetItem("7.10"))
    editor.minmax_table.setItem(2, 1, QTableWidgetItem("1.20"))
    editor.minmax_table.setItem(2, 3, QTableWidgetItem("1.40"))
    editor._refresh_preview()
    return dialog


def _add_dialog_manual_points(context: ReviewContext):
    dialog, _fixture_data = _add_dialog_manual(context)
    _populate_raw_points(dialog.manual_editor)
    return dialog


def _add_dialog_manual_invalid(context: ReviewContext):
    """Enter-manually tab with no input: honest empty state, Add to plot
    stays disabled, matching the picker's other empty-state scenarios."""
    dialog, _fixture_data = _add_dialog_manual(context)
    return dialog


def _open_taxon_popup(dialog):
    dialog.taxon_target_combo.showPopup()
    return dialog.taxon_target_combo.view().window()


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
            viewport=(900, 720),
            build=_parmasto,
        ),
        ReviewScenario(
            id="reference.attach-taxon-filter",
            group="reference-library",
            title="Attach library reference with taxon filter",
            description="The attachment chooser is scoped to the active taxon with a candidate selected.",
            viewport=(980, 520),
            build=_make_attach_dialog,
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
            id="reference.dark",
            group="reference-library",
            title="Reference attachment chooser in application dark mode",
            description="The real dark palette and stylesheet exercise inputs, selection, disabled states, and buttons.",
            viewport=(980, 520),
            build=_make_attach_dialog,
            theme="dark",
        ),
        ReviewScenario(
            id="reference.community-preview",
            group="reference-library",
            title="Community review pane — ReferencePreviewPane standalone (light)",
            description="Exercises ReferencePreviewPane with a full summary table, long title, method fields, raw spores, and provenance in light theme.",
            viewport=(600, 500),
            build=_community_preview,
        ),
        ReviewScenario(
            id="reference.community-preview-dark",
            group="reference-library",
            title="Community review pane — ReferencePreviewPane standalone (dark)",
            description="Same ReferencePreviewPane state in dark theme to verify palette correctness.",
            viewport=(600, 500),
            build=_community_preview,
            theme="dark",
        ),
        ReviewScenario(
            id="reference.provenance-preview",
            group="reference-library",
            title="Reference review pane — reported-source wording + derived markers (light)",
            description="Method tab reworded for reported-source context (incl. a not-reported case) and a Summary table with derived-value footnote markers.",
            viewport=(600, 520),
            build=_reference_provenance_preview,
        ),
        ReviewScenario(
            id="reference.provenance-preview-method",
            group="reference-library",
            title="Reference review pane — Method tab, reported-source wording",
            description="Method tab reworded for reported-source context, including a not-reported case (mount medium, sample type, contrast, objective, scale all blank).",
            viewport=(600, 520),
            build=_reference_provenance_preview_method_tab,
        ),
        ReviewScenario(
            id="reference.provenance-preview-nb-no",
            group="reference-library",
            title="Reference review pane — Method tab, reported-source wording (Norwegian Bokmål)",
            description="The real Norwegian translator exercises the reworded Method tab labels.",
            viewport=(600, 520),
            build=_reference_provenance_preview_method_tab,
            locale="nb_NO",
        ),
        ReviewScenario(
            id="reference.provenance-preview-dark",
            group="reference-library",
            title="Reference review pane — reported-source wording + derived markers (dark)",
            description="Same state in dark theme to verify the footnote marker and tooltip-bearing cells stay legible.",
            viewport=(600, 520),
            build=_reference_provenance_preview,
            theme="dark",
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
            id="reference.add-dialog-library",
            group="reference-library",
            title="Add-reference picker — Library tab, result selected",
            description="Library tab with three results (incl. a 60+ char title and æøå names); one selected populates the preview pane.",
            viewport=(900, 560),
            build=_add_dialog_library,
        ),
        ReviewScenario(
            id="reference.add-dialog-library-dark",
            group="reference-library",
            title="Add-reference picker — Library tab (dark)",
            description="Same Library-tab state in dark theme to verify contrast.",
            viewport=(900, 560),
            build=_add_dialog_library,
            theme="dark",
        ),
        ReviewScenario(
            id="reference.add-dialog-library-empty",
            group="reference-library",
            title="Add-reference picker — Library tab, no matches",
            description="\"Only this taxon\" checked with zero matches: honest empty state, Add to plot stays disabled.",
            viewport=(900, 560),
            build=_add_dialog_library_empty,
        ),
        ReviewScenario(
            id="reference.add-dialog-library-empty-dark",
            group="reference-library",
            title="Add-reference picker — Library tab, no matches (dark)",
            description="Same empty-state case in dark theme.",
            viewport=(900, 560),
            build=_add_dialog_library_empty,
            theme="dark",
        ),
        ReviewScenario(
            id="reference.add-dialog-myobs",
            group="reference-library",
            title="Add-reference picker — My observations tab, result selected",
            description="Three personal observations of the working taxon (incl. æøå names and a long locality); one selected populates the shared preview pane with raw spores.",
            viewport=(900, 560),
            build=_add_dialog_my_observations,
        ),
        ReviewScenario(
            id="reference.add-dialog-myobs-dark",
            group="reference-library",
            title="Add-reference picker — My observations tab (dark)",
            description="Same My-observations-tab state in dark theme to verify contrast.",
            viewport=(900, 560),
            build=_add_dialog_my_observations,
            theme="dark",
        ),
        ReviewScenario(
            id="reference.add-dialog-community",
            group="reference-library",
            title="Add-reference picker — Community tab, range summary selected",
            description="A community reference result with a 60+ char source label and a long æøå contributor; both title and detail lines elide independently. Range summary radio populates the shared preview pane.",
            viewport=(900, 560),
            build=_add_dialog_community,
        ),
        ReviewScenario(
            id="reference.add-dialog-community-dark",
            group="reference-library",
            title="Add-reference picker — Community tab (dark)",
            description="Same Community-tab state in dark theme to verify contrast.",
            viewport=(900, 560),
            build=_add_dialog_community,
            theme="dark",
        ),
        ReviewScenario(
            id="reference.add-dialog-community-points",
            group="reference-library",
            title="Add-reference picker — Community tab, raw points selected",
            description="A community observation dataset with the \"Raw points\" radio checked; the radio label and preview show the real n=24.",
            viewport=(900, 560),
            build=_add_dialog_community_points,
        ),
        ReviewScenario(
            id="reference.add-dialog-community-empty",
            group="reference-library",
            title="Add-reference picker — Community tab, no results",
            description="No community results for the working taxon: honest empty state, Add to plot stays disabled.",
            viewport=(900, 560),
            build=_add_dialog_community_empty,
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
            id="reference.add-dialog-default-size",
            group="reference-library",
            title="Add-reference picker — derived default size",
            description="All four source tabs and all five preview sub-tabs are visible with no scroll arrows at the dialog's own derived minimum/default size.",
            viewport=(1400, 760),
            build=_add_dialog_default_size,
            natural_size=True,
        ),
        ReviewScenario(
            id="reference.add-dialog-taxon-selector",
            group="reference-library",
            title="Add-reference picker — taxon target selector",
            description="An AI candidate is active in the taxon target selector, showing its match percentage; the Library tab and dialog title have re-filtered to it.",
            viewport=(1400, 760),
            build=_add_dialog_taxon_selector,
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

    for suffix, builder, description in (
        (
            "manual-range",
            _add_dialog_manual_range,
            "Enter-manually tab, a realistic parsed literature range with a selected publication; the shared preview pane shows entered data, Save to library is enabled and Add to plot waits for saving.",
        ),
        (
            "manual-saved",
            _add_dialog_manual_saved,
            "Saved reference remains open with Add to plot enabled and duplicate saving disabled.",
        ),
        (
            "manual-points",
            _add_dialog_manual_points,
            "Enter-manually tab, eight raw paired spore points; the shared preview pane shows n and the raw spore data.",
        ),
        (
            "manual-invalid",
            _add_dialog_manual_invalid,
            "Enter-manually tab with no input yet: honest empty preview, Add to plot stays disabled.",
        ),
    ):
        for theme in ("light", "dark"):
            is_dark = theme == "dark"
            registry.register(ReviewScenario(
                id=f"reference.add-dialog-{suffix}" + ("-dark" if is_dark else ""),
                group="reference-library",
                title=f"Add-reference picker — {suffix.replace('-', ' ')}" + (" (dark)" if is_dark else ""),
                description=description,
                viewport=(1400, 760),
                build=builder,
                natural_size=True,
                theme=theme,
            ))
    registry.register(ReviewScenario(
        id="reference.add-dialog-manual-species-mean",
        group="reference-library",
        title="Add-reference picker — Enter manually, reported species mean",
        description="A directly reported Parmasto species mean renders with no derived marker while a genuine typical-range fallback (Q row) still shows one, using the real editor's _refresh_preview.",
        viewport=(1400, 760),
        build=_add_dialog_manual_species_mean,
        natural_size=True,
    ))
    registry.register(ReviewScenario(
        id="reference.add-dialog-manual-nb-no",
        group="reference-library",
        title="Add-reference picker — Enter manually in Norwegian Bokmål",
        description="A parsed literature range on the Enter-manually tab, exercised with the real Norwegian translator: labels, publication picker, and Data section fit without clipping.",
        viewport=(1400, 760),
        build=_add_dialog_manual_range,
        natural_size=True,
        locale="nb_NO",
    ))

    for theme in ("light", "dark"):
        for suffix, builder, viewport, natural, capture in (
            ("suppressed", _comparison_list_suppressed, (420, 260), False, None),
            ("colors", _comparison_list_colors, (420, 260), False, None),
            ("natural", _add_dialog_default_size, (1400, 760), True, None),
            ("popup", _add_dialog_taxon_selector, (1400, 760), True, _open_taxon_popup),
        ):
            registry.register(ReviewScenario(
                id=f"reference.fix2-{suffix}-{theme}", group="reference-library",
                title=f"Reference correction — {suffix} ({theme})",
                description=f"Deterministic {suffix} evidence with isolated settings.",
                build=builder, viewport=viewport, theme=theme,
                natural_size=natural, capture_target=capture,
            ))
    for suffix, builder, viewport, capture in (
        ("selector", _add_dialog_taxon_selector, (1400, 760), _open_taxon_popup),
        ("hint", _comparison_list_suppressed, (620, 260), None),
    ):
        registry.register(ReviewScenario(
            id=f"reference.fix2-{suffix}-nb-no", group="reference-library",
            title=f"Norwegian reference {suffix}",
            description="Scoped Norwegian catalogue loaded by the production Qt translator.",
            build=builder, viewport=viewport, locale="nb_NO", capture_target=capture,
        ))
