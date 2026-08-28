"""Reference Library scenarios and isolated SQLite fixture construction."""
from __future__ import annotations

import json
from unittest.mock import patch

from PySide6.QtWidgets import QTableWidgetItem

from database import schema as db_schema

from ..context import ReviewContext
from ..registry import ReviewScenario, ScenarioRegistry


def _fixture(context: ReviewContext):
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
    dialog = ReferenceLibraryManagerDialog(context.host, active_observation_id=42)
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
    )
    for scenario in scenarios:
        registry.register(scenario)
