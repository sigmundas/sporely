"""The Analysis tab in a real ``MainWindow`` (redesign plan stage 7).

A *Cortinarius limonius* observation with 24 measured spores, a reference
library seeded like the reference-picker scenarios (``references._seed_library``)
and one of its measurement sets attached, in the shared isolated review
profile (``tools/review_ui/live_lab_harness.py``). No network or live data.
"""
from __future__ import annotations

import random

from PySide6.QtCore import QPointF
from PySide6.QtWidgets import QWidget

from ..context import ReviewContext
from ..live_lab_harness import create_observation, review_profile, use_database
from ..registry import ReviewScenario, ScenarioRegistry
from .measure_tab import _settle


def seed_analysis(context: ReviewContext, *, attach: bool = True) -> dict:
    """An observation with spores, a seeded library and (optionally) one attached set."""
    from database.models import ImageDB, MeasurementDB, ObservationDB
    from database.reference_library import ObservationReferenceUseRepository

    from .references import _seed_library

    profile = review_profile(context)
    count = int(context.state.get("analysis-tab.builds", 0)) + 1
    context.state["analysis-tab.builds"] = count
    use_database(profile, f"analysis-tab-{count}")
    observation_id = create_observation(
        profile, f"analysis-{count}", genus="Cortinarius", species="limonius", common_name="Sitronslørsopp",
    )
    ObservationDB.update_observation(observation_id, sporely_taxon_id=7)
    image_id = int(
        ImageDB.add_image(
            observation_id, str(profile.jpeg(f"A{count:04d}.JPG", seed=5)), "microscope",
            scale=0.1, objective_name="100X", contrast="DIC",
        )
    )
    rng = random.Random(11)
    for index in range(24):
        length = round(rng.gauss(9.7, 0.7), 2)
        width = round(rng.gauss(5.2, 0.35), 2)
        x, y = 40 + (index % 8) * 50, 40 + (index // 8) * 60
        points = [QPointF(x, y), QPointF(x + 28, y), QPointF(x + 14, y - 8), QPointF(x + 14, y + 8)]
        MeasurementDB.add_measurement(image_id, length, width, measurement_type="Spores", points=points)
    # An earlier observation of the same taxon, for "My observations".
    earlier_id = create_observation(
        profile, f"analysis-{count}-earlier", genus="Cortinarius", species="limonius",
        common_name="Sitronslørsopp", date="2025-09-21", location="Lianvatnet, Trondheim",
    )
    earlier_image = int(
        ImageDB.add_image(earlier_id, str(profile.jpeg(f"E{count:04d}.JPG", seed=6)), "microscope", scale=0.1)
    )
    for index in range(12):
        x = 40 + index * 30
        points = [QPointF(x, 50), QPointF(x + 28, 50), QPointF(x + 14, 42), QPointF(x + 14, 58)]
        MeasurementDB.add_measurement(
            earlier_image, round(rng.gauss(10.2, 0.6), 2), round(rng.gauss(5.5, 0.3), 2),
            measurement_type="spores", points=points,
        )
    _work, sets = _seed_library()
    if attach:
        ObservationReferenceUseRepository.attach(observation_id, sets[0].id, role="compared")
    return {"observation_id": observation_id, "image_id": image_id, "sets": sets}


def _analysis_tab(context: ReviewContext, *, size: tuple[int, int] = (1500, 950), after=None) -> QWidget:
    from ui.main_window import MainWindow

    seeded = seed_analysis(context)
    window = MainWindow()
    window.resize(*size)
    window.show()
    _settle(0.2)
    window.on_observation_selected(seeded["observation_id"], "Cortinarius limonius", switch_tab=False)
    window.tab_widget.setCurrentWidget(window.analysis_tab)
    _settle(0.8)
    if after is not None:
        after(window)
        _settle(0.5)
    return window


COMMUNITY_RESULTS = [
    {
        "_kind": "observation", "observation_id": 4242, "genus": "Cortinarius", "species": "limonius",
        "contributor_label": "sporely_community_user_7", "observed_on": "2025-05-01", "measurement_count": 3,
        "q_min": 1.7, "q_p50": 1.9, "q_max": 2.1,
        "measurements_json": [
            {"length_um": 9.0, "width_um": 5.0}, {"length_um": 9.6, "width_um": 5.2},
            {"length_um": 10.3, "width_um": 5.4},
        ],
        "length_min": 9.0, "length_p50": 9.6, "length_max": 10.3,
        "width_min": 5.0, "width_p50": 5.2, "width_max": 5.4,
    },
    {
        "_kind": "reference", "genus": "Cortinarius", "species": "limonius", "source": "mycena.no",
        "contributor_label": "mycena.no", "measurement_count": 0,
        "length_min": 8.0, "length_max": 10.5, "width_min": 4.5, "width_max": 6.0,
    },
]


def _open_source(source: str):
    def after(window) -> None:
        # Deterministic results instead of a Sporely Cloud search.
        window.reference_picker_community_results = lambda: [dict(row) for row in COMMUNITY_RESULTS]
        window._toggle_reference_drawer()
        page = window.reference_picker_page
        page.show_source(source)
        if source == "community":
            pane = page.community_pane
            pane.results_list.itemWidget(pane.results_list.item(1)).checkbox.click()
        else:
            row = page.my_observations_list.itemWidget(page.my_observations_list.item(0))
            if row is not None:
                row.checkbox.click()
    return after


def _open_drawer(window) -> None:
    window._toggle_reference_drawer()
    page = window.reference_picker_page
    rows = [page.results_list.itemWidget(page.results_list.item(i)) for i in range(page.results_list.count())]
    ticked = [row for row in rows if row is not None and row.is_checked()]
    if ticked:
        ticked[0].set_expanded(True)


CREATE_PASTED = "(9.0–)9.5–11.0(–12.0) × (5.0–)5.3–6.0(–6.4) µm, Q = 1.6–2.0, n = 30"


def _create_dialog(context: ReviewContext, state: str, *, linked: bool = True) -> QWidget:
    """The two-step "New reference" dialog (stage 7d) in ``state``:
    "source", "measurements" (nothing parsed) or "parsed"."""
    from ui.reference_create_dialog import MEASUREMENTS_STEP, ReferenceCreateDialog

    seeded = seed_analysis(context, attach=False)
    work_id = None
    if state != "source" or linked:
        from database.reference_library import TaxonTreatmentRepository

        work_id = TaxonTreatmentRepository.get(seeded["sets"][0].taxon_treatment_id).reference_work_id
    dialog = ReferenceCreateDialog(
        None,
        genus="Cortinarius",
        species="limonius",
        common_name="Sitronslørsopp",
        observation_id=seeded["observation_id"],
        sporely_taxon_id=7 if linked else None,
        save_callback=lambda _editor: None,
        add_callback=lambda _editor: False,
    )
    dialog.resize(760, 640)
    editor = dialog.editor
    if work_id:
        editor.publication_combo.setCurrentIndex(editor.publication_combo.findData(work_id))
        editor.locator_input.setText("pp. 146–148")
    if state != "source":
        dialog.show_step(MEASUREMENTS_STEP)
    if state == "parsed":
        editor.measurement_paste_input.setText(CREATE_PASTED)
        editor._on_parse_measurement_clicked()
    dialog.show()
    _settle(0.3)
    return dialog


def register_analysis_tab_scenarios(registry: ScenarioRegistry) -> None:
    for theme in ("light", "dark"):
        suffix = "" if theme == "light" else "-dark"
        registry.register(
            ReviewScenario(
                id=f"analysis.tab{suffix}",
                group="analysis",
                title=f"Analysis tab (nb{', dark' if theme == 'dark' else ''})",
                description="24 spores and one attached library reference in the plot and the comparison list.",
                build=lambda context: _analysis_tab(context),
                viewport=(1500, 950),
                locale="nb_NO",
                theme=theme,
            )
        )
    for theme in ("light", "dark"):
        suffix = "" if theme == "light" else "-dark"
        registry.register(
            ReviewScenario(
                id=f"analysis.reference-drawer{suffix}",
                group="analysis",
                title=f"Analysis tab, reference drawer open (nb{', dark' if theme == 'dark' else ''})",
                description="The reference drawer over the plot: one set attached (ticked), one row expanded (stage 7).",
                build=lambda context: _analysis_tab(context, after=_open_drawer),
                viewport=(1500, 950),
                locale="nb_NO",
                theme=theme,
            )
        )
    for source, label in (("community", "Community"), ("my-observations", "My observations")):
        registry.register(
            ReviewScenario(
                id=f"analysis.reference-drawer-{source}",
                group="analysis",
                title=f"Analysis tab, reference drawer: {label} (nb)",
                description=f"The {label} source in the reference drawer with one source ticked and plotted (stage 7c).",
                build=lambda context, source=source: _analysis_tab(
                    context, after=_open_source(source.replace("-", "_")),
                ),
                viewport=(1500, 950),
                locale="nb_NO",
            )
        )
    registry.register(
        ReviewScenario(
            id="analysis.reference-drawer-de",
            group="analysis",
            title="Analysis tab, reference drawer open (de)",
            description="The reference drawer with German strings.",
            build=lambda context: _analysis_tab(context, after=_open_drawer),
            viewport=(1500, 950),
            locale="de_DE",
        )
    )
    create_states = (
        ("source", "step 1, the publication (it already has sets for this taxon)"),
        ("measurements", "step 2 before parsing: only the paste field, Parse and Fill in manually"),
        ("parsed", "step 2 after Parse: the value grid"),
    )
    for state, description in create_states:
        registry.register(
            ReviewScenario(
                id=f"analysis.create-reference-{state}",
                group="analysis",
                title=f"New reference dialog: {state} (nb)",
                description=f"The two-step create dialog (stage 7d), {description}.",
                build=lambda context, state=state: _create_dialog(context, state),
                viewport=(760, 640),
                locale="nb_NO",
            )
        )
    registry.register(
        ReviewScenario(
            id="analysis.create-reference-parsed-dark",
            group="analysis",
            title="New reference dialog: parsed (nb, dark)",
            description="Step 2 after Parse in the dark theme.",
            build=lambda context: _create_dialog(context, "parsed"),
            viewport=(760, 640),
            locale="nb_NO",
            theme="dark",
        )
    )
    registry.register(
        ReviewScenario(
            id="analysis.create-reference-parsed-de",
            group="analysis",
            title="New reference dialog: parsed (de)",
            description="Step 2 after Parse with German strings.",
            build=lambda context: _create_dialog(context, "parsed"),
            viewport=(760, 640),
            locale="de_DE",
        )
    )
    registry.register(
        ReviewScenario(
            id="analysis.create-reference-unlinked",
            group="analysis",
            title="New reference dialog: unlinked taxon (nb)",
            description="Step 1 when the observation's taxon is not linked: the notice applies.",
            build=lambda context: _create_dialog(context, "source", linked=False),
            viewport=(760, 640),
            locale="nb_NO",
        )
    )
    registry.register(
        ReviewScenario(
            id="analysis.tab-small",
            group="analysis",
            title="Analysis tab, small window (nb)",
            description="The Analysis tab in a 1100 × 720 window.",
            build=lambda context: _analysis_tab(context, size=(1100, 720)),
            viewport=(1100, 720),
            locale="nb_NO",
        )
    )
