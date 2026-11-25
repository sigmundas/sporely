"""The Analysis tab in a real ``MainWindow`` (redesign plan stage 7).

A *Cortinarius limonius* observation with 24 measured spores, a reference
library seeded like the reference-picker scenarios (``references._seed_library``)
and one of its measurement sets attached, in the shared isolated review
profile (``tools/review_ui/live_lab_harness.py``). No network or live data.
"""
from __future__ import annotations

import random

from dataclasses import replace

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


def _squeeze_gallery(window) -> None:
    """A gallery strip saved at the old 100 px floor, restored as a user resize."""
    from ui.splitter_state import GALLERY_MIN_HEIGHT

    splitter = window.gallery_splitter
    total = sum(splitter.sizes())
    splitter.setSizes([total - GALLERY_MIN_HEIGHT, GALLERY_MIN_HEIGHT])
    window._gallery_splitter_user_resized = True
    window._set_gallery_strip_height()


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


#: Extra library rows for the range bars: the same genus, the rest of the
#: library (a long name, one far outside the axes, one without a width) and
#: enough of them to scroll. ``(name, taxon id, length, width)``: a range is
#: ``(min, core min, core max, max)`` with ``None`` where nothing was stated.
EXTRA_LIBRARY = (
    ("Cortinarius pulchripes Rob. Henry", "98", (None, 9.0, 11.5, None), (None, 5.5, 6.5, None)),
    ("Cortinarius albonigrellus Rob. Henry", "97", (7.5, 8.0, 9.5, 10.0), (4.0, 4.5, 5.0, 5.5)),
    ("Hygrophorus hypothejus (Fr.) Fr.", "201", (None, 7.0, 9.0, None), (None, 4.0, 5.0, None)),
    ("Hypholoma capnoides (Fr.) P. Kumm.", "202", (None, 7.0, 9.0, None), (None, 4.0, 5.0, None)),
    ("Strobilurus stephanocystis (Kühner & Romagn. ex Hora) Singer", "203", (None, 5.8, 7.0, None), (None, 3.0, 3.9, None)),
    ("Inocybe geophylla (Bull.) P. Kumm.", "204", (None, 7.5, 10.5, None), (None, 4.5, 6.5, None)),
    ("Sarcomyxa serotina (Pers.) P. Karst.", "205", (None, 4.0, 6.5, None), (None, 1.0, 2.0, None)),
    ("Macrolepiota procera (Scop.) Singer", "206", (12.0, 13.0, 17.0, 19.0), (8.0, 9.0, 11.0, 12.0)),
    ("Morchella esculenta (L.) Pers.", "207", (None, 18.0, 25.0, None), (None, 11.0, 15.0, None)),
    ("Laccaria laccata (Scop.) Cooke", "208", (None, 7.0, 10.0, None), None),
    ("Flammulina velutipes (Curtis) Singer", "209", (None, 5.9, 8.1, None), (None, 3.0, 4.1, None)),
)


def _seed_extra_library() -> None:
    from database.reference_library import (
        MeasurementSet,
        MeasurementSetRepository,
        ReferenceWork,
        ReferenceWorkRepository,
        TaxonTreatment,
        TaxonTreatmentRepository,
    )

    work = ReferenceWorkRepository.create(ReferenceWork(
        id="", type="book", title="Funga Nordica", short_label="Funga Nordica", year=2008,
    ))
    for name, taxon_id, length, width in EXTRA_LIBRARY:
        treatment = TaxonTreatmentRepository.create(TaxonTreatment(
            id="", reference_work_id=work.id, taxon_id=taxon_id, name_as_published=name,
        ))
        values = {}
        for metric, bounds in (("length", length), ("width", width)):
            if bounds is None:
                continue
            low, core_low, core_high, high = bounds
            values.update({
                f"{metric}_min": low, f"{metric}_core_min": core_low,
                f"{metric}_core_max": core_high, f"{metric}_max": high,
            })
        text = " × ".join(
            f"{b[1]}–{b[2]}" for b in (length, width) if b is not None
        ) + " µm"
        MeasurementSetRepository.create(MeasurementSet(
            id="", taxon_treatment_id=treatment.id, character="spore_size", data_kind="range",
            raw_text=text, **values,
        ))


def _range_bars():
    """The drawer with the whole library listed, each row with its range bars."""
    def after(window) -> None:
        _seed_extra_library()
        window._toggle_reference_drawer()
        page = window.reference_picker_page
        page.only_this_taxon_checkbox.setChecked(False)
    return after


def _open_drawer(window) -> None:
    window._toggle_reference_drawer()
    page = window.reference_picker_page
    rows = [page.results_list.itemWidget(page.results_list.item(i)) for i in range(page.results_list.count())]
    ticked = [row for row in rows if row is not None and row.is_checked()]
    if ticked:
        ticked[0].set_expanded(True)


#: Stored AI predictions for "Compare with": a species in the sample
#: taxonomy, a second one, and one it does not have (a name to search for).
COMPARE_AI_STATE = {
    "predictions": {
        "0": [
            {"probability": 0.41, "scientificName": "Cortinarius pulchripes", "name": "Cortinarius pulchripes"},
            {"probability": 0.18, "scientificName": "Cortinarius rubellus", "name": "Cortinarius rubellus"},
            {"probability": 0.07, "scientificName": "Cortinarius albonigrellus", "name": "Cortinarius albonigrellus"},
        ],
    },
}


def _compare(context: ReviewContext, state: str):
    """The reference drawer's "Compare with" (stage 7e): "picker" while
    changing it (the AI suggestions under the search), "other" after a name
    the library has sets for was chosen."""
    def after(window) -> None:
        import json

        from database.models import ObservationDB

        from ..taxon_sample import sample_lookup

        ObservationDB.update_observation(
            int(window.active_observation_id), ai_state_json=json.dumps(COMPARE_AI_STATE)
        )
        path = review_profile(context).root / f"taxonomy-sample-{context.state.get('analysis-tab.builds', 0)}.sqlite3"
        # The sample taxonomy instead of the installed names database.
        window.reference_picker_taxon_lookup = lambda: sample_lookup(path)
        window._toggle_reference_drawer()
        page = window.reference_picker_page
        page.begin_target_change()
        if state == "other":
            picker = page.target_picker
            picker.search_for("Cortinarius rubellus")
            # Not in the taxonomy: the last entry uses it as a name.
            picker._activate_index(picker.model.index(picker.model.rowCount() - 1, 0))
    return after


def _update_failed(window) -> None:
    """Update from library for an attachment that is gone: the error on
    Analysis's bar with Details open (stage 10e2). The real handler, so the
    error under Details is the repository's own."""
    window._update_reference_use_from_library("5f0c2a9e-7d41-4c55-9a0b-3e2f8c1d6b70", int(window.active_observation_id))
    window.gallery_hint_bar._details_btn.click()


CREATE_PASTED = "(9.0–)9.5–11.0(–12.0) × (5.0–)5.3–6.0(–6.4) µm, Q = 1.6–2.0, n = 30"


def _create_dialog(context: ReviewContext, state: str, *, linked: bool = True) -> QWidget:
    """The two-step "New reference" dialog (stage 7d) in ``state``:
    "source", "measurements" (nothing parsed) or "parsed"."""
    from ui.reference_create_dialog import MEASUREMENTS_STEP, ReferenceCreateDialog

    seeded = seed_analysis(context, attach=False)
    work_id = None
    if state != "source" or linked:
        from database.reference_library import ReferenceWorkRepository, TaxonTreatmentRepository

        work_id = TaxonTreatmentRepository.get(seeded["sets"][0].taxon_treatment_id).reference_work_id
        if state == "ambiguous":
            # A second publication with the same label (stage 10e2).
            ReferenceWorkRepository.create(replace(ReferenceWorkRepository.get(work_id), id="", doi=None))
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
    if state not in ("source", "ambiguous"):
        dialog.show_step(MEASUREMENTS_STEP)
    if state == "parsed":
        editor.measurement_paste_input.setText(CREATE_PASTED)
        editor._on_parse_measurement_clicked()
    if state == "ambiguous":
        # The typed name matches both publications; Next says so by the field.
        label = editor.publication_combo.itemText(editor.publication_combo.findData(work_id))
        editor.publication_combo.setCurrentIndex(0)
        editor.publication_combo.lineEdit().setText(label)
        dialog._on_next_clicked()
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
    for suffix, theme, locale, size in (
        ("", "light", "nb_NO", (1500, 950)),
        ("-dark", "dark", "nb_NO", (1500, 950)),
        ("-de", "light", "de_DE", (1500, 950)),
        ("-small", "light", "nb_NO", (1100, 760)),
    ):
        registry.register(
            ReviewScenario(
                id=f"analysis.reference-drawer-bars{suffix}",
                group="analysis",
                title=f"Analysis tab, reference drawer: range bars ({locale}, {theme}, {size[0]}x{size[1]})",
                description="The whole library listed (\"Only this taxon\" off), each row with its length and width bars against the observation's spores.",
                build=lambda context, size=size: _analysis_tab(context, size=size, after=_range_bars()),
                viewport=size,
                locale=locale,
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
    compare_states = (
        ("picker", "", "nb_NO", "light", "changing it: the taxon search with the AI suggestions under it"),
        ("picker", "-dark", "nb_NO", "dark", "changing it, dark theme"),
        ("picker", "-de", "de_DE", "light", "changing it, German strings"),
        ("other", "", "nb_NO", "light", "another taxon chosen: the library lists its sets, and Reset goes back"),
        ("other", "-de", "de_DE", "light", "another taxon chosen, German strings"),
    )
    for state, suffix, locale, theme, description in compare_states:
        registry.register(
            ReviewScenario(
                id=f"analysis.reference-compare-{state}{suffix}",
                group="analysis",
                title=f"Analysis tab, reference drawer: Compare with ({state}, {locale}, {theme})",
                description=f"Compare with in the reference drawer, {description} (stage 7e).",
                build=lambda context, state=state: _analysis_tab(context, after=_compare(context, state)),
                viewport=(1500, 950),
                locale=locale,
                theme=theme,
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
    for theme, locale, suffix in (("light", "nb_NO", ""), ("dark", "nb_NO", "-dark"), ("light", "de_DE", "-de")):
        registry.register(
            ReviewScenario(
                id=f"analysis.reference-update-failed{suffix}",
                group="analysis",
                title=f"Analysis tab: a library update failed ({locale[:2]}{', dark' if theme == 'dark' else ''})",
                description="Update from library for a gone attachment: the error on Analysis's bar, Details open (stage 10e2).",
                build=lambda context: _analysis_tab(context, after=_update_failed),
                viewport=(1500, 950),
                locale=locale,
                theme=theme,
            )
        )
    for locale, suffix in (("nb_NO", ""), ("de_DE", "-de")):
        registry.register(
            ReviewScenario(
                id=f"analysis.create-reference-ambiguous{suffix}",
                group="analysis",
                title=f"New reference dialog: publication name matches two ({locale[:2]})",
                description="Step 1: the typed name matches two publications; Next says so in red under the field (stage 10e2).",
                build=lambda context: _create_dialog(context, "ambiguous"),
                viewport=(760, 640),
                locale=locale,
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
    for suffix, theme, size in (("", "light", (1500, 950)), ("-dark", "dark", (1500, 950)), ("-small", "light", (1100, 720))):
        registry.register(
            ReviewScenario(
                id=f"analysis.gallery-squeezed{suffix}",
                group="analysis",
                title=f"Analysis tab, gallery strip dragged small (nb, {theme}, {size[0]}x{size[1]})",
                description="The gallery splitter restored at the old 100 px floor: the plot gives way, the thumbnails stay whole above the hint bar.",
                build=lambda context, size=size: _analysis_tab(context, size=size, after=_squeeze_gallery),
                viewport=size,
                locale="nb_NO",
                theme=theme,
            )
        )
