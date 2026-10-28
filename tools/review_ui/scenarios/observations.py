"""Observation scenarios: image view metadata and taxon identification."""
from __future__ import annotations

from unittest.mock import patch

from types import SimpleNamespace

from PySide6.QtGui import QColor, QPixmap

from ui import observations_tab

from ..context import ReviewContext
from ..registry import ReviewScenario, ScenarioRegistry


def _image_metadata_tags(context: ReviewContext):
    assert context.temporary_root is not None
    image_path = context.temporary_root / "observation-image.png"
    pixmap = QPixmap(960, 600)
    pixmap.fill(QColor("#d9d2c3"))
    pixmap.save(str(image_path))
    context.enter_fixture(
        patch.object(
            observations_tab.ImageDB,
            "get_image",
            lambda _image_id: {
                "id": 7,
                "objective_name": "40x",
                "contrast": "DIC",
                "mount_medium": "Water",
                "stain": "Not_set",
                "sample_type": "Fresh",
                "sample_source": "Hymenium",
            },
        )
    )
    context.enter_fixture(
        patch.object(
            observations_tab,
            "load_objectives",
            lambda: {"40x": {"magnification": 40, "name": "40x"}},
        )
    )
    browser = observations_tab._ObservationImageBrowser()
    browser.set_items([{"id": 7, "path": str(image_path)}])
    return browser


def _flammulina_prediction() -> dict:
    """An Artsorakel prediction as synced from Android (recorded shape)."""
    taxon = {"scientificName": "Flammulina velutipes", "scientific_name": "Flammulina velutipes",
             "name": "Flammulina velutipes", "vernacularName": "vintersopp",
             "vernacular_name": "vintersopp", "id": "NBIC:54701", "taxonId": "NBIC:54701",
             "taxon_id": "NBIC:54701"}
    return {"rank": 1, "service": "artsorakel", "taxon_id": "NBIC:54701", "probability": 0.979365,
            "scientific_name": "Flammulina velutipes", "vernacular_name": "vintersopp",
            "taxon": taxon, "scientificName": "Flammulina velutipes", "vernacularName": "vintersopp"}


def _observation_editor(context: ReviewContext, *, draft: dict, with_ai: bool):
    """The real observation editor on the tracked taxonomy bundle.

    Isolated: a temporary database, no settings, no cloud client.
    """
    from database import schema as db_schema
    from ui.image_import_dialog import ImageImportResult
    from utils import taxonomy_v2

    root = context.temporary_root
    assert root is not None
    artifact = context.state.get("observations.taxonomy_artifact")
    if artifact is None:
        artifact = taxonomy_v2.ensure_installed(app_data_dir=root / "taxonomy")
        context.state["observations.taxonomy_artifact"] = artifact
    dialog_cls = observations_tab.ObservationDetailsDialog
    fake_client = SimpleNamespace(
        user_id=None,
        fetch_cloud_plan_profile=lambda: {"cloud_plan": "free", "is_pro": False},
        count_remote_privacy_slots=lambda: 0,
        list_remote_observations=lambda: [],
    )
    for target, name, value in (
        (db_schema, "get_database_path", lambda: root / "editor-mushrooms.db"),
        (observations_tab.SettingsDB, "get_setting",
         lambda key, default=None: "no" if key == "vernacular_language" else default),
        (observations_tab, "resolve_vernacular_db_path", lambda _lang: artifact),
        (dialog_cls, "_load_objectives", lambda self: {"default": {"is_default": True}}),
        (dialog_cls, "_load_tag_options", lambda self, category: []),
        (dialog_cls, "_load_habitat_tree", lambda self, filename: []),
        (dialog_cls, "_apply_primary_metadata", lambda self: None),
        (dialog_cls, "_apply_suggested_taxon", lambda self: None),
        (dialog_cls, "_sync_taxon_cache", lambda self: None),
        (dialog_cls, "_complete_deferred_dialog_setup", lambda self: None),
        (observations_tab.SporelyCloudClient, "from_stored_credentials", lambda: fake_client),
    ):
        context.enter_fixture(patch.object(target, name, value))
    image_path = root / "editor-field.png"
    pixmap = QPixmap(640, 480)
    pixmap.fill(QColor("#b8a27a"))
    pixmap.save(str(image_path))
    ai_state = {"predictions": {"0": [_flammulina_prediction()]}} if with_ai else None
    return dialog_cls(
        parent=None,
        draft_data=draft,
        image_results=[ImageImportResult(filepath=str(image_path), image_type="field")],
        ai_state=ai_state,
    )


def _taxon_link_offer(context: ReviewContext):
    return _observation_editor(
        context,
        draft={"genus": "Flammulina", "species": "velutipes", "common_name": "vintersopp",
               "ai_selected_service": "artsorakel", "ai_selected_taxon_id": "NBIC:54701",
               "ai_selected_scientific_name": "Flammulina velutipes"},
        with_ai=True,
    )


def _editor_with_ai(context: ReviewContext):
    return _observation_editor(context, draft={}, with_ai=True)


def _bound_editor(context: ReviewContext):
    """Saved with the COL entry, Usikker and Mikroskopi; Artsorakel says
    NBIC:54701 (the NorTaxa entry). The saved row comes from a real pick."""
    first = _observation_editor(context, draft={}, with_ai=False)
    page = first._taxon_lookup.search_taxa("Flammulina velutipes")
    row = next(r for r in page.rows if r.name == "Flammulina velutipes")
    first._on_taxon_picker_concept(row, next(c for c in row.concepts if c.source_name == "COL"))
    saved = dict(first.get_data())
    first._cleanup_dialog_threads()
    first.deleteLater()
    saved.update(uncertain=1, determination_method=1, ai_selected_service="artsorakel",
                 ai_selected_taxon_id="NBIC:54701", ai_selected_scientific_name="Flammulina velutipes")
    return _observation_editor(context, draft=saved, with_ai=True)


def _free_name_editor(context: ReviewContext):
    return _observation_editor(context, draft={"species": "Cortinarius sp. «SKA-2026-14»"}, with_ai=False)


def _settle_editor_layout(dialog):
    """Give the gallery its compact default height, as the deferred dialog
    setup (skipped here: it reads user settings) would."""
    from PySide6.QtWidgets import QApplication

    dialog._apply_dialog_gallery_splitter_height(None)
    QApplication.processEvents()
    return dialog


def _search_in_editor(query: str):
    def capture(dialog):
        from PySide6.QtWidgets import QApplication

        _settle_editor_layout(dialog)
        picker = dialog.taxon_picker
        if not picker.search_input.isVisible():
            picker.begin_edit()
        picker.search_for(query)
        QApplication.processEvents()
        return dialog
    return capture


def register_observation_scenarios(registry: ScenarioRegistry) -> None:
    for scenario in (
        ReviewScenario(
            id="observations.taxon-link-offer",
            group="observations",
            title="Observation editor — link offer from an Artsorakel identifier",
            description=(
                "Flammulina velutipes synced from Android with NBIC:54701 and no "
                "database taxon: one line offers the database taxon, with Link."
            ),
            viewport=(1280, 1000),
            locale="nb_NO",
            build=_taxon_link_offer,
            capture_target=_settle_editor_layout,
        ),
        ReviewScenario(
            id="observations.taxon-picker-search",
            group="observations",
            title="Observation editor — species search with two sources",
            description=(
                "Flammulina vel in the unified picker: the Flammulina velutipes row offers NorTaxa and COL, "
                "NorTaxa filled because the Artsorakel identifier points to it; the highlighted row "
                "shows how to switch source."
            ),
            viewport=(1280, 1000),
            locale="nb_NO",
            build=_editor_with_ai,
            capture_target=_search_in_editor("Flammulina vel"),
        ),
        ReviewScenario(
            id="observations.taxon-picker-card",
            group="observations",
            title="Observation editor — saved identification",
            description=(
                "Saved with the COL entry, Usikker and Mikroskopi: the card keeps COL although "
                "Artsorakel points to NorTaxa; Kilde switches, and the Artsorakel chips only offer."
            ),
            viewport=(1280, 1000),
            locale="nb_NO",
            build=_bound_editor,
            capture_target=_settle_editor_layout,
        ),
        ReviewScenario(
            id="observations.taxon-picker-free-name",
            group="observations",
            title="Observation editor — name without a taxon",
            description="A provisional name kept whole without a taxon: the grey card.",
            viewport=(1280, 1000),
            locale="nb_NO",
            build=_free_name_editor,
            capture_target=_settle_editor_layout,
        ),
        ReviewScenario(
            id="observations.taxon-picker-search-dark",
            group="observations",
            title="Observation editor — species search, dark theme",
            description="The velutipes search in the dark theme.",
            viewport=(1280, 1000),
            theme="dark",
            locale="nb_NO",
            build=_editor_with_ai,
            capture_target=_search_in_editor("velutipes"),
        ),
    ):
        registry.register(scenario)
    registry.register(
        ReviewScenario(
            id="observations.image-metadata-tags",
            group="observations",
            title="Observation image metadata tags",
            description="Image view exposes editable microscope tags and an explicit no-stain state.",
            viewport=(1040, 680),
            build=_image_metadata_tags,
        )
    )

