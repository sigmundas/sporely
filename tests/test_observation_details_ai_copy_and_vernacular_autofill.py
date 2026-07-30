"""Regression tests for two observation-detail dialog bugs seen on
2026-07-30:

Bug A — the Artsorakel Copy button rejected genus-only rows (e.g.
``trevlesopper (Inocybe)``) with the orange status "Could not parse
genus/species from AI suggestion." even though a genus-level Norwegian
common name plus its parenthesised scientific genus is a valid
identification level and should populate Name + Genus while leaving
Species and Red List empty. Applies equally to iNaturalist genus-only
suggestions.

Bug B — typing a Norwegian vernacular into the Name (vernacular) field
did not auto-populate the Genus field even when the common name
resolved unambiguously to a single genus across all matching taxa.
Stage 3B.5 invariant: a vernacular-only edit MUST NOT clear an existing
Red List badge and MUST NOT overwrite user-typed Genus.

Both suites drive the real ``ObservationDetailsDialog`` so the actual
signal wiring is covered — the controller's ``_suspended`` context, the
identity-clear handler, and the new vernacular-editing-finished hook
all run in production shape.
"""
from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PIL import Image
from PySide6.QtWidgets import QApplication

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import ui.observations_tab as observations_tab
from ui.image_import_dialog import ImageImportResult


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def _seed_vernacular_v2_db(db_path: Path) -> None:
    """Seed a taxonomy-v2 shape with three vernaculars useful for the
    Bug B suite:

    * ``alvetrevlesopp`` -> two ``Inocybe`` species (unique genus,
      multiple species — the exact real DB shape at
      ``tax-2026.07.30-02``);
    * ``trevlesopper`` -> two different genera (``Inocybe`` +
      ``Inosperma``) — the ambiguous-across-genera guard;
    * ``knappesopp`` -> one ``Agaricus`` species (unique species-level
      match — the controller's own ``on_vernacular_editing_finished``
      would fire here; the dialog-level genus autofill hook must NOT
      race against it).
    """
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("CREATE TABLE taxonomy_meta (key TEXT PRIMARY KEY, value TEXT)")
        conn.execute(
            "INSERT INTO taxonomy_meta VALUES ('release', 'tax-2026.07.30-02')",
        )
        conn.execute(
            """
            CREATE TABLE taxon_min (
                taxon_id INTEGER PRIMARY KEY,
                genus TEXT,
                specific_epithet TEXT,
                family TEXT,
                canonical_scientific_name TEXT,
                taxon_rank TEXT,
                canonical_source_system TEXT,
                taxonomic_status TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE scientific_name_min (
                scientific_name_id INTEGER PRIMARY KEY AUTOINCREMENT,
                taxon_id INTEGER,
                language_code TEXT,
                scientific_name TEXT,
                is_preferred_name INTEGER,
                source TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE vernacular_min (
                vernacular_id INTEGER PRIMARY KEY AUTOINCREMENT,
                taxon_id INTEGER,
                vernacular_name TEXT,
                is_preferred_name INTEGER,
                language_code TEXT,
                source TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE taxon_redlist_min (
                taxon_id INTEGER,
                source_system TEXT,
                source_release TEXT,
                assessment_area TEXT,
                assessment_id TEXT,
                category_raw TEXT,
                category_code TEXT,
                category_is_downgraded INTEGER,
                criteria TEXT,
                expert_group TEXT,
                assessment_url TEXT,
                scientific_name_snapshot TEXT,
                authorship_snapshot TEXT,
                taxon_rank_snapshot TEXT,
                assessed_name_source TEXT,
                assessed_name_namespace TEXT,
                assessed_name_id TEXT
            )
            """
        )
        # Inocybe species — three of them, two share the vernacular
        # ``alvetrevlesopp``.
        conn.execute(
            "INSERT INTO taxon_min VALUES (500001, 'Inocybe', 'mystica', "
            "'Inocybaceae', 'Inocybe mystica', 'species', 'col_xr', 'accepted')"
        )
        conn.execute(
            "INSERT INTO taxon_min VALUES (500002, 'Inocybe', 'dvaliniana', "
            "'Inocybaceae', 'Inocybe dvaliniana', 'species', 'col_xr', 'accepted')"
        )
        conn.execute(
            "INSERT INTO taxon_min VALUES (500003, 'Inocybe', 'geophylla', "
            "'Inocybaceae', 'Inocybe geophylla', 'species', 'col_xr', 'accepted')"
        )
        # Inosperma species, sharing the ambiguous ``trevlesopper`` root
        # vernacular but NOT the ``alvetrevlesopp`` name.
        conn.execute(
            "INSERT INTO taxon_min VALUES (500010, 'Inosperma', 'erubescens', "
            "'Inocybaceae', 'Inosperma erubescens', 'species', 'col_xr', 'accepted')"
        )
        # Agaricus species — unique species-level match for ``knappesopp``.
        conn.execute(
            "INSERT INTO taxon_min VALUES (500020, 'Agaricus', 'bisporus', "
            "'Agaricaceae', 'Agaricus bisporus', 'species', 'col_xr', 'accepted')"
        )
        # Cantharellus cibarius — used in Bug A regression: pre-existing
        # species-level Red List badge that must clear on the genus-only
        # copy.
        conn.execute(
            "INSERT INTO taxon_min VALUES (500030, 'Cantharellus', 'cibarius', "
            "'Cantharellaceae', 'Cantharellus cibarius', 'species', 'nortaxa', 'valid')"
        )
        for tid in (500001, 500002, 500003, 500010, 500020, 500030):
            conn.execute(
                "INSERT INTO scientific_name_min "
                "(taxon_id, language_code, scientific_name, is_preferred_name, source) "
                "VALUES (?, 'sci', "
                "(SELECT canonical_scientific_name FROM taxon_min WHERE taxon_id=?), "
                "1, (SELECT canonical_source_system FROM taxon_min WHERE taxon_id=?))",
                (tid, tid, tid),
            )

        # alvetrevlesopp -> two Inocybe species, single genus.
        conn.execute(
            "INSERT INTO vernacular_min "
            "(taxon_id, vernacular_name, is_preferred_name, language_code, source) "
            "VALUES (500001, 'alvetrevlesopp', 1, 'nb', 'artsdatabanken')"
        )
        conn.execute(
            "INSERT INTO vernacular_min "
            "(taxon_id, vernacular_name, is_preferred_name, language_code, source) "
            "VALUES (500002, 'alvetrevlesopp', 1, 'nb', 'artsdatabanken')"
        )
        # trevlesopper -> two distinct genera, ambiguous.
        conn.execute(
            "INSERT INTO vernacular_min "
            "(taxon_id, vernacular_name, is_preferred_name, language_code, source) "
            "VALUES (500003, 'trevlesopper', 1, 'nb', 'artsdatabanken')"
        )
        conn.execute(
            "INSERT INTO vernacular_min "
            "(taxon_id, vernacular_name, is_preferred_name, language_code, source) "
            "VALUES (500010, 'trevlesopper', 1, 'nb', 'artsdatabanken')"
        )
        # knappesopp -> single species (Agaricus bisporus).
        conn.execute(
            "INSERT INTO vernacular_min "
            "(taxon_id, vernacular_name, is_preferred_name, language_code, source) "
            "VALUES (500020, 'knappesopp', 1, 'nb', 'artsdatabanken')"
        )
        # kantarell -> single species (Cantharellus cibarius). Used in
        # the "prior species-level badge clears" test.
        conn.execute(
            "INSERT INTO vernacular_min "
            "(taxon_id, vernacular_name, is_preferred_name, language_code, source) "
            "VALUES (500030, 'kantarell', 1, 'nb', 'artsdatabanken')"
        )
        # Cantharellus cibarius carries a Red List assessment so a
        # species-level copy can produce a real badge that the
        # subsequent genus-only copy has to clear.
        conn.execute(
            "INSERT INTO taxon_redlist_min VALUES (500030, 'artsdatabanken', "
            "'2021', 'Norge', '500030-N', 'LC', 'LC', 0, NULL, NULL, NULL, "
            "'Cantharellus cibarius', NULL, 'species', 'artsdatabanken', "
            "'artsnavnebase', '500030-N')"
        )
        conn.commit()


def _make_dialog_patches(monkeypatch, *, taxonomy_db_path: Path | None = None) -> None:
    fake_client = SimpleNamespace(
        user_id="user-abc",
        fetch_cloud_plan_profile=lambda: {"cloud_plan": "free", "is_pro": False},
        count_remote_privacy_slots=lambda: 0,
        list_remote_observations=lambda: [],
    )
    monkeypatch.setattr(
        observations_tab.SettingsDB,
        "get_setting",
        lambda key, default=None: "no" if key == "vernacular_language" else default,
    )
    monkeypatch.setattr(
        observations_tab,
        "resolve_vernacular_db_path",
        lambda _lang: taxonomy_db_path,
    )
    monkeypatch.setattr(
        observations_tab.ObservationDetailsDialog,
        "_load_objectives",
        lambda self: {"default": {"is_default": True}},
    )
    monkeypatch.setattr(
        observations_tab.ObservationDetailsDialog,
        "_load_tag_options",
        lambda self, category: [f"{category}-default"],
    )
    monkeypatch.setattr(
        observations_tab.ObservationDetailsDialog,
        "_load_habitat_tree",
        lambda self, filename: [],
    )
    monkeypatch.setattr(
        observations_tab.ObservationDetailsDialog,
        "_apply_primary_metadata",
        lambda self: None,
    )
    monkeypatch.setattr(
        observations_tab.ObservationDetailsDialog,
        "_apply_suggested_taxon",
        lambda self: None,
    )
    monkeypatch.setattr(
        observations_tab.ObservationDetailsDialog,
        "_sync_taxon_cache",
        lambda self: None,
    )
    monkeypatch.setattr(
        observations_tab.ObservationDetailsDialog,
        "_complete_deferred_dialog_setup",
        lambda self: None,
    )
    monkeypatch.setattr(
        observations_tab.SporelyCloudClient,
        "from_stored_credentials",
        lambda: fake_client,
    )


def _make_image(path: Path) -> Path:
    image = Image.new("RGB", (2, 2), color=(180, 180, 180))
    image.save(path, quality=90)
    return path


def _build_dialog(
    monkeypatch,
    qapp,
    *,
    tmp_path: Path,
    observation: dict | None = None,
    draft_data: dict | None = None,
    taxonomy_db_path: Path | None = None,
):
    _make_dialog_patches(monkeypatch, taxonomy_db_path=taxonomy_db_path)
    image_path = _make_image(tmp_path / "img.jpg")
    dialog = observations_tab.ObservationDetailsDialog(
        parent=None,
        observation=observation,
        draft_data=draft_data,
        image_results=[
            ImageImportResult(filepath=str(image_path), image_type="field"),
        ],
    )
    qapp.processEvents()
    return dialog


def _select_prediction(
    dialog: observations_tab.ObservationDetailsDialog,
    prediction: dict,
    *,
    source: str,
) -> None:
    """Stage the AI prediction as the current dialog selection so
    ``_on_ai_copy_to_taxonomy`` treats it as the active row.

    Bypasses the QTableWidget rendering pipeline (which requires an
    image index) — the copy handler falls back to the by-index selected
    dict which is what the panel actually reads on button press.
    """
    dialog._current_ai_selected_fields = {}
    dialog.image_gallery_selection = []
    # ``_current_ai_index`` returns the primary image index; stub to 0
    # so the by-index caches are populated for the fallback lookup.
    if source == "inat":
        dialog._inat_selected_by_index[0] = prediction
        dialog._inat_predictions_by_index[0] = [prediction]
    else:
        dialog._ai_selected_by_index[0] = prediction
        dialog._ai_predictions_by_index[0] = [prediction]
    dialog._ai_selected_taxon = observations_tab._normalize_ai_prediction_taxon(
        prediction, source=source,
    )


# ---------------------------------------------------------------------------
# Bug A — genus-only Artsorakel/iNat rows must copy without error.
# ---------------------------------------------------------------------------


def test_artsorakel_copy_genus_only_row_populates_name_and_genus_leaves_species_empty(
    monkeypatch, qapp, tmp_path: Path,
) -> None:
    """Reproduces the ``trevlesopper (Inocybe)`` bug: Copy must fill
    Genus + Name, leave Species empty, keep Red List clear, and emit
    the green "Copied to taxonomy." status — never the orange
    "Could not parse genus/species from AI suggestion." refusal.
    """
    dialog = _build_dialog(monkeypatch, qapp, tmp_path=tmp_path)
    try:
        prediction = {
            "probability": 0.451,
            "scientificName": "Inocybe",
            "name": "Inocybe",
            "vernacularName": "trevlesopper",
            "taxonId": "NBIC:12345",
            "id": "NBIC:12345",
        }
        _select_prediction(dialog, prediction, source="arts")
        # Baseline: species field non-empty from a hypothetical prior
        # species-level state so we can prove the genus-only copy
        # cleared it.
        with dialog._taxon_controller._suspended():
            dialog.species_input.setText("stalemate")

        dialog._on_ai_copy_to_taxonomy("arts")
        qapp.processEvents()

        assert dialog.vernacular_input.text() == "trevlesopper"
        assert dialog.genus_input.text() == "Inocybe"
        assert dialog.species_input.text() == ""
        # No species-level identity → no Red List badge (state, not
        # widget visibility — the offscreen platform reports False for
        # every unattached widget, so we assert on the model field).
        assert dialog._red_list_category == ""
        # Status footer must reflect success, not the parse-failure hint.
        arts_status = dialog.ai_status_labels.get("arts")
        assert arts_status is not None
        assert "Could not parse" not in arts_status.text()
        assert arts_status.text() == "Copied to taxonomy."
        # AI selection metadata still captured — the "Selected AI" summary
        # row should be populated even for a genus-only pick.
        assert dialog._current_ai_selected_fields.get("ai_selected_service") == "artsorakel"
        assert dialog._current_ai_selected_fields.get("ai_selected_taxon_id") == "NBIC:12345"
        assert dialog._current_ai_selected_fields.get("ai_selected_scientific_name") == "Inocybe"
        # Unidentified checkbox flipped off so the fields become editable.
        assert dialog.unidentified_checkbox.isChecked() is False
    finally:
        dialog._cleanup_dialog_threads()
        dialog.deleteLater()


def test_artsorakel_copy_genus_only_row_clears_prior_species_level_red_list(
    monkeypatch, qapp, tmp_path: Path,
) -> None:
    """A species-level Artsorakel copy attached an LC badge; a following
    genus-only copy must clear that badge because a bare genus cannot
    carry an unambiguous species-level Red List category. Prevents a
    stale species-level VU/LC from bleeding into a genus-only pick.
    """
    dialog = _build_dialog(monkeypatch, qapp, tmp_path=tmp_path)
    try:
        # Fake a prior species-level Artsorakel state: badge painted +
        # taxonomy text populated.
        dialog._set_red_list_category("LC", {"no": "LC"})
        with dialog._taxon_controller._suspended():
            dialog.genus_input.setText("Amanita")
            dialog.species_input.setText("muscaria")
            dialog.vernacular_input.setText("rød fluesopp")
        qapp.processEvents()
        assert dialog._red_list_category == "LC"

        # Now perform a genus-only Copy.
        prediction = {
            "probability": 0.42,
            "scientificName": "Inocybe",
            "name": "Inocybe",
            "vernacularName": "trevlesopper",
            "taxonId": "NBIC:12345",
            "id": "NBIC:12345",
        }
        _select_prediction(dialog, prediction, source="arts")
        dialog._on_ai_copy_to_taxonomy("arts")
        qapp.processEvents()

        assert dialog.genus_input.text() == "Inocybe"
        assert dialog.species_input.text() == ""
        assert dialog._red_list_category == ""
    finally:
        dialog._cleanup_dialog_threads()
        dialog.deleteLater()


def test_inat_copy_genus_only_row_also_succeeds(
    monkeypatch, qapp, tmp_path: Path,
) -> None:
    """iNaturalist genus-level suggestions travel the same code path.
    The iNat branch already sets Red List to empty on every copy, so we
    verify the same field-population semantics without the badge
    checks.
    """
    dialog = _build_dialog(monkeypatch, qapp, tmp_path=tmp_path)
    try:
        prediction = {
            "score": 0.35,
            "combined_score": 0.35,
            "taxon": {
                "id": 47504,
                "name": "Inocybe",
                "rank": "genus",
                "preferred_common_name": "fibrecaps",
            },
        }
        _select_prediction(dialog, prediction, source="inat")
        dialog._on_ai_copy_to_taxonomy("inat")
        qapp.processEvents()

        assert dialog.genus_input.text() == "Inocybe"
        assert dialog.species_input.text() == ""
        assert dialog.vernacular_input.text() == "fibrecaps"
        assert dialog._red_list_category == ""
        # Should NOT show the parse-failure message.
        inat_status = dialog.ai_status_labels.get("inat")
        assert inat_status is not None
        assert "Could not parse" not in inat_status.text()
        assert inat_status.text() == "Copied to taxonomy."
    finally:
        dialog._cleanup_dialog_threads()
        dialog.deleteLater()


def test_extract_genus_species_handles_genus_only_taxon(
    monkeypatch, qapp, tmp_path: Path,
) -> None:
    """Unit-level guard for the extraction helper: a taxon dict that
    carries only a genus in either structured or scientific-name form
    must produce ``(genus, None)`` instead of ``(None, None)``. This is
    the underlying primitive that unblocks the copy handler."""
    dialog = _build_dialog(monkeypatch, qapp, tmp_path=tmp_path)
    try:
        # Structured genus, no species.
        assert dialog._extract_genus_species_from_taxon(
            {"genus": "Inocybe"},
        ) == ("Inocybe", None)

        # scientific_name is a bare genus token.
        assert dialog._extract_genus_species_from_taxon(
            {"scientific_name": "Inocybe"},
        ) == ("Inocybe", None)

        # Full binomial still works.
        assert dialog._extract_genus_species_from_taxon(
            {"scientific_name": "Inocybe geophylla"},
        ) == ("Inocybe", "geophylla")
    finally:
        dialog._cleanup_dialog_threads()
        dialog.deleteLater()


# ---------------------------------------------------------------------------
# Bug B — manual vernacular typing populates Genus when unambiguous.
# ---------------------------------------------------------------------------


def test_manual_vernacular_typing_populates_genus_when_unique(
    monkeypatch, qapp, tmp_path: Path,
) -> None:
    """``alvetrevlesopp`` resolves to two ``Inocybe`` species (unique
    genus across matches). Typing it and tabbing out should populate
    Genus to ``Inocybe``. Species stays empty; Red List stays empty
    (no bound identity, no derived category)."""
    tax_db = tmp_path / "taxonomy_v2.sqlite3"
    _seed_vernacular_v2_db(tax_db)
    dialog = _build_dialog(
        monkeypatch, qapp, tmp_path=tmp_path, taxonomy_db_path=tax_db,
    )
    try:
        assert dialog.vernacular_db is not None
        dialog.vernacular_input.setText("alvetrevlesopp")
        qapp.processEvents()
        dialog.vernacular_input.editingFinished.emit()
        qapp.processEvents()

        assert dialog.genus_input.text() == "Inocybe"
        assert dialog.species_input.text() == ""
        assert dialog._red_list_category == ""
        # Vernacular text preserved verbatim.
        assert dialog.vernacular_input.text() == "alvetrevlesopp"
    finally:
        dialog._cleanup_dialog_threads()
        dialog.deleteLater()


def test_manual_vernacular_typing_does_not_overwrite_user_typed_genus(
    monkeypatch, qapp, tmp_path: Path,
) -> None:
    """A pre-existing Genus (typed by the user) must never be
    overwritten by the vernacular autofill. The observer is authoritative."""
    tax_db = tmp_path / "taxonomy_v2.sqlite3"
    _seed_vernacular_v2_db(tax_db)
    dialog = _build_dialog(
        monkeypatch, qapp, tmp_path=tmp_path, taxonomy_db_path=tax_db,
    )
    try:
        with dialog._taxon_controller._suspended():
            dialog.genus_input.setText("Amanita")
        dialog.vernacular_input.setText("alvetrevlesopp")
        qapp.processEvents()
        dialog.vernacular_input.editingFinished.emit()
        qapp.processEvents()

        assert dialog.genus_input.text() == "Amanita"
    finally:
        dialog._cleanup_dialog_threads()
        dialog.deleteLater()


def test_manual_vernacular_typing_ambiguous_leaves_genus_empty(
    monkeypatch, qapp, tmp_path: Path,
) -> None:
    """``trevlesopper`` resolves to matches spanning multiple genera
    (Inocybe, Inosperma). Ambiguous → auto-populate MUST NOT fire; the
    observer disambiguates via the picker or manual genus entry."""
    tax_db = tmp_path / "taxonomy_v2.sqlite3"
    _seed_vernacular_v2_db(tax_db)
    dialog = _build_dialog(
        monkeypatch, qapp, tmp_path=tmp_path, taxonomy_db_path=tax_db,
    )
    try:
        dialog.vernacular_input.setText("trevlesopper")
        qapp.processEvents()
        dialog.vernacular_input.editingFinished.emit()
        qapp.processEvents()

        assert dialog.genus_input.text() == ""
        assert dialog.species_input.text() == ""
    finally:
        dialog._cleanup_dialog_threads()
        dialog.deleteLater()


def test_manual_vernacular_typing_unknown_leaves_genus_empty(
    monkeypatch, qapp, tmp_path: Path,
) -> None:
    """Vernacular not present in the DB → no autofill (zero matches
    behaves the same as ambiguous: leave everything untouched)."""
    tax_db = tmp_path / "taxonomy_v2.sqlite3"
    _seed_vernacular_v2_db(tax_db)
    dialog = _build_dialog(
        monkeypatch, qapp, tmp_path=tmp_path, taxonomy_db_path=tax_db,
    )
    try:
        dialog.vernacular_input.setText("hokuspokusnavn")
        qapp.processEvents()
        dialog.vernacular_input.editingFinished.emit()
        qapp.processEvents()

        assert dialog.genus_input.text() == ""
        assert dialog.species_input.text() == ""
    finally:
        dialog._cleanup_dialog_threads()
        dialog.deleteLater()


def test_manual_vernacular_typing_does_not_clear_prior_red_list_badge(
    monkeypatch, qapp, tmp_path: Path,
) -> None:
    """Stage 3B.5 vernacular-only invariant: typing a common name (and
    even having the genus autofill fire) MUST NOT clear an existing Red
    List badge. Only structured-identity edits (genus/species/scientific
    name) clear derived state."""
    tax_db = tmp_path / "taxonomy_v2.sqlite3"
    _seed_vernacular_v2_db(tax_db)
    dialog = _build_dialog(
        monkeypatch, qapp, tmp_path=tmp_path, taxonomy_db_path=tax_db,
    )
    try:
        # Prior state: a species-level VU badge (from an Artsorakel
        # source snapshot in a previous session).
        dialog._set_red_list_category("VU", {"no": "VU"})
        qapp.processEvents()
        assert dialog._red_list_category == "VU"

        dialog.vernacular_input.setText("alvetrevlesopp")
        qapp.processEvents()
        dialog.vernacular_input.editingFinished.emit()
        qapp.processEvents()

        # Genus autofill fired (unique genus) — the vernacular-only edit
        # invariant means the badge remains untouched.
        assert dialog.genus_input.text() == "Inocybe"
        assert dialog._red_list_category == "VU"
    finally:
        dialog._cleanup_dialog_threads()
        dialog.deleteLater()


def test_manual_vernacular_typing_skipped_when_snapshot_committed(
    monkeypatch, qapp, tmp_path: Path,
) -> None:
    """When the taxonomy controller already holds a committed snapshot
    (explicit picker or manual-resolver choice), a vernacular typing
    event MUST NOT overwrite the bound genus. This guards against
    a race where the user typed a vernacular after picking a taxon —
    identity wins."""
    tax_db = tmp_path / "taxonomy_v2.sqlite3"
    _seed_vernacular_v2_db(tax_db)
    dialog = _build_dialog(
        monkeypatch, qapp, tmp_path=tmp_path, taxonomy_db_path=tax_db,
    )
    try:
        with dialog._taxon_controller._suspended():
            dialog.genus_input.setText("Amanita")
            dialog.species_input.setText("muscaria")
        dialog._taxon_controller._committed_snapshot = {
            "genus": "Amanita",
            "species": "muscaria",
            "scientific_name": "Amanita muscaria",
            "taxon_rank_snapshot": "species",
            "sporely_taxon_id": 999999,
            "link_kind": "canonical",
            "canonical_scientific_name": "Amanita muscaria",
            "canonical_rank": "species",
        }
        # Clear the genus programmatically to force the autofill trigger
        # into a path where it *could* set text — proving the snapshot
        # guard is what blocks it.
        with dialog._taxon_controller._suspended():
            dialog.genus_input.setText("")
        dialog.vernacular_input.setText("alvetrevlesopp")
        qapp.processEvents()
        dialog.vernacular_input.editingFinished.emit()
        qapp.processEvents()

        # Snapshot present → autofill skipped. Genus remains empty as
        # left by the test.
        assert dialog.genus_input.text() == ""
    finally:
        dialog._cleanup_dialog_threads()
        dialog.deleteLater()


def test_manual_vernacular_unique_species_match_still_binds_via_controller(
    monkeypatch, qapp, tmp_path: Path,
) -> None:
    """Sanity check: a vernacular that maps to exactly one species-level
    match is handled by ``TaxonInputController.on_vernacular_editing_finished``
    (which fires first on the same signal). Our genus-autofill hook
    runs afterwards and must be a no-op in that case (genus is already
    set)."""
    tax_db = tmp_path / "taxonomy_v2.sqlite3"
    _seed_vernacular_v2_db(tax_db)
    dialog = _build_dialog(
        monkeypatch, qapp, tmp_path=tmp_path, taxonomy_db_path=tax_db,
    )
    try:
        dialog.vernacular_input.setText("knappesopp")
        qapp.processEvents()
        dialog.vernacular_input.editingFinished.emit()
        qapp.processEvents()

        # Controller filled both genus + species (single match).
        assert dialog.genus_input.text() == "Agaricus"
        assert dialog.species_input.text() == "bisporus"
    finally:
        dialog._cleanup_dialog_threads()
        dialog.deleteLater()
