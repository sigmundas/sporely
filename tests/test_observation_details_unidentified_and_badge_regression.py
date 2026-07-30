"""Regression tests for the ``ObservationDetailsDialog`` UX around the
"Unidentified" checkbox and around the manual (genus, species) →
Red List badge refresh flow.

Covers two user-reported bugs on the observation-detail dialog:

Bug 1 — UX: opening a fresh / no-identification observation must NOT
auto-check the "Unidentified" checkbox, because that state disables the
Name, Genus, Species and Determination controls. Before this fix the
observer had to discover the checkbox and uncheck it (or apply an
Artsorakel result, which unchecks it as a side effect) before typing.

Bug 2 — resolver: manual entry of a (genus, species) pair whose
canonical concept is duplicated in the taxonomy DB (e.g. NorTaxa-owned
row alongside a COL-owned duplicate) must still refresh the Red List
badge without requiring a Save + Reopen cycle. The strict
``taxon_id_from_scientific`` refuses to bind identity in that case; the
manual resolver's data-driven Red-List-presence tiebreak now picks the
assessed concept.

Both tests instantiate the real ``ObservationDetailsDialog`` so we cover
the actual signal wiring (``editingFinished``, ``_taxon_controller``
suspension, ``_schedule_final_redlist_resolution``) rather than isolated
handler stubs.
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


def _seed_manual_taxonomy_db(db_path: Path) -> None:
    """Seed a minimal taxonomy-v2 shape that mirrors the real
    ``Cantharellus cibarius`` duplication (two species-rank canonicals
    sharing the name, only one carrying a Red List assessment)."""
    with sqlite3.connect(str(db_path)) as conn:
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
                taxon_id INTEGER,
                vernacular_name TEXT,
                is_preferred_name INTEGER,
                language_code TEXT
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
        # Two variety rows (drop-out) + two species rows (double-canonical) +
        # a Red List assessment on the second species row only.
        conn.execute(
            "INSERT INTO taxon_min VALUES (150931, 'Cantharellus', 'cibarius', "
            "'Hydnaceae', 'Cantharellus cibarius var. monstrosus', 'variety', "
            "'col_xr', 'accepted')"
        )
        conn.execute(
            "INSERT INTO taxon_min VALUES (159987, 'Cantharellus', 'cibarius', "
            "'Hydnaceae', 'Cantharellus cibarius var. carneoalbus', 'variety', "
            "'col_xr', 'accepted')"
        )
        conn.execute(
            "INSERT INTO taxon_min VALUES (168873, 'Cantharellus', 'cibarius', "
            "'Hydnaceae', 'Cantharellus cibarius', 'species', 'col_xr', "
            "'accepted')"
        )
        conn.execute(
            "INSERT INTO taxon_min VALUES (626243, 'Cantharellus', 'cibarius', "
            "'Cantharellaceae', 'Cantharellus cibarius', 'species', 'nortaxa', "
            "'valid')"
        )
        for tid in (150931, 159987, 168873, 626243):
            conn.execute(
                "INSERT INTO scientific_name_min "
                "(taxon_id, language_code, scientific_name, is_preferred_name, source) "
                "VALUES (?, 'sci', "
                "(SELECT canonical_scientific_name FROM taxon_min WHERE taxon_id=?), "
                "1, (SELECT canonical_source_system FROM taxon_min WHERE taxon_id=?))",
                (tid, tid, tid),
            )
        conn.execute(
            "INSERT INTO taxon_redlist_min VALUES (626243, 'artsdatabanken', "
            "'2021', 'Norge', '626243-N', 'LC', 'LC', 0, NULL, NULL, NULL, "
            "'Cantharellus cibarius', NULL, 'species', 'artsdatabanken', "
            "'artsnavnebase', '626243-N')"
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
    image = Image.new("RGB", (2, 2), color=(200, 120, 60))
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


# ---------------------------------------------------------------------------
# Bug 1 — Unidentified auto-check UX regression.
# ---------------------------------------------------------------------------


def test_fresh_new_observation_leaves_identification_fields_active(
    monkeypatch, qapp, tmp_path: Path,
) -> None:
    """A brand-new dialog (no observation, no draft, no AI predictions)
    must let the user type immediately. Regression: before this fix,
    empty identification auto-checked the "Unidentified" checkbox which
    disabled the Name/Genus/Species inputs and the Determination combo
    until the user either unchecked the box themselves or applied an
    Artsorakel result (which unchecks it as a side effect)."""
    dialog = _build_dialog(monkeypatch, qapp, tmp_path=tmp_path)
    try:
        assert dialog.unidentified_checkbox.isChecked() is False
        assert dialog.vernacular_input.isEnabled() is True
        assert dialog.genus_input.isEnabled() is True
        assert dialog.species_input.isEnabled() is True
        assert dialog.scientific_name_input.isEnabled() is True
        assert dialog.determination_method_combo.isEnabled() is True
        # Read-only side is unrelated to the checkbox but sanity-check
        # anyway so a future regression that installs a read-only guard
        # trips the same test.
        assert dialog.genus_input.isReadOnly() is False
        assert dialog.species_input.isReadOnly() is False
        assert dialog.scientific_name_input.isReadOnly() is False
    finally:
        dialog._cleanup_dialog_threads()
        dialog.deleteLater()


def test_existing_observation_without_identification_leaves_fields_active(
    monkeypatch, qapp, tmp_path: Path,
) -> None:
    """Editing an existing observation whose identity fields were never
    populated (e.g. a placeholder row saved before any taxonomy work)
    must not auto-disable the identity widgets. The observer opens the
    dialog and starts typing without discovering the checkbox first."""
    dialog = _build_dialog(
        monkeypatch, qapp, tmp_path=tmp_path,
        observation={"id": 999, "genus": "", "species": "", "common_name": ""},
    )
    try:
        assert dialog.unidentified_checkbox.isChecked() is False
        assert dialog.genus_input.isEnabled() is True
        assert dialog.species_input.isEnabled() is True
        assert dialog.determination_method_combo.isEnabled() is True
    finally:
        dialog._cleanup_dialog_threads()
        dialog.deleteLater()


def test_explicit_unidentified_check_still_disables_fields(
    monkeypatch, qapp, tmp_path: Path,
) -> None:
    """The Unidentified checkbox retains its side effect on user input:
    when the observer explicitly checks it, the identification widgets
    disable and unchecking re-enables them. Proves the fix only removes
    the auto-check-on-load behaviour, not the checkbox's function."""
    dialog = _build_dialog(monkeypatch, qapp, tmp_path=tmp_path)
    try:
        assert dialog.genus_input.isEnabled() is True

        dialog.unidentified_checkbox.setChecked(True)
        qapp.processEvents()
        assert dialog.genus_input.isEnabled() is False
        assert dialog.species_input.isEnabled() is False
        assert dialog.determination_method_combo.isEnabled() is False
        assert dialog.vernacular_input.isEnabled() is False

        dialog.unidentified_checkbox.setChecked(False)
        qapp.processEvents()
        assert dialog.genus_input.isEnabled() is True
        assert dialog.species_input.isEnabled() is True
        assert dialog.determination_method_combo.isEnabled() is True
        assert dialog.vernacular_input.isEnabled() is True
    finally:
        dialog._cleanup_dialog_threads()
        dialog.deleteLater()


# ---------------------------------------------------------------------------
# Bug 2 — Cantharellus cibarius: manual re-type must refresh the badge.
# ---------------------------------------------------------------------------


def _apply_artsorakel_source_snapshot(
    dialog: observations_tab.ObservationDetailsDialog,
) -> None:
    """Mirror the state left by ``_apply_ai_selected`` without the
    Artsorakel network call: uncheck Unidentified, populate the source
    Red List category, and set the identity text widgets to the source
    taxon."""
    if dialog.is_unidentified():
        dialog.unidentified_checkbox.setChecked(False)
    dialog._set_red_list_category("VU", {"no": "VU"})
    with dialog._taxon_controller._suspended():
        dialog.genus_input.setText("Amanita")
        dialog.species_input.setText("muscaria")
        dialog.scientific_name_input.setText("Amanita muscaria")


def test_manual_cantharellus_cibarius_refreshes_red_list_badge(
    monkeypatch, qapp, tmp_path: Path,
) -> None:
    """Bug 2 reproduction: with a location resolved to Norway, apply an
    Artsorakel-like source snapshot, then clear the taxonomy text and
    manually type ``Cantharellus`` / ``cibarius``. On editingFinished
    the manual resolver must bind ``sporely_taxon_id = 626243`` (the
    NorTaxa-owned assessed row) and the deferred Red List apply must
    populate the LC badge — no Save + Reopen required.

    The taxonomy DB seeded here mirrors the exact real-DB duplication:
    two variety rows share ``(Cantharellus, cibarius)``, two species
    rows share the canonical name, and only the second species row
    carries the Norwegian LC assessment.
    """
    tax_db = tmp_path / "taxonomy_v2.sqlite3"
    _seed_manual_taxonomy_db(tax_db)
    dialog = _build_dialog(
        monkeypatch, qapp, tmp_path=tmp_path, taxonomy_db_path=tax_db,
    )
    try:
        # Location resolved to mainland Norway so ``determine_redlist_area``
        # returns "Norge" and the deferred apply proceeds.
        dialog._location_country_code = "no"

        # Simulate an Artsorakel apply that populated a prior badge and
        # taxonomy text.
        _apply_artsorakel_source_snapshot(dialog)

        # Now clear taxonomy text and type Cantharellus cibarius.
        # ``setText`` triggers ``textChanged`` which invalidates the
        # (non-existent) snapshot and clears the badge — that's the
        # invariant behaviour we do NOT want to weaken.
        dialog.genus_input.setText("")
        dialog.species_input.setText("")
        dialog.scientific_name_input.setText("")
        qapp.processEvents()

        dialog.genus_input.setText("Cantharellus")
        dialog.species_input.setText("cibarius")
        qapp.processEvents()

        # The observer tabs away from the species field → editingFinished.
        dialog.species_input.editingFinished.emit()
        qapp.processEvents()

        # Snapshot bound to the NorTaxa-owned assessed row.
        snap = dialog._taxon_controller.committed_snapshot()
        assert snap is not None
        assert snap["sporely_taxon_id"] == 626243
        assert snap["scientific_name"] == "Cantharellus cibarius"

        # Deferred badge apply must have run. It is a 0-ms QTimer;
        # ``processEvents`` on the app drains it.
        qapp.processEvents()
        qapp.processEvents()
        assert dialog._red_list_category == "LC"
    finally:
        dialog._cleanup_dialog_threads()
        dialog.deleteLater()


def test_manual_editing_finished_preserves_prior_picker_snapshot(
    monkeypatch, qapp, tmp_path: Path,
) -> None:
    """Regression guard for the earlier Stage 3B.5 invariant that the
    manual-resolver path must NEVER overwrite a snapshot that was
    already committed via the picker (e.g. a ``synonym_of_accepted``
    choice). Bind a snapshot programmatically, then fire
    editingFinished on the species widget without changing text — the
    identity must survive unchanged."""
    tax_db = tmp_path / "taxonomy_v2.sqlite3"
    _seed_manual_taxonomy_db(tax_db)
    dialog = _build_dialog(
        monkeypatch, qapp, tmp_path=tmp_path, taxonomy_db_path=tax_db,
    )
    try:
        with dialog._taxon_controller._suspended():
            dialog.genus_input.setText("Cantharellus")
            dialog.species_input.setText("cibarius")
            dialog.scientific_name_input.setText("Cantharellus cibarius")
        # Simulate an explicit picker choice: 168873 (col_xr) — even
        # though the tiebreak would prefer 626243, the prior explicit
        # snapshot must win.
        dialog._taxon_controller._committed_snapshot = {
            "genus": "Cantharellus",
            "species": "cibarius",
            "scientific_name": "Cantharellus cibarius",
            "taxon_rank_snapshot": "species",
            "sporely_taxon_id": 168873,
            "link_kind": "canonical",
            "canonical_scientific_name": "Cantharellus cibarius",
            "canonical_rank": "species",
        }
        # Fire editingFinished without altering the text.
        dialog.species_input.editingFinished.emit()
        qapp.processEvents()
        snap = dialog._taxon_controller.committed_snapshot()
        assert snap is not None
        # Prior explicit choice preserved — the manual resolver's
        # snapshot-present guard skipped the overwrite.
        assert snap["sporely_taxon_id"] == 168873
    finally:
        dialog._cleanup_dialog_threads()
        dialog.deleteLater()
