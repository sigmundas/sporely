"""Historical Artsorakel predictions must never become current Red List data.

Found in the taxonomy-v2 closeout observation-917 integrity round-trip:
observation 917 holds the committed identity Sporely 83668 (Conocybe rugosa),
which has no assessment in the release. Its ``ai_state_json`` still records a
per-image Artsorakel pick of a DIFFERENT taxon — Pholiotina vexans,
NBIC:58766, Red List LC. Opening the dialog borrowed that LC because no
category was stored, and an unrelated save then persisted it onto 917, from
where sync carried it to the cloud and to another device.

Invariant: the Red List restored on load comes only from the persisted
columns. Provider history is kept, never promoted. These tests drive the real
``ObservationDetailsDialog`` so the load → edit → ``get_data`` chain is the
production one.
"""
from __future__ import annotations

import os
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


# The per-image Artsorakel pick recorded in observation 917's ai_state_json.
_VEXANS_PREDICTION = {
    "name": "Pholiotina vexans",
    "scientificName": "Pholiotina vexans",
    "scientific_name": "Pholiotina vexans",
    "scientific_name_id": "NBIC:58766",
    "probability": 0.397645,
    "redListCategory": "LC",
    "redListCategories": {"NO": "LC"},
    "vernacularName": "vrang ringerlehatt",
}


def _observation_917(**overrides) -> dict:
    """Observation 917 after the picker commit: proven 83668, no Red List."""
    row = {
        "id": 917,
        "date": "2026-09-15",
        "genus": "Conocybe",
        "species": "rugosa",
        "common_name": "slank ringkjeglesopp",
        "species_guess": "Conocybe rugosa",
        "scientific_name_snapshot": "Conocybe rugosa",
        "taxon_rank_snapshot": "species",
        "sporely_taxon_id": 83668,
        "taxon_identity_state": "sporely_v2",
        "taxon_identity_proof": "taxonomy_v2_artifact",
        "taxon_identity_source_system": "sporely",
        "taxon_identity_namespace": "sporely_taxon_id",
        "taxon_identity_external_id": "83668",
        "country_code": "NO",
        "gps_latitude": 63.416128,
        "gps_longitude": 10.404458,
        "red_list_category": None,
        "red_list_categories_json": None,
        "ai_selected_service": None,
        "ai_selected_scientific_name": None,
        "notes": "",
    }
    row.update(overrides)
    return row


def _patch_dialog_collaborators(monkeypatch) -> None:
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
    monkeypatch.setattr(observations_tab, "resolve_vernacular_db_path", lambda _lang: None)
    Dialog = observations_tab.ObservationDetailsDialog
    monkeypatch.setattr(Dialog, "_load_objectives", lambda self: {"default": {"is_default": True}})
    monkeypatch.setattr(Dialog, "_load_tag_options", lambda self, category: [f"{category}-default"])
    monkeypatch.setattr(Dialog, "_load_habitat_tree", lambda self, filename: [])
    monkeypatch.setattr(Dialog, "_apply_primary_metadata", lambda self: None)
    monkeypatch.setattr(Dialog, "_apply_suggested_taxon", lambda self: None)
    monkeypatch.setattr(Dialog, "_sync_taxon_cache", lambda self: None)
    # Offline: the deferred setup would start a geocode worker.
    monkeypatch.setattr(Dialog, "_complete_deferred_dialog_setup", lambda self: None)
    monkeypatch.setattr(
        observations_tab.SporelyCloudClient, "from_stored_credentials", lambda: fake_client,
    )


def _open_dialog(monkeypatch, qapp, tmp_path: Path, observation: dict, ai_state: dict | None):
    _patch_dialog_collaborators(monkeypatch)
    image_path = tmp_path / "img.jpg"
    Image.new("RGB", (2, 2), color=(200, 120, 60)).save(image_path, quality=90)
    dialog = observations_tab.ObservationDetailsDialog(
        parent=None,
        observation=observation,
        image_results=[ImageImportResult(filepath=str(image_path), image_type="field")],
        ai_state=ai_state,
    )
    qapp.processEvents()
    return dialog


def _close(dialog) -> None:
    dialog._cleanup_dialog_threads()
    dialog.deleteLater()


def test_historical_prediction_red_list_is_not_borrowed_on_open(monkeypatch, qapp, tmp_path):
    dialog = _open_dialog(
        monkeypatch, qapp, tmp_path, _observation_917(),
        ai_state={"selected": {"3": dict(_VEXANS_PREDICTION)}},
    )
    try:
        assert dialog._red_list_category == ""
        assert dialog._red_list_categories is None
        # The provider history itself is kept, not destroyed.
        assert dialog._ai_selected_by_index[3]["scientific_name_id"] == "NBIC:58766"
    finally:
        _close(dialog)


def test_unrelated_save_does_not_persist_another_taxons_red_list(monkeypatch, qapp, tmp_path):
    """The exact 917 sequence: open, make an unrelated edit, save."""
    dialog = _open_dialog(
        monkeypatch, qapp, tmp_path, _observation_917(),
        ai_state={"selected": {"3": dict(_VEXANS_PREDICTION)}},
    )
    try:
        dialog.open_comment_input.setPlainText("unrelated edit")
        qapp.processEvents()
        data = dialog.get_data()
        assert data["red_list_category"] is None
        assert data["red_list_categories_json"] is None
        # The committed identity is untouched by the save.
        assert data["sporely_taxon_id"] == 83668
        assert data["taxon_identity_proof"] == "taxonomy_v2_artifact"
    finally:
        _close(dialog)


def test_stored_red_list_still_round_trips(monkeypatch, qapp, tmp_path):
    """Removing the fallback must not drop a category that IS persisted."""
    dialog = _open_dialog(
        monkeypatch, qapp, tmp_path,
        _observation_917(red_list_category="NT", red_list_categories_json='{"NO": "NT"}'),
        ai_state={"selected": {"3": dict(_VEXANS_PREDICTION)}},
    )
    try:
        assert dialog._red_list_category == "NT"
        data = dialog.get_data()
        assert data["red_list_category"] == "NT"
        assert data["red_list_categories_json"] == '{"NO": "NT"}'
    finally:
        _close(dialog)
