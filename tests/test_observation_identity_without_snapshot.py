"""A proven identity stored without name/rank snapshots survives a save.

Found before the tax-2026.10.23-11 desktop release: the 2026-10-07 stage 4
identity repair bound 148 local observations (e.g. 497, Cantharellus cibarius,
NorTaxa 56210 -> 168873) without ``scientific_name_snapshot`` or
``taxon_rank_snapshot``. The editor restored an identity only when both
snapshots were present, so opening such an observation and saving it with no
change wrote every identity column as NULL.

Invariant: the stored proven identity is the committed identity on load, as
stored, without a name derived from text. Only an explicit identification
edit changes it. These tests drive the real ``ObservationDetailsDialog``.
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
from utils.taxon_identity import IDENTITY_COLUMNS, STATE_NONE, TaxonIdentity

_IDENTITY_KEYS = ("sporely_taxon_id", *IDENTITY_COLUMNS)

# Observation 497's stored identity, as the stage 4 repair wrote it.
_REPAIRED_497 = {
    "sporely_taxon_id": 168873,
    "taxon_identity_state": "sporely_v2",
    "taxon_identity_proof": "external_id_resolution",
    "taxon_identity_source_system": "nortaxa",
    "taxon_identity_namespace": "nortaxa_taxon_id",
    "taxon_identity_external_id": "56210",
    "taxon_identity_raw_external_id": "NBIC:56210",
    "taxon_identity_provenance": "sporely_taxonomy_v2_stage4_repair:tax-2026.10.07-01",
}


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def _observation(**overrides) -> dict:
    row = {
        "id": 497,
        "date": "2026-07-06 13:31:08",
        "genus": "Cantharellus",
        "species": "cibarius",
        "common_name": "kantarell",
        "species_guess": "Cantharellus cibarius",
        "scientific_name_snapshot": None,
        "taxon_rank_snapshot": None,
        **_REPAIRED_497,
        "country_code": "NO",
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
    monkeypatch.setattr(Dialog, "_complete_deferred_dialog_setup", lambda self: None)
    monkeypatch.setattr(
        observations_tab.SporelyCloudClient, "from_stored_credentials", lambda: fake_client,
    )


def _open_dialog(monkeypatch, qapp, tmp_path: Path, observation: dict):
    _patch_dialog_collaborators(monkeypatch)
    image_path = tmp_path / "img.jpg"
    Image.new("RGB", (2, 2), color=(200, 120, 60)).save(image_path, quality=90)
    dialog = observations_tab.ObservationDetailsDialog(
        parent=None,
        observation=observation,
        image_results=[ImageImportResult(filepath=str(image_path), image_type="field")],
    )
    qapp.processEvents()
    return dialog


def _close(dialog) -> None:
    dialog._cleanup_dialog_threads()
    dialog.deleteLater()


def _identity(data: dict) -> dict:
    return {key: data.get(key) for key in _IDENTITY_KEYS}


def test_unchanged_save_keeps_a_snapshotless_proven_identity(monkeypatch, qapp, tmp_path):
    dialog = _open_dialog(monkeypatch, qapp, tmp_path, _observation())
    try:
        data = dialog.get_data()
        assert _identity(data) == _REPAIRED_497
        # Nothing is derived: the snapshots stay empty (the DB layer stores
        # an empty name snapshot as NULL).
        assert not data["scientific_name_snapshot"]
        assert data["taxon_rank_snapshot"] is None
        assert (data["genus"], data["species"]) == ("Cantharellus", "cibarius")
    finally:
        _close(dialog)


def test_unrelated_edit_keeps_a_snapshotless_proven_identity(monkeypatch, qapp, tmp_path):
    dialog = _open_dialog(monkeypatch, qapp, tmp_path, _observation())
    try:
        dialog.open_comment_input.setPlainText("unrelated edit")
        qapp.processEvents()
        assert _identity(dialog.get_data()) == _REPAIRED_497
    finally:
        _close(dialog)


@pytest.mark.parametrize("species", ["lutescens", ""])
def test_changing_or_clearing_the_species_drops_the_identity(monkeypatch, qapp, tmp_path, species):
    dialog = _open_dialog(monkeypatch, qapp, tmp_path, _observation())
    try:
        dialog.species_input.setText(species)
        qapp.processEvents()
        data = dialog.get_data()
        # An explicit identification edit invalidates the committed identity;
        # no new identity is inferred from the typed name.
        identity = TaxonIdentity.from_row(data)
        assert identity.state == STATE_NONE
        assert data["sporely_taxon_id"] is None
        assert not identity.has_external_evidence
        assert (data["species"] or "") == species
    finally:
        _close(dialog)


def test_identity_with_snapshots_still_round_trips(monkeypatch, qapp, tmp_path):
    """Observation 1032's shape: proven identity with both snapshots."""
    stored = {**_REPAIRED_497, "sporely_taxon_id": 624659, "taxon_identity_external_id": "52330",
              "taxon_identity_raw_external_id": "NBIC:52330",
              "taxon_identity_provenance": "taxonomy_v2:tax-2026.10.07-01:taxon_min.canonical_source_usage"}
    dialog = _open_dialog(monkeypatch, qapp, tmp_path, _observation(
        id=1032, genus="Panaeolina", species="foenisecii",
        scientific_name_snapshot="Panaeolina foenisecii", taxon_rank_snapshot="species", **stored,
    ))
    try:
        data = dialog.get_data()
        assert _identity(data) == stored
        assert data["scientific_name_snapshot"] == "Panaeolina foenisecii"
        assert data["taxon_rank_snapshot"] == "species"
    finally:
        _close(dialog)


def test_unresolved_external_identity_still_round_trips(monkeypatch, qapp, tmp_path):
    stored = {
        "sporely_taxon_id": None,
        "taxon_identity_state": "external_unresolved",
        "taxon_identity_proof": None,
        "taxon_identity_source_system": "nortaxa",
        "taxon_identity_namespace": "nortaxa_taxon_id",
        "taxon_identity_external_id": "53966",
        "taxon_identity_raw_external_id": "NBIC:53966",
        "taxon_identity_provenance": None,
    }
    dialog = _open_dialog(monkeypatch, qapp, tmp_path, _observation(
        genus="Hygrocybe", species="conica coll.",
        scientific_name_snapshot="Hygrocybe conica coll.", taxon_rank_snapshot="species", **stored,
    ))
    try:
        assert _identity(dialog.get_data()) == stored
    finally:
        _close(dialog)


# An unchanged save must not turn a stored NULL host into '' (or '' into
# NULL): sync compares the values literally, so either would be a cloud change.
@pytest.mark.parametrize("stored", [None, ""])
def test_unchanged_save_keeps_the_stored_empty_host(monkeypatch, qapp, tmp_path, stored):
    dialog = _open_dialog(monkeypatch, qapp, tmp_path, _observation(
        habitat_host_genus=stored, habitat_host_species=stored,
    ))
    try:
        data = dialog.get_data()
        assert data["habitat_host_genus"] is stored or data["habitat_host_genus"] == stored
        assert data["habitat_host_species"] is stored or data["habitat_host_species"] == stored
        assert type(data["habitat_host_genus"]) is type(stored)
    finally:
        _close(dialog)


def test_typed_host_is_saved(monkeypatch, qapp, tmp_path):
    dialog = _open_dialog(monkeypatch, qapp, tmp_path, _observation(
        habitat_host_genus=None, habitat_host_species=None,
    ))
    try:
        dialog.host_genus_input.setText("Betula")
        qapp.processEvents()
        assert dialog.get_data()["habitat_host_genus"] == "Betula"
    finally:
        _close(dialog)
