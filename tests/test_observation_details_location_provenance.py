"""The Observation details dialog attributes the coordinate edits made in it.

Contract: docs/supabase-sync-contract.md, "Private ingestion and location
provenance" (desktop requirement 5 and the desktop implementation notes).
These check what ``get_data()`` returns; interactive behavior is gate G4.
"""
from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication

import ui.observations_tab as observations_tab

_PAIR = ("location_source", "location_status")


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


@pytest.fixture
def make_dialog(monkeypatch, qapp):
    fake_client = SimpleNamespace(
        user_id="user-123",
        fetch_cloud_plan_profile=lambda: {"cloud_plan": "free", "is_pro": False},
        count_remote_privacy_slots=lambda: 0,
        list_remote_observations=lambda: [],
    )
    dialog_cls = observations_tab.ObservationDetailsDialog
    monkeypatch.setattr(observations_tab.SettingsDB, "get_setting", lambda key, default=None: default)
    monkeypatch.setattr(dialog_cls, "_load_objectives", lambda self: {"default": {"is_default": True}})
    monkeypatch.setattr(dialog_cls, "_load_tag_options", lambda self, category: [f"{category}-default"])
    monkeypatch.setattr(dialog_cls, "_load_habitat_tree", lambda self, filename: [])
    monkeypatch.setattr(dialog_cls, "_apply_primary_metadata", lambda self: None)
    monkeypatch.setattr(dialog_cls, "_apply_suggested_taxon", lambda self: None)
    monkeypatch.setattr(dialog_cls, "_sync_taxon_cache", lambda self: None)
    monkeypatch.setattr(dialog_cls, "_complete_deferred_dialog_setup", lambda self: None)
    monkeypatch.setattr(dialog_cls, "_schedule_location_lookup", lambda self, *a, **k: None)
    monkeypatch.setattr(observations_tab.SporelyCloudClient, "from_stored_credentials", lambda: fake_client)
    dialogs = []

    def _make(observation=None, draft_data=None):
        dialog = dialog_cls(parent=None, observation=observation, draft_data=draft_data, image_results=[])
        qapp.processEvents()
        dialogs.append(dialog)
        return dialog

    yield _make
    for dialog in dialogs:
        dialog.deleteLater()
    qapp.processEvents()


def _existing():
    return {
        "id": 1, "date": "2026-09-20 10:00:00", "genus": "Agaricus", "species": "campestris",
        "gps_latitude": 63.4, "gps_longitude": 10.4,
        "location_source": "device_gps", "location_status": "acquired",
    }


def _pair(data):
    return tuple(data.get(field) for field in _PAIR) if _PAIR[0] in data else None


def test_unchanged_coordinates_keep_the_stored_pair(make_dialog):
    dialog = make_dialog(_existing())
    assert _pair(dialog.get_data()) == ("device_gps", "acquired")
    unattributed = dict(_existing(), location_source=None, location_status=None)
    assert _pair(make_dialog(unattributed).get_data()) is None


def test_typed_coordinates_are_manual(make_dialog):
    dialog = make_dialog(_existing())
    dialog.lat_input.setValue(60.0)
    assert _pair(dialog.get_data()) == ("manual", "acquired")


def test_coordinates_applied_from_photo_exif_are_image_exif(make_dialog):
    dialog = make_dialog(_existing())
    dialog.image_metadata = [{"latitude": 61.5, "longitude": 9.25, "filename": "a.jpg"}]
    dialog._apply_metadata_from_index(0)
    assert _pair(dialog.get_data()) == ("image_exif", "acquired")
    dialog.lat_input.setValue(61.0)
    assert _pair(dialog.get_data()) == ("manual", "acquired"), "a later typed edit is manual"


def test_clearing_existing_coordinates_is_none_skipped(make_dialog):
    dialog = make_dialog(_existing())
    dialog.lat_input.setValue(dialog.lat_input.minimum())
    dialog.lon_input.setValue(dialog.lon_input.minimum())
    data = dialog.get_data()
    assert (data["gps_latitude"], data["gps_longitude"]) == (None, None)
    assert _pair(data) == ("none", "skipped")


def test_new_observation_without_coordinates_stays_unattributed(make_dialog):
    dialog = make_dialog()
    assert _pair(dialog.get_data()) is None
    dialog.lat_input.setValue(60.0)
    dialog.lon_input.setValue(10.0)
    assert _pair(dialog.get_data()) == ("manual", "acquired")


# ── Edit Images detour (review round 2) ─────────────────────────────────────


def test_pending_pair_survives_only_while_its_coordinates_stay_selected():
    draft = {"gps_latitude": 60.0, "gps_longitude": 10.0, "location_source": "manual", "location_status": "acquired"}
    assert observations_tab._pending_location_pair(draft, 60.0, 10.0) == ("manual", "acquired")
    assert observations_tab._pending_location_pair(draft, 61.0, 10.0) is None, "Prepare Images replaced them"
    assert observations_tab._pending_location_pair({"gps_latitude": 60.0, "gps_longitude": 10.0}, 60.0, 10.0) is None
    cleared = {"gps_latitude": None, "gps_longitude": None, "location_source": "none", "location_status": "skipped"}
    assert observations_tab._pending_location_pair(cleared, None, None) == ("none", "skipped")


def test_existing_observation_reopened_after_edit_images_keeps_the_manual_attribution(make_dialog):
    first = make_dialog(_existing())
    first.lat_input.setValue(60.0)
    first.lon_input.setValue(10.0)
    data = first.get_data()
    assert _pair(data) == ("manual", "acquired")
    # ObservationsTab reopens the dialog on the draft merged over the record.
    reopened = make_dialog(dict(_existing(), **data))
    assert _pair(reopened.get_data()) == ("manual", "acquired")


def test_new_observation_draft_keeps_the_exif_attribution(make_dialog):
    first = make_dialog()
    first.image_metadata = [{"latitude": 61.5, "longitude": 9.25, "filename": "a.jpg"}]
    first._apply_metadata_from_index(0)
    data = first.get_data()
    assert _pair(data) == ("image_exif", "acquired")
    reopened = make_dialog(draft_data=dict(data))
    assert _pair(reopened.get_data()) == ("image_exif", "acquired")


def test_draft_whose_coordinates_were_replaced_is_not_reattributed(make_dialog):
    draft = {"date": "2026-09-20 10:00:00", "gps_latitude": 60.0, "gps_longitude": 10.0}
    # The new-observation flow drops the pair when Prepare Images moved the point.
    reopened = make_dialog(draft_data=draft)
    assert _pair(reopened.get_data()) is None
