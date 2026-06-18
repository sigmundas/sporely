from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QGridLayout, QVBoxLayout

import ui.observations_tab as observations_tab


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_edit_observation_cloud_controls_share_one_inline_row(monkeypatch, qapp) -> None:
    fake_client = SimpleNamespace(
        user_id="user-123",
        fetch_cloud_plan_profile=lambda: {"cloud_plan": "free", "is_pro": False},
        count_remote_privacy_slots=lambda: 0,
        list_remote_observations=lambda: [],
    )
    monkeypatch.setattr(observations_tab.SettingsDB, "get_setting", lambda key, default=None: default)
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
    monkeypatch.setattr(observations_tab.ObservationDetailsDialog, "_apply_primary_metadata", lambda self: None)
    monkeypatch.setattr(observations_tab.ObservationDetailsDialog, "_apply_suggested_taxon", lambda self: None)
    monkeypatch.setattr(observations_tab.ObservationDetailsDialog, "_sync_taxon_cache", lambda self: None)
    monkeypatch.setattr(
        observations_tab.ObservationDetailsDialog,
        "_complete_deferred_dialog_setup",
        lambda self: None,
    )
    monkeypatch.setattr(observations_tab.SporelyCloudClient, "from_stored_credentials", lambda: fake_client)

    dialog = observations_tab.ObservationDetailsDialog(parent=None, observation=None, draft_data=None, image_results=[])
    qapp.processEvents()
    dialog._refresh_cloud_privacy_slots_summary(force=True)
    qapp.processEvents()

    cloud_controls = dialog._cloud_controls
    assert cloud_controls.objectName() == "observationCloudControls"

    cloud_layout = cloud_controls.layout()
    assert cloud_layout is not None
    assert cloud_layout.count() >= 2

    cloud_grid = cloud_layout.itemAt(1).layout()
    assert cloud_grid is not None
    assert isinstance(cloud_grid, QGridLayout)

    share_label = cloud_grid.itemAtPosition(0, 0).widget()
    precision_label = cloud_grid.itemAtPosition(0, 1).widget()
    share_selector = cloud_grid.itemAtPosition(1, 0).widget()
    precision_selector = cloud_grid.itemAtPosition(1, 1).widget()
    draft_container = cloud_grid.itemAtPosition(1, 2).widget()

    assert share_label is not None
    assert precision_label is not None
    assert share_selector is not None
    assert precision_selector is not None
    assert draft_container is not None

    assert share_label.text() == "Share with.."
    assert precision_label.text() == "Location precision:"
    assert share_selector is dialog.sharing_scope_selector
    assert precision_selector is dialog.location_precision_selector
    assert dialog.is_draft_checkbox.parent() is draft_container
    assert draft_container.layout().itemAt(0).widget() is dialog.is_draft_checkbox
    assert dialog.is_draft_checkbox.text() == "Draft / WIP"
    assert dialog.cloud_privacy_slots_label.text() == "Available private slots: 20 of 20"

    dialog.sharing_scope_selector.set_selected_value("public", emit=True)
    qapp.processEvents()
    assert dialog.cloud_privacy_slots_label.text() == "Available private slots: 20 of 20"

    dialog.location_precision_selector.set_selected_value("fuzzed", emit=True)
    qapp.processEvents()
    assert dialog.cloud_privacy_slots_label.text() == "Available private slots: 19 of 20"

    dialog.deleteLater()


def test_edit_observation_cloud_controls_hide_private_slot_summary_for_pro(monkeypatch, qapp) -> None:
    fake_client = SimpleNamespace(
        user_id="user-123",
        fetch_cloud_plan_profile=lambda: {"cloud_plan": "pro", "is_pro": True},
        count_remote_privacy_slots=lambda: 1,
        list_remote_observations=lambda: [
            {"visibility": "private", "location_precision": "fuzzed"},
        ],
    )
    monkeypatch.setattr(observations_tab.SettingsDB, "get_setting", lambda key, default=None: default)
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
    monkeypatch.setattr(observations_tab.ObservationDetailsDialog, "_apply_primary_metadata", lambda self: None)
    monkeypatch.setattr(observations_tab.ObservationDetailsDialog, "_apply_suggested_taxon", lambda self: None)
    monkeypatch.setattr(observations_tab.ObservationDetailsDialog, "_sync_taxon_cache", lambda self: None)
    monkeypatch.setattr(
        observations_tab.ObservationDetailsDialog,
        "_complete_deferred_dialog_setup",
        lambda self: None,
    )
    monkeypatch.setattr(observations_tab.SporelyCloudClient, "from_stored_credentials", lambda: fake_client)

    dialog = observations_tab.ObservationDetailsDialog(parent=None, observation=None, draft_data=None, image_results=[])
    qapp.processEvents()
    dialog._refresh_cloud_privacy_slots_summary(force=True)
    qapp.processEvents()

    assert dialog.cloud_privacy_slots_label.isVisible() is False
    assert dialog.cloud_privacy_slots_label.text() == ""

    dialog.deleteLater()


def test_edit_observation_cloud_controls_show_selected_ai_summary(monkeypatch, qapp) -> None:
    fake_client = SimpleNamespace(
        user_id="user-123",
        fetch_cloud_plan_profile=lambda: {"cloud_plan": "free", "is_pro": False},
        count_remote_privacy_slots=lambda: 0,
        list_remote_observations=lambda: [],
    )
    monkeypatch.setattr(observations_tab.SettingsDB, "get_setting", lambda key, default=None: default)
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
    monkeypatch.setattr(observations_tab.ObservationDetailsDialog, "_apply_primary_metadata", lambda self: None)
    monkeypatch.setattr(observations_tab.ObservationDetailsDialog, "_apply_suggested_taxon", lambda self: None)
    monkeypatch.setattr(observations_tab.ObservationDetailsDialog, "_sync_taxon_cache", lambda self: None)
    monkeypatch.setattr(
        observations_tab.ObservationDetailsDialog,
        "_complete_deferred_dialog_setup",
        lambda self: None,
    )
    monkeypatch.setattr(observations_tab.SporelyCloudClient, "from_stored_credentials", lambda: fake_client)

    dialog = observations_tab.ObservationDetailsDialog(
        parent=None,
        observation=None,
        draft_data={
            "ai_selected_service": "inat",
            "ai_selected_taxon_id": "12345",
            "ai_selected_scientific_name": "Entoloma clypeatum",
            "ai_selected_probability": 0.97,
            "ai_selected_at": "2026-05-01T12:34:56Z",
        },
        image_results=[],
    )
    qapp.processEvents()

    assert "Selected AI:" in dialog.ai_selected_summary_label.text()
    assert "Entoloma clypeatum" in dialog.ai_selected_summary_label.text()

    data = dialog.get_data()
    assert data["ai_selected_service"] == "inat"
    assert data["ai_selected_taxon_id"] == "12345"
    assert data["ai_selected_scientific_name"] == "Entoloma clypeatum"
    assert data["ai_selected_probability"] == 0.97
    assert data["ai_selected_at"] == "2026-05-01T12:34:56Z"

    dialog.deleteLater()
