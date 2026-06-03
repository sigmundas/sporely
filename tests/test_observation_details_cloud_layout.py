from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication, QVBoxLayout

import ui.observations_tab as observations_tab


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def test_edit_observation_cloud_controls_share_one_inline_row(monkeypatch, qapp) -> None:
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

    dialog = observations_tab.ObservationDetailsDialog(parent=None, observation=None, draft_data=None, image_results=[])
    qapp.processEvents()

    cloud_controls = dialog._cloud_controls
    assert cloud_controls.objectName() == "observationCloudControls"
    assert dialog.is_draft_checkbox.parent() is cloud_controls

    cloud_layout = cloud_controls.layout()
    assert cloud_layout is not None
    assert cloud_layout.count() >= 2

    cloud_row = cloud_layout.itemAt(1).layout()
    assert cloud_row is not None

    share_field = cloud_row.itemAt(0).widget()
    precision_field = cloud_row.itemAt(1).widget()
    draft_checkbox = cloud_row.itemAt(2).widget()

    assert share_field is not None
    assert precision_field is not None
    assert draft_checkbox is dialog.is_draft_checkbox

    assert isinstance(share_field.layout(), QVBoxLayout)
    assert isinstance(precision_field.layout(), QVBoxLayout)
    assert share_field.layout().itemAt(0).widget().text() == "Share with.."
    assert share_field.layout().itemAt(1).widget() is dialog.sharing_scope_selector
    assert precision_field.layout().itemAt(0).widget().text() == "Location precision:"
    assert precision_field.layout().itemAt(1).widget() is dialog.location_precision_selector
    assert dialog.is_draft_checkbox.text() == "Draft / WIP"

    dialog.deleteLater()
