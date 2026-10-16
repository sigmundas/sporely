"""The Edit Observation dialog leaves an image's objective alone unless the
user changes it (meta-g8-edit-sync-371, audit 4544: an unrelated comment save
wrote objective_name '' over the web's 'web 40x').

The real ``ObservationsTab._apply_import_results_to_observation`` runs against
a real SQLite database; only widget helpers and the objective list are stubbed.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PIL import Image
from PySide6.QtWidgets import QApplication

from database.models import ImageDB, ObservationDB
from ui import observations_tab
from ui.image_import_dialog import ImageImportResult
from ui.observations_tab import ObservationsTab
from tests.test_cloud_inbox_import_identity import _TabStub
from tests.test_sync_key_observation_identity import two_desktops  # noqa: F401  (fixture)

OBJECTIVES = {"40x": {"name": "40x", "microns_per_pixel": 0.1}, "100x": {"name": "100x", "microns_per_pixel": 0.05}}


@pytest.fixture(autouse=True)
def _app(monkeypatch):
    monkeypatch.setattr(observations_tab, "load_objectives", lambda: dict(OBJECTIVES))
    return QApplication.instance() or QApplication([])


def _image(tmp_path, obs_id, *, image_type, objective):
    path = tmp_path / f"{image_type}-{objective}.jpg"
    Image.new("RGB", (64, 48), (90, 60, 30)).save(path, format="JPEG")
    image_id = ImageDB.add_image(observation_id=obs_id, filepath=str(path), image_type=image_type,
                                 objective_name=objective, copy_to_folder=False)
    return image_id, str(path)


def _save(obs_id, image_id, path, **result_fields):
    existing = ImageDB.get_images_for_observation(obs_id)
    result = ImageImportResult(filepath=path, preview_path=path, original_filepath=path,
                               image_id=image_id, **result_fields)
    ObservationsTab._apply_import_results_to_observation(_TabStub(), obs_id, [result], existing_images=existing)
    return ImageDB.get_image(image_id)["objective_name"]


def test_unrelated_save_keeps_a_field_image_objective(two_desktops, tmp_path):
    obs_id = ObservationDB.create_observation(date="2026-10-16", genus="Boletus", species="edulis")
    image_id, path = _image(tmp_path, obs_id, image_type="field", objective="web 40x")
    assert _save(obs_id, image_id, path, image_type="field") == "web 40x"


def test_changing_a_microscope_image_to_field_still_clears_its_objective(two_desktops, tmp_path):
    obs_id = ObservationDB.create_observation(date="2026-10-16", genus="Boletus", species="edulis")
    image_id, path = _image(tmp_path, obs_id, image_type="microscope", objective="40x")
    assert _save(obs_id, image_id, path, image_type="field") in ("", None)


def test_a_real_objective_edit_is_saved(two_desktops, tmp_path):
    obs_id = ObservationDB.create_observation(date="2026-10-16", genus="Boletus", species="edulis")
    image_id, path = _image(tmp_path, obs_id, image_type="microscope", objective="40x")
    assert _save(obs_id, image_id, path, image_type="microscope", objective="100x") == "100x"
