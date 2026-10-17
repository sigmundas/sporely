"""Stage 5: the cloud-inbox import keeps each cloud row's identity.

An observation imported from the cloud inbox is a copy of existing cloud rows:
the local rows store the cloud rows' ``sync_key`` (NULL stays NULL) and link by
the cloud row each image came from, never by list position. The real
``ObservationsTab`` methods run against a real SQLite database; only the
widget helpers unrelated to identity are stubbed.
"""
from __future__ import annotations

import os
import sqlite3
import uuid

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PIL import Image
from PySide6.QtWidgets import QApplication

from database import schema
from database.models import ObservationDB
from ui.image_import_dialog import ImageImportResult
from ui.observations_tab import ObservationsTab
from tests.test_sync_key_observation_identity import two_desktops  # noqa: F401  (fixture)

KEY_OBS = str(uuid.UUID(int=100))
KEY_1 = str(uuid.UUID(int=1))
KEY_3 = str(uuid.UUID(int=3))


@pytest.fixture(autouse=True)
def _app():
    return QApplication.instance() or QApplication([])


class _TabStub:
    """The non-identity helpers ``_apply_import_results_to_observation`` calls."""

    def __init__(self):
        self.status: list[str] = []
        self.refreshed = 0

    def __getattr__(self, name):
        return lambda *args, **kwargs: None

    def _compute_resample_scale_factor(self, *args, **kwargs):
        return 1.0

    def _get_image_size(self, *args, **kwargs):
        return 64, 48

    def _publish_excluded_image_ids(self, *args, **kwargs):
        return set()

    def tr(self, text):
        return text

    def refresh_observations(self):
        self.refreshed += 1

    def set_status_message(self, message, **kwargs):
        self.status.append(message)


def _photo(tmp_path, name: str) -> str:
    path = tmp_path / name
    Image.new("RGB", (64, 48), (90, 60, 30)).save(path, format="JPEG")
    return str(path)


def _result(tmp_path, name: str, cloud_image_id: str, cloud_sync_key: str | None) -> ImageImportResult:
    path = _photo(tmp_path, name)
    return ImageImportResult(
        filepath=path, preview_path=path, image_type="field", original_filepath=path,
        cloud_image_id=cloud_image_id, cloud_sync_key=cloud_sync_key,
    )


def _images(obs_id: int) -> list[dict]:
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.row_factory = sqlite3.Row
        return [dict(r) for r in connection.execute(
            "SELECT id, cloud_id, sync_key FROM images WHERE observation_id = ? ORDER BY id", (obs_id,),
        )]


def test_reordered_and_omitted_inbox_images_link_by_their_own_cloud_rows(two_desktops, tmp_path):
    # The cloud observation has c1, c2 (keyless) and c3; the dialog reordered
    # them to c3, c1 and dropped c2.
    results = [
        _result(tmp_path, "three.jpg", "c3", KEY_3),
        _result(tmp_path, "one.jpg", "c1", KEY_1),
    ]
    obs_id = ObservationDB.create_observation(date="2026-10-01", genus="Amanita", sync_key=KEY_OBS)

    ObservationsTab._apply_import_results_to_observation(_TabStub(), obs_id, results)
    ObservationsTab._mark_cloud_observation_imported(
        None, {"cloud_id": "cobs", "raw": {"id": "cobs"}}, obs_id, results,
    )

    by_cloud = {row["cloud_id"]: row for row in _images(obs_id)}
    assert set(by_cloud) == {"c1", "c3"}, "an omitted image links nothing; no positional link"
    assert by_cloud["c3"]["id"] == results[0].image_id and by_cloud["c3"]["sync_key"] == KEY_3
    assert by_cloud["c1"]["id"] == results[1].image_id and by_cloud["c1"]["sync_key"] == KEY_1
    observation = ObservationDB.get_observation(obs_id)
    assert observation["cloud_id"] == "cobs" and observation["sync_key"] == KEY_OBS


def test_a_keyless_inbox_image_stays_keyless(two_desktops, tmp_path):
    results = [_result(tmp_path, "two.jpg", "c2", None)]
    obs_id = ObservationDB.create_observation(date="2026-10-01", genus="Amanita", sync_key=None)

    ObservationsTab._apply_import_results_to_observation(_TabStub(), obs_id, results)
    ObservationsTab._mark_cloud_observation_imported(None, {"cloud_id": "cobs"}, obs_id, results)

    [image] = _images(obs_id)
    assert image["cloud_id"] == "c2" and image["sync_key"] is None
    assert ObservationDB.get_observation(obs_id)["sync_key"] is None


def test_an_ordinary_import_still_mints_a_key(two_desktops, tmp_path):
    path = _photo(tmp_path, "local.jpg")
    results = [ImageImportResult(filepath=path, preview_path=path, original_filepath=path)]
    obs_id = ObservationDB.create_observation(date="2026-10-01", genus="Amanita")
    ObservationsTab._apply_import_results_to_observation(_TabStub(), obs_id, results)
    [image] = _images(obs_id)
    assert image["cloud_id"] is None and uuid.UUID(image["sync_key"])


def test_inbox_import_results_carry_each_cloud_rows_identity(tmp_path):
    rows = [
        {"id": "c1", "sync_key": KEY_1.upper(), "image_type": "field", "_local_path": _photo(tmp_path, "a.jpg")},
        {"id": "c2", "sync_key": None, "image_type": "field", "_local_path": _photo(tmp_path, "b.jpg")},
    ]
    stub = _TabStub()
    stub._download_cloud_observation_images = lambda row_data, download_all=True: rows
    results = ObservationsTab._cloud_image_import_results(stub, {"raw": {"date": "2026-10-01"}})
    assert [(r.cloud_image_id, r.cloud_sync_key) for r in results] == [("c1", KEY_1), ("c2", None)]


def test_a_stale_inbox_row_already_on_this_desktop_is_never_imported_twice(two_desktops):
    ObservationDB.create_observation(date="2026-10-01", genus="Amanita", sync_key=KEY_OBS)
    stub = _TabStub()
    opened = []
    stub._cloud_image_import_results = lambda row_data: opened.append(row_data) or []

    ObservationsTab._open_cloud_observation_editor(
        stub, {"cloud_id": "cobs", "raw": {"id": "cobs", "sync_key": KEY_OBS}},
    )

    assert opened == [], "the editor never opens for a held row"
    assert stub.refreshed == 1 and stub.status == ["Sporely Cloud already up to date."]
    assert len(ObservationDB.get_all_observations()) == 1
