"""F04c: calibration reference upload reads only files inside the library.

Calibration metadata (``image_filepath`` and ``measurements_json.images[].path``)
can arrive from a cloud pull or a bundle import. A push must never open or
upload a file it names outside the managed images folder or the configured
originals folder. These tests run the real resolver and selector; the
recorders below only observe calls and pass them through.
"""

import json
import os
import uuid
from pathlib import Path

import pytest
from PIL import Image

from database import models
from utils import cloud_sync


def _write_image(path: Path, size=(640, 480), color=(90, 120, 60)) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", size, color).save(path)
    return path


@pytest.fixture
def library(monkeypatch, tmp_path):
    images_dir = tmp_path / "profile" / "images"
    images_dir.mkdir(parents=True)
    originals_dir = tmp_path / "originals-elsewhere"
    originals_dir.mkdir()
    outside_dir = tmp_path / "outside"
    outside_dir.mkdir()

    monkeypatch.setattr(cloud_sync, "get_images_dir", lambda: images_dir)

    original_get_setting = models.SettingsDB.get_setting

    def fake_get_setting(key, default=None):
        if key == "originals_dir":
            return str(originals_dir)
        return original_get_setting(key, default)

    monkeypatch.setattr(models.SettingsDB, "get_setting", staticmethod(fake_get_setting))

    probed: list[str] = []
    real_probe = cloud_sync._is_readable_local_file

    def recording_probe(path):
        probed.append(os.path.realpath(str(path)))
        return real_probe(path)

    monkeypatch.setattr(cloud_sync, "_is_readable_local_file", recording_probe)

    opened: list[str] = []
    real_open = Image.open

    def recording_open(fp, *args, **kwargs):
        if isinstance(fp, (str, os.PathLike)):
            opened.append(os.path.realpath(str(fp)))
        return real_open(fp, *args, **kwargs)

    monkeypatch.setattr(Image, "open", recording_open)

    return {
        "images_dir": images_dir,
        "originals_dir": originals_dir,
        "outside_dir": outside_dir,
        "probed": probed,
        "opened": opened,
    }


def _push(monkeypatch, calibration):
    client = cloud_sync.SporelyCloudClient("token", "user-123")
    uploads = []
    patches = []

    class RecordingWorker:
        def put_bytes(self, data, key, **kwargs):
            uploads.append((key, bytes(data)))
            return {"ok": True, "key": key}

    class RecordingR2:
        def put_bytes(self, data, key, **kwargs):
            uploads.append((key, bytes(data)))

    monkeypatch.setattr(client, "_get_media_worker", lambda: RecordingWorker())
    monkeypatch.setattr(client, "_get_r2", lambda: RecordingR2())
    monkeypatch.setattr(client, "fetch_cloud_plan_profile", lambda: {"cloud_plan": "free"})
    monkeypatch.setattr(client, "_patch", lambda path, payload: patches.append((path, dict(payload))))
    warning = client.push_calibration_reference_image(
        calibration,
        cloud_row_id="cloud-cal-1",
        remote_row={"id": "cloud-cal-1", "image_storage_path": None},
    )
    return warning, uploads, patches


def _calibration(**fields):
    base = {
        "calibration_uuid": str(uuid.uuid4()),
        "objective_key": "100X",
        "calibration_date": "2026-05-08 08:30:00",
        "microns_per_pixel": 0.0315,
    }
    base.update(fields)
    return base


def _assert_sentinel_untouched(library, sentinel: Path):
    real = os.path.realpath(str(sentinel))
    assert real not in library["probed"]
    assert real not in library["opened"]


def test_push_never_opens_or_uploads_out_of_library_calibration_image(monkeypatch, library):
    sentinel = _write_image(library["outside_dir"] / "sentinel.jpg")
    calibration = _calibration(
        image_filepath=str(sentinel),
        measurements_json=json.dumps({"images": [{"path": str(sentinel)}]}),
    )

    warning, uploads, patches = _push(monkeypatch, calibration)

    assert warning is not None
    assert "no readable local calibration image" in warning
    assert uploads == []
    assert patches == []
    _assert_sentinel_untouched(library, sentinel)


@pytest.mark.parametrize(
    "make_path",
    [
        # relative path that escapes the images folder
        lambda lib, sentinel: os.path.join("..", "..", "outside", sentinel.name),
        # absolute path that starts inside the library and walks out
        lambda lib, sentinel: str(lib["images_dir"] / ".." / ".." / "outside" / sentinel.name),
    ],
    ids=["relative-traversal", "absolute-dotdot"],
)
def test_selector_rejects_path_traversal_out_of_library(library, make_path):
    sentinel = _write_image(library["outside_dir"] / "sentinel.jpg")
    path_text = make_path(library, sentinel)
    calibration = _calibration(
        image_filepath=path_text,
        measurements_json={"images": [{"path": path_text}]},
    )

    assert cloud_sync._select_representative_calibration_image_path(calibration) is None
    _assert_sentinel_untouched(library, sentinel)


@pytest.mark.skipif(not hasattr(os, "symlink"), reason="symlinks unavailable")
def test_selector_rejects_symlink_inside_library_pointing_outside(library):
    sentinel = _write_image(library["outside_dir"] / "sentinel.jpg")
    link = library["images_dir"] / "calibrations" / "100X" / "link.jpg"
    link.parent.mkdir(parents=True)
    try:
        os.symlink(sentinel, link)
    except OSError:
        pytest.skip("cannot create symlink")
    calibration = _calibration(image_filepath=str(link))

    assert cloud_sync._select_representative_calibration_image_path(calibration) is None
    _assert_sentinel_untouched(library, sentinel)


def test_selector_skips_out_of_library_entry_and_uses_in_library_fallback(library):
    sentinel = _write_image(library["outside_dir"] / "sentinel.jpg")
    inside = _write_image(library["images_dir"] / "calibrations" / "100X" / "20260508_ab12cd34.jpg")
    calibration = _calibration(
        image_filepath=str(sentinel),
        measurements_json={"images": [{"path": str(sentinel)}, {"path": str(inside)}]},
    )

    selected = cloud_sync._select_representative_calibration_image_path(calibration)

    assert selected is not None
    assert os.path.realpath(str(selected)) == os.path.realpath(str(inside))
    _assert_sentinel_untouched(library, sentinel)


def test_in_library_calibration_image_is_still_selected_and_uploaded(monkeypatch, library):
    inside = _write_image(library["images_dir"] / "calibrations" / "100X" / "20260508_ab12cd34.jpg")
    calibration = _calibration(image_filepath=str(inside))

    warning, uploads, patches = _push(monkeypatch, calibration)

    assert warning is None
    assert len(uploads) == 1
    assert len(patches) == 1
    assert os.path.realpath(str(inside)) in library["opened"]


@pytest.mark.parametrize(
    "path_text",
    [
        "calibrations/100X/20260508_ab12cd34.jpg",
        "images/calibrations/100X/20260508_ab12cd34.jpg",
    ],
)
def test_relative_in_library_path_is_selected(library, path_text):
    inside = _write_image(library["images_dir"] / "calibrations" / "100X" / "20260508_ab12cd34.jpg")
    calibration = _calibration(image_filepath=path_text)

    selected = cloud_sync._select_representative_calibration_image_path(calibration)

    assert selected is not None
    assert os.path.realpath(str(selected)) == os.path.realpath(str(inside))


def test_configured_originals_folder_is_accepted(library):
    inside = _write_image(library["originals_dir"] / "observation_1" / "scale.jpg")
    calibration = _calibration(measurements_json={"images": [{"path": str(inside)}]})

    selected = cloud_sync._select_representative_calibration_image_path(calibration)

    assert selected is not None
    assert os.path.realpath(str(selected)) == os.path.realpath(str(inside))
