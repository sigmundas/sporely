"""F14: preview and calibration temp folders are owner-only, per process."""
from __future__ import annotations

import os
import stat
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest
from PySide6.QtGui import QColor, QImage

from utils import private_temp

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="POSIX modes")


@pytest.fixture
def shared_tmp(tmp_path, monkeypatch):
    """A permissive, shared-looking temp dir and a permissive umask."""
    shared = tmp_path / "shared-tmp"
    shared.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(shared))
    monkeypatch.setattr(private_temp, "_ROOT", None)
    old = os.umask(0o000)
    yield shared
    os.umask(old)
    private_temp._remove_root()


def _mode(path) -> int:
    return stat.S_IMODE(os.stat(path).st_mode)


def _big_image(path: Path) -> str:
    image = QImage(600, 400, QImage.Format.Format_RGB32)
    image.fill(QColor("blue"))
    assert image.save(str(path), "PNG")
    return str(path)


def test_dialog_preview_dir_is_owner_only(qapp, shared_tmp, tmp_path):
    from ui.observation_details_dialog import ObservationDetailsDialog

    stub = SimpleNamespace(_dialog_preview_path_cache={}, _dialog_temp_preview_paths=set())
    preview = ObservationDetailsDialog._build_dialog_preview_path(stub, _big_image(tmp_path / "a.png"))
    assert preview
    folder = Path(preview).parent
    assert _mode(folder) == 0o700
    # the private root is the only thing created in the shared temp dir
    root = folder.parent
    assert root.parent == shared_tmp and _mode(root) == 0o700
    assert not (shared_tmp / "sporely_dialog_previews").exists()


def test_calibration_dir_is_owner_only(qapp, shared_tmp, tmp_path, monkeypatch):
    import utils.raw_render as raw_render
    from ui.live_lab_tab import LiveLabTab

    src = tmp_path / "raw.dng"
    src.write_bytes(b"x")
    seen = {}

    def fake_render(source, **kwargs):
        seen["dir"] = Path(kwargs["output_dir"])
        return str(seen["dir"] / "out.jpg")

    monkeypatch.setattr(raw_render, "render_raw_image", fake_render)
    stub = SimpleNamespace(
        _current_pending_raw_capture=lambda: SimpleNamespace(source_path=str(src), raw_settings=None),
        _pending_raw_source_exif=lambda pending: (None, None, None),
        _show_status=lambda *a, **k: None,
    )
    path, is_temp = LiveLabTab._prepare_calibration_source_image(stub)
    assert is_temp and path
    assert _mode(seen["dir"]) == 0o700
    root = seen["dir"].parent
    assert root.parent == shared_tmp and _mode(root) == 0o700
    assert not (shared_tmp / "sporely").exists()


def test_helper_is_one_root_per_process_and_removed_on_exit(shared_tmp):
    a = private_temp.private_temp_dir("a")
    b = private_temp.private_temp_dir("b")
    assert a.parent == b.parent
    assert _mode(a.parent) == 0o700
    root = a.parent
    private_temp._remove_root()
    assert not root.exists()
