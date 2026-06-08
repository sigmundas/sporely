import importlib

import pytest

from utils.raw_render import RawRenderingUnavailableError
from utils.rawpy_import import import_rawpy, read_rawpy_capture_datetime


def test_import_rawpy_raises_clear_error_when_missing(monkeypatch):
    real_import_module = importlib.import_module

    def fake_import_module(name, package=None):
        if name == "rawpy":
            raise ModuleNotFoundError("No module named 'rawpy'")
        return real_import_module(name, package)

    monkeypatch.setattr(importlib, "import_module", fake_import_module)

    with pytest.raises(RawRenderingUnavailableError, match="rawpy"):
        import_rawpy()


def test_read_rawpy_capture_datetime_returns_none_when_rawpy_is_missing(monkeypatch, tmp_path):
    real_import_module = importlib.import_module

    def fake_import_module(name, package=None):
        if name == "rawpy":
            raise ModuleNotFoundError("No module named 'rawpy'")
        return real_import_module(name, package)

    monkeypatch.setattr(importlib, "import_module", fake_import_module)

    raw_path = tmp_path / "sample.nef"
    raw_path.write_bytes(b"raw-bytes")

    assert read_rawpy_capture_datetime(raw_path) is None
