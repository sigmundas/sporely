import importlib
import logging

import pytest

from utils.raw_render import RawRenderingUnavailableError
from utils.rawpy_import import RawpyDiagnostic, import_rawpy, rawpy_diagnostics, read_rawpy_capture_datetime


def test_import_rawpy_raises_clear_error_when_missing(monkeypatch):
    real_import_module = importlib.import_module

    def fake_import_module(name, package=None):
        if name == "rawpy":
            raise ModuleNotFoundError("No module named 'rawpy'")
        return real_import_module(name, package)

    monkeypatch.setattr(importlib, "import_module", fake_import_module)

    with pytest.raises(RawRenderingUnavailableError, match="rawpy") as excinfo:
        import_rawpy()

    assert isinstance(excinfo.value.__cause__, ModuleNotFoundError)


def test_rawpy_diagnostics_reports_missing_rawpy(monkeypatch):
    real_import_module = importlib.import_module

    def fake_import_module(name, package=None):
        if name == "rawpy":
            raise ModuleNotFoundError("No module named 'rawpy'")
        return real_import_module(name, package)

    monkeypatch.setattr(importlib, "import_module", fake_import_module)

    diagnostic = rawpy_diagnostics()

    assert diagnostic == RawpyDiagnostic(
        available=False,
        error_type="ModuleNotFoundError",
        error_message="No module named 'rawpy'",
        rawpy_version=None,
    )


def test_rawpy_diagnostics_reports_broken_import_and_exception_chain(monkeypatch):
    real_import_module = importlib.import_module

    def fake_import_module(name, package=None):
        if name == "rawpy":
            raise RuntimeError("boom")
        return real_import_module(name, package)

    monkeypatch.setattr(importlib, "import_module", fake_import_module)

    diagnostic = rawpy_diagnostics()

    assert diagnostic.available is False
    assert diagnostic.error_type == "RuntimeError"
    assert diagnostic.error_message == "boom"

    with pytest.raises(RawRenderingUnavailableError) as excinfo:
        import_rawpy()

    assert isinstance(excinfo.value.__cause__, RuntimeError)
    assert str(excinfo.value.__cause__) == "boom"


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


def test_read_rawpy_capture_datetime_logs_useful_details_in_debug_mode(monkeypatch, tmp_path, caplog):
    real_import_module = importlib.import_module

    def fake_import_module(name, package=None):
        if name == "rawpy":
            raise ModuleNotFoundError("No module named 'rawpy'")
        return real_import_module(name, package)

    monkeypatch.setenv("SPORELY_DEBUG_RAWPY", "1")
    monkeypatch.setattr(importlib, "import_module", fake_import_module)

    raw_path = tmp_path / "sample.nef"
    raw_path.write_bytes(b"raw-bytes")

    with caplog.at_level(logging.DEBUG, logger="utils.rawpy_import"):
        assert read_rawpy_capture_datetime(raw_path) is None

    assert any("rawpy capture timestamp import failed" in record.message for record in caplog.records)
    assert any("ModuleNotFoundError" in record.message for record in caplog.records)
