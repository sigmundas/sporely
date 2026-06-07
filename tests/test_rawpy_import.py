import importlib

import pytest

from utils.raw_render import RawRenderingUnavailableError
from utils.rawpy_import import import_rawpy


def test_import_rawpy_raises_clear_error_when_missing(monkeypatch):
    real_import_module = importlib.import_module

    def fake_import_module(name, package=None):
        if name == "rawpy":
            raise ModuleNotFoundError("No module named 'rawpy'")
        return real_import_module(name, package)

    monkeypatch.setattr(importlib, "import_module", fake_import_module)

    with pytest.raises(RawRenderingUnavailableError, match="rawpy"):
        import_rawpy()
