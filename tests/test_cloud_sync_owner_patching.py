"""Tests for the test-only same-object facade patch helper."""

from __future__ import annotations

import types

import pytest

from tests.cloud_sync_owner_patching import install_facade_owner_patching

FACADE = "fake_facade"
PREFIX = "fake_owners."


def _modules(monkeypatch):
    shared = object()
    facade = types.ModuleType(FACADE)
    owner = types.ModuleType(PREFIX + "a")
    other = types.ModuleType(PREFIX + "b")
    unrelated = types.ModuleType("elsewhere")
    facade.shared = owner.shared = other.shared = unrelated.shared = shared
    facade.distinct = object()
    owner.distinct = object()
    for module in (facade, owner, other, unrelated):
        monkeypatch.setitem(__import__("sys").modules, module.__name__, module)
    return facade, owner, other, unrelated, shared


def test_facade_patch_reaches_same_object_owner_bindings_and_is_undone():
    mp = pytest.MonkeyPatch()
    facade, owner, other, unrelated, shared = _modules(mp)
    install_facade_owner_patching(mp, facade=FACADE, owner_prefix=PREFIX)
    mp.setattr(facade, "shared", "fake")
    assert facade.shared == owner.shared == other.shared == "fake"
    assert unrelated.shared is shared  # outside the owner package
    mp.undo()
    assert facade.shared is owner.shared is other.shared is shared


def test_string_target_form_reaches_owners():
    mp = pytest.MonkeyPatch()
    facade, owner, *_ = _modules(mp)
    install_facade_owner_patching(mp, facade=FACADE, owner_prefix=PREFIX)
    mp.setattr(f"{FACADE}.shared", "fake")
    assert owner.shared == "fake"
    mp.undo()


def test_different_object_owner_binding_is_not_patched():
    mp = pytest.MonkeyPatch()
    facade, owner, *_ = _modules(mp)
    before = owner.distinct
    install_facade_owner_patching(mp, facade=FACADE, owner_prefix=PREFIX)
    mp.setattr(facade, "distinct", "fake")
    assert facade.distinct == "fake"
    assert owner.distinct is before
    mp.undo()


def test_missing_facade_name_fails_even_without_raising():
    mp = pytest.MonkeyPatch()
    facade, *_ = _modules(mp)
    install_facade_owner_patching(mp, facade=FACADE, owner_prefix=PREFIX)
    with pytest.raises(AttributeError):
        mp.setattr(facade, "absent", 1, raising=False)
    mp.undo()


def test_non_facade_targets_are_untouched():
    mp = pytest.MonkeyPatch()
    facade, owner, _other, unrelated, shared = _modules(mp)
    install_facade_owner_patching(mp, facade=FACADE, owner_prefix=PREFIX)
    mp.setattr(unrelated, "shared", "fake")
    assert facade.shared is owner.shared is shared
    mp.undo()


def test_production_code_never_imports_the_helper():
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    offenders = [
        str(path.relative_to(root))
        for top in ("utils", "database", "ui", "tools")
        for path in (root / top).rglob("*.py")
        if "cloud_sync_owner_patching" in path.read_text(encoding="utf-8", errors="ignore")
    ]
    assert offenders == []


def test_patch_facade_object_reaches_same_object_owners_and_restores():
    from tests.cloud_sync_owner_patching import patch_facade_object

    mp = pytest.MonkeyPatch()
    facade, owner, other, unrelated, shared = _modules(mp)
    distinct = owner.distinct
    with patch_facade_object(facade, "shared", facade=FACADE, owner_prefix=PREFIX) as fake:
        assert facade.shared is owner.shared is other.shared is fake
        assert unrelated.shared is shared
    with patch_facade_object(facade, "distinct", return_value=1, facade=FACADE, owner_prefix=PREFIX):
        assert owner.distinct is distinct
    assert facade.shared is owner.shared is other.shared is shared
    with pytest.raises(AttributeError):
        with patch_facade_object(facade, "absent", facade=FACADE, owner_prefix=PREFIX):
            pass
    mp.undo()
