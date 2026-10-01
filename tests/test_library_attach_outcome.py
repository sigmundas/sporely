"""Host-side outcome reporting for the Library tab's (batch) attach.

``MainWindow._attach_normalized_reference_outcome`` must tell a batch caller
exactly what happened per item -- attached / already attached / failed --
without showing a dialog, and must pass each item's own role through.
"""
from __future__ import annotations

import os
from types import MethodType, SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

import ui.main_window as mw
from database.reference_library import ReferenceLibraryError
from ui.main_window import MainWindow


def _stub():
    added: list[dict] = []
    stub = SimpleNamespace(
        tr=lambda text: text,
        _add_reference_series_entry=lambda entry: added.append(entry) or True,
        _build_malformed_reference_series_entry=lambda use: {"warning": use.id},
    )
    stub._attach_normalized_reference_outcome = MethodType(
        MainWindow._attach_normalized_reference_outcome, stub
    )
    return stub, added


@pytest.fixture
def repo(monkeypatch):
    state = {"existing": set(), "fail": set(), "calls": [], "detached": []}

    def attach_with_status(obs_id, ms_id, *, role):
        state["calls"].append((obs_id, ms_id, role))
        if ms_id in state["fail"]:
            raise ReferenceLibraryError("boom")
        created = ms_id not in state["existing"]
        state["existing"].add(ms_id)
        return SimpleNamespace(id=f"use-{ms_id}", role=role), created

    monkeypatch.setattr(
        mw.ObservationReferenceUseRepository, "attach_with_status", staticmethod(attach_with_status)
    )
    monkeypatch.setattr(
        mw.ObservationReferenceUseRepository,
        "detach",
        staticmethod(lambda use_id: state["detached"].append(use_id)),
    )
    monkeypatch.setattr(
        mw.MeasurementSetPreferenceRepository, "mark_used", staticmethod(lambda _id: None)
    )
    monkeypatch.setattr(mw, "translate_observation_reference_use", lambda use: {"use": use.id})
    return state


def test_outcomes_are_attached_already_attached_and_failed_with_roles(repo):
    stub, added = _stub()
    repo["existing"].add("ms-b")
    repo["fail"].add("ms-c")
    results = [
        stub._attach_normalized_reference_outcome(5, "ms-a", "contradicts"),
        stub._attach_normalized_reference_outcome(5, "ms-b", "compared"),
        stub._attach_normalized_reference_outcome(5, "ms-c", "supports_identification"),
    ]
    assert [r[0] for r in results] == ["attached", "already_attached", "failed"]
    assert "boom" in results[2][1]
    assert repo["calls"] == [
        (5, "ms-a", "contradicts"),
        (5, "ms-b", "compared"),
        (5, "ms-c", "supports_identification"),
    ]
    # Earlier successes are never rolled back by a later failure.
    assert repo["detached"] == []
    assert added == [{"use": "use-ms-a"}, {"use": "use-ms-b"}]


def test_untranslatable_new_row_is_rolled_back_and_reported_failed(repo, monkeypatch):
    stub, added = _stub()
    monkeypatch.setattr(mw, "translate_observation_reference_use", lambda use: None)
    status, reason, _severity = stub._attach_normalized_reference_outcome(5, "ms-a", "compared")
    assert status == "failed" and reason
    assert repo["detached"] == ["use-ms-a"]
    assert added == []
