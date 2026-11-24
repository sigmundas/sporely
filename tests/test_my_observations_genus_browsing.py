"""The My observations tab must browse a whole genus, not require a species.

``default_my_observation_candidates`` previously returned ``[]`` without
touching the database whenever the species was blank, and the tab reported
that as "No previous observations of this taxon have spore measurements" --
a statement about data that had never been queried. A genus-only
identification is the common case while an observation is still being
worked out, which is exactly when comparing it against one's own earlier
collections is most useful.

Uses the ``my_observations=`` constructor override so no real database is
needed, except where the query itself is the subject, which is
monkeypatched at ``ObservationDB``.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

from ui.add_reference_dialog import (
    default_my_observation_candidates,
)


@pytest.fixture(autouse=True)
def isolated_picker_settings(tmp_path, monkeypatch):
    from PySide6.QtCore import QSettings
    import ui.window_state as geometry

    def settings(*_args):
        return QSettings(str(tmp_path / "picker.ini"), QSettings.IniFormat)

    monkeypatch.setattr(geometry, "QSettings", settings, raising=False)


def _patch_observation_db(monkeypatch, rows: list[dict]) -> dict:
    """Route the My-observations loader at fixed rows, recording its call."""
    from ui import add_reference_dialog as mod

    seen: dict = {}

    def fake_rows(genus, species="", exclude_observation_id=None):
        seen["genus"] = genus
        seen["species"] = species
        return rows

    monkeypatch.setattr(
        mod.ObservationDB,
        "get_personal_observations_for_species",
        staticmethod(fake_rows),
    )
    monkeypatch.setattr(
        mod.MeasurementDB,
        "get_measurements_for_observation",
        staticmethod(
            lambda _obs_id: [
                {"length_um": 8.0, "width_um": 5.0, "measurement_type": "spore"}
            ]
        ),
    )
    monkeypatch.setattr(
        mod.ObservationDB,
        "get_observation",
        staticmethod(lambda _obs_id: {"location": "Oppland"}),
    )
    return seen


def test_genus_only_identification_still_loads_observations(monkeypatch):
    seen = _patch_observation_db(
        monkeypatch,
        [
            {"id": 301, "date": "2024-03-03", "author": "A", "genus": "Amanita", "species": "muscaria"},
            {"id": 302, "date": "2024-04-04", "author": "A", "genus": "Amanita", "species": "rubescens"},
        ],
    )

    result = default_my_observation_candidates("Amanita", "")

    assert [c.observation_id for c in result] == [301, 302]
    assert seen["genus"] == "Amanita"
    assert seen["species"] == ""
    # The row's own taxon is carried through, so a genus-wide list can say
    # which species each row is.
    assert [c.species for c in result] == ["muscaria", "rubescens"]


def test_no_genus_at_all_still_returns_nothing_without_querying(monkeypatch):
    """Widening to genus-only must not widen all the way to "every observation"."""
    seen = _patch_observation_db(monkeypatch, [{"id": 1, "date": "", "author": ""}])
    assert default_my_observation_candidates("", "") == []
    assert seen == {}


def test_species_still_narrows_as_before(monkeypatch):
    """The previous exact-taxon behaviour is unchanged when a species is given."""
    seen = _patch_observation_db(
        monkeypatch,
        [{"id": 401, "date": "2024-05-05", "author": "A", "genus": "Amanita", "species": "muscaria"}],
    )
    result = default_my_observation_candidates("Amanita", "muscaria")
    assert [c.observation_id for c in result] == [401]
    assert seen["species"] == "muscaria"


