"""Tests for the module functions in ui.add_reference_dialog.

The Add-reference dialog itself is retired (stage 10a2): the reference
drawer reuses these helpers -- the library filter and the
My-observations candidate loader. No real database is needed.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest


from database.reference_library import MeasurementSetCandidate
from ui.add_reference_dialog import (
    default_my_observation_candidates,
    filter_library_candidates,
)


@pytest.fixture(autouse=True)
def isolated_picker_settings(tmp_path, monkeypatch):
    from PySide6.QtCore import QSettings
    import ui.window_state as geometry

    def settings(*_args):
        return QSettings(str(tmp_path / "picker.ini"), QSettings.IniFormat)

    monkeypatch.setattr(geometry, "QSettings", settings)


def _candidates() -> list[MeasurementSetCandidate]:
    return [
        MeasurementSetCandidate(
            measurement_set_id="ms-1",
            short_label="Niskanen, Liimatainen & Kytövuori 2018",
            name_as_published="Cortinarius limonius (Fr.) Fr.",
            locator_text="pp. 146–148",
            data_kind="range",
            raw_text="(8.1–)8.5–10.8(–11.4) µm",
            revision=1,
            reference_work_id="w-1",
            reference_treatment_id="t-1",
            taxon_id="7",
        ),
        MeasurementSetCandidate(
            measurement_set_id="ms-2",
            short_label="Niskanen, Liimatainen & Kytövuori 2018",
            name_as_published="Cortinarius limonius (Fr.) Fr.",
            locator_text="supplementary dataset S4",
            data_kind="raw_points",
            raw_text="8 paired holotype measurements",
            revision=1,
            reference_work_id="w-1",
            reference_treatment_id="t-1",
            taxon_id="7",
        ),
        MeasurementSetCandidate(
            measurement_set_id="ms-3",
            short_label="Brandrud et al. 2020",
            name_as_published="Cortinarius rubellus Cooke",
            locator_text="Vol. 2, p. 311",
            data_kind="range",
            raw_text="8.0–9.5 µm",
            revision=1,
            reference_work_id="w-2",
            reference_treatment_id="t-2",
            taxon_id="99",
        ),
    ]


def _ai_candidates() -> list[dict]:
    return [
        {
            "source": "arts",
            "scientific_name": "Cortinarius rubellus",
            "vernacular": "Bittersnerlerørsopp",
            "genus": "Cortinarius",
            "species": "rubellus",
            "score": 0.82,
        },
    ]


def test_filter_library_candidates_pure_function_matches_dialog_behavior():
    candidates = _candidates()
    only_taxon = filter_library_candidates(
        candidates, taxon_id="7", only_this_taxon=True
    )
    assert {c.measurement_set_id for c in only_taxon} == {"ms-1", "ms-2"}
    all_candidates = filter_library_candidates(
        candidates, taxon_id="7", only_this_taxon=False
    )
    assert {c.measurement_set_id for c in all_candidates} == {"ms-1", "ms-2", "ms-3"}


# ---------------------------------------------------------------------
# My observations tab
# ---------------------------------------------------------------------


# ---------------------------------------------------------------------
# Community tab
# ---------------------------------------------------------------------


def test_default_my_observation_candidates_filters_by_taxon_and_requires_points(monkeypatch):
    from ui import add_reference_dialog as mod

    def fake_get_personal_observations_for_species(genus, species, exclude_observation_id=None):
        assert (genus, species) == ("Cortinarius", "limonius")
        assert exclude_observation_id == 42
        return [
            {"id": 201, "date": "2024-01-01", "author": "A"},
            {"id": 202, "date": "2024-02-02", "author": "B"},
        ]

    def fake_get_measurements_for_observation(observation_id):
        # Observation 202 has no usable measurements and must be excluded.
        if observation_id == 201:
            return [{"length_um": 8.0, "width_um": 5.0, "measurement_type": "spore"}]
        return []

    def fake_get_observation(observation_id):
        return {"location": "Oppland"} if observation_id == 201 else {}

    monkeypatch.setattr(
        mod.ObservationDB,
        "get_personal_observations_for_species",
        staticmethod(fake_get_personal_observations_for_species),
    )
    monkeypatch.setattr(
        mod.MeasurementDB,
        "get_measurements_for_observation",
        staticmethod(fake_get_measurements_for_observation),
    )
    monkeypatch.setattr(
        mod.ObservationDB, "get_observation", staticmethod(fake_get_observation)
    )

    result = default_my_observation_candidates(
        "Cortinarius", "limonius", exclude_observation_id=42
    )
    assert [c.observation_id for c in result] == [201]
    assert result[0].location == "Oppland"
    assert result[0].n == 1


# ---------------------------------------------------------------------
# Return-through-the-real-signal-path taxon target regressions
# (stage-4b-fix2 Part 2 -- see review at
# ui/add_reference_dialog.py:425-428,488-498)
# ---------------------------------------------------------------------


# ---------------------------------------------------------------------
# Enter-manually tab (stage 4c)
# ---------------------------------------------------------------------


# ---------------------------------------------------------------------
# "Save to library": storing a manual entry without plotting it
# ---------------------------------------------------------------------


# ---------------------------------------------------------------------
# Host integration -- MainWindow._on_add_reference_clicked
# (stage-4b-fix2 Part 3 -- see review at ui/main_window.py:11540-11545)
# ---------------------------------------------------------------------


# Taxonomy-v2 closeout Stage 2 changed what `taxon_id` the picker receives.
# `_active_sporely_taxon_id` drives normalized measurement-set attachment, so
# it now returns a PROVEN identity only. A bare pre-Stage-2 integer is
# legacy-unverified — it could be an external identifier that numerically
# collides with a real Sporely concept — and must not attach. Each case below
# states the identity it carries and the `taxon_id` that follows from it.
#
# Consequence worth knowing: a pre-Stage-2 observation loses normalized
# reference attachment until the backfill re-verifies its integer against the
# taxonomy artifact. That is the same gate cloud sync applies.


# ---------------------------------------------------------------------
# "+ New publication…" must not open a modal from inside the selection
# signal (segfault regression)
# ---------------------------------------------------------------------


