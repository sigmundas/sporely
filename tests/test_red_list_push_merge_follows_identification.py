"""A cloud Red List belongs to the cloud's identification, not to the row.

``_merge_cloud_selected_ai_fields`` fills local NULL AI-selection and Red List
fields from the remote row before a push, because an older desktop row may
simply not have pulled those columns yet. ``_adopt_merge_filled_ai_fields_locally``
then writes the filled values into the local row so the snapshot and the
local columns agree.

That gap-filling is only sound while local and cloud describe the SAME
identification. After the owner re-identifies an observation on the desktop,
the dialog clears the Red List (``_clear_red_list_for_identity_change``) and
the local NULL is an explicit "the new taxon has no stored assessment". Filling
it from the cloud copies the OLD taxon's category onto the new identification,
pushes it back, and adopts it locally — the same cross-taxon leak as the
observation-917 dialog fallback, via sync.

These tests drive the exact two calls ``push_all`` / ``push_observation_to_cloud``
make, against a real schema.
"""
from __future__ import annotations

import sqlite3

import pytest

from database import models, schema
from utils import cloud_sync


@pytest.fixture
def real_db(tmp_path, monkeypatch):
    db_path = tmp_path / "sporely.db"
    monkeypatch.setattr(schema, "get_database_path", lambda: db_path)
    monkeypatch.setattr(schema, "get_reference_database_path", lambda: tmp_path / "ref.db")
    monkeypatch.setattr(schema, "get_bundled_reference_database_path", lambda: tmp_path / "bundled.db")
    monkeypatch.setattr(schema, "_migrate_reference_values", lambda *a, **k: None)
    monkeypatch.setattr(schema, "_migrate_reference_mounts_and_stains", lambda *a, **k: None)
    schema.init_database()
    return db_path


def _local_red_list(db_path, obs_id):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute(
            "SELECT red_list_category, red_list_categories_json FROM observations WHERE id = ?",
            (obs_id,),
        ).fetchone()
    finally:
        conn.close()


def _push_merge_sequence(local_id):
    """What the push path does: merge the payload, then adopt filled fields."""
    local_obs = models.ObservationDB.get_observation(local_id)
    merged = cloud_sync._merge_cloud_selected_ai_fields(local_obs, _REMOTE)
    cloud_sync._adopt_merge_filled_ai_fields_locally(local_id, local_obs, merged)
    return merged


# The cloud row still carries the previous identification and ITS Red List.
_REMOTE = {
    "id": "cloud-917",
    "genus": "Pholiotina",
    "species": "vexans",
    "common_name": "vrang ringerlehatt",
    "species_guess": "Pholiotina vexans",
    "red_list_category": "LC",
    "red_list_categories_json": {"NO": "LC"},
}


def test_reidentified_observation_does_not_inherit_the_old_taxons_red_list(real_db):
    local_id = models.ObservationDB.create_observation(
        date="2026-09-15",
        genus="Conocybe",
        species="rugosa",
        common_name="slank ringkjeglesopp",
        species_guess="Conocybe rugosa",
        red_list_category=None,
        red_list_categories_json=None,
    )

    merged = _push_merge_sequence(local_id)

    assert merged.get("red_list_category") in (None, ""), \
        "the push must not send the previous identification's Red List"
    assert merged.get("red_list_categories_json") in (None, "")
    assert _local_red_list(real_db, local_id) == (None, None), \
        "the old taxon's Red List must not be adopted into the local row"


def test_same_identification_still_backfills_a_not_yet_pulled_red_list(real_db):
    """The merge's original purpose survives: an unrelated desktop edit on a
    row that never pulled the Red List columns must not wipe the cloud value."""
    local_id = models.ObservationDB.create_observation(
        date="2026-09-15",
        genus="Pholiotina",
        species="vexans",
        common_name="vrang ringerlehatt",
        species_guess="Pholiotina vexans",
        notes="unrelated desktop edit",
    )

    merged = _push_merge_sequence(local_id)

    assert merged["red_list_category"] == "LC"
    assert _local_red_list(real_db, local_id)[0] == "LC"


def test_identification_comparison_ignores_case_and_whitespace(real_db):
    local_id = models.ObservationDB.create_observation(
        date="2026-09-15",
        genus=" pholiotina ",
        species="Vexans",
        species_guess="Pholiotina vexans",
    )

    merged = _push_merge_sequence(local_id)

    assert merged["red_list_category"] == "LC"
