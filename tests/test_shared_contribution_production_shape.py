"""Regression: the desktop parses and copies a production-shaped shared row.

``tests/fixtures/shared_contribution_production_shape.json`` is the one row
production's anon ``search_public_reference_contributions_v2`` served for
taxon 34615 on 2026-10-02 with people's names and every UUID replaced by
synthetic values (every key and type kept).
Its ``citation`` carries ``short_label`` and ``citation_override`` (sporely-web
20260930232633 onward), which the exact-key reader used to reject, so the
catalogue was always empty.
"""
from __future__ import annotations

import copy
import json
import sqlite3
from pathlib import Path

import pytest

from database import schema
from database.curated_reference_forks import (
    CuratedReferenceError,
    copy_curated_bundle_to_personal_library,
    normalize_curated_bundle,
    search_shared_reference_contributions,
    validate_frozen_curated_provenance,
)

TAXON = 34615
ROW = json.loads(
    (Path(__file__).parent / "fixtures" / "shared_contribution_production_shape.json").read_text("utf-8")
)


@pytest.fixture()
def isolated(tmp_path, monkeypatch):
    reference_path = tmp_path / "reference_values.db"
    monkeypatch.setattr(schema, "get_database_path", lambda: tmp_path / "mushrooms.db")
    monkeypatch.setattr(schema, "get_reference_database_path", lambda: reference_path)
    monkeypatch.setattr(schema, "get_bundled_reference_database_path", lambda: tmp_path / "missing.db")
    schema.init_database()
    return reference_path


def test_production_row_has_the_work_citation_keys():
    assert {"short_label", "citation_override"} <= set(ROW["citation"])
    assert ROW["contributor"]["id"] is None


def test_catalogue_search_returns_the_production_row():
    class Client:
        def search_public_reference_contributions_v2(self, *args):
            return [copy.deepcopy(ROW)]

    bundles = search_shared_reference_contributions(Client(), TAXON)
    assert len(bundles) == 1
    bundle = bundles[0]
    assert bundle.contribution_id == ROW["contribution_id"]
    assert bundle.relationship_roles == tuple(ROW["relationship_roles"])
    assert bundle.citation["short_label"] == ROW["citation"]["short_label"]
    assert bundle.measurement_details_omitted is False


@pytest.mark.parametrize(
    ("key", "value"),
    [("short_label", "x" * 513), ("citation_override", "x" * 8193), ("short_label", 7)],
)
def test_work_citation_keys_are_bounded(key, value):
    row = copy.deepcopy(ROW)
    row["citation"][key] = value
    with pytest.raises(CuratedReferenceError):
        normalize_curated_bundle(row, expected_taxon_id=TAXON)


def test_citation_without_the_work_keys_is_still_accepted():
    row = copy.deepcopy(ROW)
    del row["citation"]["short_label"], row["citation"]["citation_override"]
    normalize_curated_bundle(row, expected_taxon_id=TAXON)
    row["citation"]["short_label"] = "only one"
    with pytest.raises(CuratedReferenceError):
        normalize_curated_bundle(row, expected_taxon_id=TAXON)


def test_copy_and_frozen_round_trip(isolated):
    bundle = normalize_curated_bundle(copy.deepcopy(ROW), expected_taxon_id=TAXON)
    fork = copy_curated_bundle_to_personal_library(bundle)
    assert fork.created
    with sqlite3.connect(isolated) as connection:
        stored = connection.execute(
            "SELECT source_envelope_json, source_sha256 FROM curated_reference_forks"
        ).fetchone()
        work = connection.execute(
            "SELECT short_label FROM reference_works WHERE id=?", (fork.reference_work_id,)
        ).fetchone()
    assert work[0] == ROW["citation"]["short_citation"]
    frozen = validate_frozen_curated_provenance(
        stored[0], stored[1],
        curated_measurement_set_id=fork.curated_measurement_set_id,
        bundle_revision=fork.bundle_revision,
        sporely_taxon_id=TAXON,
    )
    assert frozen.citation["citation_override"] is None
    assert copy_curated_bundle_to_personal_library(bundle).created is False
