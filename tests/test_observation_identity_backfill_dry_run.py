"""Identity backfill dry run: self-identity, recovered Artsorakel ids, D3.

Extends ``database/audit_observation_identity.py``. The owner's principles:
an AI suggestion is never the observer's identification unless the observer
kept it; a resolved identifier binds only when the observer's identification
is that concept's name (necessary, not sufficient — the identifier chose the
concept); a name alone is a review candidate, never identity; same-name
concepts are never chosen between.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from database.audit_observation_identity import (
    ProductionMigrationGate,
    TaxonomyArtifact,
    apply_repairs,
    audit,
    read_desktop_rows,
    render_review_summary,
)
from utils.taxon_identity import TaxonIdentity


@pytest.fixture
def artifact(tmp_path: Path):
    path = tmp_path / "candidate.sqlite3"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE taxonomy_meta (key TEXT PRIMARY KEY, value TEXT);
        INSERT INTO taxonomy_meta VALUES ('content_release_id', 'tax-test-01');
        CREATE TABLE taxon_min (taxon_id INTEGER PRIMARY KEY,
                                canonical_scientific_name TEXT, source_system TEXT,
                                canonical_source_system TEXT, canonical_external_id TEXT);
        INSERT INTO taxon_min VALUES
            (7821, 'Entoloma conferendum', 'col_xr', 'col_xr', '39ZCL'),
            (624659, 'Panaeolina foenisecii', 'nortaxa', 'nortaxa', '52330'),
            (40673, 'Panaeolina foenisecii', 'col_xr', 'col_xr', '4C943'),
            (626870, 'Russula sanguinea', 'nortaxa', 'nortaxa', '57843'),
            (61951, 'Russula emetica', 'col_xr', 'col_xr', '4TRVW'),
            (626279, 'Hydnum repandum coll.', 'nortaxa', 'nortaxa', '56280'),
            (16187, 'Hydnum repandum', 'col_xr', 'col_xr', 'XYZ1'),
            (40672, 'Panaeolina castaneifolia', 'col_xr', 'col_xr', 'XYZ2');
        CREATE TABLE taxon_external_id_text_min (taxon_id INTEGER, source_system TEXT,
                                                 namespace TEXT, external_id TEXT);
        INSERT INTO taxon_external_id_text_min VALUES
            (7821, 'nortaxa', 'nortaxa_taxon_id', '53482');
        """
    )
    conn.commit()
    conn.close()
    art = TaxonomyArtifact(path)
    yield art
    art.close()


def _state(name, name_id):
    """The stored AI state shape the Artsorakel API produces (no taxonId)."""
    prediction = {"name": name, "scientificName": name, "scientific_name_id": name_id,
                  "scientific_name_id_shared": "GBIF:2526698"}
    return json.dumps({"predictions": {"0": [prediction]}, "selected": {"0": prediction}})


def _db(tmp_path: Path, rows: list[dict]) -> Path:
    path = tmp_path / "observations.sqlite3"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY, genus TEXT, species TEXT, common_name TEXT,
            sporely_taxon_id INTEGER, taxon_identity_state TEXT, taxon_identity_proof TEXT,
            taxon_identity_source_system TEXT, taxon_identity_namespace TEXT,
            taxon_identity_external_id TEXT, taxon_identity_raw_external_id TEXT,
            taxon_identity_provenance TEXT, ai_selected_taxon_id TEXT,
            ai_selected_scientific_name TEXT, ai_selected_service TEXT, ai_state_json TEXT,
            scientific_name_snapshot TEXT, taxon_rank_snapshot TEXT,
            cloud_id TEXT, sync_status TEXT);
        CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT);
        """
    )
    for row in rows:
        keys = sorted(row)
        conn.execute(
            f"INSERT INTO observations ({', '.join(keys)}) VALUES ({', '.join('?' for _ in keys)})",
            tuple(row[k] for k in keys),
        )
    conn.commit()
    conn.close()
    return path


ARTSORAKEL = {"ai_selected_service": "artsorakel"}


def _records(tmp_path, artifact, rows):
    report = audit(read_desktop_rows(_db(tmp_path, rows)), artifact, origin="desktop")
    return report, {r.observation_id: r for r in report.records}


def test_a_nortaxa_only_concept_binds_through_its_own_id(tmp_path, artifact):
    _, records = _records(tmp_path, artifact, [
        {"id": 1, "genus": "Panaeolina", "species": "foenisecii", **ARTSORAKEL,
         "ai_selected_taxon_id": "NBIC:52330",
         "ai_selected_scientific_name": "Panaeolina foenisecii"},
    ])
    r = records[1]
    assert (r.identity_class, r.candidate_sporely_taxon_id) == (
        "proven_external_with_unique_bridge", 624659)
    assert "taxon_min.canonical_source_usage" in r.evidence


def test_a_lost_artsorakel_id_is_recovered_from_the_stored_prediction(tmp_path, artifact):
    _, records = _records(tmp_path, artifact, [
        # Observation 1032's shape: id only in scientific_name_id.
        {"id": 1, "genus": "Panaeolina", "species": "foenisecii", **ARTSORAKEL,
         "ai_selected_scientific_name": "Panaeolina foenisecii",
         "ai_state_json": _state("Panaeolina foenisecii", "NBIC:52330")},
        # The selected prediction is a different taxon than the recorded pick.
        {"id": 2, "genus": "Panaeolina", "species": "foenisecii", **ARTSORAKEL,
         "ai_selected_scientific_name": "Panaeolina foenisecii",
         "ai_state_json": _state("Entoloma conferendum", "NBIC:53482")},
    ])
    assert records[1].provider_id_recovered is True
    assert records[1].preserved_raw_external_id == "NBIC:52330"   # never the GBIF id
    assert records[1].candidate_sporely_taxon_id == 624659
    assert records[2].provider_id_recovered is False
    assert records[2].candidate_sporely_taxon_id is None


@pytest.mark.parametrize("genus, species, ai_name, raw", [
    ("Russula", "emetica", "Russula sanguinea", "NBIC:57843"),       # other species
    ("Hydnum", "repandum", "Hydnum repandum coll.", "NBIC:56280"),   # aggregate vs strict
])
def test_an_overridden_ai_suggestion_is_never_proposed(tmp_path, artifact, genus, species, ai_name, raw):
    report, records = _records(tmp_path, artifact, [
        {"id": 1, "genus": genus, "species": species, **ARTSORAKEL,
         "ai_selected_taxon_id": raw, "ai_selected_scientific_name": ai_name},
        {"id": 2, "genus": genus, "species": species, **ARTSORAKEL,
         "ai_selected_scientific_name": ai_name, "ai_state_json": _state(ai_name, raw)},
    ])
    for r in records.values():
        assert r.identity_class == "ai_suggestion_not_identification"
        assert r.proposed_action == "report_only"
        assert r.candidate_sporely_taxon_id is None
    assert report.repairable() == []


def test_a_row_identifier_naming_another_taxon_is_not_bound(tmp_path, artifact):
    # The row's own preserved tuple is the aggregate; the identification is strict.
    _, records = _records(tmp_path, artifact, [
        {"id": 1, "genus": "Hydnum", "species": "repandum",
         **TaxonIdentity.from_prefixed_external_id("NBIC:56280").to_row()},
    ])
    assert records[1].identity_class == "identifier_name_mismatch"
    assert records[1].candidate_sporely_taxon_id is None


def test_a_name_is_a_candidate_never_an_identity(tmp_path, artifact):
    report, records = _records(tmp_path, artifact, [
        {"id": 1, "genus": "Panaeolina", "species": "castaneifolia"},
        {"id": 2, "genus": "Panaeolina", "species": "foenisecii"},   # COL + NorTaxa
        {"id": 3, "genus": "Russula", "species": "emetica", **ARTSORAKEL,
         "ai_selected_taxon_id": "NBIC:57843", "ai_selected_scientific_name": "Russula sanguinea"},
    ])
    assert records[1].name_candidate_ids == (40672,)
    assert records[2].name_candidate_ids == (40673, 624659)
    assert records[3].name_candidate_ids == (61951,)
    assert all(r.candidate_sporely_taxon_id is None for r in report.records)
    assert report.repairable() == []


def test_confirmed_identities_are_kept_and_get_no_candidates(tmp_path, artifact):
    _, records = _records(tmp_path, artifact, [
        {"id": 1, "genus": "Panaeolina", "species": "castaneifolia",
         **TaxonIdentity.from_taxonomy_v2_artifact(40672).to_row()},
    ])
    assert records[1].identity_class == "proven_sporely_identity"
    assert records[1].proposed_action == "no_change_required"
    assert records[1].name_candidate_ids == ()


def test_apply_binds_only_the_gated_rows(tmp_path, artifact):
    rows = [
        {"id": 1, "genus": "Panaeolina", "species": "foenisecii", **ARTSORAKEL,
         "ai_selected_scientific_name": "Panaeolina foenisecii",
         "ai_state_json": _state("Panaeolina foenisecii", "NBIC:52330")},
        {"id": 2, "genus": "Russula", "species": "emetica", **ARTSORAKEL,
         "ai_selected_taxon_id": "NBIC:57843", "ai_selected_scientific_name": "Russula sanguinea"},
        {"id": 3, "genus": "Panaeolina", "species": "castaneifolia"},
    ]
    db = _db(tmp_path, rows)
    report = audit(read_desktop_rows(db), artifact, origin="desktop")
    gate = ProductionMigrationGate(True, True, True, True, True, evidence=("test",))
    apply_repairs(report, observation_db_path=db, release_id="tax-test-01", gate=gate)
    conn = sqlite3.connect(db)
    got = dict(conn.execute("SELECT id, sporely_taxon_id FROM observations").fetchall())
    status = dict(conn.execute("SELECT id, sync_status FROM observations").fetchall())
    conn.close()
    assert got == {1: 624659, 2: None, 3: None}
    assert status == {1: None, 2: None, 3: None}   # the repair never touches sync state


def test_the_summary_reports_cloud_context_and_every_section(tmp_path, artifact):
    rows = [
        {"id": 1, "genus": "Panaeolina", "species": "foenisecii", **ARTSORAKEL,
         "ai_selected_scientific_name": "Panaeolina foenisecii", "cloud_id": "917",
         "sync_status": "synced", "ai_state_json": _state("Panaeolina foenisecii", "NBIC:52330")},
        {"id": 2, "genus": "Russula", "species": "emetica", **ARTSORAKEL,
         "ai_selected_taxon_id": "NBIC:57843", "ai_selected_scientific_name": "Russula sanguinea"},
        {"id": 3, "genus": "Panaeolina", "species": "foenisecii"},
    ]
    db = _db(tmp_path, rows)
    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO settings VALUES (?, ?)", (
        "sporely_cloud_snapshot_obs_917", json.dumps({"observation": {"taxon_identity": ""}})))
    conn.commit()
    conn.close()
    report = audit(read_desktop_rows(db), artifact, origin="desktop")
    text = render_review_summary(report, artifact=artifact, observation_db_path=db)
    assert "Observations audited: **3**" in text
    assert "recovered NBIC:52330" in text
    assert "cloud id 917, cloud identity none, synced" in text
    assert "AI suggestion the observer changed" in text
    assert "40673 Panaeolina foenisecii (col_xr); 624659 Panaeolina foenisecii (nortaxa)" in text
