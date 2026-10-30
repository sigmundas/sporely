"""Cloud → desktop taxonomy identity (taxonomy-v2 closeout).

Found in the observation-917 integrity round-trip: the cloud row held
``selected_sporely_taxon_id = 83668`` / ``taxon_identity_state = sporely_v2``,
yet a fresh desktop profile created the observation with no identity at all,
because neither the create nor the update pull path read those columns.

The desktop now adopts the cloud identity conservatively
(``cloud_selected_unverified``, only when the installed artifact contains the
concept), preserves an unconfirmable Sporely ID as unresolved evidence, keeps
a cloud ``external_unresolved`` tuple, and never stores a bare integer.
"""
from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import pytest

from database import models, schema
from database.taxon_lookup import InstalledTaxonConcept, installed_taxon_concept
from utils import cloud_sync
from utils.taxon_identity import (
    PROOF_CLOUD_SELECTED_UNVERIFIED,
    PROOF_EXTERNAL_ID_RESOLUTION,
    STATE_EXTERNAL_UNRESOLVED,
    STATE_SPORELY,
    TaxonIdentity,
)

RELEASE = "tax-2026.09.23-01"
_CONCEPTS = {83668: InstalledTaxonConcept(83668, RELEASE, "Conocybe rugosa", "species")}


@pytest.fixture
def real_db(tmp_path, monkeypatch):
    db_path = tmp_path / "sporely.db"
    monkeypatch.setattr(schema, "get_database_path", lambda: db_path)
    monkeypatch.setattr(schema, "get_reference_database_path", lambda: tmp_path / "ref.db")
    monkeypatch.setattr(schema, "get_bundled_reference_database_path", lambda: tmp_path / "bundled.db")
    monkeypatch.setattr(schema, "_migrate_reference_values", lambda *a, **k: None)
    monkeypatch.setattr(schema, "_migrate_reference_mounts_and_stains", lambda *a, **k: None)
    schema.init_database()
    monkeypatch.setattr(cloud_sync, "_installed_taxon_concept", lambda sid: _CONCEPTS.get(int(sid)))
    monkeypatch.setattr(
        cloud_sync, "_import_remote_images",
        lambda *a, **k: {"imported": 0, "warnings": [], "errors": [], "complete": True},
    )
    monkeypatch.setattr(
        cloud_sync, "_import_remote_measurements_for_observation",
        lambda *a, **k: {"warnings": []},
    )
    return db_path


def _remote_917(**overrides) -> dict:
    """Cloud observation 14 as run 3 captured it after the push."""
    row = {
        "id": "14",
        "desktop_id": 917,
        "date": "2026-09-15",
        "genus": "Conocybe",
        "species": "rugosa",
        "common_name": "slank ringkjeglesopp",
        "species_guess": "Conocybe rugosa",
        "country_code": "NO",
        "selected_sporely_taxon_id": 83668,
        "taxon_identity_state": "sporely_v2",
        "taxon_identity_source_system": None,
        "taxon_identity_namespace": None,
        "taxon_identity_external_id": None,
        "taxon_identity_raw_external_id": None,
    }
    row.update(overrides)
    return row


def _identity(local_id) -> TaxonIdentity:
    return TaxonIdentity.from_row(models.ObservationDB.get_observation(local_id))


# ── Create path (fresh profile) ──────────────────────────────────────────────


def test_fresh_profile_adopts_the_cloud_identity_as_cloud_selected_unverified(real_db):
    local_id = cloud_sync._create_local_from_remote(_remote_917())
    row = models.ObservationDB.get_observation(local_id)
    identity = TaxonIdentity.from_row(row)

    assert identity.sporely_taxon_id == 83668
    assert identity.state == STATE_SPORELY
    assert identity.identity_proof == PROOF_CLOUD_SELECTED_UNVERIFIED
    assert identity.is_proven_sporely is False
    assert RELEASE in identity.provenance
    assert "cloud_state=sporely_v2" in identity.provenance
    assert row["scientific_name_snapshot"] == "Conocybe rugosa"
    assert row["taxon_rank_snapshot"] == "species"
    assert (row["genus"], row["species"]) == ("Conocybe", "rugosa")


def test_pre_provenance_cloud_row_is_adopted_with_its_null_state_recorded(real_db):
    """A row bound before the cloud recorded provenance has state NULL."""
    local_id = cloud_sync._create_local_from_remote(_remote_917(taxon_identity_state=None))
    identity = _identity(local_id)
    assert identity.identity_proof == PROOF_CLOUD_SELECTED_UNVERIFIED
    assert "cloud_state=null" in identity.provenance


def test_server_without_provenance_columns_still_adopts_the_selected_id(real_db):
    remote = {k: v for k, v in _remote_917().items() if not k.startswith("taxon_identity_")}
    identity = _identity(cloud_sync._create_local_from_remote(remote))
    assert identity.sporely_taxon_id == 83668
    assert identity.identity_proof == PROOF_CLOUD_SELECTED_UNVERIFIED


def test_id_absent_from_the_installed_artifact_is_preserved_but_never_bound(real_db):
    local_id = cloud_sync._create_local_from_remote(
        _remote_917(selected_sporely_taxon_id=999001)
    )
    row = models.ObservationDB.get_observation(local_id)
    identity = TaxonIdentity.from_row(row)
    assert row["sporely_taxon_id"] is None, "never a bare or unconfirmed integer"
    assert identity.state == STATE_EXTERNAL_UNRESOLVED
    assert (identity.source_system, identity.namespace, identity.external_id) == (
        "sporely", "sporely_taxon_id", "999001",
    )
    assert "absent_from_local_release" in identity.provenance
    assert row["scientific_name_snapshot"] == "Conocybe rugosa"


def test_cloud_external_unresolved_tuple_is_preserved(real_db):
    local_id = cloud_sync._create_local_from_remote(_remote_917(
        genus="Entoloma", species="conferendum", species_guess="Entoloma conferendum",
        selected_sporely_taxon_id=None,
        taxon_identity_state="external_unresolved",
        taxon_identity_source_system="nortaxa",
        taxon_identity_namespace="nortaxa_taxon_id",
        taxon_identity_external_id="53482",
        taxon_identity_raw_external_id="NBIC:53482",
    ))
    row = models.ObservationDB.get_observation(local_id)
    identity = TaxonIdentity.from_row(row)
    assert row["sporely_taxon_id"] is None
    assert identity.state == STATE_EXTERNAL_UNRESOLVED
    assert (identity.source_system, identity.namespace, identity.external_id,
            identity.raw_external_id) == ("nortaxa", "nortaxa_taxon_id", "53482", "NBIC:53482")
    assert row["scientific_name_snapshot"] == "Entoloma conferendum"


def test_cloud_without_identity_creates_a_name_only_observation(real_db):
    local_id = cloud_sync._create_local_from_remote(
        _remote_917(selected_sporely_taxon_id=None, taxon_identity_state=None)
    )
    row = models.ObservationDB.get_observation(local_id)
    assert row["sporely_taxon_id"] is None
    assert row["taxon_identity_state"] is None
    assert row["scientific_name_snapshot"] is None
    assert (row["genus"], row["species"]) == ("Conocybe", "rugosa")


def test_legacy_52369_never_arrives_as_a_sporely_identity(real_db):
    """A provider integer is never a Sporely ID because it is numeric."""
    local_id = cloud_sync._create_local_from_remote(
        _remote_917(selected_sporely_taxon_id=52369)
    )
    row = models.ObservationDB.get_observation(local_id)
    assert row["sporely_taxon_id"] is None


# ── Full-apply update paths ("cloud wins") ───────────────────────────────────


def test_full_apply_writes_the_cloud_identity_onto_an_existing_row(real_db):
    local_id = models.ObservationDB.create_observation(
        date="2026-09-15", genus="Pholiotina", species="rugosa",
    )
    cloud_sync._apply_remote_observation_fields(local_id, _remote_917())
    identity = _identity(local_id)
    assert identity.sporely_taxon_id == 83668
    assert identity.identity_proof == PROOF_CLOUD_SELECTED_UNVERIFIED


def test_partial_field_apply_leaves_identity_alone(real_db):
    local_id = models.ObservationDB.create_observation(
        date="2026-09-15", genus="Conocybe", species="rugosa",
        scientific_name_snapshot="Conocybe rugosa", taxon_rank_snapshot="species",
        **TaxonIdentity.from_taxonomy_v2_artifact(83668, scientific_name="Conocybe rugosa").to_row(),
    )
    cloud_sync._apply_remote_observation_fields(
        local_id, _remote_917(selected_sporely_taxon_id=None, notes="x"), fields={"notes"},
    )
    assert _identity(local_id).is_proven_sporely


def test_row_without_identity_columns_never_touches_local_identity(real_db):
    local_id = models.ObservationDB.create_observation(
        date="2026-09-15", genus="Conocybe", species="rugosa",
        scientific_name_snapshot="Conocybe rugosa", taxon_rank_snapshot="species",
        **TaxonIdentity.from_taxonomy_v2_artifact(83668, scientific_name="Conocybe rugosa").to_row(),
    )
    remote = {k: v for k, v in _remote_917().items()
              if k != "selected_sporely_taxon_id" and not k.startswith("taxon_identity_")}
    cloud_sync._apply_remote_observation_fields(local_id, remote)
    assert _identity(local_id).is_proven_sporely


def test_full_apply_never_downgrades_an_identity_the_cloud_agrees_with(real_db):
    """Local picker-proven 83668 and cloud 83668: nothing to write, and
    rewriting would replace artifact proof with the weaker cloud token."""
    local_id = models.ObservationDB.create_observation(
        date="2026-09-15", genus="Conocybe", species="rugosa",
        scientific_name_snapshot="Conocybe rugosa", taxon_rank_snapshot="species",
        **TaxonIdentity.from_taxonomy_v2_artifact(83668, scientific_name="Conocybe rugosa").to_row(),
    )
    cloud_sync._apply_remote_observation_fields(local_id, _remote_917())
    identity = _identity(local_id)
    assert identity.is_proven_sporely
    assert identity.identity_proof == "taxonomy_v2_artifact"


def _insert_legacy_row(db_path, *, genus, species, sporely_taxon_id) -> int:
    """A pre-Stage-2 row: a bare integer with no provenance (the persistence
    API refuses to write these now, so seed it directly)."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(
            "INSERT INTO observations (date, genus, species, sporely_taxon_id) VALUES (?, ?, ?, ?)",
            ("2026-09-15", genus, species, sporely_taxon_id),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def test_cloud_without_identity_does_not_erase_local_evidence_for_the_same_taxon(real_db):
    local_id = _insert_legacy_row(real_db, genus="Conocybe", species="rugosa", sporely_taxon_id=52369)
    cloud_sync._apply_remote_observation_fields(
        local_id, _remote_917(selected_sporely_taxon_id=None, taxon_identity_state=None),
    )
    identity = _identity(local_id)
    assert identity.is_legacy_unverified and identity.sporely_taxon_id == 52369


def test_cloud_reidentification_without_identity_clears_the_stale_local_one(real_db):
    local_id = _insert_legacy_row(real_db, genus="Pholiotina", species="rugosa", sporely_taxon_id=52369)
    cloud_sync._apply_remote_observation_fields(
        local_id, _remote_917(selected_sporely_taxon_id=None, taxon_identity_state=None),
    )
    row = models.ObservationDB.get_observation(local_id)
    assert row["sporely_taxon_id"] is None
    assert (row["genus"], row["species"]) == ("Conocybe", "rugosa")


def test_legacy_local_52369_is_replaced_by_the_cloud_canonical_identity(real_db):
    """The existing-device 917 case: legacy NorTaxa integer locally, cloud
    now holds the reviewed canonical concept."""
    local_id = _insert_legacy_row(real_db, genus="Pholiotina", species="rugosa", sporely_taxon_id=52369)
    cloud_sync._apply_remote_observation_fields(local_id, _remote_917())
    identity = _identity(local_id)
    assert identity.sporely_taxon_id == 83668
    assert identity.identity_proof == PROOF_CLOUD_SELECTED_UNVERIFIED


# ── Select columns ───────────────────────────────────────────────────────────


class _ObservationReadClient(cloud_sync.SporelyCloudClient):
    """Records reads; optionally behaves like a server without the columns."""

    def __init__(self, *, supports_identity_columns: bool):
        self.user_id = "user-1"
        self.paths: list[str] = []
        self._supported = supports_identity_columns

    def _get(self, path):
        self.paths.append(path)
        if "taxon_identity_state" in path and not self._supported:
            raise cloud_sync.CloudSyncError(
                "GET failed (400): column observations.taxon_identity_state does not exist"
            )
        return [{"id": "14"}]

    _get_paginated = _get


def test_reads_carry_identity_columns_without_an_extra_request():
    client = _ObservationReadClient(supports_identity_columns=True)
    assert client.get_observation("14") == {"id": "14"}
    assert len(client.paths) == 1
    assert "taxon_identity_state" in client.paths[0]
    assert "user_id=eq.user-1" in client.paths[0]


def test_server_without_the_columns_costs_one_retry_then_is_remembered():
    client = _ObservationReadClient(supports_identity_columns=False)
    assert client.list_remote_observations() == [{"id": "14"}]
    assert client.pull_web_observations() == [{"id": "14"}]
    first_retry, *rest = client.paths[1:]
    assert "taxon_identity_state" not in first_retry
    assert "selected_sporely_taxon_id" in first_retry
    assert all("taxon_identity_state" not in path for path in rest)
    assert len(client.paths) == 3


def test_unrelated_read_errors_are_not_swallowed():
    class _Failing(_ObservationReadClient):
        def _get(self, path):
            raise cloud_sync.CloudSyncError("GET failed (500): boom")

    with pytest.raises(cloud_sync.CloudSyncError):
        _Failing(supports_identity_columns=True).get_observation("14")


# ── Installed artifact accessor ──────────────────────────────────────────────


def _artifact(tmp_path, *, schema_version="2", release=RELEASE) -> str:
    path = tmp_path / "taxonomy.sqlite3"
    conn = sqlite3.connect(path)
    conn.executescript(
        "CREATE TABLE taxonomy_meta (key TEXT PRIMARY KEY, value TEXT);"
        "CREATE TABLE taxon_min (taxon_id INTEGER PRIMARY KEY, "
        "canonical_scientific_name TEXT, taxon_rank TEXT);"
        "INSERT INTO taxon_min VALUES (83668, 'Conocybe rugosa', 'SPECIES');"
    )
    conn.execute("INSERT INTO taxonomy_meta VALUES ('taxonomy_schema_version', ?)", (schema_version,))
    if release:
        conn.execute("INSERT INTO taxonomy_meta VALUES ('content_release_id', ?)", (release,))
    conn.commit()
    conn.close()
    return str(path)


def test_installed_concept_reads_canonical_name_rank_and_release(tmp_path):
    concept = installed_taxon_concept(_artifact(tmp_path), 83668)
    assert concept == InstalledTaxonConcept(83668, RELEASE, "Conocybe rugosa", "species")


@pytest.mark.parametrize("kwargs, taxon_id", [
    ({}, 52369),                          # not in the artifact
    ({"schema_version": "1"}, 83668),     # not a taxonomy-v2 artifact
    ({"release": None}, 83668),           # nothing to say which release
])
def test_installed_concept_refuses_what_it_cannot_confirm(tmp_path, kwargs, taxon_id):
    assert installed_taxon_concept(_artifact(tmp_path, **kwargs), taxon_id) is None


def test_installed_concept_with_no_database_is_none(tmp_path):
    assert installed_taxon_concept(None, 83668) is None
    assert installed_taxon_concept(tmp_path / "missing.sqlite3", 83668) is None


# ── A cloud selection the desktop can re-derive from its provider tuple ─────
#
# A web/Android client that resolved Artsorakel's NBIC:57217 writes the
# selected Sporely ID together with the provider tuple it came from. When the
# installed artifact resolves that tuple to the same concept, the desktop has
# proven the identity itself (owner's observation 1044, cloud 1513).


def _remote_resolved(**overrides) -> dict:
    return _remote_917(**{
        "taxon_identity_source_system": "nortaxa",
        "taxon_identity_namespace": "nortaxa_taxon_id",
        "taxon_identity_external_id": "57217",
        "taxon_identity_raw_external_id": "NBIC:57217",
        **overrides,
    })


@pytest.fixture
def resolver(monkeypatch):
    """The installed artifact's authoritative resolver, recording its calls."""
    import database.taxon_lookup as taxon_lookup
    from utils import vernacular_utils

    calls = []
    answer = {"taxon_id": 83668}

    def resolve(db_path, *, source_system, namespace, external_id):
        calls.append((source_system, namespace, external_id))
        taxon_id = answer["taxon_id"]
        return (
            InstalledTaxonConcept(taxon_id, RELEASE, "Conocybe rugosa", "species")
            if taxon_id is not None else None
        )

    monkeypatch.setattr(taxon_lookup, "resolve_installed_external_identity", resolve)
    monkeypatch.setattr(vernacular_utils, "resolve_vernacular_db_path", lambda: "taxonomy.sqlite3")
    return SimpleNamespace(calls=calls, answer=answer)


def test_a_cloud_selection_its_own_tuple_resolves_to_is_proven(real_db, resolver):
    local_id = cloud_sync._create_local_from_remote(_remote_resolved())
    identity = _identity(local_id)
    assert identity.is_proven_sporely
    assert identity.sporely_taxon_id == 83668
    assert identity.identity_proof == PROOF_EXTERNAL_ID_RESOLUTION
    assert (identity.source_system, identity.namespace, identity.external_id, identity.raw_external_id) == (
        "nortaxa", "nortaxa_taxon_id", "57217", "NBIC:57217")
    assert resolver.calls == [("nortaxa", "nortaxa_taxon_id", "57217")]


@pytest.mark.parametrize("resolved_to", [None, 98530])
def test_a_tuple_that_does_not_resolve_to_the_selection_stays_unverified(real_db, resolver, resolved_to):
    resolver.answer["taxon_id"] = resolved_to
    local_id = cloud_sync._create_local_from_remote(_remote_resolved())
    identity = _identity(local_id)
    assert identity.sporely_taxon_id == 83668
    assert identity.identity_proof == PROOF_CLOUD_SELECTED_UNVERIFIED


def test_a_sporely_namespace_tuple_is_not_independent_evidence(real_db, resolver):
    local_id = cloud_sync._create_local_from_remote(_remote_resolved(
        taxon_identity_source_system="sporely", taxon_identity_namespace="sporely_taxon_id",
        taxon_identity_external_id="83668", taxon_identity_raw_external_id=None,
    ))
    assert _identity(local_id).identity_proof == PROOF_CLOUD_SELECTED_UNVERIFIED
    assert resolver.calls == []


def test_the_provider_tuple_never_changes_the_identity_sync_key():
    from utils.cloud_sync_impl.reconciliation.identity import _remote_identity_claim

    assert _remote_identity_claim(_remote_resolved()).key == _remote_identity_claim(_remote_917()).key


def test_an_unverified_local_selection_is_raised_when_the_cloud_proves_it(real_db, resolver):
    local_id = cloud_sync._create_local_from_remote(_remote_917())
    assert _identity(local_id).identity_proof == PROOF_CLOUD_SELECTED_UNVERIFIED
    cloud_sync._apply_remote_observation_fields(local_id, _remote_resolved())
    identity = _identity(local_id)
    assert identity.identity_proof == PROOF_EXTERNAL_ID_RESOLUTION
    assert identity.sporely_taxon_id == 83668


def test_a_picker_proven_local_identity_is_not_replaced_by_the_cloud_proof(real_db, resolver):
    local_id = models.ObservationDB.create_observation(
        date="2026-09-15", genus="Conocybe", species="rugosa",
        scientific_name_snapshot="Conocybe rugosa", taxon_rank_snapshot="species",
        **TaxonIdentity.from_taxonomy_v2_artifact(83668, scientific_name="Conocybe rugosa").to_row(),
    )
    cloud_sync._apply_remote_observation_fields(local_id, _remote_resolved())
    assert _identity(local_id).identity_proof == "taxonomy_v2_artifact"
    assert resolver.calls == []
