"""Source identity of an association-bound observation across desktop → cloud → desktop.

A provider id (an Artsorakel ``NBIC:`` value bridged to NorTaxa) that a
release name association resolves to the COL primary reaches the cloud with
its tuple, through ``set_observation_identification_v2``. A second desktop
verifies the tuple against its own release and keeps it, verified or not, so
a later release can reverse the association. A picker selection carries no
provider id and none is invented; a reversal leaves it unified and flags it.

The fake cloud applies the production RPC bodies (sporely-web migrations
20260922130000 and 20260922140000): active-release membership, and
``set_observation_selected_taxon_v2`` leaving the tuple columns as they are.
"""
from __future__ import annotations

import sqlite3

import pytest

from database import models, schema
from database.taxon_lookup import resolve_installed_external_identity
from tests.test_live_provider_identity_resolution import _push_client
from tests.test_taxon_name_associations import build, reversal, rows
from utils import cloud_sync, vernacular_utils
from utils.cloud_sync_impl.errors import CloudWriteRejectedError
from utils.cloud_sync_impl.identity_state import _apply_remote_identity_to_local
from utils.taxon_identity import TaxonIdentity

CLOUD_RELEASE = {10, 11, 12}  # the scoped cloud release holds the COL concepts


class FakeCloud:
    def __init__(self, remote: dict):
        self.remote, self.rpcs = dict(remote), []

    def rpc(self, name, payload):
        self.rpcs.append((name, dict(payload)))
        taxon_id = payload["p_sporely_taxon_id"]
        if taxon_id is not None and taxon_id not in CLOUD_RELEASE:
            raise CloudWriteRejectedError(f"RPC {name} failed", status_code=400, code="22023",
                                          message=f"sporely_taxon_id {taxon_id} is missing from the "
                                                  "active taxonomy-v2 release")
        if name == "set_observation_selected_taxon_v2":
            self.remote.update(selected_sporely_taxon_id=taxon_id, taxon_identity_state="sporely_v2")
        elif name == "set_observation_identification_v2":
            self.remote.update(
                selected_sporely_taxon_id=taxon_id, taxon_identity_state=payload["p_identity_state"],
                taxon_identity_source_system=payload["p_source_system"],
                taxon_identity_namespace=payload["p_namespace"],
                taxon_identity_external_id=payload["p_external_id"],
                taxon_identity_raw_external_id=payload["p_raw_external_id"])

    def push(self, local_row: dict):
        client = _push_client()
        client._resolve_existing_observation_for_push = lambda _obs, remote_obs=None: str(self.remote["id"])
        client._rpc = self.rpc
        client.push_observation(dict(local_row), remote_obs=dict(self.remote))
        assert not any(k.startswith("taxon_identity") or k == "selected_sporely_taxon_id"
                       for _path, payload in client.patches for k in payload)


def _remote(name: str, **identity) -> dict:
    genus, species = name.split(" ", 1)
    return {"id": 4242, "date": "2026-10-31", "genus": genus, "species": species,
            "selected_sporely_taxon_id": None, "taxon_identity_state": None,
            "taxon_identity_source_system": None, "taxon_identity_namespace": None,
            "taxon_identity_external_id": None, "taxon_identity_raw_external_id": None, **identity}


def _provider_identity(release, raw: str, name: str) -> TaxonIdentity:
    """What the desktop commits for a provider id (``commit_provider_identity``)."""
    provider = TaxonIdentity.from_prefixed_external_id(raw, scientific_name=name, rank="species")
    concept = resolve_installed_external_identity(
        release, source_system=provider.source_system, namespace=provider.namespace,
        external_id=provider.external_id)
    return provider.resolved_to(concept.sporely_taxon_id,
                                provenance=f"taxonomy_v2:{concept.release_id}:{concept.resolved_through}")


def _desktop_row(identity: TaxonIdentity, name: str) -> dict:
    genus, species = name.split(" ", 1)
    return {"id": 1, "cloud_id": "4242", "date": "2026-10-31", "genus": genus, "species": species,
            "scientific_name_snapshot": name, "taxon_rank_snapshot": "species", **identity.to_row()}


@pytest.fixture
def releases(tmp_path):
    release = build(tmp_path / "tax-2026.10.31-01.sqlite3")
    (tmp_path / "rev").mkdir()
    reversed_release = build(tmp_path / "rev" / "tax-2026.11.01-01.sqlite3", exclusions=("500", "501", "502"))
    return release, reversed_release


@pytest.fixture
def second_desktop(tmp_path, monkeypatch, releases):
    """A fresh desktop profile; ``use(release)`` picks its installed taxonomy."""
    db_path = tmp_path / "second" / "sporely.db"
    db_path.parent.mkdir()
    monkeypatch.setattr(schema, "get_database_path", lambda: db_path)
    monkeypatch.setattr(schema, "get_reference_database_path", lambda: tmp_path / "second" / "ref.db")
    monkeypatch.setattr(schema, "get_bundled_reference_database_path", lambda: tmp_path / "second" / "bundled.db")
    monkeypatch.setattr(schema, "_migrate_reference_values", lambda *a, **k: None)
    monkeypatch.setattr(schema, "_migrate_reference_mounts_and_stains", lambda *a, **k: None)
    schema.init_database()
    monkeypatch.setattr(cloud_sync, "_import_remote_images",
                        lambda *a, **k: {"imported": 0, "warnings": [], "errors": [], "complete": True})
    monkeypatch.setattr(cloud_sync, "_import_remote_measurements_for_observation",
                        lambda *a, **k: {"warnings": []})

    def use(release):
        monkeypatch.setattr(vernacular_utils, "resolve_vernacular_db_path", lambda *_a, **_k: release)

    use(releases[0])
    return db_path, use


def _identity(local_id) -> TaxonIdentity:
    return TaxonIdentity.from_row(models.ObservationDB.get_observation(local_id))


@pytest.mark.parametrize("raw, name, primary, member", [
    ("NBIC:502", "Flammulina fennae", 12, None),       # a NorTaxa usage with no concept (cf. observation 966)
    ("NBIC:500", "Russula aeruginea", 10, 20),         # a NorTaxa concept presented as the COL species
])
def test_a_provider_identity_reaches_the_second_desktop_with_its_source(
        releases, second_desktop, raw, name, primary, member):
    release, reversed_release = releases
    db_path, _use = second_desktop
    identity = _provider_identity(release, raw, name)
    assert identity.sporely_taxon_id == primary and "association=" in identity.provenance

    cloud = FakeCloud(_remote(name))
    cloud.push(_desktop_row(identity, name))
    assert cloud.rpcs == [("set_observation_identification_v2", {
        "p_observation_id": 4242, "p_sporely_taxon_id": primary, "p_identity_state": "sporely_v2",
        "p_source_system": "nortaxa", "p_namespace": "nortaxa_taxon_id", "p_external_id": raw[5:],
        "p_raw_external_id": raw, "p_write_name": False,
        "p_genus": None, "p_species": None, "p_common_name": None})]
    # The cloud keeps the name the desktop wrote; only identity columns moved.
    assert (cloud.remote["genus"], cloud.remote["species"]) == tuple(name.split(" ", 1))

    # The originating desktop pushes nothing more for the unchanged observation.
    cloud.push(_desktop_row(identity, name))
    assert len(cloud.rpcs) == 1

    local_id = cloud_sync._create_local_from_remote(dict(cloud.remote))
    received = _identity(local_id)
    assert (received.sporely_taxon_id, received.identity_proof) == (primary, "external_id_resolution")
    assert (received.source_system, received.namespace, received.external_id, received.raw_external_id) == \
        ("nortaxa", "nortaxa_taxon_id", raw[5:], raw)
    assert f"association=nortaxa:{raw[5:]}" in received.provenance

    # A second pull and a push from the receiving desktop change nothing.
    before = models.ObservationDB.get_observation(local_id)
    assert _apply_remote_identity_to_local(local_id, dict(cloud.remote)) == "unchanged"
    cloud.push({**models.ObservationDB.get_observation(local_id), "cloud_id": "4242"})
    assert len(cloud.rpcs) == 1
    assert models.ObservationDB.get_observation(local_id) == before

    # Reversing the association on the receiving desktop recovers the source.
    report = reversal.audit(db_path, release, reversed_release)
    (affected,) = [a for a in report["affected"] if a["observation_id"] == local_id]
    assert affected["attributed_by"] == "provenance"
    if member is None:
        assert affected["proposal"] == {"action": "keep_provider_tuple_unresolved", "sporely_taxon_id": None,
                                        "provider_tuple": ["nortaxa", "nortaxa_taxon_id", raw[5:]]}
    else:
        assert affected["proposal"] == {"action": "rebind_to_member_concept", "sporely_taxon_id": member}


def test_a_tuple_the_receiving_release_cannot_verify_is_kept_and_flagged(releases, second_desktop):
    """The second desktop already runs a release without the association: the
    cloud's NorTaxa tuple no longer names the selected COL species. The
    selection stays unified and unverified, the tuple is kept beside it and
    flagged, and nothing is rebound or pushed."""
    release, reversed_release = releases
    _db_path, use = second_desktop
    cloud = FakeCloud(_remote("Flammulina fennae"))
    cloud.push(_desktop_row(_provider_identity(release, "NBIC:502", "Flammulina fennae"), "Flammulina fennae"))

    use(reversed_release)
    local_id = cloud_sync._create_local_from_remote(dict(cloud.remote))
    received = _identity(local_id)
    assert (received.sporely_taxon_id, received.identity_proof) == (12, "cloud_selected_unverified")
    assert (received.source_system, received.namespace, received.external_id, received.raw_external_id) == \
        ("nortaxa", "nortaxa_taxon_id", "502", "NBIC:502")
    assert received.provenance.endswith("cloud_provider_tuple_unverified")

    before = models.ObservationDB.get_observation(local_id)
    assert _apply_remote_identity_to_local(local_id, dict(cloud.remote)) == "unchanged"
    cloud.push({**before, "cloud_id": "4242"})
    assert len(cloud.rpcs) == 1
    assert models.ObservationDB.get_observation(local_id) == before

    # The unverified tuple is a lead for review, not evidence a reversal acts on.
    report = reversal.audit(_db_path, release, reversed_release)
    assert not [a for a in report["affected"] if a["observation_id"] == local_id]
    (flagged,) = [u for u in report["unattributed"] if u["observation_id"] == local_id]
    assert flagged["proposal"]["action"] == "keep_unified_identity"
    assert flagged["unverified_provider_tuple"] == ["nortaxa", "nortaxa_taxon_id", "502"]


def test_a_picker_selection_carries_no_invented_source_and_reverses_flagged(releases, second_desktop):
    """Picking "Rugosomyces carneus" (presented as COL Calocybe carnea) is a
    name, not a provider id: the cloud gets the selection alone. The
    receiving desktop cannot attribute it, so a reversal keeps the unified
    identity and lists it as unattributed instead of guessing."""
    release, reversed_release = releases
    db_path, _use = second_desktop
    pick = rows(release, "Rugosomyces")[0].default_concept.as_scientific_suggestion()
    identity = TaxonIdentity.from_taxonomy_v2_artifact(
        pick["sporely_taxon_id"], scientific_name=pick["scientific_name"], rank="species",
        provenance=pick["provenance"])
    cloud = FakeCloud(_remote("Rugosomyces carneus"))
    cloud.push(_desktop_row(identity, "Rugosomyces carneus"))
    assert cloud.rpcs == [("set_observation_selected_taxon_v2",
                           {"p_observation_id": 4242, "p_sporely_taxon_id": 11})]
    assert cloud.remote["taxon_identity_source_system"] is None

    local_id = cloud_sync._create_local_from_remote(dict(cloud.remote))
    received = _identity(local_id)
    assert (received.sporely_taxon_id, received.source_system) == (11, "sporely")

    report = reversal.audit(db_path, release, reversed_release)
    assert not [a for a in report["affected"] if a["observation_id"] == local_id]
    (flagged,) = [u for u in report["unattributed"] if u["observation_id"] == local_id]
    assert flagged["proposal"] == {"action": "keep_unified_identity", "sporely_taxon_id": 11,
                                   "reason": "no stored source evidence names the association"}
    assert [a["association_id"] for a in flagged["dropped_associations"]] == ["nortaxa:501"]


def test_an_unchanged_cloud_selection_is_not_rewritten_to_add_the_tuple(releases):
    """A cloud row that already holds the selection (an existing observation
    synced before this change) is left as it is."""
    release, _reversed = releases
    cloud = FakeCloud(_remote("Flammulina fennae", selected_sporely_taxon_id=12,
                              taxon_identity_state="sporely_v2"))
    cloud.push(_desktop_row(_provider_identity(release, "NBIC:502", "Flammulina fennae"), "Flammulina fennae"))
    assert cloud.rpcs == []


def test_a_concept_the_cloud_lacks_still_preserves_its_tuple_unresolved(tmp_path):
    """A NorTaxa-only concept outside the cloud release (no association): the
    identification RPC refuses it like the selection RPC did, and the tuple
    is preserved as external_unresolved as before."""
    identity = TaxonIdentity.from_prefixed_external_id(
        "NBIC:52330", scientific_name="Panaeolina foenisecii", rank="species").resolved_to(
        624659, provenance="taxonomy_v2:tax-2026.10.31-01:taxon_min.canonical_source_usage")
    cloud = FakeCloud(_remote("Panaeolina foenisecii"))
    cloud.push(_desktop_row(identity, "Panaeolina foenisecii"))
    assert [name for name, _ in cloud.rpcs] == ["set_observation_identification_v2"] * 2
    assert (cloud.remote["selected_sporely_taxon_id"], cloud.remote["taxon_identity_state"],
            cloud.remote["taxon_identity_external_id"]) == (None, "external_unresolved", "52330")


def test_a_later_picker_selection_replaces_the_provider_tuple(releases, second_desktop):
    """After a provider-resolved push, picking another species writes that
    selection with its own Sporely tuple: the old NorTaxa id is not left
    paired with the new species, so the second desktop stores none."""
    release, _reversed = releases
    cloud = FakeCloud(_remote("Flammulina fennae"))
    cloud.push(_desktop_row(_provider_identity(release, "NBIC:502", "Flammulina fennae"), "Flammulina fennae"))
    picked = TaxonIdentity.from_taxonomy_v2_artifact(10, scientific_name="Russula aeruginea", rank="species")
    cloud.push(_desktop_row(picked, "Russula aeruginea"))
    assert cloud.rpcs[-1] == ("set_observation_identification_v2", {
        "p_observation_id": 4242, "p_sporely_taxon_id": 10, "p_identity_state": "sporely_v2",
        "p_source_system": "sporely", "p_namespace": "sporely_taxon_id", "p_external_id": "10",
        "p_raw_external_id": None, "p_write_name": False,
        "p_genus": None, "p_species": None, "p_common_name": None})

    received = _identity(cloud_sync._create_local_from_remote(dict(cloud.remote)))
    assert (received.sporely_taxon_id, received.source_system, received.external_id) == (10, "sporely", "10")
    assert "cloud_provider_tuple_unverified" not in (received.provenance or "")
