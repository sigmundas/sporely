"""Desktop behaviour on a release with name associations (``taxon_association_min``).

The artifact is built by the real overlay (``name_associations.derive`` +
``apply_overlay``) on a small base release, so search, provider resolution,
display, saving and the reversal preview all read what the compiler writes.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "database" / "taxonomy" / "scripts"))
import association_reversal_audit as reversal  # noqa: E402
import name_associations as na  # noqa: E402
from build_sqlite_candidate import CREATE_SCHEMA_SQL  # noqa: E402

from database.taxon_lookup import resolve_installed_external_identity  # noqa: E402
from database.taxon_search import search  # noqa: E402
from database.vernacular_db import VernacularDB  # noqa: E402
from utils.taxon_identity import TaxonIdentity  # noqa: E402

# taxon_id, name, rank, canonical source, canonical external id
CONCEPTS = [
    (10, "Russula aeruginea", "species", "col_xr", "C10"),
    (20, "Russula aeruginea", "species", "nortaxa", "500"),
    (11, "Calocybe carnea", "species", "col_xr", "C11"),
    (21, "Rugosomyces carneus", "species", "nortaxa", "501"),
    (12, "Flammulina fennae", "species", "col_xr", "C12"),
]
POLICY = {"format": na.POLICY_FORMAT, "policy_version": "test", "exclusions": []}


def usages():
    u = lambda i, n, **kw: na.Usage(source="nortaxa", identifier=i, core_id=i, name=n, authorship="A.",
                                    rank="species", status=kw.get("status", "valid"),
                                    accepted_id=kw.get("accepted"), family="", qualified=False)
    return {"nortaxa": [u("500", "Russula aeruginea"), u("501", "Rugosomyces carneus"),
                        u("601", "Calocybe carnea", status="synonym", accepted="501"),
                        u("502", "Flammulina fennae")],
            "dyntaxa": []}


def build(path: Path, exclusions=()) -> Path:
    conn = sqlite3.connect(path)
    conn.executescript(CREATE_SCHEMA_SQL)
    for tid, name, rank, css, ext in CONCEPTS:
        genus, _, epithet = name.partition(" ")
        conn.execute(
            "INSERT INTO taxon_min (taxon_id, genus, specific_epithet, canonical_scientific_name, taxon_rank, "
            "taxonomic_status, source_system, canonical_source_system, canonical_external_id, norwegian_taxon_id) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (tid, genus, epithet, name, rank, "valid" if css == "nortaxa" else "accepted", css, css, ext,
             int(ext) if css == "nortaxa" else None))
        conn.execute("INSERT INTO scientific_name_min (taxon_id, language_code, scientific_name, "
                     "is_preferred_name, source) VALUES (?, 'sci', ?, 1, ?)", (tid, name, css))
    conn.executemany(
        "INSERT INTO vernacular_min (taxon_id, language_code, vernacular_name, is_preferred_name, source) "
        "VALUES (?, 'nb', ?, 1, 'nortaxa')", [(20, "grønnkremle"), (21, "rosa fagerhatt")])
    conn.executemany("INSERT INTO taxonomy_meta VALUES (?, ?)",
                     [("taxonomy_schema_version", "2"), ("content_release_id", path.stem)])
    policy = dict(POLICY, exclusions=[
        {"member": {"source": "nortaxa", "namespace": "nortaxa_taxon_id", "identifier": i}, "reason": "test"}
        for i in exclusions])
    na.apply_overlay(conn, na.derive(conn, usages(), policy, set(), {}), release_id=path.stem,
                     policy=policy, norm_dir=path.parent)
    conn.commit()
    conn.close()
    return path


@pytest.fixture
def release(tmp_path):
    return build(tmp_path / "tax-2026.10.31-01.sqlite3")


def rows(db: Path, query: str, **kw):
    with sqlite3.connect(db) as conn:
        return search(conn, query, language_codes=("nb",), **kw).rows


def test_one_entry_per_associated_species(release):
    found = rows(release, "Russula aeruginea")
    assert [(r.name, [c.sporely_taxon_id for c in r.concepts]) for r in found] == [("Russula aeruginea", [10])]
    assert not found[0].has_source_choice
    # A vernacular only the NorTaxa concept carried finds the one entry too.
    assert [(r.name, r.concepts[0].sporely_taxon_id) for r in rows(release, "grønnkremle")] == \
        [("Russula aeruginea", 10)]


def test_national_name_is_a_synonym_entry_that_records_its_association(release):
    found = rows(release, "Rugosomyces")
    assert [(r.name, r.kind, r.concepts[0].sporely_taxon_id) for r in found] == \
        [("Rugosomyces carneus", "synonym", 11)]
    suggestion = found[0].default_concept.as_scientific_suggestion()
    assert suggestion["provenance"] == "name_association:nortaxa:501"
    # COL's name finds the same single concept.
    assert [r.concepts[0].sporely_taxon_id for r in rows(release, "Calocybe carnea")] == [11]


def test_a_member_as_current_identity_boosts_its_primary(release):
    found = rows(release, "Russula", current_taxon_id=20)
    assert found[0].default_concept.sporely_taxon_id == 10 and found[0].default_concept.is_current


def test_provider_ids_resolve_to_the_primary_with_association_provenance(release):
    for external_id, expected in (("500", 10), ("501", 11), ("601", 11), ("502", 12)):
        concept = resolve_installed_external_identity(
            release, source_system="nortaxa", namespace="nortaxa_taxon_id", external_id=external_id)
        assert concept is not None and concept.sporely_taxon_id == expected, external_id
        assert "name_association:" in concept.resolved_through
    assert "association=nortaxa:502" in resolve_installed_external_identity(
        release, source_system="nortaxa", namespace="nortaxa_taxon_id", external_id="502").resolved_through


def test_member_ids_display_as_the_primary(release):
    names = VernacularDB(release).display_scientific_names([20, 21, 10])
    assert names[20] == names[10]
    assert names[21][0] == "Calocybe carnea"


def save(conn, oid, identity: TaxonIdentity, name: str):
    row = identity.to_row()
    conn.execute(
        "INSERT INTO observations (id, genus, species, scientific_name_snapshot, sporely_taxon_id, "
        "taxon_identity_state, taxon_identity_proof, taxon_identity_source_system, taxon_identity_namespace, "
        "taxon_identity_external_id, taxon_identity_provenance) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (oid, *name.split(" ", 1), name, row["sporely_taxon_id"], row["taxon_identity_state"],
         row["taxon_identity_proof"], row["taxon_identity_source_system"], row["taxon_identity_namespace"],
         row["taxon_identity_external_id"], row["taxon_identity_provenance"]))


def test_reversal_recovers_the_member_identity(tmp_path, release):
    obs_db = tmp_path / "mushrooms.db"
    with sqlite3.connect(obs_db) as conn:
        conn.execute(
            "CREATE TABLE observations (id INTEGER PRIMARY KEY, genus TEXT, species TEXT, "
            "scientific_name_snapshot TEXT, sporely_taxon_id INTEGER, taxon_identity_state TEXT, "
            "taxon_identity_proof TEXT, taxon_identity_source_system TEXT, taxon_identity_namespace TEXT, "
            "taxon_identity_external_id TEXT, taxon_identity_provenance TEXT)")
        # 1: picked "Rugosomyces carneus" in the unified picker (bound to COL 11).
        pick = rows(release, "Rugosomyces")[0].default_concept.as_scientific_suggestion()
        save(conn, 1, TaxonIdentity.from_taxonomy_v2_artifact(
            pick["sporely_taxon_id"], scientific_name=pick["scientific_name"], rank="species",
            provenance=pick["provenance"]), "Rugosomyces carneus")
        # 2: Artsorakel NBIC:54697-style id for Flammulina fennae (no NorTaxa concept).
        provider = TaxonIdentity.from_prefixed_external_id("NBIC:502", scientific_name="Flammulina fennae",
                                                           rank="species")
        concept = resolve_installed_external_identity(
            release, source_system=provider.source_system, namespace=provider.namespace,
            external_id=provider.external_id)
        save(conn, 2, provider.resolved_to(concept.sporely_taxon_id, provenance=(
            f"taxonomy_v2:{concept.release_id}:{concept.resolved_through}")), "Flammulina fennae")
        # 3: an older observation bound to the NorTaxa concept itself.
        save(conn, 3, TaxonIdentity.from_taxonomy_v2_artifact(20, scientific_name="Russula aeruginea",
                                                              rank="species"), "Russula aeruginea")
        # 4: an ordinary COL pick with no association.
        save(conn, 4, TaxonIdentity.from_taxonomy_v2_artifact(10, scientific_name="Russula aeruginea",
                                                              rank="species"), "Russula aeruginea")
    (tmp_path / "rev").mkdir()
    reversed_release = build(tmp_path / "rev" / "tax-2026.11.01-01.sqlite3", exclusions=("501", "502"))
    before = sqlite3.connect(obs_db).execute("SELECT * FROM observations ORDER BY id").fetchall()
    report = reversal.audit(obs_db, release, reversed_release)
    assert sqlite3.connect(obs_db).execute("SELECT * FROM observations ORDER BY id").fetchall() == before
    assert report["dropped_associations"] == 2
    assert report["counts"]["bound_to_member_unaffected"] == 1
    proposals = {a["observation_id"]: a["proposal"] for a in report["affected"]}
    assert proposals[1] == {"action": "rebind_to_member_concept", "sporely_taxon_id": 21}
    assert proposals[2]["action"] == "keep_provider_tuple_unresolved"
    assert proposals[2]["provider_tuple"] == ["nortaxa", "nortaxa_taxon_id", "502"]
    assert set(proposals) == {1, 2}
    # After the reversal the two concepts are separate entries again.
    assert sorted(r.concepts[0].sporely_taxon_id for r in rows(reversed_release, "Rugosomyces")) == [21]
    assert [c.sporely_taxon_id for c in rows(reversed_release, "Russula aeruginea")[0].concepts] == [10]
