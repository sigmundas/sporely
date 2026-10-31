"""Exact-name associations: rules, blocks, projection and reversal."""
import copy
import gzip
import hashlib
import json
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import name_associations as na  # noqa: E402
from build_sqlite_candidate import CREATE_SCHEMA_SQL  # noqa: E402

POLICY = {
    "format": na.POLICY_FORMAT,
    "policy_version": "name-association-test",
    "exclusions": [],
}

# taxon_id, name, rank, canonical source, canonical external id, family
CONCEPTS = [
    (10, "Russula aeruginea", "species", "col_xr", "C10", "Russulaceae"),
    (20, "Russula aeruginea", "species", "nortaxa", "500", "Russulaceae"),
    (11, "Calocybe carnea", "species", "col_xr", "C11", "Lyophyllaceae"),
    (21, "Rugosomyces carneus", "species", "nortaxa", "501", "Lyophyllaceae"),
    (12, "Flammulina fennae", "species", "col_xr", "C12", "Physalacriaceae"),
    (13, "Platygloea disciformis", "species", "col_xr", "C13", "Platygloeaceae"),
    (14, "Achroomyces tiliae", "species", "col_xr", "C14", "Platygloeaceae"),
    (23, "Achroomyces disciformis", "species", "nortaxa", "503", "Platygloeaceae"),
    (15, "Hygrocybe conica", "species", "col_xr", "C15", "Hygrophoraceae"),
    (25, "Hygrocybe conica coll.", "species", "nortaxa", "505", "Hygrophoraceae"),
    (16, "Lycoperdon", "genus", "col_xr", "C16", "Lycoperdaceae"),
    (17, "Lycoperdon", "genus", "col_xr", "C17", "Agaricaceae"),
    (26, "Lycoperdon", "genus", "nortaxa", "506", "Lycoperdaceae"),
    (18, "Panaeolina foenisecii", "species", "col_xr", "C18", "Galeropsidaceae"),
    (28, "Panaeolina foenisecii", "species", "nortaxa", "508", "Bolbitiaceae"),
    (19, "Fomes fomentarius", "species", "col_xr", "C19", "Polyporaceae"),
]


def usage(source, identifier, name, *, rank="species", status=None, accepted=None,
          family="", authorship="Auth.", qualified=False):
    status = status or ("valid" if source == "nortaxa" else "accepted")
    return na.Usage(source=source, identifier=identifier, core_id=identifier, name=name,
                    authorship=authorship, rank=rank, status=status, accepted_id=accepted,
                    family=family, qualified=qualified)


def nortaxa_usages():
    return [
        usage("nortaxa", "500", "Russula aeruginea", family="Russulaceae"),
        usage("nortaxa", "501", "Rugosomyces carneus", family="Lyophyllaceae"),
        usage("nortaxa", "601", "Calocybe carnea", status="synonym", accepted="501"),
        usage("nortaxa", "502", "Flammulina fennae"),
        usage("nortaxa", "503", "Achroomyces disciformis"),
        usage("nortaxa", "603", "Platygloea disciformis", status="synonym", accepted="503"),
        usage("nortaxa", "604", "Achroomyces tiliae", status="synonym", accepted="503"),
        usage("nortaxa", "505", "Hygrocybe conica coll.", qualified=True),
        usage("nortaxa", "506", "Lycoperdon", rank="genus"),
        usage("nortaxa", "508", "Panaeolina foenisecii", family="Bolbitiaceae", authorship="(Peck) Maire"),
        usage("nortaxa", "509", "Fomes fomentarius"),
        usage("nortaxa", "609", "Polyporus fomentarius", status="synonym", accepted="509"),
    ]


def dyntaxa_usages():
    lsid = "urn:lsid:dyntaxa.se:Taxon:"
    return [
        usage("dyntaxa", lsid + "1", "Russula aeruginea"),
        usage("dyntaxa", lsid + "2", "Rugosomyces carneus"),
        usage("dyntaxa", lsid + "3", "Fomes fomentarius"),
        usage("dyntaxa", lsid + "4", "Ramaria s.str.", rank="genus", qualified=True),
    ]


@pytest.fixture
def base(tmp_path):
    conn = sqlite3.connect(tmp_path / "base.sqlite3")
    conn.executescript(CREATE_SCHEMA_SQL)
    for tid, name, rank, css, ext, family in CONCEPTS:
        genus, _, epithet = name.partition(" ")
        conn.execute(
            "INSERT INTO taxon_min (taxon_id, genus, specific_epithet, family, canonical_scientific_name, "
            "taxon_rank, taxonomic_status, source_system, canonical_source_system, canonical_external_id, "
            "norwegian_taxon_id, inaturalist_taxon_id, sporely_content_release_id) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?, 'tax-base')",
            (tid, genus, epithet, family, name, rank, "valid" if css == "nortaxa" else "accepted", css, css, ext,
             int(ext) if css == "nortaxa" else None, 900 + tid if css == "nortaxa" else None))
    conn.execute("INSERT INTO vernacular_min (taxon_id, language_code, vernacular_name, is_preferred_name, source) "
                 "VALUES (20, 'nb', 'grønnkremle', 1, 'nortaxa')")
    conn.execute("INSERT INTO taxon_external_id_min (taxon_id, source_system, external_id, id_role, is_preferred, "
                 "external_name, note) VALUES (20, 'artsdatabanken', 500, 'accepted', 0, 'Russula aeruginea', '')")
    # Fomes: a reviewed bridge already exists, NorTaxa 509 is authoritative on COL 19.
    conn.execute("INSERT INTO taxon_external_id_text_min (taxon_id, source_system, namespace, external_id, id_role, "
                 "is_preferred, external_name, note) VALUES (19, 'nortaxa', 'nortaxa_taxon_id', '509', 'accepted', 0, "
                 "'Fomes fomentarius', 'authoritative_bridge:reviewed_supersession')")
    conn.execute("INSERT INTO taxon_redlist_min (taxon_id, source_system, source_release, assessment_id, "
                 "assessment_area, assessed_name_source, assessed_name_namespace, assessed_name_id, "
                 "scientific_name_snapshot, category_raw, category_code) VALUES "
                 "(20, 'artsdatabanken_redlist', '2021', 'A1', 'Norge', 'artsdatabanken', "
                 "'artsnavnebase_scientific_name_id', '500', 'Russula aeruginea', 'LC', 'LC'), "
                 "(NULL, 'artsdatabanken_redlist', '2021', 'A2', 'Norge', 'artsdatabanken', "
                 "'artsnavnebase_scientific_name_id', '502', 'Flammulina fennae', 'NT', 'NT')")
    conn.execute("INSERT INTO taxonomy_meta VALUES ('content_release_id', 'tax-base')")
    conn.execute("INSERT INTO taxonomy_meta VALUES ('taxonomy_schema_version', '2')")
    conn.commit()
    yield conn
    conn.close()


def derive(conn, policy=POLICY, holds=frozenset()):
    usages = {"nortaxa": nortaxa_usages(), "dyntaxa": dyntaxa_usages()}
    return na.derive(conn, usages, policy, set(holds), {})


def by_member(derivation):
    return {a["association_id"]: a for a in derivation.associations}


def test_exact_name_associates_despite_family_and_authorship(base):
    assoc = by_member(derive(base))
    a = assoc["nortaxa:508"]
    assert (a["primary_taxon_id"], a["member_taxon_id"], a["rule"]) == (18, 28, na.EXACT)
    assert {"family_differs", "authorship_differs"} <= set(a["flags"])
    # A NorTaxa usage without its own concept attaches as a source usage.
    f = assoc["nortaxa:502"]
    assert (f["primary_taxon_id"], f["member_kind"], f["member_taxon_id"]) == (12, "source_usage", None)


def test_unique_synonym_bridges_a_genus_transfer(base):
    a = by_member(derive(base))["nortaxa:501"]
    assert (a["primary_taxon_id"], a["member_taxon_id"]) == (11, 21)
    assert (a["rule"], a["via_scientific_name"]) == (na.SYNONYM, "Calocybe carnea")


def test_genuine_ambiguity_blocks(base):
    d = derive(base)
    assoc = by_member(d)
    # One NorTaxa concept whose synonyms are two COL species: never merged.
    assert "nortaxa:503" not in assoc
    assert d.blocked[("nortaxa", "multiple_primary_candidates", "species_and_below")] == 1
    # Two COL genera named Lycoperdon.
    assert "nortaxa:506" not in assoc
    assert d.blocked[("nortaxa", "multiple_primary_candidates", "genus")] == 1
    # A qualified (coll.) concept stays separate.
    assert "nortaxa:505" not in assoc
    assert d.blocked[("nortaxa", "qualified_concept", "species_and_below")] == 1
    assert d.blocked[("dyntaxa", "qualified_concept", "genus")] == 1
    # An existing reviewed bridge is kept as is.
    assert "nortaxa:509" not in assoc
    assert d.blocked[("nortaxa", "already_bridged", "species_and_below")] == 1


def test_source_homonym_blocks(base):
    usages = {"nortaxa": nortaxa_usages() + [usage("nortaxa", "700", "Russula aeruginea")], "dyntaxa": []}
    d = na.derive(base, usages, POLICY, set(), {})
    assert "nortaxa:500" not in by_member(d) and "nortaxa:700" not in by_member(d)
    assert d.blocked[("nortaxa", "source_homonym", "species_and_below")] == 2


def test_dyntaxa_collapses_onto_the_nortaxa_primary(base):
    assoc = by_member(derive(base))
    lsid = "urn:lsid:dyntaxa.se:Taxon:"
    # Dyntaxa agrees with NorTaxa (Rugosomyces), COL says Calocybe: one taxon.
    assert assoc["dyntaxa:" + lsid + "2"]["primary_taxon_id"] == 11
    # Reached through the NorTaxa association, and labelled so.
    assert (assoc["dyntaxa:" + lsid + "2"]["rule"], assoc["dyntaxa:" + lsid + "2"]["via_scientific_name"]) == \
        (na.VIA_ASSOCIATION, "Rugosomyces carneus")
    assert assoc["dyntaxa:" + lsid + "1"]["rule"] == na.EXACT
    assert assoc["dyntaxa:" + lsid + "1"]["primary_taxon_id"] == 10
    # Dyntaxa is not blocked by NorTaxa's reviewed bridge on Fomes.
    assert assoc["dyntaxa:" + lsid + "3"]["primary_taxon_id"] == 19


def test_holds_and_exclusions_block(base):
    d = derive(base, holds={("nortaxa", "nortaxa_taxon_id", "500")})
    assert "nortaxa:500" not in by_member(d)
    policy = copy.deepcopy(POLICY)
    policy["exclusions"] = [{"member": {"source": "nortaxa", "namespace": "nortaxa_taxon_id", "identifier": "501"},
                             "reason": "test"}]
    d = derive(base, policy=policy)
    assert "nortaxa:501" not in by_member(d)
    assert d.blocked[("nortaxa", "reviewed_exclusion", "species_and_below")] == 1


def snapshot_ids(conn):
    return (conn.execute("SELECT taxon_id, canonical_scientific_name, canonical_external_id FROM taxon_min "
                         "ORDER BY taxon_id").fetchall(),
            conn.execute("SELECT * FROM taxon_supersession_min ORDER BY retired_taxon_id").fetchall())


def test_overlay_projects_without_touching_identity(base, tmp_path):
    before = snapshot_ids(base)
    d = derive(base)
    na.apply_overlay(base, d, release_id="tax-cand", policy=POLICY, norm_dir=tmp_path)
    assert snapshot_ids(base) == before
    rows = base.execute("SELECT member_taxon_id, primary_taxon_id FROM taxon_association_min "
                        "WHERE member_taxon_id IS NOT NULL ORDER BY 1").fetchall()
    assert rows == [(20, 10), (21, 11), (28, 18)]
    # NorTaxa ids now resolve to the primary through the namespaced table.
    text = base.execute("SELECT taxon_id, note FROM taxon_external_id_text_min "
                        "WHERE source_system='nortaxa' AND external_id='500'").fetchall()
    assert text == [(10, "name_association:exact_accepted_name;association=nortaxa:500")]
    syn = base.execute("SELECT taxon_id, id_role, note FROM taxon_external_id_text_min "
                       "WHERE source_system='nortaxa' AND external_id='601'").fetchall()
    assert syn == [(11, "synonym", "name_association:unique_synonym_bridge;association=nortaxa:501")]
    # A synonym-rule member's id is not mirrored into the integer table, where
    # legacy genus+species lookups would find two names for one concept.
    assert base.execute("SELECT count(*) FROM taxon_external_id_min WHERE external_id IN (501, 601)").fetchone() == (0,)
    # Names, vernaculars, Red List and the national display name follow.
    assert base.execute("SELECT vernacular_name FROM vernacular_min WHERE taxon_id=10").fetchall() == [("grønnkremle",)]
    assert base.execute("SELECT taxon_id FROM taxon_redlist_min WHERE assessment_id='A1'").fetchone() == (10,)
    assert base.execute("SELECT taxon_id FROM taxon_redlist_min WHERE assessment_id='A2'").fetchone() == (12,)
    assert base.execute("SELECT preferred_scientific_name_no, preferred_scientific_name_no_external_id, "
                        "preferred_scientific_name_sv FROM taxon_min WHERE taxon_id=11").fetchone() == \
        ("Rugosomyces carneus", "501", "Rugosomyces carneus")
    assert base.execute("SELECT inaturalist_taxon_id FROM taxon_min WHERE taxon_id=10").fetchone() == (920,)
    assert ("Rugosomyces carneus",) in base.execute(
        "SELECT scientific_name FROM scientific_name_min WHERE taxon_id=11").fetchall()


def test_build_is_reproducible_and_reversible(base, tmp_path):
    base.close()
    raw = (tmp_path / "base.sqlite3").read_bytes()
    gz = tmp_path / "base.sqlite3.gz"
    with gzip.GzipFile(gz, "wb", mtime=0) as fh:
        fh.write(raw)
    sha = hashlib.sha256(raw).hexdigest()
    norm = tmp_path / "norm"
    for source, rows in (("nortaxa", nortaxa_usages()), ("dyntaxa", dyntaxa_usages()), ("col_xr", [])):
        (norm / source).mkdir(parents=True)
        with open(norm / source / "taxa.jsonl", "w") as fh:
            for u in rows:
                fh.write(json.dumps({
                    "taxon_id": {"value": u.identifier}, "core_row_id": {"value": u.core_id},
                    "scientific_name": u.name,
                    "authorship": "auct." if u.qualified else u.authorship, "rank": u.rank,
                    "taxonomic_status": u.status, "source_code": source,
                    "accepted_name_usage_id": {"value": u.accepted_id} if u.accepted_id else None,
                    "classification": {"kingdom": "Fungi", "family": u.family}}) + "\n")
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(json.dumps(POLICY))

    def run(out):
        return na.build(base_gz=gz, base_sqlite_sha256=sha, norm_dir=norm, policy_path=policy_path,
                        holds_path=None, release_id="tax-2026.10.31-99", out_dir=tmp_path / out)

    first, second = run("a"), run("b")
    assert first["sqlite_sha256"] == second["sqlite_sha256"]
    assert first["associations_total"] > 0
    # Reversal: exclude one association, rebuild. The member is a plain live
    # concept again and the primary no longer carries its id; no id changed.
    policy = copy.deepcopy(POLICY)
    policy["exclusions"] = [{"member": {"source": "nortaxa", "namespace": "nortaxa_taxon_id", "identifier": "500"},
                             "reason": "different species"}]
    policy_path.write_text(json.dumps(policy))
    reversed_ = run("c")
    # The Dyntaxa usage of the same name now has two distinct candidates and
    # is blocked as ambiguous instead of silently following either side.
    assert reversed_["associations_total"] == first["associations_total"] - 2
    assert reversed_["blocked"]["dyntaxa|multiple_primary_candidates|species_and_below"] == 1
    conn = sqlite3.connect(tmp_path / "c" / "tax-2026.10.31-99.sqlite3")
    try:
        assert conn.execute("SELECT 1 FROM taxon_association_min WHERE member_taxon_id=20").fetchone() is None
        assert conn.execute("SELECT taxon_id FROM taxon_external_id_text_min WHERE external_id='500'").fetchall() == []
        assert conn.execute("SELECT taxon_id FROM taxon_redlist_min WHERE assessment_id='A1'").fetchone() == (20,)
        assert conn.execute("SELECT count(*) FROM taxon_min").fetchone()[0] == len(CONCEPTS)
    finally:
        conn.close()
    with pytest.raises(na.AssociationError):
        na.build(base_gz=gz, base_sqlite_sha256="0" * 64, norm_dir=norm, policy_path=policy_path,
                 holds_path=None, release_id="tax-2026.10.31-99", out_dir=tmp_path / "d")


def test_cloud_export_accepts_association_provenance_only(base, tmp_path):
    from database.taxonomy import cloud_export as ce

    na.apply_overlay(base, derive(base), release_id="tax-cand", policy=POLICY, norm_dir=tmp_path)
    base.execute("CREATE TEMP TABLE _cloud_export_scope (taxon_id INTEGER PRIMARY KEY)")
    base.execute("INSERT INTO _cloud_export_scope SELECT taxon_id FROM taxon_min")
    present = {r[1] for r in base.execute("PRAGMA table_info(taxon_min)")}
    ce._validate_national_names(base, present)
    base.execute("UPDATE taxon_external_id_text_min SET note = 'cross_source_automatic_exact' "
                 "WHERE taxon_id = 11 AND external_id = '501'")
    with pytest.raises(ce.ExportError, match="without an authoritative bridge"):
        ce._validate_national_names(base, present)


def test_synonym_ids_follow_an_already_bridged_usage(base, tmp_path):
    na.apply_overlay(base, derive(base), release_id="tax-cand", policy=POLICY, norm_dir=tmp_path)
    # Fomes: the NorTaxa usage was bridged by review; its old-name id now
    # resolves too, without any new association.
    assert base.execute("SELECT taxon_id, id_role, note FROM taxon_external_id_text_min "
                        "WHERE source_system='nortaxa' AND external_id='609'").fetchall() == \
        [(19, "synonym", "name_association:own_source_synonym")]


def test_held_usages_project_no_synonym_ids(base, tmp_path):
    d = derive(base, holds={("nortaxa", "nortaxa_taxon_id", "509")})
    na.apply_overlay(base, d, release_id="tax-cand", policy=POLICY, norm_dir=tmp_path)
    assert base.execute("SELECT count(*) FROM taxon_external_id_text_min WHERE external_id='609'").fetchone() == (0,)
