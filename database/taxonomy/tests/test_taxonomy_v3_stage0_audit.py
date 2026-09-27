"""Taxonomy v3 Stage 0 audit: population queries and manifest determinism.

The full audit runs against the tracked release and the gitignored source
archives; these tests pin the pieces that decide membership and fingerprints on
a tiny in-memory release.
"""
import importlib.util
import sqlite3
from pathlib import Path

_SCRIPT = (Path(__file__).resolve().parents[1] / "evidence" / "taxonomy-v3"
           / "audit_stage0_coverage.py")
_spec = importlib.util.spec_from_file_location("audit_stage0_coverage", _SCRIPT)
audit = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(audit)


def _release() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE taxon_min (taxon_id INTEGER PRIMARY KEY,
            canonical_scientific_name TEXT, canonical_source_system TEXT,
            canonical_external_id TEXT, norwegian_taxon_id INTEGER);
        CREATE TABLE taxon_external_id_min (taxon_id INTEGER,
            source_system TEXT, external_id INTEGER, id_role TEXT, note TEXT);
        CREATE TABLE taxon_external_id_text_min (taxon_id INTEGER,
            source_system TEXT, namespace TEXT, external_id TEXT,
            id_role TEXT, note TEXT);
        INSERT INTO taxon_min VALUES
            (1, 'Craterellus tubaeformis', 'col_xr', 'Z8TV', NULL),
            (2, 'Cantharellus cibarius', 'col_xr', 'QMKY', NULL),
            (3, 'Cantharellus cibarius', 'nortaxa', '56210', 56210),
            (4, 'Pholiotina vexans', 'nortaxa', '58766', 58766),
            (5, 'Entoloma conferendum', 'col_xr', '39ZCL', NULL),
            (6, 'Conocybe rugosa', 'col_xr', '5ZT3G', NULL);
        INSERT INTO taxon_external_id_min VALUES
            (1, 'artsdatabanken', 56227, 'accepted', 'cross_source_automatic_exact'),
            (1, 'artsdatabanken', 62407, 'synonym', 'synonym_of_accepted'),
            (5, 'artsdatabanken', 53482, 'accepted', 'manual_approved_exact'),
            (5, 'artsdatabanken', 59746, 'synonym', 'synonym_of_accepted'),
            (6, 'artsdatabanken', 52369, 'accepted', 'reviewed_supersession'),
            (6, 'artsdatabanken', 58722, 'synonym', 'synonym_of_accepted');
        INSERT INTO taxon_external_id_text_min VALUES
            (5, 'nortaxa', 'nortaxa_taxon_id', '53482', 'accepted',
             'authoritative_bridge:manual_approved_exact'),
            (6, 'nortaxa', 'nortaxa_taxon_id', '52369', 'accepted',
             'authoritative_bridge:reviewed_supersession'),
            (6, 'nortaxa', 'nortaxa_taxon_id', '58722', 'synonym',
             'authoritative_bridge:reviewed_supersession');
    """)
    return conn


def test_populations_are_separated():
    conn = _release()
    assert audit.group_a(conn) == [("56227", "Z8TV", 1)]
    # Name equality sizes Group B; a synonym-level duplicate is not in it.
    assert audit.group_b(conn) == [("56210", 3, "QMKY", 2)]
    assert [r["nortaxa_taxon_id"] for r in audit.reviewed_bridges(conn)] == [
        "52369", "53482"]


class _Policy:
    def rejection_reason(self, evidence_class):
        return "rejected:" + evidence_class


def test_emission_census_is_scoped_and_reconciles_raw_rows():
    conn = _release()
    # NorTaxa-canonical concepts 3 and 4 carry norwegian_taxon_id but are
    # outside the scope, like every NorTaxa concept in the cloud export.
    emitted = audit.authoritative_nortaxa_emission(conn, {1, 2, 5, 6})
    assert [(r["sporely_taxon_id"], r["nortaxa_taxon_id"]) for r in emitted] \
        == [(5, "53482"), (6, "52369"), (6, "58722")]
    rows = audit.reconcile_emitting_hosts(conn, emitted, _Policy())
    unpublished = [r for r in rows if not r["published"]]
    assert [(r["nortaxa_taxon_id"], r["rejection_reason"]) for r in unpublished] \
        == [("59746", "rejected:intra_source_synonym")]
    # A synonym re-keyed by a supersession is published despite its raw note.
    assert {r["nortaxa_taxon_id"]: r["raw_note"] for r in rows
            if r["published"]}["58722"] == "synonym_of_accepted"


def test_emission_census_fails_closed_on_unpublished_reviewed_row():
    conn = _release()
    conn.execute("DELETE FROM taxon_external_id_text_min "
                 "WHERE external_id = '52369'")
    emitted = audit.authoritative_nortaxa_emission(conn, {5, 6})
    try:
        audit.reconcile_emitting_hosts(conn, emitted, _Policy())
    except SystemExit as exc:
        assert "52369" in str(exc)
    else:
        raise AssertionError("unpublished reviewed row was accepted")


def _row(nortaxa_id, evidence_class, shared):
    row = {column: None for column in audit.MANIFEST_COLUMNS}
    row.update(nortaxa_taxon_id=nortaxa_id, col_usage_id="C" + nortaxa_id,
               sporely_taxon_id=int(nortaxa_id), evidence_class=evidence_class,
               shared_synonym_count=shared, in_cloud_scope=True)
    return row


def test_manifest_is_order_independent_and_class_scoped():
    rows = [_row("2", audit.EVIDENCE_SHARED_SYNONYMY, 3),
            _row("1", audit.EVIDENCE_SHARED_SYNONYMY, 1),
            _row("9", audit.EVIDENCE_NONE, 0)]
    pins = {"release": {"sqlite_sha256": "x"}}
    first = audit.build_manifest(audit.EVIDENCE_SHARED_SYNONYMY, rows, pins)
    second = audit.build_manifest(audit.EVIDENCE_SHARED_SYNONYMY,
                                  list(reversed(rows)), pins)
    assert first == second
    assert first["member_count"] == 2
    assert [m[0] for m in first["members"]] == ["1", "2"]
    assert first["review_status"] == "needs_review"


def _concept(name, authorship, rank, synonyms):
    import cross_reference_evidence  # on sys.path once the audit is loaded
    key = cross_reference_evidence._name_key
    return {"accepted": key(name, authorship), "rank": rank,
            "synonyms": {key(n, a) for n, a in synonyms}}


def _shared_row(bridge, backbone):
    return {"accepted_names_agree": bridge["accepted"] == backbone["accepted"],
            "bridge_synonym_count": len(bridge["synonyms"]),
            "backbone_synonym_count": len(backbone["synonyms"])}


def _review_class(bridge_synonyms, backbone_synonyms, *, name="Inocybe calospora",
                  authorship="Quél.", rank="species", backbone_authorship=None):
    bridge = _concept(name, authorship, rank, bridge_synonyms)
    backbone = _concept(name, authorship if backbone_authorship is None
                        else backbone_authorship, rank, backbone_synonyms)
    return audit.shared_review_class(_shared_row(bridge, backbone),
                                     bridge, backbone)


def test_synonym_kinds():
    accepted = {"inocybe undulatospora"}
    kind = audit.synonym_kind
    assert kind(("hyphoderma patricium", "ined."), accepted) == "ined"
    assert kind(("trichaptum prosector", "comb. ined."), accepted) == "ined"
    assert kind(("lecidea leprosa", "nom. herb."), accepted) == "invalid_name"
    assert kind(("inocybe undulatopspora", ""), accepted) \
        == "accepted_name_variant"
    assert kind(("inocybe undulatospora", "Kuyper ex X"), accepted) \
        == "accepted_name_variant"
    assert kind(("telamonia undulatospora", ""), accepted) == "unauthored"
    assert kind(("inocybe carpta", "Bres."), accepted) == "ordinary"


def test_shared_review_class_precedence():
    shared = [("Cantharellus infundibuliformis", "(Scop.) Fr."),
              ("Merulius cantharelloides", "(Bull.) Purton"),
              ("Merulius infundibularis", "Kuntze")]
    # Craterellus tubaeformis (NorTaxa 56227): three ordinary shared synonyms.
    assert _review_class(shared, shared + [("X y", "Z")] * 1,
                         name="Craterellus tubaeformis",
                         authorship="(Fr.) Quél.") == "ordinary"
    assert _review_class(shared, shared, rank="genus") == "non_species_rank"
    assert _review_class(shared, shared, rank="variety") == "infraspecific_rank"
    assert _review_class(shared, shared, backbone_authorship="") \
        == "accepted_authorship_disagrees"
    assert _review_class([("Hyphoderma patricium", "ined.")],
                         [("Hyphoderma patricium", "ined.")]) == "only_ined"
    assert _review_class([("Inocybe calosporra", "Quél.")],
                         [("Inocybe calosporra", "Quél.")]) \
        == "only_accepted_name_variant"
    assert _review_class([("Inocybella calospora", ""),
                          ("Inocybe calosporra", "")],
                         [("Inocybella calospora", ""),
                          ("Inocybe calosporra", "")]) == "only_mixed_weak"
    many = [(f"Agaricus s{i}", "Fr.") for i in range(5)]
    assert _review_class(shared[:1] + many,
                         shared[:1] + [(f"Agaricus t{i}", "Fr.")
                                       for i in range(5)]) \
        == "single_shared_synonym_low_overlap"
    # A weak synonym beside an ordinary one does not demote the association.
    assert _review_class(shared[:2] + [("Hyphoderma x", "ined.")],
                         shared[:2] + [("Hyphoderma x", "ined.")]) == "ordinary"


def test_review_manifests_partition_the_parent():
    rows = [_row("1", audit.EVIDENCE_SHARED_SYNONYMY, 3),
            _row("2", audit.EVIDENCE_SHARED_SYNONYMY, 1),
            _row("3", audit.EVIDENCE_NONE, 0)]
    rows[0]["review_class"], rows[1]["review_class"] = "ordinary", "only_ined"
    rows[1]["in_cloud_scope"] = False
    pins = {"release": {"sqlite_sha256": "x"}}
    parent = audit.build_manifest(audit.EVIDENCE_SHARED_SYNONYMY, rows, pins)
    split = {cls: audit.build_review_manifest(cls, rows, pins, parent)
             for cls in audit.SHARED_REVIEW_CLASSES}
    assert [m[0] for m in split["ordinary"]["members"]] == ["1"]
    assert [m[0] for m in split["only_ined"]["members"]] == ["2"]
    assert split["only_ined"]["cloud_scope_member_count"] == 0
    assert split["ordinary"]["parent_manifest"]["members_sha256"] \
        == parent["members_sha256"]
    assert sum(m["member_count"] for m in split.values()) \
        == parent["member_count"]
    # Distinct classes never share a members fingerprint unless both are empty.
    assert split["ordinary"]["members_sha256"] \
        != split["only_ined"]["members_sha256"]
    assert split["ordinary"]["review_status"] == "needs_review"


def test_summary_distributions():
    rows = [_row("1", audit.EVIDENCE_SHARED_SYNONYMY, 1),
            _row("2", audit.EVIDENCE_SHARED_SYNONYMY, 1),
            _row("3", audit.EVIDENCE_NONE, 0)]
    summary = audit.summarise(rows, taxon_key="sporely_taxon_id")
    assert summary["reviewable_total"] == 2
    assert summary["shared_synonym_count_distribution"][
        audit.EVIDENCE_SHARED_SYNONYMY] == {"1": 2}
