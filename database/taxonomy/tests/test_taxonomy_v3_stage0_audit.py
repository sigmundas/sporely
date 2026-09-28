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


def _key(name, authorship):
    import cross_reference_evidence  # on sys.path once the audit is loaded
    return cross_reference_evidence._name_key(name, authorship)


def _concept(name, authorship, rank, synonyms):
    """A read_nortaxa/read_col concept. ``synonyms`` entries are
    ``(name, authorship)``, ``(name, authorship, {(field, value), ...})`` or,
    for COL, ``(name, authorship, statuses, [usage, ...])`` with usages as
    ``_usage`` builds them."""
    concept = {"accepted": _key(name, authorship), "rank": rank,
               "synonyms": set(), "synonym_status": {}, "synonym_usages": {}}
    for entry in synonyms:
        key = _key(entry[0], entry[1])
        concept["synonyms"].add(key)
        concept["synonym_status"][key] = set(entry[2]) if len(entry) > 2 \
            else set()
        if len(entry) > 3:
            concept["synonym_usages"][key] = list(entry[3])
    return concept


#: The COL source the pinned archives identify as NorTaxa.
_NORTAXA_IN_COL = frozenset({"2030"})


def _usage(col_id, source_id, merged, remarks="", name_remarks=""):
    return {"col_usage_id": col_id, "source_id": source_id, "merged": merged,
            "remarks": remarks, "name_remarks": name_remarks}


def _shared_row(bridge, backbone):
    return {"accepted_names_agree": bridge["accepted"] == backbone["accepted"],
            "bridge_synonym_count": len(bridge["synonyms"]),
            "backbone_synonym_count": len(backbone["synonyms"])}


def _review_class(bridge_synonyms, backbone_synonyms, *, name="Inocybe calospora",
                  authorship="Quél.", rank="species", backbone_rank=None,
                  backbone_authorship=None):
    bridge = _concept(name, authorship, rank, bridge_synonyms)
    backbone = _concept(name, authorship if backbone_authorship is None
                        else backbone_authorship,
                        rank if backbone_rank is None else backbone_rank,
                        backbone_synonyms)
    return audit.shared_review_class(_shared_row(bridge, backbone),
                                     bridge, backbone)


def _kind(name, authorship, *, nortaxa=(), col=(), usages=(),
          accepted=("inocybe undulatospora",)):
    evidence = {"name": name, "authorship": authorship,
                "status": frozenset({("nortaxa", f, v) for f, v in nortaxa}
                                    | {("col", f, v) for f, v in col}),
                "col_usages": tuple(usages),
                "nortaxa_col_sources": _NORTAXA_IN_COL,
                "accepted": frozenset(accepted)}
    return audit.classify_synonym(evidence)[0]


_NT_SYN = ("taxonomicStatus", "synonym")
_COL_SYN = ("col:status", "synonym")


def _nt(status):
    return {_NT_SYN, ("nomenclaturalStatus", status)}


def _col(name_status="", status="synonym"):
    return {("col:status", status), ("col:nameStatus", name_status)}


def test_synonym_kinds():
    assert _kind("hyphoderma patricium", "ined.") == "unpublished"
    assert _kind("trichaptum prosector", "comb. ined.") == "unpublished"
    assert _kind("lecidea leprosa", "nom. herb.") == "unpublished"
    assert _kind("x y", "Z", col={("col:nameStatus", "manuscript")}) \
        == "unpublished"
    assert _kind("x y", "Lynge nom.nud.") == "not_validly_published"
    assert _kind("x y", "Motyka, nom. inval.") == "not_validly_published"
    assert _kind("x y", "Flagey, nom. illeg.") == "illegitimate"
    assert _kind("x y", "Fr. ss. Bres.") == "interpretation_qualified"
    assert _kind("x y", "s. Brandrud et al.") == "interpretation_qualified"
    assert _kind("x y", "auct. non Fr.") == "interpretation_qualified"
    assert _kind("x y", "Malme", col={("col:status", "misapplied")}) \
        == "interpretation_qualified"
    assert _kind("x y", "Fr.", col={("col:status", "ambiguous synonym")}) \
        == "pro_parte"
    assert _kind("x y", "Fr. p.p.") == "pro_parte"
    assert _kind("inocybe undulatospora", "Kuyper ex X") \
        == "accepted_name_reauthored"
    assert _kind("telamonia undulatospora", "") == "unauthored"
    # An unauthored near-spelling is unauthored: exact facts before heuristics.
    assert _kind("inocybe undulatopspora", "") == "unauthored"
    assert _kind("inocybe undulatopspora", "Kuyper") == "name_variant"
    # The heuristic no longer needs the genus to match ...
    assert _kind("inocyba undulatospora", "Kuyper") == "name_variant"
    # ... and a structured status beats any string heuristic.
    assert _kind("inocyba undulatospora", "Kuyper",
                 nortaxa={("nomenclaturalStatus", "orthographic")}) \
        == "orthographic_variant"
    assert _kind("inocybe carpta", "Bres.") == "ordinary"
    # Illegitimacy statuses are recorded but do not make a synonym weak.
    assert _kind("inocybe carpta", "Bres.",
                 nortaxa={("nomenclaturalStatus", "illegitimate")},
                 col={("col:nameStatus", "unacceptable")}) == "ordinary"


def test_col_remarks_carry_explicit_nomenclatural_warnings():
    def kind(remarks="", name_remarks=""):
        return _kind("inocybe carpta", "Bres.", usages=[
            _usage("U1", "2041", "true", remarks, name_remarks)])
    # 56896 / 3F9D5: COL RQ6NJ, Polyporus cupreolaccatus Kalchbr.
    assert kind("Publ.: Kalchbrenner, 1885. Öst. bot. Z. 35, not seen. Nom. "
                "illeg., acc to Ryvarden (1976). Check legitimacy!") \
        == "illegitimate"
    assert kind('"A later homonym to Cantharellus pallidus Yasuda (1917). "') \
        == "illegitimate"
    # 204742 / MD6D7 and 74124 / R3ZLN.
    assert kind('"nom. nud. "') == "not_validly_published"
    assert kind(name_remarks="nom. nud.") == "not_validly_published"
    assert kind("Name published without a valid description.") \
        == "not_validly_published"
    assert kind(name_remarks="Norman, ined.") == "unpublished"
    # Other remarks are not evidence either way.
    for remark in ("Check spelling!", "Erroneous author citation.",
                   "Publ.: Fries, T.M., 1871, not seen.",
                   '"non Arthopyrenia subfallax (Nyl.) Müll.Arg. "',
                   '"är möjligen en egen art "'):
        assert kind(remark) == "ordinary", remark


def test_col_assertions_from_nortaxa_are_not_independent():
    nortaxa = _usage("QNNBM", "2030", "true")
    # Only NorTaxa, republished through COL: one source, not two.
    assert _kind("inocybe carpta", "Bres.", usages=[nortaxa]) \
        == "nortaxa_derived"
    # A genuinely independent COL source counts, merged or not.
    for independent in (_usage("QMNY", "2073", "false"),
                        _usage("MD6D7", "2041", "true")):
        assert _kind("inocybe carpta", "Bres.", usages=[independent]) \
            == "ordinary"
        assert _kind("inocybe carpta", "Bres.",
                     usages=[nortaxa, independent]) == "ordinary"
    # A weak name stays labelled by its nomenclatural kind.
    assert _kind("inocybe carpta", "ined.", usages=[nortaxa]) == "unpublished"


def test_nortaxa_col_source_is_resolved_from_the_pinned_titles():
    sources = {"2030": {"title": "Nortaxa (Artsnavnebasen)"},
               "2073": {"title": "Species Fungorum Plus"}}
    assert audit.nortaxa_col_source("Nortaxa (Artsnavnebasen)", sources)[
        "col_source_id"] == "2030"
    for bad in ({"2073": sources["2073"]},
                {**sources, "9": {"title": "Nortaxa (Artsnavnebasen)"}}):
        try:
            audit.nortaxa_col_source("Nortaxa (Artsnavnebasen)", bad)
        except SystemExit:
            continue
        raise AssertionError(f"resolved NorTaxa from {bad}")


# Pinned NorTaxa 1.284 / COL XR 2026-07-17 shared synonyms, with the statuses
# both sources publish on them, for the associations the independent review
# of the first split named.
_CRATERELLUS = [
    ("Cantharellus infundibuliformis", "(Scop.) Fr.",
     _nt("illegitimate"), _col(), [_usage("QMNY", "2073", "false")]),
    ("Merulius cantharelloides", "(Bull.) Purton",
     _nt("illegitimate"), _col("unacceptable"),
     [_usage("QNNBM", "2030", "true")]),
    ("Merulius infundibularis", "Kuntze",
     _nt("illegitimate"), _col("unacceptable"),
     [_usage("QNPGH", "2030", "true")]),
]
_REVIEWED_CASES = {
    # nortaxa / col: (accepted name, authorship, shared synonyms, class)
    "59001/YKPL": ("Cortinarius diosmus", "Kühner", [
        ("Cortinarius argillaceosericeus", "Kytöv., Niskanen & Liimat.",
         _nt("notvalidlypublished"), _col("not established"))],
        "only_not_validly_published"),
    "62374/5WY75": ("Cantharellus borealis", "R.H. Petersen & Ryvarden", [
        ("Craterellus ryvardenii", "(R.H. Petersen & Ryvarden)",
         _nt("notvalidlypublished"), _col())],
        "only_not_validly_published"),
    "128412/STJ62": ("Scolicosporium betulae", "Rostr.", [
        ("Scolecosporium betulae", "Rostr.",
         _nt("orthographic"), _col("not established"))],
        "only_orthographic_variant"),
    "56689/4RSKZ": ("Repetobasidium vestitum", "J. Erikss. & Hjortstam", [
        ("Repetobasidiellum vestitum", "J. Erikss. & Hjortstam",
         _nt("orthographic"), _col("not established"))],
        "only_orthographic_variant"),
    "75928/76VPT": ("Phaeocalicium polyporaeum", "(Nyl.) Tibell", [
        ("Calicium polyporaceum", "", _nt(""), _col()),
        ("Phaeocalicium polyporaceum", "", _nt(""), _col())],
        "only_unauthored"),
    "170703/YLM6": ("Cortinarius nefastus", "Carteret & Reumaux", [
        ("Cortinarius holophaeus", "s. Brandrud et al.", _nt(""), _col())],
        "only_interpretation_qualified"),
    # One independently corroborated synonym while NorTaxa publishes 7 and
    # COL 51: the other two reach COL only from NorTaxa.
    "56227/Z8TV": ("Craterellus tubaeformis", "(Fr.) Quél.", _CRATERELLUS,
                   "single_shared_synonym_low_overlap", 7, 51),
    "56896/3F9D5": ("Ganoderma pfeifferi", "Bres.", [
        ("Polyporus cupreolaccatus", "Kalchbr.", _nt("illegitimate"),
         _col("acceptable"), [_usage(
             "RQ6NJ", "2041", "true",
             "Publ.: Kalchbrenner, 1885. Öst. bot. Z. 35, not seen. Nom. "
             "illeg., acc to Ryvarden (1976). Check legitimacy!")])],
        "only_illegitimate"),
    # The nom. nud. item is weak, but an independent Species Fungorum Plus
    # synonym and a Dyntaxa one (merged into COL, still not NorTaxa) remain.
    "204742/4VPGB": ("Sclerococcum homoclinellum", "(Nyl.) Ertz & Diederich", [
        ("Buellia procervula", "Norman", _nt("illegitimate"),
         _col("acceptable"), [_usage("MD6D7", "2041", "true", '"nom. nud. "')]),
        ("Dactylospora homoclinella", "(Nyl.) Hafellner", {_NT_SYN}, _col(),
         [_usage("33WS7", "2073", "false")]),
        ("Karschia homoclinella", "(Nyl.) Arnold", _nt("illegitimate"),
         _col("acceptable"), [_usage("Q9JLJ", "2041", "true")])],
        "ordinary", 3, 6),
}


def _pinned_class(name, authorship, shared, bridge_total=0, backbone_total=0,
                  *, rank="species", backbone_rank=None):
    """Shared entries are ``(name, authorship, nortaxa statuses, COL
    statuses[, COL usages])``; each side is padded with unshared synonyms up
    to its published synonym total."""
    bridge = _concept(name, authorship, rank,
                      [(e[0], e[1], e[2]) for e in shared]
                      + [(f"Bridgea s{i}", "X")
                         for i in range(bridge_total - len(shared))])
    backbone = _concept(name, authorship, backbone_rank or rank,
                        [(e[0], e[1], e[3], *e[4:]) for e in shared]
                        + [(f"Colia s{i}", "X")
                           for i in range(backbone_total - len(shared))])
    return audit.shared_review_class(_shared_row(bridge, backbone),
                                     bridge, backbone, _NORTAXA_IN_COL)


def test_reviewed_associations_classify_on_structured_status():
    for case, (name, authorship, shared, expected, *totals) \
            in _REVIEWED_CASES.items():
        assert _pinned_class(name, authorship, shared, *totals) == expected, \
            case


def test_craterellus_counts_only_independent_corroboration():
    name, authorship, shared = "Craterellus tubaeformis", "(Fr.) Quél.", \
        _CRATERELLUS
    assert _pinned_class(name, authorship, shared, 7, 51) \
        == "single_shared_synonym_low_overlap"
    # Were the two NorTaxa-sourced COL rows independent, it would be ordinary.
    independent = [e[:4] + ([_usage("X", "2073", "false")],) for e in shared]
    assert _pinned_class(name, authorship, independent, 7, 51) == "ordinary"


def test_rank_mismatch_never_reaches_ordinary():
    assert _pinned_class("Craterellus tubaeformis", "(Fr.) Quél.",
                         _CRATERELLUS, backbone_rank="variety") \
        == "rank_mismatch"
    assert _pinned_class("Craterellus tubaeformis", "(Fr.) Quél.",
                         _CRATERELLUS, rank="variety",
                         backbone_rank="form") == "rank_mismatch"
    assert _pinned_class("Craterellus tubaeformis", "(Fr.) Quél.",
                         _CRATERELLUS, rank="variety") == "infraspecific_rank"


def test_shared_review_class_precedence():
    shared = [("Cantharellus infundibuliformis", "(Scop.) Fr."),
              ("Merulius cantharelloides", "(Bull.) Purton"),
              ("Merulius infundibularis", "Kuntze")]
    assert _review_class(shared, shared + [("X y", "Z")] * 1,
                         name="Craterellus tubaeformis",
                         authorship="(Fr.) Quél.") == "ordinary"
    assert _review_class(shared, shared, rank="genus") == "non_species_rank"
    assert _review_class(shared, shared, rank="variety") == "infraspecific_rank"
    assert _review_class(shared, shared, backbone_authorship="") \
        == "accepted_authorship_disagrees"
    assert _review_class([("Hyphoderma patricium", "ined.")],
                         [("Hyphoderma patricium", "ined.")]) \
        == "only_unpublished"
    assert _review_class([("Inocybe calosporra", "Quél.")],
                         [("Inocybe calosporra", "Quél.")]) \
        == "only_name_variant"
    assert _review_class([("Inocybella calospora", ""),
                          ("Inocybe calosporra", "Quél.")],
                         [("Inocybella calospora", ""),
                          ("Inocybe calosporra", "Quél.")]) == "only_mixed_weak"
    many = [(f"Agaricus s{i}", "Fr.") for i in range(5)]
    assert _review_class(shared[:1] + many,
                         shared[:1] + [(f"Agaricus t{i}", "Fr.")
                                       for i in range(5)]) \
        == "single_shared_synonym_low_overlap"
    # A weak synonym beside an ordinary one does not demote the association.
    assert _review_class(shared[:2] + [("Hyphoderma x", "ined.")],
                         shared[:2] + [("Hyphoderma x", "ined.")]) == "ordinary"
    assert _review_class(shared[:2] + [("Inocybe x", "ss. Lange")],
                         shared[:2] + [("Inocybe x", "ss. Lange")]) \
        == "ordinary"


def test_published_rules_are_the_executed_rules():
    tests = audit.SHARED_REVIEW_CLASS_TESTS
    assert audit.SHARED_REVIEW_CLASSES == {n: r for n, r, _ in tests}
    assert audit.SYNONYM_KINDS == {
        **{k: r for k, r, _ in audit.SYNONYM_KIND_TESTS},
        "ordinary": "meets none of the tests above"}
    for kind in audit.WEAK_SYNONYM_KINDS:
        assert audit.SHARED_REVIEW_CLASSES[f"only_{kind}"] \
            == f"every shared synonym is of kind '{kind}'"
    # Every combination of the facts a rule reads lands in the first class
    # whose predicate holds, and the ordinary rule is complete on its own:
    # it holds exactly when no earlier rule does.
    import itertools
    kind_sets = [[k] for k in audit.SYNONYM_KINDS] + [
        list(p) for p in itertools.combinations(audit.SYNONYM_KINDS, 2)] + [
        ["ordinary", "ordinary"], ["unauthored", "unauthored"]]
    for ranks, agree, kinds, low in itertools.product(
            [("species", "species"), ("species", "variety"),
             ("variety", "variety"), ("genus", "species")],
            (True, False), kind_sets, (True, False)):
        facts = {"bridge_rank": ranks[0], "backbone_rank": ranks[1],
                 "accepted_names_agree": agree, "kinds": kinds,
                 "bridge_synonym_count": 9 if low else 1,
                 "backbone_synonym_count": 9 if low else 1}
        name = audit.review_class_for(facts)
        first = next(n for n, _, pred in tests if pred(facts))
        assert name == first
        ordinary = dict((n, p) for n, _, p in tests)["ordinary"]
        assert ordinary(facts) == (name == "ordinary"), facts
        if name == "ordinary":
            assert ranks == ("species", "species") and agree \
                and "ordinary" in kinds


def test_review_manifests_partition_the_parent():
    rows = [_row("1", audit.EVIDENCE_SHARED_SYNONYMY, 3),
            _row("2", audit.EVIDENCE_SHARED_SYNONYMY, 1),
            _row("3", audit.EVIDENCE_NONE, 0)]
    rows[0]["review_class"], rows[1]["review_class"] = "ordinary", \
        "only_unpublished"
    for row in rows[:2]:
        row["shared_synonym_kind_counts"] = {"ordinary": 1}
        row["shared_synonym_evidence"] = ["ordinary: none of the weak kinds"]
    rows[1]["in_cloud_scope"] = False
    pins = {"release": {"sqlite_sha256": "x"}}
    parent = audit.build_manifest(audit.EVIDENCE_SHARED_SYNONYMY, rows, pins)
    split = {cls: audit.build_review_manifest(cls, rows, pins, parent)
             for cls in audit.SHARED_REVIEW_CLASSES}
    assert [m[0] for m in split["ordinary"]["members"]] == ["1"]
    assert [m[0] for m in split["only_unpublished"]["members"]] == ["2"]
    assert split["only_unpublished"]["cloud_scope_member_count"] == 0
    assert split["ordinary"]["parent_manifest"]["members_sha256"] \
        == parent["members_sha256"]
    width = len(audit.MANIFEST_COLUMNS)
    assert sorted(m[:width] for s in split.values() for m in s["members"]) \
        == sorted(parent["members"])
    assert split["ordinary"]["columns"][width:] == audit.REVIEW_EXTRA_COLUMNS
    # Distinct classes never share a members fingerprint unless both are empty.
    assert split["ordinary"]["members_sha256"] \
        != split["only_unpublished"]["members_sha256"]
    assert split["ordinary"]["review_status"] == "needs_review"
    assert split["ordinary"]["membership_rule"] \
        == audit.SHARED_REVIEW_CLASSES["ordinary"]


_STAGE0 = _SCRIPT.parent / "stage0"


def _tracked(name):
    import json
    return json.loads((_STAGE0 / name).read_text(encoding="utf-8"))


def test_tracked_review_manifests_partition_and_obey_their_rules():
    """The committed Stage 0 split, checked against its own parent and the
    rules it publishes. Needs only tracked files."""
    parent = _tracked("group-a-shared-synonymy.manifest.json")
    width = len(parent["columns"])
    placed, split = {}, []
    for cls in audit.SHARED_REVIEW_CLASSES:
        manifest = _tracked(f"group-a-shared-synonymy--"
                            f"{cls.replace('_', '-')}.manifest.json")
        assert manifest["membership_rule"] == audit.SHARED_REVIEW_CLASSES[cls]
        assert manifest["review_status"] == "needs_review"
        assert manifest["parent_manifest"]["members_sha256"] \
            == parent["members_sha256"]
        counts = manifest["columns"].index("shared_synonym_kind_counts")
        for member in manifest["members"]:
            kinds = member[counts]
            if cls.startswith("only_") and cls != "only_mixed_weak":
                assert set(kinds) == {cls[len("only_"):]}, member[:3]
            if cls == "ordinary":
                assert kinds.get("ordinary", 0) >= 1, member[:3]
            if cls == "only_mixed_weak":
                assert "ordinary" not in kinds and len(kinds) > 1
            placed[(member[0], member[1])] = cls
            split.append(member[:width])
    assert sorted(split) == sorted(parent["members"])
    assert len(placed) == len(split) == parent["member_count"] == 4861
    for case, entry in _REVIEWED_CASES.items():
        assert placed[tuple(case.split("/"))] == entry[3], case


def test_summary_distributions():
    rows = [_row("1", audit.EVIDENCE_SHARED_SYNONYMY, 1),
            _row("2", audit.EVIDENCE_SHARED_SYNONYMY, 1),
            _row("3", audit.EVIDENCE_NONE, 0)]
    summary = audit.summarise(rows, taxon_key="sporely_taxon_id")
    assert summary["reviewable_total"] == 2
    assert summary["shared_synonym_count_distribution"][
        audit.EVIDENCE_SHARED_SYNONYMY] == {"1": 2}
