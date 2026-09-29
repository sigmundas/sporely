"""Taxonomy v3 Stage 2: Group-B duplicate candidates graded for review.

The full audit runs against the tracked release and the gitignored source
archives. These tests pin the rules that decide a pair's sub-class and review
order, and check the committed manifests and the policy ledgers using only
tracked files.
"""
import hashlib
import importlib.util
import json
from pathlib import Path

_HERE = Path(__file__).resolve().parents[1] / "evidence" / "taxonomy-v3"
_spec = importlib.util.spec_from_file_location(
    "audit_stage2_group_b", _HERE / "audit_stage2_group_b.py")
audit = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(audit)

_STAGE2 = _HERE / "stage2"
_POLICIES = Path(__file__).resolve().parents[1] / "policies"


def _concept(name, authorship):
    return {"accepted": (name.casefold(), authorship), "authorship": authorship}


def _difference(nortaxa_authorship, col_authorship, *, col_name="Cantharellus cibarius"):
    return audit.authorship_difference(
        _concept("Cantharellus cibarius", nortaxa_authorship),
        _concept(col_name, col_authorship))


def test_authorship_difference_kinds():
    assert _difference("Fr.", "Fr.") == "same"
    assert _difference("Fr.", "Fr.", col_name="Craterellus cibarius") \
        == "accepted_name_differs"
    assert _difference("(R. Heim & L. Remy) M.M. Moser",
                       "(R. Heim & L. Rémy) M.M. Moser") == "typography_only"
    assert _difference("(Berk. & M. A. Curtis ) Singer",
                       "(Berk. & M.A. Curtis) Singer") == "typography_only"
    # Sanctioning citations, inside the parenthesis and at the end.
    assert _difference("(Pers. : Fr.) Boud.", "(Pers.) Boud.") \
        == "sanctioning_citation"
    assert _difference("(Dicks.: Pers.) Pers.", "(Dicks.) Pers.") \
        == "sanctioning_citation"
    assert _difference("Fr. : Fr.", "Fr.") == "sanctioning_citation"
    # Different authors, and abbreviations, are never read as the same.
    assert _difference("(Lasch) Singer", "(Scop.) Métrod") \
        == "different_authorship"
    assert _difference("(Bres.) Borovicka", "(Bres.) Borov.") \
        == "different_authorship"
    assert _difference("(Fr. : Fr.) Romagn.", "(Fr.) Pat. ex Romagn.") \
        == "different_authorship"
    assert audit.authorship_difference(None, _concept("X y", "Fr.")) is None


def test_sub_class_reads_the_synonyms_as_if_the_keys_agreed():
    facts = {"bridge_rank": "species", "backbone_rank": "species",
             "accepted_names_agree": False, "kinds": ["ordinary", "ordinary"],
             "bridge_synonym_count": 2, "backbone_synonym_count": 2}
    assert audit.stage0.review_class_for(facts) == "accepted_authorship_disagrees"
    assert audit.authorship_sub_class("sanctioning_citation", facts) \
        == "sanctioning_citation--ordinary"
    weak = {**facts, "kinds": ["nortaxa_derived"]}
    assert audit.authorship_sub_class("typography_only", weak) \
        == "typography_only--only_nortaxa_derived"


def _member(n_id, c_id, in_scope, vernaculars):
    row = dict.fromkeys(audit.B_COLUMNS)
    row.update(nortaxa_taxon_id=n_id, col_usage_id=c_id,
               col_in_cloud_scope=in_scope,
               nortaxa_vernacular_languages=vernaculars)
    return row


def test_members_are_in_cloud_impact_order():
    rows = [_member("1", "A", False, ["nb"]), _member("2", "B", True, []),
            _member("3", "C", True, ["nb"]), _member("0", "D", False, [])]
    ids = [m[0] for m in audit._members(rows, audit.B_COLUMNS)]
    assert ids == ["3", "2", "1", "0"]
    assert audit._members(list(reversed(rows)), audit.B_COLUMNS) \
        == audit._members(rows, audit.B_COLUMNS)


def _tracked(name):
    return json.loads((_STAGE2 / name).read_text(encoding="utf-8"))


def _file_sha(name):
    return hashlib.sha256((_STAGE2 / name).read_bytes()).hexdigest()


def test_tracked_manifests_are_pinned_partitioned_and_undecided():
    report = _tracked("group-b-report.json")
    stage0 = json.loads((_HERE / "stage0" / "coverage-report.json")
                        .read_text(encoding="utf-8"))
    assert report["pins"] == stage0["pins"]
    for name, entry in report["manifests"].items():
        manifest = _tracked(name)
        assert _file_sha(name) == entry["file_sha256"], name
        assert manifest["pins"] == report["pins"]
        assert manifest["review_status"] == "needs_review"
        assert manifest["member_count"] == len(manifest["members"])
        columns = manifest["columns"]
        members = manifest["members"]
        assert members == audit._members(
            [dict(zip(columns, m)) for m in members], columns), name
    evidence = [_tracked(f"group-b-{c.replace('_', '-')}.manifest.json")
                for c in audit.EVIDENCE_CLASSES]
    assert sum(m["member_count"] for m in evidence) == 7423
    assert [m["approval_mode"] for m in evidence] == [
        "decided_through_its_partition", "individual",
        "decided_through_its_partition", "not_approvable"]

    # Every pair is in exactly one decision leaf, and the leaves partition
    # each parent kept for accounting.
    leaves = {n: _tracked(n) for n, e in report["manifests"].items()
              if e["approval_mode"] != "decided_through_its_partition"}
    placed = [(m[0], m[2]) for leaf in leaves.values() for m in leaf["members"]]
    assert len(placed) == len(set(placed)) == 7423
    width = len(audit.B_COLUMNS)
    for parent_name in ("group-b-shared-synonymy.manifest.json",
                        "group-b-reciprocal-accepted-synonymy.manifest.json",
                        "group-b-shared-synonymy--accepted-authorship-"
                        "disagrees.manifest.json"):
        parent = {(m[0], m[2]): m[:width]
                  for m in _tracked(parent_name)["members"]}
        parts = sorted(m[:width] for leaf in leaves.values()
                       for m in leaf["members"] if (m[0], m[2]) in parent)
        assert parts == sorted(parent.values()), parent_name
    sub = audit.SUB_CLASS_COLUMN
    for name, manifest in leaves.items():
        if "sub_class" in manifest:
            column = manifest["columns"].index(sub)
            assert {m[column] for m in manifest["members"]} \
                <= {manifest["sub_class"]}, name
        if manifest["approval_mode"] == "batch_by_file_sha256":
            one = manifest["columns"].index("one_to_one")
            assert all(m[one] for m in manifest["members"]), name


def test_pair_review_queue_is_one_global_cloud_impact_order():
    report = _tracked("group-b-report.json")
    queue = report["pair_review_queue"]
    assert [e["position"] for e in queue] == list(range(1, len(queue) + 1))
    # Every pair a decision can reach, and no other, exactly once.
    assert len(queue) == 62 + 2032
    assert len({(e["nortaxa_taxon_id"], e["col_usage_id"]) for e in queue}) \
        == len(queue)
    assert {e["approval_mode"] for e in queue} \
        == {"batch_by_file_sha256", "individual"}
    tiers = [(not e["col_in_cloud_scope"], not e["nortaxa_has_vernaculars"],
              e["nortaxa_taxon_id"], e["col_usage_id"]) for e in queue]
    assert tiers == sorted(tiers)
    # No cloud pair comes after a non-cloud pair, whatever its manifest.
    first_off_cloud = next(i for i, e in enumerate(queue)
                           if not e["col_in_cloud_scope"])
    assert all(not e["col_in_cloud_scope"] for e in queue[first_off_cloud:])
    for entry in queue:
        assert report["manifests"][entry["decision_manifest"]]["file_sha256"] \
            == entry["file_sha256"]
        member_pairs = {(m[0], m[2]) for m in
                        _tracked(entry["decision_manifest"])["members"]}
        assert (entry["nortaxa_taxon_id"], entry["col_usage_id"]) \
            in member_pairs


def test_not_one_to_one_pairs_are_enumerated_and_never_batched():
    report = _tracked("group-b-report.json")
    ambiguous = report["not_one_to_one"]
    assert ambiguous["pair_count"] == len(ambiguous["pairs"]) == 217
    assert ambiguous["nortaxa_concepts_with_several_col_twins"] == 82
    assert ambiguous["col_concepts_with_several_nortaxa_twins"] == 25
    for pair in ambiguous["pairs"]:
        assert pair["col_twins_of_nortaxa_concept"] > 1 \
            or pair["nortaxa_twins_of_col_concept"] > 1
        mode = report["manifests"][pair["decision_manifest"]]["approval_mode"]
        assert mode in ("individual", "not_approvable"), pair
    routed = _tracked(audit.NOT_ONE_TO_ONE_MANIFEST)
    assert routed["approval_mode"] == "individual"
    assert {(m[0], m[2]) for m in routed["members"]} == {
        (p["nortaxa_taxon_id"], p["col_usage_id"]) for p in ambiguous["pairs"]
        if p["decision_manifest"] == audit.NOT_ONE_TO_ONE_MANIFEST}
    assert routed["member_count"] == 6


def test_one_to_one_is_computed_over_both_sides():
    pairs = [("1", 10, "A", 20), ("1", 10, "B", 21),   # NorTaxa 1 has two twins
             ("2", 11, "C", 22), ("3", 12, "C", 22),   # COL C has two twins
             ("4", 13, "D", 23)]
    rows = audit.pair_rows(pairs, set(), {}, {})
    assert [r["one_to_one"] for r in rows] == [False, False, False, False, True]


def test_regression_outcomes_are_explicit_and_open():
    outcomes = _tracked("group-b-report.json")["regression_outcomes"]
    cantharellus = outcomes["Cantharellus cibarius"]
    assert cantharellus["col_sporely_taxon_id"] == 168873
    assert "nb" not in cantharellus["col_vernacular_languages"]
    assert "nb" in cantharellus["nortaxa_vernacular_languages"]
    vexans = outcomes["Conocybe vexans / Pholiotina vexans"]
    assert vexans["evidence_class"] == "reciprocal_accepted_synonymy"
    assert (vexans["nortaxa_sporely_taxon_id"],
            vexans["col_sporely_taxon_id"]) == (627000, 617026)
    gloeophyllum = outcomes["Gloeophyllum odoratum (NBIC:56449)"]
    assert gloeophyllum["nortaxa_sporely_taxon_id"] == 626327
    assert {o["outcome"] for o in outcomes.values()} \
        == {"left_open", "not_reconciled"}
    for species, spec in audit.REGRESSION_OUTCOMES.items():
        assert (outcomes[species]["evidence_class"],
                outcomes[species]["authorship_sub_class"]) == spec["expect"]


def test_no_group_b_pair_is_reconciled_without_a_decision():
    """No Group-B pair, and not the undecided Conocybe vexans pair, has a
    supersession or mapping; the one existing supersession is 52369's."""
    ledger = json.loads((_POLICIES / "concept_supersessions.yml")
                        .read_text(encoding="utf-8"))
    assert [s["superseded_sporely_taxon_id"]
            for s in ledger["supersessions"]] == [624680]
    mappings = json.loads((_POLICIES / "manual_mappings.yml")
                          .read_text(encoding="utf-8"))
    mapped = {m["source_usage"]["identifier"] for m in mappings["mappings"]
              if m["source_usage"]["namespace"] == "nortaxa_taxon_id"}
    group_b = set()
    for name in _tracked("group-b-report.json")["manifests"]:
        group_b |= {m[0] for m in _tracked(name)["members"]}
    assert len(group_b) == 7338
    assert not group_b & mapped
    assert "58766" not in mapped
    assert all("stage2" not in a["path"]
               for a in mappings["approved_manifests"])
