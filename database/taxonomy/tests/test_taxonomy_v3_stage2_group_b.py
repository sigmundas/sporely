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


def test_regression_outcomes_are_the_owner_decisions():
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
    assert {s: o["outcome"] for s, o in outcomes.items()} == {
        "Cantharellus cibarius": "approved",
        "Conocybe vexans / Pholiotina vexans": "approved",
        "Gloeophyllum odoratum (NBIC:56449)": "reconciled"}
    for species, spec in audit.REGRESSION_OUTCOMES.items():
        assert (outcomes[species]["evidence_class"],
                outcomes[species]["authorship_sub_class"]) == spec["expect"]


# ------------------------------------------------ approved supersessions ---

_gen_spec = importlib.util.spec_from_file_location(
    "generate_stage2_supersessions", _HERE / "generate_stage2_supersessions.py")
generator = importlib.util.module_from_spec(_gen_spec)
_gen_spec.loader.exec_module(generator)

from bridge_emission import (  # noqa: E402
    BridgeEmissionError,
    verify_supersession_manifest_approvals,
)
from compile_release import CompilerError, compile_release  # noqa: E402
from identity_registry import IdentityRegistry  # noqa: E402
from test_bridge_emission import (  # noqa: E402
    _POLICY_PATH,
    _candidate,
    _compile,
    _resolve,
    _usages,
)
from test_compile_release import (  # noqa: E402
    _with_fixture_provenance,
    _write_manual_mappings,
)
import pytest  # noqa: E402

_APPROVED = {
    "typography_only--ordinary":
        "dd7a7bd0dcb6b57e0f147b5a7f27f91eb1e513f640bdcf126dc72c5485808e42",
    "sanctioning_citation--ordinary":
        "b49338db18c097689fb0239bf68adc5640409604ad6816cad74967c20c9fdcba",
    "different_authorship--ordinary":
        "7edfb0f253bd278af6c4d5e3f7462cc69c7fa6f9aac1c95b71840a4271bd8653",
}


def _ledger():
    return json.loads((_POLICIES / "concept_supersessions.yml")
                      .read_text(encoding="utf-8"))


def test_generator_names_exactly_the_owner_approved_manifests():
    assert {sub: sha for _, sub, sha in generator.APPROVED_MANIFESTS} \
        == _APPROVED
    report = _tracked("group-b-report.json")["manifests"]
    for path, sub, sha in generator.APPROVED_MANIFESTS:
        name = Path(path).name
        assert report[name]["file_sha256"] == sha == _file_sha(name)
        assert report[name]["approval_mode"] == "batch_by_file_sha256"


def test_shipped_ledger_missing_one_member_is_refused():
    """Dropping any one manifest-bound record from the shipped ledger makes
    it fail verification, so compile and validation fail closed."""
    ledger = _ledger()
    bound = [i for i, s in enumerate(ledger["supersessions"])
             if "approved_manifest" in s]
    for index in (bound[0], bound[len(bound) // 2], bound[-1]):
        partial = {**ledger, "supersessions": [
            s for i, s in enumerate(ledger["supersessions"]) if i != index]}
        with pytest.raises(BridgeEmissionError,
                           match="with no supersession record"):
            verify_supersession_manifest_approvals(partial)


def test_shipped_ledger_is_current_and_supersedes_only_approved_pairs():
    """The committed ledger is what the generator renders from the approved
    manifests, and retires exactly their members plus Conocybe vexans and
    the earlier 52369 supersession — no other Group-B pair."""
    text = (_POLICIES / "concept_supersessions.yml").read_text(encoding="utf-8")
    assert generator.render() == text
    ledger = json.loads(text)
    assert {a["file_sha256"] for a in ledger["approved_manifests"]} \
        == set(_APPROVED.values())
    bound = verify_supersession_manifest_approvals(ledger)
    assert len(bound) == 179 + 336 + 836

    approved = {}
    for path, _, _ in generator.APPROVED_MANIFESTS:
        manifest = _tracked(Path(path).name)
        for row in manifest["members"]:
            m = dict(zip(manifest["columns"], row))
            approved[m["nortaxa_sporely_taxon_id"]] = m["col_usage_id"]
    approved[627000] = "XQZ6"
    retired = {s["superseded_sporely_taxon_id"]: s["current_source_usage"]
               ["identifier"] for s in ledger["supersessions"]}
    assert retired.pop(624680) == "5ZT3G"
    assert retired == approved
    assert len(retired) == 1352

    # Every record carries reviewed provenance; name equality is never cited
    # as the evidence.
    for record in ledger["supersessions"]:
        assert record["review_status"] == "approved"
        assert record["reviewer"] and record["evidence_references"]
        assert record["relationship"] == "exact"

    # No unapproved Group-B pair is superseded or mapped.
    report = _tracked("group-b-report.json")
    unapproved = set()
    for name, entry in report["manifests"].items():
        if entry["approval_mode"] == "decided_through_its_partition" \
                or entry["file_sha256"] in _APPROVED.values():
            continue
        columns = _tracked(name)["columns"]
        position = columns.index("nortaxa_sporely_taxon_id")
        unapproved |= {m[position] for m in _tracked(name)["members"]}
    assert not unapproved & set(retired)
    mappings = json.loads((_POLICIES / "manual_mappings.yml")
                          .read_text(encoding="utf-8"))
    assert not {m["source_usage"]["identifier"] for m in mappings["mappings"]
                if m["source_usage"]["namespace"] == "nortaxa_taxon_id"} \
        & {"58766", "56210", "56449"}


def test_regression_pairs_are_superseded_as_decided():
    by_retired = {s["superseded_sporely_taxon_id"]: s
                  for s in _ledger()["supersessions"]}
    for retired, col in ((626243, "QMKY"), (626327, "3GBK2"), (627000, "XQZ6")):
        record = by_retired[retired]
        assert record["current_source_usage"] == {
            "source": "col_xr", "namespace": "col_usage_id", "identifier": col}
        assert any("gate fbeea497a1cb45c8a94baae48be43acc" in ref
                   for ref in record["evidence_references"])
    # Conocybe vexans is individually reviewed, in no manifest.
    assert "approved_manifest" not in by_retired[627000]
    assert by_retired[626243]["approved_manifest"]["file_sha256"] \
        == _APPROVED["sanctioning_citation--ordinary"]
    # Retired concepts keep their registry anchors; the ledger selects the
    # survivor without rewriting the allocation.
    registry = IdentityRegistry(_HERE.parents[1] / "registry" / "canonical")
    registry.load()
    for nortaxa_id, retired in (("56210", 626243), ("56449", 626327),
                                ("58766", 627000)):
        found = registry.lookup("nortaxa", "nortaxa_taxon_id", nortaxa_id)
        assert (found.sporely_taxon_id, found.kind) == (retired, "anchor")


# --- the verifier, on a synthetic manifest ---------------------------------

_PINS = {"release": {"content_release_id": "tax-test"}}


def _manifest_file(tmp_path, members, *, approval_mode="batch_by_file_sha256",
                   pins=_PINS, name="leaf.manifest.json"):
    columns = ["nortaxa_taxon_id", "nortaxa_sporely_taxon_id",
               "col_usage_id", "col_sporely_taxon_id", "one_to_one"]
    path = tmp_path / name
    path.write_text(json.dumps({
        "approval_mode": approval_mode, "pins": pins, "columns": columns,
        "members": members}, sort_keys=True), encoding="utf-8")
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def _document(path, sha, records, *, pins=_PINS):
    return {
        "approved_manifests": [{
            "path": str(path), "file_sha256": sha, "pins": pins,
            "approved_by": "Owner", "approved_at": "2026-09-29",
            "decision_reference": "test decision"}],
        "supersessions": _with_fixture_provenance(records),
    }


def _supersession(nortaxa_id, retired, col_id, survivor, sha, *,
                  current=None):
    return {
        "supersession_id": f"s-{nortaxa_id}",
        "superseded_sporely_taxon_id": retired,
        "current_source_usage": {"source": "col_xr",
                                 "namespace": "col_usage_id",
                                 "identifier": current or col_id},
        "relationship": "exact", "review_status": "approved",
        "approved_manifest": {"file_sha256": sha, "member": {
            "nortaxa_taxon_id": nortaxa_id,
            "nortaxa_sporely_taxon_id": retired,
            "col_usage_id": col_id, "col_sporely_taxon_id": survivor}},
    }


def test_verifier_accepts_one_record_per_member(tmp_path):
    path, sha = _manifest_file(tmp_path, [["1", 10, "A", 20, True]])
    doc = _document(path, sha, [_supersession("1", 10, "A", 20, sha)])
    assert verify_supersession_manifest_approvals(doc) == {"s-1": sha}


@pytest.mark.parametrize("case, match", [
    ("tampered", "has sha256"),
    ("pins", "pins differ"),
    ("individual", "approval_mode"),
    ("ambiguous", "not one-to-one"),
    ("non_member", "is not a member"),
    ("wrong_survivor", "do not match the manifest member"),
    ("duplicate", "more than one record"),
    ("unapproved_manifest", "no approved_manifests entry"),
    ("missing_member", "with no supersession record"),
])
def test_verifier_refuses(tmp_path, case, match):
    members = [["1", 10, "A", 20, True]]
    kwargs = {}
    if case == "individual":
        kwargs["approval_mode"] = "individual"
    if case == "ambiguous":
        members = [["1", 10, "A", 20, False]]
    if case == "missing_member":
        members = [["1", 10, "A", 20, True], ["2", 11, "B", 21, True]]
    path, sha = _manifest_file(tmp_path, members, **kwargs)
    records = [_supersession("1", 10, "A", 20, sha)]
    pins = _PINS
    if case == "tampered":
        path.write_text(path.read_text(encoding="utf-8") + " ",
                        encoding="utf-8")
    if case == "pins":
        pins = {"release": {"content_release_id": "tax-other"}}
    if case == "non_member":
        records = [_supersession("2", 11, "B", 21, sha)]
    if case == "wrong_survivor":
        records = [_supersession("1", 10, "A", 20, sha, current="B")]
    if case == "duplicate":
        records = [_supersession("1", 10, "A", 20, sha),
                   {**_supersession("1", 10, "A", 20, sha),
                    "supersession_id": "s-1-again"}]
    if case == "unapproved_manifest":
        records = [_supersession("1", 10, "A", 20, "0" * 64)]
    with pytest.raises(BridgeEmissionError, match=match):
        verify_supersession_manifest_approvals(
            _document(path, sha, records, pins=pins))


# --- compile and resolve through a manifest-bound supersession -------------


def _recompile(tmp_path, ledger, name):
    ledger_path = tmp_path / f"{name}.yml"
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")
    release = tmp_path / name
    compile_release(
        normalized_source_dirs=[tmp_path / "sources" / "col_xr",
                                tmp_path / "sources" / "nortaxa"],
        manual_mappings_path=_write_manual_mappings(tmp_path / f"{name}-m.yml",
                                                    []),
        mapping_policy_path=_POLICY_PATH,
        registry_path=tmp_path / "registry.jsonl",
        output_dir=release,
        release_id="tax-2026.09.29-02",
        concept_supersessions_path=ledger_path,
    )
    return release


def test_manifest_bound_supersession_compiles_and_resolves(tmp_path):
    """A batch-approved Group-B-shaped record retires an allocated concept:
    the retired NorTaxa id resolves to the survivor, its vernacular follows,
    the registry keeps the allocation, and a non-member cannot compile."""
    first = _compile(tmp_path, release_id="tax-2026.09.29-01")
    own_id = _usages(first)[("nortaxa", "52369")]["sporely_taxon_id"]
    survivor = _usages(first)[("col_xr", "5ZT3G")]["sporely_taxon_id"]
    path, sha = _manifest_file(tmp_path, [["52369", own_id, "5ZT3G", survivor,
                                           True]])
    record = _supersession("52369", own_id, "5ZT3G", survivor, sha)
    record["current_source_usage"]["namespace"] = "col_xr_taxon_id"
    release = _recompile(tmp_path, _document(path, sha, [record]), "approved")

    registry = IdentityRegistry(tmp_path / "registry.jsonl")
    registry.load()
    assert registry.lookup("nortaxa", "nortaxa_taxon_id",
                           "52369").sporely_taxon_id == own_id
    conn, _ = _candidate(tmp_path, release, "approved.sqlite3")
    try:
        assert _resolve(conn, "nortaxa", "nortaxa_taxon_id", "52369") \
            == [survivor]
        assert conn.execute("SELECT COUNT(*) FROM taxon_min WHERE taxon_id = ?",
                            (own_id,)).fetchone()[0] == 0
        assert "slank ringkjeglesopp" in {r["vernacular_name"] for r in
                                          conn.execute(
            "SELECT vernacular_name FROM vernacular_min WHERE taxon_id = ?",
            (survivor,))}
    finally:
        conn.close()

    # The same concept retired by a record that is not a member is refused.
    other, other_sha = _manifest_file(tmp_path, [["1", 1, "Z", 2, True]],
                                      name="other.manifest.json")
    stray = _supersession("52369", own_id, "5ZT3G", survivor, other_sha)
    stray["current_source_usage"]["namespace"] = "col_xr_taxon_id"
    with pytest.raises(CompilerError, match="is not a member"):
        _recompile(tmp_path, _document(other, other_sha, [stray]), "stray")
