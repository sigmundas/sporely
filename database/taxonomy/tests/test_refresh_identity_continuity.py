"""Compiler-level refresh identity continuity (R2d): duplicate-id prevention for
renumbered COL usages, NorTaxa fallback for vanished COL anchors, species ->
aggregate source changes, red-list association and historical redirects."""
from __future__ import annotations

import json
import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
TESTS = Path(__file__).resolve().parent
for p in (SCRIPTS, TESTS):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import carry_forward  # noqa: E402
from compile_release import _build_redlist_resolution_index, _release_string, compile_release  # noqa: E402
from identity_registry import IdentityRegistry  # noqa: E402
from test_bridge_emission import _write_supersessions  # noqa: E402
from test_compile_release import _POLICY_PATH, _write_manual_mappings, _write_normalized_source  # noqa: E402

FAM = {"family": "Agaricaceae"}


def _src(root: Path, code: str, version: str, rows: list[dict], vernacular: list[tuple[str, str]] = ()) -> Path:
    prefix = {"col_xr": "COL:", "nortaxa": "NBIC:"}[code]
    d = _write_normalized_source(root / code, source_code=code,
                                 source_release={"version": version, "issued_date": "2026-01-01"},
                                 identifier_namespace_prefix=prefix, rows=rows)
    if code == "col_xr":  # the real COL namespace
        for name in ("taxa.jsonl", "report.json"):
            f = d / name
            f.write_text(f.read_text().replace('"col_xr_taxon_id"', '"col_usage_id"'))
    if vernacular:
        cores = {}
        for line in (d / "taxa.jsonl").read_text().splitlines():
            r = json.loads(line)
            cores[r["taxon_id"]["value"]] = r["core_row_id"]
        (d / "vernacular.jsonl").write_text("".join(json.dumps({
            "source_code": code, "source_release": {"version": version, "issued_date": "2026-01-01"},
            "core_row_id": cores[tid], "language": "nb", "is_preferred": True, "vernacular_name": name}) + "\n"
            for tid, name in vernacular))
    return d


def _col(root, version, extra):
    return _src(root, "col_xr", version, [
        {"core_row_id": "K", "taxon_id": "K", "scientific_name": "Fungi", "rank": "kingdom"},
        {"core_row_id": "G", "taxon_id": "G", "parent": "K", "parent_resolution": "resolved",
         "scientific_name": "Echinoderma", "rank": "genus"}] + extra)


def _sp(tid, name, author="(Knudsen) Bon", status="accepted", **kw):
    return {"core_row_id": tid, "taxon_id": tid, "parent": "G", "parent_resolution": "resolved",
            "scientific_name": name, "authorship": author, "status": status, "classification": FAM, **kw}


def _receipt(tmp_path, previous, current, mappings, supersessions, registry, continuity=()):
    pins = {}
    for d in current.values():
        rep = json.loads((d / "report.json").read_text())
        pins[rep["profile_source_code"]] = {
            "source_release_id": f"{rep['profile_source_code']}:{_release_string(rep['profile_source_release'])}",
            "sha256": rep["archive_sha256"]}
    receipt = carry_forward.build_receipt(
        release_id="tax-2026.01.02-01", previous_dirs=previous, current_dirs=current, previous_pins=pins,
        current_pins=pins, manual_mappings=mappings, supersessions=supersessions,
        anchors=carry_forward.registry_anchors(registry),
        id_continuity={("col_xr", "col_usage_id", c["old_col_id"]): c for c in continuity})
    path = tmp_path / "receipt.json"
    path.write_text(json.dumps(receipt))
    return path, receipt


def _taxa(release: Path) -> dict[int, dict]:
    return {r["sporely_taxon_id"]: r for r in map(json.loads, (release / "taxa.jsonl").read_text().splitlines())}


def _ids(registry: Path) -> dict[tuple, int]:
    reg = IdentityRegistry(registry)
    reg.load()
    return {e.key(): e.sporely_taxon_id for e in reg.all_entries()}


def test_renumbered_col_usage_is_bound_by_approved_continuity_and_unapproved_match_is_not_allocated(tmp_path):
    reg = tmp_path / "registry.jsonl"
    common = dict(manual_mappings_path=_write_manual_mappings(tmp_path / "m.json", []),
                  mapping_policy_path=_POLICY_PATH, registry_path=reg)
    old = _col(tmp_path / "v1", "1", [_sp("A", "Echinoderma asperum"), _sp("B", "Echinoderma carinii")])
    compile_release(normalized_source_dirs=[old], output_dir=tmp_path / "r1", release_id="tax-2026.01.01-01", **common)
    before = _ids(reg)
    a_id, b_id = before[("col_xr", "col_usage_id", "A")], before[("col_xr", "col_usage_id", "B")]
    # A and B are renumbered to A2 and B2 with identical name, rank, authorship and family.
    new = _col(tmp_path / "v2", "2", [_sp("A2", "Echinoderma asperum"), _sp("B2", "Echinoderma carinii")])
    path, receipt = _receipt(tmp_path, {"col_xr": old}, {"col_xr": new}, {"mappings": []}, {"supersessions": []}, reg,
                             continuity=[{"old_col_id": "A", "new_col_id": "A2", "sporely_taxon_id": a_id}])
    pairs = {p["old_col_id"]: p["new_col_id"] for p in receipt["col_continuity_candidates"]["pairs"]}
    assert pairs == {"A": "A2", "B": "B2"}
    compile_release(normalized_source_dirs=[new], output_dir=tmp_path / "r2", release_id="tax-2026.01.02-01",
                    continuity_receipt_path=path, **common)
    after = _ids(reg)
    taxa = _taxa(tmp_path / "r2")
    assert after[("col_xr", "col_usage_id", "A2")] == a_id and a_id in taxa  # same id, no duplicate
    assert ("col_xr", "col_usage_id", "B2") not in after  # unapproved match: no new id allocated
    assert b_id not in taxa
    assert all(after[k] == v for k, v in before.items())  # no existing source key rebound
    diag = json.loads((tmp_path / "r2" / "diagnostics.json").read_text())["source_continuity"]
    assert "B2" in diag["col_continuity_review_withheld"]


def test_historical_redirect_to_a_renumbered_usage_still_resolves(tmp_path):
    reg = tmp_path / "registry.jsonl"
    maps = _write_manual_mappings(tmp_path / "m.json", [])
    nt = _src(tmp_path / "v1", "nortaxa", "1", [
        {"core_row_id": "R", "taxon_id": "R", "scientific_name": "Fungi", "rank": "kingdom"},
        {"core_row_id": "N", "taxon_id": "N", "parent": "R", "parent_resolution": "resolved",
         "scientific_name": "Lepiota aspera", "authorship": "(Pers.) Quél.", "status": "valid"}])
    old = _col(tmp_path / "v1", "1", [_sp("A", "Echinoderma asperum")])
    compile_release(normalized_source_dirs=[old, nt], output_dir=tmp_path / "r1", release_id="tax-2026.01.01-01",
                    manual_mappings_path=maps, mapping_policy_path=_POLICY_PATH, registry_path=reg)
    ids = _ids(reg)
    retired = ids[("nortaxa", "nortaxa_taxon_id", "N")]
    a_id = ids[("col_xr", "col_usage_id", "A")]
    assert retired != a_id
    sup = {"supersessions": [{"supersession_id": "s-N-A", "superseded_sporely_taxon_id": retired,
                              "current_source_usage": {"source": "col_xr", "namespace": "col_usage_id",
                                                       "identifier": "A"},
                              "relationship": "exact", "review_status": "approved"}]}
    new = _col(tmp_path / "v2", "2", [_sp("A2", "Echinoderma asperum")])
    path, _ = _receipt(tmp_path, {"col_xr": old, "nortaxa": nt}, {"col_xr": new, "nortaxa": nt},
                       {"mappings": []}, sup, reg,
                       continuity=[{"old_col_id": "A", "new_col_id": "A2", "sporely_taxon_id": a_id}])
    compile_release(normalized_source_dirs=[new, nt], output_dir=tmp_path / "r2", release_id="tax-2026.01.02-01",
                    manual_mappings_path=maps, mapping_policy_path=_POLICY_PATH, registry_path=reg,
                    concept_supersessions_path=_write_supersessions(tmp_path / "s.json", sup["supersessions"]),
                    continuity_receipt_path=path)
    usages = [json.loads(l) for l in (tmp_path / "r2" / "source_usages.jsonl").read_text().splitlines()]
    assert a_id in _taxa(tmp_path / "r2")
    moved = [u for u in usages if u.get("superseded_from_sporely_taxon_id") == retired]
    assert moved and all(u["sporely_taxon_id"] == a_id for u in moved)


def _nortaxa_case(tmp_path, nt_new_rows, nt_new_vern, *, keep_species: bool, approve_fallback: bool = False):
    """R1: COL species S, NorTaxa N mapped to S (approved). R2: NorTaxa N changes;
    COL S stays (keep_species) or vanishes (then N's mapping is no longer listed)."""
    reg = tmp_path / "registry.jsonl"
    mapping = [{"mapping_id": "map-N-S", "review_status": "approved",
                "source_usage": {"source": "nortaxa", "namespace": "nortaxa_taxon_id", "identifier": "N"},
                "target": {"source_usage": {"source": "col_xr", "namespace": "col_usage_id", "identifier": "S"}},
                "relationship": "exact"}]
    maps = _write_manual_mappings(tmp_path / "m.json", mapping)
    root = {"core_row_id": "R", "taxon_id": "R", "scientific_name": "Fungi", "rank": "kingdom"}
    nt1 = _src(tmp_path / "v1", "nortaxa", "1", [root, {
        "core_row_id": "N", "taxon_id": "N", "parent": "R", "parent_resolution": "resolved",
        "scientific_name": "Echinoderma jacobi", "authorship": "(Vellinga & Knudsen) Gminder", "status": "valid"}],
        [("N", "langeparasollsopp")])
    species = [_sp("S", "Echinoderma jacobi", "(Vellinga & Knudsen) Gminder")]
    col1 = _col(tmp_path / "v1", "1", species)
    compile_release(normalized_source_dirs=[col1, nt1], output_dir=tmp_path / "r1", release_id="tax-2026.01.01-01",
                    manual_mappings_path=maps, mapping_policy_path=_POLICY_PATH, registry_path=reg)
    s_id = _ids(reg)[("col_xr", "col_usage_id", "S")]
    assert _ids(reg)[("nortaxa", "nortaxa_taxon_id", "N")] == s_id
    nt2 = _src(tmp_path / "v2", "nortaxa", "2", [root] + nt_new_rows, nt_new_vern)
    col2 = _col(tmp_path / "v2", "2", species if keep_species else [])
    maps2 = maps if keep_species else _write_manual_mappings(tmp_path / "m2.json", [])
    fallback = [{"sporely_taxon_id": s_id, "vanished_col_id": "S", "nortaxa_taxon_id": "N"}] if approve_fallback else []
    pins = {}
    for d in (col2, nt2):
        rep = json.loads((d / "report.json").read_text())
        pins[rep["profile_source_code"]] = {
            "source_release_id": f"{rep['profile_source_code']}:{_release_string(rep['profile_source_release'])}",
            "sha256": rep["archive_sha256"]}
    receipt = carry_forward.build_receipt(
        release_id="tax-2026.01.02-01", previous_dirs={"col_xr": col1, "nortaxa": nt1},
        current_dirs={"col_xr": col2, "nortaxa": nt2}, previous_pins=pins, current_pins=pins,
        manual_mappings=json.loads(maps2.read_text()), supersessions={"supersessions": []},
        anchors=carry_forward.registry_anchors(reg), nortaxa_fallback=fallback)
    path = tmp_path / "receipt.json"
    path.write_text(json.dumps(receipt))
    compile_release(normalized_source_dirs=[col2, nt2], output_dir=tmp_path / "r2", release_id="tax-2026.01.02-01",
                    manual_mappings_path=maps2, mapping_policy_path=_POLICY_PATH, registry_path=reg,
                    continuity_receipt_path=path)
    return s_id, receipt


VALID_N = [{"core_row_id": "N", "taxon_id": "N", "parent": "R", "parent_resolution": "resolved",
            "scientific_name": "Echinoderma jacobi", "authorship": "(Vellinga & Knudsen) Gminder", "status": "valid"}]


def test_vanished_col_anchor_without_approval_is_not_kept_alive(tmp_path):
    s_id, _ = _nortaxa_case(tmp_path, VALID_N, [("N", "langeparasollsopp")], keep_species=False)
    assert s_id not in _taxa(tmp_path / "r2")


def test_vanished_col_anchor_keeps_its_id_from_the_reviewed_valid_nortaxa_record(tmp_path):
    s_id, _ = _nortaxa_case(tmp_path, VALID_N, [("N", "langeparasollsopp")], keep_species=False,
                            approve_fallback=True)
    taxa = _taxa(tmp_path / "r2")
    assert taxa[s_id]["canonical_source_usage"] == {"source": "nortaxa", "namespace": "nortaxa_taxon_id",
                                                    "identifier": "N"}
    diag = json.loads((tmp_path / "r2" / "diagnostics.json").read_text())["source_continuity"]
    assert diag["nortaxa_fallback_canonical"][0]["sporely_taxon_id"] == s_id


def test_species_to_aggregate_source_change_keeps_species_names_and_redlist_on_the_species(tmp_path):
    s_id, receipt = _nortaxa_case(tmp_path, [{
        "core_row_id": "N", "taxon_id": "N", "parent": "R", "parent_resolution": "resolved",
        "scientific_name": "Echinoderma jacobi coll.", "authorship": "", "status": "valid"}],
        [("N", "langeparasollsopp-gruppen")], keep_species=True)
    record = receipt["records"]["aggregate-meaning-change:nortaxa/nortaxa_taxon_id/N"]
    assert record["status"] == "species_history"
    assert receipt["records"]["map-N-S"]["status"] == "carried"
    taxa = _taxa(tmp_path / "r2")
    # The species id stays the species (from the reviewed previous record), never the aggregate.
    assert taxa[s_id]["scientific_name"] == "Echinoderma jacobi"
    names = {(v["sporely_taxon_id"], v.get("vernacular_name") or v.get("name"))
             for v in map(json.loads, (tmp_path / "r2" / "vernacular.jsonl").read_text().splitlines())}
    assert (s_id, "langeparasollsopp") in names
    assert not any(n == "langeparasollsopp-gruppen" for _, n in names)
    usages = [json.loads(l) for l in (tmp_path / "r2" / "source_usages.jsonl").read_text().splitlines()]
    unique, _ = _build_redlist_resolution_index(usages)
    assert unique["N"] == (s_id, "Echinoderma jacobi")  # a red-list row on name-id N lands on the species


def test_aggregate_nortaxa_record_never_supplies_a_fallback(tmp_path):
    s_id, _ = _nortaxa_case(tmp_path, [{
        "core_row_id": "N", "taxon_id": "N", "parent": "R", "parent_resolution": "resolved",
        "scientific_name": "Echinoderma jacobi coll.", "authorship": "", "status": "valid"}],
        [], keep_species=False, approve_fallback=True)
    taxa = _taxa(tmp_path / "r2")
    # Species history keeps N on the species; the id is live from it, never as "coll.".
    assert all("coll." not in t["scientific_name"] for t in taxa.values())


def test_approved_mapping_to_a_renumbered_col_usage_follows_the_continuity(tmp_path):
    reg = tmp_path / "registry.jsonl"
    maps = _write_manual_mappings(tmp_path / "m.json", [{
        "mapping_id": "map-N-A", "review_status": "approved", "relationship": "exact",
        "source_usage": {"source": "nortaxa", "namespace": "nortaxa_taxon_id", "identifier": "N"},
        "target": {"source_usage": {"source": "col_xr", "namespace": "col_usage_id", "identifier": "A"}}}])
    nt = _src(tmp_path / "v1", "nortaxa", "1", [
        {"core_row_id": "R", "taxon_id": "R", "scientific_name": "Fungi", "rank": "kingdom"},
        {"core_row_id": "N", "taxon_id": "N", "parent": "R", "parent_resolution": "resolved",
         "scientific_name": "Cladonia squamosa", "authorship": "Hoffm.", "status": "valid"}])
    old = _col(tmp_path / "v1", "1", [_sp("A", "Cladonia squamosa", "Hoffm.")])
    compile_release(normalized_source_dirs=[old, nt], output_dir=tmp_path / "r1", release_id="tax-2026.01.01-01",
                    manual_mappings_path=maps, mapping_policy_path=_POLICY_PATH, registry_path=reg)
    a_id = _ids(reg)[("col_xr", "col_usage_id", "A")]
    new = _col(tmp_path / "v2", "2", [_sp("A2", "Cladonia squamosa", "Hoffm.")])
    path, receipt = _receipt(tmp_path, {"col_xr": old, "nortaxa": nt}, {"col_xr": new, "nortaxa": nt},
                             json.loads(maps.read_text()), {"supersessions": []}, reg,
                             continuity=[{"old_col_id": "A", "new_col_id": "A2", "sporely_taxon_id": a_id}])
    assert receipt["records"]["map-N-A"]["status"] == "carried"
    compile_release(normalized_source_dirs=[new, nt], output_dir=tmp_path / "r2", release_id="tax-2026.01.02-01",
                    manual_mappings_path=maps, mapping_policy_path=_POLICY_PATH, registry_path=reg,
                    continuity_receipt_path=path)
    ids = _ids(reg)
    assert ids[("nortaxa", "nortaxa_taxon_id", "N")] == a_id == ids[("col_xr", "col_usage_id", "A2")]
    assert a_id in _taxa(tmp_path / "r2")


def test_vernaculars_of_a_withheld_review_usage_are_dropped_not_fatal(tmp_path):
    reg = tmp_path / "registry.jsonl"
    common = dict(manual_mappings_path=_write_manual_mappings(tmp_path / "m.json", []),
                  mapping_policy_path=_POLICY_PATH, registry_path=reg)
    old = _col(tmp_path / "v1", "1", [_sp("B", "Echinoderma carinii")])
    compile_release(normalized_source_dirs=[old], output_dir=tmp_path / "r1", release_id="tax-2026.01.01-01", **common)
    new = _col(tmp_path / "v2", "2", [_sp("B2", "Echinoderma carinii")])
    cores = {json.loads(l)["taxon_id"]["value"]: json.loads(l)["core_row_id"] for l in (new / "taxa.jsonl").read_text().splitlines()}
    (new / "vernacular.jsonl").write_text(json.dumps({
        "source_code": "col_xr", "source_release": {"version": "2", "issued_date": "2026-01-01"},
        "core_row_id": cores["B2"], "language": "en", "is_preferred": True, "vernacular_name": "x"}) + "\n")
    path, _ = _receipt(tmp_path, {"col_xr": old}, {"col_xr": new}, {"mappings": []}, {"supersessions": []}, reg)
    compile_release(normalized_source_dirs=[new], output_dir=tmp_path / "r2", release_id="tax-2026.01.02-01",
                    continuity_receipt_path=path, **common)
    assert ("col_xr", "col_usage_id", "B2") not in _ids(reg)


def test_r2_refresh_supersessions_are_candidate_only_and_cite_the_owner_decision():
    ledger = json.loads((SCRIPTS.parent / "policies" / "concept_supersessions.yml").read_text(encoding="utf-8"))
    r2 = [s for s in ledger["supersessions"] if s["supersession_id"].startswith("taxonomy-r2-")]
    assert [s["superseded_sporely_taxon_id"] for s in r2] == [624546, 152153]
    for s in r2:
        assert s["approval_scope"] == "candidate_build_only" and s["release_approval"] == "pending"
        assert "Owner decision 2026-10-24" in s["rationale"]


# ----- name-record transfers, live duplicates, authorship/year (R2e) ---------

def _with_records(directory: Path, records: dict[str, list[str]]) -> Path:
    rows = [json.loads(l) for l in (directory / "taxa.jsonl").read_text().splitlines()]
    for r in rows:
        r["name_record_ids"] = records.get(r["taxon_id"]["value"], [])
    (directory / "taxa.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
    return directory


def _two_releases(tmp_path, v1_rows, v2_rows, v1_records=None, v2_records=None, nortaxa_rows=None, continuity=()):
    reg = tmp_path / "registry.jsonl"
    common = dict(manual_mappings_path=_write_manual_mappings(tmp_path / "m.json", []),
                  mapping_policy_path=_POLICY_PATH, registry_path=reg)
    nt = (_src(tmp_path / "nt", "nortaxa", "1", [{"core_row_id": "R", "taxon_id": "R", "scientific_name": "Fungi",
                                                    "rank": "kingdom"}] + nortaxa_rows) if nortaxa_rows else None)
    old = _with_records(_col(tmp_path / "v1", "1", v1_rows), v1_records or {})
    compile_release(normalized_source_dirs=[old] + ([nt] if nt else []), output_dir=tmp_path / "r1",
                    release_id="tax-2026.01.01-01", **common)
    before = _ids(reg)
    new = _with_records(_col(tmp_path / "v2", "2", v2_rows), v2_records or {})
    prev, cur = {"col_xr": old}, {"col_xr": new}
    if nt:
        prev["nortaxa"] = cur["nortaxa"] = nt
    path, receipt = _receipt(tmp_path, prev, cur, {"mappings": []}, {"supersessions": []}, reg, continuity=continuity)
    compile_release(normalized_source_dirs=[new] + ([nt] if nt else []), output_dir=tmp_path / "r2",
                    release_id="tax-2026.01.02-01", continuity_receipt_path=path, **common)
    diag = json.loads((tmp_path / "r2" / "diagnostics.json").read_text())["source_continuity"]
    return before, _ids(reg), _taxa(tmp_path / "r2"), receipt, diag


def test_two_col_ids_exchanging_name_records_are_held_not_relabelled(tmp_path):
    before, after, taxa, receipt, diag = _two_releases(
        tmp_path,
        [_sp("A", "Kaskaskia macrospore", "G.C. Zhao"), _sp("B", "Kaskaskia macrospora", "G.C. Zhao")],
        [_sp("A", "Kaskaskia macrospora", "G.C. Zhao"), _sp("B", "Kaskaskia macrospore", "G.C. Zhao")],
        {"A": ["indexfungorum:800412"], "B": ["indexfungorum:840860"]},
        {"A": ["indexfungorum:840860"], "B": ["indexfungorum:800412"]})
    a, b = before[("col_xr", "col_usage_id", "A")], before[("col_xr", "col_usage_id", "B")]
    assert {t["col_id"] for t in receipt["col_continuity_candidates"]["name_record_transfers"]} == {"A", "B"}
    assert a not in taxa and b not in taxa  # held: neither id takes the other's name record
    assert set(diag["col_continuity_review_withheld"]) == {"A", "B"}
    assert after == before  # nothing rebound, nothing allocated


def test_spelling_correction_on_the_same_name_record_is_not_held(tmp_path):
    before, after, taxa, receipt, _ = _two_releases(
        tmp_path, [_sp("A", "Boletellus lepidospora", "Hongo")], [_sp("A", "Boletellus lepidosporus", "Hongo")],
        {"A": ["indexfungorum:1"]}, {"A": ["indexfungorum:1"]})
    a = before[("col_xr", "col_usage_id", "A")]
    assert receipt["col_continuity_candidates"]["name_record_transfers"] == []
    assert taxa[a]["scientific_name"] == "Boletellus lepidosporus"


def test_new_col_usage_exactly_duplicating_a_live_concept_reuses_its_id(tmp_path):
    before, after, taxa, _, diag = _two_releases(
        tmp_path, [_sp("A", "Entoloma sericeum", "Quél.")],
        [_sp("A", "Entoloma sericeum", "Quél."), _sp("C", "Entoloma sericeum", "Quél.")])
    a = before[("col_xr", "col_usage_id", "A")]
    assert after[("col_xr", "col_usage_id", "C")] == a  # append-only alias, no new permanent id
    assert len(set(after.values())) == len(set(before.values()))
    assert diag["live_duplicate_bound"][0]["sporely_taxon_id"] == a


def test_new_col_usage_with_different_authorship_is_withheld_not_allocated(tmp_path):
    before, after, taxa, _, diag = _two_releases(
        tmp_path, [_sp("A", "Entoloma sericeum", "Quél.")],
        [_sp("A", "Entoloma sericeum", "Quél."), _sp("C", "Entoloma sericeum", "(Bull.) Quél.")])
    assert ("col_xr", "col_usage_id", "C") not in after
    assert before[("col_xr", "col_usage_id", "A")] in taxa
    assert diag["live_duplicate_withheld"]["C"]["class"] == "different_authorship_or_classification"


def test_new_col_usage_matching_a_live_nortaxa_concept_reuses_it_only_when_exact(tmp_path):
    nt = [{"core_row_id": "N", "taxon_id": "N", "parent": "R", "parent_resolution": "resolved",
           "scientific_name": "Nosema apis", "authorship": "Zander", "status": "valid"}]
    before, after, taxa, _, diag = _two_releases(
        tmp_path, [], [_sp("C", "Nosema apis", "Zander")], nortaxa_rows=nt)
    n = before[("nortaxa", "nortaxa_taxon_id", "N")]
    assert after[("col_xr", "col_usage_id", "C")] == n and n in taxa


def test_authorship_year_change_on_the_same_name_record_is_a_continuity_candidate(tmp_path):
    reg_rows = [_sp("A", "Russula xerampelina", "(Schaeff.) Fr. 1838")]
    before, after, taxa, receipt, diag = _two_releases(
        tmp_path, reg_rows, [_sp("A2", "Russula xerampelina", "(Schaeff.) Fr.")],
        {"A": ["indexfungorum:9"]}, {"A2": ["indexfungorum:9"]})
    pairs = receipt["col_continuity_candidates"]["pairs"]
    assert pairs == [{"old_col_id": "A", "new_col_id": "A2", "sporely_taxon_id": before[("col_xr", "col_usage_id", "A")],
                      "scientific_name": "Russula xerampelina", "rank": "species",
                      "evidence": "same_name_record_authorship_differs"}]
    assert ("col_xr", "col_usage_id", "A2") not in after  # unapproved: reviewed, not allocated
    # A substantively different citation without a shared record stays out of the pairs.
    assert "A2" in diag["col_continuity_review_withheld"]


def test_normalizer_reads_index_fungorum_and_alternative_record_ids():
    from normalize_col_xr import _name_record_ids
    assert _name_record_ids("http://www.indexfungorum.org/Names/NamesRecord.asp?RecordID=800412",
                            "wikidata:Q107588245", "") == ["indexfungorum:800412", "wikidata:Q107588245"]
    assert _name_record_ids("", "", "") == []


def test_names_of_a_live_duplicate_alias_do_not_fail_the_compile(tmp_path):
    reg = tmp_path / "registry.jsonl"
    common = dict(manual_mappings_path=_write_manual_mappings(tmp_path / "m.json", []),
                  mapping_policy_path=_POLICY_PATH, registry_path=reg)
    old = _col(tmp_path / "v1", "1", [_sp("A", "Entoloma sericeum", "Quél.")])
    compile_release(normalized_source_dirs=[old], output_dir=tmp_path / "r1", release_id="tax-2026.01.01-01", **common)
    new = _col(tmp_path / "v2", "2", [_sp("A", "Entoloma sericeum", "Quél."), _sp("C", "Entoloma sericeum", "Quél.")])
    cores = {json.loads(l)["taxon_id"]["value"]: json.loads(l)["core_row_id"] for l in (new / "taxa.jsonl").read_text().splitlines()}
    (new / "vernacular.jsonl").write_text(json.dumps({
        "source_code": "col_xr", "source_release": {"version": "2", "issued_date": "2026-01-01"},
        "core_row_id": cores["C"], "language": "en", "is_preferred": True, "vernacular_name": "silky pinkgill"}) + "\n")
    path, _ = _receipt(tmp_path, {"col_xr": old}, {"col_xr": new}, {"mappings": []}, {"supersessions": []}, reg)
    compile_release(normalized_source_dirs=[new], output_dir=tmp_path / "r2", release_id="tax-2026.01.02-01",
                    continuity_receipt_path=path, **common)
    diag = json.loads((tmp_path / "r2" / "diagnostics.json").read_text())["source_continuity"]
    assert diag["live_duplicate_vernaculars_dropped"] == 1
    # Second compile on the grown registry (as build_release's second build does).
    compile_release(normalized_source_dirs=[new], output_dir=tmp_path / "r3", release_id="tax-2026.01.02-01",
                    continuity_receipt_path=path, **common)
    diag = json.loads((tmp_path / "r3" / "diagnostics.json").read_text())["source_continuity"]
    assert diag["live_duplicate_vernaculars_dropped"] == 1


def test_name_record_transfer_is_seen_when_another_identifier_stayed(tmp_path):
    before, after, taxa, receipt, diag = _two_releases(
        tmp_path,
        [_sp("XQVS", "Conocybe ochraceodiscus", "Watling"), _sp("XQVT", "Conocybe ochraceodisca", "Watling")],
        [_sp("XQVS", "Conocybe ochraceodisca", "Watling"), _sp("XQVT", "Conocybe ochraceodiscus", "Watling")],
        {"XQVS": ["indexfungorum:344355"], "XQVT": ["indexfungorum:311682", "wikidata:Q107588245"]},
        {"XQVS": ["indexfungorum:311682"], "XQVT": ["indexfungorum:344355", "wikidata:Q107588245"]})
    assert {t["col_id"] for t in receipt["col_continuity_candidates"]["name_record_transfers"]} == {"XQVS", "XQVT"}


def test_second_compile_on_the_grown_registry_reports_the_same_diagnostics(tmp_path):
    reg = tmp_path / "registry.jsonl"
    common = dict(manual_mappings_path=_write_manual_mappings(tmp_path / "m.json", []),
                  mapping_policy_path=_POLICY_PATH, registry_path=reg)
    old = _col(tmp_path / "v1", "1", [_sp("A", "Entoloma sericeum", "Quél."), _sp("B", "Entoloma sinuatum", "Gillet")])
    compile_release(normalized_source_dirs=[old], output_dir=tmp_path / "r1", release_id="tax-2026.01.01-01", **common)
    new = _col(tmp_path / "v2", "2", [_sp("A", "Entoloma sericeum", "Quél."), _sp("C", "Entoloma sericeum", "Quél."),
                                      _sp("B", "Entoloma sinuatum", "Gillet"), _sp("D", "Entoloma sinuatum", "Fr.")])
    path, _ = _receipt(tmp_path, {"col_xr": old}, {"col_xr": new}, {"mappings": []}, {"supersessions": []}, reg)
    outs = []
    for name in ("b1", "b2"):
        compile_release(normalized_source_dirs=[new], output_dir=tmp_path / name, release_id="tax-2026.01.02-01",
                        continuity_receipt_path=path, **common)
        outs.append({f.name: f.read_bytes() for f in sorted((tmp_path / name).iterdir()) if f.is_file()})
    diag = json.loads(outs[0]["diagnostics.json"])["source_continuity"]
    assert [b["usage"] for b in diag["live_duplicate_bound"]] == ["col_xr/col_usage_id/C"]
    assert list(diag["live_duplicate_withheld"]) == ["D"]
    assert outs[0] == outs[1]  # diagnostics and the manifest (hence its hash) are identical


def test_approved_name_record_follow_keeps_each_id_with_its_original_record(tmp_path):
    rows1 = [_sp("A", "Kaskaskia macrospore", "G.C. Zhao"), _sp("B", "Kaskaskia macrospora", "G.C. Zhao")]
    rows2 = [_sp("A", "Kaskaskia macrospora", "G.C. Zhao"), _sp("B", "Kaskaskia macrospore", "G.C. Zhao")]
    recs1 = {"A": ["indexfungorum:800412"], "B": ["indexfungorum:840860"]}
    recs2 = {"A": ["indexfungorum:840860"], "B": ["indexfungorum:800412"]}
    before, after, taxa, receipt, _ = _two_releases(tmp_path, rows1, rows2, recs1, recs2)
    a, b = before[("col_xr", "col_usage_id", "A")], before[("col_xr", "col_usage_id", "B")]
    proposals = receipt["name_record_follow"]
    assert [(e["sporely_taxon_id"], e["follows_col_id"], e["status"]) for e in proposals] == \
        sorted([(a, "B", "proposed"), (b, "A", "proposed")])
    assert a not in taxa and b not in taxa  # proposed only: still held
    # One member approved is not enough.
    half = carry_forward.name_record_follow(receipt["col_continuity_candidates"]["name_record_transfers"],
                                            [{"sporely_taxon_id": a, "col_id": "A", "follows_col_id": "B"}])
    assert {e["status"] for e in half} == {"proposed"}
    approved = [{"sporely_taxon_id": e["sporely_taxon_id"], "col_id": e["col_id"], "follows_col_id": e["follows_col_id"]}
                for e in proposals]
    receipt["name_record_follow"] = carry_forward.name_record_follow(
        receipt["col_continuity_candidates"]["name_record_transfers"], approved)
    path = tmp_path / "receipt-approved.json"
    path.write_text(json.dumps(receipt))
    new = tmp_path / "v2" / "col_xr"
    compile_release(normalized_source_dirs=[new], output_dir=tmp_path / "r3", release_id="tax-2026.01.02-01",
                    continuity_receipt_path=path, manual_mappings_path=_write_manual_mappings(tmp_path / "m.json", []),
                    mapping_policy_path=_POLICY_PATH, registry_path=tmp_path / "registry.jsonl")
    taxa = _taxa(tmp_path / "r3")
    assert taxa[a]["scientific_name"] == "Kaskaskia macrospore"  # its R1 name / IF 800412, now on COL B
    assert taxa[b]["scientific_name"] == "Kaskaskia macrospora"
    assert _ids(tmp_path / "registry.jsonl") == before  # no key rebound, nothing allocated


def test_supersession_reviewed_in_the_current_release_carries_its_vanished_retired_usage(tmp_path):
    reg = tmp_path / "registry.jsonl"
    old = _col(tmp_path / "v1", "1", [_sp("A", "Thyrea granatica", ""), _sp("B", "Lichinella cribellifera", "(Nyl.) Moreno & Egea")])
    compile_release(normalized_source_dirs=[old], output_dir=tmp_path / "r1", release_id="tax-2026.01.01-01",
                    manual_mappings_path=_write_manual_mappings(tmp_path / "m.json", []),
                    mapping_policy_path=_POLICY_PATH, registry_path=reg)
    a = _ids(reg)[("col_xr", "col_usage_id", "A")]
    new = _col(tmp_path / "v2", "2", [_sp("B", "Lichinella cribellifera", "(Nyl.) Moreno & Egea")])

    def status(last):
        sup = {"supersessions": [{"supersession_id": "s", "superseded_sporely_taxon_id": a, "review_status": "approved",
                                  "current_source_usage": {"source": "col_xr", "namespace": "col_usage_id", "identifier": "B"},
                                  "source_release_range": {"first": "1", "last": last}}]}
        _, receipt = _receipt(tmp_path, {"col_xr": old}, {"col_xr": new}, {"mappings": []}, sup, reg)
        return receipt["records"]["s"]

    current = status("2")  # reviewed against the release in which A vanished
    assert current["status"] == "carried"
    assert current["carried_by"] == ["col_xr/col_usage_id/A:retired_usage_vanished_reviewed_in_current_release"]
    stale = status("1")  # reviewed earlier: the vanished usage is a change since review
    assert stale["status"] == "flagged" and stale["reasons"] == ["col_xr/col_usage_id/A:missing"]


def test_lookup_by_current_col_id_of_a_followed_swap_returns_the_concept_now_carrying_that_name(tmp_path):
    from build_sqlite_candidate import build_candidate
    import sqlite3
    rows1 = [_sp("A", "Kaskaskia macrospore", "G.C. Zhao"), _sp("B", "Kaskaskia macrospora", "G.C. Zhao")]
    rows2 = [_sp("A", "Kaskaskia macrospora", "G.C. Zhao"), _sp("B", "Kaskaskia macrospore", "G.C. Zhao")]
    before, _, _, receipt, _ = _two_releases(tmp_path, rows1, rows2, {"A": ["indexfungorum:800412"], "B": ["indexfungorum:840860"]},
                                             {"A": ["indexfungorum:840860"], "B": ["indexfungorum:800412"]})
    a, b = before[("col_xr", "col_usage_id", "A")], before[("col_xr", "col_usage_id", "B")]
    receipt["name_record_follow"] = carry_forward.name_record_follow(
        receipt["col_continuity_candidates"]["name_record_transfers"],
        [{"sporely_taxon_id": e["sporely_taxon_id"], "col_id": e["col_id"], "follows_col_id": e["follows_col_id"]}
         for e in receipt["name_record_follow"]])
    path = tmp_path / "receipt-approved.json"
    path.write_text(json.dumps(receipt))
    compile_release(normalized_source_dirs=[tmp_path / "v2" / "col_xr"], output_dir=tmp_path / "r3",
                    release_id="tax-2026.01.02-01", continuity_receipt_path=path,
                    manual_mappings_path=_write_manual_mappings(tmp_path / "m.json", []),
                    mapping_policy_path=_POLICY_PATH, registry_path=tmp_path / "registry.jsonl")
    db = tmp_path / "c.sqlite3"
    build_candidate(release_dir=tmp_path / "r3", registry_path=tmp_path / "registry.jsonl", output_db=db)
    c = sqlite3.connect(db)
    lookup = lambda col: c.execute("SELECT taxon_id, external_name FROM taxon_external_id_text_min "
                                   "WHERE source_system='col_xr' AND namespace='col_usage_id' AND external_id=?",
                                   (col,)).fetchall()
    # In the current COL release A carries "macrospora" (IF 840860), now B's concept.
    assert lookup("A") == [(b, "Kaskaskia macrospora")]
    assert lookup("B") == [(a, "Kaskaskia macrospore")]
    assert dict(c.execute("SELECT taxon_id, canonical_external_id FROM taxon_min WHERE taxon_id IN (?,?)", (a, b))) \
        == {a: "B", b: "A"}
    assert _ids(tmp_path / "registry.jsonl") == before  # registry unchanged
