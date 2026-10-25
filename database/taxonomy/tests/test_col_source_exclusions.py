"""UNITE SH clusters are excluded from COL compilation by the approved scope
policy rule; named UNITE concepts stay. Excluded allocations are reported as
policy exclusions, not unexplained losses."""
from __future__ import annotations

import json
import sqlite3
import sys
import zipfile
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from normalize_col_xr import (  # noqa: E402
    ColNormalizeError,
    load_source_exclusions,
    normalize_col_xr,
)
import prepare_refresh  # noqa: E402

POLICY = Path(__file__).resolve().parents[1] / "policies" / "global-macrofungi-scope.yml"
HEADER = "\t".join(["col:ID", "col:sourceID", "col:parentID", "col:status", "col:scientificName",
                    "col:authorship", "col:rank", "col:kingdom"])
ROWS = [
    ("F", "", "", "accepted", "Fungi", "", "kingdom", ""),
    ("G", "", "F", "accepted", "Russula", "", "genus", "Fungi"),
    # Genuine UNITE species hypothesis: UNITE source, unranked, name == id.
    ("SH1234567.10FU", "30477", "G", "accepted", "SH1234567.10FU", "", "unranked", "Fungi"),
    # Named UNITE concepts (ranked, Latin names) are kept.
    ("35WJB", "30477", "G", "accepted", "Russula unitensis", "Author", "species", "Fungi"),
    ("SH3KM", "30477", "G", "accepted", "Russula shkm", "Author", "species", "Fungi"),
    # SH-shaped cluster from another source: the source check keeps it.
    ("SH7654321.10FU", "2073", "G", "accepted", "SH7654321.10FU", "", "unranked", "Fungi"),
    # UNITE unranked usage whose name is not its identifier: kept.
    ("SH9999999.10FU", "30477", "G", "accepted", "Russula sp. SH9999999", "", "unranked", "Fungi"),
]


def _archive(path: Path, rows=ROWS) -> Path:
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("NameUsage.tsv", HEADER + "\n" + "\n".join("\t".join(r) for r in rows) + "\n")
        zf.writestr("source/30477.yaml", "---\nkey: 30477\nalias: UNITE\nversion: 10.0\n")
        zf.writestr("source/2073.yaml", "---\nkey: 2073\nalias: Species Fungorum Plus\n")
    return path


def _ids(output: Path) -> set[str]:
    return {json.loads(line)["taxon_id"]["value"] for line in (output / "taxa.jsonl").read_text().splitlines()}


def _normalize(tmp_path: Path, policy: Path | None, name: str = "out") -> tuple[dict, Path]:
    out = tmp_path / name
    report = normalize_col_xr(archive_path=_archive(tmp_path / f"{name}.zip"), output_dir=out,
                              source_release={"version": "t", "issued_date": "2026-09-25"},
                              source_exclusion_policy=policy)
    return report, out


def test_genuine_unite_sh_cluster_is_excluded_named_unite_concepts_kept(tmp_path: Path) -> None:
    report, out = _normalize(tmp_path, POLICY)
    assert _ids(out) == {"F", "G", "35WJB", "SH3KM", "SH7654321.10FU", "SH9999999.10FU"}
    excluded = [json.loads(line) for line in (out / "excluded_by_policy.jsonl").read_text().splitlines()]
    assert excluded == [{"code": "exclude_unite_sequence_clusters", "col_usage_id": "SH1234567.10FU"}]
    exclusions = report["source_exclusions"]
    assert exclusions["excluded_by_code"] == {"exclude_unite_sequence_clusters": 1}
    check = exclusions["checks"]["exclude_unite_sequence_clusters"]
    assert (check["rank"], check["col_id_prefix"], check["source_alias"]) == ("unranked", "SH", "UNITE")
    assert check["label"] == "excluded_by_policy:unite_sh_cluster"
    assert report["record_counts"]["ExcludedByPolicy"] == 1


def test_without_policy_nothing_is_excluded(tmp_path: Path) -> None:
    report, out = _normalize(tmp_path, None)
    assert "SH1234567.10FU" in _ids(out)
    assert "source_exclusions" not in report
    assert not (out / "excluded_by_policy.jsonl").exists()


def test_excluded_cluster_is_not_pulled_back_as_ancestor(tmp_path: Path) -> None:
    rows = ROWS + [("CHILD", "30477", "SH1234567.10FU", "accepted", "Russula child", "A", "species", "Fungi")]
    out = tmp_path / "anc"
    report = normalize_col_xr(archive_path=_archive(tmp_path / "anc.zip", rows), output_dir=out,
                              source_release={"version": "t", "issued_date": "2026-09-25"},
                              source_exclusion_policy=POLICY)
    assert "SH1234567.10FU" not in _ids(out) and "CHILD" in _ids(out)
    assert report["reference_gaps"]["orphan_parent_reference_count"] == 1


def test_approved_exclusion_without_source_check_is_refused(tmp_path: Path) -> None:
    policy = json.loads(POLICY.read_text())
    policy["source_characteristic_exclusions"].append(dict(policy["source_characteristic_exclusions"][0],
                                                           code="exclude_something_else"))
    path = tmp_path / "policy.yml"
    path.write_text(json.dumps(policy))
    with pytest.raises(ColNormalizeError, match="no normalizer source check"):
        load_source_exclusions(path)


def _sqlite(path: Path, live: set[int]) -> Path:
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE taxon_min (taxon_id INTEGER)")
        conn.execute("CREATE TABLE taxon_supersession_min (retired_taxon_id INTEGER, current_taxon_id INTEGER)")
        conn.execute("CREATE TABLE taxonomy_meta (key TEXT, value TEXT)")
        conn.executemany("INSERT INTO taxon_min VALUES (?)", [(i,) for i in sorted(live)])
    return path


def test_identity_comparison_separates_policy_exclusions(tmp_path: Path) -> None:
    _, out = _normalize(tmp_path, POLICY)
    anchors = {1: ("col_xr", "col_usage_id", "G"), 2: ("col_xr", "col_usage_id", "SH1234567.10FU"),
               3: ("col_xr", "col_usage_id", "GONE")}
    excluded = prepare_refresh.policy_exclusions(out, anchors)
    assert excluded == {2: "excluded_by_policy:unite_sh_cluster"}
    result = prepare_refresh.identity_comparison(_sqlite(tmp_path / "a.sqlite3", {1, 2, 3}),
                                                 _sqlite(tmp_path / "b.sqlite3", {1}), excluded)
    assert result["stopped_excluded_by_policy"] == {"excluded_by_policy:unite_sh_cluster": 1}
    assert result["stopped_unexplained"] == [3]
    assert result["policy_excluded_but_live"] == []


def test_base_backing_is_a_diagnostic_tier(tmp_path: Path) -> None:
    _, out = _normalize(tmp_path, POLICY)
    base_zip = tmp_path / "base.zip"
    base_rows = [("G", "", "F", "accepted", "Russula", "", "genus", "Fungi"),
                 ("35WJB", "", "G", "accepted", "Russula unitensis", "A", "species", "Fungi"),
                 ("SH3KM", "", "G", "accepted", "Russula other", "A", "species", "Fungi")]
    with zipfile.ZipFile(base_zip, "w") as zf:
        zf.writestr("NameUsage.tsv", HEADER + "\n" + "\n".join("\t".join(r) for r in base_rows) + "\n")
    base = prepare_refresh.col_base_usages(base_zip)
    assert set(base) == {"G", "35WJB", "SH3KM"}
    tiers = prepare_refresh.base_backing(out, base)
    assert tiers["base_backed"] == {"G", "35WJB"}
    assert tiers["base_id_differs"] == {"SH3KM"}
    assert tiers["xr_only"] == {"F", "SH7654321.10FU", "SH9999999.10FU"}


def test_vanished_unite_cluster_is_classified_by_policy_not_unexplained(tmp_path: Path) -> None:
    # Current release: the cluster SH1234567.10FU is gone from COL entirely.
    rows = [r for r in ROWS if r[0] != "SH1234567.10FU"]
    out = tmp_path / "cur"
    normalize_col_xr(archive_path=_archive(tmp_path / "cur.zip", rows), output_dir=out,
                     source_release={"version": "t", "issued_date": "2026-09-25"}, source_exclusion_policy=POLICY)
    previous = _archive(tmp_path / "prev.zip")  # previous release still had it
    anchors = {2: ("col_xr", "col_usage_id", "SH1234567.10FU"), 4: ("col_xr", "col_usage_id", "35WJB"),
               5: ("col_xr", "col_usage_id", "GONE")}
    assert prepare_refresh.policy_exclusions(out, anchors) == {}
    excluded = prepare_refresh.policy_exclusions(out, anchors, previous)
    assert excluded == {2: "excluded_by_policy:unite_sh_cluster"}
    result = prepare_refresh.identity_comparison(_sqlite(tmp_path / "a.sqlite3", {2, 4, 5}),
                                                 _sqlite(tmp_path / "b.sqlite3", {4}), excluded)
    assert result["stopped_excluded_by_policy"] == {"excluded_by_policy:unite_sh_cluster": 1}
    assert result["stopped_unexplained"] == [5]
    # Headline counters reconcile: every stopped id is in exactly one bucket.
    assert result["stopped_being_live"] == (result["stopped_by_supersession"] + len(result["stopped_unexplained"])
                                            + sum(result["stopped_excluded_by_policy"].values())
                                            + sum(result["stopped_held_by_continuity"].values()))


def test_col_continuity_holds_are_classified_as_held_not_unexplained(tmp_path: Path) -> None:
    receipt = {"records": {}, "col_id_continuity": [{"old_col_id": "P2", "new_col_id": "N2"}],
               "col_continuity_candidates": {
                   "vanished_anchors": {"P1": {"sporely_taxon_id": 3}, "P2": {"sporely_taxon_id": 4},
                                        "Q1": {"sporely_taxon_id": 6}, "Q2": {"sporely_taxon_id": 7}},
                   "name_record_transfers": [{"col_id": "A", "sporely_taxon_id": 1},
                                             {"col_id": "B", "sporely_taxon_id": 2}],
                   "pairs": [{"old_col_id": "P1", "new_col_id": "N1", "sporely_taxon_id": 3},
                             {"old_col_id": "P2", "new_col_id": "N2", "sporely_taxon_id": 4}],
                   "ambiguous": [{"old_col_ids": ["Q1", "Q2"], "new_col_ids": ["M1", "M2"]}]}}
    held = prepare_refresh.held_by_continuity(receipt, {})
    assert held == {1: "held:name_record_transfer", 2: "held:name_record_transfer",
                    3: "held:col_continuity_unapproved", 6: "held:col_continuity_unapproved",
                    7: "held:col_continuity_unapproved"}
    result = prepare_refresh.identity_comparison(_sqlite(tmp_path / "a.sqlite3", {1, 2, 3, 4, 5, 6, 7, 8}),
                                                 _sqlite(tmp_path / "b.sqlite3", {8}), None, held)
    assert result["stopped_held_by_continuity"] == {"held:col_continuity_unapproved": 3,
                                                    "held:name_record_transfer": 2}
    assert result["stopped_unexplained"] == [4, 5]  # approved continuity that did not resolve; plain loss
