"""An existing Sporely id never takes a species->aggregate meaning in place."""
from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from carry_forward import Snapshot, anchor_meaning_changes, is_aggregate_name  # noqa: E402
from compile_release import continuity_held_usages  # noqa: E402


def snap(name: str, status: str = "valid", author: str = "(Knudsen) Bon") -> list[Snapshot]:
    return [Snapshot(name, author, "species", status, "", "", "Fungi")]


def test_aggregate_markers() -> None:
    for name in ("Echinoderma pseudoasperula coll.", "Russula xerampelina agg.", "Amanita s. lat.",
                 "Cortinarius sp. group", "Boletus edulis s.l."):
        assert is_aggregate_name(name), name
    for name in ("Echinoderma pseudoasperula", "Collybia dryophila", "Agaricus aggregatus"):
        assert not is_aggregate_name(name), name


def test_nortaxa_species_renamed_to_coll_keeps_species_history() -> None:
    usage = ("nortaxa", "nortaxa_taxon_id", "52027")
    old = {usage: snap("Echinoderma pseudoasperula")}
    new = {usage: snap("Echinoderma pseudoasperula coll.", "synonym")}
    records, swaps = anchor_meaning_changes(old, new, {624546: usage})
    record = records["aggregate-meaning-change:nortaxa/nortaxa_taxon_id/52027"]
    assert record["status"] == "species_history" and record["sporely_taxon_id"] == 624546
    assert record["kind"] == "aggregate_meaning_change"
    # Not withheld: the compile keeps the species record (see test_refresh_identity_continuity).
    assert continuity_held_usages({"records": records}) == set()
    assert swaps == []


def test_reviewed_non_anchor_usage_is_checked_too() -> None:
    usage = ("nortaxa", "nortaxa_taxon_id", "52025")
    records, _ = anchor_meaning_changes({usage: snap("Echinoderma jacobi")},
                                        {usage: snap("Echinoderma jacobi coll.")}, {}, [usage])
    assert records["aggregate-meaning-change:nortaxa/nortaxa_taxon_id/52025"]["status"] == "species_history"


def test_backbone_aggregate_change_is_flagged_and_unchanged_names_pass() -> None:
    col = ("col_xr", "col_usage_id", "ABC")
    nt = ("nortaxa", "nortaxa_taxon_id", "1")
    old = {col: snap("Amanita muscaria"), nt: snap("Amanita muscaria coll.")}
    new = {col: snap("Amanita muscaria agg."), nt: snap("Amanita muscaria coll.")}
    records, _ = anchor_meaning_changes(old, new, {1: col, 2: nt})
    assert list(records) == ["aggregate-meaning-change:col_xr/col_usage_id/ABC"]
    assert records["aggregate-meaning-change:col_xr/col_usage_id/ABC"]["status"] == "flagged"
    assert continuity_held_usages({"records": records}) == set()


def test_name_swap_between_anchored_usages_is_reported() -> None:
    a, b = ("col_xr", "col_usage_id", "3R3HS"), ("col_xr", "col_usage_id", "9ZNFC")
    old = {a: snap("Kaskaskia macrospore", "accepted"), b: snap("Kaskaskia macrospora", "accepted")}
    new = {a: snap("Kaskaskia macrospora", "accepted"), b: snap("Kaskaskia macrospore", "accepted")}
    records, swaps = anchor_meaning_changes(old, new, {20250: a, 137682: b})
    assert records == {}
    assert swaps == [{"sporely_taxon_ids": [20250, 137682], "usages": ["col_xr/col_usage_id/3R3HS",
                                                                        "col_xr/col_usage_id/9ZNFC"],
                      "names_before": ["Kaskaskia macrospore", "Kaskaskia macrospora"], "same_authorship": True}]
