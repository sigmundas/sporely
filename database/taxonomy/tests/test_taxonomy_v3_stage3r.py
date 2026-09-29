"""Taxonomy v3 Stage 3R: release-build inputs follow concept supersessions.

A pinned publishing-id input (Artportalen overlay, iNaturalist refresh) whose
entry is keyed to a concept a reviewed supersession retired is re-keyed at
build time to the surviving concept, with the retired id and the
``supersession_id`` kept as provenance. The committed inputs are not
rewritten; collisions, unknown ids and unresolvable supersessions fail.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

_TAXONOMY = Path(__file__).resolve().parents[1]
_REPO = _TAXONOMY.parents[1]
_SCRIPTS = _TAXONOMY / "scripts"
for _path in (_SCRIPTS, _TAXONOMY, _REPO):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

import artportalen_overlay as ao  # noqa: E402
import build_release as br  # noqa: E402
import publishing_ids as pid  # noqa: E402
from build_sqlite_candidate import BuildError, _release_supersessions, build_candidate  # noqa: E402

from test_taxonomy_v3_stage3p import _superseded_release, _taxon  # noqa: E402

SUPERSEDED = {624588: pid.Supersession("sup-624588", 78915)}
OVERLAY_ENTRY = {"sporely_taxon_id": 624588, "scientific_name": "Amanita muscaria",
                 "artportalen_taxon_id": 236537, "artportalen_scientific_name": "Amanita muscaria s.str.",
                 "decision": "accepted_after_review:split", "accepted_by": "t", "accepted_on": "x"}
PROVENANCE = ";superseded_from:624588;supersession:sup-624588"


def _conn(concepts):
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE taxon_min (taxon_id INTEGER PRIMARY KEY, canonical_scientific_name TEXT, "
                 "inaturalist_taxon_id INTEGER)")
    conn.executemany("INSERT INTO taxon_min VALUES (?, ?, ?)", concepts)
    return conn


def _refresh(entries):
    return pid.InaturalistRefresh("2099-01-01", tuple(pid.RefreshEntry(*e) for e in entries))


# ------------------------------------------------------------- re-keying

def test_a_superseded_overlay_entry_is_rekeyed_with_its_provenance():
    entries, report = pid.rekey_artportalen_overlay([OVERLAY_ENTRY], SUPERSEDED)
    assert OVERLAY_ENTRY["sporely_taxon_id"] == 624588  # the loaded input is not rewritten
    assert report == [{"superseded_from_sporely_taxon_id": 624588, "sporely_taxon_id": 78915,
                       "supersession_id": "sup-624588", "scientific_name": "Amanita muscaria"}]
    rows: list = []
    assert pid.apply_artportalen_overlay(_conn([(78915, "Amanita muscaria", None)]), entries, rows) == 1
    assert rows == [(78915, "artportalen", 236537, "publishing", 1, "Amanita muscaria s.str.",
                     "reviewed_publishing_overlay:accepted_after_review:split" + PROVENANCE)]


def test_a_superseded_refresh_entry_is_rekeyed_with_its_provenance():
    refresh = _refresh([(5, "Boletus pinetorum", None, None),
                        (624588, "Amanita muscaria", 48715, "Amanita muscaria")])
    rekeyed, report = pid.rekey_inaturalist_refresh(refresh, SUPERSEDED)
    assert [e.sporely_taxon_id for e in rekeyed.entries] == [5, 78915]
    assert [(r["superseded_from_sporely_taxon_id"], r["sporely_taxon_id"]) for r in report] == [(624588, 78915)]
    conn = _conn([(5, "Boletus pinetorum", None), (78915, "Amanita muscaria", None)])
    kept, counts = pid.apply_inaturalist_refresh_rows(conn, rekeyed, [])
    assert kept == [(78915, "inaturalist", 48715, "accepted", 1, "Amanita muscaria",
                     "inaturalist_refresh:2099-01-01" + PROVENANCE)]
    assert counts["resolved"] == 1
    pid.set_refreshed_inaturalist_columns(conn, rekeyed)
    assert conn.execute("SELECT inaturalist_taxon_id FROM taxon_min WHERE taxon_id = 78915").fetchone() == (48715,)


def test_an_entry_not_keyed_to_a_retired_concept_is_unchanged():
    entry = {**OVERLAY_ENTRY, "sporely_taxon_id": 2}
    entries, report = pid.rekey_artportalen_overlay([entry], SUPERSEDED)
    assert entries == [entry] and report == []


@pytest.mark.parametrize("ids", [
    [78915, 624588],            # the survivor already has its own entry
    [624588, 624589],           # two retired concepts share one survivor
])
def test_a_retired_plus_survivor_collision_fails(ids):
    supersessions = {**SUPERSEDED, 624589: pid.Supersession("sup-624589", 78915)}
    with pytest.raises(pid.PublishingIdError, match="review them into one entry"):
        pid.rekey_entries("Artportalen overlay", ids, supersessions)
    refresh = _refresh([(i, "Amanita muscaria", None, None) for i in ids])
    with pytest.raises(pid.PublishingIdError, match="iNaturalist refresh: .* review them into one"):
        pid.rekey_inaturalist_refresh(refresh, supersessions)


def test_an_unknown_id_still_fails():
    """Neither in the release nor recorded as superseded: nothing re-keys it."""
    entries, _ = pid.rekey_artportalen_overlay([{**OVERLAY_ENTRY, "sporely_taxon_id": 999}], SUPERSEDED)
    with pytest.raises(pid.PublishingIdError, match="concept 999 .* is not in this release"):
        pid.apply_artportalen_overlay(_conn([(78915, "Amanita muscaria", None)]), entries, [])


def test_a_rekeyed_entry_must_match_the_survivors_name():
    """The id was reviewed against a name; a survivor with another name is a new review."""
    entries, _ = pid.rekey_artportalen_overlay([OVERLAY_ENTRY], SUPERSEDED)
    with pytest.raises(pid.PublishingIdError, match="review it again"):
        pid.apply_artportalen_overlay(_conn([(78915, "Amanita regalis", None)]), entries, [])


# ------------------------------------------------ builder, end to end

def _pinned_inputs(tmp_path: Path, retired: int, name: str) -> tuple[Path, Path]:
    overlay = {**ao.empty_overlay(), "entries": [{**OVERLAY_ENTRY, "sporely_taxon_id": retired,
                                                  "scientific_name": name}]}
    overlay_path = tmp_path / "overlay.json"
    overlay_path.write_text(json.dumps(overlay), encoding="utf-8")
    refresh = {"format": pid.INATURALIST_REFRESH_FORMAT, "acquired_on": "2099-01-01", "entries": [
        {"sporely_taxon_id": retired, "scientific_name": name, "status": "resolved",
         "inaturalist": {"taxon_id": 999001, "name": name}}]}
    refresh_path = tmp_path / "refresh.json"
    refresh_path.write_text(json.dumps(refresh), encoding="utf-8")
    return overlay_path, refresh_path


def test_provenance_survives_into_the_build_output(tmp_path):
    release, own_id, host_id = _superseded_release(tmp_path)
    plain = tmp_path / "plain.sqlite3"
    build_candidate(release_dir=release, registry_path=tmp_path / "registry.jsonl", output_db=plain)
    host_name = _taxon(plain, host_id)["canonical_scientific_name"]
    overlay, refresh = _pinned_inputs(tmp_path, own_id, host_name)
    before = {p: p.read_bytes() for p in (overlay, refresh)}

    # Without the ledger the retired id is, as before, not in the release.
    with pytest.raises(BuildError, match=f"concept {own_id} .* is not in this release"):
        build_candidate(release_dir=release, registry_path=tmp_path / "registry.jsonl",
                        output_db=tmp_path / "refused.sqlite3", publishing_overlay_path=overlay,
                        inaturalist_refresh_path=refresh)

    db = tmp_path / "rekeyed.sqlite3"
    summary = build_candidate(release_dir=release, registry_path=tmp_path / "registry.jsonl",
                              output_db=db, publishing_overlay_path=overlay,
                              inaturalist_refresh_path=refresh,
                              concept_supersessions_path=tmp_path / "supersessions.yml")
    assert {p: p.read_bytes() for p in (overlay, refresh)} == before  # inputs byte-identical
    expected = [{"superseded_from_sporely_taxon_id": own_id, "sporely_taxon_id": host_id,
                 "supersession_id": "supersede-52369", "scientific_name": host_name}]
    assert summary["publishing_ids"]["superseded_rekeys"] == {
        "inaturalist_refresh": expected, "artportalen_overlay": expected}
    provenance = f";superseded_from:{own_id};supersession:supersede-52369"
    conn = sqlite3.connect(db)
    rows = conn.execute("SELECT taxon_id, source_system, external_id, note FROM taxon_external_id_min "
                        "WHERE source_system IN ('artportalen', 'inaturalist') ORDER BY source_system").fetchall()
    assert rows == [
        (host_id, "artportalen", 236537, "reviewed_publishing_overlay:accepted_after_review:split" + provenance),
        (host_id, "inaturalist", 999001, "inaturalist_refresh:2099-01-01" + provenance)]
    assert _taxon(db, host_id)["inaturalist_taxon_id"] == 999001
    assert conn.execute("SELECT COUNT(*) FROM taxon_min WHERE taxon_id = ?", (own_id,)).fetchone() == (0,)


def _manifest(ledger: Path, pairs) -> dict:
    return {"concept_supersessions_sha256": br.sha256_file(ledger),
            "counts": {"concept_supersessions": [
                {"superseded_sporely_taxon_id": a, "current_sporely_taxon_id": b} for a, b in pairs]}}


def _ledger(path: Path, records) -> Path:
    path.write_text(json.dumps({"supersessions": [
        {"supersession_id": sid, "superseded_sporely_taxon_id": retired, "review_status": status,
         "current_source_usage": {"source": "col_xr", "namespace": "col_usage_id", "identifier": sid}}
        for sid, retired, status in records]}), encoding="utf-8")
    return path


def test_the_builder_takes_survivors_from_the_compilers_record(tmp_path):
    ledger = _ledger(tmp_path / "s.json", [("a", 1, "approved"), ("b", 2, "proposed")])
    assert _release_supersessions(_manifest(ledger, [(1, 10)]), ledger) == {1: pid.Supersession("a", 10)}
    with pytest.raises(BuildError, match="differ from the approved records"):
        _release_supersessions(_manifest(ledger, [(1, 10), (3, 11)]), ledger)
    with pytest.raises(BuildError, match="was compiled with"):
        _release_supersessions({**_manifest(ledger, [(1, 10)]), "concept_supersessions_sha256": "0" * 64},
                               ledger)


# ------------------------------------------------- release-recipe preflight

def _recipe(tmp_path: Path, *, ledger_records, registry_lines, overlay_ids=(), refresh_ids=()) -> br.Recipe:
    ledger = _ledger(tmp_path / "ledger.json", ledger_records)
    registry = tmp_path / "registry.jsonl"
    registry.write_text("".join(json.dumps({
        "sporely_taxon_id": taxon_id, "source": "col_xr", "namespace": "col_usage_id", "identifier": ident,
        "allocated_in_release": "tax-2099.01.01-01", "first_seen_source_release": "x", "kind": "anchor"}) + "\n"
        for taxon_id, ident in registry_lines), encoding="utf-8")
    overlay = tmp_path / "overlay.json"
    overlay.write_text(json.dumps({**ao.empty_overlay(), "entries": [
        {**OVERLAY_ENTRY, "sporely_taxon_id": i, "artportalen_taxon_id": 1000 + i} for i in overlay_ids]}))
    refresh = tmp_path / "refresh.json"
    refresh.write_text(json.dumps({"format": pid.INATURALIST_REFRESH_FORMAT, "acquired_on": "x", "entries": [
        {"sporely_taxon_id": i, "scientific_name": "A b", "status": "unresolved"} for i in refresh_ids]}))
    return br.Recipe(sources=(), redlist_workbook="", redlist_sha256="", legacy_enabled=False, legacy_db=None,
                     legacy_sha256=None, policies={"concept_supersessions": str(ledger)},
                     registry=str(registry), artportalen_overlay={"path": str(overlay), "sha256": ""},
                     inaturalist_refresh={"path": str(refresh), "sha256": ""})


def test_preflight_reports_every_superseded_reference(tmp_path):
    recipe = _recipe(tmp_path, ledger_records=[("s1", 1, "approved"), ("s2", 2, "approved")],
                     registry_lines=[(1, "x1"), (2, "x2"), (10, "s1"), (20, "s2")],
                     overlay_ids=[1, 5], refresh_ids=[1, 2, 6])
    report = br.check_superseded_references(recipe)
    assert [(r["superseded_from_sporely_taxon_id"], r["sporely_taxon_id"], r["supersession_id"])
            for r in report["artportalen_overlay"]] == [(1, 10, "s1")]
    assert [(r["superseded_from_sporely_taxon_id"], r["sporely_taxon_id"])
            for r in report["inaturalist_refresh"]] == [(1, 10), (2, 20)]


@pytest.mark.parametrize("ledger_records, registry_lines, message", [
    ([("s1", 1, "approved")], [(1, "x1")], "has no registry allocation"),
    ([("s1", 1, "approved"), ("s2", 10, "approved")], [(1, "x1"), (10, "s1"), (20, "s2")],
     "which is itself superseded"),
])
def test_preflight_fails_on_an_unresolved_superseded_reference(tmp_path, ledger_records, registry_lines,
                                                               message):
    recipe = _recipe(tmp_path, ledger_records=ledger_records, registry_lines=registry_lines,
                     refresh_ids=[1])
    with pytest.raises(br.BuildError, match=f"cannot be resolved: 1: .*{message}"):
        br.check_superseded_references(recipe)


def test_preflight_fails_on_a_collision(tmp_path):
    recipe = _recipe(tmp_path, ledger_records=[("s1", 1, "approved")], registry_lines=[(1, "x1"), (10, "s1")],
                     refresh_ids=[1, 10])
    with pytest.raises(br.BuildError, match="review them into one entry"):
        br.check_superseded_references(recipe)


def test_committed_recipe_preflight_resolves_every_superseded_reference():
    report = br.check_superseded_references(br.load_recipe(br.DEFAULT_RECIPE))
    counts = {key: len(entries) for key, entries in report.items()}
    assert counts == {"artportalen_overlay": 1, "inaturalist_refresh": 20}
    assert report["artportalen_overlay"][0]["superseded_from_sporely_taxon_id"] == 624588
