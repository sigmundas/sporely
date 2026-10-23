"""R1: P10 inherited usages and the retired-id map, end to end.

Policy P10 (``mapping_policy.yml`` ``inherited_usage_under_supersession``):
under a ``names_only`` supersession only the retired concept's own anchor
usage becomes a ``reviewed_supersession`` bridge. Every other usage (here a
NorTaxa synonym with a blocked nomenclatural status) moves to the survivor
as a name, keeps its own evidence class and statuses, and is marked
``inherited_via_supersession``. Build tests H16 and H2 of the audit's
build-test proposal: compiler, SQLite builder and cloud export, plus the
additive ``taxon_supersession_min`` map and the desktop lookups that read it.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

_TAXONOMY = Path(__file__).resolve().parents[1]
_REPO = _TAXONOMY.parents[1]
for _path in (_TAXONOMY / "scripts", _TAXONOMY, _REPO):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from bridge_emission import (  # noqa: E402
    BridgeEmissionError,
    verify_supersession_manifest_approvals,
)
from build_sqlite_candidate import build_candidate  # noqa: E402
from compile_release import CompilerError, compile_release  # noqa: E402

from test_bridge_emission import (  # noqa: E402
    _NORTAXA_RELEASE,
    _POLICY_PATH,
    _col_source,
    _export_and_scope,
    _resolvable_parents,
    _usages,
    _write_supersessions,
)
from test_compile_release import (  # noqa: E402
    _write_manual_mappings,
    _write_normalized_source,
)


def _nortaxa_with_synonym(root: Path) -> Path:
    source_dir = _resolvable_parents(_write_normalized_source(
        root / "nortaxa",
        source_code="nortaxa",
        source_release=_NORTAXA_RELEASE,
        identifier_namespace_prefix="NBIC:",
        rows=[
            {"core_row_id": "row-K", "taxon_id": "9999",
             "scientific_name": "Fungi", "rank": "kingdom"},
            {"core_row_id": "row-52369", "taxon_id": "52369",
             "parent": "9999", "parent_resolution": "resolved",
             "scientific_name": "Pholiotina rugosa",
             "authorship": "(Peck) Singer", "status": "valid"},
            # A synonym of the retiree with a blocked nomenclatural status:
            # what P10 must never publish as a reviewed bridge.
            {"core_row_id": "row-77001", "taxon_id": "77001",
             "accepted": "52369", "parent": "9999",
             "parent_resolution": "resolved",
             "scientific_name": "Galera rugosa",
             "authorship": "(Peck) Sacc.", "status": "synonym"},
        ],
    ), "nortaxa")
    taxa = source_dir / "taxa.jsonl"
    rows = [json.loads(line) for line in taxa.read_text(encoding="utf-8").splitlines() if line]
    for row in rows:
        row["nomenclatural_status"] = "illegitimate" if row["taxon_id"]["value"] == "77001" else ""
    taxa.write_text("".join(json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n"
                            for r in rows), encoding="utf-8")
    return source_dir


def _release(tmp_path: Path, inherited_usages: str | None,
             **extra) -> tuple[Path, int, int]:
    root = tmp_path / "sources"
    sources = [_col_source(root), _nortaxa_with_synonym(root)]

    def compile_(out: str, release_id: str, ledger: Path | None) -> Path:
        compile_release(
            normalized_source_dirs=sources,
            manual_mappings_path=_write_manual_mappings(tmp_path / f"{out}.yml", []),
            mapping_policy_path=_POLICY_PATH,
            registry_path=tmp_path / "registry.jsonl",
            output_dir=tmp_path / out,
            release_id=release_id,
            concept_supersessions_path=ledger,
        )
        return tmp_path / out

    first = compile_("release1", "tax-2026.10.23-01", None)
    own_id = _usages(first)[("nortaxa", "52369")]["sporely_taxon_id"]
    host_id = _usages(first)[("col_xr", "5ZT3G")]["sporely_taxon_id"]
    record = {
        "supersession_id": "r1-supersede-52369",
        "superseded_sporely_taxon_id": own_id,
        "current_source_usage": {"source": "col_xr", "namespace": "col_xr_taxon_id",
                                 "identifier": "5ZT3G"},
        "relationship": "exact",
        "review_status": "approved",
    }
    if inherited_usages is not None:
        record["inherited_usages"] = inherited_usages
    record.update(extra)
    ledger = _write_supersessions(tmp_path / "supersessions.yml", [record])
    return compile_("release2", "tax-2026.10.23-02", ledger), own_id, host_id


def _db(tmp_path: Path, release: Path) -> sqlite3.Connection:
    db_path = tmp_path / "candidate.sqlite3"
    build_candidate(release_dir=release, registry_path=tmp_path / "registry.jsonl",
                    output_db=db_path,
                    concept_supersessions_path=tmp_path / "supersessions.yml")
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def test_names_only_bridges_the_anchor_and_keeps_the_synonym_as_a_name(tmp_path: Path) -> None:
    release, own_id, host_id = _release(tmp_path, "names_only")
    usages = _usages(release)
    anchor, synonym = usages[("nortaxa", "52369")], usages[("nortaxa", "77001")]
    assert anchor["bridge_evidence_class"] == "reviewed_supersession"
    assert "inheritance" not in anchor
    assert synonym["sporely_taxon_id"] == host_id
    assert synonym["superseded_from_sporely_taxon_id"] == own_id
    assert synonym["bridge_evidence_class"] == "intra_source_synonym"
    assert synonym["alias_reason"] == "synonym_of_accepted"
    assert synonym["inheritance"] == "inherited_via_supersession"
    assert synonym["taxonomic_status"] == "synonym"
    assert synonym["nomenclatural_status"] == "illegitimate"

    conn = _db(tmp_path, release)
    bridges = conn.execute(
        "SELECT external_id FROM taxon_external_id_text_min "
        "WHERE note LIKE 'authoritative_bridge:%' AND source_system='nortaxa'").fetchall()
    assert [r[0] for r in bridges] == ["52369"]          # H16: one bridge, the anchor's
    expected_note = ("synonym_of_accepted|inherited_via_supersession|"
                     f"superseded_from={own_id}|evidence_class=intra_source_synonym|"
                     "taxonomic_status=synonym|nomenclatural_status=illegitimate")
    name = conn.execute("SELECT * FROM scientific_name_min WHERE scientific_name='Galera rugosa'"
                        ).fetchone()
    assert (name["taxon_id"], name["is_preferred_name"], name["note"]) == (host_id, 0, expected_note)
    ext = conn.execute("SELECT * FROM taxon_external_id_min WHERE external_id = 77001").fetchone()
    assert (ext["taxon_id"], ext["id_role"], ext["note"]) == (host_id, "synonym", expected_note)
    # H2: the retired id resolves to a live survivor through the additive map.
    row = conn.execute("SELECT * FROM taxon_supersession_min").fetchone()
    assert (row["retired_taxon_id"], row["current_taxon_id"], row["supersession_id"],
            row["content_release_id"]) == (own_id, host_id, "r1-supersede-52369",
                                           "tax-2026.10.23-02")
    assert conn.execute("SELECT COUNT(*) FROM taxon_min WHERE taxon_id=?", (own_id,)).fetchone()[0] == 0
    assert conn.execute("SELECT value FROM taxonomy_meta WHERE key='taxonomy_schema_version'"
                        ).fetchone()[0] == "2"


def test_names_only_provenance_survives_the_cloud_export(tmp_path: Path) -> None:
    release, own_id, host_id = _release(tmp_path, "names_only")
    export_dir, _gz = _export_and_scope(tmp_path, release)
    names = [json.loads(line) for line in
             (export_dir / "scientific_name.jsonl").read_text(encoding="utf-8").splitlines()]
    galera = [n for n in names if n["scientific_name"] == "Galera rugosa"]
    assert len(galera) == 1
    assert galera[0]["taxon_id"] == host_id
    assert galera[0]["note"].startswith("synonym_of_accepted|inherited_via_supersession|"
                                        f"superseded_from={own_id}|")
    bridges = [json.loads(line) for line in
               (export_dir / "taxon_external_id.jsonl").read_text(encoding="utf-8").splitlines()]
    nortaxa = sorted(b["external_id"] for b in bridges
                     if b.get("source_system") == "nortaxa"
                     and str(b.get("note") or "").startswith("authoritative_bridge:"))
    assert nortaxa == ["52369"]


def test_old_records_keep_their_behaviour(tmp_path: Path) -> None:
    """A record without ``inherited_usages`` still bridges every re-keyed usage."""
    release, _own_id, _host_id = _release(tmp_path, None)
    synonym = _usages(release)[("nortaxa", "77001")]
    assert synonym["bridge_evidence_class"] == "reviewed_supersession"
    assert "inheritance" not in synonym and "nomenclatural_status" not in synonym


def test_unknown_inherited_usages_value_is_refused(tmp_path: Path) -> None:
    with pytest.raises(CompilerError, match="inherited_usages"):
        _release(tmp_path, "everything")


def test_names_only_approval_refuses_a_record_without_it(tmp_path: Path) -> None:
    import hashlib
    manifest = {
        "approval_mode": "batch_by_file_sha256", "pins": {"release": {"x": 1}},
        "columns": ["nortaxa_taxon_id", "nortaxa_sporely_taxon_id", "col_usage_id",
                    "col_sporely_taxon_id", "one_to_one"],
        "members": [["52369", 10, "5ZT3G", 20, True]],
    }
    path = tmp_path / "m.manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    record = {"supersession_id": "s", "superseded_sporely_taxon_id": 10,
              "current_source_usage": {"source": "col_xr", "namespace": "col_usage_id",
                                       "identifier": "5ZT3G"},
              "relationship": "exact", "review_status": "approved",
              "approved_manifest": {"file_sha256": sha, "member": {
                  "nortaxa_taxon_id": "52369", "nortaxa_sporely_taxon_id": 10,
                  "col_usage_id": "5ZT3G", "col_sporely_taxon_id": 20}}}
    document = {"approved_manifests": [{
        "path": str(path), "file_sha256": sha, "approved_by": "o", "approved_at": "d",
        "decision_reference": "r", "pins": manifest["pins"],
        "inherited_usages": "names_only"}], "supersessions": [record]}
    with pytest.raises(BridgeEmissionError, match="names_only"):
        verify_supersession_manifest_approvals(document)
    record["inherited_usages"] = "names_only"
    assert verify_supersession_manifest_approvals(document) == {"s": sha}


def test_desktop_lookups_resolve_a_retired_id(tmp_path: Path) -> None:
    from database.taxon_lookup import TaxonLookupService
    from database.vernacular_db import VernacularDB

    release, own_id, host_id = _release(tmp_path, "names_only")
    _db(tmp_path, release).close()
    vdb = VernacularDB(tmp_path / "candidate.sqlite3", language_code="nb")
    assert vdb.current_taxon_ids([own_id, host_id, 999999]) == {own_id: host_id}
    names = vdb.display_scientific_names([own_id])
    assert names[own_id] == vdb.display_scientific_names([host_id])[host_id]
    lookup = TaxonLookupService(vernacular_db=vdb, language_code="nb",
                                include_reference_data=False)
    assert lookup.current_taxon_id(own_id) == host_id
    assert lookup.current_taxon_id(host_id) == host_id
    assert lookup.current_taxon_id(999999) is None


def test_candidate_only_release_is_marked_and_not_exported_for_import(tmp_path: Path) -> None:
    from database.taxonomy.cloud_export import ExportError

    release, _own, _host = _release(tmp_path, "names_only", release_approval="pending",
                                    approval_scope="candidate_build_only")
    manifest = json.loads((release / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["release_approval"]["status"] == "pending:candidate_build_only"
    conn = _db(tmp_path, release)
    assert conn.execute("SELECT value FROM taxonomy_meta WHERE key='release_approval'"
                        ).fetchone()[0] == "pending:candidate_build_only"
    conn.close()
    with pytest.raises(ExportError, match="candidate_build_only"):
        _export_and_scope(tmp_path, release)


def test_redlist_lookup_follows_a_retired_id(tmp_path: Path) -> None:
    from database.taxon_lookup import TaxonLookupService
    from database.vernacular_db import VernacularDB

    release, own_id, host_id = _release(tmp_path, "names_only")
    _db(tmp_path, release).close()
    db_path = tmp_path / "candidate.sqlite3"
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO taxon_redlist_min (taxon_id, source_system, source_release, assessment_id, "
        "assessment_area, assessed_name_source, assessed_name_namespace, assessed_name_id, "
        "scientific_name_snapshot, category_raw, category_code) VALUES "
        "(?, 'artsdatabanken_redlist', '2021', '1', 'Norge', 'nortaxa', 'nortaxa_name_id', '1', "
        "'Conocybe rugosa', 'VU', 'VU')", (host_id,))
    conn.commit()
    conn.close()
    lookup = TaxonLookupService(vernacular_db=VernacularDB(db_path, language_code="nb"),
                                language_code="nb", include_reference_data=False)
    retired, current = lookup.get_redlist_lookup(own_id), lookup.get_redlist_lookup(host_id)
    assert (retired.status, retired.assessment.category_code) == ("unique", "VU")
    assert retired.assessment == current.assessment
    assert lookup.get_redlist_lookup_with_overlay(own_id).status == "unique"
