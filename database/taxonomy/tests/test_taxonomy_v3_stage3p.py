"""Taxonomy v3 Stage 3P: national preferred scientific names.

`taxon_min.preferred_scientific_name_no` is filled from NorTaxa's accepted
name only on a concept whose NorTaxa identity is an emitted reviewed bridge
(approved manual mapping or concept supersession). It is display and search
metadata with provenance, never identity: ids and `canonical_scientific_name`
do not change. The cloud export carries it with that provenance, and the
desktop shows it for Norwegian and falls back to the canonical name.

The fixture is `test_bridge_emission`'s: COL `5ZT3G` Conocybe rugosa and
NorTaxa 52369 Pholiotina rugosa (the Sporely 83668 case), COL `39ZCL` /
NorTaxa 53482 Entoloma conferendum, and controls that are bound but never
emitted.
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

from build_sqlite_candidate import build_candidate  # noqa: E402
from compile_release import compile_release  # noqa: E402

from test_bridge_emission import (  # noqa: E402
    _POLICY_PATH,
    _REVIEWED_53482,
    _compile,
    _export_and_scope,
    _usages,
    _write_supersessions,
)
from test_compile_release import _write_manual_mappings  # noqa: E402

_REVIEWED_52369 = {
    "mapping_id": "nortaxa-52369-to-col-5ZT3G",
    "source_usage": {"source": "nortaxa", "namespace": "nortaxa_taxon_id",
                     "identifier": "52369"},
    "target": {"source_usage": {"source": "col_xr",
                                "namespace": "col_xr_taxon_id",
                                "identifier": "5ZT3G"}},
    "relationship": "exact",
    "review_status": "approved",
}

_NAME_COLUMNS = (
    "preferred_scientific_name_no",
    "preferred_scientific_name_no_source_system",
    "preferred_scientific_name_no_namespace",
    "preferred_scientific_name_no_external_id",
    "preferred_scientific_name_sv",
    "preferred_scientific_name_sv_source_system",
    "preferred_scientific_name_sv_namespace",
    "preferred_scientific_name_sv_external_id",
)


def _build(tmp_path: Path, release: Path, name: str = "candidate.sqlite3"
           ) -> tuple[Path, dict]:
    db_path = tmp_path / name
    summary = build_candidate(
        release_dir=release,
        registry_path=tmp_path / "registry.jsonl",
        output_db=db_path,
    )
    return db_path, summary


def _taxon(db_path: Path, taxon_id: int) -> dict:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        return dict(conn.execute(
            "SELECT * FROM taxon_min WHERE taxon_id = ?", (taxon_id,)).fetchone())
    finally:
        conn.close()


def _superseded_release(tmp_path: Path) -> tuple[Path, int, int]:
    """52369 first gets its own concept, then a reviewed supersession moves it
    onto the COL concept — how production reached 52369 → 83668."""
    first = _compile(tmp_path, release_id="tax-2026.09.23-01")
    own_id = _usages(first)[("nortaxa", "52369")]["sporely_taxon_id"]
    host_id = _usages(first)[("col_xr", "5ZT3G")]["sporely_taxon_id"]
    release = tmp_path / "release2"
    compile_release(
        normalized_source_dirs=[tmp_path / "sources" / "col_xr",
                                tmp_path / "sources" / "nortaxa"],
        manual_mappings_path=_write_manual_mappings(tmp_path / "m2.yml", []),
        mapping_policy_path=_POLICY_PATH,
        registry_path=tmp_path / "registry.jsonl",
        output_dir=release,
        release_id="tax-2026.09.23-02",
        concept_supersessions_path=_write_supersessions(
            tmp_path / "supersessions.yml", [{
                "supersession_id": "supersede-52369",
                "superseded_sporely_taxon_id": own_id,
                "current_source_usage": {"source": "col_xr",
                                         "namespace": "col_xr_taxon_id",
                                         "identifier": "5ZT3G"},
                "relationship": "exact",
                "review_status": "approved",
            }]),
    )
    return release, own_id, host_id


# ------------------------------------------------------------- compiler ---


def test_reviewed_supersession_fills_the_norwegian_name_with_provenance(
    tmp_path: Path,
) -> None:
    release, _own_id, host_id = _superseded_release(tmp_path)
    db_path, summary = _build(tmp_path, release)
    row = _taxon(db_path, host_id)

    assert row["preferred_scientific_name_no"] == "Pholiotina rugosa"
    assert row["preferred_scientific_name_no_source_system"] == "nortaxa"
    assert row["preferred_scientific_name_no_namespace"] == "nortaxa_taxon_id"
    assert row["preferred_scientific_name_no_external_id"] == "52369"
    # Display metadata only: the canonical name and identity are untouched.
    assert row["canonical_scientific_name"] == "Conocybe rugosa"
    assert (row["canonical_source_system"], row["canonical_external_id"]) == \
        ("col_xr", "5ZT3G")
    # Swedish has no source until Stage 4P.
    assert row["preferred_scientific_name_sv"] is None
    assert row["preferred_scientific_name_sv_source_system"] is None
    assert summary["national_preferred_scientific_names"]["no"] == {
        "filled": 1, "ambiguous_left_null": 0}


def test_approved_manual_mapping_fills_the_name(tmp_path: Path) -> None:
    release = _compile(tmp_path, manual=[_REVIEWED_53482, _REVIEWED_52369])
    usages = _usages(release)
    db_path, summary = _build(tmp_path, release)

    conocybe = _taxon(db_path, usages[("col_xr", "5ZT3G")]["sporely_taxon_id"])
    assert conocybe["preferred_scientific_name_no"] == "Pholiotina rugosa"
    # Agreeing names: the national name is filled even though it equals the
    # canonical one, so the rule does not depend on names differing.
    entoloma = _taxon(db_path, usages[("col_xr", "39ZCL")]["sporely_taxon_id"])
    assert entoloma["preferred_scientific_name_no"] == "Entoloma conferendum"
    assert entoloma["preferred_scientific_name_no_external_id"] == "53482"
    assert summary["national_preferred_scientific_names"]["no"]["filled"] == 2


def test_no_name_without_an_approved_national_identity(tmp_path: Path) -> None:
    """Automatic matches, missing-authorship bindings and unbridged concepts
    keep NULL — the concept displays its COL name."""
    release = _compile(tmp_path)
    usages = _usages(release)
    # Mycena absentia is bound onto the COL concept but not emitted.
    assert usages[("nortaxa", "70002")]["identity_binding"] == "alias"
    db_path, summary = _build(tmp_path, release)
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        filled = conn.execute(
            "SELECT COUNT(*) FROM taxon_min WHERE " + " OR ".join(
                f"{c} IS NOT NULL" for c in _NAME_COLUMNS)).fetchone()[0]
    finally:
        conn.close()
    assert filled == 0
    assert summary["national_preferred_scientific_names"]["no"]["filled"] == 0


def test_two_accepted_national_identities_leave_the_name_null(
    tmp_path: Path,
) -> None:
    """Never choose between two reviewed NorTaxa names for one concept."""
    second = {
        "mapping_id": "nortaxa-70002-to-col-5ZT3G",
        "source_usage": {"source": "nortaxa", "namespace": "nortaxa_taxon_id",
                         "identifier": "70002"},
        "target": {"source_usage": {"source": "col_xr",
                                    "namespace": "col_xr_taxon_id",
                                    "identifier": "5ZT3G"}},
        "relationship": "exact",
        "review_status": "approved",
    }
    release = _compile(tmp_path, manual=[_REVIEWED_52369, second])
    usages = _usages(release)
    host = usages[("col_xr", "5ZT3G")]["sporely_taxon_id"]
    assert usages[("nortaxa", "52369")]["sporely_taxon_id"] == host
    assert usages[("nortaxa", "70002")]["sporely_taxon_id"] == host
    db_path, summary = _build(tmp_path, release)
    row = _taxon(db_path, host)
    assert row["preferred_scientific_name_no"] is None
    assert row["preferred_scientific_name_no_external_id"] is None
    assert summary["national_preferred_scientific_names"]["no"] == {
        "filled": 0, "ambiguous_left_null": 1}


def test_identity_and_canonical_names_are_unchanged(tmp_path: Path) -> None:
    """Filling national names changes no id and no canonical name.

    The compiled release's ``taxa.jsonl`` is the identity the registry bound;
    the SQLite projection must publish exactly those concepts with exactly
    those canonical names, national names or not.
    """
    release = _compile(tmp_path, manual=[_REVIEWED_53482, _REVIEWED_52369])
    compiled = {
        int(row["sporely_taxon_id"]): (
            row.get("scientific_name"),
            row["canonical_source_usage"]["source"],
            row["canonical_source_usage"]["identifier"],
        )
        for row in (json.loads(line) for line in (release / "taxa.jsonl").read_text(
            encoding="utf-8").splitlines() if line.strip())
    }
    db_path, summary = _build(tmp_path, release)
    assert summary["national_preferred_scientific_names"]["no"]["filled"] == 2
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        projected = {
            taxon_id: (name, source, identifier)
            for taxon_id, name, source, identifier in conn.execute(
                "SELECT taxon_id, canonical_scientific_name, source_system, "
                "canonical_external_id FROM taxon_min")
        }
    finally:
        conn.close()
    assert projected == compiled


def test_build_is_deterministic(tmp_path: Path) -> None:
    release = _compile(tmp_path, manual=[_REVIEWED_52369])
    _a, first = _build(tmp_path, release, "a.sqlite3")
    _b, second = _build(tmp_path, release, "b.sqlite3")
    assert first["sqlite_sha256"] == second["sqlite_sha256"]


# --------------------------------------------------------------- export ---


def _jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()]


def test_export_carries_the_name_with_traceable_provenance(tmp_path: Path) -> None:
    release = _compile(tmp_path, manual=[_REVIEWED_52369])
    host = _usages(release)[("col_xr", "5ZT3G")]["sporely_taxon_id"]
    export_dir, _gz = _export_and_scope(tmp_path, release)

    taxa = {row["taxon_id"]: row for row in _jsonl(export_dir / "taxon.jsonl")}
    row = taxa[host]
    assert row["canonical_scientific_name"] == "Conocybe rugosa"
    assert row["preferred_scientific_name_no"] == "Pholiotina rugosa"
    triple = (row["preferred_scientific_name_no_source_system"],
              row["preferred_scientific_name_no_namespace"],
              row["preferred_scientific_name_no_external_id"])
    assert triple == ("nortaxa", "nortaxa_taxon_id", "52369")
    # Every taxon row carries all eight fields, null where not filled.
    assert all(set(_NAME_COLUMNS) <= set(r) for r in taxa.values())
    assert all(r["preferred_scientific_name_sv"] is None for r in taxa.values())
    # The provenance is a published authoritative bridge row.
    bridges = [r for r in _jsonl(export_dir / "taxon_external_id.jsonl")
               if (r["taxon_id"], r["source_system"], r["namespace"],
                   r["external_id"]) == (host, *triple)]
    assert len(bridges) == 1
    assert bridges[0]["note"].startswith("authoritative_bridge:")
    assert bridges[0]["external_name"] == "Pholiotina rugosa"
    # Additive: the dataset list and export schema version are unchanged.
    manifest = json.loads((export_dir / "taxonomy_export_manifest.json").read_text(
        encoding="utf-8"))
    assert manifest["export_schema_version"] == 1
    assert manifest["taxonomy_schema_version"] == 2


def test_export_is_deterministic(tmp_path: Path) -> None:
    import shutil

    release = _compile(tmp_path, manual=[_REVIEWED_52369])
    runs = []
    for name in ("one", "two"):
        # The helper reads the registry from beside its working directory.
        (tmp_path / name).mkdir()
        shutil.copy(tmp_path / "registry.jsonl", tmp_path / name / "registry.jsonl")
        runs.append(_export_and_scope(tmp_path / name, release)[0])
    first, second = runs
    a = json.loads((first / "taxonomy_export_manifest.json").read_text(encoding="utf-8"))
    b = json.loads((second / "taxonomy_export_manifest.json").read_text(encoding="utf-8"))
    assert a["whole_export_sha256"] == b["whole_export_sha256"]
    assert a["files"] == b["files"]


def test_export_refuses_a_name_without_a_bridge(tmp_path: Path) -> None:
    """A name whose provenance is not an authoritative bridge row is refused."""
    import gzip

    from database.taxonomy import cloud_export as ce

    release = _compile(tmp_path, manual=[_REVIEWED_52369])
    host = _usages(release)[("col_xr", "5ZT3G")]["sporely_taxon_id"]
    _export_dir, gz_path = _export_and_scope(tmp_path, release)
    sqlite_path = tmp_path / "tampered.sqlite3"
    with gzip.open(gz_path, "rb") as src:
        sqlite_path.write_bytes(src.read())
    conn = sqlite3.connect(sqlite_path)
    conn.execute("UPDATE taxon_min SET preferred_scientific_name_no_external_id = '999' "
                 "WHERE taxon_id = ?", (host,))
    conn.commit()
    conn.execute("CREATE TEMP TABLE _cloud_export_scope (taxon_id INTEGER PRIMARY KEY)")
    conn.execute("INSERT INTO _cloud_export_scope SELECT taxon_id FROM taxon_min")
    conn.row_factory = sqlite3.Row
    present = {r["name"] for r in conn.execute("PRAGMA table_info(taxon_min)")}
    with pytest.raises(ce.ExportError, match="without an authoritative bridge"):
        ce._validate_national_names(conn, present)
    conn.execute("UPDATE taxon_min SET preferred_scientific_name_no_external_id = NULL "
                 "WHERE taxon_id = ?", (host,))
    with pytest.raises(ce.ExportError, match="partial provenance"):
        ce._validate_national_names(conn, present)
    # An artifact without provenance columns may not carry a name at all.
    with pytest.raises(ce.ExportError, match="no provenance columns"):
        ce._validate_national_names(conn, present - {
            "preferred_scientific_name_no_source_system"})
    conn.close()


def test_scoped_export_requires_the_cited_bridge(tmp_path: Path) -> None:
    from database.taxonomy.macrofungi_scope import ScopeError, validate_national_names

    release = _compile(tmp_path, manual=[_REVIEWED_52369])
    host = _usages(release)[("col_xr", "5ZT3G")]["sporely_taxon_id"]
    export_dir, _gz = _export_and_scope(tmp_path, release)
    assert validate_national_names(export_dir) == {"no": 1, "sv": 0}

    # A scoped projection that drops the bridge row must not keep the name.
    external = export_dir / "taxon_external_id.jsonl"
    kept = [line for line in external.read_text(encoding="utf-8").splitlines()
            if not (json.loads(line)["taxon_id"] == host
                    and json.loads(line)["source_system"] == "nortaxa")]
    external.write_text("\n".join(kept) + "\n", encoding="utf-8")
    with pytest.raises(ScopeError, match="cites no published reviewed bridge"):
        validate_national_names(export_dir)


# -------------------------------------------------------------- desktop ---


def _lookup(db_path: Path, language: str):
    from database.taxon_lookup import TaxonLookupService
    from database.vernacular_db import VernacularDB

    return TaxonLookupService(
        vernacular_db=VernacularDB(db_path, language_code=language),
        language_code=language,
        include_reference_data=False,
    )


@pytest.fixture()
def superseded_db(tmp_path: Path) -> tuple[Path, int]:
    release, _own_id, host_id = _superseded_release(tmp_path)
    db_path, _summary = _build(tmp_path, release)
    return db_path, host_id


def test_desktop_label_uses_the_national_name_in_norwegian(superseded_db) -> None:
    db_path, host = superseded_db
    lookup = _lookup(db_path, "no")
    assert lookup.display_scientific_name(host) == "Pholiotina rugosa"
    assert lookup.taxon_display_label(host) == "slank ringkjeglesopp (Pholiotina rugosa)"


def test_desktop_label_falls_back_to_col_in_swedish(superseded_db) -> None:
    db_path, host = superseded_db
    lookup = _lookup(db_path, "sv")
    assert lookup.display_scientific_name(host) == "Conocybe rugosa"
    assert lookup.taxon_display_label(host) == "Conocybe rugosa"
    assert _lookup(db_path, "en").display_scientific_name(host) == "Conocybe rugosa"


def test_desktop_concept_without_approved_identity_shows_col(tmp_path: Path) -> None:
    release = _compile(tmp_path)
    host = _usages(release)[("col_xr", "5ZT3G")]["sporely_taxon_id"]
    mycena = _usages(release)[("col_xr", "NOAUT")]["sporely_taxon_id"]
    db_path, _summary = _build(tmp_path, release)
    lookup = _lookup(db_path, "no")
    assert lookup.display_scientific_name(host) == "Conocybe rugosa"
    # Bound by an automatic rule only: still the COL name.
    assert lookup.display_scientific_name(mycena) == "Mycena absentia"


@pytest.mark.parametrize("language", ["no", "sv"])
@pytest.mark.parametrize("prefix", ["Pholiotina rug", "Conocybe rug"])
def test_desktop_search_finds_the_concept_by_either_name(
    superseded_db, language: str, prefix: str,
) -> None:
    db_path, host = superseded_db
    suggestions = _lookup(db_path, language).suggest_scientific_names(prefix)
    assert host in {s["sporely_taxon_id"] for s in suggestions}


def test_desktop_completer_label_names_the_display_concept(superseded_db) -> None:
    from ui.taxon_input_controller import _format_scientific_choice_display

    db_path, host = superseded_db

    def labels(language: str) -> dict[str, str]:
        rows = [s for prefix in ("Pholiotina rug", "Conocybe rug")
                for s in _lookup(db_path, language).suggest_scientific_names(prefix)
                if s["sporely_taxon_id"] == host]
        return {s["scientific_name"]: _format_scientific_choice_display(s) for s in rows}

    norwegian = labels("no")
    assert norwegian["Pholiotina rugosa"] == "Pholiotina rugosa"
    assert norwegian["Conocybe rugosa"] == "Conocybe rugosa  ·  → Pholiotina rugosa"
    swedish = labels("sv")
    assert swedish["Conocybe rugosa"] == "Conocybe rugosa"
    assert swedish["Pholiotina rugosa"] == "Pholiotina rugosa  ·  → Conocybe rugosa"
