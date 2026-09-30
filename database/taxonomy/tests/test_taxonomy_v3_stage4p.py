"""Taxonomy v3 Stage 4P: Dyntaxa as a reviewed-identity-only national source.

Dyntaxa usages reach a Sporely concept only through a reviewed relationship:
an approved ``manual_mappings.yml`` record binds the accepted usage, and its
own synonyms follow it. Nothing is aliased by name, nothing is allocated, and
the bridged LSID is published only as a reviewed bridge. A bridged concept
gains Swedish vernaculars and ``preferred_scientific_name_sv`` from Dyntaxa
under the Stage 3P rule, without touching identity or the canonical name.

The COL/NorTaxa fixture is ``test_bridge_emission``'s. The synthetic Dyntaxa
source reuses its taxa: Pholiotina rugosa (the Sporely 83668 case, COL
``5ZT3G`` Conocybe rugosa), an Entoloma conferendum that agrees with COL
``39ZCL`` on name and authorship, and a Dyntaxa-only species.
"""
from __future__ import annotations

import io
import json
import sqlite3
import sys
import zipfile
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
import national_source  # noqa: E402

from test_bridge_emission import (  # noqa: E402
    _POLICY_PATH,
    _REVIEWED_53482,
    _col_source,
    _export_and_scope,
    _nortaxa_source,
    _resolvable_parents,
    _resolve,
)
from test_compile_release import (  # noqa: E402
    _write_manual_mappings,
    _write_normalized_source,
)

_DYNTAXA_RELEASE = {"version": "2026-09-30", "issued_date": "2026-09-30"}
_LSID = "urn:lsid:dyntaxa.se:Taxon:"
_NAME_LSID = "urn:lsid:dyntaxa.se:TaxonName:"
_RUGOSA = f"{_LSID}1001"
_CONFERENDUM = f"{_LSID}1002"
_DYNTAXA_ONLY = f"{_LSID}1003"

_REVIEWED_RUGOSA = {
    "mapping_id": "dyntaxa-1001-to-col-5ZT3G",
    "source_usage": {"source": "dyntaxa", "namespace": "dyntaxa_taxon_id",
                     "identifier": _RUGOSA},
    "target": {"source_usage": {"source": "col_xr",
                                "namespace": "col_xr_taxon_id",
                                "identifier": "5ZT3G"}},
    "relationship": "exact",
    "review_status": "approved",
    "source_release_range": {"first": "2026-09-30", "last": "2026-09-30"},
    # ``_compile`` replaces this with the pins of the fixture inputs as
    # written, before any test tampers with them: what a reviewer saw.
    "reviewed_against_source_archives": "FIXTURE_PINS",
}


def _fixture_pins(root: Path, codes=("col_xr", "dyntaxa")) -> dict:
    pins = {}
    for code in codes:
        report = json.loads((root / code / "report.json").read_text())
        release = report["profile_source_release"]
        pins[code] = {
            "source_release_id":
                f"{code}:{release['version']}:{release['issued_date']}",
            "sha256": report["archive_sha256"]}
    return pins


def _tamper(root: Path, code: str, **changes) -> None:
    """Change what a later compile sees as a source's release or bytes."""
    path = root / code / "report.json"
    report = json.loads(path.read_text())
    report.update(changes)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")


def _dyntaxa_source(root: Path) -> Path:
    source_dir = _resolvable_parents(_write_normalized_source(
        root / "dyntaxa",
        source_code="dyntaxa",
        source_release=_DYNTAXA_RELEASE,
        identifier_namespace_prefix="urn:lsid:dyntaxa.se:",
        rows=[
            {"core_row_id": f"{_LSID}5000039", "taxon_id": f"{_LSID}5000039",
             "scientific_name": "Fungi", "rank": "kingdom"},
            {"core_row_id": _RUGOSA, "taxon_id": _RUGOSA,
             "parent": f"{_LSID}5000039", "parent_resolution": "resolved",
             "scientific_name": "Pholiotina rugosa",
             "authorship": "(Peck) Singer"},
            # Dyntaxa's split synonym statuses bind with their accepted usage.
            {"core_row_id": f"{_NAME_LSID}2001", "taxon_id": f"{_NAME_LSID}2001",
             "accepted": _RUGOSA, "parent": f"{_LSID}5000039",
             "parent_resolution": "resolved",
             "scientific_name": "Conocybe rugosa",
             "authorship": "(Peck) Watling", "status": "homotypicSynonym"},
            # A misapplied name is another taxon's name used in error.
            {"core_row_id": f"{_NAME_LSID}2002", "taxon_id": f"{_NAME_LSID}2002",
             "accepted": _RUGOSA, "parent": f"{_LSID}5000039",
             "parent_resolution": "resolved",
             "scientific_name": "Pholiotina blattaria",
             "authorship": "auct.", "status": "misapplied"},
            # Same name and authorship as COL 39ZCL: still not identity.
            {"core_row_id": _CONFERENDUM, "taxon_id": _CONFERENDUM,
             "parent": f"{_LSID}5000039", "parent_resolution": "resolved",
             "scientific_name": "Entoloma conferendum",
             "authorship": "(Britzelm.) Noordel."},
            {"core_row_id": _DYNTAXA_ONLY, "taxon_id": _DYNTAXA_ONLY,
             "parent": f"{_LSID}5000039", "parent_resolution": "resolved",
             "scientific_name": "Cortinarius suecicus",
             "authorship": "Sw."},
        ],
    ), "dyntaxa")
    vernaculars = [
        (_RUGOSA, "sv", "rynkig nålskivling", True),
        (_CONFERENDUM, "sv", "stjärnrödhätting", True),
        (_DYNTAXA_ONLY, "sv", "svensk spindling", True),
    ]
    with (source_dir / "vernacular.jsonl").open("w", encoding="utf-8") as handle:
        for core_row_id, language, name, preferred in vernaculars:
            handle.write(json.dumps({
                "source_code": "dyntaxa",
                "source_release": _DYNTAXA_RELEASE,
                "core_row_id": {"value": core_row_id,
                                "namespace": "dyntaxa_dwc_id"},
                "vernacular_name": name,
                "language": language,
                "is_preferred": preferred,
            }, ensure_ascii=False, sort_keys=True) + "\n")
    report_path = source_dir / "report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["record_counts"]["VernacularName"] = len(vernaculars)
    report["outputs"]["vernacular"] = "vernacular.jsonl"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n",
                           encoding="utf-8")
    return source_dir


def _compile(tmp_path: Path, *, manual: list[dict], with_dyntaxa: bool = True,
             registry: str = "registry.jsonl",
             release_id: str = "tax-2026.09.30-01",
             tamper: dict[str, dict] | None = None) -> Path:
    root = tmp_path / "sources"
    sources = [_col_source(root), _nortaxa_source(root)]
    if with_dyntaxa:
        sources.append(_dyntaxa_source(root))
        pins = _fixture_pins(root)
        manual = [
            {**m, "reviewed_against_source_archives": pins}
            if m.get("reviewed_against_source_archives") == "FIXTURE_PINS" else m
            for m in manual]
    for code, changes in (tamper or {}).items():
        _tamper(root, code, **changes)
    out = tmp_path / f"release-{registry}-{release_id}"
    compile_release(
        normalized_source_dirs=sources,
        manual_mappings_path=_write_manual_mappings(
            tmp_path / f"manual-{registry}.yml", manual),
        mapping_policy_path=_POLICY_PATH,
        registry_path=tmp_path / registry,
        output_dir=out,
        release_id=release_id,
    )
    return out


def _lines(path: Path) -> list[dict]:
    return [json.loads(line) for line in
            path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _dyntaxa_usages(release: Path) -> dict[str, dict]:
    return {u["source_usage"]["identifier"]: u
            for u in _lines(release / "source_usages.jsonl")
            if u["source_code"] == "dyntaxa"}


def _candidate(tmp_path: Path, release: Path,
               registry: str = "registry.jsonl") -> sqlite3.Connection:
    db_path = tmp_path / f"{release.name}.sqlite3"
    build_candidate(release_dir=release, registry_path=tmp_path / registry,
                    output_db=db_path)
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


# ------------------------------------------------------------- compiler ---


def test_unreviewed_dyntaxa_usages_gain_no_identity(tmp_path: Path) -> None:
    release = _compile(tmp_path, manual=[])

    # No binding at all: not by the agreeing name, not as a new concept.
    assert _dyntaxa_usages(release) == {}
    registry = _lines(tmp_path / "registry.jsonl")
    assert not [e for e in registry if e.get("source") == "dyntaxa"]
    diagnostics = json.loads((release / "diagnostics.json").read_text())
    dyntaxa = diagnostics["counts"]["reviewed_identity_only_sources"]["dyntaxa"]
    assert dyntaxa["bound_usages"] == 0
    assert dyntaxa["in_scope_non_synonym_usages"] == 5
    assert dyntaxa["synonyms_left_unbound"] == 1
    assert dyntaxa["vernacular_rows_attached"] == 0
    assert dyntaxa["vernacular_rows_dropped_unbound"] == 3

    conn = _candidate(tmp_path, release)
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM taxon_external_id_text_min "
            "WHERE source_system = 'dyntaxa'").fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM taxon_min "
            "WHERE preferred_scientific_name_sv IS NOT NULL").fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM vernacular_min WHERE source = 'dyntaxa'"
        ).fetchone()[0] == 0
        # The agreeing-name Dyntaxa id resolves to nothing.
        assert _resolve(conn, "dyntaxa", "dyntaxa_taxon_id", _CONFERENDUM) == []
    finally:
        conn.close()


def test_reviewed_bridge_carries_swedish_names_without_touching_identity(
    tmp_path: Path,
) -> None:
    release = _compile(tmp_path, manual=[_REVIEWED_RUGOSA])
    usages = _dyntaxa_usages(release)
    host = usages[_RUGOSA]["sporely_taxon_id"]
    assert usages[_RUGOSA]["alias_reason"] == "manual_approved_exact"
    # The homotypic synonym follows its accepted usage; the misapplied name,
    # the agreeing name and the Dyntaxa-only species do not bind.
    assert usages[f"{_NAME_LSID}2001"]["sporely_taxon_id"] == host
    assert usages[f"{_NAME_LSID}2001"]["alias_reason"] == "synonym_of_accepted"
    assert set(usages) == {_RUGOSA, f"{_NAME_LSID}2001"}

    conn = _candidate(tmp_path, release)
    try:
        row = dict(conn.execute("SELECT * FROM taxon_min WHERE taxon_id = ?",
                                (host,)).fetchone())
        assert row["canonical_scientific_name"] == "Conocybe rugosa"
        assert (row["canonical_source_system"], row["canonical_external_id"]) \
            == ("col_xr", "5ZT3G")
        assert row["preferred_scientific_name_sv"] == "Pholiotina rugosa"
        assert row["preferred_scientific_name_sv_source_system"] == "dyntaxa"
        assert row["preferred_scientific_name_sv_namespace"] == "dyntaxa_taxon_id"
        assert row["preferred_scientific_name_sv_external_id"] == _RUGOSA
        assert row["preferred_scientific_name_no"] is None
        assert [tuple(r) for r in conn.execute(
            "SELECT language_code, vernacular_name, source FROM vernacular_min "
            "WHERE taxon_id = ? AND language_code = 'sv'", (host,))] == [
            ("sv", "rynkig nålskivling", "dyntaxa")]

        # Only the reviewed accepted LSID is published, never its synonym.
        assert _resolve(conn, "dyntaxa", "dyntaxa_taxon_id", _RUGOSA) == [host]
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM taxon_external_id_text_min "
            "WHERE source_system = 'dyntaxa'")]
        assert len(rows) == 1
        assert rows[0]["note"] == "authoritative_bridge:manual_approved_exact"
        assert rows[0]["is_preferred"] == 0
        assert rows[0]["id_role"] == "accepted"
        # The synonym stays searchable.
        assert conn.execute(
            "SELECT COUNT(*) FROM scientific_name_min "
            "WHERE taxon_id = ? AND scientific_name = 'Conocybe rugosa'",
            (host,)).fetchone()[0] >= 1
    finally:
        conn.close()


@pytest.mark.parametrize("later_mapping", [
    None,
    {**_REVIEWED_RUGOSA, "review_status": "rejected"},
], ids=["approval-removed", "approval-rejected"])
def test_retained_registry_aliases_contribute_nothing_without_approval(
    tmp_path: Path, later_mapping: dict | None,
) -> None:
    """The registry is append-only, so the aliases an approved build made
    survive into a later build. They are history: once the approval is
    removed or rejected, the release carries no Dyntaxa binding, name,
    vernacular or bridge, and no synonym is newly aliased."""
    first = _compile(tmp_path, manual=[_REVIEWED_RUGOSA],
                     release_id="tax-2026.09.30-01")
    host = _dyntaxa_usages(first)[_RUGOSA]["sporely_taxon_id"]
    retained = [e for e in _lines(tmp_path / "registry.jsonl")
                if e.get("source") == "dyntaxa"]
    assert len(retained) == 2

    later = _compile(tmp_path, manual=[later_mapping] if later_mapping else [],
                     release_id="tax-2026.09.30-02")

    assert _dyntaxa_usages(later) == {}
    # History is kept; nothing was added.
    assert [e for e in _lines(tmp_path / "registry.jsonl")
            if e.get("source") == "dyntaxa"] == retained
    diagnostics = json.loads((later / "diagnostics.json").read_text())
    dyntaxa = diagnostics["counts"]["reviewed_identity_only_sources"]["dyntaxa"]
    assert dyntaxa["bound_usages"] == 0
    assert dyntaxa["registry_aliases_withheld"] == 2
    assert dyntaxa["vernacular_rows_attached"] == 0
    assert not [v for v in _lines(later / "vernacular.jsonl")
                if v["source_code"] == "dyntaxa"]

    conn = _candidate(tmp_path, later)
    try:
        row = dict(conn.execute("SELECT * FROM taxon_min WHERE taxon_id = ?",
                                (host,)).fetchone())
        assert row["preferred_scientific_name_sv"] is None
        assert row["preferred_scientific_name_sv_source_system"] is None
        assert _resolve(conn, "dyntaxa", "dyntaxa_taxon_id", _RUGOSA) == []
        assert conn.execute(
            "SELECT COUNT(*) FROM vernacular_min WHERE source = 'dyntaxa'"
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM scientific_name_min WHERE taxon_id = ? "
            "AND scientific_name = 'Pholiotina rugosa'", (host,)
        ).fetchone()[0] == 0
    finally:
        conn.close()

    # Re-approving restores exactly the earlier contribution.
    again = _compile(tmp_path, manual=[_REVIEWED_RUGOSA],
                     release_id="tax-2026.09.30-03")
    assert set(_dyntaxa_usages(again)) == {_RUGOSA, f"{_NAME_LSID}2001"}


@pytest.mark.parametrize("tamper,reason", [
    ({"dyntaxa": {"profile_source_release": {"version": "2026-12-31",
                                             "issued_date": "2026-12-31"}}},
     "source_release_range_is_not_the_compiled_release"),
    ({"dyntaxa": {"archive_sha256": "d" * 64}},
     "compiled_dyntaxa_differs_from_reviewed_pin"),
    ({"col_xr": {"archive_sha256": "c" * 64}},
     "compiled_col_xr_differs_from_reviewed_pin"),
], ids=["later-dyntaxa-release", "changed-dyntaxa-bytes", "changed-col-bytes"])
def test_unchanged_approval_does_not_apply_to_other_source_inputs(
    tmp_path: Path, tamper: dict, reason: str,
) -> None:
    """The approval stays exactly as reviewed; only the compiled inputs move.
    An approved build first leaves its aliases in the registry, so this also
    shows the retained aliases stay inert."""
    first = _compile(tmp_path, manual=[_REVIEWED_RUGOSA],
                     release_id="tax-2026.09.30-01")
    host = _dyntaxa_usages(first)[_RUGOSA]["sporely_taxon_id"]

    later = _compile(tmp_path, manual=[_REVIEWED_RUGOSA],
                     release_id="tax-2026.09.30-02", tamper=tamper)

    assert _dyntaxa_usages(later) == {}
    dyntaxa = json.loads((later / "diagnostics.json").read_text())[
        "counts"]["reviewed_identity_only_sources"]["dyntaxa"]
    assert dyntaxa["approved_mappings_not_applicable"] == {reason: 1}
    assert dyntaxa["registry_aliases_withheld"] == 2
    assert dyntaxa["vernacular_rows_attached"] == 0
    conn = _candidate(tmp_path, later)
    try:
        assert _resolve(conn, "dyntaxa", "dyntaxa_taxon_id", _RUGOSA) == []
        assert conn.execute(
            "SELECT preferred_scientific_name_sv FROM taxon_min "
            "WHERE taxon_id = ?", (host,)).fetchone()[0] is None
    finally:
        conn.close()


def test_approval_without_reviewed_pins_does_not_apply(tmp_path: Path) -> None:
    unpinned = {k: v for k, v in _REVIEWED_RUGOSA.items()
                if k != "reviewed_against_source_archives"}
    release = _compile(tmp_path, manual=[unpinned])
    assert _dyntaxa_usages(release) == {}
    dyntaxa = json.loads((release / "diagnostics.json").read_text())[
        "counts"]["reviewed_identity_only_sources"]["dyntaxa"]
    assert dyntaxa["approved_mappings_not_applicable"] == {
        "no_reviewed_source_pins": 1}


def test_existing_nortaxa_and_col_bindings_are_unchanged(tmp_path: Path) -> None:
    """Production order: the registry already holds the COL and NorTaxa
    concepts, and a later release adds Dyntaxa. The same later release
    without Dyntaxa is the control."""
    import shutil

    _compile(tmp_path, manual=[_REVIEWED_53482], with_dyntaxa=False,
             release_id="tax-2026.09.29-01")
    shutil.copy(tmp_path / "registry.jsonl", tmp_path / "registry-b.jsonl")
    before = _lines(tmp_path / "registry.jsonl")
    without = _compile(tmp_path, manual=[_REVIEWED_53482], with_dyntaxa=False)
    with_dyntaxa = _compile(tmp_path, manual=[_REVIEWED_53482, _REVIEWED_RUGOSA],
                            registry="registry-b.jsonl")

    def others(release: Path) -> list[dict]:
        return [u for u in _lines(release / "source_usages.jsonl")
                if u["source_code"] != "dyntaxa"]

    assert others(without) == others(with_dyntaxa)
    assert _lines(without / "taxa.jsonl") == _lines(with_dyntaxa / "taxa.jsonl")
    # The registry only gains the two reviewed Dyntaxa aliases.
    after = _lines(tmp_path / "registry-b.jsonl")
    assert all(entry in after for entry in before)
    added = [entry for entry in after if entry not in before]
    assert sorted((e["source"], e["identifier"], e["kind"]) for e in added) == [
        ("dyntaxa", _RUGOSA, "alias"), ("dyntaxa", f"{_NAME_LSID}2001", "alias")]
    host = _dyntaxa_usages(with_dyntaxa)[_RUGOSA]["sporely_taxon_id"]
    assert {e["sporely_taxon_id"] for e in added} == {host}

    a = _candidate(tmp_path, without)
    b = _candidate(tmp_path, with_dyntaxa, "registry-b.jsonl")
    try:
        query = ("SELECT taxon_id, source_system, namespace, external_id, note "
                 "FROM taxon_external_id_text_min WHERE source_system != 'dyntaxa' "
                 "ORDER BY 1, 2, 3, 4")
        assert [tuple(r) for r in a.execute(query)] == \
            [tuple(r) for r in b.execute(query)]
        assert _resolve(b, "nortaxa", "nortaxa_taxon_id", "53482") == \
            _resolve(a, "nortaxa", "nortaxa_taxon_id", "53482") != []
    finally:
        a.close()
        b.close()


def test_reviewed_dyntaxa_bridge_reaches_the_cloud_export(tmp_path: Path) -> None:
    release = _compile(tmp_path, manual=[_REVIEWED_RUGOSA])
    host = _dyntaxa_usages(release)[_RUGOSA]["sporely_taxon_id"]
    export_dir, _gz = _export_and_scope(tmp_path, release)

    external = [r for r in _lines(export_dir / "taxon_external_id.jsonl")
                if r["source_system"] == "dyntaxa"]
    assert external == [{
        "taxon_id": host, "source_system": "dyntaxa",
        "namespace": "dyntaxa_taxon_id", "external_id": _RUGOSA,
        "id_role": "accepted", "is_preferred": False,
        "external_name": "Pholiotina rugosa",
        "note": "authoritative_bridge:manual_approved_exact",
    }]
    vernacular = [r for r in _lines(export_dir / "vernacular.jsonl")
                  if r["taxon_id"] == host and r["language_code"] == "sv"]
    assert [(r["vernacular_name"], r["source"]) for r in vernacular] == [
        ("rynkig nålskivling", "dyntaxa")]
    taxon = next(r for r in _lines(export_dir / "taxon.jsonl")
                 if r["taxon_id"] == host)
    assert taxon["preferred_scientific_name_sv"] == "Pholiotina rugosa"
    assert taxon["preferred_scientific_name_sv_source_system"] == "dyntaxa"
    assert taxon["preferred_scientific_name_sv_namespace"] == "dyntaxa_taxon_id"
    assert taxon["preferred_scientific_name_sv_external_id"] == _RUGOSA
    assert taxon["canonical_scientific_name"] == "Conocybe rugosa"


# ------------------------------------------------------------- adapter ---


_META = """<?xml version="1.0" encoding="UTF-8"?>
<archive metadata="eml.xml" xmlns="http://rs.tdwg.org/dwc/text/">
  <core encoding="UTF-8" fieldsTerminatedBy="\\t" linesTerminatedBy="\\n" fieldsEnclosedBy="" ignoreHeaderLines="1" rowType="http://rs.tdwg.org/dwc/terms/Taxon">
    <files><location>Taxon.csv</location></files>
    <id index="0" />
    <field index="0" term="http://rs.tdwg.org/dwc/terms/taxonID" />
    <field index="1" term="http://rs.tdwg.org/dwc/terms/acceptedNameUsageID" />
    <field index="2" term="http://rs.tdwg.org/dwc/terms/parentNameUsageID" />
    <field index="3" term="http://rs.tdwg.org/dwc/terms/scientificName" />
    <field index="4" term="http://rs.tdwg.org/dwc/terms/taxonRank" />
    <field index="5" term="http://rs.tdwg.org/dwc/terms/scientificNameAuthorship" />
    <field index="6" term="http://rs.tdwg.org/dwc/terms/taxonomicStatus" />
    <field index="7" term="http://rs.tdwg.org/dwc/terms/kingdom" />
    <field index="8" term="http://rs.tdwg.org/dwc/terms/family" />
    <field index="9" term="http://rs.tdwg.org/dwc/terms/genus" />
  </core>
  <extension encoding="UTF8" fieldsTerminatedBy="\\t" linesTerminatedBy="\\n" fieldsEnclosedBy="" ignoreHeaderLines="1" rowType="http://rs.gbif.org/terms/1.0/VernacularName">
    <files><location>VernacularName.csv</location></files>
    <coreid index="0" />
    <field index="1" term="http://rs.tdwg.org/dwc/terms/vernacularName" />
    <field index="2" term="http://purl.org/dc/terms/language" />
    <field index="3" term="http://rs.tdwg.org/dwc/terms/countryCode" />
    <field index="4" term="http://rs.gbif.org/terms/1.0/isPreferredName" />
  </extension>
  <extension encoding="UTF8" fieldsTerminatedBy="\\t" linesTerminatedBy="\\n" fieldsEnclosedBy="" ignoreHeaderLines="1" rowType="http://rs.gbif.org/terms/1.0/Reference">
    <files><location>Reference.csv</location></files>
    <coreid index="0" />
    <field index="1" term="http://purl.org/dc/terms/title" />
  </extension>
</archive>
"""


def _synthetic_archive(path: Path, *, reference_location: str = "Reference.csv"
                       ) -> Path:
    """The real Dyntaxa export's shape: LSID ids, a UTF8-labelled extension
    and a Reference extension the adapter does not import."""
    taxa = "\n".join("\t".join(row) for row in [
        ["taxonId", "acceptedNameUsageID", "parentNameUsageID", "scientificName",
         "taxonRank", "scientificNameAuthorship", "taxonomicStatus", "kingdom",
         "family", "genus"],
        [f"{_LSID}5000039", f"{_LSID}5000039", "", "Fungi", "kingdom", "",
         "accepted", "Fungi", "", ""],
        [_RUGOSA, _RUGOSA, f"{_LSID}5000039", "Pholiotina rugosa", "species",
         "(Peck) Singer", "accepted", "Fungi", "Bolbitiaceae", "Pholiotina"],
        [f"{_NAME_LSID}2001", _RUGOSA, f"{_LSID}5000039", "Conocybe rugosa",
         "species", "(Peck) Watling", "homotypicSynonym", "Fungi",
         "Bolbitiaceae", "Conocybe"],
    ]) + "\n"
    vernacular = "\n".join("\t".join(row) for row in [
        ["taxonId", "vernacularName", "language", "countryCode", "isPreferredName"],
        [_RUGOSA, "rynkig nålskivling", "sv", "SE", "true"],
    ]) + "\n"
    meta = _META.replace("<location>Reference.csv</location>",
                         f"<location>{reference_location}</location>")
    with zipfile.ZipFile(path, "w") as bundle:
        bundle.writestr("meta.xml", "﻿" + meta)
        bundle.writestr("eml.xml", "<eml/>")
        bundle.writestr("Taxon.csv", "﻿" + taxa)
        bundle.writestr("VernacularName.csv", "﻿" + vernacular)
        bundle.writestr(reference_location, f"taxonId\ttitle\n{_RUGOSA}\tx\n")
    return path


def _profile(tmp_path: Path) -> Path:
    """The committed Dyntaxa profile, without its Distribution extension."""
    real = _TAXONOMY / "national_sources" / "dyntaxa" / "2026-09-30" / "source.json"
    profile = json.loads(real.read_text(encoding="utf-8"))
    profile.pop("distribution")
    path = tmp_path / "source.json"
    path.write_text(json.dumps(profile), encoding="utf-8")
    return path


def test_profile_normalizes_lsids_verbatim_and_ignores_references(
    tmp_path: Path,
) -> None:
    archive = _synthetic_archive(tmp_path / "dyntaxa.zip")
    profile = national_source.load_profile(_profile(tmp_path))
    report = national_source.normalize_archive(profile, archive,
                                               tmp_path / "out")
    assert report["result"] == "passed"
    assert "Reference" not in report["record_counts"]
    taxa = _lines(tmp_path / "out" / "taxa.jsonl")
    assert [t["taxon_id"] for t in taxa][1] == {
        "namespace": "dyntaxa_taxon_id", "value": _RUGOSA}
    assert taxa[2]["taxonomic_status"] == "homotypicSynonym"
    vernacular = _lines(tmp_path / "out" / "vernacular.jsonl")
    assert [(v["vernacular_name"], v["language"], v["is_preferred"])
            for v in vernacular] == [("rynkig nålskivling", "sv", True)]


def test_ignored_extension_must_match_the_profile(tmp_path: Path) -> None:
    archive = _synthetic_archive(tmp_path / "dyntaxa.zip",
                                 reference_location="Other.csv")
    profile = national_source.load_profile(_profile(tmp_path))
    with pytest.raises(national_source.NationalSourceError,
                       match="ignored extension location mismatch"):
        national_source.validate_archive(profile, archive)


def test_undeclared_extension_is_still_refused(tmp_path: Path) -> None:
    archive = _synthetic_archive(tmp_path / "dyntaxa.zip")
    raw = json.loads(_profile(tmp_path).read_text(encoding="utf-8"))
    raw.pop("ignored_extensions")
    path = tmp_path / "strict.json"
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(national_source.NationalSourceError,
                       match="unsupported extension row type"):
        national_source.validate_archive(national_source.load_profile(path),
                                         archive)


def test_profile_cannot_ignore_the_vernacular_extension(tmp_path: Path) -> None:
    raw = json.loads(_profile(tmp_path).read_text(encoding="utf-8"))
    raw["ignored_extensions"].append({
        "row_type": "http://rs.gbif.org/terms/1.0/VernacularName",
        "location": "VernacularName.csv"})
    path = tmp_path / "bad.json"
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(national_source.NationalSourceError,
                       match="cannot ignore"):
        national_source.load_profile(path)


def test_acquisition_record_pins_archive_licence_and_citation() -> None:
    source = _TAXONOMY / "sources" / "dyntaxa" / "2026-09-30"
    manifest = json.loads((source / "manifest.json").read_text(encoding="utf-8"))
    request = json.loads((source / "request.json").read_text(encoding="utf-8"))
    assert manifest["download"]["sha256"] == manifest["download"]["archive_sha256"]
    assert len(manifest["download"]["sha256"]) == 64
    assert manifest["licence"]["identifier"] == "CC0 1.0"
    assert "10.15468/j43wfc" in manifest["citation"]["gbif"]
    assert request["gbif_dataset_key"] == "de8934f4-a136-481c-a87a-b0b202b80a31"
    # SLU's API key is not copied into the repository.
    assert "subscription-key=<" in request["archive_endpoint"]
    profile = json.loads((_TAXONOMY / "national_sources" / "dyntaxa" /
                          "2026-09-30" / "source.json").read_text())
    assert profile["source_release"] == {
        "version": manifest["release"]["version"],
        "issued_date": manifest["release"]["issued_date"]}
