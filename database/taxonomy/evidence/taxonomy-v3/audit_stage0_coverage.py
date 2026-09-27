#!/usr/bin/env python3
"""Taxonomy v3 Stage 0: bridge coverage and duplicate-candidate audit.

Recomputes, against the tracked release artifact and its pinned source
archives, the two populations the Taxonomy v3 plan works on:

* **Group A** — a NorTaxa taxonID carried on a COL-canonical concept by the
  compiler's automatic exact match (``note = cross_source_automatic_exact``).
  Automatic matches are not authoritative, so none of them is published as a
  bridge.
* **Group B** — a NorTaxa-canonical concept and a COL-canonical concept that
  share a ``canonical_scientific_name`` but are separate Sporely concepts. Name
  equality only sizes this population; it proves nothing.

Every association in both groups is graded by
``scripts/cross_reference_evidence.py`` — the sources' own published synonymy
— and each group is reported for the full release and for the cloud scope
(``global_macrofungi_policy_v1``, computed in-process with
``macrofungi_scope`` exactly as the scoped export builds it).

Group A is also written out as one immutable candidate manifest per evidence
class. A manifest lists every association with its evidence detail, is pinned
to the release and source-archive fingerprints, and carries a SHA-256 over its
members. Manifests are review inputs: every member is ``needs_review``.

Read-only over the release, archives, policies and registry. Fails closed if an
archive does not match the fingerprint the release records for it. Writes only
into ``--out-dir``; output is byte-deterministic.

    .venv/bin/python database/taxonomy/evidence/taxonomy-v3/audit_stage0_coverage.py
"""
from __future__ import annotations

import argparse
import collections
import gzip
import hashlib
import json
import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path

_REPO = Path(__file__).resolve().parents[4]
_TAXONOMY = _REPO / "database" / "taxonomy"
for _path in (_REPO, _TAXONOMY / "scripts"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from cross_reference_evidence import (  # noqa: E402
    EVIDENCE_NONE,
    EVIDENCE_ONE_DIRECTIONAL,
    EVIDENCE_RECIPROCAL,
    EVIDENCE_SHARED_SYNONYMY,
    REVIEWABLE,
    grade,
    read_col,
    read_nortaxa,
)
from database.taxonomy import cloud_export, macrofungi_scope  # noqa: E402

EVIDENCE_CLASSES = (EVIDENCE_RECIPROCAL, EVIDENCE_ONE_DIRECTIONAL,
                    EVIDENCE_SHARED_SYNONYMY, EVIDENCE_NONE)

GENERATED = Path("database/reference_data/generated/taxonomy_v2")
DEFAULT_MANIFEST = GENERATED / "manifest.json"
SCOPE_POLICY = Path("database/taxonomy/policies/global-macrofungi-scope.yml")
DEFAULT_NORTAXA = Path("database/taxonomy/sources/nortaxa/1.284/archive.zip")
DEFAULT_COL = Path("database/taxonomy/sources/col_xr/2026-07-17-XR/archive.zip")
DEFAULT_OUT = Path("database/taxonomy/evidence/taxonomy-v3/stage0")

#: Reviewed (non-automatic) NorTaxa bridge notes the compiler writes.
REVIEWED_NOTES = ("manual_approved_exact", "reviewed_supersession")

#: The plan's regression species, located by identifier, never by name.
REGRESSIONS = {
    "Entoloma conferendum": {"nortaxa_taxon_id": "53482"},
    "Pholiotina rugosa": {"nortaxa_taxon_id": "52369"},
    "Craterellus tubaeformis": {"nortaxa_taxon_id": "56227"},
    # Canonical names differ, so this pair is outside the name-equality
    # Group B; it is graded as an explicit pair instead.
    "Conocybe vexans / Pholiotina vexans": {"nortaxa_taxon_id": "58766",
                                            "graded_pair": ["58766", "XQZ6"]},
    "Cantharellus cibarius": {"sporely_taxon_id": 168873},
}

MANIFEST_COLUMNS = [
    "nortaxa_taxon_id",
    "col_usage_id",
    "sporely_taxon_id",
    "in_cloud_scope",
    "bridge_accepted_name",
    "backbone_accepted_name",
    "accepted_names_agree",
    "bridge_lists_backbone_accepted_name_as_synonym",
    "backbone_lists_bridge_accepted_name_as_synonym",
    "shared_synonym_count",
    "shared_synonyms",
    "bridge_synonym_count",
    "backbone_synonym_count",
]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


def _write_json(path: Path, value) -> str:
    data = (json.dumps(value, ensure_ascii=False, indent=1, sort_keys=True)
            + "\n").encode("utf-8")
    path.write_bytes(data)
    return hashlib.sha256(data).hexdigest()


# ------------------------------------------------------------------ inputs ---


def verify_release(manifest_path: Path, sqlite_path: Path) -> dict:
    """Check the tracked artifact against its manifest; return fingerprints."""
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    gz_path = manifest_path.parent / manifest["gz_artifact"]
    gz_sha = _sha256_file(gz_path)
    if gz_sha != manifest["gz_sha256"]:
        raise SystemExit(f"release artifact does not match manifest: {gz_path}")
    with gzip.open(gz_path, "rb") as source, sqlite_path.open("wb") as target:
        shutil.copyfileobj(source, target, 1024 * 1024)
    if _sha256_file(sqlite_path) != manifest["sqlite_sha256"]:
        raise SystemExit("decompressed release does not match manifest")
    return {
        "content_release_id": manifest["content_release_id"],
        "gz_artifact": str(GENERATED / manifest["gz_artifact"]),
        "gz_sha256": gz_sha,
        "sqlite_sha256": manifest["sqlite_sha256"],
    }


def verify_archive(conn: sqlite3.Connection, source: str, path: Path) -> dict:
    """Fail closed unless the archive is the one the release was built from."""
    meta = dict(conn.execute("SELECT key, value FROM taxonomy_meta"))
    expected = meta[f"source_release[{source}].archive_sha256"]
    actual = _sha256_file(path)
    if actual != expected:
        raise SystemExit(
            f"{source} archive {path} has sha256 {actual}, but the release "
            f"was built from {expected}")
    # No path: the fingerprint identifies the archive wherever it lives.
    return {"sha256": actual,
            "source_release_id": meta[f"source_release[{source}].id"]}


def cloud_scope(conn: sqlite3.Connection, policy_path: Path) -> set[int]:
    """Concepts the ``global_macrofungi_policy_v1`` export carries.

    Mirrors ``macrofungi_scope.build_export``: included concepts plus their
    required classification ancestors, restricted to the W1 concept set the
    scoped export filters from.
    """
    policy = macrofungi_scope.load_policy(policy_path)
    taxa, by_col = macrofungi_scope.load_taxa(conn)
    rules = macrofungi_scope.resolve_rules(policy, by_col)
    results = macrofungi_scope.evaluate(
        taxa, rules, policy.get("source_characteristic_exclusions", []))
    included = {t for t, r in results.items() if r["state"] == "include"}
    exported = included | macrofungi_scope.required_ancestors(taxa, included)
    conn.row_factory = sqlite3.Row
    try:
        w1 = set(cloud_export.build_concept_set(conn).concept_ids)
    finally:
        conn.row_factory = None
    return exported & w1


def group_a(conn: sqlite3.Connection) -> list[tuple[str, str, int]]:
    """``(nortaxa_taxon_id, col_usage_id, sporely_taxon_id)`` per automatic row."""
    return [
        (str(ext), str(col), int(tid))
        for ext, col, tid in conn.execute(
            "SELECT e.external_id, t.canonical_external_id, t.taxon_id "
            "FROM taxon_external_id_min e JOIN taxon_min t USING (taxon_id) "
            "WHERE e.source_system = 'artsdatabanken' "
            "AND e.note = 'cross_source_automatic_exact' "
            "AND t.canonical_source_system = 'col_xr' "
            "ORDER BY e.external_id, t.taxon_id")
    ]


def group_a_unfiltered_count(conn: sqlite3.Connection) -> int:
    return conn.execute(
        "SELECT count(*) FROM taxon_external_id_min "
        "WHERE source_system = 'artsdatabanken' "
        "AND note = 'cross_source_automatic_exact'").fetchone()[0]


def group_b(conn: sqlite3.Connection) -> list[tuple[str, int, str, int]]:
    """``(nortaxa_taxon_id, nortaxa_sporely_id, col_usage_id, col_sporely_id)``."""
    return [
        (str(n_ext), int(n_id), str(c_ext), int(c_id))
        for n_ext, n_id, c_ext, c_id in conn.execute(
            "SELECT n.canonical_external_id, n.taxon_id, "
            "       c.canonical_external_id, c.taxon_id "
            "FROM taxon_min n JOIN taxon_min c "
            "  ON c.canonical_scientific_name = n.canonical_scientific_name "
            " AND c.canonical_source_system = 'col_xr' "
            "WHERE n.canonical_source_system = 'nortaxa' "
            "ORDER BY n.canonical_external_id, c.canonical_external_id")
    ]


def reviewed_bridges(conn: sqlite3.Connection) -> list[dict]:
    placeholders = ",".join("?" * len(REVIEWED_NOTES))
    return [
        {"nortaxa_taxon_id": str(ext), "sporely_taxon_id": int(tid),
         "note": note}
        for ext, tid, note in conn.execute(
            "SELECT external_id, taxon_id, note FROM taxon_external_id_min "
            "WHERE source_system = 'artsdatabanken' "
            f"AND note IN ({placeholders}) ORDER BY external_id",
            REVIEWED_NOTES)
    ]


# --------------------------------------------------------------- reporting ---


def _histogram(values) -> dict[str, int]:
    counts = collections.Counter(values)
    return {str(k): counts[k] for k in sorted(counts)}


def summarise(rows: list[dict], *, taxon_key: str) -> dict:
    """Counts by evidence class, plus shared-synonym distributions."""
    by_class = collections.Counter(row["evidence_class"] for row in rows)
    return {
        "associations": len(rows),
        "distinct_nortaxa_taxon_ids": len({r["nortaxa_taxon_id"] for r in rows}),
        "distinct_sporely_taxa": len({r[taxon_key] for r in rows}),
        "by_evidence_class": {cls: by_class.get(cls, 0)
                              for cls in EVIDENCE_CLASSES},
        "reviewable_total": sum(by_class.get(cls, 0) for cls in REVIEWABLE),
        "shared_synonym_count_distribution": {
            cls: _histogram(r["shared_synonym_count"] for r in rows
                            if r["evidence_class"] == cls)
            for cls in REVIEWABLE
        },
    }


def build_manifest(evidence_class: str, rows: list[dict],
                   pins: dict) -> dict:
    members = [[row[column] for column in MANIFEST_COLUMNS]
               for row in rows if row["evidence_class"] == evidence_class]
    members.sort(key=lambda m: (m[0], m[1], m[2]))
    return {
        "format": "sporely-taxonomy-v3-candidate-manifest-v1",
        "population": "group_a_automatic_nortaxa_bridge",
        "evidence_class": evidence_class,
        "review_status": "needs_review",
        "note": ("Immutable review input. Membership is pinned to the release "
                 "and source archives below; it approves nothing and never "
                 "extends to taxa in a later source release. shared_synonyms "
                 "lists at most 10 names; shared_synonym_count is exact."),
        "pins": pins,
        "columns": MANIFEST_COLUMNS,
        "member_count": len(members),
        "members_sha256": hashlib.sha256(
            _canonical({"columns": MANIFEST_COLUMNS, "members": members})
        ).hexdigest(),
        "members": members,
    }


def _extra_pairs() -> list[tuple[str, str]]:
    return [tuple(p["graded_pair"]) for p in REGRESSIONS.values()
            if "graded_pair" in p]


def classify_regressions(a_rows, b_rows, reviewed, scope, extra) -> dict:
    out = {}
    reviewed_by_id = {r["nortaxa_taxon_id"]: r for r in reviewed}
    for species, probe in REGRESSIONS.items():
        nortaxa_id = probe.get("nortaxa_taxon_id")
        sporely_id = probe.get("sporely_taxon_id")
        placements = []
        if nortaxa_id in reviewed_by_id:
            r = reviewed_by_id[nortaxa_id]
            placements.append({
                "population": "reviewed_bridge_emitted",
                "nortaxa_taxon_id": nortaxa_id,
                "sporely_taxon_id": r["sporely_taxon_id"],
                "note": r["note"],
                "in_cloud_scope": r["sporely_taxon_id"] in scope})
        for row in a_rows:
            if row["nortaxa_taxon_id"] == nortaxa_id or \
                    row["sporely_taxon_id"] == sporely_id:
                placements.append({
                    "population": "group_a", **{
                        k: row[k] for k in (
                            "nortaxa_taxon_id", "col_usage_id",
                            "sporely_taxon_id", "in_cloud_scope",
                            "evidence_class")}})
        for row in b_rows:
            if row["nortaxa_taxon_id"] == nortaxa_id or \
                    row["col_sporely_taxon_id"] == sporely_id:
                placements.append({
                    "population": "group_b", **{
                        k: row[k] for k in (
                            "nortaxa_taxon_id", "nortaxa_sporely_taxon_id",
                            "col_usage_id", "col_sporely_taxon_id",
                            "col_in_cloud_scope", "evidence_class")}})
        pair = tuple(probe.get("graded_pair") or ())
        if pair and not any(p["population"] == "group_b" for p in placements):
            placements.append({
                "population": "explicit_pair_outside_group_b",
                "nortaxa_taxon_id": pair[0], "col_usage_id": pair[1],
                **extra[pair]})
        if not placements:
            raise SystemExit(f"regression species not classified: {species}")
        out[species] = {"probe": probe, "placements": placements}
    return out


def audit(args: argparse.Namespace) -> dict[str, dict]:
    with tempfile.TemporaryDirectory(prefix="sporely-tv3-stage0-") as tmp:
        sqlite_path = Path(tmp) / "release.sqlite3"
        release = verify_release(args.manifest, sqlite_path)
        conn = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
        try:
            archives = {
                "nortaxa": verify_archive(conn, "nortaxa", args.nortaxa_archive),
                "col_xr": verify_archive(conn, "col_xr", args.col_archive),
            }
            scope = cloud_scope(conn, args.scope_policy)
            a_pairs = group_a(conn)
            a_unfiltered = group_a_unfiltered_count(conn)
            b_pairs = group_b(conn)
            reviewed = reviewed_bridges(conn)
            scope_col_canonical = sum(
                1 for (tid,) in conn.execute(
                    "SELECT taxon_id FROM taxon_min "
                    "WHERE canonical_source_system = 'col_xr'")
                if tid in scope)
        finally:
            conn.close()

    if a_unfiltered != len(a_pairs):
        raise SystemExit("Group A row on a non-COL-canonical concept")

    nortaxa = read_nortaxa(args.nortaxa_archive,
                           {p[0] for p in a_pairs} | {p[0] for p in b_pairs}
                           | {p[0] for p in _extra_pairs()})
    col = read_col(args.col_archive,
                   {p[1] for p in a_pairs} | {p[2] for p in b_pairs}
                   | {p[1] for p in _extra_pairs()})
    extra = {pair: grade(nortaxa.get(pair[0]), col.get(pair[1]))
             for pair in _extra_pairs()}

    a_rows = []
    for nortaxa_id, col_id, taxon_id in a_pairs:
        a_rows.append({
            "nortaxa_taxon_id": nortaxa_id, "col_usage_id": col_id,
            "sporely_taxon_id": taxon_id, "in_cloud_scope": taxon_id in scope,
            **grade(nortaxa.get(nortaxa_id), col.get(col_id))})
    b_rows = []
    for nortaxa_id, n_taxon, col_id, c_taxon in b_pairs:
        b_rows.append({
            "nortaxa_taxon_id": nortaxa_id, "nortaxa_sporely_taxon_id": n_taxon,
            "col_usage_id": col_id, "col_sporely_taxon_id": c_taxon,
            "col_in_cloud_scope": c_taxon in scope,
            "nortaxa_in_cloud_scope": n_taxon in scope,
            **grade(nortaxa.get(nortaxa_id), col.get(col_id))})
    for row in a_rows + b_rows:
        if "shared_synonym_count" not in row:  # no accepted usage in source
            row.update({column: None for column in MANIFEST_COLUMNS
                        if column not in row})
            row["shared_synonym_count"] = 0

    pins = {"release": release, "source_archives": archives,
            "cloud_scope_policy": {
                "path": str(SCOPE_POLICY),
                "sha256": _sha256_file(args.scope_policy),
                "scope_predicate_id": "global_macrofungi_policy_v1"}}

    manifests = {cls: build_manifest(cls, a_rows, pins)
                 for cls in EVIDENCE_CLASSES}
    report = {
        "format": "sporely-taxonomy-v3-stage0-coverage-v1",
        "pins": pins,
        "cloud_scope": {
            "concepts": len(scope),
            "col_canonical_concepts": scope_col_canonical,
        },
        "reviewed_bridges_in_release": reviewed,
        "group_a": {
            "definition": ("taxon_external_id_min rows with source_system "
                           "'artsdatabanken' and note "
                           "'cross_source_automatic_exact'; pair graded is "
                           "(NorTaxa taxonID, host concept's COL id)"),
            "full_release": summarise(a_rows, taxon_key="sporely_taxon_id"),
            "cloud_scope": summarise(
                [r for r in a_rows if r["in_cloud_scope"]],
                taxon_key="sporely_taxon_id"),
        },
        "group_b": {
            "definition": ("NorTaxa-canonical concept and COL-canonical "
                           "concept with equal canonical_scientific_name; "
                           "cloud scope counts pairs whose COL side is in "
                           "scope"),
            "full_release": summarise(b_rows, taxon_key="nortaxa_sporely_taxon_id"),
            "cloud_scope": summarise(
                [r for r in b_rows if r["col_in_cloud_scope"]],
                taxon_key="nortaxa_sporely_taxon_id"),
            "nortaxa_side_in_cloud_scope": sum(
                1 for r in b_rows if r["nortaxa_in_cloud_scope"]),
        },
        "regressions": classify_regressions(
            a_rows, b_rows, reviewed, scope, extra),
    }
    return {"report": report, "manifests": manifests}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--scope-policy", type=Path, default=SCOPE_POLICY)
    parser.add_argument("--nortaxa-archive", type=Path, default=DEFAULT_NORTAXA)
    parser.add_argument("--col-archive", type=Path, default=DEFAULT_COL)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = audit(args)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    manifest_files = {}
    for cls, manifest in result["manifests"].items():
        name = f"group-a-{cls.replace('_', '-')}.manifest.json"
        manifest_files[name] = {
            "evidence_class": cls,
            "member_count": manifest["member_count"],
            "members_sha256": manifest["members_sha256"],
            "file_sha256": _write_json(args.out_dir / name, manifest),
        }
    report = {**result["report"], "manifests": manifest_files}
    _write_json(args.out_dir / "coverage-report.json", report)
    summary = {k: report[k] for k in ("cloud_scope", "manifests")}
    summary["group_a"] = {k: v["by_evidence_class"]
                          for k, v in report["group_a"].items()
                          if isinstance(v, dict)}
    summary["group_b"] = {k: v["by_evidence_class"]
                          for k, v in report["group_b"].items()
                          if isinstance(v, dict)}
    print(json.dumps(summary, indent=1, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
