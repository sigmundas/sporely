#!/usr/bin/env python3
"""Promote a compiled taxonomy-v2 SQLite into the tracked desktop bundle.

The desktop installer (``utils/taxonomy_v2.py``) consumes exactly two tracked
files under ``database/reference_data/generated/taxonomy_v2/``: a
deterministic gzip of the release SQLite and the bundle ``manifest.json``
that pins it. This tool writes both from authoritative build outputs so a
bundle is reproducible rather than hand-edited, and refreshes the release
pins in ``database/taxonomy/desktop-compatibility.json``.

Every value is either read from the build or carried forward under a check:

* ``content_release_id``, ``taxonomy_schema_version``, ``state``,
  ``publication``, ``compiler_manifest_sha256`` — the artifact's own
  ``taxonomy_meta``, cross-checked against the compiler ``manifest.json``
  (whose SHA-256 must equal the recorded ``compiler_manifest_sha256``);
* ``registry_concatenated_sha256`` — the committed registry shard manifest,
  which must equal the artifact's recorded ``registry_sha256``;
* ``sqlite_*`` / ``gz_*`` — hashed from the bytes written;
* ``redlist_no`` counts — the normalizer report, the compiler's
  ``redlist_no_diagnostics.json`` and the compiled ``redlist_no.jsonl``;
* ``redlist_no`` curated provenance (citation, publisher, licence trail …)
  describes the source *workbook*, not the build. It is carried forward from
  the previous bundle manifest only when the workbook this build consumed is
  byte-identical (same SHA-256, re-hashed from ``--redlist-workbook``);
  otherwise the tool refuses, because that text would then be unverified;
* ``install_target_name`` — a runtime contract constant, carried forward.

Offline and deterministic: the gzip header carries a fixed name and
``mtime=0``, so the same SQLite always yields the same bundle bytes.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import shutil
import sqlite3
import sys
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_BUNDLE_DIR = REPO_ROOT / "database/reference_data/generated/taxonomy_v2"
DEFAULT_REGISTRY_MANIFEST = REPO_ROOT / "database/taxonomy/registry/canonical/manifest.json"
DEFAULT_COMPATIBILITY = REPO_ROOT / "database/taxonomy/desktop-compatibility.json"

MANIFEST_SCHEMA_VERSION = 1
RELEASE_ID_RE = re.compile(r"^tax-\d{4}\.\d{2}\.\d{2}-\d{2}$")
SUPPORTED_TAXONOMY_SCHEMA = 2

# The compiler's per-row reason tokens → the bundle manifest's vocabulary.
_REASON_NAMES = {
    "unresolved_name_id_not_found": "name_id_not_found_in_nortaxa",
    "unresolved_name_id_name_mismatch": "name_id_name_mismatch",
    "unresolved_name_id_ambiguous": "name_id_ambiguous",
}

# Workbook-level provenance: true of the source workbook, not derivable from
# a build, and only valid while the workbook bytes are unchanged.
_CURATED_REDLIST_FIELDS = (
    "assessment_edition", "publisher", "publication_date", "export_url",
    "citation", "citation_evidence", "assessed_name_registry_evidence",
    "resolution_rule", "license",
)


class PromotionError(RuntimeError):
    """The inputs do not license a bundle; nothing has been written."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _dump_json(data: dict) -> str:
    return json.dumps(data, indent=2, ensure_ascii=False) + "\n"


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise PromotionError(message)


def read_artifact_meta(sqlite_path: Path) -> dict[str, str]:
    """``taxonomy_meta`` of a compiled artifact, after an integrity check."""
    conn = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
    try:
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        _require(integrity == "ok", f"{sqlite_path.name}: integrity_check = {integrity!r}")
        return {k: v for k, v in conn.execute("SELECT key, value FROM taxonomy_meta")}
    finally:
        conn.close()


def _unresolved_reason_counts(redlist_jsonl: Path) -> dict[str, int]:
    counts: Counter[str] = Counter()
    with redlist_jsonl.open(encoding="utf-8") as handle:
        for line in handle:
            reason = json.loads(line).get("unresolved_reason")
            if reason is None:
                continue
            _require(reason in _REASON_NAMES, f"unknown red-list unresolved_reason {reason!r}")
            counts[_REASON_NAMES[reason]] += 1
    return {name: counts.get(name, 0) for name in _REASON_NAMES.values()}


def build_redlist_block(
    *,
    compiler_manifest: dict,
    release_dir: Path,
    redlist_report: dict,
    workbook: Path,
    previous_block: dict,
) -> dict:
    binding = (compiler_manifest.get("redlist_no") or {}).get("source_binding") or {}
    _require(bool(binding), "compiler manifest has no redlist_no.source_binding")
    input_sha = redlist_report["input_sha256"]
    _require(binding.get("input_sha256") == input_sha,
             "normalizer report and compiler disagree on the red-list workbook SHA-256")
    _require(_sha256_file(workbook) == input_sha,
             f"{workbook} is not the workbook this build consumed")
    _require(
        previous_block.get("workbook_sha256") == input_sha
        and previous_block.get("source_release") == redlist_report["source_release"],
        "the red-list workbook changed since the previous bundle; its curated "
        "provenance (citation, licence …) must be re-established, not carried forward",
    )
    diagnostics = _read_json(release_dir / "redlist_no_diagnostics.json")
    counts = diagnostics["counts"]
    _require(counts["total"] == redlist_report["row_count"], "red-list row counts disagree")
    reasons = _unresolved_reason_counts(release_dir / "redlist_no.jsonl")
    _require(sum(reasons.values()) == counts["unresolved"], "red-list unresolved counts disagree")

    derived = {
        "source_system": redlist_report["source_system"],
        "source_release": redlist_report["source_release"],
        "source_url": redlist_report["source_url"],
        "workbook_filename": redlist_report["input_filename"],
        "workbook_sha256": input_sha,
        "workbook_bytes": workbook.stat().st_size,
        "sheet_name": redlist_report["sheet_name"],
        "row_count": redlist_report["row_count"],
        "resolved_count": counts["resolved"],
        "unresolved_count": counts["unresolved"],
        "unresolved_reason_counts": reasons,
        "collision_summary": diagnostics["collisions"]["summary"],
    }
    curated = {key: previous_block[key] for key in _CURATED_REDLIST_FIELDS if key in previous_block}
    # Keep the previous block's key order (nested too) so the committed diff
    # shows changed values only.
    for key, value in derived.items():
        prior = previous_block.get(key)
        if isinstance(value, dict) and isinstance(prior, dict) and set(value) == set(prior):
            derived[key] = {k: value[k] for k in prior}
    order = list(previous_block) + [k for k in (*derived, *curated) if k not in previous_block]
    merged = {**curated, **derived}
    return {key: merged[key] for key in order if key in merged}


DERIVED_REDLIST_FIELDS = (
    "source_system", "source_release", "source_url", "workbook_filename",
    "workbook_sha256", "workbook_bytes", "sheet_name", "row_count",
    "resolved_count", "unresolved_count", "unresolved_reason_counts",
    "collision_summary",
)


def write_deterministic_gzip(sqlite_path: Path, gz_path: Path, *, member_name: str) -> None:
    tmp = gz_path.with_name(gz_path.name + ".tmp")
    with sqlite_path.open("rb") as source, tmp.open("wb") as raw:
        with gzip.GzipFile(filename=member_name, mode="wb", fileobj=raw,
                           compresslevel=9, mtime=0) as gz:
            shutil.copyfileobj(source, gz, 1 << 20)
    os.replace(tmp, gz_path)


def promote(
    *,
    sqlite_path: Path,
    release_dir: Path,
    redlist_report_path: Path,
    redlist_workbook: Path,
    bundle_dir: Path = DEFAULT_BUNDLE_DIR,
    registry_manifest_path: Path = DEFAULT_REGISTRY_MANIFEST,
    compatibility_path: Path | None = DEFAULT_COMPATIBILITY,
) -> dict:
    """Write the bundle gzip + manifest (and compatibility pins); return the manifest."""
    previous_path = bundle_dir / "manifest.json"
    previous = _read_json(previous_path)
    compiler_manifest_path = release_dir / "manifest.json"
    compiler_manifest = _read_json(compiler_manifest_path)
    meta = read_artifact_meta(sqlite_path)
    registry_manifest = _read_json(registry_manifest_path)

    release_id = meta.get("content_release_id", "")
    _require(bool(RELEASE_ID_RE.fullmatch(release_id)),
             f"artifact content_release_id {release_id!r} is not tax-YYYY.MM.DD-NN")
    _require(meta.get("taxonomy_schema_version") == str(SUPPORTED_TAXONOMY_SCHEMA),
             f"unsupported taxonomy_schema_version {meta.get('taxonomy_schema_version')!r}")
    _require(compiler_manifest.get("content_release_id") == release_id,
             "compiler manifest and artifact disagree on content_release_id")
    compiler_sha = _sha256_file(compiler_manifest_path)
    _require(meta.get("compiler_manifest_sha256") == compiler_sha,
             "artifact was not built from this compiler manifest")
    registry_sha = registry_manifest["concatenated_sha256"]
    _require(meta.get("registry_sha256") == registry_sha
             and compiler_manifest.get("registry_sha256") == registry_sha,
             "artifact was not compiled against the committed registry")
    _require(bool(previous.get("install_target_name")), "previous manifest has no install_target_name")
    # File names that reach the filesystem pass the installer's own path-safety
    # rule (bare *.sqlite3.gz beside the manifest), checked before any write.
    from utils.taxonomy_v2 import TaxonomyV2InstallError, _safe_manifest_artifact_name
    previous_gz = previous.get("gz_artifact")
    try:
        if previous_gz:
            _safe_manifest_artifact_name(previous_gz)
        _safe_manifest_artifact_name(f"{release_id}.sqlite3.gz")
    except TaxonomyV2InstallError as exc:
        raise PromotionError(str(exc)) from exc

    redlist_block = build_redlist_block(
        compiler_manifest=compiler_manifest,
        release_dir=release_dir,
        redlist_report=_read_json(redlist_report_path),
        workbook=redlist_workbook,
        previous_block=previous.get("redlist_no") or {},
    )

    sqlite_name = f"{release_id}.sqlite3"
    gz_path = bundle_dir / f"{sqlite_name}.gz"
    write_deterministic_gzip(sqlite_path, gz_path, member_name=sqlite_name)
    manifest = {
        "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
        "taxonomy_schema_version": SUPPORTED_TAXONOMY_SCHEMA,
        "content_release_id": release_id,
        "state": meta.get("state", ""),
        "publication": meta.get("publication", ""),
        "gz_artifact": gz_path.name,
        "gz_sha256": _sha256_file(gz_path),
        "gz_bytes": gz_path.stat().st_size,
        "sqlite_sha256": _sha256_file(sqlite_path),
        "sqlite_bytes": sqlite_path.stat().st_size,
        "registry_concatenated_sha256": registry_sha,
        "compiler_manifest_sha256": compiler_sha,
        "install_target_name": previous["install_target_name"],
        "redlist_no": redlist_block,
    }
    previous_path.write_text(_dump_json(manifest), encoding="utf-8")
    if previous_gz and previous_gz != gz_path.name:
        (bundle_dir / previous_gz).unlink(missing_ok=True)

    if compatibility_path is not None and compatibility_path.exists():
        compat = _read_json(compatibility_path)
        compat.update({
            "tested_taxonomy_release": release_id,
            "bundled_sqlite_sha256": manifest["sqlite_sha256"],
            "bundled_gz_sha256": manifest["gz_sha256"],
            "registry_concatenated_sha256": registry_sha,
            "state": manifest["state"],
            "publication": manifest["publication"],
        })
        # Only build-derived facts are re-pinned; the compatibility file's own
        # curated prose is left exactly as written.
        compat_redlist = compat.get("redlist_no")
        if isinstance(compat_redlist, dict):
            for key in list(compat_redlist):
                if key in DERIVED_REDLIST_FIELDS and key in redlist_block:
                    compat_redlist[key] = redlist_block[key]
        compatibility_path.write_text(_dump_json(compat), encoding="utf-8")
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--sqlite", type=Path, required=True,
                        help="compiled release SQLite (build_sqlite_candidate.py output)")
    parser.add_argument("--release-dir", type=Path, required=True,
                        help="compile_release.py output directory the SQLite was built from")
    parser.add_argument("--redlist-report", type=Path, required=True,
                        help="normalize_redlist_no.py report.json")
    parser.add_argument("--redlist-workbook", type=Path, required=True,
                        help="the red-list workbook the build consumed (re-hashed)")
    parser.add_argument("--bundle-dir", type=Path, default=DEFAULT_BUNDLE_DIR)
    parser.add_argument("--registry-manifest", type=Path, default=DEFAULT_REGISTRY_MANIFEST)
    parser.add_argument("--compatibility", type=Path, default=DEFAULT_COMPATIBILITY)
    args = parser.parse_args(argv)
    try:
        manifest = promote(
            sqlite_path=args.sqlite, release_dir=args.release_dir,
            redlist_report_path=args.redlist_report, redlist_workbook=args.redlist_workbook,
            bundle_dir=args.bundle_dir, registry_manifest_path=args.registry_manifest,
            compatibility_path=args.compatibility,
        )
    except PromotionError as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 1
    print(_dump_json(manifest), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
