#!/usr/bin/env python3
"""Freeze validated taxonomy artifacts, then publish exactly the frozen bytes.

Freeze checks compiler output fingerprints, SQLite metadata, registry and
red-list provenance. It prepares a deterministic SQLite gzip, an inventoried
compiler/evidence archive, bundle manifest and compatibility file. Promotion
accepts only the expected frozen descriptor hash and copies these files without
compilation, compression or evidence generation. Earlier evidence remains
available. The runtime SQLite schema and identity contract are unchanged.
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
import tarfile
import tempfile
import io
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:  # runnable as a plain script
    sys.path.insert(0, str(REPO_ROOT))
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


def freeze(
    *,
    sqlite_path: Path,
    release_dir: Path,
    redlist_report_path: Path,
    redlist_workbook: Path,
    bundle_dir: Path = DEFAULT_BUNDLE_DIR,
    registry_manifest_path: Path = DEFAULT_REGISTRY_MANIFEST,
    compatibility_path: Path | None = DEFAULT_COMPATIBILITY,
    output_dir: Path,
    validation_receipt_path: Path,
    evidence_inputs: dict[str, Path] | None = None,
) -> dict:
    """Prepare the immutable publication set; never write the installed bundle."""
    _require(not output_dir.exists(), f"freeze output already exists: {output_dir}")
    validate_compiler(release_dir)
    receipt = _read_json(validation_receipt_path)
    _require(receipt.get("validated") is True, "validation receipt does not approve this set")
    _require(receipt.get("sqlite_sha256") == _sha256_file(sqlite_path)
             and receipt.get("compiler_manifest_sha256") == _sha256_file(release_dir / "manifest.json"),
             "validation receipt does not bind this SQLite/compiler set")
    expected = receipt.get("expected_vernacular_enrichment")
    if expected:
        with (release_dir / "vernacular_evidence.jsonl").open() as handle:
            additions = [r for r in map(json.loads, handle) if r.get("projection_status") == "added"]
        counts = {"automatic": sum(r["evidence_class"] == "automatic_vernacular_enrichment" for r in additions),
                  "reviewed": sum(r["evidence_class"] == "owner_reviewed_vernacular_association" for r in additions),
                  "total": len(additions), "affected_concepts": len({r["target_sporely_taxon_id"] for r in additions})}
        _require(counts == expected, f"pinned vernacular projection counts disagree: {counts}")
        _require(all(len(r["full_bundle_target_ids"]) == 1 for r in additions
                     if r["evidence_class"] == "automatic_vernacular_enrichment"), "automatic projection is not unique")
    previous_path = bundle_dir / "manifest.json"
    previous = _read_json(previous_path)
    compiler_manifest_path = release_dir / "manifest.json"
    compiler_manifest = _read_json(compiler_manifest_path)
    meta = read_artifact_meta(sqlite_path)
    with sqlite3.connect(f"file:{sqlite_path.resolve()}?mode=ro", uri=True) as connection:
        _require(connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok", "SQLite integrity check failed")
        _require(not connection.execute("PRAGMA foreign_key_check").fetchall(), "SQLite foreign-key check failed")
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
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".freeze-", dir=output_dir.parent))
    try:
        gz_path = staging / f"{sqlite_name}.gz"
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
            "freeze_artifact": f"{release_id}.freeze.json",
            "install_target_name": previous["install_target_name"],
            "redlist_no": redlist_block,
        }
        archive_path = staging / f"{release_id}.evidence.tar.gz"
        members = {f"compiler/{p.name}": p for p in release_dir.iterdir() if p.is_file()}
        members["validation/receipt.json"] = validation_receipt_path
        members["inputs/redlist-report.json"] = redlist_report_path
        members["inputs/registry-manifest.json"] = registry_manifest_path
        members["inputs/previous-publication-manifest.json"] = previous_path
        if compatibility_path and compatibility_path.exists():
            members["inputs/previous-compatibility.json"] = compatibility_path
        for name, path in (evidence_inputs or {}).items():
            _require(bool(re.fullmatch(r"[A-Za-z0-9_.-]+", name)), "unsafe evidence input name")
            _require(f"inputs/{name}" not in members, "duplicate evidence input member")
            _require(path.is_file() and not path.is_symlink(), f"missing evidence input: {name}")
            members[f"inputs/{name}"] = path
        inventory = {name: {"sha256": _sha256_file(path), "bytes": path.stat().st_size}
                     for name, path in sorted(members.items())}
        archive_inventory = {"format": "sporely-compiler-evidence-v1", "content_release_id": release_id,
                             "members": inventory, "sqlite_sha256": manifest["sqlite_sha256"],
                             "compiler_manifest_sha256": compiler_sha}
        with archive_path.open("wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", filename="", mtime=0) as gz:
            with tarfile.open(fileobj=gz, mode="w|") as tar:
                for name, path in sorted(members.items()):
                    info = tarfile.TarInfo(name); info.size = path.stat().st_size; info.mode = 0o644
                    with path.open("rb") as source: tar.addfile(info, source)
                payload = _dump_json(archive_inventory).encode()
                info = tarfile.TarInfo("inventory.json"); info.size = len(payload); info.mode = 0o644
                tar.addfile(info, io.BytesIO(payload))
        manifest["compiler_evidence"] = {"artifact": archive_path.name, "sha256": _sha256_file(archive_path),
                                        "bytes": archive_path.stat().st_size, "format": archive_inventory["format"]}
        (staging / "manifest.json").write_text(_dump_json(manifest), encoding="utf-8")

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
            (staging / "compatibility.json").write_text(_dump_json(compat), encoding="utf-8")
        frozen = {"format": "sporely-frozen-publication-v1", "content_release_id": release_id,
                  "files": {p.name: {"sha256": _sha256_file(p), "bytes": p.stat().st_size}
                            for p in sorted(staging.iterdir())},
                  "baseline_manifest_sha256": _sha256_file(previous_path),
                  "baseline_compatibility_sha256": _sha256_file(compatibility_path) if compatibility_path and compatibility_path.exists() else None}
        (staging / "freeze.json").write_text(_dump_json(frozen), encoding="utf-8")
        verify_frozen(staging)
        os.replace(staging, output_dir)
        return frozen
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise



def validate_compiler(release_dir: Path) -> None:
    manifest = _read_json(release_dir / "manifest.json")
    outputs = manifest.get("outputs") or {}
    _require(bool(outputs), "compiler manifest has no output inventory")
    if manifest.get("vernacular_enrichment"):
        _require({"vernacular_evidence", "vernacular_changes", "vernacular_reviews"} <= outputs.keys(),
                 "compiler vernacular evidence set is incomplete")
    _require(not (release_dir / "manifest.json").is_symlink(), "compiler manifest is a symlink")
    expected = {"manifest.json"}
    for item in outputs.values():
        name = item["name"]
        _require(Path(name).name == name and name not in {"", ".", ".."}, "unsafe compiler artifact name")
        path = release_dir / name
        _require(path.is_file() and not path.is_symlink(), f"missing compiler artifact: {name}")
        _require(_sha256_file(path) == item["sha256"] and path.stat().st_size == item["bytes"],
                 f"compiler artifact fingerprint mismatch: {name}")
        expected.add(name)
    _require({p.name for p in release_dir.iterdir()} == expected, "compiler artifact inventory is incomplete")


def verify_frozen(frozen_dir: Path) -> dict:
    _require((frozen_dir / "freeze.json").is_file() and not (frozen_dir / "freeze.json").is_symlink(),
             "frozen descriptor missing or symlinked")
    frozen = _read_json(frozen_dir / "freeze.json")
    _require(frozen.get("format") == "sporely-frozen-publication-v1", "unsupported frozen publication")
    files = frozen.get("files") or {}
    _require({p.name for p in frozen_dir.iterdir()} == set(files) | {"freeze.json"}, "frozen inventory mismatch")
    for name, item in files.items():
        _require(Path(name).name == name and name not in {"", ".", ".."}, "unsafe frozen artifact name")
        path = frozen_dir / name
        _require(path.is_file() and not path.is_symlink() and path.stat().st_size == item["bytes"]
                 and _sha256_file(path) == item["sha256"], f"frozen artifact fingerprint mismatch: {name}")
    manifest = _read_json(frozen_dir / "manifest.json")
    _require(manifest["content_release_id"] == frozen["content_release_id"], "frozen release mismatch")
    for name, digest in [(manifest["gz_artifact"], manifest["gz_sha256"]),
                         (manifest["compiler_evidence"]["artifact"], manifest["compiler_evidence"]["sha256"])]:
        _require(name in files and files[name]["sha256"] == digest, "publication fingerprint mismatch")
    archive_path = frozen_dir / manifest["compiler_evidence"]["artifact"]
    with tarfile.open(archive_path, "r:gz") as archive:
        entries = archive.getmembers()
        _require(all(x.isfile() and not x.name.startswith("/") and ".." not in Path(x.name).parts for x in entries),
                 "unsafe evidence archive member")
        _require(len({x.name for x in entries}) == len(entries), "duplicate evidence archive member")
        inventory = json.load(archive.extractfile("inventory.json"))
        _require({x.name for x in entries} == set(inventory["members"]) | {"inventory.json"}, "evidence archive inventory mismatch")
        _require(inventory["compiler_manifest_sha256"] == manifest["compiler_manifest_sha256"]
                 and inventory["sqlite_sha256"] == manifest["sqlite_sha256"], "evidence archive release binding mismatch")
        for name, expected in inventory["members"].items():
            source = archive.extractfile(name); digest = hashlib.sha256(); size = 0
            for chunk in iter(lambda: source.read(1 << 20), b""):
                size += len(chunk); digest.update(chunk)
            _require(size == expected["bytes"] and digest.hexdigest() == expected["sha256"], f"evidence member fingerprint mismatch: {name}")
        archived_compiler = json.load(archive.extractfile("compiler/manifest.json"))
        _require(inventory["members"]["compiler/manifest.json"]["sha256"] == manifest["compiler_manifest_sha256"],
                 "archived compiler manifest mismatch")
        for item in archived_compiler["outputs"].values():
            _require(inventory["members"].get("compiler/" + item["name"]) == {"sha256": item["sha256"], "bytes": item["bytes"]},
                     "archived compiler output mismatch")
        receipt = json.load(archive.extractfile("validation/receipt.json"))
        _require(receipt.get("validated") is True and receipt.get("sqlite_sha256") == manifest["sqlite_sha256"]
                 and receipt.get("compiler_manifest_sha256") == manifest["compiler_manifest_sha256"], "archived validation receipt mismatch")
    digest = hashlib.sha256(); size = 0
    with gzip.open(frozen_dir / manifest["gz_artifact"], "rb") as source:
        for chunk in iter(lambda: source.read(1 << 20), b""):
            size += len(chunk); digest.update(chunk)
    _require(digest.hexdigest() == manifest["sqlite_sha256"] and size == manifest["sqlite_bytes"], "frozen SQLite gzip mismatch")
    return frozen


def promote(*, frozen_dir: Path, bundle_dir: Path = DEFAULT_BUNDLE_DIR,
            registry_manifest_path: Path = DEFAULT_REGISTRY_MANIFEST,
            compatibility_path: Path | None = DEFAULT_COMPATIBILITY,
            expected_freeze_sha256: str) -> dict:
    """Publish only frozen bytes; never compile, generate evidence or compress."""
    _require(_sha256_file(frozen_dir / "freeze.json") == expected_freeze_sha256, "freeze fingerprint mismatch")
    frozen = verify_frozen(frozen_dir)
    previous_path = bundle_dir / "manifest.json"
    previous = _read_json(previous_path)
    manifest = _read_json(frozen_dir / "manifest.json")
    _require(manifest["registry_concatenated_sha256"] == _read_json(registry_manifest_path)["concatenated_sha256"],
             "frozen set does not match committed registry")
    same = _sha256_file(previous_path) == frozen["files"]["manifest.json"]["sha256"]
    _require(same or _sha256_file(previous_path) == frozen["baseline_manifest_sha256"], "publication baseline changed; freeze again")
    if "compatibility.json" in frozen["files"]:
        _require(compatibility_path is not None and compatibility_path.is_file(), "compatibility target missing")
        _require(_sha256_file(compatibility_path) in {frozen["baseline_compatibility_sha256"],
                  frozen["files"]["compatibility.json"]["sha256"]}, "compatibility baseline changed; freeze again")
    old_gz = previous.get("gz_artifact")
    if old_gz:
        from utils.taxonomy_v2 import _safe_manifest_artifact_name, TaxonomyV2InstallError
        try: _safe_manifest_artifact_name(old_gz)
        except TaxonomyV2InstallError as exc: raise PromotionError(str(exc)) from exc
    freeze_name = manifest.get("freeze_artifact")
    _require(freeze_name == f"{manifest['content_release_id']}.freeze.json", "unsafe freeze artifact name")
    existing_freeze = bundle_dir / freeze_name
    _require(not existing_freeze.exists() or _sha256_file(existing_freeze) == expected_freeze_sha256,
             "immutable freeze descriptor already differs")
    # Existing evidence archives remain immutable and available across releases.
    for name in (manifest["gz_artifact"], manifest["compiler_evidence"]["artifact"]):
        target = bundle_dir / name
        _require(not target.exists() or _sha256_file(target) == frozen["files"][name]["sha256"],
                 f"immutable publication artifact already differs: {name}")
    temporary = existing_freeze.with_name(existing_freeze.name + ".tmp")
    shutil.copyfile(frozen_dir / "freeze.json", temporary); os.replace(temporary, existing_freeze)
    for name in (manifest["gz_artifact"], manifest["compiler_evidence"]["artifact"], "manifest.json"):
        target = bundle_dir / name
        temporary = target.with_name(target.name + ".tmp")
        shutil.copyfile(frozen_dir / name, temporary); os.replace(temporary, target)
    if "compatibility.json" in frozen["files"]:
        temporary = compatibility_path.with_name(compatibility_path.name + ".tmp")
        shutil.copyfile(frozen_dir / "compatibility.json", temporary); os.replace(temporary, compatibility_path)
    old_gz = previous.get("gz_artifact")
    if old_gz and old_gz != manifest["gz_artifact"]:
        from utils.taxonomy_v2 import _safe_manifest_artifact_name
        _safe_manifest_artifact_name(old_gz)
        (bundle_dir / old_gz).unlink(missing_ok=True)
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publish a validated frozen taxonomy set")
    parser.add_argument("--frozen-dir", type=Path, required=True)
    parser.add_argument("--expect-freeze-sha256", required=True)
    parser.add_argument("--bundle-dir", type=Path, default=DEFAULT_BUNDLE_DIR)
    parser.add_argument("--registry-manifest", type=Path, default=DEFAULT_REGISTRY_MANIFEST)
    parser.add_argument("--compatibility", type=Path, default=DEFAULT_COMPATIBILITY)
    args = parser.parse_args(argv)
    try:
        manifest = promote(frozen_dir=args.frozen_dir, expected_freeze_sha256=args.expect_freeze_sha256,
            bundle_dir=args.bundle_dir, registry_manifest_path=args.registry_manifest, compatibility_path=args.compatibility)
    except (PromotionError, OSError, ValueError, KeyError) as exc:
        print(f"refused: {exc}", file=sys.stderr); return 1
    print(_dump_json(manifest), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
