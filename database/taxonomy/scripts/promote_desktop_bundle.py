#!/usr/bin/env python3
"""Freeze validated taxonomy artifacts, then publish exactly the frozen bytes.

Freeze checks compiler output fingerprints, SQLite metadata, registry and
red-list provenance. It prepares a deterministic SQLite gzip, an inventoried
compiler/evidence archive, bundle manifest and compatibility file. Promotion
accepts only the expected frozen descriptor hash and copies these files without
compilation, compression or evidence generation. Earlier evidence remains
available. The runtime SQLite schema and identity contract are unchanged.

A derived release (``name_associations.py`` applied to a frozen base) has no
compiler output of its own. :func:`freeze_derived` freezes it instead: it
requires the published bundle to be the frozen base, rebuilds the release from
the base gzip, the pinned normalized inputs, policy and holds and requires
every output byte-identical, checks that no Sporely id was added, removed or
re-identified, and archives the derivation evidence with the base freeze and
the overlay implementation pinned. Its manifest names that archive as
``derivation_evidence``; promotion is the same for both kinds.
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


COMPILER_EVIDENCE_FORMAT = "sporely-compiler-evidence-v1"
DERIVED_EVIDENCE_FORMAT = "sporely-derived-evidence-v1"
DERIVATION_REPORT_FORMAT = "sporely-name-association-build-v1"
#: The overlay implementation a derived freeze reconstructs with; each file's
#: bytes are pinned in the manifest and archived.
DERIVATION_IMPLEMENTATION = ("name_associations.py", "cross_source_mapping.py", "vernacular_projection.py")
_SCRIPTS_DIR = Path(__file__).resolve().parent


def _evidence_pointer(manifest: dict) -> dict:
    """The one evidence archive a bundle manifest names, of either kind."""
    pointers = [manifest[key] for key in ("compiler_evidence", "derivation_evidence") if key in manifest]
    _require(len(pointers) == 1, "manifest must name exactly one evidence archive")
    return pointers[0]


def _write_evidence_archive(archive_path: Path, members: dict[str, Path], header: dict) -> dict:
    """Deterministic tar.gz of ``members`` plus an ``inventory.json`` fingerprinting each."""
    inventory = {"members": {name: {"sha256": _sha256_file(path), "bytes": path.stat().st_size}
                             for name, path in sorted(members.items())}}
    archive_inventory = {"format": header["format"], "content_release_id": header["content_release_id"],
                         **inventory, **{k: v for k, v in header.items() if k not in ("format", "content_release_id")}}
    with archive_path.open("wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", filename="", mtime=0) as gz:
        with tarfile.open(fileobj=gz, mode="w|") as tar:
            for name, path in sorted(members.items()):
                info = tarfile.TarInfo(name); info.size = path.stat().st_size; info.mode = 0o644
                with path.open("rb") as source: tar.addfile(info, source)
            payload = _dump_json(archive_inventory).encode()
            info = tarfile.TarInfo("inventory.json"); info.size = len(payload); info.mode = 0o644
            tar.addfile(info, io.BytesIO(payload))
    return archive_inventory


def _write_frozen_compatibility(staging: Path, compatibility_path: Path | None, manifest: dict,
                                redlist_block: dict) -> None:
    if compatibility_path is None or not compatibility_path.exists():
        return
    compat = _read_json(compatibility_path)
    compat.update({
        "tested_taxonomy_release": manifest["content_release_id"],
        "bundled_sqlite_sha256": manifest["sqlite_sha256"],
        "bundled_gz_sha256": manifest["gz_sha256"],
        "registry_concatenated_sha256": manifest["registry_concatenated_sha256"],
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


def _write_freeze_descriptor(staging: Path, release_id: str, previous_path: Path,
                             compatibility_path: Path | None, **extra) -> dict:
    frozen = {"format": "sporely-frozen-publication-v1", "content_release_id": release_id,
              "files": {p.name: {"sha256": _sha256_file(p), "bytes": p.stat().st_size}
                        for p in sorted(staging.iterdir())},
              "baseline_manifest_sha256": _sha256_file(previous_path),
              "baseline_compatibility_sha256": _sha256_file(compatibility_path) if compatibility_path and compatibility_path.exists() else None,
              **extra}
    (staging / "freeze.json").write_text(_dump_json(frozen), encoding="utf-8")
    return frozen


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
        archive_inventory = _write_evidence_archive(archive_path, members, {
            "format": COMPILER_EVIDENCE_FORMAT, "content_release_id": release_id,
            "sqlite_sha256": manifest["sqlite_sha256"], "compiler_manifest_sha256": compiler_sha})
        manifest["compiler_evidence"] = {"artifact": archive_path.name, "sha256": _sha256_file(archive_path),
                                        "bytes": archive_path.stat().st_size, "format": archive_inventory["format"]}
        (staging / "manifest.json").write_text(_dump_json(manifest), encoding="utf-8")
        _write_frozen_compatibility(staging, compatibility_path, manifest, redlist_block)
        frozen = _write_freeze_descriptor(staging, release_id, previous_path, compatibility_path)
        verify_frozen(staging)
        os.replace(staging, output_dir)
        return frozen
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise



def _verify_derived_artifact(sqlite_path: Path, base_sqlite: Path, *, report: dict, receipt: dict,
                             previous: dict, registry_sha: str, associations_path: Path) -> None:
    """The derived SQLite keeps every base id and identity and holds exactly the reported associations."""
    release_id = report["content_release_id"]
    meta = read_artifact_meta(sqlite_path)
    _require(meta.get("content_release_id") == release_id, "artifact and derivation report disagree on content_release_id")
    _require(meta.get("taxonomy_schema_version") == str(SUPPORTED_TAXONOMY_SCHEMA),
             f"unsupported taxonomy_schema_version {meta.get('taxonomy_schema_version')!r}")
    total = report["associations_total"]
    _require(meta.get("name_association.base_release_id") == report["base_release_id"]
             and meta.get("name_association.base_sqlite_sha256") == report["base_sqlite_sha256"]
             and meta.get("name_association.policy_sha256") == report["policy_sha256"]
             and meta.get("name_association.count") == str(total),
             "artifact metadata does not describe this derivation")
    _require(meta.get("registry_sha256") == registry_sha == previous.get("registry_concatenated_sha256"),
             "artifact was not compiled against the committed registry")
    _require(meta.get("compiler_manifest_sha256") == previous.get("compiler_manifest_sha256"),
             "artifact does not carry its base release's compiler manifest")
    _require(receipt.get("expected_associations_total") == total, f"pinned association count disagrees: {total}")
    with associations_path.open(encoding="utf-8") as handle:
        listed = [json.loads(line)["association_id"] for line in handle]
    conn = sqlite3.connect(f"file:{sqlite_path.resolve()}?mode=ro", uri=True)
    try:
        _require(not conn.execute("PRAGMA foreign_key_check").fetchall(), "SQLite foreign-key check failed")
        conn.execute("ATTACH DATABASE ? AS base", (f"file:{base_sqlite.resolve()}?mode=ro",))
        # No id allocated, retired or renumbered, and every id keeps its identity.
        for added, removed in (("main", "base"), ("base", "main")):
            _require(not conn.execute(
                f"SELECT 1 FROM {added}.taxon_min a WHERE NOT EXISTS "
                f"(SELECT 1 FROM {removed}.taxon_min r WHERE r.taxon_id = a.taxon_id) LIMIT 1").fetchone(),
                "derived release does not hold exactly the base release's Sporely ids")
        _require(not conn.execute(
            "SELECT 1 FROM taxon_min d JOIN base.taxon_min b USING (taxon_id) "
            "WHERE d.canonical_source_system IS NOT b.canonical_source_system "
            "OR d.canonical_external_id IS NOT b.canonical_external_id "
            "OR d.canonical_scientific_name IS NOT b.canonical_scientific_name "
            "OR d.taxon_rank IS NOT b.taxon_rank LIMIT 1").fetchone(),
            "derived release changed the canonical identity of a base id")
        stored = [r[0] for r in conn.execute("SELECT association_id FROM taxon_association_min")]
        _require(len(stored) == len(set(stored)) == total and set(stored) == set(listed) and len(listed) == total,
                 "association table, list and report disagree")
        _require(not conn.execute(
            "SELECT 1 FROM taxon_association_min a WHERE NOT EXISTS "
            "(SELECT 1 FROM taxon_min t WHERE t.taxon_id = a.primary_taxon_id) OR (a.member_taxon_id IS NOT NULL "
            "AND NOT EXISTS (SELECT 1 FROM taxon_min t WHERE t.taxon_id = a.member_taxon_id)) LIMIT 1").fetchone(),
            "an association names a concept the release does not hold")
        rows, resolved = conn.execute("SELECT count(*), count(taxon_id) FROM taxon_redlist_min").fetchone()
    finally:
        conn.close()
    # The base's red-list block is published unchanged, so it must still be true.
    redlist = previous.get("redlist_no") or {}
    _require(rows == redlist.get("row_count") and resolved == redlist.get("resolved_count")
             and rows - resolved == redlist.get("unresolved_count"),
             "the derivation changed red-list resolution; the base's red-list block no longer describes it")


def freeze_derived(
    *,
    derivation_dir: Path,
    norm_dir: Path,
    policy_path: Path,
    holds_path: Path | None,
    bundle_dir: Path = DEFAULT_BUNDLE_DIR,
    registry_manifest_path: Path = DEFAULT_REGISTRY_MANIFEST,
    compatibility_path: Path | None = DEFAULT_COMPATIBILITY,
    output_dir: Path,
    validation_receipt_path: Path,
) -> dict:
    """Freeze a release derived from the published, frozen base; never write the installed bundle."""
    _require(not output_dir.exists(), f"freeze output already exists: {output_dir}")
    report_path = derivation_dir / "association-report.json"
    _require(report_path.is_file() and not report_path.is_symlink(), "derivation report missing")
    report = _read_json(report_path)
    _require(report.get("format") == DERIVATION_REPORT_FORMAT, "unsupported derivation report")
    release_id = report.get("content_release_id", "")
    base_id = report.get("base_release_id", "")
    _require(bool(RELEASE_ID_RE.fullmatch(release_id)) and bool(RELEASE_ID_RE.fullmatch(base_id)),
             "derivation release ids are not tax-YYYY.MM.DD-NN")
    sqlite_name = f"{release_id}.sqlite3"
    outputs = {sqlite_name, f"{sqlite_name}.gz", "association-report.json", "associations.jsonl", "manifest.json"}
    _require({p.name for p in derivation_dir.iterdir()} == outputs, "derivation output inventory mismatch")
    _require(not any((derivation_dir / name).is_symlink() for name in outputs), "derivation output is a symlink")
    sqlite_path = derivation_dir / sqlite_name
    sqlite_sha = _sha256_file(sqlite_path)
    _require(report.get("sqlite_sha256") == sqlite_sha
             and report.get("gz_sha256") == _sha256_file(derivation_dir / f"{sqlite_name}.gz"),
             "derivation report does not bind its artifacts")
    report_sha = _sha256_file(report_path)
    receipt = _read_json(validation_receipt_path)
    _require(receipt.get("validated") is True, "validation receipt does not approve this set")
    _require(receipt.get("sqlite_sha256") == sqlite_sha and receipt.get("association_report_sha256") == report_sha,
             "validation receipt does not bind this SQLite/derivation set")

    # The base: the published bundle is exactly the frozen base publication.
    base_freeze_path = bundle_dir / f"{base_id}.freeze.json"
    _require(base_freeze_path.is_file() and not base_freeze_path.is_symlink(),
             f"base freeze descriptor missing: {base_freeze_path.name}")
    base_freeze = _read_json(base_freeze_path)
    _require(base_freeze.get("format") == "sporely-frozen-publication-v1"
             and base_freeze.get("content_release_id") == base_id, "base freeze descriptor does not describe the base")
    previous_path = bundle_dir / "manifest.json"
    previous = _read_json(previous_path)
    _require(_sha256_file(previous_path) == base_freeze["files"]["manifest.json"]["sha256"],
             "published bundle manifest is not the frozen base's")
    _require(previous.get("content_release_id") == base_id
             and previous.get("sqlite_sha256") == report.get("base_sqlite_sha256"),
             "base manifest does not describe the derivation's base SQLite")
    base_gz_name = previous["gz_artifact"]
    base_gz = bundle_dir / base_gz_name
    _require(base_gz.is_file() and not base_gz.is_symlink() and base_gz_name in base_freeze["files"]
             and _sha256_file(base_gz) == base_freeze["files"][base_gz_name]["sha256"],
             f"base artifact missing or altered: {base_gz_name}")
    from utils.taxonomy_v2 import TaxonomyV2InstallError, _safe_manifest_artifact_name
    try:
        _safe_manifest_artifact_name(base_gz_name)
        _safe_manifest_artifact_name(f"{sqlite_name}.gz")
    except TaxonomyV2InstallError as exc:
        raise PromotionError(str(exc)) from exc

    # Pinned inputs: policy and every normalized source file the derivation read.
    _require(_sha256_file(policy_path) == report.get("policy_sha256"), "policy is not the one the derivation used")
    for name, digest in (report.get("inputs_sha256") or {}).items():
        relative = Path(name)
        _require(relative.parts[0] == "norm" and ".." not in relative.parts, f"unsafe derivation input {name!r}")
        path = norm_dir.joinpath(*relative.parts[1:])
        _require(path.is_file() and _sha256_file(path) == digest, f"derivation input missing or changed: {name}")
    registry_sha = _read_json(registry_manifest_path)["concatenated_sha256"]
    implementation = {name: _SCRIPTS_DIR / name for name in DERIVATION_IMPLEMENTATION}

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".derive-", dir=output_dir.parent) as work:
        work = Path(work)
        # Reconstruction from the frozen base with this implementation must
        # give the very bytes that were validated.
        from database.taxonomy.scripts import name_associations
        try:
            name_associations.build(
                base_gz=base_gz, base_sqlite_sha256=report["base_sqlite_sha256"], norm_dir=norm_dir,
                policy_path=policy_path, holds_path=holds_path, release_id=release_id,
                out_dir=work / "rebuild", base_manifest=previous_path)
        except name_associations.AssociationError as exc:
            raise PromotionError(f"reconstruction refused: {exc}") from exc
        for name in sorted(outputs):
            _require(_sha256_file(work / "rebuild" / name) == _sha256_file(derivation_dir / name),
                     f"reconstruction is not byte-identical: {name}")
        base_sqlite = work / "base.sqlite3"
        with gzip.open(base_gz, "rb") as source, base_sqlite.open("wb") as target:
            shutil.copyfileobj(source, target, 1 << 20)
        _require(_sha256_file(base_sqlite) == report["base_sqlite_sha256"], "base gzip does not hold the base SQLite")
        _verify_derived_artifact(sqlite_path, base_sqlite, report=report, receipt=receipt, previous=previous,
                                 registry_sha=registry_sha, associations_path=derivation_dir / "associations.jsonl")

    derivation_manifest = _read_json(derivation_dir / "manifest.json")
    _require(derivation_manifest.get("content_release_id") == release_id
             and derivation_manifest.get("sqlite_sha256") == sqlite_sha
             and (derivation_manifest.get("name_association") or {}).get("associations_total") == report["associations_total"],
             "derivation manifest does not describe this artifact")
    staging = Path(tempfile.mkdtemp(prefix=".freeze-", dir=output_dir.parent))
    try:
        gz_path = staging / f"{sqlite_name}.gz"
        write_deterministic_gzip(sqlite_path, gz_path, member_name=sqlite_name)
        _require(_sha256_file(gz_path) == report["gz_sha256"], "frozen gzip differs from the validated one")
        base_freeze_sha = _sha256_file(base_freeze_path)
        manifest = dict(derivation_manifest)
        manifest["freeze_artifact"] = f"{release_id}.freeze.json"
        manifest["name_association"] = {
            **derivation_manifest["name_association"],
            "base_freeze_sha256": base_freeze_sha,
            "implementation_sha256": {name: _sha256_file(path) for name, path in implementation.items()},
        }
        members = {
            "derivation/association-report.json": report_path,
            "derivation/associations.jsonl": derivation_dir / "associations.jsonl",
            "derivation/manifest.json": derivation_dir / "manifest.json",
            "inputs/base-freeze.json": base_freeze_path,
            "inputs/name-association-policy.json": policy_path,
            "inputs/registry-manifest.json": registry_manifest_path,
            "inputs/previous-publication-manifest.json": previous_path,
            "validation/receipt.json": validation_receipt_path,
            **{f"implementation/{name}": path for name, path in implementation.items()},
        }
        if holds_path is not None:
            members["inputs/source-continuity-holds.json"] = holds_path
        if compatibility_path and compatibility_path.exists():
            members["inputs/previous-compatibility.json"] = compatibility_path
        for name, path in members.items():
            _require(path.is_file() and not path.is_symlink(), f"missing evidence input: {name}")
        archive_path = staging / f"{release_id}.evidence.tar.gz"
        _write_evidence_archive(archive_path, members, {
            "format": DERIVED_EVIDENCE_FORMAT, "content_release_id": release_id, "sqlite_sha256": sqlite_sha,
            "association_report_sha256": report_sha, "base_release_id": base_id,
            "base_freeze_sha256": base_freeze_sha, "inputs_sha256": report.get("inputs_sha256") or {}})
        manifest["derivation_evidence"] = {"artifact": archive_path.name, "sha256": _sha256_file(archive_path),
                                           "bytes": archive_path.stat().st_size, "format": DERIVED_EVIDENCE_FORMAT}
        (staging / "manifest.json").write_text(_dump_json(manifest), encoding="utf-8")
        _write_frozen_compatibility(staging, compatibility_path, manifest, manifest.get("redlist_no") or {})
        frozen = _write_freeze_descriptor(staging, release_id, previous_path, compatibility_path,
                                          derived_from={"content_release_id": base_id,
                                                        "freeze_sha256": base_freeze_sha})
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
    evidence = _evidence_pointer(manifest)
    for name, digest in [(manifest["gz_artifact"], manifest["gz_sha256"]),
                         (evidence["artifact"], evidence["sha256"])]:
        _require(name in files and files[name]["sha256"] == digest, "publication fingerprint mismatch")
    archive_path = frozen_dir / evidence["artifact"]
    with tarfile.open(archive_path, "r:gz") as archive:
        entries = archive.getmembers()
        _require(all(x.isfile() and not x.name.startswith("/") and ".." not in Path(x.name).parts for x in entries),
                 "unsafe evidence archive member")
        _require(len({x.name for x in entries}) == len(entries), "duplicate evidence archive member")
        inventory = json.load(archive.extractfile("inventory.json"))
        _require({x.name for x in entries} == set(inventory["members"]) | {"inventory.json"}, "evidence archive inventory mismatch")
        _require(inventory["sqlite_sha256"] == manifest["sqlite_sha256"], "evidence archive release binding mismatch")
        if "derivation_evidence" in manifest:
            _require(inventory.get("format") == evidence.get("format") == DERIVED_EVIDENCE_FORMAT,
                     "derivation evidence archive has the wrong format")
        else:
            _require(inventory["compiler_manifest_sha256"] == manifest["compiler_manifest_sha256"],
                     "evidence archive release binding mismatch")
        for name, expected in inventory["members"].items():
            source = archive.extractfile(name); digest = hashlib.sha256(); size = 0
            for chunk in iter(lambda: source.read(1 << 20), b""):
                size += len(chunk); digest.update(chunk)
            _require(size == expected["bytes"] and digest.hexdigest() == expected["sha256"], f"evidence member fingerprint mismatch: {name}")
        if "derivation_evidence" in manifest:
            _verify_derivation_evidence(archive, inventory, manifest)
        else:
            _verify_compiler_evidence(archive, inventory, manifest)
    digest = hashlib.sha256(); size = 0
    with gzip.open(frozen_dir / manifest["gz_artifact"], "rb") as source:
        for chunk in iter(lambda: source.read(1 << 20), b""):
            size += len(chunk); digest.update(chunk)
    _require(digest.hexdigest() == manifest["sqlite_sha256"] and size == manifest["sqlite_bytes"], "frozen SQLite gzip mismatch")
    return frozen


def _verify_compiler_evidence(archive: tarfile.TarFile, inventory: dict, manifest: dict) -> None:
    archived_compiler = json.load(archive.extractfile("compiler/manifest.json"))
    _require(inventory["members"]["compiler/manifest.json"]["sha256"] == manifest["compiler_manifest_sha256"],
             "archived compiler manifest mismatch")
    for item in archived_compiler["outputs"].values():
        _require(inventory["members"].get("compiler/" + item["name"]) == {"sha256": item["sha256"], "bytes": item["bytes"]},
                 "archived compiler output mismatch")
    receipt = json.load(archive.extractfile("validation/receipt.json"))
    _require(receipt.get("validated") is True and receipt.get("sqlite_sha256") == manifest["sqlite_sha256"]
             and receipt.get("compiler_manifest_sha256") == manifest["compiler_manifest_sha256"], "archived validation receipt mismatch")


def _verify_derivation_evidence(archive: tarfile.TarFile, inventory: dict, manifest: dict) -> None:
    """The archived derivation binds this artifact to its frozen base, inputs and implementation."""
    members = inventory["members"]
    block = manifest.get("name_association") or {}
    _require(inventory.get("base_release_id") == block.get("base_release_id")
             and inventory.get("base_freeze_sha256") == block.get("base_freeze_sha256")
             and members.get("inputs/base-freeze.json", {}).get("sha256") == block.get("base_freeze_sha256"),
             "derivation evidence does not bind the frozen base")
    implementation = block.get("implementation_sha256") or {}
    _require(set(implementation) == set(DERIVATION_IMPLEMENTATION)
             and all(members.get(f"implementation/{name}", {}).get("sha256") == digest
                     for name, digest in implementation.items()),
             "derivation evidence does not bind the overlay implementation")
    report_member = members.get("derivation/association-report.json") or {}
    _require(report_member.get("sha256") == inventory.get("association_report_sha256"),
             "archived derivation report mismatch")
    report = json.load(archive.extractfile("derivation/association-report.json"))
    _require(report.get("format") == DERIVATION_REPORT_FORMAT
             and report.get("content_release_id") == manifest["content_release_id"]
             and report.get("sqlite_sha256") == manifest["sqlite_sha256"]
             and report.get("gz_sha256") == manifest["gz_sha256"]
             and report.get("base_release_id") == block.get("base_release_id")
             and report.get("base_sqlite_sha256") == block.get("base_sqlite_sha256")
             and report.get("policy_sha256") == block.get("policy_sha256")
             == members.get("inputs/name-association-policy.json", {}).get("sha256")
             and report.get("inputs_sha256") == inventory.get("inputs_sha256")
             and report.get("associations_total") == block.get("associations_total"),
             "archived derivation report does not describe this artifact")
    lines = sum(1 for _ in archive.extractfile("derivation/associations.jsonl"))
    _require(lines == report["associations_total"], "archived association list is incomplete")
    receipt = json.load(archive.extractfile("validation/receipt.json"))
    _require(receipt.get("validated") is True and receipt.get("sqlite_sha256") == manifest["sqlite_sha256"]
             and receipt.get("association_report_sha256") == inventory.get("association_report_sha256")
             and receipt.get("expected_associations_total") == report["associations_total"],
             "archived validation receipt mismatch")


def _require_release_decision(frozen_dir: Path, decision_path: Path | None) -> None:
    """A candidate_build_only artifact is promoted only with its release decision."""
    from database.taxonomy.scripts.release_decision import ReleaseDecisionError, require_release_decision
    manifest = _read_json(frozen_dir / "manifest.json")
    with tempfile.TemporaryDirectory() as scratch:
        sqlite_path = Path(scratch) / "artifact.sqlite3"
        with gzip.open(frozen_dir / manifest["gz_artifact"], "rb") as source, sqlite_path.open("wb") as target:
            shutil.copyfileobj(source, target, 1 << 20)
        meta = read_artifact_meta(sqlite_path)
    try:
        require_release_decision(meta_value=meta.get("release_approval"),
                                 content_release_id=manifest["content_release_id"],
                                 sqlite_sha256=manifest["sqlite_sha256"], decision_path=decision_path)
    except ReleaseDecisionError as exc:
        raise PromotionError(str(exc)) from exc


def promote(*, frozen_dir: Path, bundle_dir: Path = DEFAULT_BUNDLE_DIR,
            registry_manifest_path: Path = DEFAULT_REGISTRY_MANIFEST,
            compatibility_path: Path | None = DEFAULT_COMPATIBILITY,
            expected_freeze_sha256: str,
            release_decision_path: Path | None = None) -> dict:
    """Publish only frozen bytes; never compile, generate evidence or compress."""
    _require(_sha256_file(frozen_dir / "freeze.json") == expected_freeze_sha256, "freeze fingerprint mismatch")
    frozen = verify_frozen(frozen_dir)
    _require_release_decision(frozen_dir, release_decision_path)
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
    evidence_artifact = _evidence_pointer(manifest)["artifact"]
    for name in (manifest["gz_artifact"], evidence_artifact):
        target = bundle_dir / name
        _require(not target.exists() or _sha256_file(target) == frozen["files"][name]["sha256"],
                 f"immutable publication artifact already differs: {name}")
    temporary = existing_freeze.with_name(existing_freeze.name + ".tmp")
    shutil.copyfile(frozen_dir / "freeze.json", temporary); os.replace(temporary, existing_freeze)
    for name in (manifest["gz_artifact"], evidence_artifact, "manifest.json"):
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
    parser.add_argument("--release-decision", type=Path, default=None,
                        help="owner release decision for a candidate_build_only artifact")
    args = parser.parse_args(argv)
    try:
        manifest = promote(frozen_dir=args.frozen_dir, expected_freeze_sha256=args.expect_freeze_sha256,
            bundle_dir=args.bundle_dir, registry_manifest_path=args.registry_manifest, compatibility_path=args.compatibility,
            release_decision_path=args.release_decision)
    except (PromotionError, OSError, ValueError, KeyError) as exc:
        print(f"refused: {exc}", file=sys.stderr); return 1
    print(_dump_json(manifest), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
