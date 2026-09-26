#!/usr/bin/env python3
"""Build a taxonomy-v2 release candidate from the committed release recipe.

One command runs the whole desktop pipeline that used to be a sequence of
hand-typed steps:

1. preflight — recipe, release ID, source archives and the red-list workbook
   checked against their pinned SHA-256 values, registry shards against
   ``registry/canonical/manifest.json``;
2. normalize every recipe source, the red-list workbook and (when enabled) the
   legacy-enrichment export of the bundled legacy database;
3. compile twice from separate registry copies and build two SQLite
   candidates; every compile output, the post-compile registry and the SQLite
   hash must be identical;
4. summarize coverage (taxa, vernacular languages, external-ID sources) and
   compare it with the currently bundled release;
5. optionally promote candidate A into the desktop bundle
   (``promote_desktop_bundle.py``).

Everything is written under ``--build-dir``, which must not exist yet;
``build-report.json`` there is the record of the run. Nothing in the
repository changes unless ``--promote`` is given. The cloud copy is prepared
afterwards from the promoted bundle (see ``database/taxonomy/README.md``).

Tracked inputs are passed to the pipeline as repository-relative paths, so the
compiler manifest does not depend on the checkout location. The gitignored
archives and workbook are looked up under ``--archive-root`` (default: this
checkout); reproduce a build with the same value.
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
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_REL = Path("database/taxonomy/scripts")
DEFAULT_RECIPE = REPO_ROOT / "database/taxonomy/release-recipe.json"
DEFAULT_BUNDLE_DIR = REPO_ROOT / "database/reference_data/generated/taxonomy_v2"
RECIPE_FORMAT = "sporely-taxonomy-release-recipe-v1"
RELEASE_ID_RE = re.compile(r"^tax-\d{4}\.\d{2}\.\d{2}-\d{2}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SOURCE_CODE_RE = re.compile(r"^[a-z][a-z0-9_]{1,31}$")
NORMALIZERS = ("col_xr", "national_source")
TAXON_ID_COLUMNS = ("norwegian_taxon_id", "swedish_taxon_id", "inaturalist_taxon_id")


class BuildError(Exception):
    """A preflight or pipeline check failed; nothing is promoted."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise BuildError(message)


def _read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise BuildError(f"cannot read {path}: {exc}") from exc


# --------------------------------------------------------------------------
# Recipe
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class Source:
    code: str
    normalizer: str
    release_dir: str            # repository-relative, tracked
    version: str | None = None
    issued_date: str | None = None
    profile: str | None = None  # repository-relative, national_source only

    @property
    def manifest(self) -> str:
        return f"{self.release_dir}/manifest.json"

    @property
    def archive(self) -> str:
        return f"{self.release_dir}/archive.zip"


@dataclass(frozen=True)
class Recipe:
    sources: tuple[Source, ...]
    redlist_workbook: str
    redlist_sha256: str
    legacy_enabled: bool
    legacy_db: str | None
    legacy_sha256: str | None
    policies: dict[str, str]
    registry: str
    # Optional committed, fingerprinted publishing-id inputs (publishing_ids.py):
    # {"path": repo-relative, "sha256": ...}
    artportalen_overlay: dict | None = None
    inaturalist_refresh: dict | None = None


def load_recipe(path: Path) -> Recipe:
    raw = _read_json(path)
    _require(raw.get("format") == RECIPE_FORMAT, f"{path}: format must be {RECIPE_FORMAT}")
    sources: list[Source] = []
    seen: set[str] = set()
    for entry in raw.get("sources") or []:
        code = entry.get("code", "")
        _require(isinstance(code, str) and bool(SOURCE_CODE_RE.match(code)),
                 f"recipe source code {code!r} is not a short lowercase identifier")
        _require(code not in seen, f"recipe source {code!r} is listed twice")
        seen.add(code)
        normalizer = entry.get("normalizer")
        _require(normalizer in NORMALIZERS,
                 f"recipe source {code!r}: normalizer must be one of {NORMALIZERS}")
        source = Source(code=code, normalizer=normalizer, release_dir=entry.get("release_dir", ""),
                        version=entry.get("version"), issued_date=entry.get("issued_date"),
                        profile=entry.get("profile"))
        _require(bool(source.release_dir), f"recipe source {code!r}: release_dir is required")
        if normalizer == "col_xr":
            _require(bool(source.version and source.issued_date),
                     f"recipe source {code!r}: col_xr needs version and issued_date")
        else:
            _require(bool(source.profile), f"recipe source {code!r}: national_source needs a profile")
        sources.append(source)
    _require(bool(sources), "recipe lists no sources")

    redlist = raw.get("redlist_no") or {}
    legacy = raw.get("legacy_enrichment") or {}
    legacy_enabled = bool(legacy.get("enabled"))
    for label, value in (("redlist_no.sha256", redlist.get("sha256")),
                         *((("legacy_enrichment.sha256", legacy.get("sha256")),) if legacy_enabled else ())):
        _require(isinstance(value, str) and bool(SHA256_RE.match(value)),
                 f"recipe {label} must be a lowercase SHA-256")
    policies = raw.get("policies") or {}
    for key in ("manual_mappings", "concept_supersessions", "mapping_policy"):
        _require(bool(policies.get(key)), f"recipe policies.{key} is required")
    _require(bool(redlist.get("workbook")), "recipe redlist_no.workbook is required")
    _require(bool(raw.get("registry")), "recipe registry is required")
    if legacy_enabled:
        _require(bool(legacy.get("bundled_db")), "recipe legacy_enrichment.bundled_db is required")
    pinned: dict[str, dict | None] = {}
    for key, raw_entry in (("artportalen_overlay", (raw.get("publishing_overlays") or {}).get("artportalen")),
                           ("inaturalist_refresh", raw.get("inaturalist_refresh"))):
        if raw_entry is None:
            pinned[key] = None
            continue
        _require(bool(raw_entry.get("path")), f"recipe {key}.path is required")
        _require(isinstance(raw_entry.get("sha256"), str) and bool(SHA256_RE.match(raw_entry["sha256"])),
                 f"recipe {key}.sha256 must be a lowercase SHA-256")
        pinned[key] = {"path": raw_entry["path"], "sha256": raw_entry["sha256"]}
    return Recipe(sources=tuple(sources), redlist_workbook=redlist["workbook"],
                  redlist_sha256=redlist["sha256"], legacy_enabled=legacy_enabled,
                  legacy_db=legacy.get("bundled_db") if legacy_enabled else None,
                  legacy_sha256=legacy.get("sha256") if legacy_enabled else None,
                  policies=dict(policies), registry=raw["registry"],
                  artportalen_overlay=pinned["artportalen_overlay"],
                  inaturalist_refresh=pinned["inaturalist_refresh"])


def manifest_archive_sha256(manifest: dict) -> str:
    """The archive hash an acquisition manifest pins (COL and NorTaxa shapes)."""
    download = manifest.get("download") or {}
    for key in ("sha256", "archive_sha256"):
        value = download.get(key)
        if isinstance(value, str) and SHA256_RE.match(value):
            return value
    raise BuildError("acquisition manifest pins no archive SHA-256 under download.sha256 "
                     "or download.archive_sha256")


# --------------------------------------------------------------------------
# Coverage summary
# --------------------------------------------------------------------------

def _grouped(conn: sqlite3.Connection, sql: str) -> dict[str, int]:
    return {str(key): int(count) for key, count in conn.execute(sql)}


def summarize_sqlite(path: Path) -> dict:
    """Counts that show what a release carries, for comparing releases."""
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        columns = {row[1] for row in conn.execute("PRAGMA table_info(taxon_min)")}
        summary: dict = {
            "taxa": conn.execute("SELECT COUNT(*) FROM taxon_min").fetchone()[0],
            "taxa_by_source_status": _grouped(conn,
                "SELECT source_system || '/' || COALESCE(taxonomic_status, ''), COUNT(*) "
                "FROM taxon_min GROUP BY 1 ORDER BY 1"),
            "vernacular_by_language": _grouped(conn,
                "SELECT language_code, COUNT(*) FROM vernacular_min GROUP BY 1 ORDER BY 1"),
            "taxon_id_columns": {
                column: conn.execute(f"SELECT COUNT({column}) FROM taxon_min").fetchone()[0]
                for column in TAXON_ID_COLUMNS if column in columns
            },
        }
        if "taxon_external_id_min" in tables:
            summary["external_ids_by_source"] = _grouped(conn,
                "SELECT source_system, COUNT(*) FROM taxon_external_id_min GROUP BY 1 ORDER BY 1")
        if "taxon_external_id_text_min" in tables:
            summary["text_external_ids_by_namespace"] = _grouped(conn,
                "SELECT source_system || '/' || namespace, COUNT(*) "
                "FROM taxon_external_id_text_min GROUP BY 1 ORDER BY 1")
        if "taxon_redlist_min" in tables:
            summary["redlist_rows"] = conn.execute("SELECT COUNT(*) FROM taxon_redlist_min").fetchone()[0]
        if "taxonomy_meta" in tables:
            meta = dict(conn.execute("SELECT key, value FROM taxonomy_meta"))
            summary["content_release_id"] = meta.get("content_release_id")
        return summary
    finally:
        conn.close()


def _flatten(summary: dict, prefix: str = "") -> dict[str, object]:
    flat: dict[str, object] = {}
    for key, value in summary.items():
        name = f"{prefix}{key}"
        if isinstance(value, dict):
            flat.update(_flatten(value, f"{name}."))
        else:
            flat[name] = value
    return flat


def coverage_delta(baseline: dict, candidate: dict) -> dict[str, dict]:
    """Every count that differs between two summaries, as {baseline, candidate}."""
    before, after = _flatten(baseline), _flatten(candidate)
    return {key: {"baseline": before.get(key), "candidate": after.get(key)}
            for key in sorted(set(before) | set(after))
            if before.get(key) != after.get(key)}


def compare_trees(left: Path, right: Path) -> tuple[dict[str, str], list[str]]:
    """Hash every file under two directories; return (hashes of left, mismatches)."""
    def hashes(root: Path) -> dict[str, str]:
        return {p.relative_to(root).as_posix(): sha256_file(p)
                for p in sorted(root.rglob("*")) if p.is_file()}
    a, b = hashes(left), hashes(right)
    mismatches = sorted(name for name in set(a) | set(b) if a.get(name) != b.get(name))
    return a, mismatches


# --------------------------------------------------------------------------
# Pipeline
# --------------------------------------------------------------------------

Runner = Callable[[str, list[str]], str]


class StepRunner:
    """Runs one pipeline script per step from the repository root.

    Output goes to files under ``<build>/logs`` as it is produced; a one-line
    progress record per step goes to stdout and ``<build>/progress.log``.
    """

    def __init__(self, build_dir: Path, python: str):
        self.logs = build_dir / "logs"
        self.logs.mkdir(parents=True, exist_ok=True)
        self.progress = build_dir / "progress.log"
        self.python = python

    def log(self, message: str) -> None:
        line = f"[{time.strftime('%H:%M:%S')}] {message}"
        print(line, flush=True)
        with self.progress.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")

    def __call__(self, label: str, argv: list[str]) -> str:
        self.log(f"START {label}")
        started = time.monotonic()
        out_path, err_path = self.logs / f"{label}.out", self.logs / f"{label}.err"
        with out_path.open("wb") as out, err_path.open("wb") as err:
            result = subprocess.run([self.python, *argv], cwd=REPO_ROOT, stdout=out, stderr=err)
        if result.returncode != 0:
            tail = err_path.read_text(encoding="utf-8", errors="replace")[-2000:]
            self.log(f"FAIL {label} (exit {result.returncode})")
            raise BuildError(f"{label} failed with exit {result.returncode}: {tail}")
        self.log(f"OK {label} ({time.monotonic() - started:.0f}s)")
        return out_path.read_text(encoding="utf-8")


@dataclass(frozen=True)
class Options:
    release_id: str
    build_dir: Path
    recipe_path: Path = DEFAULT_RECIPE
    archive_root: Path = REPO_ROOT
    bundle_dir: Path = DEFAULT_BUNDLE_DIR
    promote: bool = False
    compare_baseline: bool = True
    # SQLite SHA-256 of an earlier, independent run; promotion requires equality.
    expect_sqlite_sha256: str | None = None


def _script(name: str) -> str:
    return (SCRIPTS_REL / name).as_posix()


def _gitignored_input(options: Options, rel: str) -> str:
    """Path to a gitignored input, relative when it lives in this checkout."""
    if options.archive_root.resolve() == REPO_ROOT:
        return rel
    return str(options.archive_root / rel)


def preflight(options: Options, recipe: Recipe) -> dict:
    """Check every input before any step runs. Returns the verified inputs."""
    _require(bool(RELEASE_ID_RE.match(options.release_id)),
             f"release id {options.release_id!r} is not tax-YYYY.MM.DD-NN")
    bundled = options.bundle_dir / "manifest.json"
    if bundled.exists():
        bundled_id = _read_json(bundled).get("content_release_id")
        _require(bundled_id != options.release_id,
                 f"{options.release_id} is the bundled release; releases are immutable, pick a new id")
    _require(not options.build_dir.exists(), f"build dir {options.build_dir} already exists")

    inputs: dict = {"sources": {}, "registry_sha256": verify_registry(REPO_ROOT / recipe.registry)}
    for source in recipe.sources:
        manifest_path = REPO_ROOT / source.manifest
        _require(manifest_path.is_file(), f"{source.code}: missing acquisition manifest {source.manifest}")
        expected = manifest_archive_sha256(_read_json(manifest_path))
        archive = options.archive_root / source.archive
        _require(archive.is_file(), f"{source.code}: archive not found at {archive}")
        actual = sha256_file(archive)
        _require(actual == expected,
                 f"{source.code}: archive SHA-256 {actual} does not match its manifest ({expected})")
        if source.profile:
            _require((REPO_ROOT / source.profile).is_file(), f"{source.code}: missing profile {source.profile}")
        inputs["sources"][source.code] = {"archive_sha256": actual, "manifest": source.manifest}

    workbook = options.archive_root / recipe.redlist_workbook
    _require(workbook.is_file(), f"red-list workbook not found at {workbook}")
    _require(sha256_file(workbook) == recipe.redlist_sha256,
             "red-list workbook SHA-256 does not match the recipe")
    inputs["redlist_workbook_sha256"] = recipe.redlist_sha256

    if recipe.legacy_enabled:
        legacy = REPO_ROOT / recipe.legacy_db
        _require(legacy.is_file(), f"legacy database not found at {legacy}")
        _require(sha256_file(legacy) == recipe.legacy_sha256,
                 "legacy database SHA-256 does not match the recipe")
        inputs["legacy_db_sha256"] = recipe.legacy_sha256

    for key, rel in recipe.policies.items():
        _require((REPO_ROOT / rel).is_file(), f"missing policy {key}: {rel}")
    for key, pinned in (("artportalen_overlay", recipe.artportalen_overlay),
                        ("inaturalist_refresh", recipe.inaturalist_refresh)):
        if pinned is not None:
            inputs[f"{key}_sha256"] = verify_pinned(key, pinned)
    return inputs


def _registry_shards(registry_dir: Path) -> tuple[list[Path], str]:
    manifest_path = registry_dir / "manifest.json"
    _require(manifest_path.is_file(), f"missing registry manifest {manifest_path}")
    manifest = _read_json(manifest_path)
    shards = [registry_dir / shard["name"] for shard in manifest.get("shards") or []]
    _require(bool(shards), f"{manifest_path} lists no shards")
    for shard in shards:
        _require(shard.is_file(), f"missing registry shard {shard}")
    return shards, str(manifest.get("concatenated_sha256"))


def verify_registry(registry_dir: Path) -> str:
    """Hash the shards in manifest order; they must reproduce the manifest's hash."""
    shards, expected = _registry_shards(registry_dir)
    digest = hashlib.sha256()
    for shard in shards:
        with shard.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                digest.update(chunk)
    actual = digest.hexdigest()
    _require(actual == expected, f"registry shards concatenate to {actual}, manifest says {expected}")
    return actual


def verify_pinned(key: str, pinned: dict) -> str:
    """A committed recipe input must exist and match its pinned SHA-256."""
    path = REPO_ROOT / pinned["path"]
    _require(path.is_file(), f"missing {key}: {pinned['path']}")
    _require(sha256_file(path) == pinned["sha256"], f"{key} SHA-256 does not match the recipe")
    return pinned["sha256"]


def assemble_registry(registry_dir: Path, dest: Path) -> str:
    """Concatenate the canonical shards in manifest order and verify the result."""
    shards, expected = _registry_shards(registry_dir)
    with dest.open("wb") as out:
        for shard in shards:
            with shard.open("rb") as handle:
                shutil.copyfileobj(handle, out, 1 << 20)
    actual = sha256_file(dest)
    _require(actual == expected, f"registry shards concatenate to {actual}, manifest says {expected}")
    return actual


def build(options: Options, run: Runner | None = None, python: str = sys.executable) -> dict:
    recipe = load_recipe(options.recipe_path)
    inputs = preflight(options, recipe)
    b = options.build_dir
    b.mkdir(parents=True)
    runner = StepRunner(b, python)
    run = run or runner
    rel = str  # build outputs are passed as absolute paths; the compiler does not record them

    # 1. Normalize.
    norm = b / "norm"
    for source in recipe.sources:
        archive = _gitignored_input(options, source.archive)
        if source.normalizer == "col_xr":
            argv = [_script("normalize_col_xr.py"), "--archive", archive,
                    "--output", rel(norm / source.code),
                    "--source-release-version", source.version,
                    "--source-release-issued-date", source.issued_date]
        else:
            argv = [_script("national_source.py"), "normalize", "--profile", source.profile,
                    "--archive", archive, "--output", rel(norm / source.code)]
        run(f"normalize_{source.code}", argv)
    run("normalize_redlist_no", [_script("normalize_redlist_no.py"),
                                 "--input", _gitignored_input(options, recipe.redlist_workbook),
                                 "--output", rel(norm / "redlist_no")])
    legacy_jsonl = norm / "legacy_enrichment.jsonl"
    if recipe.legacy_enabled:
        run("export_legacy_enrichment", [_script("export_legacy_enrichment.py"),
                                         "--bundled-db", recipe.legacy_db, "--output", rel(legacy_jsonl)])

    # 2. Two compiles and two SQLite candidates from separate registry copies.
    base_sha = assemble_registry(REPO_ROOT / recipe.registry, b / "registry-base.jsonl")
    sqlite_out: dict[str, dict] = {}
    for name in ("A", "B"):
        registry = b / f"registry-{name}.jsonl"
        shutil.copyfile(b / "registry-base.jsonl", registry)
        argv = [_script("compile_release.py")]
        for source in recipe.sources:
            argv += ["--source", rel(norm / source.code)]
        argv += ["--redlist", rel(norm / "redlist_no"),
                 "--manual-mappings", recipe.policies["manual_mappings"],
                 "--concept-supersessions", recipe.policies["concept_supersessions"],
                 "--mapping-policy", recipe.policies["mapping_policy"],
                 "--registry", rel(registry), "--output", rel(b / f"release{name}"),
                 "--release-id", options.release_id]
        for source in recipe.sources:
            argv += ["--source-release-manifest", f"{source.code}={source.manifest}"]
        if recipe.legacy_enabled:
            argv += ["--legacy-enrichment-input", rel(legacy_jsonl)]
        run(f"compile{name}", argv)
        sqlite_argv = [_script("build_sqlite_candidate.py"),
                       "--release-dir", rel(b / f"release{name}"),
                       "--registry", rel(registry),
                       "--output", rel(b / f"{options.release_id}-{name}.sqlite3")]
        if recipe.artportalen_overlay:
            sqlite_argv += ["--publishing-overlay", recipe.artportalen_overlay["path"]]
        if recipe.inaturalist_refresh:
            sqlite_argv += ["--inaturalist-refresh", recipe.inaturalist_refresh["path"]]
        stdout = run(f"sqlite{name}", sqlite_argv)
        sqlite_out[name] = json.loads(stdout)

    # 3. Determinism and registry checks.
    release_hashes, release_mismatches = compare_trees(b / "releaseA", b / "releaseB")
    registry_a, registry_b = sha256_file(b / "registry-A.jsonl"), sha256_file(b / "registry-B.jsonl")
    sqlite_a = sha256_file(b / f"{options.release_id}-A.sqlite3")
    sqlite_b = sha256_file(b / f"{options.release_id}-B.sqlite3")
    mismatches = list(release_mismatches)
    if registry_a != registry_b:
        mismatches.append("registry")
    if sqlite_a != sqlite_b:
        mismatches.append("sqlite")
    determinism = {"identical": not mismatches, "mismatches": mismatches,
                   "compile_artifacts": release_hashes,
                   "registry_after_compile": registry_a, "sqlite_sha256": sqlite_a}
    base_lines = (b / "registry-base.jsonl").read_bytes().count(b"\n")
    after_lines = (b / "registry-A.jsonl").read_bytes().count(b"\n")
    registry_state = {"base_sha256": base_sha, "after_sha256": registry_a,
                      "unchanged": registry_a == base_sha, "new_allocations": after_lines - base_lines}

    # 4. Coverage, compared with the bundled release.
    candidate = b / f"{options.release_id}-A.sqlite3"
    coverage = {"candidate": summarize_sqlite(candidate)}
    bundled_manifest = options.bundle_dir / "manifest.json"
    if options.compare_baseline and bundled_manifest.exists():
        manifest = _read_json(bundled_manifest)
        baseline_db = b / "baseline.sqlite3"
        try:
            with gzip.open(options.bundle_dir / manifest["gz_artifact"], "rb") as src, \
                    baseline_db.open("wb") as dst:
                shutil.copyfileobj(src, dst, 1 << 20)
            coverage["baseline"] = summarize_sqlite(baseline_db)
        except (KeyError, OSError, EOFError, sqlite3.DatabaseError) as exc:
            raise BuildError(f"cannot read the bundled release for comparison ({exc}); "
                             "rerun with --no-baseline to skip it") from exc
        coverage["baseline_release_id"] = manifest.get("content_release_id")
        coverage["delta"] = coverage_delta(coverage["baseline"], coverage["candidate"])
        baseline_db.unlink()

    report = {
        "release_id": options.release_id,
        "recipe": os.path.relpath(options.recipe_path, REPO_ROOT),
        "recipe_sha256": sha256_file(options.recipe_path),
        "inputs": inputs,
        "legacy_enrichment": recipe.legacy_enabled,
        "determinism": determinism,
        "registry": registry_state,
        "sqlite": {"sha256": sqlite_a, "counts": sqlite_out["A"].get("counts"),
                   "publishing_ids": sqlite_out["A"].get("publishing_ids"),
                   "authoritative_bridge_emission": sqlite_out["A"].get("authoritative_bridge_emission")},
        "coverage": coverage,
        "promoted": False,
    }

    # 5. Promotion, only for a deterministic build that allocated nothing new.
    if options.expect_sqlite_sha256:
        report["expected_sqlite_sha256"] = options.expect_sqlite_sha256
        report["matches_expected"] = sqlite_a == options.expect_sqlite_sha256
    if options.promote:
        _require(determinism["identical"], f"not promoting: builds differ in {mismatches}")
        _require(report.get("matches_expected", True),
                 f"not promoting: SQLite {sqlite_a} differs from the earlier run's {options.expect_sqlite_sha256}")
        _require(registry_state["unchanged"],
                 f"not promoting: the compile allocated {registry_state['new_allocations']} new IDs; "
                 "re-shard and commit the registry first (database/taxonomy/README.md)")
        run("promote", [_script("promote_desktop_bundle.py"),
                        "--sqlite", rel(candidate), "--release-dir", rel(b / "releaseA"),
                        "--redlist-report", rel(norm / "redlist_no/report.json"),
                        "--redlist-workbook", _gitignored_input(options, recipe.redlist_workbook),
                        "--bundle-dir", rel(options.bundle_dir)])
        report["promoted"] = True

    (b / "build-report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report


def _print_summary(report: dict) -> None:
    det, reg, cov = report["determinism"], report["registry"], report["coverage"]
    print(f"\nrelease {report['release_id']}  sqlite {det['sqlite_sha256']}")
    print(f"deterministic: {det['identical']}" + ("" if det["identical"] else f"  mismatches: {det['mismatches']}"))
    print(f"registry unchanged: {reg['unchanged']}  new allocations: {reg['new_allocations']}")
    langs = cov["candidate"]["vernacular_by_language"]
    print("vernacular languages: " + ", ".join(f"{k}={v}" for k, v in langs.items()))
    print("external ids: " + ", ".join(f"{k}={v}" for k, v in cov["candidate"].get("external_ids_by_source", {}).items()))
    if "delta" in cov:
        print(f"changes vs bundled {cov['baseline_release_id']}: {len(cov['delta'])} counts differ (see build-report.json)")
    if "matches_expected" in report:
        print(f"matches earlier run: {report['matches_expected']}")
    print(f"promoted: {report['promoted']}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--release-id", required=True, help="new release id, tax-YYYY.MM.DD-NN")
    parser.add_argument("--build-dir", type=Path, required=True, help="scratch directory; must not exist")
    parser.add_argument("--recipe", type=Path, default=DEFAULT_RECIPE)
    parser.add_argument("--archive-root", type=Path, default=REPO_ROOT,
                        help="checkout holding the gitignored source archives and workbook")
    parser.add_argument("--promote", action="store_true",
                        help="write the candidate into the desktop bundle when every check passes")
    parser.add_argument("--no-baseline", action="store_true",
                        help="skip the comparison with the bundled release")
    parser.add_argument("--expect-sqlite-sha256",
                        help="SQLite SHA-256 of an earlier independent run; --promote requires equality")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    options = Options(release_id=args.release_id, build_dir=args.build_dir.resolve(),
                      recipe_path=args.recipe.resolve(), archive_root=args.archive_root.resolve(),
                      promote=args.promote, compare_baseline=not args.no_baseline,
                      expect_sqlite_sha256=args.expect_sqlite_sha256)
    try:
        report = build(options)
    except BuildError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    _print_summary(report)
    return 0 if report["determinism"]["identical"] and report.get("matches_expected", True) else 1


if __name__ == "__main__":
    raise SystemExit(main())
