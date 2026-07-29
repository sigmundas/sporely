"""Tests for the deterministic Sporely taxonomy shared compiler."""
from __future__ import annotations

import hashlib
import json
import shutil
import sys
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from compile_release import CompilerError, compile_release  # noqa: E402
from identity_registry import IdentityRegistry  # noqa: E402


_POLICY_PATH = Path(__file__).resolve().parents[1] / "policies" / "mapping_policy.yml"


def _write_normalized_source(
    root: Path,
    *,
    source_code: str,
    source_release: dict,
    identifier_namespace_prefix: str,
    rows: list[dict],
    reference_gaps: dict | None = None,
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    ns = {
        "core_row_id": f"{source_code}_dwc_id",
        "taxon_id": f"{source_code}_taxon_id",
        "accepted_name_usage_id": f"{source_code}_accepted_name_usage_id",
        "parent_name_usage_id": f"{source_code}_parent_name_usage_id",
    }
    taxa_path = root / "taxa.jsonl"
    with taxa_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            record = {
                "source_code": source_code,
                "source_release": source_release,
                "core_row_id": {
                    "value": row["core_row_id"], "namespace": ns["core_row_id"],
                },
                "taxon_id": {
                    "value": row["taxon_id"], "namespace": ns["taxon_id"],
                },
                "accepted_name_usage_id": (
                    {"value": row["accepted"], "namespace": ns["accepted_name_usage_id"]}
                    if row.get("accepted") else None
                ),
                "parent_name_usage_id": (
                    {"value": row["parent"], "namespace": ns["parent_name_usage_id"]}
                    if row.get("parent") else None
                ),
                "parent_reference_resolution": row.get("parent_resolution", "absent"),
                "identifier_namespace": identifier_namespace_prefix,
                "scientific_name": row["scientific_name"],
                "authorship": row.get("authorship", ""),
                "rank": row.get("rank", "species"),
                "taxonomic_status": row.get("status", "accepted"),
                "external_ids": row.get("external_ids", {}),
                "classification": {
                    "kingdom": row.get("kingdom", "Fungi"),
                    "phylum": "", "class": "", "order": "",
                    "family": "", "genus": "",
                    "specific_epithet": "", "infraspecific_epithet": "",
                },
                "provenance": {
                    "source_code": source_code,
                    "source_release": source_release,
                    "identifier_namespace": identifier_namespace_prefix,
                    "member": "taxa.tsv",
                    "row_index": 0,
                },
            }
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    report = {
        "result": "passed",
        "profile_source_code": source_code,
        "profile_source_release": source_release,
        "record_counts": {"Taxon": len(rows), "VernacularName": 0},
        "outputs": {"taxa": "taxa.jsonl"},
        "distribution_imported": False,
        "identifier_namespaces": ns,
        "archive_sha256": hashlib.sha256(
            json.dumps(rows, sort_keys=True).encode("utf-8")
        ).hexdigest(),
        "reference_gaps": reference_gaps or {
            "orphan_parent_reference_count": 0,
            "orphan_accepted_reference_count": 0,
            "orphan_parent_reference_samples": [],
            "orphan_accepted_reference_samples": [],
            "sample_bound": 25,
        },
        "hierarchy_complete": True,
        "compiler_ready": True,
    }
    (root / "report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return root


def _write_manual_mappings(path: Path, mappings: list[dict]) -> Path:
    payload = {
        "format": "sporely-taxonomy-manual-mappings-v1",
        "schema": {},
        "mappings": mappings,
    }
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def _col_source(root: Path) -> Path:
    return _write_normalized_source(
        root / "col_xr",
        source_code="col_xr",
        source_release={"version": "2026-07-17-XR", "issued_date": "2026-07-17"},
        identifier_namespace_prefix="COL:",
        rows=[
            {"core_row_id": "COL-K", "taxon_id": "COL-K", "scientific_name": "Fungi",
             "rank": "kingdom"},
            {"core_row_id": "COL-G", "taxon_id": "COL-G", "parent": "COL-K",
             "parent_resolution": "resolved",
             "scientific_name": "Candolleomyces", "rank": "genus"},
            {"core_row_id": "COL-A", "taxon_id": "COL-A", "parent": "COL-G",
             "parent_resolution": "resolved",
             "scientific_name": "Candolleomyces candolleanus"},
        ],
    )


def _nortaxa_source(root: Path) -> Path:
    return _write_normalized_source(
        root / "nortaxa",
        source_code="nortaxa",
        source_release={"version": "1.284", "issued_date": "2026-07-17"},
        identifier_namespace_prefix="NBIC:",
        rows=[
            {"core_row_id": "row-R", "taxon_id": "taxon:root",
             "scientific_name": "Fungi", "rank": "kingdom"},
            {"core_row_id": "row-G", "taxon_id": "taxon:genus",
             "parent": "taxon:root", "parent_resolution": "resolved",
             "scientific_name": "Candolleomyces", "rank": "genus"},
            {"core_row_id": "row-A", "taxon_id": "taxon:accepted",
             "parent": "taxon:genus", "parent_resolution": "resolved",
             "scientific_name": "Candolleomyces candolleanus"},
        ],
    )


def test_compile_is_byte_deterministic_across_two_runs(tmp_path: Path) -> None:
    src = tmp_path / "src"
    col = _col_source(src)
    nor = _nortaxa_source(src)
    mappings_path = _write_manual_mappings(tmp_path / "mappings.json", [])

    def do_run(run_dir: Path) -> tuple[bytes, bytes, bytes, bytes]:
        registry_path = run_dir / "registry.jsonl"
        out = run_dir / "release"
        compile_release(
            normalized_source_dirs=[col, nor],
            manual_mappings_path=mappings_path,
            mapping_policy_path=_POLICY_PATH,
            registry_path=registry_path,
            output_dir=out,
            release_id="tax-2026.07.28-01",
        )
        return (
            (out / "taxa.jsonl").read_bytes(),
            (out / "mappings.jsonl").read_bytes(),
            (out / "diagnostics.json").read_bytes(),
            registry_path.read_bytes(),
        )

    run_a = do_run(tmp_path / "run_a")
    run_b = do_run(tmp_path / "run_b")
    assert run_a == run_b, "compiler outputs must be byte-identical across runs"


def test_compile_ids_stable_under_input_order_change(tmp_path: Path) -> None:
    src = tmp_path / "src"
    col = _col_source(src)
    nor = _nortaxa_source(src)
    mappings_path = _write_manual_mappings(tmp_path / "mappings.json", [])

    def do_run(order: list[Path], run_dir: Path) -> dict:
        registry_path = run_dir / "registry.jsonl"
        out = run_dir / "release"
        compile_release(
            normalized_source_dirs=order,
            manual_mappings_path=mappings_path,
            mapping_policy_path=_POLICY_PATH,
            registry_path=registry_path,
            output_dir=out,
            release_id="tax-2026.07.28-01",
        )
        return {
            row["source_usage"]["identifier"]: row["sporely_taxon_id"]
            for row in (
                json.loads(line)
                for line in (out / "source_usages.jsonl").read_text(encoding="utf-8").splitlines()
                if line.strip()
            )
        }

    forwards = do_run([col, nor], tmp_path / "run_forward")
    reverse = do_run([nor, col], tmp_path / "run_reverse")
    assert forwards == reverse


def test_name_only_matches_do_not_merge(tmp_path: Path) -> None:
    """Two sources with identical scientific names but no manual exact mapping
    must retain distinct Sporely IDs."""
    src = tmp_path / "src"
    col = _col_source(src)
    nor = _nortaxa_source(src)
    mappings_path = _write_manual_mappings(tmp_path / "mappings.json", [])
    registry_path = tmp_path / "registry.jsonl"
    compile_release(
        normalized_source_dirs=[col, nor],
        manual_mappings_path=mappings_path,
        mapping_policy_path=_POLICY_PATH,
        registry_path=registry_path,
        output_dir=tmp_path / "release",
        release_id="tax-2026.07.28-01",
    )
    rows = [
        json.loads(line)
        for line in (tmp_path / "release" / "source_usages.jsonl").read_text(
            encoding="utf-8"
        ).splitlines()
        if line.strip()
    ]
    # Two rows both scientific_name == "Candolleomyces candolleanus": one COL,
    # one NorTaxa. No manual mapping → they must have different sporely IDs.
    matching = [r for r in rows
                if r["scientific_name"] == "Candolleomyces candolleanus"]
    assert len(matching) == 2
    assert len({r["sporely_taxon_id"] for r in matching}) == 2


def test_manual_exact_mapping_shares_identity(tmp_path: Path) -> None:
    src = tmp_path / "src"
    col = _col_source(src)
    nor = _nortaxa_source(src)
    mappings_path = _write_manual_mappings(tmp_path / "mappings.json", [
        {
            "mapping_id": "col-A_equals_nortaxa-A",
            "source_usage": {"source": "nortaxa", "namespace": "nortaxa_taxon_id",
                             "identifier": "taxon:accepted"},
            "target": {"source_usage": {
                "source": "col_xr", "namespace": "col_xr_taxon_id",
                "identifier": "COL-A",
            }},
            "relationship": "exact",
            "review_status": "approved",
            "reviewer": "test",
            "rationale": "test",
            "evidence_references": [],
            "created_at": "2026-07-28T00:00:00Z",
            "updated_at": "2026-07-28T00:00:00Z",
            "source_release_range": {"first": "1.284", "last": "1.284"},
            "supersedes": None,
        },
    ])
    registry_path = tmp_path / "registry.jsonl"
    compile_release(
        normalized_source_dirs=[col, nor],
        manual_mappings_path=mappings_path,
        mapping_policy_path=_POLICY_PATH,
        registry_path=registry_path,
        output_dir=tmp_path / "release",
        release_id="tax-2026.07.28-01",
    )
    rows = [
        json.loads(line)
        for line in (tmp_path / "release" / "source_usages.jsonl").read_text(
            encoding="utf-8"
        ).splitlines()
        if line.strip()
    ]
    col_a = next(r for r in rows if r["source_usage"]["identifier"] == "COL-A")
    nor_a = next(r for r in rows
                 if r["source_usage"]["identifier"] == "taxon:accepted")
    assert col_a["sporely_taxon_id"] == nor_a["sporely_taxon_id"]


def test_conflicting_approved_exact_mappings_fail_closed(tmp_path: Path) -> None:
    mappings_path = _write_manual_mappings(tmp_path / "mappings.json", [
        {
            "mapping_id": "m1",
            "source_usage": {"source": "nortaxa", "namespace": "nortaxa_taxon_id",
                             "identifier": "taxon:accepted"},
            "target": {"source_usage": {
                "source": "col_xr", "namespace": "col_xr_taxon_id",
                "identifier": "COL-A",
            }},
            "relationship": "exact", "review_status": "approved",
        },
        {
            "mapping_id": "m2",
            "source_usage": {"source": "nortaxa", "namespace": "nortaxa_taxon_id",
                             "identifier": "taxon:accepted"},
            "target": {"source_usage": {
                "source": "col_xr", "namespace": "col_xr_taxon_id",
                "identifier": "COL-G",
            }},
            "relationship": "exact", "review_status": "approved",
        },
    ])
    src = tmp_path / "src"
    col = _col_source(src)
    nor = _nortaxa_source(src)
    with pytest.raises(CompilerError, match="conflicts with earlier mapping"):
        compile_release(
            normalized_source_dirs=[col, nor],
            manual_mappings_path=mappings_path,
            mapping_policy_path=_POLICY_PATH,
            registry_path=tmp_path / "registry.jsonl",
            output_dir=tmp_path / "release",
            release_id="tax-2026.07.28-01",
        )


def test_unresolved_parent_is_warning_not_blocker(tmp_path: Path) -> None:
    root = _write_normalized_source(
        tmp_path / "s",
        source_code="nortaxa",
        source_release={"version": "1.284", "issued_date": "2026-07-17"},
        identifier_namespace_prefix="NBIC:",
        rows=[
            {"core_row_id": "row-A", "taxon_id": "taxon:accepted",
             "parent": "taxon:missing", "parent_resolution": "unresolved",
             "scientific_name": "Candolleomyces candolleanus"},
        ],
        reference_gaps={
            "orphan_parent_reference_count": 1,
            "orphan_accepted_reference_count": 0,
            "orphan_parent_reference_samples": [
                {"source_taxon_id": "taxon:accepted",
                 "raw_reference": "taxon:missing"},
            ],
            "orphan_accepted_reference_samples": [],
            "sample_bound": 25,
        },
    )
    mappings_path = _write_manual_mappings(tmp_path / "mappings.json", [])
    manifest = compile_release(
        normalized_source_dirs=[root],
        manual_mappings_path=mappings_path,
        mapping_policy_path=_POLICY_PATH,
        registry_path=tmp_path / "registry.jsonl",
        output_dir=tmp_path / "release",
        release_id="tax-2026.07.28-01",
    )
    diag = json.loads((tmp_path / "release" / "diagnostics.json").read_text("utf-8"))
    assert diag["counts"]["unresolved_parent_references"] == 1
    assert manifest["counts"]["compiled_rows"] == 1  # not blocked


def test_normalized_input_unresolved_accepted_is_rejected_by_normalizer():
    """The normalizer (national_source.normalize_archive) fails closed when an
    accepted reference does not resolve. The compiler enforces this by refusing
    normalized inputs whose ``compiler_ready`` is not true. This documents the
    contract; the actual normalizer test lives in
    ``tests/test_national_source.py``."""
    # Kept as a docstring-only marker; no additional assertion needed.


def test_adding_new_source_does_not_renumber_existing(tmp_path: Path) -> None:
    src = tmp_path / "src"
    col = _col_source(src)
    mappings_path = _write_manual_mappings(tmp_path / "mappings.json", [])
    registry_path = tmp_path / "registry.jsonl"

    # First run: COL only.
    compile_release(
        normalized_source_dirs=[col],
        manual_mappings_path=mappings_path,
        mapping_policy_path=_POLICY_PATH,
        registry_path=registry_path,
        output_dir=tmp_path / "release_1",
        release_id="tax-2026.07.28-01",
    )
    rows_1 = {
        r["source_usage"]["identifier"]: r["sporely_taxon_id"]
        for r in (json.loads(line) for line in
                  (tmp_path / "release_1" / "source_usages.jsonl").read_text("utf-8").splitlines()
                  if line.strip())
    }

    # Second run: COL + NorTaxa, reusing the same (append-only) registry.
    nor = _nortaxa_source(src)
    compile_release(
        normalized_source_dirs=[col, nor],
        manual_mappings_path=mappings_path,
        mapping_policy_path=_POLICY_PATH,
        registry_path=registry_path,
        output_dir=tmp_path / "release_2",
        release_id="tax-2026.07.29-01",
    )
    rows_2 = {
        r["source_usage"]["identifier"]: r["sporely_taxon_id"]
        for r in (json.loads(line) for line in
                  (tmp_path / "release_2" / "source_usages.jsonl").read_text("utf-8").splitlines()
                  if line.strip())
    }
    # COL IDs unchanged.
    for col_id, sporely_id in rows_1.items():
        assert rows_2[col_id] == sporely_id


def test_external_identifiers_are_byte_preserved(tmp_path: Path) -> None:
    src = tmp_path / "src"
    nor = _write_normalized_source(
        src / "nortaxa",
        source_code="nortaxa",
        source_release={"version": "1.284", "issued_date": "2026-07-17"},
        identifier_namespace_prefix="NBIC:",
        rows=[
            {"core_row_id": "row-A", "taxon_id": "taxon:accepted",
             "scientific_name": "Candolleomyces candolleanus",
             "external_ids": {
                 "http://rs.tdwg.org/dwc/terms/scientificNameID": "NBIC:54995",
             }},
        ],
    )
    mappings_path = _write_manual_mappings(tmp_path / "mappings.json", [])
    compile_release(
        normalized_source_dirs=[nor],
        manual_mappings_path=mappings_path,
        mapping_policy_path=_POLICY_PATH,
        registry_path=tmp_path / "registry.jsonl",
        output_dir=tmp_path / "release",
        release_id="tax-2026.07.28-01",
    )
    row = next(iter(
        json.loads(line)
        for line in (tmp_path / "release" / "source_usages.jsonl").read_text("utf-8").splitlines()
        if line.strip()
    ))
    assert row["external_ids"]["http://rs.tdwg.org/dwc/terms/scientificNameID"] \
        == "NBIC:54995"


def test_release_id_pattern_enforced(tmp_path: Path) -> None:
    src = tmp_path / "src"
    col = _col_source(src)
    mappings_path = _write_manual_mappings(tmp_path / "mappings.json", [])
    with pytest.raises(CompilerError, match="release_id"):
        compile_release(
            normalized_source_dirs=[col],
            manual_mappings_path=mappings_path,
            mapping_policy_path=_POLICY_PATH,
            registry_path=tmp_path / "registry.jsonl",
            output_dir=tmp_path / "release",
            release_id="latest",
        )


def test_manifest_binds_schema_version_and_source_hashes(tmp_path: Path) -> None:
    src = tmp_path / "src"
    col = _col_source(src)
    nor = _nortaxa_source(src)
    mappings_path = _write_manual_mappings(tmp_path / "mappings.json", [])
    manifest = compile_release(
        normalized_source_dirs=[col, nor],
        manual_mappings_path=mappings_path,
        mapping_policy_path=_POLICY_PATH,
        registry_path=tmp_path / "registry.jsonl",
        output_dir=tmp_path / "release",
        release_id="tax-2026.07.28-01",
    )
    assert manifest["taxonomy_schema_version"] == 2
    assert manifest["content_release_id"] == "tax-2026.07.28-01"
    assert manifest["state"] == "candidate"
    for binding in manifest["source_bindings"]:
        assert binding["archive_sha256"]
    for name in ("taxa", "mappings", "diagnostics"):
        assert manifest["outputs"][name]["sha256"]
        assert manifest["outputs"][name]["bytes"] >= 0
    assert manifest["outputs"]["taxa"]["bytes"] > 0
    assert manifest["outputs"]["diagnostics"]["bytes"] > 0
