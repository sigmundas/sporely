"""Tests for the formal supplement-chain validator.

The user-facing contract is: every fail-closed rule in
``database/taxonomy/reconciliation/supplement_loader.py`` must be exercised.
"""
from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import pytest

from database.taxonomy.reconciliation.supplement_loader import (
    ARTIFACT_KIND,
    SupplementLineageError,
    load_supplement_chain,
)


REPO = Path(__file__).resolve().parents[2]
BASE_RELEASE = (
    REPO
    / "database/reference_data/generated/taxonomy_v2/global_macrofungi_tax-2026.08.01-01"
)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _make_supplement(
    directory: Path,
    *,
    release_id: str,
    base_release_id: str,
    depends_on: list[dict] | None = None,
    shard_lines: list[str] | None = None,
    artifact_kind: str = ARTIFACT_KIND,
    override_shard_sha: str | None = None,
    override_manifest_sha: str | None = None,
    base_export_manifest_sha: str | None = None,
    base_scope_manifest_sha: str | None = None,
) -> Path:
    """Materialise a minimal but structurally-valid supplement on disk."""
    canonical = directory / "canonical"
    release = directory / "release"
    canonical.mkdir(parents=True, exist_ok=True)
    release.mkdir(parents=True, exist_ok=True)

    header = {
        "__registry_header__": True,
        "registry_schema_version": 1,
        "description": f"test supplement {release_id}",
    }
    lines = [json.dumps(header, sort_keys=True)]
    if shard_lines:
        lines.extend(shard_lines)
    shard = canonical / "part-0001.jsonl"
    shard.write_text("\n".join(lines) + "\n")
    shard_bytes = shard.stat().st_size
    shard_sha = _sha256(shard)

    manifest = {
        "concatenated_sha256": shard_sha,
        "manifest_schema_version": 1,
        "registry_schema_version": 1,
        "shard_bytes_target": shard_bytes,
        "shards": [
            {
                "bytes": shard_bytes,
                "line_count": len(lines) + 1,
                "name": shard.name,
                "sha256": shard_sha,
            }
        ],
        "total_bytes": shard_bytes,
        "total_line_count": len(lines) + 1,
    }
    manifest_path = canonical / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, sort_keys=True, ensure_ascii=False, indent=2) + "\n"
    )
    manifest_sha = _sha256(manifest_path)

    # Compute base release hashes if not overridden.
    if base_export_manifest_sha is None:
        base_export_manifest_sha = _sha256(
            BASE_RELEASE / "taxonomy_export_manifest.json"
        )
    if base_scope_manifest_sha is None:
        base_scope_manifest_sha = json.loads(
            (BASE_RELEASE / "taxonomy_export_manifest.json").read_text()
        )["scope_manifest_sha256"]

    release_doc = {
        "artifact_kind": artifact_kind,
        "supplement_contract_version": "supplement-contract-1.0.0",
        "supplement_release_id": release_id,
        "base_release_id": base_release_id,
        "base_release_dependency": {
            "base_release_id": base_release_id,
            "base_release_export_manifest_sha256": base_export_manifest_sha,
            "base_release_scope_manifest_sha256": base_scope_manifest_sha,
        },
        "depends_on": depends_on or [],
        "supplement_shard_sha256": override_shard_sha or shard_sha,
        "supplement_registry_manifest_sha256": override_manifest_sha or manifest_sha,
        "supplement_external_id_sha256": None,
    }
    (release / "release.json").write_text(
        json.dumps(release_doc, sort_keys=True, ensure_ascii=False, indent=2) + "\n"
    )
    return directory


def test_load_empty_chain_returns_valid_shell(tmp_path):
    chain = load_supplement_chain(
        base_release_dir=BASE_RELEASE, supplement_dirs=[]
    )
    assert chain.base_release_id == "tax-2026.08.01-01"
    assert chain.supplements == ()


def test_reject_standalone_supplement_load(tmp_path):
    supp = _make_supplement(
        tmp_path / "s", release_id="tax-t-01", base_release_id="tax-2026.08.01-01"
    )
    with pytest.raises(SupplementLineageError, match="cannot load supplements without a base"):
        load_supplement_chain(base_release_dir=None, supplement_dirs=[supp])


def test_reject_unknown_artifact_kind(tmp_path):
    supp = _make_supplement(
        tmp_path / "s",
        release_id="tax-t-01",
        base_release_id="tax-2026.08.01-01",
        artifact_kind="release",
    )
    with pytest.raises(SupplementLineageError, match="artifact_kind"):
        load_supplement_chain(base_release_dir=BASE_RELEASE, supplement_dirs=[supp])


def test_reject_base_release_id_mismatch(tmp_path):
    supp = _make_supplement(
        tmp_path / "s",
        release_id="tax-t-01",
        base_release_id="tax-something-else",
    )
    with pytest.raises(SupplementLineageError, match="base_release_id"):
        load_supplement_chain(base_release_dir=BASE_RELEASE, supplement_dirs=[supp])


def test_reject_base_hash_mismatch(tmp_path):
    supp = _make_supplement(
        tmp_path / "s",
        release_id="tax-t-01",
        base_release_id="tax-2026.08.01-01",
        base_export_manifest_sha="00" * 32,
    )
    with pytest.raises(SupplementLineageError, match="base_release_export_manifest_sha256 mismatch"):
        load_supplement_chain(base_release_dir=BASE_RELEASE, supplement_dirs=[supp])


def test_reject_missing_dependency(tmp_path):
    supp = _make_supplement(
        tmp_path / "s",
        release_id="tax-t-02",
        base_release_id="tax-2026.08.01-01",
        depends_on=[
            {
                "supplement_release_id": "tax-t-01",
                "supplement_shard_sha256": "aa" * 32,
                "supplement_registry_manifest_sha256": "bb" * 32,
            }
        ],
    )
    with pytest.raises(SupplementLineageError, match="not present earlier"):
        load_supplement_chain(base_release_dir=BASE_RELEASE, supplement_dirs=[supp])


def test_reject_out_of_order_supplements(tmp_path):
    s1 = _make_supplement(
        tmp_path / "s1",
        release_id="tax-t-01",
        base_release_id="tax-2026.08.01-01",
    )
    s2_shard_sha = _sha256(s1 / "canonical/part-0001.jsonl")
    s2_manifest_sha = _sha256(s1 / "canonical/manifest.json")
    s2 = _make_supplement(
        tmp_path / "s2",
        release_id="tax-t-02",
        base_release_id="tax-2026.08.01-01",
        depends_on=[
            {
                "supplement_release_id": "tax-t-01",
                "supplement_shard_sha256": s2_shard_sha,
                "supplement_registry_manifest_sha256": s2_manifest_sha,
            }
        ],
    )
    # Correct order works.
    ok = load_supplement_chain(
        base_release_dir=BASE_RELEASE, supplement_dirs=[s1, s2]
    )
    assert [s.release_id for s in ok.supplements] == ["tax-t-01", "tax-t-02"]
    # Swapped order fails closed.
    with pytest.raises(SupplementLineageError, match="not present earlier"):
        load_supplement_chain(
            base_release_dir=BASE_RELEASE, supplement_dirs=[s2, s1]
        )


def test_reject_dependency_hash_mismatch(tmp_path):
    s1 = _make_supplement(
        tmp_path / "s1",
        release_id="tax-t-01",
        base_release_id="tax-2026.08.01-01",
    )
    s2 = _make_supplement(
        tmp_path / "s2",
        release_id="tax-t-02",
        base_release_id="tax-2026.08.01-01",
        depends_on=[
            {
                "supplement_release_id": "tax-t-01",
                "supplement_shard_sha256": "de" * 32,  # wrong
                "supplement_registry_manifest_sha256": "ad" * 32,
            }
        ],
    )
    with pytest.raises(SupplementLineageError, match="supplement_shard_sha256 mismatch"):
        load_supplement_chain(
            base_release_dir=BASE_RELEASE, supplement_dirs=[s1, s2]
        )


def test_reject_release_id_reuse_with_different_hashes(tmp_path):
    s1 = _make_supplement(
        tmp_path / "s1",
        release_id="tax-t-01",
        base_release_id="tax-2026.08.01-01",
        shard_lines=['{"kind":"anchor","source":"nortaxa","namespace":"nortaxa_taxon_id","identifier":"1","sporely_taxon_id":900001,"allocated_in_release":"tax-t-01","first_seen_source_release":"src"}'],
    )
    s1b = _make_supplement(
        tmp_path / "s1b",
        release_id="tax-t-01",  # same release_id …
        base_release_id="tax-2026.08.01-01",
        shard_lines=['{"kind":"anchor","source":"nortaxa","namespace":"nortaxa_taxon_id","identifier":"2","sporely_taxon_id":900002,"allocated_in_release":"tax-t-01","first_seen_source_release":"src"}'],  # …different bytes
    )
    with pytest.raises(SupplementLineageError, match="release-ID reuse"):
        load_supplement_chain(
            base_release_dir=BASE_RELEASE, supplement_dirs=[s1, s1b]
        )


def test_reject_self_dependency_cycle(tmp_path):
    s = _make_supplement(
        tmp_path / "s",
        release_id="tax-t-cycle",
        base_release_id="tax-2026.08.01-01",
        depends_on=[
            {
                "supplement_release_id": "tax-t-cycle",
                "supplement_shard_sha256": "ff" * 32,
                "supplement_registry_manifest_sha256": "ff" * 32,
            }
        ],
    )
    with pytest.raises(SupplementLineageError, match="self-dependency"):
        load_supplement_chain(base_release_dir=BASE_RELEASE, supplement_dirs=[s])


def test_reject_shard_sha_mismatch_on_disk(tmp_path):
    s = _make_supplement(
        tmp_path / "s",
        release_id="tax-t-01",
        base_release_id="tax-2026.08.01-01",
    )
    # Corrupt the shard on disk after the supplement was authored.
    shard = s / "canonical/part-0001.jsonl"
    shard.write_bytes(shard.read_bytes() + b"\n{}")
    with pytest.raises(SupplementLineageError, match="sha256 mismatch"):
        load_supplement_chain(base_release_dir=BASE_RELEASE, supplement_dirs=[s])


def test_reject_declared_shard_sha_disagrees_with_manifest(tmp_path):
    s = _make_supplement(
        tmp_path / "s",
        release_id="tax-t-01",
        base_release_id="tax-2026.08.01-01",
        override_shard_sha="00" * 32,
    )
    with pytest.raises(SupplementLineageError, match="supplement_shard_sha256"):
        load_supplement_chain(base_release_dir=BASE_RELEASE, supplement_dirs=[s])
