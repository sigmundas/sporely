"""Focused tests for the canonical registry shard directory format.

Covers only storage semantics — no mapping, identity, scope, synonym,
vernacular, or compiler behavior is exercised here.
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from identity_registry import (  # noqa: E402
    IdentityRegistry,
    RegistryError,
    SHARD_DEFAULT_TARGET_BYTES,
    SHARD_MANIFEST_FILENAME,
    iter_shard_lines,
    load_shard_manifest,
    shard_registry,
)


def _seed_registry(path: Path, entries: int) -> None:
    reg = IdentityRegistry(path)
    reg.load()
    for i in range(entries):
        reg.allocate(
            source="col_xr", namespace="col_usage_id", identifier=f"COL-{i:06d}",
            allocated_in_release="tax-2026.07.29-01",
            first_seen_source_release="2026-07-17-XR:2026-07-17",
        )
    reg.flush()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_single_file_and_sharded_load_identically(tmp_path: Path) -> None:
    source = tmp_path / "source.jsonl"
    _seed_registry(source, entries=500)

    shard_dir = tmp_path / "canonical"
    shard_registry(source, shard_dir, shard_bytes_target=8 * 1024)

    single = IdentityRegistry(source)
    single.load()
    sharded = IdentityRegistry(shard_dir)
    sharded.load()

    assert single.anchor_count() == sharded.anchor_count()
    assert single.alias_count() == sharded.alias_count()
    for anchor in [single.get_anchor(i) for i in range(1, single.anchor_count() + 1)]:
        assert anchor is not None
        assert sharded.get_anchor(anchor.sporely_taxon_id) == anchor


def test_concatenated_shard_bytes_equal_source(tmp_path: Path) -> None:
    source = tmp_path / "source.jsonl"
    _seed_registry(source, entries=1200)
    source_sha = _sha256(source)
    source_bytes = source.read_bytes()

    shard_dir = tmp_path / "canonical"
    manifest = shard_registry(source, shard_dir, shard_bytes_target=16 * 1024)

    # 1. Manifest reports the same SHA.
    assert manifest["concatenated_sha256"] == source_sha
    # 2. Streamed concatenation reproduces every byte in manifest order.
    joined = b"".join(iter_shard_lines(shard_dir))
    assert joined == source_bytes
    assert hashlib.sha256(joined).hexdigest() == source_sha


def test_missing_shard_is_rejected(tmp_path: Path) -> None:
    source = tmp_path / "source.jsonl"
    _seed_registry(source, entries=800)
    shard_dir = tmp_path / "canonical"
    shard_registry(source, shard_dir, shard_bytes_target=8 * 1024)
    # Delete one of the middle shards.
    parts = sorted(p for p in shard_dir.iterdir()
                   if p.name.startswith("part-"))
    assert len(parts) >= 2
    parts[1].unlink()
    with pytest.raises(RegistryError, match="missing declared shards"):
        load_shard_manifest(shard_dir)
    with pytest.raises(RegistryError):
        IdentityRegistry(shard_dir).load()


def test_extra_file_in_shard_dir_is_rejected(tmp_path: Path) -> None:
    source = tmp_path / "source.jsonl"
    _seed_registry(source, entries=200)
    shard_dir = tmp_path / "canonical"
    shard_registry(source, shard_dir, shard_bytes_target=8 * 1024)
    (shard_dir / "part-9999.jsonl").write_bytes(b"stray\n")
    with pytest.raises(RegistryError, match="extra files"):
        load_shard_manifest(shard_dir)


def test_tampered_shard_bytes_are_rejected(tmp_path: Path) -> None:
    source = tmp_path / "source.jsonl"
    _seed_registry(source, entries=400)
    shard_dir = tmp_path / "canonical"
    shard_registry(source, shard_dir, shard_bytes_target=8 * 1024)
    victim = shard_dir / "part-0001.jsonl"
    tampered = bytearray(victim.read_bytes())
    # Flip a bit inside the payload while preserving byte size and line count.
    for i, b in enumerate(tampered):
        if b == ord("0"):
            tampered[i] = ord("9")
            break
    else:
        pytest.skip("no target byte to flip in this fixture")
    victim.write_bytes(bytes(tampered))
    with pytest.raises(RegistryError, match="SHA-256 mismatch"):
        load_shard_manifest(shard_dir)


def test_reordering_shard_manifest_is_rejected(tmp_path: Path) -> None:
    """Rewriting manifest.shards[] in a different order yields a
    concatenated SHA-256 that no longer matches the recorded value."""
    source = tmp_path / "source.jsonl"
    _seed_registry(source, entries=400)
    shard_dir = tmp_path / "canonical"
    shard_registry(source, shard_dir, shard_bytes_target=8 * 1024)
    manifest_path = shard_dir / SHARD_MANIFEST_FILENAME
    m = json.loads(manifest_path.read_text("utf-8"))
    if len(m["shards"]) < 2:
        pytest.skip("shard count too small to reorder")
    m["shards"].reverse()
    manifest_path.write_text(json.dumps(m, indent=2, sort_keys=True))
    with pytest.raises(RegistryError, match="concatenated SHA-256 mismatch"):
        load_shard_manifest(shard_dir)


def test_no_shard_exceeds_25_mib(tmp_path: Path) -> None:
    """Explicit 25 MiB ceiling from the storage requirement."""
    source = tmp_path / "source.jsonl"
    _seed_registry(source, entries=5000)
    shard_dir = tmp_path / "canonical"
    manifest = shard_registry(source, shard_dir,
                              shard_bytes_target=SHARD_DEFAULT_TARGET_BYTES)
    for entry in manifest["shards"]:
        assert entry["bytes"] <= SHARD_DEFAULT_TARGET_BYTES, entry


def test_committed_canonical_shards_verify(tmp_path: Path) -> None:
    """The canonical shard directory checked into the repository must load
    and self-verify without any external fixtures."""
    canonical = Path(__file__).resolve().parents[1] / "registry" / "canonical"
    if not canonical.exists():
        pytest.skip("canonical shard directory not present in this checkout")
    manifest = load_shard_manifest(canonical)
    assert manifest["registry_schema_version"] == 1
    # Every shard must be at or under the 25 MiB target.
    for entry in manifest["shards"]:
        assert entry["bytes"] <= SHARD_DEFAULT_TARGET_BYTES
    # The recorded concatenated hash matches the streamed concatenation.
    d = hashlib.sha256()
    for line in iter_shard_lines(canonical):
        d.update(line)
    assert d.hexdigest() == manifest["concatenated_sha256"]


def test_flush_refuses_shard_directory_path(tmp_path: Path) -> None:
    shard_dir = tmp_path / "canonical"
    source = tmp_path / "src.jsonl"
    _seed_registry(source, entries=10)
    shard_registry(source, shard_dir, shard_bytes_target=8 * 1024)
    # Loading works; flushing to a directory path is refused.
    reg = IdentityRegistry(shard_dir)
    reg.load()
    with pytest.raises(RegistryError, match="single JSONL file"):
        reg.flush()
