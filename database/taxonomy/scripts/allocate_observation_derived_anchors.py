#!/usr/bin/env python3
"""W2E-A2: allocate observation-derived NorTaxa registry anchors.

The pipeline that produces the macrofungi release (compile_release.py) is
scoped to Fungi. Historical Sporely observations sometimes carry NorTaxa
identifiers that fall outside that scope — plants, animals, non-fungi
taxa. Under the W2E-A2 policy those observations may still attach to a
stable ``sporely_taxon_id`` through a source-backed registry anchor,
without broadening the macrofungi search cache.

This script is the ONLY sanctioned pipeline layer for allocating such
anchors. It:

* refuses to run against production;
* re-verifies every input identifier against the pinned NorTaxa 1.284
  archive — no ID is allocated without source evidence;
* refuses when the archive's ``taxonomicStatus`` for the identifier is
  neither ``valid`` nor ``synonym`` (aligned with policy);
* refuses when a synonym's ``acceptedNameUsageID`` is not itself declared
  as an anchor in the same input policy (prevents dangling aliases);
* uses ``IdentityRegistry.allocate()`` and ``bind_alias()`` verbatim —
  no ad-hoc sporely_taxon_id assignment;
* emits a NEW registry supplement directory (a fresh append-only shard
  set) and a matching release manifest. The existing macrofungi release
  bytes are untouched.

Output shape (deterministic):

  <output>/canonical/manifest.json
  <output>/canonical/part-0001.jsonl
  <output>/release/observation-supplement-release.json
  <output>/release/taxon_external_id_supplement.jsonl
  <output>/release/taxon_external_id_supplement.sha256.txt

Run twice into distinct output directories → byte-identical output.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Support running the compile-time helpers without pyyaml. This module
# needs YAML only for a tiny, well-scoped input schema — a stdlib mini
# parser is safer than adding a dependency to a taxonomy-compile script.
try:
    import yaml as _yaml
except ImportError:  # pragma: no cover — fallback path
    _yaml = None

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from database.taxonomy.scripts.identity_registry import (  # noqa: E402
    IdentityRegistry,
    REGISTRY_SCHEMA_VERSION,
    REGISTRY_HEADER_KEY,
    shard_registry,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
NORTAXA_ARCHIVE = REPO_ROOT / "database/taxonomy/sources/nortaxa/1.284/archive.zip"


@dataclass(frozen=True)
class AnchorSpec:
    taxon_id: str
    scientific_name: str
    kingdom: str
    rank: str
    diagnostic_bucket: str


@dataclass(frozen=True)
class AliasSpec:
    taxon_id: str
    scientific_name: str
    kingdom: str
    rank: str
    accepted_taxon_id: str
    diagnostic_bucket: str


@dataclass(frozen=True)
class Policy:
    release_id: str
    first_seen_source_release: str
    source_system: str
    namespace: str
    anchors: tuple[AnchorSpec, ...]
    aliases: tuple[AliasSpec, ...]


def _load_yaml(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if _yaml is not None:
        return _yaml.safe_load(text)
    # Minimal parser: expects the exact structure of the accepted policy
    # file — top-level scalars, `anchors:` list, `aliases:` list.
    doc: dict[str, Any] = {"anchors": [], "aliases": []}
    section = None
    current: dict[str, Any] | None = None
    for raw in text.splitlines():
        stripped = raw.rstrip()
        if not stripped or stripped.lstrip().startswith("#"):
            continue
        if stripped.endswith(":") and not stripped.startswith(" "):
            section = stripped[:-1].strip()
            if section in ("anchors", "aliases"):
                current = None
            continue
        if section is None:
            if ":" in stripped:
                key, _, val = stripped.partition(":")
                doc[key.strip()] = val.strip().strip('"')
            continue
        # inside anchors: or aliases:
        indent = len(stripped) - len(stripped.lstrip(" "))
        content = stripped.lstrip(" ")
        if content.startswith("- "):
            current = {}
            doc[section].append(current)
            content = content[2:]
        if ":" in content and current is not None:
            key, _, val = content.partition(":")
            current[key.strip()] = val.strip().strip('"')
    return doc


def load_policy(path: Path) -> Policy:
    raw = _load_yaml(path)
    return Policy(
        release_id=raw["release_id"],
        first_seen_source_release=raw["first_seen_source_release"],
        source_system=raw["source_system"],
        namespace=raw["namespace"],
        anchors=tuple(
            AnchorSpec(**a) for a in raw.get("anchors", [])
        ),
        aliases=tuple(
            AliasSpec(**a) for a in raw.get("aliases", [])
        ),
    )


def load_nortaxa_index() -> dict[str, dict[str, str]]:
    if not NORTAXA_ARCHIVE.is_file():
        raise SystemExit(f"NorTaxa archive missing: {NORTAXA_ARCHIVE}")
    csv.field_size_limit(sys.maxsize)
    idx: dict[str, dict[str, str]] = {}
    with zipfile.ZipFile(NORTAXA_ARCHIVE) as z:
        taxon_file = next(
            (n for n in z.namelist() if n.lower().endswith("taxon.txt")),
            None,
        )
        if taxon_file is None:
            raise SystemExit("archive contains no taxon.txt")
        with z.open(taxon_file) as f:
            reader = csv.DictReader(
                (line.decode("utf-8", errors="replace") for line in f),
                delimiter="\t",
            )
            for row in reader:
                tid = row.get("taxonID")
                if tid:
                    idx[str(tid)] = {
                        "scientificName": row.get("scientificName") or "",
                        "kingdom": row.get("kingdom") or "",
                        "taxonRank": row.get("taxonRank") or "",
                        "taxonomicStatus": row.get("taxonomicStatus") or "",
                        "acceptedNameUsageID": row.get("acceptedNameUsageID") or "",
                    }
    return idx


def _verify_against_archive(
    policy: Policy, archive: dict[str, dict[str, str]]
) -> None:
    """Refuse any policy entry not exactly evidenced by the archive."""
    anchor_ids = {a.taxon_id for a in policy.anchors}
    for anchor in policy.anchors:
        row = archive.get(anchor.taxon_id)
        if row is None:
            raise SystemExit(
                f"anchor {anchor.taxon_id} not in NorTaxa 1.284 archive; refusing"
            )
        if row["scientificName"] != anchor.scientific_name:
            raise SystemExit(
                f"anchor {anchor.taxon_id}: scientificName mismatch — "
                f"policy={anchor.scientific_name!r} vs archive={row['scientificName']!r}"
            )
        if row["kingdom"] != anchor.kingdom:
            raise SystemExit(
                f"anchor {anchor.taxon_id}: kingdom mismatch — "
                f"policy={anchor.kingdom!r} vs archive={row['kingdom']!r}"
            )
        if row["taxonRank"] != anchor.rank:
            raise SystemExit(
                f"anchor {anchor.taxon_id}: rank mismatch — "
                f"policy={anchor.rank!r} vs archive={row['taxonRank']!r}"
            )
        if row["taxonomicStatus"] not in ("valid", "accepted"):
            raise SystemExit(
                f"anchor {anchor.taxon_id}: taxonomicStatus="
                f"{row['taxonomicStatus']!r} is not 'valid'; refusing"
            )
        if row["acceptedNameUsageID"] and row["acceptedNameUsageID"] != anchor.taxon_id:
            raise SystemExit(
                f"anchor {anchor.taxon_id} is not self-accepted "
                f"(acceptedNameUsageID={row['acceptedNameUsageID']}); "
                f"refusing to allocate as anchor"
            )
    for alias in policy.aliases:
        row = archive.get(alias.taxon_id)
        if row is None:
            raise SystemExit(
                f"alias {alias.taxon_id} not in NorTaxa 1.284 archive"
            )
        if row["taxonomicStatus"] != "synonym":
            raise SystemExit(
                f"alias {alias.taxon_id}: taxonomicStatus="
                f"{row['taxonomicStatus']!r} is not 'synonym'; refusing"
            )
        if row["acceptedNameUsageID"] != alias.accepted_taxon_id:
            raise SystemExit(
                f"alias {alias.taxon_id}: acceptedNameUsageID mismatch — "
                f"policy={alias.accepted_taxon_id!r} vs archive={row['acceptedNameUsageID']!r}"
            )
        if alias.accepted_taxon_id not in anchor_ids:
            raise SystemExit(
                f"alias {alias.taxon_id} points to accepted_taxon_id="
                f"{alias.accepted_taxon_id} which is not declared as an anchor "
                f"in this policy; refusing"
            )


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def run(policy_path: Path, existing_registry: Path, output_dir: Path) -> None:
    if output_dir.exists():
        raise SystemExit(f"output directory already exists: {output_dir}; refusing")

    policy = load_policy(policy_path)
    archive = load_nortaxa_index()
    _verify_against_archive(policy, archive)

    # Load the existing shard-based registry into an in-memory single-file
    # working registry so we can allocate without touching the shard bytes.
    tmp_flat = output_dir.parent / (
        f".{output_dir.name}.working-registry.jsonl"
    )
    tmp_flat.parent.mkdir(parents=True, exist_ok=True)
    if tmp_flat.exists():
        tmp_flat.unlink()

    working = IdentityRegistry(tmp_flat)
    working.load()

    src = IdentityRegistry(existing_registry)
    src.load()
    for entry in src.all_entries():
        working._by_key[entry.key()] = entry  # noqa: SLF001 — controlled construction
        if entry.kind == "anchor":
            working._anchors[entry.sporely_taxon_id] = entry  # noqa: SLF001
            working._next_id = max(working._next_id, entry.sporely_taxon_id + 1)
        else:
            working._aliases.append(entry)  # noqa: SLF001

    baseline_next_id = working._next_id  # noqa: SLF001

    # Allocate anchors (order sorted by taxon_id for determinism).
    newly_created: list = []
    for anchor in sorted(policy.anchors, key=lambda a: int(a.taxon_id)):
        allocation = working.allocate(
            source=policy.source_system,
            namespace=policy.namespace,
            identifier=anchor.taxon_id,
            allocated_in_release=policy.release_id,
            first_seen_source_release=policy.first_seen_source_release,
        )
        if allocation.allocated_in_release == policy.release_id:
            newly_created.append(allocation)

    for alias in sorted(policy.aliases, key=lambda a: int(a.taxon_id)):
        anchor_alloc = working._by_key.get(  # noqa: SLF001
            (policy.source_system, policy.namespace, alias.accepted_taxon_id)
        )
        if anchor_alloc is None:
            raise SystemExit(
                f"internal error: accepted target {alias.accepted_taxon_id} "
                "not allocated"
            )
        binding = working.bind_alias(
            existing_sporely_taxon_id=anchor_alloc.sporely_taxon_id,
            source=policy.source_system,
            namespace=policy.namespace,
            identifier=alias.taxon_id,
            allocated_in_release=policy.release_id,
            first_seen_source_release=policy.first_seen_source_release,
        )
        if binding.allocated_in_release == policy.release_id:
            newly_created.append(binding)

    # Emit ONLY the newly allocated lines into a supplement shard.
    canonical_dir = output_dir / "canonical"
    canonical_dir.mkdir(parents=True, exist_ok=True)
    shard = canonical_dir / "part-0001.jsonl"

    header = {
        REGISTRY_HEADER_KEY: True,
        "registry_schema_version": REGISTRY_SCHEMA_VERSION,
        "description": "W2E-A2 observation-derived NorTaxa anchors (supplement)",
    }
    with shard.open("w", encoding="utf-8") as f:
        f.write(json.dumps(header, sort_keys=True, ensure_ascii=False) + "\n")
        for entry in sorted(newly_created, key=lambda a: (a.sporely_taxon_id, a.kind)):
            f.write(entry.to_json_line() + "\n")
    tmp_flat.unlink(missing_ok=True)

    # Build a manifest for the supplement.
    shard_bytes = shard.stat().st_size
    shard_sha = _sha256_file(shard)
    shard_line_count = sum(1 for _ in shard.open("r", encoding="utf-8"))
    concatenated_sha = hashlib.sha256(shard.read_bytes()).hexdigest()
    manifest = {
        "concatenated_sha256": concatenated_sha,
        "manifest_schema_version": 1,
        "registry_schema_version": REGISTRY_SCHEMA_VERSION,
        "shard_bytes_target": shard_bytes,
        "shards": [
            {
                "bytes": shard_bytes,
                "line_count": shard_line_count,
                "name": shard.name,
                "sha256": shard_sha,
            }
        ],
        "total_bytes": shard_bytes,
        "total_line_count": shard_line_count,
    }
    (canonical_dir / "manifest.json").write_text(
        json.dumps(manifest, sort_keys=True, ensure_ascii=False, indent=2) + "\n"
    )

    # Release-side supplement: (source, namespace, external_id) → sporely_taxon_id
    release_dir = output_dir / "release"
    release_dir.mkdir(parents=True, exist_ok=True)
    ext_id = release_dir / "taxon_external_id_supplement.jsonl"
    mappings = []
    id_to_name = {a.taxon_id: a.scientific_name for a in policy.anchors}
    id_to_name.update({a.taxon_id: a.scientific_name for a in policy.aliases})
    id_to_role = {a.taxon_id: "accepted" for a in policy.anchors}
    id_to_role.update({a.taxon_id: "synonym" for a in policy.aliases})
    for entry in sorted(newly_created, key=lambda a: (int(a.identifier), a.kind)):
        mappings.append(
            {
                "external_id": entry.identifier,
                "external_name": id_to_name.get(entry.identifier, ""),
                "id_role": id_to_role.get(entry.identifier, entry.kind),
                "is_preferred": entry.kind == "anchor",
                "namespace": entry.namespace,
                "note": None,
                "source_system": entry.source,
                "sporely_taxon_id": entry.sporely_taxon_id,
            }
        )
    with ext_id.open("w", encoding="utf-8") as f:
        for m in mappings:
            f.write(json.dumps(m, sort_keys=True, ensure_ascii=False) + "\n")
    ext_sha = _sha256_file(ext_id)
    (release_dir / "taxon_external_id_supplement.sha256.txt").write_text(ext_sha + "\n")

    provenance = {
        "release_id": policy.release_id,
        "based_on_registry_concatenated_sha256_before": src.path.exists()
        and hashlib.sha256((existing_registry / "manifest.json").read_bytes()).hexdigest()
        or None,
        "diagnostic_reference": "W2D-R mapping-diagnostic 2026-08-01 (buckets: sqlite_present_no_registry_anchor, source_archive_only_no_registry_anchor)",
        "nortaxa_archive_sha256": _sha256_file(NORTAXA_ARCHIVE),
        "nortaxa_release": policy.first_seen_source_release,
        "policy_sha256": _sha256_file(policy_path),
        "sporely_taxon_id_range": {
            "first_allocated": baseline_next_id,
            "last_allocated": (working._next_id - 1),  # noqa: SLF001
            "count_allocated": len(newly_created),
        },
        "supplement_registry_manifest_sha256": _sha256_file(
            canonical_dir / "manifest.json"
        ),
        "supplement_shard_sha256": shard_sha,
        "supplement_external_id_sha256": ext_sha,
        "mappings": mappings,
        "safety": {
            "production_access": False,
            "search_cache_broadened": False,
            "new_upstream_download": False,
            "old_release_mutation": False,
            "ad_hoc_anchors": False,
            "name_only_resolution": False,
        },
    }
    (release_dir / "observation-supplement-release.json").write_text(
        json.dumps(provenance, sort_keys=True, ensure_ascii=False, indent=2) + "\n"
    )


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument(
        "--existing-registry",
        type=Path,
        required=True,
        help="path to the read-only canonical registry directory (shards)",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--production",
        action="store_true",
        help="refused; kept only for explicit rejection",
    )
    args = parser.parse_args(argv)
    if args.production:
        print("refuse: --production is not honoured", file=sys.stderr)
        return 3
    run(args.policy, args.existing_registry, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
