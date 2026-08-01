#!/usr/bin/env python3
"""W2E-B: allocate registry anchors from recovered external-source evidence.

Input: a JSON policy file listing each unresolved external identifier together
with its classification, acquisition provenance (endpoint, date, raw-response
SHA-256) and — for exact_accepted_mapping or exact_synonym_replacement_mapping —
the source's own accepted name and rank. Classifications:

    exact_accepted_mapping                → allocate anchor at (source, namespace, external_id)
    exact_synonym_replacement_mapping     → bind alias at (source, namespace, external_id)
                                             to accepted_external_id (allocated first if new)
    deleted_or_unavailable                → recorded only, no allocation
    ambiguous / conflict / no_col_link    → recorded only, no allocation

Guardrails:
* refuses --production;
* refuses to overwrite an existing output directory;
* uses IdentityRegistry.allocate() / bind_alias() verbatim;
* refuses to allocate through scientific-name equality alone;
* records per-entry acquisition provenance (endpoint, date, response hash) in
  the emitted release JSON.

The output is a NEW immutable registry supplement (tax-2026.08.03-01 by
default) that depends on the base release AND any prior supplements
declared via --depends-on-supplement. Prior supplements' identity records
are never modified.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from database.taxonomy.scripts.identity_registry import (  # noqa: E402
    IdentityRegistry,
    REGISTRY_SCHEMA_VERSION,
    REGISTRY_HEADER_KEY,
)
from database.taxonomy.scripts.allocate_observation_derived_anchors import (  # noqa: E402
    SUPPLEMENT_CONTRACT_VERSION,
    _sha256_file,
    _supplement_contract,
)


ALLOCATING_CLASSIFICATIONS = frozenset(
    {"exact_accepted_mapping", "exact_synonym_replacement_mapping"}
)


def run(
    policy_path: Path,
    existing_registry: Path,
    output_dir: Path,
    base_release_dir: Path,
    depends_on_supplements: list[Path],
) -> None:
    if output_dir.exists():
        raise SystemExit(f"output directory already exists: {output_dir}; refusing")

    policy = json.loads(policy_path.read_text(encoding="utf-8"))
    release_id = policy["release_id"]

    # Load base registry (read-only) into an in-memory working copy so we can
    # allocate without touching the shard bytes. Also load each depends-on
    # supplement's registry shard into the same working copy.
    tmp_flat = output_dir.parent / f".{output_dir.name}.working-registry.jsonl"
    tmp_flat.parent.mkdir(parents=True, exist_ok=True)
    if tmp_flat.exists():
        tmp_flat.unlink()

    working = IdentityRegistry(tmp_flat)
    working.load()

    def _absorb(registry_dir: Path) -> None:
        r = IdentityRegistry(registry_dir)
        r.load()
        for e in r.all_entries():
            working._by_key[e.key()] = e  # noqa: SLF001
            if e.kind == "anchor":
                working._anchors[e.sporely_taxon_id] = e  # noqa: SLF001
                working._next_id = max(  # noqa: SLF001
                    working._next_id,  # noqa: SLF001
                    e.sporely_taxon_id + 1,
                )
            else:
                working._aliases.append(e)  # noqa: SLF001

    _absorb(existing_registry)
    for dep in depends_on_supplements:
        _absorb(dep / "canonical")

    baseline_next_id = working._next_id  # noqa: SLF001

    # Deterministic ordering: process entries alphabetically by
    # (source_system, namespace, external_id). This is also the order the
    # policy already produces.
    entries = sorted(
        policy["entries"],
        key=lambda e: (e["source_system"], e["namespace"], e["external_id"]),
    )

    newly_created: list = []
    per_entry_outcomes: list[dict] = []

    for e in entries:
        cls = e["classification"]
        source = e["source_system"]
        namespace = e["namespace"]
        ext_id = str(e["external_id"])
        outcome: dict = {
            "source_system": source,
            "namespace": namespace,
            "external_id": ext_id,
            "classification": cls,
            "acquisition": e["acquisition"],
        }

        if cls == "exact_accepted_mapping":
            if "scientific_name" not in e or not e["scientific_name"]:
                raise SystemExit(
                    f"entry {source}:{namespace}:{ext_id} missing "
                    "scientific_name; identity may not be created from "
                    "name-only evidence — but neither may it be allocated "
                    "without recording the source's accepted name"
                )
            alloc = working.allocate(
                source=source,
                namespace=namespace,
                identifier=ext_id,
                allocated_in_release=release_id,
                first_seen_source_release=(
                    e["acquisition"]["endpoint"]
                    + "?on="
                    + e["acquisition"]["date"]
                ),
            )
            if alloc.allocated_in_release == release_id:
                newly_created.append(alloc)
                outcome["result"] = "anchor_allocated"
            else:
                outcome["result"] = "anchor_already_existed"
            outcome["sporely_taxon_id"] = alloc.sporely_taxon_id
            outcome["accepted_scientific_name"] = e["scientific_name"]
            outcome["accepted_rank"] = e.get("rank")

        elif cls == "exact_synonym_replacement_mapping":
            accepted_ext_id = str(e.get("accepted_external_id") or "")
            accepted_namespace = e.get("accepted_namespace") or namespace
            if not accepted_ext_id:
                raise SystemExit(
                    f"entry {source}:{namespace}:{ext_id} missing "
                    "accepted_external_id for synonym/replacement"
                )
            # Ensure the accepted target has an anchor. If not present,
            # allocate one (still source-backed since the API response
            # was the acquisition evidence).
            accepted_alloc = working._by_key.get(  # noqa: SLF001
                (source, accepted_namespace, accepted_ext_id)
            )
            if accepted_alloc is None:
                accepted_alloc = working.allocate(
                    source=source,
                    namespace=accepted_namespace,
                    identifier=accepted_ext_id,
                    allocated_in_release=release_id,
                    first_seen_source_release=(
                        e["acquisition"]["endpoint"]
                        + "?on="
                        + e["acquisition"]["date"]
                    ),
                )
                if accepted_alloc.allocated_in_release == release_id:
                    newly_created.append(accepted_alloc)

            alias = working.bind_alias(
                existing_sporely_taxon_id=accepted_alloc.sporely_taxon_id,
                source=source,
                namespace=namespace,
                identifier=ext_id,
                allocated_in_release=release_id,
                first_seen_source_release=(
                    e["acquisition"]["endpoint"]
                    + "?on="
                    + e["acquisition"]["date"]
                ),
            )
            if alias.allocated_in_release == release_id:
                newly_created.append(alias)
                outcome["result"] = "alias_bound_to_existing_anchor" if accepted_alloc.allocated_in_release != release_id else "alias_bound_to_newly_allocated_anchor"
            else:
                outcome["result"] = "alias_already_existed"
            outcome["sporely_taxon_id"] = alias.sporely_taxon_id
            outcome["accepted_external_id"] = accepted_ext_id
            outcome["accepted_scientific_name"] = e.get("scientific_name")

        else:
            outcome["result"] = "recorded_no_allocation"

        per_entry_outcomes.append(outcome)

    # Emit the supplement shard containing ONLY entries allocated in this run.
    canonical_dir = output_dir / "canonical"
    canonical_dir.mkdir(parents=True, exist_ok=True)
    shard = canonical_dir / "part-0001.jsonl"
    header = {
        REGISTRY_HEADER_KEY: True,
        "registry_schema_version": REGISTRY_SCHEMA_VERSION,
        "description": f"W2E-B recovered-external-source anchors ({release_id})",
    }
    with shard.open("w", encoding="utf-8") as f:
        f.write(json.dumps(header, sort_keys=True, ensure_ascii=False) + "\n")
        for entry in sorted(newly_created, key=lambda a: (a.sporely_taxon_id, 0 if a.kind == "anchor" else 1)):
            f.write(entry.to_json_line() + "\n")
    tmp_flat.unlink(missing_ok=True)

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

    # Release-side supplement: external mappings for both anchors and aliases.
    release_dir = output_dir / "release"
    release_dir.mkdir(parents=True, exist_ok=True)
    ext_id_file = release_dir / "taxon_external_id_supplement.jsonl"
    mappings = []
    for entry in sorted(newly_created, key=lambda a: (int(a.identifier), a.kind)):
        # Look up the accepted name recorded per outcome for this identifier
        name = ""
        for out in per_entry_outcomes:
            if (
                out["source_system"] == entry.source
                and out["namespace"] == entry.namespace
                and out["external_id"] == entry.identifier
            ):
                name = out.get("accepted_scientific_name") or ""
                break
        mappings.append(
            {
                "external_id": entry.identifier,
                "external_name": name,
                "id_role": "accepted" if entry.kind == "anchor" else "synonym",
                "is_preferred": entry.kind == "anchor",
                "namespace": entry.namespace,
                "note": None,
                "source_system": entry.source,
                "sporely_taxon_id": entry.sporely_taxon_id,
            }
        )
    with ext_id_file.open("w", encoding="utf-8") as f:
        for m in mappings:
            f.write(json.dumps(m, sort_keys=True, ensure_ascii=False) + "\n")
    ext_sha = _sha256_file(ext_id_file)
    (release_dir / "taxon_external_id_supplement.sha256.txt").write_text(ext_sha + "\n")

    # Compose the supplement contract using the shared W2E-A2 helper.
    class _PseudoPolicy:
        release_id = policy["release_id"]

    depends_on = []
    for dep in depends_on_supplements:
        dep_release_json = json.loads(
            (dep / "release/observation-supplement-release.json").read_text()
        )
        depends_on.append(
            {
                "supplement_release_id": dep_release_json["supplement_release_id"],
                "supplement_registry_manifest_sha256": dep_release_json[
                    "supplement_registry_manifest_sha256"
                ],
                "supplement_shard_sha256": dep_release_json["supplement_shard_sha256"],
                "supplement_external_id_sha256": dep_release_json[
                    "supplement_external_id_sha256"
                ],
            }
        )

    def _base_release_dep(base: Path) -> dict:
        export_manifest = base / "taxonomy_export_manifest.json"
        scope_manifest = base / "scope-manifest.json"
        manifest_doc = json.loads(export_manifest.read_text())
        return {
            "base_release_id": manifest_doc.get("release_id"),
            "base_release_export_manifest_sha256": _sha256_file(export_manifest),
            "base_release_scope_manifest_sha256": manifest_doc.get(
                "scope_manifest_sha256"
            )
            or _sha256_file(scope_manifest),
        }

    base_dep = _base_release_dep(base_release_dir)
    contract = {
        "artifact_kind": "registry_supplement",
        "supplement_contract_version": SUPPLEMENT_CONTRACT_VERSION,
        "supplement_release_id": release_id,
        "base_release_id": base_dep["base_release_id"],
        "base_release_dependency": base_dep,
        "depends_on": depends_on,
        "required_application_order": [
            "load base release identity records first (see base_release_id)",
            "load canonical registry (base) with anchors and aliases in append-only order",
            "load any depends_on supplements in the order listed above",
            "load THIS supplement's canonical registry shards last",
        ],
        "compatibility_rules": [
            "MUST NOT be loaded as a standalone taxonomy/search release",
            "MUST be loaded strictly on top of base_release_id and every entry in depends_on",
            "MUST NOT be applied to a base whose export_manifest_sha256 or scope_manifest_sha256 differs from base_release_dependency",
            "MUST NOT broaden the search cache — supplement mappings materialise into the sparse persistent registry with cache_state=out_of_cache",
            "MUST NOT mutate base release bytes or any previously emitted supplement's identity records",
        ],
        "diagnostic_reference": "W2E-B unresolved-after-W2E-A2 (22 IDs: 19 iNaturalist + 3 NorTaxa)",
    }
    provenance = {
        **contract,
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
        "classification_counts": {
            k: sum(1 for e in policy["entries"] if e["classification"] == k)
            for k in sorted({e["classification"] for e in policy["entries"]})
        },
        "outcomes": per_entry_outcomes,
        "mappings": mappings,
        "safety": {
            "production_access": False,
            "production_writes": False,
            "search_cache_broadened": False,
            "old_release_mutation": False,
            "ad_hoc_anchors": False,
            "name_only_resolution": False,
        },
    }
    (release_dir / "recovered-supplement-release.json").write_text(
        json.dumps(provenance, sort_keys=True, ensure_ascii=False, indent=2) + "\n"
    )


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--existing-registry", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--base-release-dir", type=Path, required=True)
    parser.add_argument(
        "--depends-on-supplement",
        type=Path,
        action="append",
        default=[],
    )
    parser.add_argument("--production", action="store_true")
    args = parser.parse_args(argv)
    if args.production:
        print("refuse: --production is not honoured", file=sys.stderr)
        return 3
    run(
        args.policy,
        args.existing_registry,
        args.output,
        args.base_release_dir,
        list(args.depends_on_supplement),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
