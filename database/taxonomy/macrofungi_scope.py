#!/usr/bin/env python3
"""Build a deterministic global-macrofungi scope from the pinned taxonomy-v2 SQLite.

This module is deliberately separate from the accepted broad W1 compiler.  It
does not mutate the pinned source, identity registry, observations, runtime
activation, or the historical W1 export.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import shutil
import sqlite3
import tempfile
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


POLICY_FORMAT = "sporely-global-macrofungi-policy-v1"
SCOPE_SCHEMA_VERSION = 1
RANK_PRECEDENCE = {
    "species": 0,
    "genus": 1,
    "family": 2,
    "order": 3,
    "class": 4,
    "subphylum": 5,
}
VALID_STATES = {"include", "exclude", "review", "not_evaluated"}
CHUNK = 1024 * 1024


class ScopeError(ValueError):
    pass


@dataclass(frozen=True)
class Taxon:
    taxon_id: int
    col_id: str
    name: str
    rank: str
    parent_id: int | None
    status: str


@dataclass(frozen=True)
class Rule:
    code: str
    col_id: str
    name: str
    rank: str
    state: str
    reason: str
    review_status: str
    evidence: str
    searchable_synonyms: tuple[dict[str, Any], ...]
    taxon_id: int


def canonical_json_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(CHUNK), b""):
            digest.update(block)
    return digest.hexdigest()


def load_policy(path: Path) -> dict[str, Any]:
    try:
        policy = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ScopeError(f"invalid policy {path}: {exc}") from exc
    if policy.get("format") != POLICY_FORMAT:
        raise ScopeError(f"unsupported policy format: {policy.get('format')}")
    if policy.get("default_state") != "exclude":
        raise ScopeError("macrofungi policy must default exclude")
    if policy.get("precedence") != list(RANK_PRECEDENCE):
        raise ScopeError("policy precedence does not match executable contract")
    return policy


def _source_connection(source_gz: Path, temp_dir: Path) -> tuple[sqlite3.Connection, Path]:
    source_sqlite = temp_dir / "source.sqlite3"
    with gzip.open(source_gz, "rb") as source, source_sqlite.open("wb") as target:
        shutil.copyfileobj(source, target, CHUNK)
    connection = sqlite3.connect(source_sqlite)
    connection.execute("pragma query_only=on")
    return connection, source_sqlite


def load_taxa(connection: sqlite3.Connection) -> tuple[dict[int, Taxon], dict[str, Taxon]]:
    taxa: dict[int, Taxon] = {}
    by_col: dict[str, Taxon] = {}
    rows = connection.execute(
        """select taxon_id, canonical_external_id, canonical_scientific_name,
                  taxon_rank, parent_taxon_id, taxonomic_status
             from taxon_min
            where source_system='col_xr'
            order by taxon_id"""
    )
    for row in rows:
        taxon = Taxon(int(row[0]), str(row[1]), str(row[2]), str(row[3]), row[4], str(row[5]))
        if taxon.col_id in by_col:
            raise ScopeError(f"duplicate COL concept identifier: {taxon.col_id}")
        taxa[taxon.taxon_id] = taxon
        by_col[taxon.col_id] = taxon
    return taxa, by_col


def resolve_rules(policy: dict[str, Any], by_col: dict[str, Taxon]) -> list[Rule]:
    rules: list[Rule] = []
    codes: set[str] = set()
    occupied: dict[tuple[int, str], str] = {}
    for raw in policy.get("rules", []):
        code = str(raw.get("code", ""))
        col_id = str(raw.get("col_concept_id", ""))
        if not code or code in codes:
            raise ScopeError(f"duplicate or empty rule code: {code}")
        codes.add(code)
        taxon = by_col.get(col_id)
        if taxon is None:
            raise ScopeError(f"unresolved COL concept for {code}: {col_id}")
        rank = str(raw.get("rank"))
        state = str(raw.get("state"))
        if rank not in RANK_PRECEDENCE:
            raise ScopeError(f"unsupported rule rank for {code}: {rank}")
        if state not in VALID_STATES:
            raise ScopeError(f"invalid rule state for {code}: {state}")
        if taxon.rank != rank or taxon.name != raw.get("name"):
            raise ScopeError(
                f"policy/source mismatch for {code}: expected {raw.get('name')} {rank}, "
                f"got {taxon.name} {taxon.rank}"
            )
        conflict_key = (taxon.taxon_id, rank)
        if conflict_key in occupied:
            raise ScopeError(f"conflicting same-rank rules: {occupied[conflict_key]} and {code}")
        occupied[conflict_key] = code
        rules.append(
            Rule(
                code=code,
                col_id=col_id,
                name=taxon.name,
                rank=rank,
                state=state,
                reason=str(raw.get("reason")),
                review_status=str(raw.get("review_status")),
                evidence=str(raw.get("evidence")),
                searchable_synonyms=tuple(raw.get("searchable_synonyms", [])),
                taxon_id=taxon.taxon_id,
            )
        )
    return rules


def evaluate(
    taxa: dict[int, Taxon],
    rules: list[Rule],
    source_exclusions: list[dict[str, Any]] | None = None,
) -> dict[int, dict[str, Any]]:
    rules_by_taxon = {rule.taxon_id: rule for rule in rules}
    memo: dict[int, tuple[int, ...]] = {}

    def ancestry(taxon_id: int) -> tuple[int, ...]:
        if taxon_id in memo:
            return memo[taxon_id]
        seen: set[int] = set()
        chain: list[int] = []
        current = taxon_id
        while current in taxa and current not in seen:
            seen.add(current)
            chain.append(current)
            parent = taxa[current].parent_id
            if parent is None:
                break
            current = parent
        memo[taxon_id] = tuple(chain)
        return memo[taxon_id]

    results: dict[int, dict[str, Any]] = {}
    for taxon_id in sorted(taxa):
        taxon = taxa[taxon_id]
        matching_source_exclusions = []
        for exclusion in source_exclusions or []:
            criteria = exclusion.get("criteria", {})
            if criteria.get("rank") != taxon.rank:
                continue
            if not taxon.col_id.startswith(str(criteria.get("col_id_prefix", ""))):
                continue
            if criteria.get("name_equals_col_id") and taxon.name != taxon.col_id:
                continue
            matching_source_exclusions.append(exclusion)
        if len(matching_source_exclusions) > 1:
            raise ScopeError(f"conflicting source-characteristic exclusions for {taxon.col_id}")
        if matching_source_exclusions:
            exclusion = matching_source_exclusions[0]
            results[taxon_id] = {
                "state": "exclude",
                "reason": exclusion["reason"],
                "rule": exclusion["code"],
                "rule_rank": "source_characteristic",
                "inherited_from_col_id": taxon.col_id,
                "review_status": exclusion["review_status"],
            }
            continue
        applicable = [rules_by_taxon[item] for item in ancestry(taxon_id) if item in rules_by_taxon]
        if applicable:
            applicable.sort(key=lambda item: (RANK_PRECEDENCE[item.rank], item.code))
            winner = applicable[0]
            results[taxon_id] = {
                "state": winner.state,
                "reason": winner.reason,
                "rule": winner.code,
                "rule_rank": winner.rank,
                "inherited_from_col_id": winner.col_id,
                "review_status": winner.review_status,
            }
        else:
            results[taxon_id] = {
                "state": "exclude",
                "reason": "default_exclude",
                "rule": "default_exclude",
                "rule_rank": None,
                "inherited_from_col_id": None,
                "review_status": "approved",
            }
    return results


def descendants_summary(taxa: dict[int, Taxon], results: dict[int, dict[str, Any]], rules: list[Rule]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for rule in rules:
        won = [taxa[taxon_id] for taxon_id, result in results.items() if result["rule"] == rule.code]
        summary[rule.code] = {
            "col_concept_id": rule.col_id,
            "name": rule.name,
            "rank": rule.rank,
            "state": rule.state,
            "reason": rule.reason,
            "concept_count": len(won),
            "species_count": sum(item.rank == "species" for item in won),
            "genus_count": sum(item.rank == "genus" for item in won),
        }
    return summary


def validate_selectable_fungi(taxa: dict[int, Taxon], results: dict[int, dict[str, Any]]) -> dict[str, int]:
    fungi = next((item for item in taxa.values() if item.col_id == "F" and item.rank == "kingdom"), None)
    if fungi is None:
        raise ScopeError("pinned COL Fungi concept F is missing")
    selectable = {taxon_id for taxon_id, result in results.items() if result["state"] == "include"}
    non_fungi: list[int] = []
    for taxon_id in sorted(selectable):
        current = taxon_id
        seen: set[int] = set()
        while current in taxa and current not in seen and current != fungi.taxon_id:
            seen.add(current)
            current = taxa[current].parent_id or -1
        if current != fungi.taxon_id:
            non_fungi.append(taxon_id)
    if non_fungi:
        raise ScopeError(f"selectable non-Fungi concepts present: {non_fungi[:10]}")
    return {"plants": 0, "animals": 0, "non_fungi": 0}


def historical_compatibility(path: Path | None) -> dict[str, Any]:
    empty = {
        "observations_total": 0,
        "inside_new_macrofungi_scope": 0,
        "outside_scope_but_historically_required": 0,
        "unresolved_legacy_identity": 0,
        "manual_name_without_resolved_identity": 0,
        "no_identity_evidence": 0,
        "representative_unresolved_legacy": [],
        "representative_manual": [],
    }
    if path is None or not path.is_file():
        return empty
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    columns = {row[1] for row in connection.execute("pragma table_info(observations)")}
    required = {"sporely_taxon_id", "genus", "species", "artsdata_id", "inaturalist_taxon_id", "ai_selected_taxon_id", "ai_selected_scientific_name"}
    if not required <= columns:
        connection.close()
        raise ScopeError(f"historical observations schema lacks: {sorted(required - columns)}")
    external = "coalesce(nullif(trim(cast(artsdata_id as text)),''),nullif(trim(cast(inaturalist_taxon_id as text)),''),nullif(trim(cast(ai_selected_taxon_id as text)),'')) is not null"
    manual = "coalesce(nullif(trim(genus),''),nullif(trim(species),''),nullif(trim(ai_selected_scientific_name),'')) is not null"
    output = dict(empty)
    output["observations_total"] = connection.execute("select count(*) from observations").fetchone()[0]
    # No observation currently has a stable identity; do not infer one by name.
    stable = connection.execute("select count(*) from observations where sporely_taxon_id is not null").fetchone()[0]
    if stable:
        raise ScopeError("historical audit requires explicit stable-ID classification support")
    output["unresolved_legacy_identity"] = connection.execute(f"select count(*) from observations where sporely_taxon_id is null and {external}").fetchone()[0]
    output["manual_name_without_resolved_identity"] = connection.execute(f"select count(*) from observations where sporely_taxon_id is null and not ({external}) and {manual}").fetchone()[0]
    output["no_identity_evidence"] = connection.execute(f"select count(*) from observations where sporely_taxon_id is null and not ({external}) and not ({manual})").fetchone()[0]
    output["representative_unresolved_legacy"] = [
        {"observation_id": row[0], "genus": row[1], "species": row[2], "source": row[3], "external_id": row[4]}
        for row in connection.execute(f"select id,genus,species,ai_selected_service,ai_selected_taxon_id from observations where {external} order by id limit 5")
    ]
    output["representative_manual"] = [
        {"observation_id": row[0], "genus": row[1], "species": row[2], "selected_name": row[3]}
        for row in connection.execute(f"select id,genus,species,ai_selected_scientific_name from observations where not ({external}) and {manual} order by id limit 5")
    ]
    connection.close()
    return output


def required_ancestors(taxa: dict[int, Taxon], included: set[int]) -> set[int]:
    ancestors: set[int] = set()
    for taxon_id in included:
        current = taxa[taxon_id].parent_id
        while current in taxa:
            if current in included:
                break
            ancestors.add(current)
            current = taxa[current].parent_id
    return ancestors


def descendant_ids(taxa: dict[int, Taxon], root_col_id: str) -> set[int]:
    root = next((item for item in taxa.values() if item.col_id == root_col_id), None)
    if root is None:
        raise ScopeError(f"missing root COL concept: {root_col_id}")
    children: dict[int, list[int]] = {}
    for item in taxa.values():
        if item.parent_id is not None:
            children.setdefault(item.parent_id, []).append(item.taxon_id)
    result: set[int] = set()
    stack = [root.taxon_id]
    while stack:
        current = stack.pop()
        if current in result:
            continue
        result.add(current)
        stack.extend(children.get(current, []))
    return result


def scope_audit(taxa: dict[int, Taxon], results: dict[int, dict[str, Any]]) -> dict[str, Any]:
    fungi = descendant_ids(taxa, "F")
    basidiomycota = descendant_ids(taxa, "BM")
    ascomycota = descendant_ids(taxa, "SM")
    included = {taxon_id for taxon_id, result in results.items() if result["state"] == "include"}
    return {
        "rejected_w1_col_scope": {
            "total_col_concepts": len(fungi),
            "basidiomycota": len(basidiomycota),
            "ascomycota": len(ascomycota),
            "other_fungal_phyla": len(fungi - basidiomycota - ascomycota),
            "species": sum(taxa[item].rank == "species" for item in fungi),
            "genera": sum(taxa[item].rank == "genus" for item in fungi),
            "other_ranks": sum(taxa[item].rank not in {"species", "genus"} for item in fungi),
            "nortaxa_additional_concepts": 13919,
            "total_w1_export_concepts": 634894,
            "scientific_aliases": 27755,
            "vernacular_names": 10294,
            "external_mappings": 634894,
        },
        "candidate": {
            "included_concepts": len(included),
            "included_basidiomycota": len(included & basidiomycota),
            "included_ascomycota": len(included & ascomycota),
            "included_other_fungal_phyla": len(included - basidiomycota - ascomycota),
        },
    }


def correctness_probes(by_col: dict[str, Taxon], results: dict[int, dict[str, Any]]) -> dict[str, Any]:
    expected = {
        "Amanita muscaria": ("5TYZ9", "include"),
        "Morchella esculenta": ("44CPR", "include"),
        "Tuber melanosporum": ("59HZX", "include"),
        "Geoglossum fallax": ("6KBJR", "include"),
        "Xylaria hypoxylon": ("5CH3N", "include"),
        "Gymnosporangium": ("4RXL", "include"),
        "Mycosarcoma maydis": ("B24TM", "include"),
        "Puccinia abchazica": ("78MLW", "exclude"),
        "Ustilago abaconensis": ("7F2TK", "exclude"),
        "Saccharomyces cerevisiae": ("4TWCR", "exclude"),
        "Aspergillus fumigatus": ("HC3H", "exclude"),
        "Cladonia rangiferina": ("9XK8W", "exclude"),
    }
    output: dict[str, Any] = {}
    for label, (col_id, state) in expected.items():
        taxon = by_col[col_id]
        actual = results[taxon.taxon_id]
        if actual["state"] != state:
            raise ScopeError(f"correctness probe failed for {label}: {actual['state']} != {state}")
        output[label] = {"col_concept_id": col_id, "expected": state, "actual": actual["state"], "winning_rule": actual["rule"]}
    return output


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> tuple[int, int, str]:
    count = 0
    digest = hashlib.sha256()
    with path.open("wb") as handle:
        for row in rows:
            payload = canonical_json_bytes(row)
            handle.write(payload)
            digest.update(payload)
            count += 1
    return count, path.stat().st_size, digest.hexdigest()


def _iter_jsonl(path: Path, allowed: set[int]) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for raw in handle:
            row = json.loads(raw)
            if row.get("taxon_id") in allowed:
                yield row


def build_export(
    w1_dir: Path,
    output_dir: Path,
    taxa: dict[int, Taxon],
    results: dict[int, dict[str, Any]],
    rules: list[Rule],
    source_hashes: dict[str, str],
    policy_hash: str,
    release_id: str,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=False)
    included = {taxon_id for taxon_id, result in results.items() if result["state"] == "include"}
    ancestors = required_ancestors(taxa, included)
    export_ids = included | ancestors
    files: list[dict[str, Any]] = []

    release_row = {
        "content_release_id": release_id,
        "taxonomy_schema_version": 2,
        "scope_predicate_id": "global_macrofungi_policy_v1",
        "source_release": "2026-07-17-XR",
        "source_dataset_key": 315834,
        "source_gz_sha256": source_hashes["sqlite_gz_sha256"],
        "policy_sha256": policy_hash,
    }
    count, size, digest = _write_jsonl(output_dir / "taxonomy_release.jsonl", [release_row])
    files.append({"name": "taxonomy_release.jsonl", "row_count": count, "bytes": size, "sha256": digest})

    taxon_rows = []
    for row in _iter_jsonl(w1_dir / "taxon.jsonl", export_ids):
        taxon_id = row["taxon_id"]
        row["sporely_content_release_id"] = release_id
        row["scope_state"] = "include" if taxon_id in included else "required_ancestor"
        row["scope_reason"] = results[taxon_id]["reason"] if taxon_id in included else "classification_ancestor"
        row["scope_rule"] = results[taxon_id]["rule"] if taxon_id in included else None
        taxon_rows.append(row)
    count, size, digest = _write_jsonl(output_dir / "taxon.jsonl", taxon_rows)
    files.append({"name": "taxon.jsonl", "row_count": count, "bytes": size, "sha256": digest})

    scientific_rows = list(_iter_jsonl(w1_dir / "scientific_name.jsonl", included))
    for rule in rules:
        for synonym in rule.searchable_synonyms:
            scientific_rows.append({
                "is_preferred_name": False,
                "language_code": "sci",
                "note": f"pinned_col_synonym_usage:{','.join(synonym['col_name_usage_ids'])}",
                "scientific_name": synonym["name"],
                "source": "col_xr",
                "taxon_id": rule.taxon_id,
            })
    scientific_rows.sort(key=lambda row: (row["taxon_id"], row["scientific_name"], row["language_code"], row["source"], row.get("note") or ""))
    for filename in ("scientific_name.jsonl", "vernacular.jsonl", "taxon_external_id.jsonl", "taxon_redlist.jsonl"):
        if filename == "scientific_name.jsonl":
            rows = scientific_rows
        else:
            rows = _iter_jsonl(w1_dir / filename, included)
        count, size, digest = _write_jsonl(output_dir / filename, rows)
        files.append({"name": filename, "row_count": count, "bytes": size, "sha256": digest})
    count, size, digest = _write_jsonl(output_dir / "taxon_external_id_legacy_integer.jsonl", [])
    files.insert(-1, {"name": "taxon_external_id_legacy_integer.jsonl", "row_count": count, "bytes": size, "sha256": digest})

    semantic = {
        "scope_schema_version": SCOPE_SCHEMA_VERSION,
        "release_id": release_id,
        "pinned_col_release": "2026-07-17-XR",
        "source_hashes": source_hashes,
        "policy_sha256": policy_hash,
        "resolved_policy_concept_ids": [rule.col_id for rule in rules],
        "included_taxon_ids": sorted(included),
        "required_ancestor_ids": sorted(ancestors),
        "winning_rule_by_taxon": [
            {"taxon_id": taxon_id, **results[taxon_id]} for taxon_id in sorted(results)
        ],
        "unresolved_rules": [rule.code for rule in rules if rule.state == "review"],
        "aggregate_counts": {
            "included": len(included),
            "required_ancestors": len(ancestors),
            "review": sum(result["state"] == "review" for result in results.values()),
            "excluded": sum(result["state"] == "exclude" for result in results.values()),
        },
    }
    semantic_path = output_dir / "scope-manifest.json"
    semantic_path.write_bytes(canonical_json_bytes(semantic))
    semantic_sha = sha256_file(semantic_path)
    export_manifest = {
        "format": "sporely-global-macrofungi-export-v1",
        "release_id": release_id,
        "scope_manifest_sha256": semantic_sha,
        "policy_sha256": policy_hash,
        "source_hashes": source_hashes,
        "files": files,
    }
    (output_dir / "taxonomy_export_manifest.json").write_bytes(canonical_json_bytes(export_manifest))
    archive_path = output_dir / "scoped-export.jsonl.gz"
    dataset_order = [item["name"] for item in files]
    with archive_path.open("wb") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0, filename="scoped-export.jsonl") as compressed:
            for filename in dataset_order:
                with (output_dir / filename).open("rb") as source:
                    shutil.copyfileobj(source, compressed, CHUNK)
    export_manifest["uncompressed_dataset_bytes"] = sum(item["bytes"] for item in files)
    export_manifest["compressed_dataset_bytes"] = archive_path.stat().st_size
    export_manifest["compressed_dataset_sha256"] = sha256_file(archive_path)
    (output_dir / "taxonomy_export_manifest.json").write_bytes(canonical_json_bytes(export_manifest))
    return {"semantic": semantic, "semantic_sha256": semantic_sha, "export_manifest": export_manifest}


def validate_export(output_dir: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    expected_order = [
        "taxonomy_release.jsonl", "taxon.jsonl", "scientific_name.jsonl",
        "vernacular.jsonl", "taxon_external_id.jsonl",
        "taxon_external_id_legacy_integer.jsonl", "taxon_redlist.jsonl",
    ]
    if [item["name"] for item in manifest["files"]] != expected_order:
        raise ScopeError("export dataset order does not match W1 contract")
    taxon_ids: set[int] = set()
    previous = -1
    with (output_dir / "taxon.jsonl").open(encoding="utf-8") as handle:
        for raw in handle:
            row = json.loads(raw)
            taxon_id = int(row["taxon_id"])
            if taxon_id <= previous or taxon_id in taxon_ids:
                raise ScopeError("taxon export is not unique and strictly ordered")
            taxon_ids.add(taxon_id)
            previous = taxon_id
    for item in manifest["files"]:
        path = output_dir / item["name"]
        if sha256_file(path) != item["sha256"]:
            raise ScopeError(f"export hash mismatch: {item['name']}")
        with path.open(encoding="utf-8") as handle:
            rows = sum(1 for line in handle if line.strip())
        if rows != item["row_count"]:
            raise ScopeError(f"export row-count mismatch: {item['name']}")
    for filename in expected_order[2:]:
        with (output_dir / filename).open(encoding="utf-8") as handle:
            for raw in handle:
                taxon_id = json.loads(raw).get("taxon_id")
                if taxon_id is not None and taxon_id not in taxon_ids:
                    raise ScopeError(f"{filename} references absent taxon {taxon_id}")
    return {"status": "passed", "dataset_files": len(expected_order), "taxon_rows": len(taxon_ids)}


def build_desktop(export_dir: Path, target: Path, release_id: str) -> dict[str, Any]:
    started = time.perf_counter()
    connection = sqlite3.connect(target)
    connection.executescript(
        """
        pragma page_size=4096;
        pragma journal_mode=off;
        create table taxon (taxon_id integer primary key, parent_taxon_id integer, scientific_name text not null, rank text, family text, col_id text not null unique, is_selectable integer not null, scope_reason text);
        create table scientific_name (taxon_id integer not null, name text not null, is_preferred integer not null, source text, note text, unique(taxon_id,name));
        create table vernacular_name (taxon_id integer not null, language text not null, name text not null, is_preferred integer not null, source text, unique(taxon_id,language,name));
        create table external_mapping (taxon_id integer not null, source_system text not null, namespace text not null, external_id text not null, is_preferred integer not null, unique(source_system,namespace,external_id,taxon_id));
        create table metadata (key text primary key, value text not null);
        """
    )
    with (export_dir / "taxon.jsonl").open(encoding="utf-8") as handle:
        connection.executemany(
            "insert into taxon values(?,?,?,?,?,?,?,?)",
            ((r["taxon_id"], r.get("parent_taxon_id"), r["canonical_scientific_name"], r.get("taxon_rank"), r.get("family"), r["canonical_external_id"], int(r["scope_state"] == "include"), r["scope_reason"]) for r in map(json.loads, handle)),
        )
    with (export_dir / "scientific_name.jsonl").open(encoding="utf-8") as handle:
        connection.executemany("insert into scientific_name values(?,?,?,?,?)", ((r["taxon_id"], r["scientific_name"], int(r["is_preferred_name"]), r.get("source"), r.get("note")) for r in map(json.loads, handle)))
    with (export_dir / "vernacular.jsonl").open(encoding="utf-8") as handle:
        connection.executemany("insert into vernacular_name values(?,?,?,?,?)", ((r["taxon_id"], r["language_code"], r["vernacular_name"], int(r["is_preferred_name"]), r.get("source")) for r in map(json.loads, handle)))
    with (export_dir / "taxon_external_id.jsonl").open(encoding="utf-8") as handle:
        connection.executemany("insert into external_mapping values(?,?,?,?,?)", ((r["taxon_id"], r["source_system"], r["namespace"], str(r["external_id"]), int(r["is_preferred"])) for r in map(json.loads, handle)))
    connection.execute("insert into metadata values('release_id',?)", (release_id,))
    connection.executescript(
        """
        create index taxon_name_prefix on taxon(scientific_name collate nocase);
        create index scientific_name_prefix on scientific_name(name collate nocase);
        create index vernacular_name_prefix on vernacular_name(language,name collate nocase);
        create index external_mapping_lookup on external_mapping(source_system,namespace,external_id);
        analyze; vacuum;
        """
    )
    counts = {table: connection.execute(f"select count(*) from {table}").fetchone()[0] for table in ("taxon", "scientific_name", "vernacular_name", "external_mapping")}
    indexes = {row[0]: int(connection.execute("select coalesce(sum(pgsize),0) from dbstat where name=?", (row[0],)).fetchone()[0]) for row in connection.execute("select name from sqlite_master where type='index' order by name")}
    connection.close()
    with target.open("rb") as source, target.with_suffix(target.suffix + ".gz").open("wb") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as compressed:
            shutil.copyfileobj(source, compressed, CHUNK)
    return {
        "sqlite_bytes": target.stat().st_size,
        "compressed_bytes": target.with_suffix(target.suffix + ".gz").stat().st_size,
        "table_counts": counts,
        "index_sizes": indexes,
        "build_seconds": round(time.perf_counter() - started, 6),
        "sha256": sha256_file(target),
    }


def measure_lookups(path: Path) -> dict[str, Any]:
    connection = sqlite3.connect(path)
    vernacular_probe = connection.execute("select language,name from vernacular_name order by language,name limit 1").fetchone()
    probes = {
        "canonical_exact": ("select taxon_id from taxon where scientific_name=? collate nocase and is_selectable=1", ("Amanita muscaria",)),
        "canonical_prefix": ("select taxon_id from taxon where scientific_name like ? escape '\\' and is_selectable=1 limit 20", ("Morch%",)),
        "synonym_exact": ("select taxon_id from scientific_name where name=? collate nocase", ("Ustilago maydis",)),
        "alias_prefix": ("select taxon_id from scientific_name where name like ? escape '\\' limit 20", ("Ustil%",)),
        "external_resolution": ("select taxon_id from external_mapping where source_system=? and namespace=? and external_id=?", ("col_xr", "col_usage_id", "B24TM")),
        "genus_autocomplete": ("select taxon_id from taxon where rank='genus' and scientific_name like ? escape '\\' and is_selectable=1 limit 20", ("Aman%",)),
        "vernacular_exact": ("select taxon_id from vernacular_name where language=? and name=? collate nocase", vernacular_probe),
        "vernacular_prefix": ("select taxon_id from vernacular_name where language=? and name like ? escape '\\' limit 20", (vernacular_probe[0], vernacular_probe[1][:3] + "%")),
    }
    output: dict[str, Any] = {}
    for label, (sql, params) in probes.items():
        timings = []
        rows = []
        for _ in range(10):
            started = time.perf_counter_ns()
            rows = connection.execute(sql, params).fetchall()
            timings.append((time.perf_counter_ns() - started) / 1_000_000)
        ordered = sorted(timings)
        output[label] = {
            "min_ms": ordered[0], "p50_ms": ordered[4], "p95_ms": ordered[9],
            "max_ms": ordered[-1], "rows": len(rows),
            "query_plan": [item[3] for item in connection.execute("explain query plan " + sql, params)],
        }
    connection.close()
    return output


def build(args: argparse.Namespace) -> dict[str, Any]:
    policy = load_policy(args.policy)
    policy_hash = sha256_file(args.policy)
    source_hashes = {
        "sqlite_gz_sha256": sha256_file(args.source_gz),
        "w1_manifest_sha256": sha256_file(args.w1_dir / "taxonomy_export_manifest.json"),
    }
    with tempfile.TemporaryDirectory(prefix="sporely-macrofungi-") as directory:
        connection, _ = _source_connection(args.source_gz, Path(directory))
        taxa, by_col = load_taxa(connection)
        rules = resolve_rules(policy, by_col)
        results = evaluate(taxa, rules, policy.get("source_characteristic_exclusions", []))
        selectable_validation = validate_selectable_fungi(taxa, results)
        rule_summary = descendants_summary(taxa, results, rules)
        audit = scope_audit(taxa, results)
        probes = correctness_probes(by_col, results)
        if args.output_dir.exists():
            raise ScopeError(f"output already exists: {args.output_dir}")
        built = build_export(args.w1_dir, args.output_dir, taxa, results, rules, source_hashes, policy_hash, args.release_id)
        connection.close()
    export_validation = validate_export(args.output_dir, built["export_manifest"])
    desktop = build_desktop(args.output_dir, args.desktop, args.release_id)
    desktop["lookup_timings"] = measure_lookups(args.desktop)
    counts = Counter(result["state"] for result in results.values())
    rank_counts = Counter(taxa[taxon_id].rank for taxon_id, result in results.items() if result["state"] == "include")
    winning_counts = Counter(result["rule"] for result in results.values())
    report = {
        "format": "sporely-global-macrofungi-evidence-v1",
        "release_id": args.release_id,
        "starting_revision": args.starting_revision,
        "pinned_col_release": "2026-07-17-XR",
        "policy_sha256": policy_hash,
        "source_hashes": source_hashes,
        "scope_manifest_sha256": built["semantic_sha256"],
        "scope_counts": dict(sorted(counts.items())),
        "included_rank_counts": dict(sorted(rank_counts.items())),
        "selectable_validation": selectable_validation,
        "scope_audit": audit,
        "correctness_probes": probes,
        "export_contract_validation": export_validation,
        "all_winning_rule_counts": dict(sorted(winning_counts.items())),
        "winning_rule_counts": rule_summary,
        "export_files": built["export_manifest"]["files"],
        "export_summary": {
            "uncompressed_dataset_bytes": built["export_manifest"]["uncompressed_dataset_bytes"],
            "compressed_dataset_bytes": built["export_manifest"]["compressed_dataset_bytes"],
            "compressed_dataset_sha256": built["export_manifest"]["compressed_dataset_sha256"],
            "included_concepts": counts["include"],
            "accepted_species": rank_counts["species"],
            "genera": rank_counts["genus"],
            "scientific_name_rows": next(item["row_count"] for item in built["export_manifest"]["files"] if item["name"] == "scientific_name.jsonl"),
            "scientific_aliases": next(item["row_count"] for item in built["export_manifest"]["files"] if item["name"] == "scientific_name.jsonl") - counts["include"],
            "vernacular_names": next(item["row_count"] for item in built["export_manifest"]["files"] if item["name"] == "vernacular.jsonl"),
            "external_mappings": next(item["row_count"] for item in built["export_manifest"]["files"] if item["name"] == "taxon_external_id.jsonl"),
        },
        "desktop_candidate": desktop,
        "unresolved_clade_decisions": [rule.code for rule in rules if rule.state == "review"],
        "historical_compatibility": historical_compatibility(args.observations_db),
        "safety": {"desktop_activation_performed": False, "production_writes_performed": False},
    }
    args.evidence.parent.mkdir(parents=True, exist_ok=True)
    args.evidence.write_bytes(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2).encode() + b"\n")
    return report


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser()
    result.add_argument("--policy", type=Path, required=True)
    result.add_argument("--source-gz", type=Path, required=True)
    result.add_argument("--w1-dir", type=Path, required=True)
    result.add_argument("--output-dir", type=Path, required=True)
    result.add_argument("--desktop", type=Path, required=True)
    result.add_argument("--evidence", type=Path, required=True)
    result.add_argument("--release-id", required=True)
    result.add_argument("--starting-revision", required=True)
    result.add_argument("--observations-db", type=Path)
    return result


def main() -> None:
    args = parser().parse_args()
    report = build(args)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
