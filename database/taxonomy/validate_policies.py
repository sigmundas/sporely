#!/usr/bin/env python3
"""Offline validator for Stage 1 JSON-compatible YAML policies."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

_SCRIPTS = Path(__file__).resolve().parent / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

# One definition of what an approved reviewed relationship must carry, shared
# with the compiler so the validator and the thing it validates cannot drift.
from bridge_emission import missing_review_provenance  # noqa: E402


POLICY_DIR = Path(__file__).resolve().parent / "policies"
REQUIRED_FILES = {
    "scope": "scope.yml",
    "languages": "languages.yml",
    "source_priority": "source_priority.yml",
    "mapping_policy": "mapping_policy.yml",
    "release_thresholds": "release_thresholds.yml",
    "manual_mappings": "manual_mappings.yml",
    "concept_supersessions": "concept_supersessions.yml",
    "release_contract": "release_contract.yml",
}
RELATIONSHIPS = {"exact", "likely_exact", "broader", "narrower", "overlapping", "synonym", "unresolved"}
REVIEW_STATES = {"unreviewed", "needs_review", "approved", "rejected", "superseded"}
SEVERITIES = {"hard_failure", "review_required", "warning", "informational"}


class PolicyError(ValueError):
    pass


def _load(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PolicyError(f"{path.name}: invalid JSON-compatible YAML: {exc}") from exc
    if not isinstance(value, dict):
        raise PolicyError(f"{path.name}: root must be an object")
    return value


def _unique(values: list[str], label: str) -> None:
    duplicates = sorted({value for value in values if values.count(value) > 1})
    if duplicates:
        raise PolicyError(f"duplicate {label}: {', '.join(duplicates)}")


def validate(policy_dir: Path = POLICY_DIR) -> dict[str, Any]:
    missing = [name for name in REQUIRED_FILES.values() if not (policy_dir / name).is_file()]
    if missing:
        raise PolicyError(f"missing required policy files: {', '.join(sorted(missing))}")
    policies = {key: _load(policy_dir / name) for key, name in REQUIRED_FILES.items()}

    source_policy = policies["source_priority"]
    sources = [item["code"] for item in source_policy.get("sources", [])]
    _unique(sources, "source code")
    source_set = set(sources)
    if not source_set:
        raise PolicyError("source catalog is empty")

    namespaces = source_policy.get("identifier_namespaces", [])
    namespace_keys = [f"{item.get('source')}:{item.get('code')}" for item in namespaces]
    _unique(namespace_keys, "identifier namespace")
    for item in namespaces:
        if item.get("source") not in source_set:
            raise PolicyError(f"unknown source reference in identifier namespace: {item.get('source')}")
        if item.get("storage") not in {"text", "positive_integer"}:
            raise PolicyError(f"invalid identifier storage: {item.get('storage')}")

    languages = policies["languages"].get("languages", [])
    codes = [item["code"] for item in languages]
    _unique(codes, "language code")
    aliases: dict[str, str] = {}
    for language in languages:
        code = language["code"]
        for source in language.get("sources", []):
            if source not in source_set:
                raise PolicyError(f"unknown source reference in language {code}: {source}")
        for alias in language.get("aliases", []):
            if alias in codes and alias != code:
                raise PolicyError(f"language alias {alias} conflicts with canonical code")
            if alias in aliases and aliases[alias] != code:
                raise PolicyError(f"language alias {alias} maps to multiple canonical languages")
            aliases[alias] = code
    _unique([str(item["order"]) for item in languages], "language display order")

    priorities = source_policy.get("vernacular_priority", {})
    for language, priority_sources in priorities.items():
        if language != "default" and language not in set(codes):
            raise PolicyError(f"unknown language in vernacular priority: {language}")
        for source in priority_sources:
            if source not in source_set:
                raise PolicyError(f"unknown source in vernacular priority: {source}")

    scope_rules = policies["scope"].get("rules", [])
    rule_codes = [item["code"] for item in scope_rules]
    _unique(rule_codes, "scope rule")
    if "global_fungi" not in set(rule_codes):
        raise PolicyError("missing required scope rule: global_fungi")
    for rule in scope_rules:
        if rule.get("source") not in source_set:
            raise PolicyError(f"unknown source reference in scope rule {rule.get('code')}: {rule.get('source')}")

    mapping = policies["mapping_policy"]
    if set(mapping.get("relationships", [])) != RELATIONSHIPS:
        raise PolicyError("mapping relationship vocabulary does not match the contract")
    if set(mapping.get("review_states", [])) != REVIEW_STATES:
        raise PolicyError("mapping review-state vocabulary does not match the contract")
    for rule in mapping.get("continuity_rules", []):
        if rule.get("review") not in {
            "not_required_unless_conflict", "required_if_scope_or_rank_semantics_change",
            "required", "required_if_no_replacement",
            "not_required_only_if_automatic_identity_requirements_pass", "approved_required"
        }:
            raise PolicyError(f"invalid continuity review rule: {rule.get('review')}")

    thresholds = policies["release_thresholds"]
    if set(thresholds.get("severities", [])) != SEVERITIES:
        raise PolicyError("threshold severity vocabulary does not match the contract")
    for threshold in thresholds.get("thresholds", []):
        if threshold.get("severity") not in SEVERITIES:
            raise PolicyError(f"invalid threshold severity: {threshold.get('severity')}")

    release = policies["release_contract"]
    pattern = re.compile(release["taxonomy_release_pattern"])
    if release.get("taxonomy_schema_version") != 2:
        raise PolicyError("taxonomy_schema_version must be 2")
    if any(not pattern.fullmatch(value) for value in release.get("valid_release_examples", [])):
        raise PolicyError("valid release example does not match taxonomy release pattern")
    if any(pattern.fullmatch(value) for value in release.get("invalid_release_examples", [])):
        raise PolicyError("invalid release example matches taxonomy release pattern")
    source_release = release.get("source_release", {})
    if not source_release.get("archive_immutable") or not source_release.get("latest_forbidden_in_compilation"):
        raise PolicyError("source release archives must be immutable and floating latest must be forbidden")
    required_manifest_fields = set(source_release.get("required_manifest_fields", []))
    if not {"source", "source_release_id", "upstream_version", "sha256", "bytes"} <= required_manifest_fields:
        raise PolicyError("source release manifest contract is incomplete")

    manual = policies["manual_mappings"]
    required = set(manual.get("schema", {}).get("required", []))
    for entry in manual.get("mappings", []):
        missing_fields = sorted(required - set(entry))
        if missing_fields:
            raise PolicyError(f"manual mapping missing fields: {', '.join(missing_fields)}")
        if entry.get("relationship") not in RELATIONSHIPS:
            raise PolicyError(f"invalid manual mapping relationship: {entry.get('relationship')}")
        if entry.get("review_status") not in REVIEW_STATES:
            raise PolicyError(f"invalid manual mapping review status: {entry.get('review_status')}")
        source_usage = entry.get("source_usage", {})
        if source_usage.get("source") not in source_set:
            raise PolicyError(f"unknown source in manual mapping: {source_usage.get('source')}")
        key = f"{source_usage.get('source')}:{source_usage.get('namespace')}"
        if key not in set(namespace_keys):
            raise PolicyError(f"unknown namespace in manual mapping: {key}")
        target = entry.get("target", {})
        if ("source_usage" in target) == ("sporely_taxon_id" in target):
            raise PolicyError("manual mapping target must contain exactly one target kind")
        absent = missing_review_provenance(entry)
        if absent:
            raise PolicyError(
                f"approved manual mapping {entry.get('mapping_id')!r} carries "
                f"no {', '.join(absent)}"
            )

    supersessions = policies["concept_supersessions"]
    required = set(supersessions.get("schema", {}).get("required", []))
    seen_superseded: set[int] = set()
    for entry in supersessions.get("supersessions", []):
        missing_fields = sorted(required - set(entry))
        if missing_fields:
            raise PolicyError(
                f"concept supersession missing fields: {', '.join(missing_fields)}"
            )
        if entry.get("relationship") not in RELATIONSHIPS:
            raise PolicyError(
                f"invalid supersession relationship: {entry.get('relationship')}"
            )
        if entry.get("review_status") not in REVIEW_STATES:
            raise PolicyError(
                f"invalid supersession review status: {entry.get('review_status')}"
            )
        current = entry.get("current_source_usage", {})
        if current.get("source") not in source_set:
            raise PolicyError(
                f"unknown source in supersession: {current.get('source')}"
            )
        key = f"{current.get('source')}:{current.get('namespace')}"
        if key not in set(namespace_keys):
            raise PolicyError(f"unknown namespace in supersession: {key}")
        superseded = entry.get("superseded_sporely_taxon_id")
        if not isinstance(superseded, int) or superseded <= 0:
            raise PolicyError(
                f"supersession superseded_sporely_taxon_id must be a positive "
                f"integer, got {superseded!r}"
            )
        if superseded in seen_superseded:
            raise PolicyError(
                f"sporely_taxon_id={superseded} is superseded more than once"
            )
        seen_superseded.add(superseded)
        absent = missing_review_provenance(entry)
        if absent:
            raise PolicyError(
                f"approved supersession {entry.get('supersession_id')!r} "
                f"carries no {', '.join(absent)}"
            )

    return policies


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--policy-dir", type=Path, default=POLICY_DIR)
    args = parser.parse_args()
    policies = validate(args.policy_dir)
    print(
        f"validated {len(policies)} policy files, "
        f"{len(policies['languages']['languages'])} languages, "
        f"{len(policies['source_priority']['identifier_namespaces'])} namespaces"
    )


if __name__ == "__main__":
    main()
