"""Deterministic manifest builder.

Given a list of ``ReconciliationResult`` records, the manifest is:

* the provenance header (contract §9),
* a ``results`` array sorted by ``observation_id``,
* each result serialised with the lexicographically-sorted field order
  documented in :func:`ReconciliationResult.to_dict`.

The manifest body is serialised as UTF-8 JSON with ``\\n`` newlines,
``indent=2``, ``ensure_ascii=False`` and ``sort_keys=True``. The
``semantic_hash`` is computed after removing every field named in the
policy's ``semantic_hash_excludes`` (default: ``generated_at``,
``resolution_timestamp``, ``run_host``).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from database.taxonomy.reconciliation.errors import ReconciliationInvariantError
from database.taxonomy.reconciliation.input_model import ReconciliationResult
from database.taxonomy.reconciliation.namespace_rules import NamespaceRuleSet
from database.taxonomy.reconciliation.sources import PinnedRelease


MANIFEST_FILENAME = "reconciliation-manifest.json"
SEMANTIC_HASH_FILENAME = "reconciliation-manifest.sha256.txt"
SUMMARY_FILENAME = "reconciliation-summary.md"


@dataclass(frozen=True, slots=True)
class ManifestArtefact:
    """Return value of :func:`build_manifest_body`.

    * ``body`` — the JSON string that will be written to disk;
    * ``semantic_hash`` — the SHA-256 of the manifest body with the
      documented non-semantic fields removed.
    * ``manifest`` — the assembled dict, for callers that want to inspect
      values without re-parsing the string.
    """

    body: str
    semantic_hash: str
    manifest: dict[str, Any]


def _remove_keys(obj: Any, keys: frozenset[str]) -> Any:
    if isinstance(obj, dict):
        return {
            k: _remove_keys(v, keys) for k, v in obj.items() if k not in keys
        }
    if isinstance(obj, list):
        return [_remove_keys(item, keys) for item in obj]
    return obj


def _input_source_hash(results: Iterable[ReconciliationResult]) -> str:
    digest = hashlib.sha256()
    for record in sorted(results, key=lambda r: r.observation_id):
        # Normalize signals to a sorted array of tuples so signal order is
        # ignored; this is deterministic even when the caller iterates in a
        # different order.
        sig_tuples = sorted(
            [
                (s.source_system or "", s.namespace or "", s.external_id or "", s.origin_field, s.raw_value or "", s.rule_id)
                for s in record.signals_all
            ]
        )
        payload = json.dumps(
            {
                "observation_id": record.observation_id,
                "signals": sig_tuples,
                "manual_identification_flag": _pick_manual_flag(record),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        digest.update(payload.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _pick_manual_flag(record: ReconciliationResult) -> bool:
    # We only see the resulting record here, so we deduce the flag from
    # the review reason. The manual_unresolved state is the canonical
    # marker; any other state has the flag as false-by-default.
    return record.reconciliation_state == "manual_unresolved"


def _aggregate_counts(results: Iterable[ReconciliationResult]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for record in results:
        counts[record.reconciliation_state] = counts.get(record.reconciliation_state, 0) + 1
    return dict(sorted(counts.items()))


def build_manifest_body(
    *,
    results: list[ReconciliationResult],
    rule_set: NamespaceRuleSet,
    release: PinnedRelease,
) -> ManifestArtefact:
    """Assemble the deterministic manifest body and its semantic hash."""
    observation_ids = [r.observation_id for r in results]
    if len(observation_ids) != len(set(observation_ids)):
        seen: set[str] = set()
        duplicates: set[str] = set()
        for obs_id in observation_ids:
            if obs_id in seen:
                duplicates.add(obs_id)
            seen.add(obs_id)
        raise ReconciliationInvariantError(
            f"duplicate observation_ids in manifest: {sorted(duplicates)}"
        )

    sorted_results = sorted(results, key=lambda r: r.observation_id)

    manifest: dict[str, Any] = {
        "aggregate_counts": _aggregate_counts(sorted_results),
        "input_source_hash": _input_source_hash(sorted_results),
        "manifest_version": rule_set.manifest_version(),
        "policy_sha256": rule_set.policy_sha256,
        "policy_version": str(rule_set.policy_body.get("policy_version") or ""),
        "record_count": len(sorted_results),
        "records": [r.to_dict() for r in sorted_results],
        "registry_identity_hash": release.registry_identity_hash,
        "taxonomy_release_id": rule_set.taxonomy_release_id(),
        "taxonomy_scope_manifest_sha256": rule_set.taxonomy_scope_manifest_sha256(),
        "verified_scope_manifest_sha256": release.scope_manifest_sha256,
    }

    # Enforce contract §9 "excludes machine paths, hostnames, wall-clock
    # timestamps" by never writing them. If any downstream caller wants to
    # add generated_at they must do it outside the semantic body and mark
    # it excluded via semantic_hash_excludes.

    body = json.dumps(
        manifest,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ) + "\n"

    excludes = frozenset(rule_set.semantic_hash_excludes())
    semantic_source = _remove_keys(manifest, excludes)
    semantic_body = json.dumps(
        semantic_source,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ) + "\n"
    semantic_hash = hashlib.sha256(semantic_body.encode("utf-8")).hexdigest()

    return ManifestArtefact(body=body, semantic_hash=semantic_hash, manifest=manifest)


def render_summary(manifest: dict[str, Any]) -> str:
    """Render the Stage W2D §15 aggregate report as Markdown.

    The output is deterministic: counts sorted alphabetically by state, no
    timestamps, no hostnames.
    """
    lines: list[str] = []
    lines.append("# W2D reconciliation summary")
    lines.append("")
    lines.append(
        "SYNTHETIC FIXTURE-BACKED RUN — 337-observation real audit blocked; "
        "see w2d-reconciliation-contract.md §1."
    )
    lines.append("")
    lines.append(f"* manifest_version: `{manifest['manifest_version']}`")
    lines.append(f"* policy_version: `{manifest['policy_version']}`")
    lines.append(f"* policy_sha256: `{manifest['policy_sha256']}`")
    lines.append(f"* taxonomy_release_id: `{manifest['taxonomy_release_id']}`")
    lines.append(
        f"* taxonomy_scope_manifest_sha256: `{manifest['taxonomy_scope_manifest_sha256']}`"
    )
    lines.append(f"* record_count: {manifest['record_count']}")
    lines.append(f"* input_source_hash: `{manifest['input_source_hash']}`")
    lines.append("")
    lines.append("## Aggregate counts")
    lines.append("")
    lines.append("| state | count |")
    lines.append("|---|---:|")
    for state, count in manifest["aggregate_counts"].items():
        lines.append(f"| {state} | {count} |")
    lines.append("")
    lines.append("## Migration actions")
    lines.append("")
    lines.append("| migration_action | count |")
    lines.append("|---|---:|")
    action_counts: dict[str, int] = {}
    for record in manifest["records"]:
        action_counts[record["migration_action"]] = (
            action_counts.get(record["migration_action"], 0) + 1
        )
    for action, count in sorted(action_counts.items()):
        lines.append(f"| {action} | {count} |")
    lines.append("")
    return "\n".join(lines) + "\n"


def write_manifest(
    output_dir: Path,
    artefact: ManifestArtefact,
    *,
    write_summary: bool = True,
) -> dict[str, Path]:
    """Write the manifest + sha companion + optional Markdown summary.

    Returns a dict mapping artefact-name -> path for the caller. The output
    directory is created if missing.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / MANIFEST_FILENAME
    sha_path = output_dir / SEMANTIC_HASH_FILENAME
    manifest_path.write_text(artefact.body, encoding="utf-8")
    sha_path.write_text(artefact.semantic_hash + "\n", encoding="utf-8")
    paths = {"manifest": manifest_path, "sha256": sha_path}
    if write_summary:
        summary_path = output_dir / SUMMARY_FILENAME
        summary_path.write_text(render_summary(artefact.manifest), encoding="utf-8")
        paths["summary"] = summary_path
    return paths
