#!/usr/bin/env python3
"""Apply reviewed manual-unresolved decisions as an explicit overlay to the
accepted 369-record reconciliation manifest.

Overlay semantics:

* only the pseudonymous observation IDs explicitly listed in each decision
  are updated. Decisions do NOT infer a global name → concept mapping;
* an ``accepted_*`` choice rewrites the record to
  ``resolved_exact`` with ``resolution_method='operator_manual_review'``;
* a ``no_match`` choice leaves the observation as ``manual_unresolved``
  and records the reviewer's acknowledgement in ``review_reason``;
* a decision may cover 1..N observation IDs — all others in the manifest
  are untouched;
* the manifest header is re-emitted with an ``overlay`` block naming the
  decisions file and its SHA-256 for audit;
* record ordering, key ordering, and JSON formatting are re-canonicalised
  so the semantic SHA-256 is byte-deterministic across runs.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

FORBIDDEN_PATHS = (
    "/Users/sigmundas/Documents/Code/sporely/sporely-py/",
    "/Users/sigmundas/Documents/Code/sporely/sporely-web/",
)


def _refuse_repo_path(path: Path) -> None:
    abs_ = str(path.resolve())
    for prefix in FORBIDDEN_PATHS:
        if abs_.startswith(prefix):
            raise SystemExit(
                f"refuse: {path} lives under {prefix} — the decisions file and the new manifest MUST stay outside both repositories"
            )


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _canonicalise(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _canonicalise(obj[k]) for k in sorted(obj.keys())}
    if isinstance(obj, list):
        return [_canonicalise(v) for v in obj]
    return obj


def _stable_dumps(obj: Any) -> str:
    return json.dumps(_canonicalise(obj), ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def _load_decisions(path: Path) -> tuple[dict, dict]:
    header: dict = {}
    by_obs: dict[str, dict] = {}
    with path.open("r", encoding="utf-8") as f:
        for line_number, raw in enumerate(f, start=1):
            line = raw.strip()
            if not line:
                continue
            doc = json.loads(line)
            if doc.get("__header__"):
                header = doc
                continue
            observation_ids = doc.get("observation_ids") or []
            for oid in observation_ids:
                if oid in by_obs:
                    raise SystemExit(
                        f"decisions file line {line_number}: observation_id {oid!r} already assigned by an earlier decision"
                    )
                by_obs[oid] = doc
    return header, by_obs


def _apply(
    record: dict,
    decision: dict,
    overlay_release_id: str,
) -> dict:
    """Return a NEW record with the decision applied.

    The original observation snapshot fields (``original_*``, signals_all,
    unmapped_signals) are preserved verbatim.
    """
    choice = decision.get("choice") or ""
    if choice == "no_match":
        rec = dict(record)
        rec["review_reason"] = "manually reviewed: no match found by reviewer"
        rec["migration_action"] = "retain_unresolved_without_registry_concept"
        rec["conflicting_concepts"] = []
        # State stays manual_unresolved. Snapshot untouched.
        return rec
    if not choice.startswith("accepted"):
        raise SystemExit(f"unknown decision choice: {choice!r}")

    cand = decision.get("candidate") or {}
    sporely_taxon_id = cand.get("sporely_taxon_id")
    if sporely_taxon_id is None:
        raise SystemExit(f"decision {choice} has no sporely_taxon_id: {decision}")
    if cand.get("selectable") is False:
        raise SystemExit(
            f"decision selected a context-only candidate for observation {record['observation_id']}"
        )
    match_type = cand.get("match_type") or ""
    reconciliation_state = (
        "resolved_exact_via_synonym_relationship"
        if match_type == "nortaxa_synonym_redirect"
        else "resolved_exact"
    )
    evidence_step = {
        "level": 5,
        "method": "operator_manual_review",
        "action": "operator_manual_review",
        "source_system": cand.get("source_system"),
        "namespace": cand.get("source_namespace"),
        "external_id": cand.get("source_external_id"),
        "resolved_taxon_id": sporely_taxon_id,
        "match_type": match_type,
        "note": cand.get("note") or "operator manual review",
        "decision_group_signature": decision.get("group_signature"),
    }
    rec = dict(record)
    rec["reconciliation_state"] = reconciliation_state
    rec["resolved_sporely_taxon_id"] = sporely_taxon_id
    rec["resolved_canonical_name"] = cand.get("canonical_name")
    rec["resolved_rank"] = cand.get("rank")
    rec["resolved_scope_state"] = "include" if cand.get("cache_state") == "in_cache" else "not_evaluated"
    rec["resolved_cache_state"] = cand.get("cache_state") or "out_of_cache"
    rec["resolution_method"] = "operator_manual_review"
    rec["resolution_evidence"] = [evidence_step]
    rec["resolution_release"] = overlay_release_id
    rec["migration_action"] = "materialize_existing_taxonomy_v2_concept"
    rec["review_reason"] = None
    rec["candidate_concepts"] = []
    rec["conflicting_concepts"] = []
    return rec


def _semantic_sha256(manifest_body: dict) -> str:
    # Same canonicalisation the desktop engine uses: sort keys, indent=2,
    # trailing newline, then hash. No non-semantic fields to strip because we
    # never inline timestamps.
    return hashlib.sha256(_stable_dumps(manifest_body).encode("utf-8")).hexdigest()


def run(
    input_manifest: Path,
    decisions: Path,
    output_manifest: Path,
    overlay_release_id: str,
) -> dict:
    _refuse_repo_path(decisions)
    _refuse_repo_path(output_manifest)
    if output_manifest.exists():
        raise SystemExit(f"refuse: output already exists: {output_manifest}")

    header, decisions_by_obs = _load_decisions(decisions)
    decisions_sha = _sha256_file(decisions)

    manifest = json.loads(input_manifest.read_text())
    original_sha = _sha256_file(input_manifest)

    manual_ids = {r["observation_id"] for r in manifest["records"] if r["reconciliation_state"] == "manual_unresolved"}
    referenced_manual_ids = set(decisions_by_obs.keys())
    unknown_ids = referenced_manual_ids - manual_ids
    if unknown_ids:
        raise SystemExit(
            f"decisions reference observation_ids not present as manual_unresolved: {sorted(unknown_ids)[:5]}..."
        )
    uncovered_manual = manual_ids - referenced_manual_ids
    if uncovered_manual:
        raise SystemExit(
            f"decisions do not cover every manual_unresolved observation ({len(uncovered_manual)} missing)"
        )

    from collections import Counter
    new_records = []
    state_counts = Counter()
    method_counts = Counter()
    overlay_touched = 0
    for r in manifest["records"]:
        if r["reconciliation_state"] == "manual_unresolved":
            decision = decisions_by_obs[r["observation_id"]]
            new_r = _apply(r, decision, overlay_release_id)
            overlay_touched += 1
        else:
            new_r = r
        new_records.append(new_r)
        state_counts[new_r["reconciliation_state"]] += 1
        method = new_r.get("resolution_method")
        if method:
            method_counts[method] += 1

    # Rebuild manifest body preserving the original top-level ordering and
    # augmenting with an overlay block.
    body = dict(manifest)
    body["records"] = new_records
    body["aggregate_counts"] = dict(state_counts)
    body["overlay"] = {
        "kind": "manual_review",
        "decisions_file_sha256": decisions_sha,
        "decisions_file_reference": str(decisions.resolve()),
        "decisions_header": header,
        "input_manifest_sha256": original_sha,
        "input_manifest_semantic_sha256": manifest.get("semantic_sha256") or None,
        "overlay_release_id": overlay_release_id,
        "records_touched": overlay_touched,
        "state_counts_after": dict(state_counts),
        "resolution_method_counts_after": dict(method_counts),
    }

    canonical_text = _stable_dumps(body)
    output_manifest.write_text(canonical_text)
    semantic_sha = hashlib.sha256(canonical_text.encode("utf-8")).hexdigest()
    sha_file = output_manifest.parent / (output_manifest.name.replace(".json", ".sha256.txt"))
    if sha_file == output_manifest:
        sha_file = output_manifest.parent / (output_manifest.name + ".sha256.txt")
    sha_file.write_text(semantic_sha + "\n")
    return {
        "output_manifest": str(output_manifest),
        "output_manifest_sha256": _sha256_file(output_manifest),
        "semantic_sha256": semantic_sha,
        "record_count": len(new_records),
        "state_counts": dict(state_counts),
        "resolution_method_counts": dict(method_counts),
        "overlay_records_touched": overlay_touched,
        "decisions_file_sha256": decisions_sha,
        "input_manifest_sha256": original_sha,
    }


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-manifest", type=Path, required=True)
    parser.add_argument("--decisions", type=Path, required=True)
    parser.add_argument("--output-manifest", type=Path, required=True)
    parser.add_argument("--overlay-release-id", type=str, default="tax-2026.08.02.review-01",
                        help="release-id string stored in each overlay-resolved record's resolution_release")
    parser.add_argument("--production", action="store_true")
    args = parser.parse_args(argv)
    if args.production:
        print("refuse: --production is not honoured", file=sys.stderr)
        return 3
    summary = run(
        input_manifest=args.input_manifest,
        decisions=args.decisions,
        output_manifest=args.output_manifest,
        overlay_release_id=args.overlay_release_id,
    )
    print(json.dumps(summary, sort_keys=True, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
