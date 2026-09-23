#!/usr/bin/env python3
"""Grade every NorTaxa->Sporely bridge binding against the reviewed standard.

Stage 1 established *what evidence produced* each of the 19,808 cross-source
bridge bindings, recording the matching rule per association in
``nortaxa-bridge-association-audit.json``. Stage 3 defines the standard that
decides which of those bindings may be published as authoritative identity
(``policies/mapping_policy.yml.authoritative_bridge_emission``). This script
joins the two and reports, for the active release's scope, how many bindings
the standard emits and how many it refuses — with the reason for every
refusal.

Two populations are reported, and the distinction matters:

* **the scoped alias population** — every cross-source bridge binding sitting
  on a concept the active release retains. This is the population the standard
  actually grades.
* **the vernacular-joined subset** — the 2,041 taxa the closeout plan names,
  which carry a NorTaxa vernacular name. Stage 1 §3.1 showed this is an
  arbitrary sample of the bridge population (a taxon is in it only if
  Artsdatabanken happened to publish a Norwegian name), so it is reported as a
  subset rather than as the denominator.

Read-only. Touches no release and writes only its own JSON output.

Inputs default to the primary checkout, because generated release artifacts
and acquired archives are gitignored and therefore exist in exactly one
checkout.

    python audit_bridge_emission_coverage.py [--generated DIR] [--out-dir DIR]
"""
from __future__ import annotations

import argparse
import collections
import json
import sqlite3
import sys
from pathlib import Path

_TAXONOMY = Path(__file__).resolve().parents[2]
_SCRIPTS = _TAXONOMY / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from bridge_emission import BridgeEmissionPolicy  # noqa: E402
from cross_source_mapping import (  # noqa: E402
    EVIDENCE_CLASS_CROSS_SOURCE_MISSING_AUTHORSHIP,
    EVIDENCE_CLASS_CROSS_SOURCE_STRICT,
)

PRIMARY = Path("/Users/sigmundas/Documents/Code/sporely/sporely-py")
DEFAULT_GENERATED = PRIMARY / "database/reference_data/generated/taxonomy_v2"
CANDIDATE_SQLITE = "tax-2026.07.30-02.sqlite3"
ACTIVE_RELEASE = "global_macrofungi_tax-2026.08.01-01"
POLICY_PATH = _TAXONOMY / "policies" / "mapping_policy.yml"
STAGE1_AUDIT = Path(__file__).parent / "nortaxa-bridge-association-audit.json"

#: Stage 1 recorded the matching rule under its own labels. Map them onto the
#: evidence-class vocabulary the policy grades, so the two artifacts cannot
#: drift apart in meaning.
_RULE_TO_EVIDENCE_CLASS = {
    "strict": EVIDENCE_CLASS_CROSS_SOURCE_STRICT,
    "missing_authorship_fallback": EVIDENCE_CLASS_CROSS_SOURCE_MISSING_AUTHORSHIP,
}

#: Published cross-reference evidence per association, from
#: ``scripts/cross_reference_evidence.py``. This is the second axis of the
#: audit: the matching rule says how the compiler derived the association, and
#: this says whether either source ever published a statement connecting the
#: two concepts.
XREF_EVIDENCE = Path(__file__).parent / "stage3-cross-reference-evidence.json"
XREF_REVIEWABLE = frozenset({
    "reciprocal_accepted_synonymy",
    "one_directional_accepted_synonymy",
    "shared_synonymy",
})

#: The plan's two mandatory regression cases.
REGRESSIONS = {"52369": "divergent accepted names", "53482": "agreeing names"}


def _scoped_taxa(generated: Path) -> tuple[set[int], set[int]]:
    """Return (retained concepts, vernacular-joined concepts)."""
    release = generated / ACTIVE_RELEASE
    retained: set[int] = set()
    with (release / "taxon.jsonl").open(encoding="utf-8") as handle:
        for raw in handle:
            row = json.loads(raw)
            if row.get("scope_state") == "include":
                retained.add(int(row["taxon_id"]))
    vernacular: set[int] = set()
    with (release / "vernacular.jsonl").open(encoding="utf-8") as handle:
        for raw in handle:
            vernacular.add(int(json.loads(raw)["taxon_id"]))
    return retained, vernacular


def _active_namespaces(generated: Path) -> dict[str, int]:
    counts: collections.Counter[str] = collections.Counter()
    path = generated / ACTIVE_RELEASE / "taxon_external_id.jsonl"
    with path.open(encoding="utf-8") as handle:
        for raw in handle:
            row = json.loads(raw)
            counts[f"{row.get('source_system')}/{row.get('namespace')}"] += 1
    return dict(sorted(counts.items()))


def _bridge_bindings(generated: Path) -> list[tuple[int, str]]:
    """Every NorTaxa cross-source bridge binding, as (sporely_id, taxonID)."""
    db = generated / CANDIDATE_SQLITE
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        return [
            (int(taxon_id), str(external_id))
            for taxon_id, external_id in conn.execute(
                "SELECT taxon_id, external_id FROM taxon_external_id_min "
                "WHERE source_system = 'artsdatabanken' "
                "AND note = 'cross_source_automatic_exact' "
                "ORDER BY taxon_id, external_id"
            )
        ]
    finally:
        conn.close()


def _matching_rules() -> dict[tuple[int, str], str]:
    audit = json.loads(STAGE1_AUDIT.read_text(encoding="utf-8"))
    columns = audit["columns"]
    sporely = columns.index("sporely_taxon_id")
    nortaxa = columns.index("nortaxa_taxon_id")
    rule = columns.index("matching_rule")
    return {
        (int(row[sporely]), str(row[nortaxa])): str(row[rule])
        for row in audit["rows"]
    }


DISPOSITION_EMITTED = "emitted_reviewed_relationship"
DISPOSITION_PENDING = "reviewable_awaiting_human_decision"
DISPOSITION_REJECTED = "rejected_no_published_cross_reference"


def _cross_reference_evidence() -> dict[str, str]:
    """``{nortaxa_taxon_id: published cross-reference evidence class}``."""
    document = json.loads(XREF_EVIDENCE.read_text(encoding="utf-8"))
    columns = document["columns"]
    nortaxa = columns.index("nortaxa_taxon_id")
    evidence = columns.index("evidence_class")
    return {str(row[nortaxa]): str(row[evidence]) for row in document["rows"]}


def _approved_reviewed_usages() -> set[str]:
    """NorTaxa identifiers carrying an approved reviewed relationship.

    Reads both reviewed ledgers. A record awaiting review contributes nothing,
    which is why the emitted column is currently empty: the mechanism is in
    place and the decisions have not been made.
    """
    approved: set[str] = set()
    mappings = json.loads(
        (_TAXONOMY / "policies" / "manual_mappings.yml").read_text(
            encoding="utf-8"))
    for entry in mappings.get("mappings", []):
        if entry.get("review_status") != "approved":
            continue
        usage = entry.get("source_usage") or {}
        if usage.get("source") == "nortaxa":
            approved.add(str(usage.get("identifier")))
    supersessions_path = _TAXONOMY / "policies" / "concept_supersessions.yml"
    if supersessions_path.exists():
        document = json.loads(supersessions_path.read_text(encoding="utf-8"))
        superseded = {
            int(entry["superseded_sporely_taxon_id"])
            for entry in document.get("supersessions", [])
            if entry.get("review_status") == "approved"
        }
        if superseded:
            # A supersession is keyed by Sporely id, but this audit is keyed by
            # NorTaxa identifier, so resolve the concept back to every NorTaxa
            # usage allocated to it. Without this the audit reports an emitted
            # relationship as still awaiting review — which it did for 52369,
            # whose usage is an anchor rather than a bridge binding and so
            # never appears in the binding list at all.
            approved.update(_nortaxa_identifiers_for(superseded))
    return approved


def _nortaxa_identifiers_for(sporely_ids: set[int]) -> set[str]:
    """Every NorTaxa identifier the registry allocated to these concepts."""
    out: set[str] = set()
    registry_dir = _TAXONOMY / "registry" / "canonical"
    for shard in sorted(registry_dir.glob("part-*.jsonl")):
        with shard.open(encoding="utf-8") as handle:
            for raw in handle:
                if '"nortaxa"' not in raw:
                    continue
                entry = json.loads(raw)
                if entry.get("__registry_header__"):
                    continue
                if entry.get("source") != "nortaxa":
                    continue
                if int(entry["sporely_taxon_id"]) in sporely_ids:
                    out.add(str(entry["identifier"]))
    return out


def _disposition(external_id: str, evidence: str, approved: set[str]) -> str:
    if external_id in approved:
        return DISPOSITION_EMITTED
    if evidence in XREF_REVIEWABLE:
        return DISPOSITION_PENDING
    return DISPOSITION_REJECTED


def audit(generated: Path) -> dict:
    policy = BridgeEmissionPolicy.load(POLICY_PATH)
    retained, vernacular_joined = _scoped_taxa(generated)
    bindings = _bridge_bindings(generated)
    rules = _matching_rules()

    populations = {
        "all_bridge_bindings": lambda taxon_id: True,
        "scoped_bridge_bindings": lambda taxon_id: taxon_id in retained,
        "scoped_vernacular_joined_subset":
            lambda taxon_id: taxon_id in retained and taxon_id in vernacular_joined,
    }
    xref = _cross_reference_evidence()
    approved = _approved_reviewed_usages()

    report: dict[str, dict] = {}
    for label, predicate in populations.items():
        dispositions: collections.Counter[str] = collections.Counter()
        matching_rules: collections.Counter[str] = collections.Counter()
        xref_classes: collections.Counter[str] = collections.Counter()
        taxa: set[int] = set()
        for taxon_id, external_id in bindings:
            if not predicate(taxon_id):
                continue
            taxa.add(taxon_id)
            rule = rules.get((taxon_id, external_id), "not_graded_by_stage_1")
            matching_rules[rule] += 1
            evidence = xref.get(external_id, "not_graded_for_cross_reference")
            xref_classes[evidence] += 1
            dispositions[_disposition(external_id, evidence, approved)] += 1
        report[label] = {
            "bindings": sum(dispositions.values()),
            "distinct_taxa": len(taxa),
            "disposition": dict(sorted(dispositions.items())),
            "emitted_total": dispositions[DISPOSITION_EMITTED],
            "unemitted_total": (
                sum(dispositions.values()) - dispositions[DISPOSITION_EMITTED]
            ),
            "by_compiler_matching_rule": dict(sorted(matching_rules.items())),
            "by_published_cross_reference_evidence": dict(
                sorted(xref_classes.items())),
        }

    by_external = {external_id: taxon_id for taxon_id, external_id in bindings}
    regressions = {}
    for external_id, description in REGRESSIONS.items():
        taxon_id = by_external.get(external_id)
        rule = rules.get((taxon_id, external_id)) if taxon_id else None
        evidence = xref.get(external_id, "not_graded_for_cross_reference")
        regressions[external_id] = {
            "case": description,
            "has_cross_source_bridge_binding": taxon_id is not None,
            "host_sporely_taxon_id": taxon_id,
            "host_retained_in_active_release": (
                taxon_id in retained if taxon_id is not None else False
            ),
            "compiler_matching_rule": rule,
            "published_cross_reference_evidence": evidence,
            "reviewable": evidence in XREF_REVIEWABLE,
            "disposition": _disposition(external_id, evidence, approved),
        }

    active_namespaces = _active_namespaces(generated)
    scoped = report["scoped_bridge_bindings"]
    return {
        "policy_sha256": policy.policy_sha256,
        "eligible_evidence_classes": sorted(policy.eligible),
        "candidate_sqlite": CANDIDATE_SQLITE,
        "active_release": ACTIVE_RELEASE,
        "active_release_retained_concepts": len(retained),
        "active_release_vernacular_joined_taxa": len(vernacular_joined),
        "active_release_external_id_namespaces": active_namespaces,
        "approved_reviewed_relationships": sorted(approved),
        "cross_reference_evidence_source": str(XREF_EVIDENCE.name),
        "coverage": report,
        "regressions": regressions,
        "projected_active_release_external_id_rows": {
            "before": sum(active_namespaces.values()),
            "added_by_emitted_reviewed_relationships": scoped["emitted_total"],
            "after": sum(active_namespaces.values()) + scoped["emitted_total"],
            "note": "Only approved reviewed relationships add rows. The "
                    "reviewable-pending count is what a completed review "
                    "could add, not what this projection emits.",
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generated", type=Path, default=DEFAULT_GENERATED)
    parser.add_argument("--out-dir", type=Path, default=Path(__file__).parent)
    args = parser.parse_args()
    report = audit(args.generated)
    out = args.out_dir / "stage3-bridge-emission-coverage.json"
    out.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
