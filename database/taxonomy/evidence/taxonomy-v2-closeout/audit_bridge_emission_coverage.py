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
    report: dict[str, dict] = {}
    for label, predicate in populations.items():
        emitted: collections.Counter[str] = collections.Counter()
        rejected: collections.Counter[str] = collections.Counter()
        unknown = 0
        taxa: set[int] = set()
        for taxon_id, external_id in bindings:
            if not predicate(taxon_id):
                continue
            taxa.add(taxon_id)
            rule = rules.get((taxon_id, external_id))
            if rule is None:
                unknown += 1
                continue
            evidence_class = _RULE_TO_EVIDENCE_CLASS.get(rule, "")
            if policy.is_eligible(evidence_class):
                emitted[evidence_class] += 1
            else:
                rejected[
                    f"{evidence_class}|{policy.rejection_reason(evidence_class)}"
                ] += 1
        report[label] = {
            "bindings": sum(emitted.values()) + sum(rejected.values()) + unknown,
            "distinct_taxa": len(taxa),
            "emitted_total": sum(emitted.values()),
            "emitted_by_evidence_class": dict(sorted(emitted.items())),
            "rejected_total": sum(rejected.values()),
            "rejected_by_evidence_class_and_reason": dict(sorted(rejected.items())),
            "not_graded_by_stage_1_audit": unknown,
        }

    by_external = {external_id: taxon_id for taxon_id, external_id in bindings}
    regressions = {}
    for external_id, description in REGRESSIONS.items():
        taxon_id = by_external.get(external_id)
        rule = rules.get((taxon_id, external_id)) if taxon_id else None
        evidence_class = _RULE_TO_EVIDENCE_CLASS.get(rule or "", "")
        regressions[external_id] = {
            "case": description,
            "has_cross_source_bridge_binding": taxon_id is not None,
            "host_sporely_taxon_id": taxon_id,
            "host_retained_in_active_release": (
                taxon_id in retained if taxon_id is not None else False
            ),
            "matching_rule": rule,
            "evidence_class": evidence_class or None,
            "emitted_by_standard": bool(
                taxon_id is not None and policy.is_eligible(evidence_class)
            ),
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
        "coverage": report,
        "regressions": regressions,
        "projected_active_release_external_id_rows": {
            "before": sum(active_namespaces.values()),
            "added_by_bridge_emission": scoped["emitted_total"],
            "after": sum(active_namespaces.values()) + scoped["emitted_total"],
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
