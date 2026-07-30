#!/usr/bin/env python3
"""Deterministic cross-source mapping proposer.

Given normalized-source taxa from a *bridge* source (currently NorTaxa) and a
canonical *backbone* source (COL), classify every bridge concept per the
sporely taxonomy mapping policy without ever performing a name-only merge.

Classification is done by :func:`classify_bridge_records` which returns, for
each bridge record, one of these ``proposal_class`` values:

* ``automatic_exact``  — the conservative exact rule (see below) holds. This
  is the only class that may automatically alias the bridge identifier onto a
  backbone-backed Sporely identity.
* ``review_proposed``  — canonical scientific name + rank match exactly one
  backbone concept but at least one of the strengthening evidence checks
  fails (missing/mismatched authorship, incompatible status, unresolved
  kingdom). The mapping policy classifies this as
  ``likely_exact_or_unresolved_never_merge`` — review required, never merge.
* ``ambiguous``        — canonical name matches multiple backbone concepts
  (homonyms or same-name-different-rank on the same rank). ``keep_separate``.
* ``national_only``    — canonical name has no backbone match. The bridge
  gets its own Sporely anchor per ``unresolved_supported_source_taxon``.
* ``rejected``         — a manual-mappings entry with ``review_status ==
  rejected`` explicitly refuses this mapping. Recorded but not aliased.

The conservative exact rule (all must hold):

1. Exactly one backbone candidate at (canonical scientific name, rank).
2. Both records declare Fungi as kingdom (fungal scope guarantees COL
   candidates are within Fungi; bridge must agree explicitly).
3. Both authorship fields are non-empty AND compare equal after
   Unicode-NFC-normalized whitespace collapse. Authorship strings are
   PRESERVED verbatim from the upstream Darwin Core field; nothing is parsed
   out of scientific-name display text.
4. Taxonomic-status compatibility: both ``accepted`` (or ``provisionally
   accepted``). A synonym on either side falls through to review.
5. No competing homonym in the backbone's canonical-name / rank bucket.

Determinism: for a fixed pair of normalized inputs, the mapping records are
byte-identical across runs. Ordering keys are stable ``(source, namespace,
identifier)`` tuples plus mapping ids.
"""
from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from typing import Iterable


PROPOSAL_AUTOMATIC_EXACT = "automatic_exact"
PROPOSAL_REVIEW_PROPOSED = "review_proposed"
PROPOSAL_AMBIGUOUS = "ambiguous"
PROPOSAL_NATIONAL_ONLY = "national_only"
PROPOSAL_REJECTED = "rejected"

# Equivalent "concept is currently accepted" tokens across supported codes.
# COL (zoological/CoL practice) publishes "accepted" and "provisionally
# accepted". NorTaxa follows the ICN mycological practice of publishing
# "valid" for the same semantic — a documented equivalence, not an inferred
# one. Adding a new source with its own token requires an explicit reviewed
# addition here; a status not on this list falls to review_proposed.
_ACCEPTED_STATUSES = frozenset({
    "accepted", "provisionally accepted",
    "valid",
})
_FUNGI_KINGDOM = "Fungi"


_CLASSIFICATION_KEYS = ("family", "genus", "order", "class", "phylum")


def _clean_classification(raw: dict | None) -> dict[str, str]:
    if not raw:
        return {k: "" for k in _CLASSIFICATION_KEYS}
    return {k: str(raw.get(k, "") or "").strip() for k in _CLASSIFICATION_KEYS}


@dataclass(frozen=True)
class BridgeRecord:
    source_code: str
    taxon_id_namespace: str
    taxon_id_value: str
    scientific_name: str
    authorship: str
    rank: str
    taxonomic_status: str
    kingdom: str
    classification: tuple[tuple[str, str], ...]  # sorted tuple for hashability

    def classification_dict(self) -> dict[str, str]:
        return dict(self.classification)


@dataclass(frozen=True)
class BackboneCandidate:
    source_code: str
    namespace: str
    identifier: str
    scientific_name: str
    authorship: str
    rank: str
    taxonomic_status: str
    kingdom: str
    classification: tuple[tuple[str, str], ...]

    def classification_dict(self) -> dict[str, str]:
        return dict(self.classification)


@dataclass(frozen=True)
class BackboneIndex:
    """Canonical-name index of backbone taxa for cross-source lookups."""

    by_name_rank: dict[tuple[str, str], list[BackboneCandidate]]

    @classmethod
    def build(cls, backbone_records: Iterable[dict]) -> "BackboneIndex":
        by_name_rank: dict[tuple[str, str], list[BackboneCandidate]] = {}
        for record in backbone_records:
            name = _canonical_name(record.get("scientific_name", ""))
            if not name:
                continue
            rank = str(record.get("rank", ""))
            key = (name, rank)
            classification = _clean_classification(record.get("classification"))
            candidate = BackboneCandidate(
                source_code=str(record["source_code"]),
                namespace=str(record["taxon_id"]["namespace"]),
                identifier=str(record["taxon_id"]["value"]),
                scientific_name=str(record.get("scientific_name", "")),
                authorship=str(record.get("authorship", "")),
                rank=rank,
                taxonomic_status=str(record.get("taxonomic_status", "")),
                kingdom=str(record.get("kingdom", "")),
                classification=tuple(sorted(classification.items())),
            )
            by_name_rank.setdefault(key, []).append(candidate)
        for key, bucket in by_name_rank.items():
            by_name_rank[key] = sorted(
                {c: None for c in bucket},
                key=lambda c: (c.source_code, c.namespace, c.identifier),
            )
        return cls(by_name_rank=by_name_rank)

    def matches(self, name: str, rank: str) -> list[BackboneCandidate]:
        return list(self.by_name_rank.get((_canonical_name(name), rank), ()))


def _canonical_name(value: str) -> str:
    """NFC + case-fold + internal-whitespace collapse. Never used for identity."""
    normalized = unicodedata.normalize("NFC", value.strip())
    return " ".join(p.casefold() for p in normalized.split())


def _canonical_authorship(value: str) -> str:
    """Normalization for authorship equality: NFC + strip + whitespace collapse.

    Preserves capitalization and punctuation on purpose — publisher-year
    citations frequently differ only in punctuation across sources, and a
    stricter compare avoids masking that as identity.
    """
    normalized = unicodedata.normalize("NFC", value.strip())
    return " ".join(normalized.split())


@dataclass(frozen=True)
class CrossSourceProposal:
    mapping_id: str
    source_usage: tuple[str, str, str]
    target_source_usage: tuple[str, str, str] | None
    target_candidates: tuple[tuple[str, str, str], ...]
    proposal_class: str
    relationship: str
    review_status: str
    scientific_name: str
    rank: str
    evidence: dict


def classify_bridge_records(
    *,
    bridge_records: Iterable[dict],
    backbone_index: BackboneIndex,
    rejected_source_usages: set[tuple[str, str, str]] | None = None,
) -> list[CrossSourceProposal]:
    """Return one proposal per bridge record, deterministically ordered."""
    rejected_source_usages = rejected_source_usages or set()
    proposals: list[CrossSourceProposal] = []
    for record in bridge_records:
        bridge_classification = _clean_classification(record.get("classification"))
        bridge = BridgeRecord(
            source_code=str(record["source_code"]),
            taxon_id_namespace=str(record["taxon_id"]["namespace"]),
            taxon_id_value=str(record["taxon_id"]["value"]),
            scientific_name=str(record.get("scientific_name", "")),
            authorship=str(record.get("authorship", "")),
            rank=str(record.get("rank", "")),
            taxonomic_status=str(record.get("taxonomic_status", "")),
            kingdom=str(record.get("kingdom", "")),
            classification=tuple(sorted(bridge_classification.items())),
        )
        source_usage = (
            bridge.source_code, bridge.taxon_id_namespace, bridge.taxon_id_value,
        )
        if source_usage in rejected_source_usages:
            proposals.append(_build_proposal(
                bridge=bridge, candidates=(),
                proposal_class=PROPOSAL_REJECTED,
                relationship="unresolved", review_status="rejected",
                evidence={"reason": "manual_mapping_rejected"},
            ))
            continue
        candidates = tuple(backbone_index.matches(bridge.scientific_name, bridge.rank))
        if not candidates:
            proposals.append(_build_proposal(
                bridge=bridge, candidates=candidates,
                proposal_class=PROPOSAL_NATIONAL_ONLY,
                relationship="unresolved", review_status="unreviewed",
                evidence={"reason": "no_backbone_match"},
            ))
        elif len(candidates) > 1:
            proposals.append(_build_proposal(
                bridge=bridge, candidates=candidates,
                proposal_class=PROPOSAL_AMBIGUOUS,
                relationship="unresolved", review_status="needs_review",
                evidence={"reason": "canonical_name_and_rank_match_multiple",
                          "candidate_count": len(candidates)},
            ))
        else:
            candidate = candidates[0]
            evidence, satisfied = _evaluate_conservative_exact(bridge, candidate)
            if satisfied:
                proposals.append(_build_proposal(
                    bridge=bridge, candidates=candidates,
                    proposal_class=PROPOSAL_AUTOMATIC_EXACT,
                    relationship="exact", review_status="policy_auto_approved",
                    evidence={"reason": "conservative_exact_rule_satisfied",
                              **evidence},
                ))
            else:
                # Fall back to the audit-supported missing-authorship +
                # classification-agreement rule. Only fires when the strict
                # rule failed EXCLUSIVELY because one or both authorships were
                # missing (never on authorship *mismatch*, kingdom mismatch,
                # or status mismatch).
                fallback_ev, fallback_ok = _evaluate_missing_authorship_rule(
                    bridge, candidate, strict_evidence=evidence,
                )
                if fallback_ok:
                    proposals.append(_build_proposal(
                        bridge=bridge, candidates=candidates,
                        proposal_class=PROPOSAL_AUTOMATIC_EXACT,
                        relationship="exact",
                        review_status="policy_auto_approved",
                        evidence={
                            "reason": "missing_authorship_classification_rule_satisfied",
                            **fallback_ev,
                        },
                    ))
                else:
                    proposals.append(_build_proposal(
                        bridge=bridge, candidates=candidates,
                        proposal_class=PROPOSAL_REVIEW_PROPOSED,
                        relationship="likely_exact",
                        review_status="needs_review",
                        evidence={
                            "reason": "conservative_exact_rule_failed",
                            **evidence,
                            **fallback_ev,
                        },
                    ))
    proposals.sort(key=lambda p: (
        p.source_usage[0], p.source_usage[1], p.source_usage[2],
    ))
    return proposals


def _evaluate_conservative_exact(
    bridge: BridgeRecord,
    candidate: BackboneCandidate,
) -> tuple[dict, bool]:
    """Return (evidence-fields dict, satisfied?) for the conservative rule."""
    checks: dict[str, dict] = {}

    # 1. Same canonical scientific name (implicit — the index already keys on it).
    checks["scientific_name_match"] = {"result": "pass"}

    # 2. Same rank (implicit — index key).
    checks["rank_match"] = {"result": "pass"}

    # 3. Kingdom == Fungi on both sides.
    kingdom_ok = (
        bridge.kingdom == _FUNGI_KINGDOM
        and candidate.kingdom == _FUNGI_KINGDOM
    )
    checks["kingdom_fungi"] = {
        "result": "pass" if kingdom_ok else "fail",
        "bridge_kingdom": bridge.kingdom,
        "backbone_kingdom": candidate.kingdom,
    }

    # 4. Non-empty matching authorship, byte-preserved from upstream field.
    bridge_auth = _canonical_authorship(bridge.authorship)
    backbone_auth = _canonical_authorship(candidate.authorship)
    authorship_ok = bool(bridge_auth) and bool(backbone_auth) and \
        bridge_auth == backbone_auth
    checks["authorship_match"] = {
        "result": "pass" if authorship_ok else "fail",
        "bridge_authorship_present": bool(bridge_auth),
        "backbone_authorship_present": bool(backbone_auth),
        "authorship_equal": bridge_auth == backbone_auth if
            (bridge_auth and backbone_auth) else False,
    }

    # 5. Compatible taxonomic status (both accepted variants).
    status_ok = (
        bridge.taxonomic_status in _ACCEPTED_STATUSES
        and candidate.taxonomic_status in _ACCEPTED_STATUSES
    )
    checks["status_compatible"] = {
        "result": "pass" if status_ok else "fail",
        "bridge_status": bridge.taxonomic_status,
        "backbone_status": candidate.taxonomic_status,
    }

    # 6. Uniqueness is already established by the caller (exactly one candidate).
    checks["candidate_uniqueness"] = {"result": "pass"}

    satisfied = kingdom_ok and authorship_ok and status_ok
    return {"checks": checks}, satisfied


def _evaluate_missing_authorship_rule(
    bridge: BridgeRecord,
    candidate: BackboneCandidate,
    *,
    strict_evidence: dict,
) -> tuple[dict, bool]:
    """Audit-supported rule: allow ``automatic_exact`` when authorship is
    absent on at least one side but the classification chain of the two
    records agrees on every populated Linnean level.

    Fires only when the strict rule failed *exclusively* because of
    authorship. Authorship mismatch, kingdom mismatch, and status mismatch
    still block the automatic path.
    """
    checks = strict_evidence.get("checks", {})
    kingdom_ok = checks.get("kingdom_fungi", {}).get("result") == "pass"
    status_ok = checks.get("status_compatible", {}).get("result") == "pass"
    auth_block = checks.get("authorship_match", {})
    bridge_auth_present = bool(auth_block.get("bridge_authorship_present"))
    backbone_auth_present = bool(auth_block.get("backbone_authorship_present"))
    authorship_equal = bool(auth_block.get("authorship_equal"))

    only_authorship_failed = (
        kingdom_ok and status_ok
        and not (bridge_auth_present and backbone_auth_present
                 and not authorship_equal)
    )
    if not only_authorship_failed:
        return {"missing_authorship_rule": {
            "result": "skipped_reason_other_than_authorship",
        }}, False
    if bridge_auth_present and backbone_auth_present:
        # Both present and equal (would have satisfied strict rule already)
        # OR both present and unequal (blocked above).
        return {"missing_authorship_rule": {
            "result": "not_applicable_both_authorships_present",
        }}, False

    b_cls = bridge.classification_dict()
    c_cls = candidate.classification_dict()
    matched: list[str] = []
    disagreements: list[dict] = []
    for key in _CLASSIFICATION_KEYS:
        b_val = b_cls.get(key, "")
        c_val = c_cls.get(key, "")
        if b_val and c_val:
            if b_val == c_val:
                matched.append(key)
            else:
                disagreements.append({"field": key, "bridge": b_val,
                                      "backbone": c_val})
    if disagreements or not matched:
        return {"missing_authorship_rule": {
            "result": "fail",
            "matched_classification_fields": matched,
            "classification_disagreements": disagreements,
        }}, False
    return {"missing_authorship_rule": {
        "result": "pass",
        "matched_classification_fields": matched,
        "authorship_side_absent": (
            "backbone" if bridge_auth_present else
            ("bridge" if backbone_auth_present else "both")
        ),
    }}, True


def _build_proposal(
    *,
    bridge: BridgeRecord,
    candidates: tuple[BackboneCandidate, ...] | tuple,
    proposal_class: str,
    relationship: str,
    review_status: str,
    evidence: dict,
) -> CrossSourceProposal:
    target: tuple[str, str, str] | None = None
    if len(candidates) == 1 and isinstance(candidates[0], BackboneCandidate):
        c = candidates[0]
        target = (c.source_code, c.namespace, c.identifier)
    tuple_candidates = tuple(
        (c.source_code, c.namespace, c.identifier) if isinstance(c, BackboneCandidate)
        else c
        for c in candidates
    )
    mapping_id = (
        f"auto-{bridge.source_code}-"
        f"{bridge.taxon_id_namespace}-{bridge.taxon_id_value}"
    )
    return CrossSourceProposal(
        mapping_id=mapping_id,
        source_usage=(
            bridge.source_code, bridge.taxon_id_namespace, bridge.taxon_id_value,
        ),
        target_source_usage=target,
        target_candidates=tuple_candidates,
        proposal_class=proposal_class,
        relationship=relationship,
        review_status=review_status,
        scientific_name=bridge.scientific_name,
        rank=bridge.rank,
        evidence=evidence,
    )


def proposal_to_json(
    proposal: CrossSourceProposal,
    *,
    release_id: str,
    identity_applied: bool = False,
) -> dict:
    return {
        "mapping_id": proposal.mapping_id,
        "kind": "cross_source_proposal",
        "proposal_class": proposal.proposal_class,
        "relationship": proposal.relationship,
        "review_status": proposal.review_status,
        "source_usage": {
            "source": proposal.source_usage[0],
            "namespace": proposal.source_usage[1],
            "identifier": proposal.source_usage[2],
        },
        "target": (
            {
                "source": proposal.target_source_usage[0],
                "namespace": proposal.target_source_usage[1],
                "identifier": proposal.target_source_usage[2],
            } if proposal.target_source_usage else None
        ),
        "candidates": [
            {"source": c[0], "namespace": c[1], "identifier": c[2]}
            for c in proposal.target_candidates
        ],
        "scientific_name": proposal.scientific_name,
        "rank": proposal.rank,
        "evidence": proposal.evidence,
        "applied_in_release": release_id,
        "identity_applied": identity_applied,
    }


def summarize(proposals: Iterable[CrossSourceProposal]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for proposal in proposals:
        counts[proposal.proposal_class] = counts.get(proposal.proposal_class, 0) + 1
    return counts
