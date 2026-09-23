#!/usr/bin/env python3
"""The reviewed cross-source bridge emission policy.

A cross-source identity binding (source A's usage bound to the Sporely concept
anchored by source B's usage) is recorded by the compiler in
``source_usages.jsonl`` with an ``identity_binding`` of ``alias`` and a
``bridge_evidence_class`` naming the evidence that produced it.

Being bound is not the same as being *authoritative*. A binding good enough to
carry a vernacular name onto a concept is not automatically good enough to be
published as a resolvable ``(source, namespace, external_id)`` identity. This
module owns that distinction: it loads the reviewed standard from
``policies/mapping_policy.yml.authoritative_bridge_emission`` and answers, for
one evidence class, whether the binding may be projected as an authoritative
external identifier.

The split is deliberate. The compiler records *facts* — which rule admitted
each binding — into the immutable release artifacts. The policy decides which
of those facts constitute identity, and is applied at projection time. Revising
the standard therefore does not require recompiling identity, and a release
always records the policy digest it was projected under.

Fail-closed: an evidence class the policy does not list is ineligible and is
reported as ``unclassified_evidence_class`` rather than silently grouped with a
class that was graded.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path


POLICY_KEY = "authoritative_bridge_emission"

#: Evidence class for a binding created by an approved ``manual_mappings.yml``
#: entry — the only class backed by a per-association human decision.
EVIDENCE_CLASS_MANUAL_APPROVED_EXACT = "manual_approved_exact"

#: Evidence class for a binding re-keyed by an approved concept supersession —
#: the reviewed merge case, where a concept that was already allocated its own
#: Sporely identity is superseded by a current concept.
EVIDENCE_CLASS_REVIEWED_SUPERSESSION = "reviewed_supersession"

#: Evidence class for a synonym usage bound to its accepted concept inside one
#: source. Intra-source, so not a cross-source bridge.
EVIDENCE_CLASS_INTRA_SOURCE_SYNONYM = "intra_source_synonym"

#: An anchor binding, or any binding carrying no cross-source evidence.
EVIDENCE_CLASS_NONE = ""

UNCLASSIFIED_REASON = "unclassified_evidence_class"


class BridgeEmissionError(ValueError):
    """Raised when the emission policy is missing, malformed, or ambiguous."""


@dataclass(frozen=True)
class BridgeEmissionPolicy:
    """The reviewed standard for projecting a bridge binding as identity."""

    eligible: frozenset[str]
    rejection_reasons: dict[str, str]
    policy_sha256: str
    policy_path: str

    @classmethod
    def load(cls, path: Path) -> "BridgeEmissionPolicy":
        if not path.exists():
            raise BridgeEmissionError(f"mapping policy not found: {path}")
        raw = path.read_bytes()
        try:
            document = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise BridgeEmissionError(f"{path}: malformed policy: {exc}") from exc
        if not isinstance(document, dict):
            raise BridgeEmissionError(f"{path}: expected a policy object")
        block = document.get(POLICY_KEY)
        if not isinstance(block, dict):
            raise BridgeEmissionError(
                f"{path}: missing required {POLICY_KEY!r} block. A projection "
                f"may not guess which bridge evidence is authoritative."
            )
        eligible: set[str] = set()
        for entry in cls._entries(path, block, "eligible"):
            eligible.add(entry)
        rejection_reasons: dict[str, str] = {}
        for entry, reason in cls._rejections(path, block):
            rejection_reasons[entry] = reason
        overlap = eligible & set(rejection_reasons)
        if overlap:
            raise BridgeEmissionError(
                f"{path}: evidence class(es) {sorted(overlap)!r} appear in both "
                f"the eligible and rejected lists; the standard is ambiguous"
            )
        if not eligible:
            raise BridgeEmissionError(
                f"{path}: {POLICY_KEY}.eligible is empty; a projection that "
                f"can emit nothing is a configuration error, not a policy"
            )
        return cls(
            eligible=frozenset(eligible),
            rejection_reasons=dict(rejection_reasons),
            policy_sha256=hashlib.sha256(raw).hexdigest(),
            policy_path=str(path),
        )

    @staticmethod
    def _entries(path: Path, block: dict, key: str) -> list[str]:
        raw = block.get(key)
        if not isinstance(raw, list):
            raise BridgeEmissionError(
                f"{path}: {POLICY_KEY}.{key} must be a list"
            )
        out: list[str] = []
        for index, entry in enumerate(raw):
            if not isinstance(entry, dict) or "evidence_class" not in entry:
                raise BridgeEmissionError(
                    f"{path}: {POLICY_KEY}.{key}[{index}] needs an "
                    f"'evidence_class'"
                )
            out.append(str(entry["evidence_class"]))
        return out

    @classmethod
    def _rejections(cls, path: Path, block: dict) -> list[tuple[str, str]]:
        raw = block.get("rejected")
        if not isinstance(raw, list):
            raise BridgeEmissionError(
                f"{path}: {POLICY_KEY}.rejected must be a list"
            )
        out: list[tuple[str, str]] = []
        for index, entry in enumerate(raw):
            if not isinstance(entry, dict) or "evidence_class" not in entry:
                raise BridgeEmissionError(
                    f"{path}: {POLICY_KEY}.rejected[{index}] needs an "
                    f"'evidence_class'"
                )
            reason = str(entry.get("reason") or "").strip()
            if not reason:
                raise BridgeEmissionError(
                    f"{path}: {POLICY_KEY}.rejected[{index}] needs a 'reason'. "
                    f"A rejected class must record why, so the coverage audit "
                    f"can explain every unemitted binding."
                )
            out.append((str(entry["evidence_class"]), reason))
        return out

    def is_eligible(self, evidence_class: str) -> bool:
        """Whether a binding with this evidence class may be emitted."""
        return str(evidence_class or "") in self.eligible

    def rejection_reason(self, evidence_class: str) -> str:
        """Why a non-eligible class is not emitted.

        Returns :data:`UNCLASSIFIED_REASON` for a class the policy never
        graded, which is a finding rather than a silent pass.
        """
        key = str(evidence_class or "")
        if key in self.eligible:
            raise BridgeEmissionError(
                f"{key!r} is eligible; it has no rejection reason"
            )
        return self.rejection_reasons.get(key, UNCLASSIFIED_REASON)
