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

#: What an *approved* reviewed relationship must carry, non-empty, before any
#: compiler applies it.
#:
#: The whole eligibility standard rests on the claim that a published
#: relationship was reviewed by a person. If flipping ``review_status`` to
#: ``approved`` were enough on its own, that claim would be unfalsifiable: a
#: record could activate a concept merge while naming no reviewer, giving no
#: rationale, and citing no evidence. Requiring the provenance is what makes
#: "reviewed" mean something a later reader can check and, if necessary,
#: dispute.
REVIEW_PROVENANCE_FIELDS = ("reviewer", "rationale", "evidence_references")

APPROVED_REVIEW_STATUS = "approved"


def missing_review_provenance(entry: dict) -> list[str]:
    """Return the provenance fields an approved record leaves empty.

    Presence is not enough — a field present but blank carries no provenance.
    ``evidence_references`` must be a list holding at least one non-blank
    reference. Returns ``[]`` for a record that is not approved, because only
    an applied record makes a review claim.
    """
    if str(entry.get("review_status") or "") != APPROVED_REVIEW_STATUS:
        return []
    missing: list[str] = []
    for field in REVIEW_PROVENANCE_FIELDS:
        value = entry.get(field)
        if field == "evidence_references":
            if not isinstance(value, list) or not [
                item for item in value if str(item or "").strip()
            ]:
                missing.append(field)
        elif not str(value or "").strip():
            missing.append(field)
    return missing


class BridgeEmissionError(ValueError):
    """Raised when the emission policy is missing, malformed, or ambiguous."""


_REPO_ROOT = Path(__file__).resolve().parents[3]

#: What an owner's batch approval of a review manifest must record.
MANIFEST_APPROVAL_FIELDS = (
    "path", "file_sha256", "approved_by", "approved_at", "decision_reference",
)

#: The manifest columns a batch-approved record is checked against.
MANIFEST_MEMBER_COLUMNS = ("nortaxa_taxon_id", "col_usage_id", "sporely_taxon_id")


def verify_manifest_approvals(
    document: dict, *, repo_root: Path = _REPO_ROOT,
) -> dict[str, str]:
    """Check every batch-approved mapping against the manifest it cites.

    An owner may approve an explicitly enumerated, immutable review manifest
    with one decision (taxonomy-v3 decision 2). That decision is recorded once,
    in the ledger's ``approved_manifests`` list, bound to the manifest file's
    SHA-256. It still yields one per-association record per member, and each
    such record carries ``approved_manifest`` naming the ``file_sha256`` and
    the member it was generated from.

    The approval is a claim about an exact file, so it is checked against that
    file: the manifest must exist and hash to the approved ``file_sha256``, and
    every record citing it must be an approved exact mapping whose source
    usage, target and Sporely concept are exactly one listed member. A record
    for a taxon the manifest does not list — a sibling review class, or a new
    taxon in a later source release — therefore cannot borrow the approval.

    Returns ``mapping_id -> file_sha256`` for the manifest-bound records.
    Raises :class:`BridgeEmissionError` on any mismatch.
    """
    approvals = document.get("approved_manifests") or []
    if not isinstance(approvals, list):
        raise BridgeEmissionError("approved_manifests must be a list")
    members_by_sha: dict[str, set[tuple[str, str, int]]] = {}
    for index, approval in enumerate(approvals):
        if not isinstance(approval, dict):
            raise BridgeEmissionError(
                f"approved_manifests[{index}] is not an object")
        absent = [f for f in MANIFEST_APPROVAL_FIELDS
                  if not str(approval.get(f) or "").strip()]
        if absent:
            raise BridgeEmissionError(
                f"approved_manifests[{index}] carries no {', '.join(absent)}"
            )
        expected = str(approval["file_sha256"])
        if expected in members_by_sha:
            raise BridgeEmissionError(
                f"approved_manifests lists file_sha256 {expected} twice")
        path = Path(str(approval["path"]))
        if not path.is_absolute():
            path = repo_root / path
        if not path.is_file():
            raise BridgeEmissionError(
                f"approved manifest not found: {path}. An approval that cannot "
                f"be checked against its file approves nothing."
            )
        raw = path.read_bytes()
        actual = hashlib.sha256(raw).hexdigest()
        if actual != expected:
            raise BridgeEmissionError(
                f"approved manifest {path} has sha256 {actual}, but the "
                f"approval names {expected}. A regenerated manifest is not the "
                f"one that was approved, even with the same members."
            )
        manifest = json.loads(raw.decode("utf-8"))
        columns = list(manifest.get("columns") or [])
        missing_columns = [c for c in MANIFEST_MEMBER_COLUMNS if c not in columns]
        if missing_columns:
            raise BridgeEmissionError(
                f"approved manifest {path} lacks columns {missing_columns}")
        positions = [columns.index(c) for c in MANIFEST_MEMBER_COLUMNS]
        members_by_sha[expected] = {
            (str(row[positions[0]]), str(row[positions[1]]),
             int(row[positions[2]]))
            for row in manifest.get("members") or []
        }

    bound: dict[str, str] = {}
    seen_members: set[tuple[str, tuple[str, str, int]]] = set()
    for index, entry in enumerate(document.get("mappings") or []):
        if not isinstance(entry, dict) or "approved_manifest" not in entry:
            continue
        mapping_id = str(entry.get("mapping_id") or f"mapping-{index}")
        ref = entry.get("approved_manifest")
        if not isinstance(ref, dict):
            raise BridgeEmissionError(
                f"mapping {mapping_id!r}: approved_manifest is not an object")
        file_sha256 = str(ref.get("file_sha256") or "")
        if file_sha256 not in members_by_sha:
            raise BridgeEmissionError(
                f"mapping {mapping_id!r} cites manifest {file_sha256!r}, "
                f"which no approved_manifests entry approves"
            )
        if str(entry.get("review_status") or "") != APPROVED_REVIEW_STATUS \
                or str(entry.get("relationship") or "") != "exact":
            raise BridgeEmissionError(
                f"mapping {mapping_id!r}: a manifest approval yields only "
                f"approved exact mappings"
            )
        member = ref.get("member") or {}
        try:
            key = (str(member["nortaxa_taxon_id"]), str(member["col_usage_id"]),
                   int(member["sporely_taxon_id"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise BridgeEmissionError(
                f"mapping {mapping_id!r}: approved_manifest.member needs "
                f"{', '.join(MANIFEST_MEMBER_COLUMNS)}"
            ) from exc
        source_usage = entry.get("source_usage") or {}
        target = (entry.get("target") or {}).get("source_usage") or {}
        if (str(source_usage.get("source")), str(source_usage.get("namespace")),
                str(source_usage.get("identifier"))) \
                != ("nortaxa", "nortaxa_taxon_id", key[0]) \
                or str(target.get("source")) != "col_xr" \
                or str(target.get("identifier")) != key[1]:
            raise BridgeEmissionError(
                f"mapping {mapping_id!r}: source usage and target do not match "
                f"the manifest member it cites ({key[0]} -> {key[1]})"
            )
        if key not in members_by_sha[file_sha256]:
            raise BridgeEmissionError(
                f"mapping {mapping_id!r}: NorTaxa {key[0]} -> COL {key[1]} -> "
                f"sporely_taxon_id {key[2]} is not a member of approved "
                f"manifest {file_sha256}"
            )
        if (file_sha256, key) in seen_members:
            raise BridgeEmissionError(
                f"mapping {mapping_id!r}: manifest member {key!r} has more "
                f"than one record"
            )
        if mapping_id in bound:
            raise BridgeEmissionError(
                f"mapping_id {mapping_id!r} is used by more than one record")
        seen_members.add((file_sha256, key))
        bound[mapping_id] = file_sha256
    return bound


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
