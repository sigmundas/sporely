"""Reconciliation input / output data model.

Frozen dataclasses model every value the engine consumes and emits. Every
dataclass exposes a ``to_dict`` method that returns a ``dict`` with the
exact key order documented in the W2D contract (§2, §6, §8). Callers must
never re-key or re-order.

Serialisation is byte-deterministic when combined with
``json.dumps(..., sort_keys=False, ensure_ascii=False)`` because the dicts
themselves preserve insertion order and every list is sorted by a
documented tuple.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


def _null_or_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


@dataclass(frozen=True, slots=True)
class RawSignal:
    """One namespaced or text-only historical signal.

    Field order follows W2D contract §2. ``rule_id`` records which
    namespace-derivation rule from the policy produced this signal.
    """

    kind: str
    source_system: str | None
    namespace: str | None
    external_id: str | None
    origin_field: str
    raw_value: str | None
    rule_id: str
    notes: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "external_id": self.external_id,
            "kind": self.kind,
            "namespace": self.namespace,
            "notes": self.notes,
            "origin_field": self.origin_field,
            "raw_value": self.raw_value,
            "rule_id": self.rule_id,
            "source_system": self.source_system,
        }

    def sort_key(self) -> tuple[str, str, str, str]:
        # Signals are sorted by (source_system, namespace, external_id,
        # origin_field). Empty strings substitute for null so the tuple
        # ordering is deterministic across Python versions.
        return (
            self.source_system or "",
            self.namespace or "",
            self.external_id or "",
            self.origin_field,
        )


@dataclass(frozen=True, slots=True)
class ChainStep:
    """One evidence step in the resolver chain.

    ``level`` mirrors the resolver hierarchy (1..4 for identity-creating
    levels; 5 is candidate-only and 6 is preserve-unresolved). Every
    resolved record must expose at least one chain step; the engine hard-
    fails otherwise.
    """

    level: int
    method: str
    action: str
    source_system: str | None
    namespace: str | None
    external_id: str | None
    resolved_taxon_id: int | None
    note: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "external_id": self.external_id,
            "level": self.level,
            "method": self.method,
            "namespace": self.namespace,
            "note": self.note,
            "resolved_taxon_id": self.resolved_taxon_id,
            "source_system": self.source_system,
        }


@dataclass(frozen=True, slots=True)
class Candidate:
    """A Level-5 candidate concept. Candidates never assign identity."""

    sporely_taxon_id: int
    canonical_name: str
    rank: str | None
    scope_state: str
    match_kind: str
    match_field: str
    match_value: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "canonical_name": self.canonical_name,
            "match_field": self.match_field,
            "match_kind": self.match_kind,
            "match_value": self.match_value,
            "rank": self.rank,
            "scope_state": self.scope_state,
            "sporely_taxon_id": self.sporely_taxon_id,
        }

    def sort_key(self) -> tuple[int, str, str]:
        return (self.sporely_taxon_id, self.match_kind, self.match_field)


@dataclass(frozen=True, slots=True)
class ReconciliationInput:
    """One anonymised historical observation.

    Field order follows W2D contract §2. ``signals`` is a tuple so the
    dataclass remains hashable.
    """

    observation_id: str
    signals: tuple[RawSignal, ...] = field(default_factory=tuple)
    manual_identification_flag: bool = False
    stored_scientific_name: str | None = None
    stored_vernacular_name: str | None = None
    stored_rank: str | None = None
    source_release_or_timestamp: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "manual_identification_flag": self.manual_identification_flag,
            "observation_id": self.observation_id,
            "signals": [s.to_dict() for s in sorted(self.signals, key=RawSignal.sort_key)],
            "source_release_or_timestamp": self.source_release_or_timestamp,
            "stored_rank": self.stored_rank,
            "stored_scientific_name": self.stored_scientific_name,
            "stored_vernacular_name": self.stored_vernacular_name,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ReconciliationInput":
        """Build a ``ReconciliationInput`` from a plain-dict payload.

        Unknown keys are ignored so future contract-extension fields can be
        rolled in without breaking older callers. The ``__synthetic__``
        sentinel line used in fixture files is filtered before we reach
        here (see ``cli._iter_input``).
        """
        raw_signals: list[RawSignal] = []
        for row in data.get("signals") or ():
            raw_signals.append(
                RawSignal(
                    kind=str(row.get("kind") or ""),
                    source_system=_null_or_str(row.get("source_system")),
                    namespace=_null_or_str(row.get("namespace")),
                    external_id=_null_or_str(row.get("external_id")),
                    origin_field=str(row.get("origin_field") or ""),
                    raw_value=_null_or_str(row.get("raw_value")),
                    rule_id=str(row.get("rule_id") or ""),
                    notes=_null_or_str(row.get("notes")),
                )
            )
        return cls(
            observation_id=str(data["observation_id"]),
            signals=tuple(raw_signals),
            manual_identification_flag=bool(data.get("manual_identification_flag")),
            stored_scientific_name=_null_or_str(data.get("stored_scientific_name")),
            stored_vernacular_name=_null_or_str(data.get("stored_vernacular_name")),
            stored_rank=_null_or_str(data.get("stored_rank")),
            source_release_or_timestamp=_null_or_str(data.get("source_release_or_timestamp")),
        )


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    """One reconciliation outcome per observation.

    Field order follows W2D contract §6 exactly. Every field is emitted —
    ``null`` and empty arrays are never elided.
    """

    observation_id: str
    reconciliation_state: str
    resolved_sporely_taxon_id: int | None
    resolved_canonical_name: str | None
    resolved_rank: str | None
    resolved_scope_state: str | None
    resolved_cache_state: str | None
    resolution_method: str | None
    resolution_evidence: tuple[ChainStep, ...]
    original_legacy_taxon_id: str | None
    original_scientific_name: str | None
    original_vernacular_name: str | None
    original_source_system: str | None
    original_source_namespace: str | None
    original_external_id: str | None
    signals_all: tuple[RawSignal, ...]
    unmapped_signals: tuple[RawSignal, ...]
    candidate_concepts: tuple[Candidate, ...]
    conflicting_concepts: tuple[Candidate, ...]
    missing_source_records: tuple[str, ...]
    review_reason: str | None
    migration_action: str

    def to_dict(self) -> dict[str, Any]:
        signals_sorted = sorted(self.signals_all, key=RawSignal.sort_key)
        unmapped_sorted = sorted(self.unmapped_signals, key=RawSignal.sort_key)
        candidate_sorted = sorted(self.candidate_concepts, key=Candidate.sort_key)
        conflicting_sorted = sorted(self.conflicting_concepts, key=Candidate.sort_key)
        # Contract §6: the field order is fixed. ``resolved_scope_state`` is a
        # deterministic extension recorded here to satisfy the "outside cache"
        # scope-preservation requirement (contract §7). It is inserted
        # immediately after ``resolved_rank`` so callers can extract the
        # verbatim scope_state without consulting the release directly.
        return {
            "candidate_concepts": [c.to_dict() for c in candidate_sorted],
            "conflicting_concepts": [c.to_dict() for c in conflicting_sorted],
            "migration_action": self.migration_action,
            "missing_source_records": list(sorted(self.missing_source_records)),
            "observation_id": self.observation_id,
            "original_external_id": self.original_external_id,
            "original_legacy_taxon_id": self.original_legacy_taxon_id,
            "original_scientific_name": self.original_scientific_name,
            "original_source_namespace": self.original_source_namespace,
            "original_source_system": self.original_source_system,
            "original_vernacular_name": self.original_vernacular_name,
            "reconciliation_state": self.reconciliation_state,
            "resolution_evidence": [s.to_dict() for s in self.resolution_evidence],
            "resolution_method": self.resolution_method,
            "resolved_cache_state": self.resolved_cache_state,
            "resolved_canonical_name": self.resolved_canonical_name,
            "resolved_rank": self.resolved_rank,
            "resolved_scope_state": self.resolved_scope_state,
            "resolved_sporely_taxon_id": self.resolved_sporely_taxon_id,
            "review_reason": self.review_reason,
            "signals_all": [s.to_dict() for s in signals_sorted],
            "unmapped_signals": [s.to_dict() for s in unmapped_sorted],
        }

    def sort_key(self) -> str:
        return self.observation_id
