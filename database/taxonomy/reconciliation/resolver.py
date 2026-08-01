"""Deterministic 6-level resolver.

The resolver runs strictly against the pinned macrofungi release and an
optional canonical identity registry. It never mutates inputs; it emits
one :class:`ReconciliationResult` per :class:`ReconciliationInput`.

Contract references:

* §4 defines the 11 primary states,
* §5 defines the 6-level resolution hierarchy,
* §7 defines the migration-action classes,
* §10 defines the forbidden behaviours (scientific-name equality,
  invented source priority, etc.).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Iterable

from database.taxonomy.reconciliation.candidates import generate_candidates
from database.taxonomy.reconciliation.errors import (
    ReconciliationInvariantError,
)
from database.taxonomy.reconciliation.input_model import (
    Candidate,
    ChainStep,
    RawSignal,
    ReconciliationInput,
    ReconciliationResult,
)
from database.taxonomy.reconciliation.namespace_rules import NamespaceRuleSet
from database.taxonomy.reconciliation.sources import PinnedRelease

logger = logging.getLogger(__name__)


_STATES_WITH_IDENTITY = frozenset(
    {
        "resolved_exact",
        "resolved_exact_via_legacy_mapping",
        "resolved_exact_via_synonym_relationship",
    }
)


@dataclass(frozen=True, slots=True)
class _SignalMatch:
    """Intermediate resolution of one exact signal."""

    signal: RawSignal
    taxon_id: int
    step: ChainStep
    level: int
    method: str


@dataclass
class Resolver:
    """Stateless resolver over a pinned release + a policy rule set.

    Instances hold only the loaded release / policy references; calling
    :meth:`resolve` many times is safe and free of side effects.
    """

    release: PinnedRelease
    rule_set: NamespaceRuleSet
    trusted_secondary_providers: frozenset[str] = field(init=False)

    def __post_init__(self) -> None:
        # dataclass default-factory with a computed value from another field
        # is awkward; use post-init to freeze the set once.
        object.__setattr__(
            self,
            "trusted_secondary_providers",
            self.rule_set.trusted_secondary_providers(),
        )

    def resolve(self, observation: ReconciliationInput) -> ReconciliationResult:
        """Return the reconciliation result for one observation.

        Deterministic and side-effect free. Every hard-fail invariant
        (contract §14) raises :class:`ReconciliationInvariantError`.
        """
        signals = tuple(observation.signals)
        exact_signals = [s for s in signals if s.kind == "exact"]
        invalid_signals = [s for s in signals if s.kind == "invalid"]

        # Resolve every exact signal against the release. Preserve order of
        # first insertion; sorting is done at manifest-emit time.
        matches: list[_SignalMatch] = []
        unmapped: list[RawSignal] = []
        missing_source_records: list[str] = []
        for signal in exact_signals:
            match = self._resolve_signal(signal)
            if match is not None:
                matches.append(match)
            else:
                unmapped.append(signal)
                # Legacy sporely_taxon_id signals that miss the pinned
                # release mean the source record is unrecoverable (contract
                # §4 source_record_missing).
                if (
                    signal.source_system == "sporely"
                    and signal.namespace == "sporely_taxon_id"
                ):
                    missing_source_records.append(
                        f"{signal.origin_field}:{signal.external_id}"
                    )

        primary_state, method, chain, resolved_taxon_id, conflicting = self._pick_primary(
            observation,
            matches=matches,
            unmapped=unmapped,
            invalid_signals=invalid_signals,
        )

        # Level 5 — generate candidates for text signals. Never assigns
        # identity, so the primary state never changes here. Skip when we
        # already resolved: the caller has identity, additional candidates
        # would only pollute review queues.
        candidates: tuple[Candidate, ...] = ()
        if resolved_taxon_id is None:
            candidates = generate_candidates(
                signals=signals,
                stored_rank=observation.stored_rank,
                release=self.release,
                exclude_taxon_ids=frozenset({m.taxon_id for m in matches}),
            )
            if candidates and primary_state == "manual_unresolved":
                if len(candidates) > 1:
                    primary_state = "ambiguous_multiple_candidates"

        # Compose the result. Every field is populated even when null.
        resolved_concept = (
            self.release.concept(resolved_taxon_id) if resolved_taxon_id is not None else None
        )
        original_primary = _pick_primary_original(signals)
        review_reason = _review_reason(primary_state, candidates, matches, invalid_signals)
        migration_action = _migration_action(primary_state)

        result = ReconciliationResult(
            observation_id=observation.observation_id,
            reconciliation_state=primary_state,
            resolved_sporely_taxon_id=resolved_taxon_id,
            resolved_canonical_name=(
                resolved_concept.canonical_scientific_name if resolved_concept else None
            ),
            resolved_rank=(resolved_concept.taxon_rank if resolved_concept else None),
            resolved_scope_state=(
                resolved_concept.scope_state if resolved_concept else None
            ),
            resolution_method=method,
            resolution_evidence=tuple(chain),
            original_legacy_taxon_id=_original_legacy_taxon_id(signals),
            original_scientific_name=(
                observation.stored_scientific_name
                or _first_text_value(signals, "observations.ai_selected_scientific_name")
                or _first_text_value(signals, "observations.scientific_name_snapshot")
                or _first_text_value(signals, "observations.genus+species")
            ),
            original_vernacular_name=(
                observation.stored_vernacular_name
                or _first_text_value(signals, "observations.common_name")
            ),
            original_source_system=(original_primary.source_system if original_primary else None),
            original_source_namespace=(original_primary.namespace if original_primary else None),
            original_external_id=(original_primary.external_id if original_primary else None),
            signals_all=signals,
            unmapped_signals=tuple(unmapped),
            candidate_concepts=candidates,
            conflicting_concepts=tuple(conflicting),
            missing_source_records=tuple(sorted(set(missing_source_records))),
            review_reason=review_reason,
            migration_action=migration_action,
        )
        _check_invariants(result)
        return result

    # ------------------------------------------------------------------

    def _resolve_signal(self, signal: RawSignal) -> _SignalMatch | None:
        source = signal.source_system
        namespace = signal.namespace
        ext = signal.external_id
        if not (source and namespace and ext):
            return None

        # Level 1: direct taxonomy_v2 mapping.
        row = self.release.lookup_exact(source, namespace, ext)
        if row is not None:
            # Distinguish Levels 1 vs 2 vs 4 by the signal's provenance:
            #   * sporely_taxon_id signals → Level 2 (legacy chain verified);
            #   * trusted-secondary-provider signals that hit the release's
            #     accepted index → Level 4;
            #   * everything else (col_xr:col_usage_id, or legacy adb_taxon_id
            #     that happens to be in the release) → Level 1.
            if source == "sporely" and namespace == "sporely_taxon_id":
                level = 2
                method = "legacy_lookup_chain"
                action = "verify_sporely_taxon_id_in_release"
            elif source in self.trusted_secondary_providers and source != "col_xr":
                # Provider signals resolved via the release's own mapping
                # are considered trusted-secondary. If they had hit only
                # the canonical registry (no release row), that would still
                # be Level 4 — see the registry branch below.
                level = 4
                method = "trusted_secondary_provider_mapping"
                action = "match_taxon_external_id"
            else:
                level = 1
                method = "direct_taxonomy_v2_mapping"
                action = "match_taxon_external_id"
            step = ChainStep(
                level=level,
                method=method,
                action=action,
                source_system=source,
                namespace=namespace,
                external_id=ext,
                resolved_taxon_id=row.taxon_id,
                note=(
                    f"id_role=accepted; is_preferred={row.is_preferred}"
                ),
            )
            return _SignalMatch(signal=signal, taxon_id=row.taxon_id, step=step, level=level, method=method)

        # Level 3: pinned synonym / name-usage relationship.
        synonym_rows = self.release.lookup_synonym(source, namespace, ext)
        # Only accept when the synonym set points to exactly one accepted
        # taxon_id — the contract forbids collapsing multiple homotypic
        # synonyms into one identity.
        synonym_targets = sorted({r.taxon_id for r in synonym_rows})
        if len(synonym_targets) == 1:
            target_id = synonym_targets[0]
            if target_id in self.release.taxa_by_id:
                step = ChainStep(
                    level=3,
                    method="pinned_synonym_relationship",
                    action="follow_name_usage_to_accepted",
                    source_system=source,
                    namespace=namespace,
                    external_id=ext,
                    resolved_taxon_id=target_id,
                    note=f"id_role={synonym_rows[0].id_role}",
                )
                return _SignalMatch(
                    signal=signal,
                    taxon_id=target_id,
                    step=step,
                    level=3,
                    method="pinned_synonym_relationship",
                )

        # Level 4: canonical registry (if loaded).
        registry_id = self.release.lookup_registry(source, namespace, ext)
        if registry_id is not None and registry_id in self.release.taxa_by_id:
            step = ChainStep(
                level=4,
                method="trusted_secondary_provider_mapping",
                action="lookup_canonical_registry",
                source_system=source,
                namespace=namespace,
                external_id=ext,
                resolved_taxon_id=registry_id,
                note="matched via canonical registry; concept present in pinned release",
            )
            return _SignalMatch(
                signal=signal,
                taxon_id=registry_id,
                step=step,
                level=4,
                method="trusted_secondary_provider_mapping",
            )

        return None

    def _pick_primary(
        self,
        observation: ReconciliationInput,
        *,
        matches: list[_SignalMatch],
        unmapped: list[RawSignal],
        invalid_signals: list[RawSignal],
    ) -> tuple[str, str | None, list[ChainStep], int | None, list[Candidate]]:
        """Aggregate signal-level matches into a single primary state.

        See contract §5 for the multi-signal handling rules.
        """
        if matches:
            # Group by target taxon.
            taxa_hit = sorted({m.taxon_id for m in matches})
            if len(taxa_hit) > 1:
                # Conflicting exact evidence: emit every candidate concept.
                conflicting = []
                for match in matches:
                    concept = self.release.concept(match.taxon_id)
                    if concept is None:
                        continue
                    conflicting.append(
                        Candidate(
                            sporely_taxon_id=concept.taxon_id,
                            canonical_name=concept.canonical_scientific_name,
                            rank=concept.taxon_rank,
                            scope_state=concept.scope_state,
                            match_kind="conflicting_exact_signal",
                            match_field=match.signal.origin_field,
                            match_value=match.signal.external_id or "",
                        )
                    )
                chain = [m.step for m in matches]
                return (
                    "conflicting_exact_evidence",
                    None,
                    chain,
                    None,
                    conflicting,
                )
            resolved_taxon_id = taxa_hit[0]
            # All exact signals agree — pick the highest-priority level
            # among the participating chains.
            best = min(matches, key=lambda m: m.level)
            state_map = {
                1: "resolved_exact",
                2: "resolved_exact_via_legacy_mapping",
                3: "resolved_exact_via_synonym_relationship",
                4: "resolved_exact",  # trusted_secondary — still resolved_exact per §5
            }
            primary_state = state_map[best.level]
            chain = [m.step for m in matches]
            return primary_state, best.method, chain, resolved_taxon_id, []

        # No matches: decide unresolved flavour.
        if unmapped:
            # If any unmapped signal is a legacy sporely_taxon_id → source_record_missing.
            for signal in unmapped:
                if (
                    signal.source_system == "sporely"
                    and signal.namespace == "sporely_taxon_id"
                ):
                    return (
                        "source_record_missing",
                        None,
                        [
                            ChainStep(
                                level=6,
                                method="preserve_unresolved",
                                action="sporely_taxon_id_not_in_pinned_release",
                                source_system=signal.source_system,
                                namespace=signal.namespace,
                                external_id=signal.external_id,
                                resolved_taxon_id=None,
                                note="legacy sporely_taxon_id absent from taxonomy_v2 taxon.jsonl",
                            )
                        ],
                        None,
                        [],
                    )
            # Legacy chains that fail land as unresolved_legacy_identifier.
            # Only truly legacy fields (contract §3 Level-2 rules) escalate
            # to this state; every other unmapped namespaced signal is a
            # Level-1/4 unresolved_external_identifier.
            for signal in unmapped:
                if signal.rule_id in {"legacy_adb_taxon_id_v1"}:
                    return (
                        "unresolved_legacy_identifier",
                        None,
                        [
                            ChainStep(
                                level=6,
                                method="preserve_unresolved",
                                action="legacy_lookup_chain_no_match",
                                source_system=signal.source_system,
                                namespace=signal.namespace,
                                external_id=signal.external_id,
                                resolved_taxon_id=None,
                                note="legacy lookup produced no mapping to a pinned concept",
                            )
                        ],
                        None,
                        [],
                    )
            # Otherwise unresolved_external_identifier: namespaced signal
            # whose id is not in the pinned release.
            first = unmapped[0]
            return (
                "unresolved_external_identifier",
                None,
                [
                    ChainStep(
                        level=6,
                        method="preserve_unresolved",
                        action="external_identifier_not_in_pinned_release",
                        source_system=first.source_system,
                        namespace=first.namespace,
                        external_id=first.external_id,
                        resolved_taxon_id=None,
                        note="signal preserved; no pinned mapping found",
                    )
                ],
                None,
                [],
            )

        if invalid_signals:
            first = invalid_signals[0]
            return (
                "invalid_or_unnamespaced_identifier",
                None,
                [
                    ChainStep(
                        level=6,
                        method="preserve_unresolved",
                        action="invalid_or_unnamespaced_identifier",
                        source_system=first.source_system,
                        namespace=first.namespace,
                        external_id=first.external_id,
                        resolved_taxon_id=None,
                        note=first.notes or "unknown service or ambiguous prefix",
                    )
                ],
                None,
                [],
            )

        if observation.manual_identification_flag or _has_text_signal(observation.signals):
            return (
                "manual_unresolved",
                None,
                [
                    ChainStep(
                        level=6,
                        method="preserve_unresolved",
                        action="manual_identification_preserved",
                        source_system=None,
                        namespace=None,
                        external_id=None,
                        resolved_taxon_id=None,
                        note="text-only signals; identity not derivable",
                    )
                ],
                None,
                [],
            )

        return (
            "no_identity_evidence",
            None,
            [
                ChainStep(
                    level=6,
                    method="preserve_unresolved",
                    action="no_identity_evidence",
                    source_system=None,
                    namespace=None,
                    external_id=None,
                    resolved_taxon_id=None,
                    note="observation carried no exact or text identity signal",
                )
            ],
            None,
            [],
        )


# ---------------------------------------------------------------------------


def _has_text_signal(signals: Iterable[RawSignal]) -> bool:
    return any(s.kind == "text-only" and (s.raw_value or "").strip() for s in signals)


def _first_text_value(signals: Iterable[RawSignal], origin: str) -> str | None:
    for signal in signals:
        if signal.origin_field == origin and signal.raw_value:
            return signal.raw_value
    return None


def _pick_primary_original(signals: Iterable[RawSignal]) -> RawSignal | None:
    """Choose one signal to report in the ``original_*`` columns.

    Priority: sporely > nortaxa > col_xr > artportalen > inaturalist. Ties
    broken by ``(source_system, namespace, external_id)`` — same key used
    everywhere else for determinism.
    """
    order = {
        "sporely": 0,
        "nortaxa": 1,
        "col_xr": 2,
        "artportalen": 3,
        "inaturalist": 4,
        "artsorakel": 5,
        "mushroomobserver": 6,
    }
    exact = [s for s in signals if s.kind == "exact"]
    if not exact:
        # Fall back to a preserve_only or invalid signal so at least the
        # namespace triple is recorded.
        others = [s for s in signals if s.kind in ("preserve_only", "invalid")]
        if not others:
            return None
        others.sort(key=lambda s: (order.get(s.source_system or "", 99), s.sort_key()))
        return others[0]
    exact.sort(key=lambda s: (order.get(s.source_system or "", 99), s.sort_key()))
    return exact[0]


def _original_legacy_taxon_id(signals: Iterable[RawSignal]) -> str | None:
    for signal in signals:
        if signal.source_system == "sporely" and signal.namespace == "sporely_taxon_id":
            return signal.external_id
    for signal in signals:
        if signal.rule_id in {"legacy_adb_taxon_id_v1", "artsdata_id_v1"}:
            return signal.external_id
    return None


def _review_reason(
    primary_state: str,
    candidates: tuple[Candidate, ...],
    matches: list[_SignalMatch],
    invalid_signals: list[RawSignal],
) -> str | None:
    if primary_state == "ambiguous_multiple_candidates":
        return f"{len(candidates)} scientific-name candidates without identity binding"
    if primary_state == "conflicting_exact_evidence":
        return (
            f"{len({m.taxon_id for m in matches})} distinct concepts across "
            f"{len(matches)} exact signals"
        )
    if primary_state == "manual_unresolved":
        return "text-only signals; awaiting manual identification"
    if primary_state == "invalid_or_unnamespaced_identifier":
        return "one or more signals could not be namespaced"
    if primary_state == "unresolved_external_identifier":
        return "namespaced id not present in pinned release"
    if primary_state == "unresolved_legacy_identifier":
        return "legacy id could not chain to a pinned concept"
    if primary_state == "source_record_missing":
        return "prepopulated sporely_taxon_id no longer present in pinned release"
    if primary_state == "no_identity_evidence":
        return "no identity or text signals available"
    return None


def _migration_action(primary_state: str) -> str:
    if primary_state in _STATES_WITH_IDENTITY:
        return "materialize_existing_taxonomy_v2_concept"
    if primary_state in {"ambiguous_multiple_candidates", "conflicting_exact_evidence"}:
        return "manual_review_required"
    return "retain_unresolved_without_registry_concept"


def _check_invariants(result: ReconciliationResult) -> None:
    if result.reconciliation_state in _STATES_WITH_IDENTITY:
        if result.resolved_sporely_taxon_id is None:
            raise ReconciliationInvariantError(
                f"observation {result.observation_id}: state {result.reconciliation_state} "
                "must carry a resolved_sporely_taxon_id"
            )
        if not result.resolution_evidence:
            raise ReconciliationInvariantError(
                f"observation {result.observation_id}: resolved state {result.reconciliation_state} "
                "must have at least one resolution_evidence step"
            )
    else:
        if result.resolved_sporely_taxon_id is not None:
            raise ReconciliationInvariantError(
                f"observation {result.observation_id}: unresolved state "
                f"{result.reconciliation_state} must not carry a resolved_sporely_taxon_id"
            )
