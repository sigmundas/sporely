"""Namespace derivation and normalisation rules for W2D reconciliation.

The rules are consumed from ``w2d-reconciliation-policy.json`` — see
`database/taxonomy/docs/w2d-reconciliation-contract.md` §3 for the source
of truth. Every ``RawSignal`` emitted by this module records the exact
``rule_id`` used, so a run's outputs are traceable back to a policy row.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from database.taxonomy.reconciliation.errors import PolicyValidationError
from database.taxonomy.reconciliation.input_model import RawSignal

logger = logging.getLogger(__name__)


_REQUIRED_PRIMARY_STATES = frozenset(
    {
        "ambiguous_multiple_candidates",
        "conflicting_exact_evidence",
        "invalid_or_unnamespaced_identifier",
        "manual_unresolved",
        "no_identity_evidence",
        "resolved_exact",
        "resolved_exact_via_legacy_mapping",
        "resolved_exact_via_synonym_relationship",
        "source_record_missing",
        "unresolved_external_identifier",
        "unresolved_legacy_identifier",
    }
)
_REQUIRED_RESOLUTION_METHODS = frozenset(
    {
        "direct_taxonomy_v2_mapping",
        "legacy_lookup_chain",
        "pinned_synonym_relationship",
        "trusted_secondary_provider_mapping",
    }
)


_NBIC_PATTERN = re.compile(r"^NBIC:(\d+)$")


@dataclass(frozen=True, slots=True)
class NamespaceRuleSet:
    """Runtime view of the policy: normalisation tables + resolver hints.

    The set is immutable and hashable so callers can safely share it
    across threads / long-lived resolvers.
    """

    policy_path: Path
    policy_sha256: str
    policy_body: dict[str, Any]

    def semantic_hash_excludes(self) -> tuple[str, ...]:
        return tuple(self.policy_body.get("semantic_hash_excludes") or ())

    def taxonomy_release_id(self) -> str:
        return str(self.policy_body["taxonomy_release_id"])

    def taxonomy_scope_manifest_sha256(self) -> str:
        return str(self.policy_body["taxonomy_scope_manifest_sha256"])

    def manifest_version(self) -> str:
        return str(self.policy_body.get("manifest_version") or "reconciliation-manifest-v1")

    def service_aliases(self) -> dict[str, str]:
        return dict(self.policy_body.get("service_aliases") or {})

    def trusted_secondary_providers(self) -> frozenset[str]:
        return frozenset(self.policy_body.get("trusted_secondary_providers") or ())


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def load_policy(policy_path: str | Path) -> NamespaceRuleSet:
    """Load and validate the W2D policy JSON.

    Fail-closed: raises :class:`PolicyValidationError` if any of the
    contract-mandated fields are missing.
    """
    path = Path(policy_path)
    raw = path.read_bytes()
    body = json.loads(raw.decode("utf-8"))
    if not isinstance(body, dict):
        raise PolicyValidationError(f"policy root must be object: {path}")
    for key in (
        "manifest_version",
        "namespace_rules",
        "policy_version",
        "primary_states",
        "resolution_methods",
        "taxonomy_release_id",
        "taxonomy_scope_manifest_sha256",
    ):
        if key not in body:
            raise PolicyValidationError(f"policy missing required key: {key}")
    primary_names = {row["name"] for row in body["primary_states"]}
    missing_states = _REQUIRED_PRIMARY_STATES - primary_names
    if missing_states:
        raise PolicyValidationError(f"policy missing primary states: {sorted(missing_states)}")
    method_names = set(body["resolution_methods"])
    missing_methods = _REQUIRED_RESOLUTION_METHODS - method_names
    if missing_methods:
        raise PolicyValidationError(
            f"policy missing resolution methods: {sorted(missing_methods)}"
        )
    seen_rule_ids: set[str] = set()
    for rule in body["namespace_rules"]:
        rule_id = rule.get("rule_id")
        if not rule_id:
            raise PolicyValidationError("namespace_rule missing rule_id")
        if rule_id in seen_rule_ids:
            raise PolicyValidationError(f"duplicate rule_id: {rule_id}")
        seen_rule_ids.add(rule_id)
    # Canonical policy bytes: re-serialise with sorted keys and no trailing
    # whitespace, so the sha256 is stable even if the on-disk file changes
    # trailing newlines. This is the value that the manifest header pins.
    canonical = json.dumps(body, ensure_ascii=False, indent=2, sort_keys=True).encode(
        "utf-8"
    ) + b"\n"
    return NamespaceRuleSet(
        policy_path=path,
        policy_sha256=_sha256_bytes(canonical),
        policy_body=body,
    )


# ---------------------------------------------------------------------------
# Namespace derivation
# ---------------------------------------------------------------------------


_RULE_ARTSDATA_ID = "artsdata_id_v1"
_RULE_ARTPORTALEN_ID = "artportalen_id_v1"
_RULE_INAT_TAXON_ID = "inaturalist_taxon_id_v1"
_RULE_INAT_OBS_ID = "inaturalist_observation_id_v1"
_RULE_MO_OBS_ID = "mushroomobserver_observation_id_v1"
_RULE_NBIC = "artsorakel_nbic_prefix_v1"
_RULE_ARTSORAKEL_BARE = "artsorakel_bare_int_v1"
_RULE_AI_INAT = "ai_selected_inaturalist_v1"
_RULE_AI_INVALID = "ai_selected_invalid_v1"
_RULE_SPORELY = "sporely_taxon_id_prepopulated_v1"
_RULE_ADB = "legacy_adb_taxon_id_v1"
_RULE_TEXT_GENUS_SPECIES = "text_genus_species_v1"
_RULE_TEXT_AI_SCI = "text_ai_selected_scientific_name_v1"
_RULE_TEXT_SNAPSHOT = "text_scientific_name_snapshot_v1"
_RULE_TEXT_COMMON = "text_common_name_v1"
_RULE_TEXT_SPECIES_GUESS = "text_species_guess_v1"
_RULE_TEXT_RANK = "text_taxon_rank_snapshot_v1"


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def normalise_service(service: str | None, rule_set: NamespaceRuleSet) -> str | None:
    """Apply the service alias table (contract §3 normalisation rules)."""
    if service is None:
        return None
    lowered = str(service).strip().lower()
    if not lowered:
        return None
    aliases = rule_set.service_aliases()
    return aliases.get(lowered, lowered)


def is_bare_integer(value: str) -> bool:
    return bool(re.fullmatch(r"\d+", value))


def derive_signals_from_observation(
    observation: dict[str, Any],
    rule_set: NamespaceRuleSet,
) -> list[RawSignal]:
    """Derive ``RawSignal`` records from a canonical observation snapshot.

    ``observation`` is a plain dict keyed by observation column name. Any
    absent column or empty value is silently ignored — the contract §3
    "no signal for empty" rule.

    This function contains no I/O and is used both by the CLI's snapshot-
    file loader and by test fixtures that build inputs programmatically.
    """
    signals: list[RawSignal] = []

    # sporely_taxon_id (Level 2 legacy chain, verified against pinned release)
    sporely = observation.get("sporely_taxon_id")
    if sporely not in (None, ""):
        try:
            sporely_int = int(sporely)
        except (TypeError, ValueError):
            sporely_int = None
        if sporely_int is not None and sporely_int > 0:
            signals.append(
                RawSignal(
                    kind="exact",
                    source_system="sporely",
                    namespace="sporely_taxon_id",
                    external_id=str(sporely_int),
                    origin_field="observations.sporely_taxon_id",
                    raw_value=str(sporely),
                    rule_id=_RULE_SPORELY,
                )
            )

    # artsdata_id (NorTaxa taxon id)
    artsdata = observation.get("artsdata_id")
    if artsdata not in (None, ""):
        signals.append(
            RawSignal(
                kind="exact",
                source_system="nortaxa",
                namespace="nortaxa_taxon_id",
                external_id=str(artsdata),
                origin_field="observations.artsdata_id",
                raw_value=str(artsdata),
                rule_id=_RULE_ARTSDATA_ID,
            )
        )

    # legacy adb_taxon_id (dropped column may still exist in old databases)
    adb = observation.get("adb_taxon_id")
    if adb not in (None, ""):
        signals.append(
            RawSignal(
                kind="exact",
                source_system="nortaxa",
                namespace="nortaxa_taxon_id",
                external_id=str(adb),
                origin_field="observations.adb_taxon_id",
                raw_value=str(adb),
                rule_id=_RULE_ADB,
                notes="legacy column preserved for reconciliation",
            )
        )

    # artportalen_id
    artportalen = observation.get("artportalen_id")
    if artportalen not in (None, ""):
        signals.append(
            RawSignal(
                kind="exact",
                source_system="artportalen",
                namespace="artportalen_taxon_id",
                external_id=str(artportalen),
                origin_field="observations.artportalen_id",
                raw_value=str(artportalen),
                rule_id=_RULE_ARTPORTALEN_ID,
            )
        )

    # inaturalist_taxon_id (distinct from inaturalist_id which is the observation id)
    inat_taxon = observation.get("inaturalist_taxon_id")
    if inat_taxon not in (None, ""):
        signals.append(
            RawSignal(
                kind="exact",
                source_system="inaturalist",
                namespace="inaturalist_taxon_id",
                external_id=str(inat_taxon),
                origin_field="observations.inaturalist_taxon_id",
                raw_value=str(inat_taxon),
                rule_id=_RULE_INAT_TAXON_ID,
            )
        )

    # inaturalist_id (observation id — preserved only)
    inat_obs = observation.get("inaturalist_id")
    if inat_obs not in (None, ""):
        signals.append(
            RawSignal(
                kind="preserve_only",
                source_system="inaturalist",
                namespace="inaturalist_observation_id",
                external_id=str(inat_obs),
                origin_field="observations.inaturalist_id",
                raw_value=str(inat_obs),
                rule_id=_RULE_INAT_OBS_ID,
                notes="observation identifier; never creates identity",
            )
        )

    # mushroomobserver_id (observation id — preserved only)
    mo_obs = observation.get("mushroomobserver_id")
    if mo_obs not in (None, ""):
        signals.append(
            RawSignal(
                kind="preserve_only",
                source_system="mushroomobserver",
                namespace="mushroomobserver_observation_id",
                external_id=str(mo_obs),
                origin_field="observations.mushroomobserver_id",
                raw_value=str(mo_obs),
                rule_id=_RULE_MO_OBS_ID,
                notes="observation identifier; never creates identity",
            )
        )

    # ai_selected_service + ai_selected_taxon_id
    raw_service = observation.get("ai_selected_service")
    raw_ai_id = observation.get("ai_selected_taxon_id")
    if raw_ai_id not in (None, ""):
        service = normalise_service(raw_service, rule_set)
        ai_value = str(raw_ai_id).strip()
        nbic_match = _NBIC_PATTERN.match(ai_value)
        if nbic_match is not None:
            signals.append(
                RawSignal(
                    kind="exact",
                    source_system="nortaxa",
                    namespace="nortaxa_taxon_id",
                    external_id=nbic_match.group(1),
                    origin_field="observations.ai_selected_taxon_id",
                    raw_value=ai_value,
                    rule_id=_RULE_NBIC,
                    notes=f"stripped NBIC: prefix; original service={raw_service!r}",
                )
            )
        elif service == "artsorakel" and is_bare_integer(ai_value):
            signals.append(
                RawSignal(
                    kind="exact",
                    source_system="nortaxa",
                    namespace="nortaxa_taxon_id",
                    external_id=ai_value,
                    origin_field="observations.ai_selected_taxon_id",
                    raw_value=ai_value,
                    rule_id=_RULE_ARTSORAKEL_BARE,
                    notes="bare integer under artsorakel service — see policy artsorakel_bare_int_v1",
                )
            )
        elif service == "inaturalist" and is_bare_integer(ai_value):
            signals.append(
                RawSignal(
                    kind="exact",
                    source_system="inaturalist",
                    namespace="inaturalist_taxon_id",
                    external_id=ai_value,
                    origin_field="observations.ai_selected_taxon_id",
                    raw_value=ai_value,
                    rule_id=_RULE_AI_INAT,
                )
            )
        else:
            # Unknown service or ambiguous prefix
            signals.append(
                RawSignal(
                    kind="invalid",
                    source_system=service,
                    namespace=None,
                    external_id=ai_value,
                    origin_field="observations.ai_selected_taxon_id",
                    raw_value=ai_value,
                    rule_id=_RULE_AI_INVALID,
                    notes=f"unknown or ambiguous service={raw_service!r}",
                )
            )

    # ai_selected_scientific_name (text-only Level 5 candidate)
    ai_sci = _clean(observation.get("ai_selected_scientific_name"))
    if ai_sci:
        signals.append(
            RawSignal(
                kind="text-only",
                source_system=None,
                namespace=None,
                external_id=None,
                origin_field="observations.ai_selected_scientific_name",
                raw_value=ai_sci,
                rule_id=_RULE_TEXT_AI_SCI,
            )
        )

    # scientific_name_snapshot (text-only Level 5 candidate)
    sci_snap = _clean(observation.get("scientific_name_snapshot"))
    if sci_snap:
        signals.append(
            RawSignal(
                kind="text-only",
                source_system=None,
                namespace=None,
                external_id=None,
                origin_field="observations.scientific_name_snapshot",
                raw_value=sci_snap,
                rule_id=_RULE_TEXT_SNAPSHOT,
            )
        )

    # genus + species -> binomial
    genus = _clean(observation.get("genus"))
    species = _clean(observation.get("species"))
    if genus and species:
        signals.append(
            RawSignal(
                kind="text-only",
                source_system=None,
                namespace=None,
                external_id=None,
                origin_field="observations.genus+species",
                raw_value=f"{genus} {species}",
                rule_id=_RULE_TEXT_GENUS_SPECIES,
            )
        )

    # common_name (vernacular)
    common = _clean(observation.get("common_name"))
    if common:
        signals.append(
            RawSignal(
                kind="text-only",
                source_system=None,
                namespace=None,
                external_id=None,
                origin_field="observations.common_name",
                raw_value=common,
                rule_id=_RULE_TEXT_COMMON,
            )
        )

    # species_guess
    guess = _clean(observation.get("species_guess"))
    if guess:
        signals.append(
            RawSignal(
                kind="text-only",
                source_system=None,
                namespace=None,
                external_id=None,
                origin_field="observations.species_guess",
                raw_value=guess,
                rule_id=_RULE_TEXT_SPECIES_GUESS,
            )
        )

    rank = _clean(observation.get("taxon_rank_snapshot"))
    if rank:
        signals.append(
            RawSignal(
                kind="text-only",
                source_system=None,
                namespace=None,
                external_id=None,
                origin_field="observations.taxon_rank_snapshot",
                raw_value=rank.lower(),
                rule_id=_RULE_TEXT_RANK,
            )
        )

    return signals


def iter_exact_signals(signals: Iterable[RawSignal]) -> list[RawSignal]:
    return [s for s in signals if s.kind == "exact"]


def iter_text_signals(signals: Iterable[RawSignal]) -> list[RawSignal]:
    return [s for s in signals if s.kind == "text-only"]


def iter_invalid_signals(signals: Iterable[RawSignal]) -> list[RawSignal]:
    return [s for s in signals if s.kind == "invalid"]
