"""Typed taxonomy identity for the desktop client.

Stage 2 Part A of the taxonomy-v2 closeout. This module is the single place
that decides whether an integer may be asserted as a Sporely-owned
``sporely_taxon_id``.

The accepted architecture (``database/taxonomy/docs/identity-contract.md``)
says an external identifier is authoritative only as the tuple
``(source_system, namespace, external_id)``, that namespace-lost integers are
legacy/audit evidence only, and that scientific-name equality is not identity
evidence. Before this module the desktop encoded identity as a bare
``sporely_taxon_id`` integer plus a handful of loosely-related snapshot keys,
so nothing downstream could distinguish

* a native Sporely concept proven by a taxonomy-v2 artifact,
* a COL or NorTaxa external identifier that has never been resolved,
* an unresolved manual scientific-name string,

and the cloud-sync gate's entire proof standard was ``value > 0``.
:class:`TaxonIdentity` replaces that with one explicit, provenance-bearing
value. ``sporely_taxon_id`` is populated only on the two states that carry
proof; every other state keeps the source evidence instead, and
:attr:`TaxonIdentity.is_proven_sporely` is the one predicate a writer to
``set_observation_selected_taxon_v2`` may consult.

Nothing here imports Qt or touches a database, so both the UI controller and
the sync layer can depend on it.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, replace

# ── Sporely's own namespace ──────────────────────────────────────────────────

SPORELY_SOURCE_SYSTEM = "sporely"
SPORELY_NAMESPACE = "sporely_taxon_id"

# ── Identity states ─────────────────────────────────────────────────────────
#
# These are deliberately distinct from "the column is NULL". Missing identity
# and an explicitly-unresolved external identifier are different facts, and the
# closeout plan requires the client to be able to tell them apart.

STATE_NONE = "no_identity_evidence"
STATE_SPORELY = "sporely_v2"
STATE_EXTERNAL_UNRESOLVED = "external_unresolved"
STATE_MANUAL_UNRESOLVED = "manual_unresolved"

ALL_STATES = frozenset({
    STATE_NONE,
    STATE_SPORELY,
    STATE_EXTERNAL_UNRESOLVED,
    STATE_MANUAL_UNRESOLVED,
})

# ── Proof tokens ────────────────────────────────────────────────────────────
#
# A Sporely ID is only ever as trustworthy as the thing that produced it, so
# the producer is recorded alongside the integer rather than inferred later.

#: The integer came out of a compiled taxonomy-v2 artifact whose identity
#: contract states that its ``taxon_id`` *is* the Sporely ID
#: (``build_sqlite_candidate.py``'s ``taxon_min.taxon_id``).
PROOF_TAXONOMY_V2_ARTIFACT = "taxonomy_v2_artifact"

#: A namespaced external identifier was resolved through an authoritative
#: mapping (``resolve_taxon_external_id_v2`` or the compiled
#: ``taxon_external_id_text_min`` table) which returned exactly one Sporely ID.
PROOF_EXTERNAL_ID_RESOLUTION = "external_id_resolution"

#: The integer was already in ``observations.sporely_taxon_id`` before Stage 2
#: added provenance columns, so nothing records which producer wrote it. Such a
#: value is *unverified*, not proven: the pre-Stage-2 backfill could resolve it
#: from a namespace-lost integer or from a unique scientific-name match, both of
#: which the identity contract forbids as identity evidence.
#: ``database/migrate_observations_sporely_id.py`` promotes these to
#: :data:`PROOF_TAXONOMY_V2_ARTIFACT` for the rows that still verify against
#: ``taxon_min``, and clears the rest.
PROOF_LEGACY_UNVERIFIED = "legacy_unverified"

#: The integer was received from the cloud's ``selected_sporely_taxon_id``
#: (state ``sporely_v2``) during a pull, and the receiving desktop confirmed
#: only that the concept is present in its installed taxonomy artifact.
#:
#: That is weaker than either proven producer. The server validates
#: active-release membership at write time but cannot detect a numeric
#: collision, and pre-Stage-2 desktops asserted unproven integers through the
#: same RPC, so the cloud value is only as trustworthy as the weakest client
#: that ever wrote it — and local membership is not proof of origin either.
#: The value is kept (displayed, restored, preserved across saves) but never
#: re-asserted to the cloud and never drives identity-gated lookups. An
#: explicit picker selection replaces it with :data:`PROOF_TAXONOMY_V2_ARTIFACT`.
PROOF_CLOUD_SELECTED_UNVERIFIED = "cloud_selected_unverified"

#: Proof tokens that permit asserting a Sporely-owned identity. Deliberately
#: excludes :data:`PROOF_LEGACY_UNVERIFIED` — a gate that grandfathers every
#: pre-existing value enforces nothing — and
#: :data:`PROOF_CLOUD_SELECTED_UNVERIFIED`, for the reasons given there.
PROVEN_SPORELY_PROOFS = frozenset({
    PROOF_TAXONOMY_V2_ARTIFACT,
    PROOF_EXTERNAL_ID_RESOLUTION,
})

#: Proof tokens that keep their integer through persistence without being
#: proven. Each records a real producer that could not be verified as a
#: Sporely-namespace source.
UNPROVEN_SPORELY_PROOFS = frozenset({
    PROOF_LEGACY_UNVERIFIED,
    PROOF_CLOUD_SELECTED_UNVERIFIED,
})


# ── Prefixed provider identifiers ───────────────────────────────────────────
#
# Registry of the literal prefixes a provider response can carry, mapped to the
# namespace that prefix actually denotes. Per identity-contract.md the raw
# value ("NBIC:53482") is what must be retained; the numeric component alone is
# meaningful only under a declared bridge (see _NAMESPACE_BRIDGES).

_PREFIXED_ID_REGISTRY: dict[str, tuple[str, str]] = {
    # Artsorakel returns scientific-NAME ids, not concept ids.
    "NBIC": ("artsorakel", "nbic_scientific_name_id"),
}

_PREFIXED_ID_PATTERN = re.compile(r"^([A-Za-z][A-Za-z0-9_]*):(.+)$")

#: Declared, evidenced namespace bridges. Each entry maps a source namespace to
#: the namespace its value may also be looked up under, together with the
#: evidence that licenses the hop. identity-contract.md declares that the
#: NorTaxa Darwin Core archive's ``dwc:taxonID`` values ARE Artsnavnebase
#: scientific-name IDs — the same registry Artsorakel's ``NBIC:`` prefix
#: returns — so an Artsorakel name id may be looked up as a
#: ``nortaxa_taxon_id``. It may never be bridged to
#: ``artsdatabanken_taxon_concept_id``: numeric equality across those two
#: Artsdatabanken registries is coincidence, not identity.
_NAMESPACE_BRIDGES: dict[str, tuple[str, str, str]] = {
    # source namespace -> (target source_system, target namespace, evidence)
    "nbic_scientific_name_id": (
        "nortaxa",
        "nortaxa_taxon_id",
        "identity-contract.md: nortaxa dwc:taxonID values are Artsnavnebase "
        "scientific-name IDs, the registry Artsorakel returns under NBIC:",
    ),
}


@dataclass(frozen=True)
class PrefixedExternalId:
    """A provider identifier parsed without losing its raw form.

    ``raw`` is always the provider's verbatim string. ``numeric_component`` is
    the digits after the prefix and is *not* an identity on its own — it is
    usable only through :meth:`bridged`.
    """

    raw: str
    source_system: str
    namespace: str
    local_id: str
    numeric_component: str | None = None

    def bridged(self) -> "PrefixedExternalId | None":
        """The same value expressed in its declared bridge namespace.

        Returns ``None`` when no bridge is declared for this namespace, or
        when the value has no numeric component to carry across. The raw
        provider string is preserved on the result so the original evidence
        survives the hop.
        """
        bridge = _NAMESPACE_BRIDGES.get(self.namespace)
        if bridge is None or not self.numeric_component:
            return None
        source_system, namespace, _evidence = bridge
        return replace(
            self,
            source_system=source_system,
            namespace=namespace,
            local_id=self.numeric_component,
        )

    @property
    def bridge_evidence(self) -> str | None:
        bridge = _NAMESPACE_BRIDGES.get(self.namespace)
        return bridge[2] if bridge else None


def parse_prefixed_external_id(value: object) -> PrefixedExternalId | None:
    """Parse ``"NBIC:53482"`` into its namespaced tuple, retaining the raw.

    Returns ``None`` for empty input, for an unknown prefix, and — importantly
    — for a bare integer. A namespace-lost integer such as ``"53482"`` carries
    no evidence of which registry it came from, so inventing one here is
    exactly the defect this module exists to prevent.
    """
    raw = str(value or "").strip()
    if not raw:
        return None
    match = _PREFIXED_ID_PATTERN.match(raw)
    if match is None:
        return None
    prefix = match.group(1).upper()
    registered = _PREFIXED_ID_REGISTRY.get(prefix)
    if registered is None:
        return None
    source_system, namespace = registered
    local_id = match.group(2).strip()
    if not local_id:
        return None
    numeric = local_id if local_id.isdigit() else None
    return PrefixedExternalId(
        raw=raw,
        source_system=source_system,
        namespace=namespace,
        local_id=local_id,
        numeric_component=numeric,
    )


def _clean(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _positive_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


@dataclass(frozen=True)
class TaxonIdentity:
    """What the client knows about an observation's taxonomic identity.

    Construct through the classmethods rather than the initialiser: each one
    encodes which fields the corresponding state is allowed to populate.
    """

    state: str = STATE_NONE
    sporely_taxon_id: int | None = None
    identity_proof: str | None = None
    source_system: str | None = None
    namespace: str | None = None
    external_id: str | None = None
    raw_external_id: str | None = None
    scientific_name: str | None = None
    rank: str | None = None
    provenance: str | None = None

    # ── Predicates ─────────────────────────────────────────────────────────

    @property
    def is_proven_sporely(self) -> bool:
        """Whether this identity may be asserted as a Sporely-owned ID.

        The only gate any writer to ``sporely_taxon_id`` or to
        ``set_observation_selected_taxon_v2`` should consult. Requires the
        Sporely state, a positive integer, *and* a recognised proof token — a
        positive integer on its own is not proof, which is the whole point.
        """
        return (
            self.state == STATE_SPORELY
            and isinstance(self.sporely_taxon_id, int)
            and self.sporely_taxon_id > 0
            and self.identity_proof in PROVEN_SPORELY_PROOFS
        )

    @property
    def has_external_evidence(self) -> bool:
        """Whether a complete ``(source, namespace, external_id)`` is held."""
        return bool(self.source_system and self.namespace and self.external_id)

    @property
    def is_unresolved(self) -> bool:
        return self.state in {STATE_EXTERNAL_UNRESOLVED, STATE_MANUAL_UNRESOLVED}

    @property
    def is_legacy_unverified(self) -> bool:
        """A pre-Stage-2 integer whose producer was never recorded.

        Not unresolved (something did bind it) and not proven (nothing says
        what). It becomes proven only by re-verifying against the taxonomy
        artifact.
        """
        return (
            self.state == STATE_SPORELY
            and self.identity_proof == PROOF_LEGACY_UNVERIFIED
        )

    @property
    def is_cloud_selected_unverified(self) -> bool:
        """A cloud-selected Sporely ID present in the local artifact, unproven.

        See :data:`PROOF_CLOUD_SELECTED_UNVERIFIED`.
        """
        return (
            self.state == STATE_SPORELY
            and isinstance(self.sporely_taxon_id, int)
            and self.sporely_taxon_id > 0
            and self.identity_proof == PROOF_CLOUD_SELECTED_UNVERIFIED
        )

    # ── Constructors ───────────────────────────────────────────────────────

    @classmethod
    def none(cls) -> "TaxonIdentity":
        """No identity evidence at all — distinct from unresolved evidence."""
        return cls(state=STATE_NONE)

    @classmethod
    def from_taxonomy_v2_artifact(
        cls,
        sporely_taxon_id: object,
        *,
        scientific_name: object = None,
        rank: object = None,
        provenance: object = None,
    ) -> "TaxonIdentity":
        """A native Sporely concept picked from a compiled taxonomy-v2 artifact.

        Degrades to :meth:`none` rather than fabricating identity when the
        integer is missing or non-positive.
        """
        sporely_id = _positive_int(sporely_taxon_id)
        if sporely_id is None:
            return cls.none()
        return cls(
            state=STATE_SPORELY,
            sporely_taxon_id=sporely_id,
            identity_proof=PROOF_TAXONOMY_V2_ARTIFACT,
            source_system=SPORELY_SOURCE_SYSTEM,
            namespace=SPORELY_NAMESPACE,
            external_id=str(sporely_id),
            scientific_name=_clean(scientific_name),
            rank=_clean(rank),
            provenance=_clean(provenance),
        )

    @classmethod
    def from_cloud_selection(
        cls,
        sporely_taxon_id: object,
        *,
        local_release_id: object,
        scientific_name: object = None,
        rank: object = None,
    ) -> "TaxonIdentity":
        """The cloud's selected Sporely ID, confirmed present locally.

        The caller must already have confirmed that ``sporely_taxon_id`` is a
        concept of the installed taxonomy artifact identified by
        ``local_release_id``; ``scientific_name``/``rank`` should be that
        artifact's canonical values. Degrades to :meth:`none` without a
        positive integer or a release id — an adoption that cannot say which
        artifact it was checked against has not been checked.
        """
        sporely_id = _positive_int(sporely_taxon_id)
        release = _clean(local_release_id)
        if sporely_id is None or release is None:
            return cls.none()
        return cls(
            state=STATE_SPORELY,
            sporely_taxon_id=sporely_id,
            identity_proof=PROOF_CLOUD_SELECTED_UNVERIFIED,
            source_system=SPORELY_SOURCE_SYSTEM,
            namespace=SPORELY_NAMESPACE,
            external_id=str(sporely_id),
            scientific_name=_clean(scientific_name),
            rank=_clean(rank),
            provenance=(
                "cloud:observations.selected_sporely_taxon_id; "
                f"present_in_local_release={release}"
            ),
        )

    @classmethod
    def unresolved_external(
        cls,
        *,
        source_system: object,
        namespace: object,
        external_id: object,
        raw_external_id: object = None,
        scientific_name: object = None,
        rank: object = None,
        provenance: object = None,
    ) -> "TaxonIdentity":
        """A namespaced external identifier that has not been resolved.

        Never carries a ``sporely_taxon_id``: an unresolved external
        identifier has no Sporely-owned identity by definition, regardless of
        what its digits happen to equal.
        """
        source = _clean(source_system)
        ns = _clean(namespace)
        ext = _clean(external_id)
        if not (source and ns and ext):
            return cls.none()
        return cls(
            state=STATE_EXTERNAL_UNRESOLVED,
            sporely_taxon_id=None,
            identity_proof=None,
            source_system=source,
            namespace=ns,
            external_id=ext,
            raw_external_id=_clean(raw_external_id) or ext,
            scientific_name=_clean(scientific_name),
            rank=_clean(rank),
            provenance=_clean(provenance),
        )

    @classmethod
    def from_prefixed_external_id(
        cls,
        value: object,
        *,
        scientific_name: object = None,
        rank: object = None,
        provenance: object = None,
        bridge: bool = True,
    ) -> "TaxonIdentity":
        """Preserve a provider identifier such as ``"NBIC:53482"``.

        With ``bridge=True`` the value is also expressed in its declared
        bridge namespace (``nortaxa`` / ``nortaxa_taxon_id`` for an Artsorakel
        name id) so it can be offered to the authoritative resolver, while
        ``raw_external_id`` keeps the verbatim provider string. The result is
        always unresolved — bridging a namespace is not resolving an identity.
        """
        parsed = parse_prefixed_external_id(value)
        if parsed is None:
            return cls.none()
        target = (parsed.bridged() if bridge else None) or parsed
        return cls.unresolved_external(
            source_system=target.source_system,
            namespace=target.namespace,
            external_id=target.local_id,
            raw_external_id=parsed.raw,
            scientific_name=scientific_name,
            rank=rank,
            provenance=provenance,
        )

    @classmethod
    def manual_text(
        cls,
        scientific_name: object,
        *,
        rank: object = None,
    ) -> "TaxonIdentity":
        """Typed scientific text with no identifier behind it.

        Text is a name, never an identity: this state exists so the UI can say
        "we have a name but nothing is bound" instead of silently resolving by
        string equality.
        """
        name = _clean(scientific_name)
        if not name:
            return cls.none()
        return cls(
            state=STATE_MANUAL_UNRESOLVED,
            scientific_name=name,
            rank=_clean(rank),
        )

    # ── Transitions ────────────────────────────────────────────────────────

    def resolved_to(
        self,
        sporely_taxon_id: object,
        *,
        provenance: object = None,
    ) -> "TaxonIdentity":
        """Bind this external identity to a Sporely ID from an authoritative map.

        Only an external identity with complete evidence may be resolved, and
        the source tuple is retained afterwards so the resolution stays
        auditable. Returns ``self`` unchanged when the inputs do not license a
        binding, so a failed resolution never destroys the source evidence.
        """
        sporely_id = _positive_int(sporely_taxon_id)
        if sporely_id is None or not self.has_external_evidence:
            return self
        return replace(
            self,
            state=STATE_SPORELY,
            sporely_taxon_id=sporely_id,
            identity_proof=PROOF_EXTERNAL_ID_RESOLUTION,
            provenance=_clean(provenance) or self.provenance,
        )

    # ── Serialisation ──────────────────────────────────────────────────────
    #
    # ``to_row``/``from_row`` are the persistence boundary: the same key names
    # are used for the desktop SQLite columns and for the controller's
    # committed-snapshot dict, so provenance round-trips through save/reload
    # without a second translation layer to get wrong.

    def to_row(self) -> dict:
        # A legacy-unverified integer is written back unchanged: it is real
        # persisted data awaiting re-verification, and silently dropping it
        # would destroy evidence rather than gate it. The cloud gate reads
        # ``is_proven_sporely``, not the column, so withholding it here is
        # unnecessary.
        # A cloud-selected unverified integer is kept for the same reason: it
        # is the cloud's identity, restored for display and preserved across
        # saves, and the cloud gate still refuses it.
        keep_integer = (
            self.is_proven_sporely
            or self.is_legacy_unverified
            or self.is_cloud_selected_unverified
        )
        return {
            "sporely_taxon_id": self.sporely_taxon_id if keep_integer else None,
            "taxon_identity_state": self.state,
            "taxon_identity_proof": self.identity_proof,
            "taxon_identity_source_system": self.source_system,
            "taxon_identity_namespace": self.namespace,
            "taxon_identity_external_id": self.external_id,
            "taxon_identity_raw_external_id": self.raw_external_id,
            # Release/response provenance — e.g. the taxonomy release a
            # resolution was performed against. The brief requires preserving
            # it "if available"; it used to live only in memory, so a
            # save/reload silently discarded it.
            "taxon_identity_provenance": self.provenance,
        }

    @classmethod
    def from_row(cls, row: dict | None) -> "TaxonIdentity":
        """Rebuild an identity from a persisted observation row.

        Legacy rows predating Stage 2 have a ``sporely_taxon_id`` but no state
        column. They are read back as an artifact-proven Sporely identity,
        matching the behaviour they had when written; the backfill in
        ``database/migrate_observations_sporely_id.py`` is what re-verifies
        those values against the taxonomy artifact.
        """
        data = dict(row or {})
        state = _clean(data.get("taxon_identity_state"))
        sporely_id = _positive_int(data.get("sporely_taxon_id"))
        source_system = _clean(data.get("taxon_identity_source_system"))
        namespace = _clean(data.get("taxon_identity_namespace"))
        external_id = _clean(data.get("taxon_identity_external_id"))
        raw_external_id = _clean(data.get("taxon_identity_raw_external_id"))
        proof = _clean(data.get("taxon_identity_proof"))
        provenance = _clean(data.get("taxon_identity_provenance"))
        scientific_name = _clean(data.get("scientific_name_snapshot")) \
            or _clean(data.get("scientific_name"))
        rank = _clean(data.get("taxon_rank_snapshot")) or _clean(data.get("rank"))

        if state is None:
            if sporely_id is None:
                return cls.none()
            return cls(
                state=STATE_SPORELY,
                sporely_taxon_id=sporely_id,
                identity_proof=PROOF_LEGACY_UNVERIFIED,
                scientific_name=scientific_name,
                rank=rank,
                provenance=provenance,
            )
        if state not in ALL_STATES:
            return cls.none()
        if state == STATE_SPORELY:
            known_proofs = PROVEN_SPORELY_PROOFS | UNPROVEN_SPORELY_PROOFS
            if sporely_id is None or proof not in known_proofs:
                # A Sporely state without a proven integer is corrupt rather
                # than authoritative. Fall back to whatever source evidence
                # survived instead of trusting the integer.
                if source_system and namespace and external_id:
                    return cls.unresolved_external(
                        source_system=source_system,
                        namespace=namespace,
                        external_id=external_id,
                        raw_external_id=raw_external_id,
                        scientific_name=scientific_name,
                        rank=rank,
                        provenance=provenance,
                    )
                return cls.manual_text(scientific_name, rank=rank) if scientific_name \
                    else cls.none()
            return cls(
                state=STATE_SPORELY,
                sporely_taxon_id=sporely_id,
                identity_proof=proof,
                source_system=source_system or SPORELY_SOURCE_SYSTEM,
                namespace=namespace or SPORELY_NAMESPACE,
                external_id=external_id or str(sporely_id),
                raw_external_id=raw_external_id,
                scientific_name=scientific_name,
                rank=rank,
                provenance=provenance,
            )
        if state == STATE_EXTERNAL_UNRESOLVED:
            return cls.unresolved_external(
                source_system=source_system,
                namespace=namespace,
                external_id=external_id,
                raw_external_id=raw_external_id,
                scientific_name=scientific_name,
                rank=rank,
                provenance=provenance,
            )
        if state == STATE_MANUAL_UNRESOLVED:
            return cls.manual_text(scientific_name, rank=rank)
        return cls.none()


#: Column names added to ``observations`` by Stage 2 Part A. Exported so the
#: schema helper, the model layer and the tests agree on one list.
IDENTITY_COLUMNS = (
    "taxon_identity_state",
    "taxon_identity_proof",
    "taxon_identity_source_system",
    "taxon_identity_namespace",
    "taxon_identity_external_id",
    "taxon_identity_raw_external_id",
    "taxon_identity_provenance",
)


def proven_sporely_taxon_id(row: dict | None) -> int | None:
    """The Sporely ID a persisted row is allowed to assert, or ``None``.

    The single helper every cloud/DB writer should use instead of reading
    ``row['sporely_taxon_id']`` directly.
    """
    identity = TaxonIdentity.from_row(row)
    return identity.sporely_taxon_id if identity.is_proven_sporely else None
