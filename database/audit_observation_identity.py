#!/usr/bin/env python3
"""Classify, and only then repair, observation taxonomy identity.

Taxonomy-v2 closeout Stage 4 Part A. Two distinct production defects left
observations in a state the accepted identity contract forbids:

* the **identity leak** — a field now interpreted as a Sporely-owned
  ``sporely_taxon_id`` holds an integer whose producer nothing records, which
  may be an external/legacy identifier that merely collides numerically with a
  real Sporely concept (``database/migrate_observations_sporely_id.py``
  documents how the pre-Stage-2 backfill minted those);
* the **name loss** — a provider candidate the user copied into the
  identification was saved with null ``genus``/``species``/``common_name``
  beside a non-null ``ai_selected_scientific_name``, which is the shape the
  Stage 2 Part B save-boundary defect produces.

The two populations are counted separately because they are different repairs.
Identity repair *resolves an identity* and is only ever licensed by an
authoritative namespaced relationship. Name repair *restores a string the same
row already carries* and infers no identity at all — lower risk, but still a
production write and still behind the Part B gate.

Evidence, never number shape
----------------------------

Classification reads the row's own recorded evidence and an authoritative
taxonomy artifact. It never concludes anything from:

* two integers being equal;
* two scientific names being equal;
* a fuzzy or unique name search returning one row;
* the expected species looking obvious.

Those are exactly the inferences that produced the population being audited.
A row is repairable only when a ``(source_system, namespace, external_id)``
tuple recorded *on the row* resolves, through the artifact's authoritative
mapping table, to exactly one Sporely concept. Everything else is reported
with a refusal reason and left alone.

Three reconciling axes
----------------------

Each observation gets exactly one value on each of three axes, so each axis
independently sums to the row count and the counts reconcile:

``identity_class``
    what the row's identity evidence *is* (see :data:`IDENTITY_CLASSES`);
``name_class``
    whether the provider-candidate name survived (see :data:`NAME_CLASSES`);
``proposed_action``
    what this module would do about it (see :data:`ACTIONS`).

Idempotence
-----------

Repair is expressed as "queue a coherent identity write" and "fill a null name
field", never as "overwrite". A second run over a repaired database
reclassifies the repaired rows — a bound identity now reads as
``proven_sporely_identity``, a restored name as ``name_intact`` — so it
proposes nothing and writes nothing. Rows that were already correct before the
first run are never touched by either run.

The write boundary
------------------

Every write goes through :func:`apply_repairs`, which enforces the Part B
:class:`ProductionMigrationGate` itself rather than trusting its caller, and
which refuses an audit artifact that no longer describes the rows it
classified. A gate only the tests consult protects only the tests.

Applying is a different operation from auditing, and takes the reviewed file
as its input: ``--apply <artifact.json>``. Three things bind the write to that
file, because each catches something the others cannot:

* the artifact's own ``digest`` — catches the file being edited after review;
* :func:`require_artifact_still_describes` — re-audits the live rows and
  requires the reviewed artifact to be reproduced exactly, which is what
  catches rows *added* since the review, invisible to any per-row check;
* the per-row pre-image in :func:`apply_repairs` — catches a row changing
  between that re-audit and the write itself.

Together they make "the dry run was reviewed" a property of the applied result
rather than of the reviewer's intent.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Sequence

from database.models import update_observation_sync_state
from database.taxon_lookup import source_identity_matches
from utils.taxon_identity import (
    SPORELY_NAMESPACE,
    SPORELY_SOURCE_SYSTEM,
    TaxonIdentity,
    parse_prefixed_external_id,
)
from utils.taxon_text import split_scientific_name_text


# ── Vocabulary ──────────────────────────────────────────────────────────────

#: Exactly one applies to every audited row.
IDENTITY_CLASSES = (
    # The plan's minimum set.
    "proven_sporely_identity",
    "proven_external_with_unique_bridge",
    "unresolved_external_identity",
    "ambiguous_external_identity",
    "manual_or_no_identity_evidence",
    "suspicious_numeric_collision",
    # Two populations the minimum set leaves without a home. Stage 2 leaves
    # pre-Stage-2 integers as legacy-unverified on purpose, and an identity
    # proven against a *different* release can name a concept this artifact
    # does not contain. Both are real and neither is any of the six above.
    "legacy_unverified_identity",
    "stale_sporely_identity",
    # A Sporely ID adopted from the cloud's selection on pull. Kept on the
    # row, never proven (see PROOF_CLOUD_SELECTED_UNVERIFIED): its own
    # Sporely-namespace tuple is not independent evidence, so it must not
    # fall through to the external-bridge ladder and be "re-derived" from
    # itself. Report only.
    "cloud_selected_unverified_identity",
    # An identifier that resolves to exactly one concept, but the observer's
    # identification is not that concept's name (D3: name agreement is
    # necessary). Split by cause: the identifier is an AI suggestion the
    # observer changed, or the row's own identifier names another taxon
    # (e.g. an aggregate ``coll.`` concept for a strict species). Report only.
    "ai_suggestion_not_identification",
    "identifier_name_mismatch",
)

#: Exactly one applies to every audited row. Counted separately from
#: ``identity_class`` because name loss is a different defect and a different
#: repair, not a kind of identity evidence.
NAME_CLASSES = (
    "name_intact",
    "name_loss_repairable_from_row",
    "name_loss_unrepairable_from_row",
    # Genus and species are null but a common name survived, so this row is
    # NOT the population the stage defines and is never repaired. It is still
    # given its own class rather than being folded into ``name_intact``,
    # because a row with no binomial is a real gap an operator should see.
    "partial_name_loss_reported",
)

#: The name-loss population the acceptance gate asks to be counted is the sum
#: of these two classes, and only these two. The stage defines that population
#: as null ``genus`` AND null ``species`` AND null ``common_name`` beside a
#: non-null ``ai_selected_scientific_name``; widening it would make the
#: proposed production writes exceed what was specified and reviewed.
NAME_LOSS_CLASSES = (
    "name_loss_repairable_from_row",
    "name_loss_unrepairable_from_row",
)

#: Exactly one applies to every audited row.
ACTIONS = (
    "no_change_required",
    "bind_sporely_identity",
    "restore_lost_names",
    "bind_sporely_identity_and_restore_lost_names",
    "report_only",
)

#: Provenance written on an identity this module binds, so a later reader can
#: tell a Stage 4 repair from an ordinary client resolution.
REPAIR_PROVENANCE_PREFIX = "sporely_taxonomy_v2_stage4_repair"


# ── Taxonomy artifact ───────────────────────────────────────────────────────


class TaxonomyArtifact:
    """Read-only authoritative mapping, over either compiled artifact shape.

    ``build_sqlite_candidate.py`` emits ``taxon_min`` plus the namespaced
    ``taxon_external_id_text_min``; ``macrofungi_scope.build_desktop`` emits
    the scoped search pack's ``taxon`` plus ``external_mapping``. They carry
    the same authoritative tuple under different table names, and an audit that
    only understood one of them could not be run against whichever artifact an
    operator actually holds.

    The namespace-lost integer table (``taxon_external_id_min``) is
    deliberately never read: the identity contract designates it legacy/audit
    evidence, so resolving identity from it is the defect, not the fix.
    """

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        if not self.path.exists():
            raise SystemExit(f"taxonomy artifact not found: {self.path}")
        self._conn = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)
        tables = {
            row[0]
            for row in self._conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        if {"taxon_min", "taxon_external_id_text_min"} <= tables:
            self._taxon_table = "taxon_min"
            self._mapping_table = "taxon_external_id_text_min"
            self.release_id = self._meta_value(
                "taxonomy_meta", "content_release_id", tables
            )
        elif {"taxon", "external_mapping"} <= tables:
            self._taxon_table = "taxon"
            self._mapping_table = "external_mapping"
            self.release_id = self._meta_value("metadata", "release_id", tables)
        else:
            raise SystemExit(
                f"{self.path} is neither a taxonomy-v2 candidate "
                f"(taxon_min + taxon_external_id_text_min) nor a desktop "
                f"search pack (taxon + external_mapping)"
            )
        self._valid_ids = {
            int(row[0])
            for row in self._conn.execute(f"SELECT taxon_id FROM {self._taxon_table}")
        }
        columns = {
            row[1]
            for row in self._conn.execute(f"PRAGMA table_info({self._taxon_table})")
        }
        self._name_column = next(
            (name for name in ("canonical_scientific_name", "scientific_name")
             if name in columns),
            None,
        )

    def _meta_value(self, table: str, key: str, tables: set[str]) -> str | None:
        if table not in tables:
            return None
        row = self._conn.execute(
            f"SELECT value FROM {table} WHERE key = ?", (key,)
        ).fetchone()
        return str(row[0]).strip() if row and row[0] is not None else None

    def close(self) -> None:
        self._conn.close()

    def contains(self, sporely_taxon_id: object) -> bool:
        try:
            return int(sporely_taxon_id) in self._valid_ids
        except (TypeError, ValueError):
            return False

    def resolve(
        self, *, source_system: str, namespace: str, external_id: str
    ) -> set[int]:
        """Every Sporely concept the authoritative tuple maps to.

        Returns the full set rather than a row, so the caller can distinguish
        "no mapping" from "several mappings" instead of silently collapsing
        ambiguity into whichever row sorted first.
        """
        return self.resolve_with_route(
            source_system=source_system, namespace=namespace,
            external_id=external_id,
        )[0]

    def resolve_with_route(
        self, *, source_system: str, namespace: str, external_id: str
    ) -> tuple[set[int], str]:
        """:meth:`resolve`, plus which table answered.

        Approved bridges first; when they name nothing, the concept anchored
        by the tuple's own source usage (the desktop's live rule,
        ``taxon_lookup.resolve_installed_external_identity``). The search-pack
        shape carries no canonical source columns, so there it stays
        bridge-only.
        """
        rows = self._conn.execute(
            f"SELECT DISTINCT taxon_id FROM {self._mapping_table} "
            "WHERE source_system = ? AND namespace = ? AND external_id = ?",
            (source_system, namespace, str(external_id)),
        )
        matches = {int(row[0]) for row in rows}
        if matches or self._taxon_table != "taxon_min":
            return matches, self._mapping_table
        try:
            anchored = source_identity_matches(
                self._conn, source_system=source_system, namespace=namespace,
                external_id=str(external_id),
            )
        except sqlite3.OperationalError:
            anchored = set()
        return anchored, "taxon_min.canonical_source_usage"

    def concept_source(self, sporely_taxon_id: object) -> str | None:
        """``col_xr`` / ``nortaxa`` for a candidate artifact; ``None`` otherwise."""
        if self._taxon_table != "taxon_min":
            return None
        try:
            row = self._conn.execute(
                "SELECT source_system FROM taxon_min WHERE taxon_id = ?",
                (int(sporely_taxon_id),),
            ).fetchone()
        except sqlite3.OperationalError:
            return None
        return _text(row[0]) if row else None

    def canonical_name(self, sporely_taxon_id: object) -> str | None:
        if self._name_column is None:
            return None
        row = self._conn.execute(
            f"SELECT {self._name_column} FROM {self._taxon_table} WHERE taxon_id = ?",
            (int(sporely_taxon_id),),
        ).fetchone()
        return _text(row[0]) if row else None

    def concepts_named(self, name: str) -> list[int]:
        """Concepts whose canonical name is exactly ``name`` (case-insensitive).

        Candidate evidence for review only: a name is never identity.
        """
        if self._name_column is None or not _text(name):
            return []
        rows = self._conn.execute(
            f"SELECT taxon_id FROM {self._taxon_table} "
            f"WHERE {self._name_column} = ? COLLATE NOCASE ORDER BY taxon_id",
            (_text(name),),
        )
        return [int(row[0]) for row in rows]

    def namespaces_holding_external_id(self, external_id: object) -> list[str]:
        """Non-Sporely namespaces in which this literal value is an external ID.

        The collision signal. An integer that is simultaneously a valid
        ``taxon_id`` *and* some other registry's identifier is precisely the
        shape that let a namespace-lost external integer pass as a Sporely
        concept, and it is an evidence-based signal rather than a guess about
        the number's size or provenance.
        """
        rows = self._conn.execute(
            f"SELECT DISTINCT source_system, namespace FROM {self._mapping_table} "
            "WHERE external_id = ? AND NOT (source_system = ? AND namespace = ?)",
            (str(external_id), SPORELY_SOURCE_SYSTEM, SPORELY_NAMESPACE),
        )
        return sorted(f"{row[0]}/{row[1]}" for row in rows)


# ── Audited rows ────────────────────────────────────────────────────────────

#: Columns the audit reads. Everything else on the observation is out of
#: scope: this module must be incapable of touching microscopy or media.
AUDITED_COLUMNS = (
    "id",
    "sporely_taxon_id",
    "taxon_identity_state",
    "taxon_identity_proof",
    "taxon_identity_source_system",
    "taxon_identity_namespace",
    "taxon_identity_external_id",
    "taxon_identity_raw_external_id",
    "taxon_identity_provenance",
    "genus",
    "species",
    "common_name",
    "ai_selected_taxon_id",
    "ai_selected_scientific_name",
    "ai_selected_service",
    # Read only to recover an Artsorakel id the copy path failed to record
    # (the API carries it as ``scientific_name_id``); never written.
    "ai_state_json",
    "scientific_name_snapshot",
    "taxon_rank_snapshot",
)


@dataclass(frozen=True)
class AuditRecord:
    """One observation's classification and the evidence behind it."""

    observation_id: int
    origin: str
    identity_class: str
    name_class: str
    proposed_action: str
    stored_identity: dict
    stored_names: dict
    preserved_source_system: str | None = None
    preserved_namespace: str | None = None
    preserved_external_id: str | None = None
    preserved_raw_external_id: str | None = None
    candidate_sporely_taxon_id: int | None = None
    evidence: str | None = None
    refusal_reason: str | None = None
    restored_genus: str | None = None
    restored_species: str | None = None
    #: The provider id came from the stored AI prediction, not from
    #: ``ai_selected_taxon_id``.
    provider_id_recovered: bool = False
    #: Concepts whose canonical name equals the observer's identification,
    #: for rows with no bindable identity. Review candidates, never applied.
    name_candidate_ids: tuple[int, ...] = ()

    def as_dict(self) -> dict:
        return {
            "observation_id": self.observation_id,
            "origin": self.origin,
            "identity_class": self.identity_class,
            "name_class": self.name_class,
            "proposed_action": self.proposed_action,
            "stored_identity": dict(self.stored_identity),
            "stored_names": dict(self.stored_names),
            "preserved_source_system": self.preserved_source_system,
            "preserved_namespace": self.preserved_namespace,
            "preserved_external_id": self.preserved_external_id,
            "preserved_raw_external_id": self.preserved_raw_external_id,
            "candidate_sporely_taxon_id": self.candidate_sporely_taxon_id,
            "evidence": self.evidence,
            "refusal_reason": self.refusal_reason,
            "restored_genus": self.restored_genus,
            "restored_species": self.restored_species,
            "provider_id_recovered": self.provider_id_recovered,
            "name_candidate_ids": list(self.name_candidate_ids),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "AuditRecord":
        """Rebuild a record from an archived artifact.

        The apply path reads its pre-images from the reviewed file rather than
        from a freshly computed report, so this is the boundary that makes the
        archived artifact the thing actually applied.
        """
        return cls(
            observation_id=int(data["observation_id"]),
            origin=str(data["origin"]),
            identity_class=str(data["identity_class"]),
            name_class=str(data["name_class"]),
            proposed_action=str(data["proposed_action"]),
            stored_identity=dict(data.get("stored_identity") or {}),
            stored_names=dict(data.get("stored_names") or {}),
            preserved_source_system=data.get("preserved_source_system"),
            preserved_namespace=data.get("preserved_namespace"),
            preserved_external_id=data.get("preserved_external_id"),
            preserved_raw_external_id=data.get("preserved_raw_external_id"),
            candidate_sporely_taxon_id=data.get("candidate_sporely_taxon_id"),
            evidence=data.get("evidence"),
            refusal_reason=data.get("refusal_reason"),
            restored_genus=data.get("restored_genus"),
            restored_species=data.get("restored_species"),
            provider_id_recovered=bool(data.get("provider_id_recovered")),
            name_candidate_ids=tuple(int(i) for i in data.get("name_candidate_ids") or ()),
        )


@dataclass
class AuditReport:
    release_id: str | None
    artifact: str
    origin: str
    records: list[AuditRecord] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.records)

    def _census(self, attribute: str, vocabulary: Sequence[str]) -> dict[str, int]:
        counts = {name: 0 for name in vocabulary}
        for record in self.records:
            counts[getattr(record, attribute)] += 1
        return counts

    def counts(self) -> dict:
        identity = self._census("identity_class", IDENTITY_CLASSES)
        name = self._census("name_class", NAME_CLASSES)
        action = self._census("proposed_action", ACTIONS)
        return {
            "total_observations": self.total,
            "identity_class": identity,
            "name_class": name,
            "name_loss_population": sum(name[key] for key in NAME_LOSS_CLASSES),
            "proposed_action": action,
        }

    def reconciles(self) -> bool:
        """Whether every axis accounts for every row exactly once.

        The Part B gate requires reconciling counts; making that checkable in
        code rather than by eye is what keeps "counts reconcile" from becoming
        a box an operator ticks.
        """
        census = self.counts()
        return all(
            sum(census[axis].values()) == self.total
            for axis in ("identity_class", "name_class", "proposed_action")
        )

    def repairable(self) -> list[AuditRecord]:
        return [
            record
            for record in self.records
            if record.proposed_action != "no_change_required"
            and record.proposed_action != "report_only"
        ]

    def _body(self) -> dict:
        return {
            "artifact": self.artifact,
            "release_id": self.release_id,
            "origin": self.origin,
            "counts": self.counts(),
            "reconciles": self.reconciles(),
            "observations": [record.as_dict() for record in self.records],
        }

    def digest(self) -> str:
        """sha256 over the canonical body, excluding the digest itself.

        This is what binds an apply to the artifact a reviewer signed. Two
        different things can go wrong between review and apply — the file can
        be edited, and the database can move — and this catches the first.
        :func:`load_reviewed_artifact` catches it on read;
        :func:`require_artifact_still_describes` catches the second.
        """
        canonical = json.dumps(
            self._body(), sort_keys=True, ensure_ascii=False, separators=(",", ":")
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def as_dict(self) -> dict:
        return {**self._body(), "digest": self.digest()}

    @classmethod
    def from_dict(cls, data: dict) -> "AuditReport":
        return cls(
            release_id=data.get("release_id"),
            artifact=str(data.get("artifact") or ""),
            origin=str(data.get("origin") or ""),
            records=[
                AuditRecord.from_dict(entry)
                for entry in (data.get("observations") or [])
            ],
        )


# ── Classification ──────────────────────────────────────────────────────────


def _text(value: object) -> str | None:
    text = str(value if value is not None else "").strip()
    return text or None


def _split_binomial(name: object) -> tuple[str, str] | None:
    """``"Entoloma conferendum"`` → ``("Entoloma", "conferendum")``, or ``None``.

    Delegates to :func:`utils.taxon_text.split_scientific_name_text`, which is
    the rule the desktop already applies to exactly this field: the cloud pull
    path runs ``resolve_observation_taxon_fields`` over
    ``ai_selected_scientific_name`` whenever ``genus`` and ``species`` are both
    null, so a desktop client already reconstructs these names on its own.
    Reusing that function means the repair writes the string the client would
    have shown anyway, rather than introducing a second parsing rule that can
    drift away from it. A string that splitter cannot split — a bare genus, a
    placeholder — yields ``None``, and the row is reported unrepairable rather
    than having a guess written into it.
    """
    text = _text(name)
    if text is None:
        return None
    genus, species = split_scientific_name_text(text)
    if not genus or not species:
        return None
    return genus, species


def _classify_name(row: dict) -> tuple[str, tuple[str, str] | None]:
    """The name axis: did the save boundary destroy a usable name?

    The signal is the one the stage names, and all four parts of it are
    required: null ``genus``, null ``species``, null ``common_name``, beside a
    non-null ``ai_selected_scientific_name``. That combination is only
    reachable after the user copied a provider candidate into the
    identification (``ui/observations_tab.py``'s
    ``_preserve_ai_external_taxon_identity`` call site writes ``ai_selected_*``
    at exactly that moment), so it records an identification the user accepted
    and the save path then nulled out.

    A surviving ``common_name`` is deliberately disqualifying. The row is
    outside the defined population, so repairing it would propose production
    writes beyond what the stage specified and a reviewer approved. It is
    reported as ``partial_name_loss_reported`` instead of being repaired or
    hidden.
    """
    provider_name = _text(row.get("ai_selected_scientific_name"))
    if provider_name is None:
        return "name_intact", None
    if _text(row.get("genus")) is not None or _text(row.get("species")) is not None:
        return "name_intact", None
    if _text(row.get("common_name")) is not None:
        return "partial_name_loss_reported", None
    binomial = _split_binomial(provider_name)
    if binomial is None:
        return "name_loss_unrepairable_from_row", None
    return "name_loss_repairable_from_row", binomial


def _recovered_provider_id(row: dict) -> str | None:
    """The Artsorakel id the copy path failed to record, from the stored AI state.

    The Artsorakel API carries its prefixed id only as ``scientific_name_id``,
    which the desktop copy path did not read, so ``ai_selected_taxon_id``
    stayed null although the selected prediction held the id. Recovered only
    when every stored *selected* prediction whose name equals
    ``ai_selected_scientific_name`` agrees on one id; ``scientific_name_id_shared``
    (a GBIF id) is never used.
    """
    if _text(row.get("ai_selected_taxon_id")) is not None:
        return None
    if (_text(row.get("ai_selected_service")) or "").lower() != "artsorakel":
        return None
    chosen = _text(row.get("ai_selected_scientific_name"))
    if chosen is None:
        return None
    try:
        state = json.loads(row.get("ai_state_json") or "{}")
    except (TypeError, ValueError):
        return None
    selected = state.get("selected") if isinstance(state, dict) else None
    if not isinstance(selected, dict):
        return None
    ids = set()
    for prediction in selected.values():
        if not isinstance(prediction, dict):
            continue
        name = _text(
            prediction.get("scientificName")
            or prediction.get("scientific_name")
            or prediction.get("name")
        )
        if name is None or name.casefold() != chosen.casefold():
            continue
        name_id = _text(prediction.get("scientific_name_id") or prediction.get("scientificNameId"))
        if name_id is not None:
            ids.add(name_id)
    return next(iter(ids)) if len(ids) == 1 else None


def _provider_tuple(row: dict) -> tuple[str, str, str, str, bool] | None:
    """The row's own provider identifier, expressed in its bridge namespace.

    Returns ``(source_system, namespace, external_id, raw, recovered)``, where
    ``recovered`` says the id came from the stored AI state rather than
    ``ai_selected_taxon_id``. A bare integer yields ``None``: a namespace-lost
    integer names no registry, and inventing one is the leak this stage
    closes.
    """
    raw = _text(row.get("ai_selected_taxon_id"))
    recovered = False
    if raw is None:
        raw = _recovered_provider_id(row)
        recovered = raw is not None
    parsed = parse_prefixed_external_id(raw)
    if parsed is None:
        return None
    target = parsed.bridged() or parsed
    return (target.source_system, target.namespace, target.local_id, parsed.raw, recovered)


def _identified_name(row: dict) -> str | None:
    """The observer's identification as a name: genus and species as saved.

    A name-loss row (all names null beside a provider name the observer
    copied in) is identified by that provider name.
    """
    parts = [_text(row.get("genus")), _text(row.get("species"))]
    name = " ".join(part for part in parts if part)
    if name:
        return name
    if _classify_name(row)[0] == "name_loss_repairable_from_row":
        return _text(row.get("ai_selected_scientific_name"))
    return None


def _same_name(left: str | None, right: str | None) -> bool:
    return bool(left and right) and (
        " ".join(left.split()).casefold() == " ".join(right.split()).casefold()
    )


def _classify_identity(
    row: dict, artifact: TaxonomyArtifact
) -> tuple[str, dict]:
    """The identity axis, plus the evidence that decided it.

    Evaluated as an ordered ladder so exactly one class applies, and so the
    strongest evidence on the row always wins: a recorded proof outranks a
    re-derivable provider identifier, which outranks a bare legacy integer.
    """
    identity = TaxonIdentity.from_row(row)
    provider = _provider_tuple(row)

    # The tuple this row can be resolved through: its own preserved external
    # evidence first, the provider snapshot it was copied from second.
    if identity.has_external_evidence:
        lookup = (
            identity.source_system,
            identity.namespace,
            identity.external_id,
            identity.raw_external_id,
        )
        lookup_origin = "preserved_external_identity"
    elif provider is not None:
        lookup = provider[:4]
        lookup_origin = (
            "recovered_ai_prediction_identifier" if provider[4]
            else "provider_snapshot_identifier"
        )
    else:
        lookup = None
        lookup_origin = None

    resolved: set[int] = set()
    route = None
    if lookup is not None:
        resolved, route = artifact.resolve_with_route(
            source_system=lookup[0], namespace=lookup[1], external_id=lookup[2]
        )

    preserved = {
        "preserved_source_system": lookup[0] if lookup else None,
        "preserved_namespace": lookup[1] if lookup else None,
        "preserved_external_id": lookup[2] if lookup else None,
        "preserved_raw_external_id": (lookup[3] if lookup else None),
        "provider_id_recovered": lookup_origin == "recovered_ai_prediction_identifier",
    }
    bridge_evidence = (
        f"{lookup_origin}: {lookup[0]}/{lookup[1]}/{lookup[2]} resolved through "
        f"{artifact.path.name}:{route}"
        if lookup is not None
        else None
    )

    def name_gate(candidate: int) -> tuple[str, dict] | None:
        """D3: a resolved identifier binds only the observer's own taxon.

        Name agreement is necessary, never sufficient on its own: the
        identifier chose the concept; the name only confirms the observer
        identified that taxon. For an AI identifier the observer must also
        have kept the AI's name. ``None`` means the gate passes.
        """
        identified = _identified_name(row)
        concept = artifact.canonical_name(candidate)
        ai_name = _text(row.get("ai_selected_scientific_name"))
        from_ai = lookup_origin in (
            "provider_snapshot_identifier", "recovered_ai_prediction_identifier",
        )
        if from_ai and not _same_name(ai_name, identified):
            return "ai_suggestion_not_identification", {
                **preserved,
                "refusal_reason": (
                    f"{lookup[3]} is the AI suggestion {ai_name!r}; the "
                    f"observer's identification is {identified!r}"
                ),
                "evidence": bridge_evidence,
            }
        if not _same_name(concept, identified):
            return "identifier_name_mismatch", {
                **preserved,
                "refusal_reason": (
                    f"{lookup[3]} resolves to concept {candidate} {concept!r}, "
                    f"not the observer's identification {identified!r}"
                ),
                "evidence": bridge_evidence,
            }
        return None

    # ── A proof token already on the row ────────────────────────────────────
    if identity.is_proven_sporely:
        stored_id = int(identity.sporely_taxon_id)
        if not artifact.contains(stored_id):
            return "stale_sporely_identity", {
                **preserved,
                "refusal_reason": (
                    f"identity is proven ({identity.identity_proof}) but Sporely "
                    f"concept {stored_id} is absent from this artifact; a repair "
                    "would be a release question, not an identity question"
                ),
            }
        if resolved and stored_id not in resolved:
            return "suspicious_numeric_collision", {
                **preserved,
                "refusal_reason": (
                    f"stored proven identity {stored_id} contradicts the "
                    f"authoritative resolution {sorted(resolved)} of the row's "
                    "own namespaced identifier"
                ),
                "evidence": bridge_evidence,
            }
        return "proven_sporely_identity", {
            **preserved,
            "evidence": f"recorded proof {identity.identity_proof}",
        }

    # ── A pre-Stage-2 integer whose producer nothing records ────────────────
    if identity.is_legacy_unverified:
        stored_id = int(identity.sporely_taxon_id)
        if len(resolved) == 1:
            derived = next(iter(resolved))
            if derived == stored_id:
                refused = name_gate(derived)
                if refused is not None:
                    return refused
                return "proven_external_with_unique_bridge", {
                    **preserved,
                    "candidate_sporely_taxon_id": derived,
                    "evidence": (
                        f"{bridge_evidence}; independently re-derives the stored "
                        f"integer {stored_id}"
                    ),
                }
            return "suspicious_numeric_collision", {
                **preserved,
                "refusal_reason": (
                    f"stored legacy integer {stored_id} disagrees with the "
                    f"authoritative resolution {derived} of the row's own "
                    "namespaced identifier"
                ),
                "evidence": bridge_evidence,
            }
        if len(resolved) > 1:
            return "ambiguous_external_identity", {
                **preserved,
                "refusal_reason": (
                    f"{lookup[0]}/{lookup[1]}/{lookup[2]} matches "
                    f"{len(resolved)} concepts {sorted(resolved)}"
                ),
                "evidence": bridge_evidence,
            }
        colliding = artifact.namespaces_holding_external_id(stored_id)
        if colliding and artifact.contains(stored_id):
            return "suspicious_numeric_collision", {
                **preserved,
                "refusal_reason": (
                    f"{stored_id} is simultaneously a Sporely concept and an "
                    f"external identifier under {', '.join(colliding)}; nothing "
                    "on the row records which registry produced it"
                ),
            }
        return "legacy_unverified_identity", {
            **preserved,
            "refusal_reason": (
                f"{stored_id} has no recorded producer and the row carries no "
                "namespaced identifier to re-derive it from"
            ),
        }

    # ── A cloud-selected Sporely ID adopted on pull ─────────────────────────
    if identity.is_cloud_selected_unverified:
        stored_id = int(identity.sporely_taxon_id)
        return "cloud_selected_unverified_identity", {
            "refusal_reason": (
                f"Sporely concept {stored_id} came from the cloud selection "
                f"({identity.provenance}); the cloud cannot prove its producer "
                "and its own Sporely-namespace tuple is not independent evidence"
                + ("" if artifact.contains(stored_id) else
                   "; it is also absent from this artifact")
            ),
        }

    # ── Namespaced external evidence, resolved or not ───────────────────────
    if lookup is not None:
        if len(resolved) == 1:
            candidate = next(iter(resolved))
            refused = name_gate(candidate)
            if refused is not None:
                return refused
            return "proven_external_with_unique_bridge", {
                **preserved,
                "candidate_sporely_taxon_id": candidate,
                "evidence": bridge_evidence,
            }
        if len(resolved) > 1:
            return "ambiguous_external_identity", {
                **preserved,
                "refusal_reason": (
                    f"{lookup[0]}/{lookup[1]}/{lookup[2]} matches "
                    f"{len(resolved)} concepts {sorted(resolved)}"
                ),
                "evidence": bridge_evidence,
            }
        return "unresolved_external_identity", {
            **preserved,
            "refusal_reason": (
                f"{lookup[0]}/{lookup[1]}/{lookup[2]} has no authoritative "
                f"mapping in {artifact.path.name}"
            ),
        }

    # ── Nothing identifier-shaped at all ────────────────────────────────────
    return "manual_or_no_identity_evidence", {
        **preserved,
        "refusal_reason": (
            "no namespaced identifier on the row; a scientific name is not "
            "identity evidence"
        ),
    }


def _action_for(identity_class: str, name_class: str) -> str:
    binds = identity_class == "proven_external_with_unique_bridge"
    restores = name_class == "name_loss_repairable_from_row"
    if binds and restores:
        return "bind_sporely_identity_and_restore_lost_names"
    if binds:
        return "bind_sporely_identity"
    if restores:
        return "restore_lost_names"
    if identity_class == "proven_sporely_identity" and name_class == "name_intact":
        return "no_change_required"
    if (
        identity_class == "manual_or_no_identity_evidence"
        and name_class == "name_intact"
    ):
        return "no_change_required"
    return "report_only"


def classify_row(row: dict, artifact: TaxonomyArtifact, *, origin: str) -> AuditRecord:
    """Classify one observation. Pure: same row + same artifact, same record."""
    identity_class, details = _classify_identity(row, artifact)
    name_class, binomial = _classify_name(row)
    restored_genus, restored_species = binomial if binomial else (None, None)
    name_candidates: tuple[int, ...] = ()
    if identity_class not in _CONFIRMED_OR_BINDABLE:
        name_candidates = tuple(artifact.concepts_named(_identified_name(row) or ""))
    return AuditRecord(
        observation_id=int(row["id"]),
        origin=origin,
        identity_class=identity_class,
        name_class=name_class,
        proposed_action=_action_for(identity_class, name_class),
        stored_identity={
            key: row.get(key)
            for key in (
                "sporely_taxon_id",
                "taxon_identity_state",
                "taxon_identity_proof",
                "taxon_identity_source_system",
                "taxon_identity_namespace",
                "taxon_identity_external_id",
                "taxon_identity_raw_external_id",
                "taxon_identity_provenance",
            )
        },
        stored_names={
            key: row.get(key)
            for key in (
                "genus",
                "species",
                "common_name",
                "ai_selected_scientific_name",
                "ai_selected_taxon_id",
            )
        },
        preserved_source_system=details.get("preserved_source_system"),
        preserved_namespace=details.get("preserved_namespace"),
        preserved_external_id=details.get("preserved_external_id"),
        preserved_raw_external_id=details.get("preserved_raw_external_id"),
        candidate_sporely_taxon_id=details.get("candidate_sporely_taxon_id"),
        evidence=details.get("evidence"),
        refusal_reason=details.get("refusal_reason"),
        restored_genus=restored_genus,
        restored_species=restored_species,
        provider_id_recovered=bool(details.get("provider_id_recovered")),
        name_candidate_ids=name_candidates,
    )


#: Classes whose identity is already held or would be bound; name candidates
#: are only offered for the rest.
_CONFIRMED_OR_BINDABLE = frozenset({
    "proven_sporely_identity",
    "proven_external_with_unique_bridge",
    "cloud_selected_unverified_identity",
})


# ── Row sources ─────────────────────────────────────────────────────────────


def _available_columns(conn: sqlite3.Connection) -> list[str]:
    present = {row[1] for row in conn.execute("PRAGMA table_info(observations)")}
    missing = [name for name in AUDITED_COLUMNS if name not in present]
    if "id" in missing:
        raise SystemExit("observations table has no id column")
    return [name for name in AUDITED_COLUMNS if name in present]


def read_desktop_rows(observation_db_path: Path) -> list[dict]:
    """Every observation in a desktop SQLite database, id-ordered.

    A stable order is part of determinism: two runs over the same database
    must emit byte-identical artifacts, otherwise "the dry run was reviewed"
    says nothing about what a later run will do.
    """
    path = Path(observation_db_path)
    if not path.exists():
        raise SystemExit(f"observation database not found: {path}")
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        columns = _available_columns(conn)
        rows = conn.execute(
            f"SELECT {', '.join(columns)} FROM observations ORDER BY id"
        ).fetchall()
    finally:
        conn.close()
    return [{key: row[key] for key in row.keys()} for row in rows]


def read_cloud_rows(export_path: Path) -> list[dict]:
    """Cloud ``public.observations`` rows from a JSON export.

    Production is not readable from this module — reaching Supabase requires an
    authorized session, and the Part B gate keeps production mutation a
    separate reviewed operation anyway. So the cloud audit runs against an
    exported snapshot, which also makes the reviewed dry run reproducible
    against a frozen input rather than against a moving table.

    ``selected_sporely_taxon_id`` is the cloud spelling of the bound concept
    and is read into ``sporely_taxon_id``; ``resolved_sporely_taxon_id`` is
    derived product state and is deliberately not an audit input.
    """
    path = Path(export_path)
    if not path.exists():
        raise SystemExit(f"cloud export not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        payload = payload.get("observations", [])
    normalized = []
    for entry in payload:
        row = {key: entry.get(key) for key in AUDITED_COLUMNS}
        row["id"] = entry.get("id")
        if row.get("sporely_taxon_id") is None:
            row["sporely_taxon_id"] = entry.get("selected_sporely_taxon_id")
        normalized.append(row)
    return sorted(normalized, key=lambda row: int(row["id"]))


# ── Audit ───────────────────────────────────────────────────────────────────


def audit(
    rows: Iterable[dict], artifact: TaxonomyArtifact, *, origin: str
) -> AuditReport:
    report = AuditReport(
        release_id=artifact.release_id, artifact=artifact.path.name, origin=origin
    )
    for row in rows:
        report.records.append(classify_row(row, artifact, origin=origin))
    return report


# ── Review summary ──────────────────────────────────────────────────────────

#: The settings key under which a sync stores the last cloud state it saw
#: (``utils/cloud_sync_impl/baseline.py``); its ``observation.taxon_identity``
#: is the identity baseline (``sporely:<id>``, ``external:…``, ``""`` none).
_CLOUD_SNAPSHOT_PREFIX = "sporely_cloud_snapshot_obs_"


def _cloud_context(observation_db_path: Path) -> dict[int, dict]:
    """Per observation: cloud id, sync status and the last-synced cloud identity.

    Read-only. ``cloud_identity`` is ``None`` when no snapshot recorded an
    identity (never synced, or synced before identity joined the snapshot).
    """
    conn = sqlite3.connect(f"file:{Path(observation_db_path)}?mode=ro", uri=True)
    try:
        present = {row[1] for row in conn.execute("PRAGMA table_info(observations)")}
        if "cloud_id" not in present:
            return {}
        status = "sync_status" if "sync_status" in present else "NULL"
        rows = conn.execute(f"SELECT id, cloud_id, {status} FROM observations").fetchall()
        try:
            snapshots = dict(conn.execute(
                "SELECT key, value FROM settings WHERE key LIKE ?",
                (_CLOUD_SNAPSHOT_PREFIX + "%",),
            ).fetchall())
        except sqlite3.OperationalError:
            snapshots = {}
    finally:
        conn.close()
    context = {}
    for obs_id, cloud_id, sync_status in rows:
        identity = None
        raw = snapshots.get(f"{_CLOUD_SNAPSHOT_PREFIX}{_text(cloud_id) or ''}")
        if cloud_id and raw:
            try:
                observation = (json.loads(raw) or {}).get("observation") or {}
            except (TypeError, ValueError, AttributeError):
                observation = {}
            if isinstance(observation, dict) and "taxon_identity" in observation:
                identity = str(observation.get("taxon_identity") or "")
        context[int(obs_id)] = {
            "cloud_id": _text(cloud_id),
            "sync_status": _text(sync_status),
            "cloud_identity": identity,
        }
    return context


def _cloud_cell(context: dict) -> str:
    if not context.get("cloud_id"):
        return "no cloud id"
    identity = context.get("cloud_identity")
    known = "unknown" if identity is None else (identity or "none")
    return f"cloud id {context['cloud_id']}, cloud identity {known}, {context.get('sync_status') or '-'}"


def render_review_summary(
    report: AuditReport,
    *,
    artifact: TaxonomyArtifact,
    observation_db_path: Path | None = None,
) -> str:
    """A readable dry-run summary for the owner's review. Writes nothing."""
    context = _cloud_context(observation_db_path) if observation_db_path else {}
    records = report.records

    def name_of(record: AuditRecord) -> str:
        return _identified_name({**record.stored_names}) or "(no name)"

    def concept(taxon_id: int | None) -> str:
        if taxon_id is None:
            return "-"
        source = artifact.concept_source(taxon_id)
        label = artifact.canonical_name(taxon_id) or "?"
        return f"{taxon_id} {label}" + (f" ({source})" if source else "")

    lines = [
        "# Observation identity backfill — dry run",
        "",
        f"- Taxonomy release: `{report.release_id}` ({report.artifact})",
        f"- Observations audited: **{report.total}** ({report.origin})",
        f"- Every axis reconciles: {report.reconciles()}",
        "- Nothing was written. Proposed bindings are applied only through the "
        "gated `--apply` of a reviewed artifact.",
        "",
        "## Counts by identity class",
        "",
        "| Class | Count |",
        "|---|---:|",
    ]
    counts = report.counts()
    for name, count in counts["identity_class"].items():
        if count:
            lines.append(f"| {name} | {count} |")
    lines += ["", "## Counts by proposed action", "", "| Action | Count |", "|---|---:|"]
    for name, count in counts["proposed_action"].items():
        if count:
            lines.append(f"| {name} | {count} |")

    bindings = [r for r in records if r.identity_class == "proven_external_with_unique_bridge"]
    lines += [
        "", f"## 1. Proposed bindings ({len(bindings)})", "",
        "The identifier resolves to exactly one concept and the observer's "
        "identification is that concept's name.", "",
        "| Obs | Identification | Proposed concept | Identifier | Same-name concepts | Cloud |",
        "|---:|---|---|---|---:|---|",
    ]
    for r in bindings:
        source = "recovered " if r.provider_id_recovered else ""
        same_name = len(artifact.concepts_named(name_of(r)))
        lines.append(
            f"| {r.observation_id} | {name_of(r)} | {concept(r.candidate_sporely_taxon_id)} "
            f"| {source}{r.preserved_raw_external_id} | {same_name} "
            f"| {_cloud_cell(context.get(r.observation_id, {}))} |"
        )

    for klass, title in (
        ("ai_suggestion_not_identification", "AI suggestion the observer changed — never bound"),
        ("identifier_name_mismatch", "Identifier names another taxon — not bound"),
    ):
        rows = [r for r in records if r.identity_class == klass]
        lines += ["", f"## {title} ({len(rows)})", "",
                  "| Obs | Identification | Reason | Name candidates |", "|---:|---|---|---|"]
        for r in rows:
            lines.append(
                f"| {r.observation_id} | {name_of(r)} | {r.refusal_reason} "
                f"| {', '.join(concept(i) for i in r.name_candidate_ids) or '-'} |"
            )

    named = [r for r in records if r.identity_class not in _CONFIRMED_OR_BINDABLE
             and r.identity_class not in ("ai_suggestion_not_identification",
                                          "identifier_name_mismatch")]
    unique = [r for r in named if len(r.name_candidate_ids) == 1]
    ambiguous = [r for r in named if len(r.name_candidate_ids) > 1]
    lines += [
        "", f"## 2. Name-only candidates for review ({len(unique)})", "",
        "No usable identifier; the identification matches exactly one concept "
        "name. Candidates only — a name is not identity, and nothing here is "
        "applied.", "",
        "| Obs | Identification | Candidate | Current class | Cloud |", "|---:|---|---|---|---|",
    ]
    for r in unique:
        lines.append(
            f"| {r.observation_id} | {name_of(r)} | {concept(r.name_candidate_ids[0])} "
            f"| {r.identity_class} | {_cloud_cell(context.get(r.observation_id, {}))} |"
        )
    lines += [
        "", f"## 3. Ambiguous names — your decision ({len(ambiguous)})", "",
        "Several concepts share the name (typically a COL concept and an "
        "unreconciled NorTaxa concept). Never chosen between automatically.", "",
        "| Obs | Identification | Concepts | Current class |", "|---:|---|---|---|",
    ]
    for r in ambiguous:
        lines.append(
            f"| {r.observation_id} | {name_of(r)} "
            f"| {'; '.join(concept(i) for i in r.name_candidate_ids)} | {r.identity_class} |"
        )

    disagreements = []
    for r in records:
        if r.identity_class not in ("proven_sporely_identity", "cloud_selected_unverified_identity"):
            continue
        held = r.stored_identity.get("sporely_taxon_id")
        held_name = artifact.canonical_name(held) if held else None
        cloud = context.get(r.observation_id, {}).get("cloud_identity")
        notes = []
        if held_name and not _same_name(held_name, name_of(r)):
            notes.append(f"held concept is {held_name!r}")
        if cloud and held and cloud != f"sporely:{held}":
            notes.append(f"last-synced cloud identity is {cloud}")
        if notes:
            disagreements.append((r, "; ".join(notes)))
    lines += [
        "", f"## 4. Confirmed identities kept — disagreements to note ({len(disagreements)})", "",
        "These identities are preserved as they are; listed only where the held "
        "concept's name or the last-synced cloud identity differs.", "",
        "| Obs | Identification | Held identity | Note |", "|---:|---|---|---|",
    ]
    for r, note in disagreements:
        lines.append(
            f"| {r.observation_id} | {name_of(r)} | {concept(r.stored_identity.get('sporely_taxon_id'))} "
            f"({r.identity_class}) | {note} |"
        )

    rest = [r for r in named if not r.name_candidate_ids
            and r.identity_class != "manual_or_no_identity_evidence"]
    lines += [
        "", f"## 5. Other identifiers left unresolved ({len(rest)})", "",
        "| Obs | Identification | Class | Reason |", "|---:|---|---|---|",
    ]
    for r in rest:
        lines.append(f"| {r.observation_id} | {name_of(r)} | {r.identity_class} | {r.refusal_reason} |")
    no_evidence = [r for r in named if not r.name_candidate_ids
                   and r.identity_class == "manual_or_no_identity_evidence"]
    lines += ["", f"Rows with no identifier and no matching concept name: {len(no_evidence)} "
              "(e.g. unidentified, genus-only text without a concept, non-fungi)."]
    return "\n".join(lines) + "\n"


# ── Approved backfill manifest ──────────────────────────────────────────────

#: Proposed bindings split by how much the evidence settles. Only the first
#: three may be approved; ``same_name_ai_history_only`` rests on an AI-history
#: identifier plus name agreement while a COL concept shares the name, so the
#: choice between the two concepts would be made from the name alone.
BACKFILL_GROUPS = (
    "reviewed_bridge",
    "nortaxa_self_unique_name",
    "same_name_recorded_identity",
    "same_name_ai_history_only",
)
APPROVABLE_BACKFILL_GROUPS = BACKFILL_GROUPS[:3]
MANIFEST_FORMAT = "sporely-identity-backfill-manifest-v1"


def backfill_group(record: AuditRecord, artifact: TaxonomyArtifact) -> str | None:
    """The backfill group of a proposed binding, or ``None`` for any other row."""
    if record.identity_class != "proven_external_with_unique_bridge":
        return None
    if "taxon_min.canonical_source_usage" not in (record.evidence or ""):
        return "reviewed_bridge"
    identified = _identified_name(dict(record.stored_names))
    if len(artifact.concepts_named(identified or "")) <= 1:
        return "nortaxa_self_unique_name"
    if (record.evidence or "").startswith("preserved_external_identity"):
        return "same_name_recorded_identity"
    return "same_name_ai_history_only"


def build_approved_manifest(
    report: AuditReport,
    artifact: TaxonomyArtifact,
    *,
    groups: Sequence[str],
    batch_size: int,
    batch: int,
) -> dict:
    """A deterministic approved subset of the reviewed artifact's bindings.

    Rows are ordered by observation id within the chosen groups and cut into
    fixed-size batches, so the same artifact and arguments always name the
    same observations. The manifest is bound to the artifact by its digest.
    """
    unknown = [g for g in groups if g not in APPROVABLE_BACKFILL_GROUPS]
    if unknown:
        raise ValueError(f"not approvable: {unknown}")
    if batch_size < 1 or batch < 1:
        raise ValueError("batch and batch_size start at 1")
    selected = sorted(
        r.observation_id for r in report.records
        if backfill_group(r, artifact) in groups
    )
    start = (batch - 1) * batch_size
    return {
        "format": MANIFEST_FORMAT,
        "reviewed_artifact_digest": report.digest(),
        "release_id": report.release_id,
        "groups": list(groups),
        "batch_size": batch_size,
        "batch": batch,
        "rows_in_groups": len(selected),
        "observation_ids": selected[start:start + batch_size],
    }


def load_approved_manifest(
    path: Path, reviewed: AuditReport, artifact: TaxonomyArtifact
) -> set[int]:
    """The observation ids a manifest approves, after checking it belongs to
    ``reviewed`` and names only bindings of its own declared, approvable
    groups in it."""
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if data.get("format") != MANIFEST_FORMAT:
        raise TamperedAuditArtifact(f"{path} is not an identity backfill manifest")
    if data.get("reviewed_artifact_digest") != reviewed.digest():
        raise StaleAuditArtifact(
            f"{path} approves rows of a different reviewed artifact"
        )
    groups = list(data.get("groups") or ())
    if not groups or any(g not in APPROVABLE_BACKFILL_GROUPS for g in groups):
        raise TamperedAuditArtifact(f"{path} declares no or non-approvable groups: {groups}")
    ids = {int(i) for i in data.get("observation_ids") or ()}
    bindable = {
        r.observation_id for r in reviewed.records
        if backfill_group(r, artifact) in groups
    }
    outside = sorted(ids - bindable)
    if outside:
        raise TamperedAuditArtifact(
            f"{path} approves observations outside its groups {groups}: {outside}"
        )
    return ids


# ── Repair ──────────────────────────────────────────────────────────────────


@dataclass
class RepairStats:
    identities_bound: int = 0
    names_restored: int = 0
    #: Name-only repairs whose name was already filled in between the audit
    #: and the apply. Filling a null cannot destroy anything, so these are
    #: skipped rather than aborted — but they are counted, because a silent
    #: skip would make the applied result differ from the reviewed artifact
    #: with no trace. A coupled bind-and-restore record cannot land here: its
    #: identity pre-image includes the name columns and aborts first.
    names_skipped_changed_since_audit: int = 0
    rows_written: int = 0
    #: Rows with a cloud id whose bound identity was marked for the next push.
    marked_dirty: int = 0

    def as_dict(self) -> dict:
        return self.__dict__.copy()


class StaleAuditArtifact(RuntimeError):
    """The database no longer matches the artifact that was reviewed."""


class TamperedAuditArtifact(RuntimeError):
    """An archived artifact's contents do not match its recorded digest."""


def load_reviewed_artifact(path: Path) -> AuditReport:
    """Read an archived audit JSON and verify it is the file that was written.

    An apply that recomputes its own report is not bound to anything a
    reviewer saw: a row added or edited after the review would be classified
    and repaired in the same breath, and the pre-image guard would pass
    trivially because the pre-image came from that same run. So the apply path
    reads the reviewed file instead, and this function is where that file is
    checked for having been edited since it was written.
    """
    path = Path(path)
    if not path.exists():
        raise SystemExit(f"reviewed audit artifact not found: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    recorded = str(data.get("digest") or "")
    if not recorded:
        raise TamperedAuditArtifact(
            f"{path} carries no digest; it was not written by this tool and "
            "cannot be bound to a review"
        )
    report = AuditReport.from_dict(data)
    if report.digest() != recorded:
        raise TamperedAuditArtifact(
            f"{path} does not match its own digest — the file was edited "
            f"after it was written (recorded {recorded[:12]}…, "
            f"recomputed {report.digest()[:12]}…)"
        )
    return report


def require_artifact_still_describes(
    reviewed: AuditReport, rows: Iterable[dict], artifact: TaxonomyArtifact
) -> None:
    """Refuse unless re-auditing the live rows reproduces the reviewed file.

    Classification is deterministic, so a fresh audit over unchanged rows
    against the same release must reproduce the reviewed artifact exactly. Any
    difference — a new observation, an edited one, a different taxonomy
    release — means the reviewed file no longer describes this database, and
    therefore is neither a reviewed plan for it nor a valid rollback
    pre-image.

    This is the check the per-row pre-image guard cannot make: that guard only
    sees rows the artifact already lists, so on its own it would happily apply
    a reviewed plan to a database that has grown new unreviewed rows.
    """
    current = audit(rows, artifact, origin=reviewed.origin)
    if current.digest() == reviewed.digest():
        return
    reviewed_ids = {record.observation_id for record in reviewed.records}
    current_ids = {record.observation_id for record in current.records}
    added = sorted(current_ids - reviewed_ids)
    removed = sorted(reviewed_ids - current_ids)
    reviewed_by_id = {r.observation_id: r.as_dict() for r in reviewed.records}
    changed = sorted(
        record.observation_id
        for record in current.records
        if record.observation_id in reviewed_by_id
        and record.as_dict() != reviewed_by_id[record.observation_id]
    )
    raise StaleAuditArtifact(
        "the reviewed audit artifact no longer describes this database — "
        f"added observations {added or 'none'}, removed {removed or 'none'}, "
        f"changed {changed or 'none'}"
        + (
            ""
            if (added or removed or changed)
            else f"; release changed ({reviewed.release_id!r} → "
            f"{current.release_id!r})"
        )
        + ". Re-run the dry run and have it reviewed before applying."
    )


#: The identity columns whose pre-image must still match for an identity write
#: to be allowed. Exactly what the audit recorded in ``stored_identity``.
_IDENTITY_PRE_IMAGE_COLUMNS = (
    "sporely_taxon_id",
    "taxon_identity_state",
    "taxon_identity_proof",
    "taxon_identity_source_system",
    "taxon_identity_namespace",
    "taxon_identity_external_id",
    "taxon_identity_raw_external_id",
    "taxon_identity_provenance",
)

#: The accepted-name columns, which are part of the SAME pre-image.
#:
#: ``docs/supabase-sync-contract.md`` states that the bound concept and the
#: accepted ``genus``/``species``/``common_name`` are a single coupled change,
#: because a row whose name says one taxon and whose identity says another is
#: incoherent regardless of which half is right. So a name that moved after
#: the review disqualifies the identity write too: binding concept 7821
#: (*Entoloma conferendum*) onto a row someone has since renamed *Amanita
#: muscaria* would commit exactly that mismatch, even though the identity
#: columns themselves are untouched.
#:
#: Recorded by the audit in ``stored_names``.
_NAME_PRE_IMAGE_COLUMNS = ("genus", "species", "common_name")


def apply_repairs(
    report: AuditReport,
    *,
    observation_db_path: Path,
    release_id: str | None,
    gate: ProductionMigrationGate,
    approved_ids: set[int] | None = None,
    mark_dirty: bool = False,
) -> RepairStats:
    """Write only the repairs the audit proved, in one transaction.

    ``approved_ids`` (from :func:`load_approved_manifest`) narrows the write to
    an approved batch. ``mark_dirty`` marks every row whose identity was bound
    and that has a cloud id ``dirty`` in the same transaction, so the next sync
    pushes it deterministically; a pull alone never pushes an unchanged-cloud
    row (see the backfill plan's sync assessment).

    ``gate`` is required and is enforced here rather than by the caller.
    A gate that only the tests consult protects only the tests: every path
    that writes — the CLI, a future script, an operator at a REPL — has to
    pass through the same five conditions, so the check belongs at the write
    boundary. The name-loss repair is included in that, because restoring a
    name is still a production write.

    Three properties this function must keep, because the acceptance gate
    rests on them:

    * **Only proven rows are written.** The record's ``proposed_action`` is the
      only thing consulted, and that action is derived solely from evidence.
    * **A stale artifact cannot overwrite a newer identity.** The identity
      update matches the full pre-image the audit recorded, not just the
      observation id. If anything about the row's identity moved in between —
      a user selecting a better concept, another repair run, a pull — the
      write matches nothing and the whole apply aborts with
      :class:`StaleAuditArtifact`. Aborting rather than skipping is the point:
      the archived dry run is also the rollback pre-image, so once it stops
      describing the database it must not be applied at all. Re-run the dry
      run and have it reviewed again.
    * **A name is filled, never replaced.** The ``genus``/``species`` update
      carries its own ``IS NULL`` guard. Filling a null cannot destroy
      anything, so a row that gained a name in between is skipped and counted
      rather than aborting the run. That skip can only happen on a
      *name-only* repair: when the same record also binds an identity, the
      name columns are part of the identity pre-image above, so a changed name
      aborts instead of committing a name/identity mismatch.

    Identity is written as one coherent set of columns through
    :class:`~utils.taxon_identity.TaxonIdentity`, so a repaired row can never
    be the self-contradicting shape (new integer, old proof) that
    ``database.models.coherent_identity_columns`` exists to prevent.
    """
    require_open_gate(gate)
    # Machine-checkable regardless of what the operator asserted: a report
    # whose axes do not account for every row has not been fully classified,
    # and a reviewer cannot have reviewed what it does not describe.
    if not report.reconciles():
        raise ProductionWriteRefused(
            "NEEDS_YOU — audit counts do not reconcile; the report does not "
            "account for every row exactly once on every axis"
        )

    stats = RepairStats()
    repairs = report.repairable()
    if approved_ids is not None:
        repairs = [r for r in repairs if r.observation_id in approved_ids]
    if not repairs:
        return stats

    provenance = (
        f"{REPAIR_PROVENANCE_PREFIX}:{release_id}"
        if release_id
        else REPAIR_PROVENANCE_PREFIX
    )
    conn = sqlite3.connect(str(observation_db_path))
    try:
        conn.execute("BEGIN")
        for record in repairs:
            wrote = False
            if record.candidate_sporely_taxon_id is not None:
                identity = TaxonIdentity.unresolved_external(
                    source_system=record.preserved_source_system,
                    namespace=record.preserved_namespace,
                    external_id=record.preserved_external_id,
                    raw_external_id=record.preserved_raw_external_id,
                ).resolved_to(
                    record.candidate_sporely_taxon_id, provenance=provenance
                )
                if not identity.is_proven_sporely:
                    # Unreachable via the audit's own classification; kept as a
                    # hard stop so a future caller cannot hand this function a
                    # record that would write an unproven identity.
                    raise RuntimeError(
                        f"refusing to write an unproven identity for "
                        f"observation {record.observation_id}"
                    )
                columns = identity.to_row()
                # ``IS`` rather than ``=`` so a NULL pre-image compares as a
                # value; ``= NULL`` is never true and would abort every row
                # that had no identity, which is most of them.
                pre_image = [
                    record.stored_identity.get(name)
                    for name in _IDENTITY_PRE_IMAGE_COLUMNS
                ] + [
                    record.stored_names.get(name)
                    for name in _NAME_PRE_IMAGE_COLUMNS
                ]
                guarded = _IDENTITY_PRE_IMAGE_COLUMNS + _NAME_PRE_IMAGE_COLUMNS
                cursor = conn.execute(
                    "UPDATE observations SET "
                    + ", ".join(f"{name} = ?" for name in columns)
                    + " WHERE id = ? AND "
                    + " AND ".join(f"{name} IS ?" for name in guarded),
                    (*columns.values(), record.observation_id, *pre_image),
                )
                if not cursor.rowcount:
                    raise StaleAuditArtifact(
                        f"observation {record.observation_id}: its identity or "
                        "its accepted name changed after the dry run. The two "
                        "are a single coupled value, so binding the reviewed "
                        "concept now could leave the row naming one taxon and "
                        "identifying another. Re-run the dry run and have it "
                        "reviewed before applying."
                    )
                stats.identities_bound += 1
                wrote = True
                if mark_dirty:
                    cloud_id = conn.execute(
                        "SELECT cloud_id FROM observations WHERE id = ?",
                        (record.observation_id,),
                    ).fetchone()[0]
                    if cloud_id:
                        update_observation_sync_state(
                            conn.cursor(), record.observation_id,
                            sync_status="dirty", synced_at=None,
                        )
                        stats.marked_dirty += 1
            if record.restored_genus and record.restored_species:
                cursor = conn.execute(
                    "UPDATE observations SET genus = ?, species = ? "
                    "WHERE id = ? AND genus IS NULL AND species IS NULL",
                    (
                        record.restored_genus,
                        record.restored_species,
                        record.observation_id,
                    ),
                )
                if cursor.rowcount:
                    stats.names_restored += 1
                    wrote = True
                else:
                    stats.names_skipped_changed_since_audit += 1
            if wrote:
                stats.rows_written += 1
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()
    return stats


# ── Part B: the production migration gate ───────────────────────────────────


@dataclass(frozen=True)
class ProductionMigrationGate:
    """The five conditions Stage 4 Part B requires before a production write.

    Encoded rather than described because a gate that lives only in prose is a
    gate an operator can satisfy by believing it is satisfied. Every field
    defaults to the unsatisfied state, so the gate fails closed: a caller that
    forgets one gets ``NEEDS_YOU``, not a write.
    """

    dry_run_artifact_reviewed: bool = False
    counts_reconcile: bool = False
    candidate_release_validated: bool = False
    rollback_procedure_documented: bool = False
    integrity_checks_defined: bool = False

    #: Free-text pointers an operator records alongside the booleans, so the
    #: evidence for each "yes" is part of the same object.
    evidence: tuple[str, ...] = ()

    def blocking_reasons(self) -> list[str]:
        reasons = []
        if not self.dry_run_artifact_reviewed:
            reasons.append("dry-run artifact has not been reviewed")
        if not self.counts_reconcile:
            reasons.append("audit counts do not reconcile")
        if not self.candidate_release_validated:
            reasons.append("candidate taxonomy release is not validated")
        if not self.rollback_procedure_documented:
            reasons.append("rollback procedure is not documented")
        if not self.integrity_checks_defined:
            reasons.append(
                "observation/media/measurement integrity checks are not defined"
            )
        return reasons

    @property
    def is_open(self) -> bool:
        return not self.blocking_reasons()


class ProductionWriteRefused(SystemExit):
    """Raised as ``NEEDS_YOU`` when the Part B gate is not open."""


def require_open_gate(gate: ProductionMigrationGate) -> None:
    reasons = gate.blocking_reasons()
    if reasons:
        raise ProductionWriteRefused(
            "NEEDS_YOU — production migration gate is closed:\n"
            + "\n".join(f"  - {reason}" for reason in reasons)
        )


# ── CLI ─────────────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--taxonomy", type=Path, required=True)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--observations", type=Path)
    source.add_argument("--cloud-rows", type=Path)
    parser.add_argument("--output", type=Path, help="write the audit JSON here")
    parser.add_argument(
        "--summary",
        type=Path,
        help="dry run only: also write a readable review summary (Markdown) here",
    )
    backfill = parser.add_argument_group("Identity backfill batches")
    backfill.add_argument(
        "--manifest-out", type=Path,
        help="dry run only: write an approved-batch manifest bound to this run's artifact",
    )
    backfill.add_argument(
        "--manifest-groups", default="reviewed_bridge",
        help=f"comma-separated, from {', '.join(APPROVABLE_BACKFILL_GROUPS)}",
    )
    backfill.add_argument("--batch-size", type=int, default=10)
    backfill.add_argument("--batch", type=int, default=1)
    backfill.add_argument(
        "--approved-manifest", type=Path,
        help="with --apply: write only the observations this manifest approves",
    )
    backfill.add_argument(
        "--mark-dirty", action="store_true",
        help="with --apply: mark bound rows that have a cloud id dirty, in the same transaction",
    )
    parser.add_argument(
        "--apply",
        type=Path,
        metavar="REVIEWED_ARTIFACT",
        help=(
            "apply the repairs recorded in a previously written and reviewed "
            "audit JSON to --observations (never to cloud rows). The file is "
            "what gets applied; this command does not repair a report it "
            "computed itself"
        ),
    )
    # The Part B gate, one flag per condition. Deliberately not a single
    # --force: each condition is a separate assertion an operator is making,
    # and collapsing them into one switch would let four be satisfied by
    # remembering the fifth.
    gate_group = parser.add_argument_group(
        "Part B migration gate (all required with --apply)"
    )
    gate_group.add_argument("--gate-dry-run-reviewed", action="store_true")
    gate_group.add_argument("--gate-counts-reconcile", action="store_true")
    gate_group.add_argument("--gate-release-validated", action="store_true")
    gate_group.add_argument("--gate-rollback-documented", action="store_true")
    gate_group.add_argument("--gate-integrity-checks-defined", action="store_true")
    gate_group.add_argument(
        "--gate-evidence",
        action="append",
        default=[],
        metavar="POINTER",
        help="evidence for the assertions above; repeatable",
    )
    return parser


def gate_from_args(args) -> ProductionMigrationGate:
    return ProductionMigrationGate(
        dry_run_artifact_reviewed=args.gate_dry_run_reviewed,
        counts_reconcile=args.gate_counts_reconcile,
        candidate_release_validated=args.gate_release_validated,
        rollback_procedure_documented=args.gate_rollback_documented,
        integrity_checks_defined=args.gate_integrity_checks_defined,
        evidence=tuple(args.gate_evidence),
    )


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    artifact = TaxonomyArtifact(args.taxonomy)
    try:
        if args.observations is not None:
            rows = read_desktop_rows(args.observations)
            origin = "desktop"
        else:
            rows = read_cloud_rows(args.cloud_rows)
            origin = "cloud"
        if args.apply is None:
            # Dry run: classify and report. This is the artifact a reviewer
            # reads and archives, and the only thing --apply will accept later.
            report = audit(rows, artifact, origin=origin)
            payload = report.as_dict()
            if args.summary:
                args.summary.write_text(
                    render_review_summary(
                        report, artifact=artifact,
                        observation_db_path=args.observations,
                    ),
                    encoding="utf-8",
                )
            if args.manifest_out:
                manifest = build_approved_manifest(
                    report, artifact,
                    groups=[g.strip() for g in args.manifest_groups.split(",") if g.strip()],
                    batch_size=args.batch_size, batch=args.batch,
                )
                args.manifest_out.write_text(
                    json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8",
                )
        else:
            if origin != "desktop":
                raise ProductionWriteRefused(
                    "NEEDS_YOU — cloud rows are audited, never written. "
                    "Production mutation is a separate reviewed operation "
                    "behind the Part B gate."
                )
            # Gate first, so an operator who forgot a condition gets
            # NEEDS_YOU rather than an error about a file they were never
            # going to be allowed to apply.
            gate = gate_from_args(args)
            require_open_gate(gate)
            # The reviewed file is what gets applied. Recomputing a report
            # here and repairing that would mean applying a plan nobody read:
            # a row added or edited since the review would be classified and
            # written in the same breath, and every per-row pre-image would
            # match trivially because it came from this very run.
            try:
                reviewed = load_reviewed_artifact(args.apply)
                require_artifact_still_describes(reviewed, rows, artifact)
                approved_ids = (
                    load_approved_manifest(args.approved_manifest, reviewed, artifact)
                    if args.approved_manifest else None
                )
            except (StaleAuditArtifact, TamperedAuditArtifact) as exc:
                # Condition 1 of the gate — "the dry-run artifact has been
                # reviewed" — is false in substance whatever the flag says.
                raise ProductionWriteRefused(f"NEEDS_YOU — {exc}") from exc
            payload = reviewed.as_dict()
            payload["gate"] = {
                "is_open": gate.is_open,
                "blocking_reasons": gate.blocking_reasons(),
                "evidence": list(gate.evidence),
                "reviewed_artifact": str(args.apply),
                "reviewed_artifact_digest": reviewed.digest(),
            }
            try:
                payload["repair"] = apply_repairs(
                    reviewed,
                    observation_db_path=args.observations,
                    release_id=artifact.release_id,
                    gate=gate,
                    approved_ids=approved_ids,
                    mark_dirty=args.mark_dirty,
                ).as_dict()
            except StaleAuditArtifact as exc:
                raise ProductionWriteRefused(f"NEEDS_YOU — {exc}") from exc
        text = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False)
        if args.output:
            args.output.write_text(text + "\n", encoding="utf-8")
        else:
            print(text)
    finally:
        artifact.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
