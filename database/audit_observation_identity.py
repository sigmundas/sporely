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
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Sequence

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
)

#: Exactly one applies to every audited row. Counted separately from
#: ``identity_class`` because name loss is a different defect and a different
#: repair, not a kind of identity evidence.
NAME_CLASSES = (
    "name_intact",
    "name_loss_repairable_from_row",
    "name_loss_unrepairable_from_row",
)

#: The name-loss population the acceptance gate asks to be counted is the sum
#: of these two classes.
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
        rows = self._conn.execute(
            f"SELECT DISTINCT taxon_id FROM {self._mapping_table} "
            "WHERE source_system = ? AND namespace = ? AND external_id = ?",
            (source_system, namespace, str(external_id)),
        )
        return {int(row[0]) for row in rows}

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
        }


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

    def as_dict(self) -> dict:
        return {
            "artifact": self.artifact,
            "release_id": self.release_id,
            "origin": self.origin,
            "counts": self.counts(),
            "reconciles": self.reconciles(),
            "observations": [record.as_dict() for record in self.records],
        }


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

    The signal is the one the plan names — null ``genus`` and ``species``
    beside a non-null ``ai_selected_scientific_name``. That combination is only
    reachable after the user copied a provider candidate into the
    identification (``ui/observations_tab.py``'s
    ``_preserve_ai_external_taxon_identity`` call site writes ``ai_selected_*``
    at exactly that moment), so it records an identification the user accepted
    and the save path then nulled out.
    """
    provider_name = _text(row.get("ai_selected_scientific_name"))
    if provider_name is None:
        return "name_intact", None
    if _text(row.get("genus")) is not None or _text(row.get("species")) is not None:
        return "name_intact", None
    binomial = _split_binomial(provider_name)
    if binomial is None:
        return "name_loss_unrepairable_from_row", None
    return "name_loss_repairable_from_row", binomial


def _provider_tuple(row: dict) -> tuple[str, str, str, str] | None:
    """The row's own provider identifier, expressed in its bridge namespace.

    Returns ``(source_system, namespace, external_id, raw)``. A bare integer
    yields ``None``: a namespace-lost integer names no registry, and inventing
    one is the leak this stage closes.
    """
    parsed = parse_prefixed_external_id(row.get("ai_selected_taxon_id"))
    if parsed is None:
        return None
    target = parsed.bridged() or parsed
    return (target.source_system, target.namespace, target.local_id, parsed.raw)


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
        lookup = provider
        lookup_origin = "provider_snapshot_identifier"
    else:
        lookup = None
        lookup_origin = None

    resolved: set[int] = set()
    if lookup is not None:
        resolved = artifact.resolve(
            source_system=lookup[0], namespace=lookup[1], external_id=lookup[2]
        )

    preserved = {
        "preserved_source_system": lookup[0] if lookup else None,
        "preserved_namespace": lookup[1] if lookup else None,
        "preserved_external_id": lookup[2] if lookup else None,
        "preserved_raw_external_id": (lookup[3] if lookup else None),
    }
    bridge_evidence = (
        f"{lookup_origin}: {lookup[0]}/{lookup[1]}/{lookup[2]} resolved through "
        f"{artifact.path.name}"
        if lookup is not None
        else None
    )

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

    # ── Namespaced external evidence, resolved or not ───────────────────────
    if lookup is not None:
        if len(resolved) == 1:
            return "proven_external_with_unique_bridge", {
                **preserved,
                "candidate_sporely_taxon_id": next(iter(resolved)),
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
    )


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


# ── Repair ──────────────────────────────────────────────────────────────────


@dataclass
class RepairStats:
    identities_bound: int = 0
    names_restored: int = 0
    rows_written: int = 0

    def as_dict(self) -> dict:
        return self.__dict__.copy()


def apply_repairs(
    report: AuditReport,
    *,
    observation_db_path: Path,
    release_id: str | None,
) -> RepairStats:
    """Write only the repairs the audit proved, in one transaction.

    Two properties this function must keep, because the acceptance gate rests
    on them:

    * **Only proven rows are written.** The record's ``proposed_action`` is the
      only thing consulted, and that action is derived solely from evidence.
    * **A name is filled, never replaced.** The ``genus``/``species`` update
      carries its own ``IS NULL`` guard, so even if a record were somehow
      stale relative to the database, the write cannot destroy a name that
      appeared in between.

    Identity is written as one coherent set of columns through
    :class:`~utils.taxon_identity.TaxonIdentity`, so a repaired row can never
    be the self-contradicting shape (new integer, old proof) that
    ``database.models.coherent_identity_columns`` exists to prevent.
    """
    stats = RepairStats()
    repairs = report.repairable()
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
                conn.execute(
                    "UPDATE observations SET "
                    + ", ".join(f"{name} = ?" for name in columns)
                    + " WHERE id = ?",
                    (*columns.values(), record.observation_id),
                )
                stats.identities_bound += 1
                wrote = True
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
        "--apply",
        action="store_true",
        help="apply the proven repairs to --observations (never to cloud rows)",
    )
    return parser


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
        report = audit(rows, artifact, origin=origin)
        payload = report.as_dict()
        if args.apply:
            if origin != "desktop":
                raise ProductionWriteRefused(
                    "NEEDS_YOU — cloud rows are audited, never written. "
                    "Production mutation is a separate reviewed operation "
                    "behind the Part B gate."
                )
            payload["repair"] = apply_repairs(
                report,
                observation_db_path=args.observations,
                release_id=artifact.release_id,
            ).as_dict()
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
