#!/usr/bin/env python3
"""Backfill ``observations.sporely_taxon_id`` idempotently.

Precedence, as corrected by taxonomy-v2 closeout Stage 2:

1. an already-populated ``sporely_taxon_id`` is KEPT when it still exists in
   the taxonomy-v2 candidate and CLEARED when it does not — but membership is
   not proof of origin, so it stays *legacy-unverified* unless step 2
   independently re-derives the same value;
2. an ``ai_selected_taxon_id`` carrying an explicitly ``NBIC:``-prefixed
   identifier, resolved through the **authoritative namespaced** table
   ``taxon_external_id_text_min`` under a declared namespace bridge;
3. an ``artsdata_id`` — NOT resolved. See below;
4. a scientific-name lookup — NOT resolved. Reported only. See below;
5. no fill.

The backfill NEVER rewrites existing scientific-name / common-name / AI
snapshot columns. It touches ``sporely_taxon_id`` and the
``taxon_identity_*`` provenance columns that describe it.

Runs in one transaction. Reports per-source resolved counts. Refuses to run
against a taxonomy DB whose ``taxonomy_meta`` says schema != 2.

Why steps 2-4 changed
---------------------

This module was the only code in the desktop client that converted an
*external* identifier into a Sporely-owned ``sporely_taxon_id``, and it did so
on evidence the accepted identity contract forbids:

* It resolved against ``taxon_external_id_min``, which has no ``namespace``
  column — the namespace is discarded when that table is written. All four
  NorTaxa namespaces (``nortaxa_dwc_id``, ``nortaxa_taxon_id``,
  ``nortaxa_accepted_name_usage_id``, ``nortaxa_parent_name_usage_id``)
  collapse into one undifferentiated ``artsdatabanken`` integer space, so a
  NorTaxa ``taxonID`` could silently match a row that was really a parent- or
  accepted-name-usage reference. ``identity-contract.md`` designates such
  namespace-lost integers legacy/audit evidence only. The authoritative
  ``taxon_external_id_text_min`` table already carries
  ``(source_system, namespace, external_id)`` and is what step 2 now reads.
* ``LIMIT 1`` silently swallowed ambiguity, contradicting the rule that
  resolvers return ambiguity and never collapse it. Step 2 now requires
  exactly one distinct match.
* ``NBIC_PATTERN`` treated a *bare* integer as an NBIC identifier. A
  namespace-lost integer carries no evidence of which registry produced it, so
  accepting one invents a namespace. Only an explicit prefix is accepted now.
* ``artsdata_id`` holds an Artsobservasjoner **sighting** id on the current
  schema, and the historical claim that it holds a NorTaxa DwC taxon id is
  namespace-inferred by this module's own precedent rather than declared by
  any source. Resolving identity from it is not licensed; it is now counted
  and left NULL.
* Step 4 resolved identity from a unique scientific-name match. Name equality
  is explicitly not identity evidence. It is now a reported classification
  that repairs nothing.
* Step 1 promoted any already-populated integer that appeared in ``taxon_min``
  to an artifact-proven identity. Membership establishes existence, never
  origin: a legacy external integer that numerically collides with a real
  Sporely ID is a member too, so that promotion handed the collision a proof
  token and waved it through the cloud gate. Such a row now stays
  legacy-unverified, and the only sound promotion is step 2 re-deriving the
  same value from the row's own namespaced provider identifier.

Consequence worth stating plainly: a pre-Stage-2 row with no provider
identifier to re-derive from cannot be proven by this module at all, and its
identity will not be asserted to the cloud. That is the intended trade — the
alternative is asserting integers whose origin nothing records.

Steps 3 and 4 are therefore *reported, non-repairing*: the counters still
tell an operator how many rows have that shape, which is the audit signal the
closeout wants, without writing an unproven identity.
"""
from __future__ import annotations

import argparse
import json
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from utils.taxon_identity import (
    PROOF_EXTERNAL_ID_RESOLUTION,
    PROOF_TAXONOMY_V2_ARTIFACT,
    STATE_SPORELY,
    parse_prefixed_external_id,
)


logger = logging.getLogger(__name__)


class AmbiguousExternalIdentifier(Exception):
    """A namespaced lookup matched more than one Sporely concept."""


@dataclass
class BackfillStats:
    total_observations: int = 0
    already_populated_kept: int = 0
    already_populated_rejected: int = 0
    resolved_by_explicit_nbic: int = 0
    #: Pre-Stage-2 rows left as legacy-unverified. Membership in ``taxon_min``
    #: is not proof of origin, so these keep their integer WITHOUT a proof
    #: token and cloud sync continues to refuse to assert them.
    legacy_kept_unverified: int = 0
    #: Pre-Stage-2 rows whose stored integer was independently re-derived from
    #: their own namespaced provider identifier. The only sound promotion.
    legacy_promoted_by_namespaced_resolution: int = 0
    #: Rows carrying an ``artsdata_id``, reported only — see module docstring.
    artsdata_id_not_resolved: int = 0
    #: Rows whose scientific name matches exactly one Sporely concept.
    #: Reported only: name equality is not identity evidence.
    unique_scientific_name_not_resolved: int = 0
    ambiguous_scientific_name_left_null: int = 0
    #: Rows whose ``NBIC:`` identifier matched several concepts through the
    #: namespaced table. Left NULL rather than collapsed.
    ambiguous_external_identifier_left_null: int = 0
    unresolved_left_null: int = 0
    rows_touched: int = 0

    def as_dict(self) -> dict:
        return self.__dict__.copy()


def _fetch_valid_sporely_ids(conn: sqlite3.Connection) -> set[int]:
    return {int(r[0]) for r in conn.execute("SELECT taxon_id FROM taxon_min")}


def _resolve_namespaced_external_id(
    conn: sqlite3.Connection,
    *,
    source_system: str,
    namespace: str,
    external_id: str,
) -> int | None:
    """Resolve one authoritative ``(source, namespace, external_id)`` tuple.

    Reads the namespaced text table only. Raises
    :class:`AmbiguousExternalIdentifier` rather than picking a row when the
    tuple matches several concepts, so the caller can leave the column NULL
    instead of binding an arbitrary one.
    """
    try:
        rows = conn.execute(
            "SELECT DISTINCT taxon_id FROM taxon_external_id_text_min "
            "WHERE source_system = ? AND namespace = ? AND external_id = ?",
            (source_system, namespace, str(external_id)),
        ).fetchall()
    except sqlite3.OperationalError:
        # A taxonomy candidate built before the namespaced table existed has
        # no authoritative mapping to offer. That is "unresolved", not a
        # licence to fall back on the namespace-lost integer table.
        logger.warning(
            "taxonomy candidate has no taxon_external_id_text_min table; "
            "no external identifier can be authoritatively resolved",
        )
        return None
    matches = {int(row[0]) for row in rows}
    if not matches:
        return None
    if len(matches) > 1:
        raise AmbiguousExternalIdentifier(
            f"{source_system}/{namespace}/{external_id} matched {len(matches)} concepts"
        )
    return next(iter(matches))


def _resolve_via_scientific_name(
    conn: sqlite3.Connection, name: str,
) -> tuple[set[int], str]:
    if not name or not name.strip():
        return set(), "no_name"
    cleaned = name.strip()
    accepted = {int(r[0]) for r in conn.execute(
        "SELECT taxon_id FROM taxon_min "
        "WHERE canonical_scientific_name = ? COLLATE NOCASE", (cleaned,))}
    if len(accepted) == 1:
        return accepted, "canonical_unique"
    aliases = {int(r[0]) for r in conn.execute(
        "SELECT taxon_id FROM scientific_name_min "
        "WHERE scientific_name = ? COLLATE NOCASE", (cleaned,))}
    union = accepted | aliases
    if len(union) == 1:
        return union, "alias_unique"
    if len(union) > 1:
        return union, "ambiguous"
    return set(), "not_found"


def backfill(
    *,
    observation_db_path: Path,
    taxonomy_db_path: Path,
    dry_run: bool = False,
) -> BackfillStats:
    stats = BackfillStats()
    if not observation_db_path.exists():
        raise SystemExit(f"observation database not found: {observation_db_path}")
    if not taxonomy_db_path.exists():
        raise SystemExit(f"taxonomy candidate not found: {taxonomy_db_path}")

    tax_conn = sqlite3.connect(f"file:{taxonomy_db_path}?mode=ro", uri=True)
    try:
        meta = dict(tax_conn.execute("SELECT key, value FROM taxonomy_meta"))
    except sqlite3.DatabaseError as exc:
        raise SystemExit(f"taxonomy DB missing taxonomy_meta: {exc}") from exc
    if meta.get("taxonomy_schema_version") != "2":
        raise SystemExit(
            f"taxonomy DB schema mismatch: {meta.get('taxonomy_schema_version')!r}"
        )
    # Provenance for every identity this backfill writes: the taxonomy
    # release the re-verification was performed against. Without it a
    # re-verified row says only "artifact-proven", with no way to tell which
    # artifact — and the stage requires release provenance to be preserved
    # when it is available. `content_release_id` is what
    # `database/taxonomy/scripts/build_sqlite_candidate.py` records.
    _release_id = str(meta.get("content_release_id") or "").strip()
    backfill_provenance = (
        f"sporely_taxonomy_v2_backfill:{_release_id}" if _release_id
        else "sporely_taxonomy_v2_backfill"
    )

    valid_ids = _fetch_valid_sporely_ids(tax_conn)

    obs = sqlite3.connect(observation_db_path)
    obs.row_factory = sqlite3.Row
    try:
        cursor = obs.execute(
            "SELECT id, sporely_taxon_id, artsdata_id, "
            "       ai_selected_taxon_id, ai_selected_scientific_name, "
            "       genus, species, "
            "       taxon_identity_state, taxon_identity_proof "
            "FROM observations"
        )
        rows = cursor.fetchall()
    except sqlite3.OperationalError as exc:
        raise SystemExit(
            f"observation table lacks required column (run schema.init_database first): {exc}"
        ) from exc

    # (sporely_taxon_id, state, proof, source_system, namespace, external_id,
    #  raw_external_id, provenance, observation_id)
    #
    # Every identity column is rewritten on every queued row, provenance
    # included. A backfill that rewrote the binding but left the previous
    # provenance in place would produce exactly the incoherent rows
    # `database/models.coherent_identity_columns` exists to prevent, with the
    # old release still claiming to explain the new identity.
    updates: list[tuple] = []

    def _queue_sporely(
        obs_id: int,
        sporely_id: int,
        proof: str,
        *,
        source_system: str | None = None,
        namespace: str | None = None,
        external_id: str | None = None,
        raw_external_id: str | None = None,
        provenance: str | None = None,
    ) -> None:
        updates.append((
            sporely_id, STATE_SPORELY, proof,
            source_system, namespace, external_id, raw_external_id,
            provenance or backfill_provenance, obs_id,
        ))

    def _queue_cleared(obs_id: int) -> None:
        updates.append((None, None, None, None, None, None, None, None, obs_id))

    class _Ambiguous(Exception):
        pass

    def _resolve_from_provider_snapshot(row) -> tuple | None:
        """Re-derive identity from the row's own prefixed provider identifier.

        Returns ``(sporely_id, target, parsed)`` on a single unambiguous
        authoritative match, ``None`` when there is nothing to resolve, and
        raises :class:`_Ambiguous` when the tuple matches several concepts.

        This is the ONLY sound way this module can establish a Sporely
        identity, because it is the only one that starts from a namespaced
        external identifier rather than from a bare integer.
        """
        parsed = parse_prefixed_external_id(row["ai_selected_taxon_id"])
        if parsed is None:
            return None
        target = parsed.bridged() or parsed
        try:
            sporely = _resolve_namespaced_external_id(
                tax_conn,
                source_system=target.source_system,
                namespace=target.namespace,
                external_id=target.local_id,
            )
        except AmbiguousExternalIdentifier as exc:
            raise _Ambiguous(str(exc)) from exc
        if sporely is None:
            return None
        return (sporely, target, parsed)

    def _queue_resolved(obs_id: int, resolved: tuple) -> None:
        sporely, target, parsed = resolved
        _queue_sporely(
            obs_id, sporely, PROOF_EXTERNAL_ID_RESOLUTION,
            source_system=target.source_system,
            namespace=target.namespace,
            external_id=target.local_id,
            # The verbatim provider string, retained per
            # identity-contract.md even after a successful bridge.
            raw_external_id=parsed.raw,
        )

    for row in rows:
        stats.total_observations += 1
        obs_id = int(row["id"])
        current = row["sporely_taxon_id"]
        if current is not None:
            if int(current) not in valid_ids:
                stats.already_populated_rejected += 1
                _queue_cleared(obs_id)  # invalid → clear, provenance included
                continue
            stats.already_populated_kept += 1
            if row["taxon_identity_state"] == STATE_SPORELY and row[
                "taxon_identity_proof"
            ] in (PROOF_TAXONOMY_V2_ARTIFACT, PROOF_EXTERNAL_ID_RESOLUTION):
                # Provenance already records real proof. Untouched, which is
                # what keeps this backfill idempotent.
                continue

            # The row holds a bare pre-Stage-2 integer. Membership in
            # ``taxon_min`` is NOT proof of origin: a legacy external integer
            # that numerically collides with a real Sporely ID is a member
            # too, and promoting on membership alone would hand that
            # collision a proof token and wave it through the cloud gate —
            # precisely the defect this stage exists to close. Membership only
            # decides keep-vs-clear.
            #
            # The one sound promotion is re-derivation from the row's own
            # namespaced provider identifier, and only when it agrees with the
            # stored integer.
            try:
                resolved = _resolve_from_provider_snapshot(row)
            except _Ambiguous as exc:
                logger.warning(
                    "observation %s: keeping sporely_taxon_id unverified — %s",
                    obs_id, exc,
                )
                resolved = None
            if resolved is not None and int(resolved[0]) == int(current):
                stats.legacy_promoted_by_namespaced_resolution += 1
                _queue_resolved(obs_id, resolved)
                continue
            if resolved is not None:
                logger.warning(
                    "observation %s: stored sporely_taxon_id %s disagrees with "
                    "the authoritative resolution %s of its provider "
                    "identifier; keeping the stored value unverified",
                    obs_id, current, resolved[0],
                )
            # Left exactly as it is: the integer stays (it is real persisted
            # data), no provenance is written, so ``TaxonIdentity.from_row``
            # continues to read it as legacy-unverified and cloud sync keeps
            # refusing to assert it.
            stats.legacy_kept_unverified += 1
            continue

        # Step 2: an explicitly prefixed provider identifier, resolved through
        # the authoritative namespaced table under a declared namespace bridge.
        # A bare integer is rejected by ``parse_prefixed_external_id``.
        try:
            resolved = _resolve_from_provider_snapshot(row)
        except _Ambiguous as exc:
            logger.warning(
                "observation %s: leaving sporely_taxon_id NULL — %s",
                obs_id, exc,
            )
            stats.ambiguous_external_identifier_left_null += 1
            stats.unresolved_left_null += 1
            continue
        if resolved is not None:
            stats.resolved_by_explicit_nbic += 1
            _queue_resolved(obs_id, resolved)
            continue

        # Step 3: ``artsdata_id`` — reported, never resolved.
        if row["artsdata_id"] is not None:
            stats.artsdata_id_not_resolved += 1

        # Step 4: scientific-name match — reported, never resolved.
        name_candidates = [row["ai_selected_scientific_name"]]
        if row["genus"] and row["species"]:
            name_candidates.append(f"{row['genus']} {row['species']}")
        matched: set[int] | None = None
        for name in name_candidates:
            if not name or not str(name).strip():
                continue
            candidates, _kind = _resolve_via_scientific_name(tax_conn, str(name))
            if candidates:
                matched = candidates
                break
        if matched is not None:
            if len(matched) == 1:
                stats.unique_scientific_name_not_resolved += 1
            else:
                stats.ambiguous_scientific_name_left_null += 1

        # Every row that reaches here ends with no identity. The step-3/step-4
        # counters above describe the *shape* of the evidence that was
        # deliberately not acted on; this one counts the outcome, so the two
        # overlap by design.
        stats.unresolved_left_null += 1

    if not dry_run:
        obs.execute("BEGIN")
        try:
            for update in updates:
                obs.execute(
                    "UPDATE observations SET "
                    "  sporely_taxon_id = ?,"
                    "  taxon_identity_state = ?,"
                    "  taxon_identity_proof = ?,"
                    "  taxon_identity_source_system = ?,"
                    "  taxon_identity_namespace = ?,"
                    "  taxon_identity_external_id = ?,"
                    "  taxon_identity_raw_external_id = ?,"
                    "  taxon_identity_provenance = ?"
                    " WHERE id = ?",
                    update,
                )
            obs.execute("COMMIT")
        except Exception:
            obs.execute("ROLLBACK")
            raise
    stats.rows_touched = sum(1 for update in updates if update[0] is not None)
    tax_conn.close()
    obs.close()
    return stats


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--observations", type=Path, required=True)
    parser.add_argument("--taxonomy", type=Path, required=True)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    stats = backfill(
        observation_db_path=args.observations,
        taxonomy_db_path=args.taxonomy,
        dry_run=args.dry_run,
    )
    print(json.dumps(stats.as_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
