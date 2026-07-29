#!/usr/bin/env python3
"""Deterministic Sporely taxonomy shared compiler (Stage 3A candidate).

This is the smallest compiler layer that consumes normalized inputs from any
number of sources (COL XR, NorTaxa, later Denmark, ...) and produces:

* ``taxa.jsonl``          — canonical Sporely-identified rows.
* ``mappings.jsonl``      — explicit source-to-Sporely mapping proposals
                             classified by relationship.
* ``diagnostics.json``    — validation counts, warnings, and blockers.
* ``manifest.json``       — content-release ID, schema version, bound source
                             releases, output byte hashes.

Design rules (traceable to ``docs/identity-contract.md``,
``docs/compatibility-contract.md``, and ``policies/mapping_policy.yml``):

* ``sporely_taxon_id`` is allocated only through the persistent
  :class:`IdentityRegistry`. No source identifier value or row index is ever
  reinterpreted as a Sporely ID.
* Only ``exact`` mappings approved by ``mapping_policy.yml`` or by an
  ``approved`` manual mapping automatically share identity. Every other
  candidate mapping (``likely_exact``, name-only, fuzzy, homonym, broader,
  narrower, ...) is preserved as a review proposal without merging.
* Unresolved accepted-name references blocked the source normalizer
  earlier; here we additionally fail closed if the shared compiler is asked
  to bind a mapping whose target usage does not appear in any loaded source.
* Unresolved parent references are preserved as warnings; no synthetic
  parent edge is invented.
* Duplicate or conflicting exact mappings (two approved manual mappings
  binding the same source usage to different Sporely IDs, or an approved
  mapping contradicting an existing registry binding) fail closed.
* Adding a new source cannot renumber previously allocated Sporely IDs: the
  registry is append-only and lookups precede allocation for every source
  usage.
* Content release IDs follow ``tax-YYYY.MM.DD-NN`` and
  ``TAXONOMY_SCHEMA_VERSION`` remains ``2``.
* Determinism: for the same normalized inputs, the same registry state, the
  same manual-mappings file, and the same ``--release-id``, two runs produce
  byte-identical outputs. All record ordering, dictionary key ordering, and
  numeric formatting are canonical.

This compiler produces a candidate. It never writes to SQLite, Supabase, or
any consumer runtime store.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Iterator

_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

from cross_source_mapping import (  # noqa: E402
    BackboneIndex,
    PROPOSAL_AMBIGUOUS,
    PROPOSAL_AUTOMATIC_EXACT,
    PROPOSAL_NATIONAL_ONLY,
    PROPOSAL_REJECTED,
    PROPOSAL_REVIEW_PROPOSED,
    classify_bridge_records,
    proposal_to_json,
    summarize as summarize_proposals,
)
from identity_registry import (  # noqa: E402
    Allocation,
    IdentityRegistry,
    RegistryError,
)


TAXONOMY_SCHEMA_VERSION = 2
RELEASE_ID_RE = re.compile(r"^tax-\d{4}\.\d{2}\.\d{2}-\d{2}$")
CHUNK_BYTES = 512 * 1024

# Explicit source priority for identity allocation. The backbone allocates
# first so every alias produced by the cross-source proposer or by an approved
# manual mapping references an existing anchor rather than creating one under
# a bridge source. See policies/source_priority.yml.
SOURCE_PRIORITY: tuple[str, ...] = ("col_xr", "nortaxa", "artportalen", "inaturalist")


def _source_priority(source_code: str) -> tuple[int, str]:
    try:
        return (SOURCE_PRIORITY.index(source_code), source_code)
    except ValueError:
        return (len(SOURCE_PRIORITY), source_code)


BACKBONE_SOURCE = "col_xr"


class CompilerError(Exception):
    """Raised on any deterministic-compile precondition or consistency failure."""


# ----- Normalized source I/O -----


@dataclass(frozen=True)
class NormalizedSourceReport:
    """Summary of one normalized source directory as consumed by the compiler."""

    source_code: str
    source_release: dict[str, str]
    identifier_namespaces: dict[str, str]
    archive_sha256: str
    record_counts: dict[str, int]
    reference_gaps: dict


@dataclass(frozen=True)
class NormalizedTaxonRecord:
    """A single normalized taxon row (matches national_source output schema)."""

    source_code: str
    source_release: dict[str, str]
    core_row_id_value: str
    core_row_id_namespace: str
    taxon_id_value: str
    taxon_id_namespace: str
    accepted_name_usage_id: dict | None
    parent_name_usage_id: dict | None
    parent_reference_resolution: str
    scientific_name: str
    authorship: str
    rank: str
    taxonomic_status: str
    external_ids: dict[str, str]
    classification: dict[str, str]
    inclusion_reason: str  # "fungi", "ancestor", or ""
    raw: dict = field(compare=False, hash=False)

    def kingdom(self) -> str:
        return self.classification.get("kingdom", "") if self.classification else ""


def _read_report(source_dir: Path) -> NormalizedSourceReport:
    report_path = source_dir / "report.json"
    if not report_path.exists():
        raise CompilerError(f"normalized source is missing report.json: {source_dir}")
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise CompilerError(f"{report_path}: malformed JSON: {exc}") from exc
    required = (
        "profile_source_code", "profile_source_release",
        "identifier_namespaces", "archive_sha256", "record_counts",
    )
    for field_name in required:
        if field_name not in report:
            raise CompilerError(f"{report_path}: missing field {field_name!r}")
    if report.get("compiler_ready") is not True:
        raise CompilerError(f"{report_path}: compiler_ready is not true")
    return NormalizedSourceReport(
        source_code=str(report["profile_source_code"]),
        source_release=dict(report["profile_source_release"]),
        identifier_namespaces=dict(report["identifier_namespaces"]),
        archive_sha256=str(report["archive_sha256"]),
        record_counts=dict(report["record_counts"]),
        reference_gaps=dict(report.get("reference_gaps", {})),
    )


def _iter_taxa(source_dir: Path) -> Iterator[NormalizedTaxonRecord]:
    taxa_path = source_dir / "taxa.jsonl"
    if not taxa_path.exists():
        raise CompilerError(f"normalized source is missing taxa.jsonl: {source_dir}")
    with taxa_path.open("r", encoding="utf-8") as handle:
        for line_number, raw in enumerate(handle, start=1):
            stripped = raw.strip()
            if not stripped:
                continue
            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise CompilerError(
                    f"{taxa_path}:{line_number}: malformed JSON: {exc}"
                ) from exc
            try:
                core_row = payload["core_row_id"]
                taxon_id = payload["taxon_id"]
                yield NormalizedTaxonRecord(
                    source_code=str(payload["source_code"]),
                    source_release=dict(payload["source_release"]),
                    core_row_id_value=str(core_row["value"]),
                    core_row_id_namespace=str(core_row["namespace"]),
                    taxon_id_value=str(taxon_id["value"]),
                    taxon_id_namespace=str(taxon_id["namespace"]),
                    accepted_name_usage_id=(
                        dict(payload["accepted_name_usage_id"])
                        if payload.get("accepted_name_usage_id") else None
                    ),
                    parent_name_usage_id=(
                        dict(payload["parent_name_usage_id"])
                        if payload.get("parent_name_usage_id") else None
                    ),
                    parent_reference_resolution=str(
                        payload.get("parent_reference_resolution", "absent")
                    ),
                    scientific_name=str(payload.get("scientific_name", "")),
                    authorship=str(payload.get("authorship", "")),
                    rank=str(payload.get("rank", "")),
                    taxonomic_status=str(payload.get("taxonomic_status", "")),
                    external_ids=dict(payload.get("external_ids", {})),
                    classification=dict(payload.get("classification", {})),
                    inclusion_reason=str(payload.get("col_inclusion_reason", "")),
                    raw=payload,
                )
            except (KeyError, TypeError) as exc:
                raise CompilerError(
                    f"{taxa_path}:{line_number}: missing required field: {exc}"
                ) from exc


def _iter_vernacular(source_dir: Path) -> Iterator[dict]:
    """Yield vernacular JSONL rows from a normalized source (if any).

    The national-source normalizer emits ``vernacular.jsonl`` next to
    ``taxa.jsonl`` when a VernacularName extension exists. Sources without a
    vernacular file yield nothing.
    """
    vern_path = source_dir / "vernacular.jsonl"
    if not vern_path.exists():
        return
    with vern_path.open("r", encoding="utf-8") as handle:
        for line_number, raw in enumerate(handle, start=1):
            stripped = raw.strip()
            if not stripped:
                continue
            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise CompilerError(
                    f"{vern_path}:{line_number}: malformed JSON: {exc}"
                ) from exc
            for required in ("source_code", "source_release", "core_row_id",
                             "vernacular_name", "language", "is_preferred"):
                if required not in payload:
                    raise CompilerError(
                        f"{vern_path}:{line_number}: missing field {required!r}"
                    )
            yield payload


# ----- Policy / manual-mapping loaders -----


@dataclass(frozen=True)
class ManualMapping:
    mapping_id: str
    source_usage: tuple[str, str, str]  # (source, namespace, identifier)
    target_source_usage: tuple[str, str, str] | None
    target_sporely_taxon_id: int | None
    relationship: str
    review_status: str


def _load_manual_mappings(path: Path) -> list[ManualMapping]:
    if not path.exists():
        raise CompilerError(f"manual-mappings file not found: {path}")
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise CompilerError(f"{path}: malformed JSON: {exc}") from exc
    if not isinstance(doc, dict) or "mappings" not in doc:
        raise CompilerError(f"{path}: expected object with 'mappings' key")
    out: list[ManualMapping] = []
    seen_source_usages: dict[tuple[str, str, str], str] = {}
    for index, entry in enumerate(doc.get("mappings") or []):
        if not isinstance(entry, dict):
            raise CompilerError(f"{path}: mapping {index} is not an object")
        mapping_id = str(entry.get("mapping_id") or f"mapping-{index}")
        su = entry.get("source_usage") or {}
        try:
            source_usage = (
                str(su["source"]), str(su["namespace"]), str(su["identifier"]),
            )
        except KeyError as exc:
            raise CompilerError(
                f"{path}: mapping {mapping_id}: source_usage missing {exc}"
            ) from exc
        target = entry.get("target") or {}
        target_su: tuple[str, str, str] | None = None
        target_id: int | None = None
        if "sporely_taxon_id" in target:
            target_id = int(target["sporely_taxon_id"])
        elif "source_usage" in target:
            tsu = target["source_usage"]
            target_su = (
                str(tsu["source"]), str(tsu["namespace"]), str(tsu["identifier"]),
            )
        else:
            raise CompilerError(
                f"{path}: mapping {mapping_id}: target requires "
                f"source_usage or sporely_taxon_id"
            )
        relationship = str(entry.get("relationship", ""))
        review_status = str(entry.get("review_status", ""))
        mapping = ManualMapping(
            mapping_id=mapping_id,
            source_usage=source_usage,
            target_source_usage=target_su,
            target_sporely_taxon_id=target_id,
            relationship=relationship,
            review_status=review_status,
        )
        # Duplicate/conflicting exact mappings against the SAME source usage
        # must fail closed. A source usage may not be pointed at two
        # different targets by two approved exact mappings.
        if source_usage in seen_source_usages and mapping.relationship == "exact" \
                and mapping.review_status == "approved":
            raise CompilerError(
                f"{path}: mapping {mapping_id} conflicts with earlier mapping "
                f"{seen_source_usages[source_usage]!r} on source usage "
                f"{source_usage!r}"
            )
        if mapping.relationship == "exact" and mapping.review_status == "approved":
            seen_source_usages[source_usage] = mapping_id
        out.append(mapping)
    return out


def _load_mapping_policy(path: Path) -> dict:
    if not path.exists():
        raise CompilerError(f"mapping-policy file not found: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise CompilerError(f"{path}: malformed JSON: {exc}") from exc


# ----- Compile core -----


def _canonical_dumps(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(CHUNK_BYTES)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _resolve_source_release(source_dirs: list[Path]) -> list[dict]:
    return [
        {
            "path": str(p),
        }
        for p in source_dirs
    ]


def compile_release(
    *,
    normalized_source_dirs: list[Path],
    manual_mappings_path: Path,
    mapping_policy_path: Path,
    registry_path: Path,
    output_dir: Path,
    release_id: str,
    source_release_manifests: dict[str, Path] | None = None,
) -> dict:
    """Compile a deterministic candidate release into ``output_dir``.

    ``source_release_manifests`` optionally maps ``source_code`` to the path of
    that source's canonical manifest (for example the acquired
    ``sources/nortaxa/1.284/manifest.json``). Those paths are hashed and
    recorded in the generated manifest so a compiled release fully binds its
    source-release pin.
    """
    if not RELEASE_ID_RE.match(release_id):
        raise CompilerError(
            f"release_id must match tax-YYYY.MM.DD-NN, got {release_id!r}"
        )
    if output_dir.exists() or output_dir.is_symlink():
        raise CompilerError(f"output directory already exists: {output_dir}")
    output_dir.parent.mkdir(parents=True, exist_ok=True)

    # ----- Load everything up front so we fail fast and deterministically. --
    _load_mapping_policy(mapping_policy_path)  # validated for parsability only
    manual_mappings = _load_manual_mappings(manual_mappings_path)

    source_reports: dict[str, NormalizedSourceReport] = {}
    for source_dir in normalized_source_dirs:
        report = _read_report(source_dir)
        if report.source_code in source_reports:
            raise CompilerError(
                f"two normalized sources declare source_code "
                f"{report.source_code!r}"
            )
        source_reports[report.source_code] = report

    # Load ALL taxa into memory as (source_code, row) tuples so that we can
    # deterministically order allocation. Registry stability requires a
    # stable driving order. We order by (source_code, taxon_id_namespace,
    # taxon_id_value) — a lex order on the external identifier.
    all_records: list[NormalizedTaxonRecord] = []
    per_source_records: dict[str, list[NormalizedTaxonRecord]] = {}
    for source_code, report in source_reports.items():
        source_dir = next(
            p for p in normalized_source_dirs if _read_report(p).source_code == source_code
        )
        per_source_records[source_code] = list(_iter_taxa(source_dir))
        all_records.extend(per_source_records[source_code])

    # Cross-source usage index for manual-mapping target resolution and for
    # duplicate-detection across sources.
    usage_index: dict[tuple[str, str, str], NormalizedTaxonRecord] = {}
    for record in all_records:
        key = (record.source_code, record.taxon_id_namespace, record.taxon_id_value)
        if key in usage_index:
            raise CompilerError(
                f"duplicate source usage across normalized inputs: {key!r}"
            )
        usage_index[key] = record

    # ----- Sporely fungal scope (bridge sources) ---------------------------
    # (Fungi ∪ ancestor closure) filter runs before synonym resolution so
    # that we do not walk synonym chains outside the fungal scope.
    # policies/scope.yml requires global_fungi + fungal_navigation_ancestors
    # for every source-of-truth. COL applies the scope during normalization
    # (col_inclusion_reason marks fungi vs ancestor); bridge sources are
    # scoped here so that unrelated non-Fungi Norwegian plants and animals
    # never receive a Sporely identity.
    scope_result = _apply_bridge_fungal_scope(
        per_source_records=per_source_records,
        backbone_source=BACKBONE_SOURCE,
    )
    kept_records: list[NormalizedTaxonRecord] = scope_result["kept"]
    scope_diagnostics: dict = scope_result["diagnostics"]

    # Recompute usage_index restricted to kept records.
    usage_index = {}
    for record in kept_records:
        key = (record.source_code, record.taxon_id_namespace, record.taxon_id_value)
        usage_index[key] = record
    all_records = kept_records

    # ----- Intra-source synonym resolution ----------------------------------
    # For every in-scope bridge row with taxonomicStatus == "synonym", walk
    # acceptedNameUsageID transitively through the source's own taxonID index
    # to a terminal non-synonym usage. This is intra-source, NOT a
    # cross-source identity match. Failures (missing target, cycle, self-
    # reference) block compilation.
    synonym_resolution = _resolve_intra_source_synonyms(
        per_source_records={
            code: records for code, records in per_source_records.items()
            if code != BACKBONE_SOURCE
        },
        per_source_records_unfiltered={
            code: records for code, records in per_source_records.items()
            if code != BACKBONE_SOURCE
        },
        kept_records=[r for r in all_records if r.source_code != BACKBONE_SOURCE],
    )
    synonym_to_accepted: dict[tuple[str, str, str], tuple[str, str, str]] = (
        synonym_resolution["synonym_to_accepted"]
    )
    synonym_diagnostics: dict = synonym_resolution["diagnostics"]
    # Pull required accepted targets back into scope. These are non-Fungi
    # kingdom rows that a Fungi synonym references as its accepted concept —
    # required for identity resolution, so they must exist in usage_index
    # even though the naive kingdom filter would exclude them.
    for record in synonym_resolution["pulled_in_accepted_records"]:
        key = (record.source_code, record.taxon_id_namespace, record.taxon_id_value)
        if key in usage_index:
            continue
        all_records.append(record)
        usage_index[key] = record

    # ----- Registry-driven identity allocation ------------------------------
    registry = IdentityRegistry(registry_path)
    try:
        registry.load()
    except RegistryError as exc:
        raise CompilerError(f"registry load failed: {exc}") from exc

    # Apply approved manual exact mappings first, so their target IDs (either
    # pre-existing anchors or targets to be allocated) are recorded before
    # any source-native allocation.
    approved_exact = sorted(
        (m for m in manual_mappings
         if m.relationship == "exact" and m.review_status == "approved"),
        key=lambda m: m.source_usage,
    )
    # First pass: for each mapping whose target is another source_usage, make
    # sure that target usage is anchored (allocate if new). Then bind the
    # source_usage as an alias.
    manual_bindings: dict[tuple[str, str, str], int] = {}
    for mapping in approved_exact:
        source_usage = mapping.source_usage
        if mapping.target_sporely_taxon_id is not None:
            target_id = mapping.target_sporely_taxon_id
            if registry.get_anchor(target_id) is None:
                raise CompilerError(
                    f"manual mapping {mapping.mapping_id!r} targets unknown "
                    f"sporely_taxon_id={target_id}"
                )
        else:
            assert mapping.target_source_usage is not None
            target_su = mapping.target_source_usage
            if target_su not in usage_index:
                raise CompilerError(
                    f"manual mapping {mapping.mapping_id!r} targets source "
                    f"usage {target_su!r} that is absent from every "
                    f"normalized input"
                )
            target_rec = usage_index[target_su]
            target_release = _release_string(target_rec.source_release)
            target_anchor = registry.allocate(
                source=target_rec.source_code,
                namespace=target_rec.taxon_id_namespace,
                identifier=target_rec.taxon_id_value,
                allocated_in_release=release_id,
                first_seen_source_release=target_release,
            )
            target_id = target_anchor.sporely_taxon_id
        # If source_usage is already anchored to a DIFFERENT ID, fail closed.
        existing = registry.lookup(*source_usage)
        if existing is not None and existing.sporely_taxon_id != target_id:
            raise CompilerError(
                f"manual mapping {mapping.mapping_id!r} conflicts with "
                f"registry: {source_usage!r} is already sporely_taxon_id="
                f"{existing.sporely_taxon_id}, cannot rebind to {target_id}"
            )
        if source_usage in manual_bindings and manual_bindings[source_usage] != target_id:
            raise CompilerError(
                f"conflicting approved exact mappings for {source_usage!r}: "
                f"{manual_bindings[source_usage]} vs {target_id}"
            )
        if source_usage in usage_index:
            source_rec = usage_index[source_usage]
            source_release = _release_string(source_rec.source_release)
        else:
            # Manual mapping may bind a historical source usage that is not
            # present in the current normalized inputs. That's permitted for
            # continuity records; we still need a source_release string.
            source_release = "manual-mapping"
        try:
            registry.bind_alias(
                existing_sporely_taxon_id=target_id,
                source=source_usage[0],
                namespace=source_usage[1],
                identifier=source_usage[2],
                allocated_in_release=release_id,
                first_seen_source_release=source_release,
            )
        except RegistryError as exc:
            raise CompilerError(str(exc)) from exc
        manual_bindings[source_usage] = target_id

    # ----- Phase 2a: allocate backbone anchors ------------------------------
    backbone_records = sorted(
        (r for r in all_records if r.source_code == BACKBONE_SOURCE),
        key=lambda r: (r.taxon_id_namespace, r.taxon_id_value),
    )
    for record in backbone_records:
        key = (record.source_code, record.taxon_id_namespace, record.taxon_id_value)
        if registry.lookup(*key) is not None:
            continue
        registry.allocate(
            source=key[0], namespace=key[1], identifier=key[2],
            allocated_in_release=release_id,
            first_seen_source_release=_release_string(record.source_release),
        )

    # ----- Phase 2b: classify TERMINAL ACCEPTED bridge records --------------
    # Cross-source mapping only applies to concepts, not to synonym usages.
    # Synonym usages are attached to their terminal accepted concept in
    # phase 2e via the intra-source `synonym_to_accepted` mapping.
    approved_manual_bridge_usages: set[tuple[str, str, str]] = set(manual_bindings)
    rejected_bridge_usages: set[tuple[str, str, str]] = {
        m.source_usage for m in manual_mappings if m.review_status == "rejected"
    }
    backbone_index = BackboneIndex.build([
        {
            "source_code": r.source_code,
            "taxon_id": {
                "namespace": r.taxon_id_namespace, "value": r.taxon_id_value,
            },
            "scientific_name": r.scientific_name,
            "authorship": r.authorship,
            "rank": r.rank,
            "taxonomic_status": r.taxonomic_status,
            "kingdom": r.kingdom(),
            "classification": r.classification,
        }
        for r in backbone_records
    ])
    bridge_records = [r for r in all_records if r.source_code != BACKBONE_SOURCE]
    synonym_usages: set[tuple[str, str, str]] = set(synonym_to_accepted.keys())
    accepted_bridge_records = [
        r for r in bridge_records
        if (r.source_code, r.taxon_id_namespace, r.taxon_id_value)
        not in synonym_usages
    ]
    cross_source_proposals = classify_bridge_records(
        bridge_records=[
            {
                "source_code": r.source_code,
                "taxon_id": {
                    "namespace": r.taxon_id_namespace, "value": r.taxon_id_value,
                },
                "scientific_name": r.scientific_name,
                "authorship": r.authorship,
                "rank": r.rank,
                "taxonomic_status": r.taxonomic_status,
                "kingdom": r.kingdom(),
                "classification": r.classification,
            }
            for r in accepted_bridge_records
            if (r.source_code, r.taxon_id_namespace, r.taxon_id_value)
            not in approved_manual_bridge_usages
        ],
        backbone_index=backbone_index,
        rejected_source_usages=rejected_bridge_usages,
    )

    # ----- Phase 2c: apply automatic-exact aliases to registry -------------
    auto_alias_applied: set[tuple[str, str, str]] = set()
    for proposal in sorted(cross_source_proposals,
                           key=lambda p: p.source_usage):
        if proposal.proposal_class != PROPOSAL_AUTOMATIC_EXACT:
            continue
        assert proposal.target_source_usage is not None
        target_anchor = registry.lookup(*proposal.target_source_usage)
        if target_anchor is None:
            raise CompilerError(
                f"auto-exact target {proposal.target_source_usage!r} has "
                f"no registry anchor (should have been allocated in phase 2a)"
            )
        bridge_su = proposal.source_usage
        bridge_rec = usage_index[bridge_su]
        try:
            registry.bind_alias(
                existing_sporely_taxon_id=target_anchor.sporely_taxon_id,
                source=bridge_su[0], namespace=bridge_su[1],
                identifier=bridge_su[2],
                allocated_in_release=release_id,
                first_seen_source_release=_release_string(bridge_rec.source_release),
            )
        except RegistryError as exc:
            raise CompilerError(str(exc)) from exc
        auto_alias_applied.add(bridge_su)

    # ----- Phase 2d: allocate remaining accepted bridge anchors ------------
    for record in sorted(accepted_bridge_records,
                         key=lambda r: (_source_priority(r.source_code),
                                        r.taxon_id_namespace, r.taxon_id_value)):
        key = (record.source_code, record.taxon_id_namespace, record.taxon_id_value)
        if registry.lookup(*key) is not None:
            continue
        registry.allocate(
            source=key[0], namespace=key[1], identifier=key[2],
            allocated_in_release=release_id,
            first_seen_source_release=_release_string(record.source_release),
        )

    # ----- Phase 2e: bind synonym usages to their terminal accepted --------
    # This is an intra-source relationship. The synonym's original identity
    # (taxonID, scientificName, authorship, taxonomicStatus) is preserved in
    # source_usages.jsonl. No canonical taxon row is created for a synonym.
    synonym_alias_applied: set[tuple[str, str, str]] = set()
    for synonym_key in sorted(synonym_to_accepted.keys()):
        accepted_key = synonym_to_accepted[synonym_key]
        accepted_anchor = registry.lookup(*accepted_key)
        if accepted_anchor is None:
            raise CompilerError(
                f"synonym {synonym_key!r} resolves to accepted "
                f"{accepted_key!r} which has no registry anchor"
            )
        # If the accepted target ended up as a cross-source alias (folded
        # onto COL), we still bind the synonym onto the SAME Sporely ID —
        # the anchor's sporely_taxon_id is authoritative regardless of kind.
        synonym_rec = usage_index.get(synonym_key)
        try:
            registry.bind_alias(
                existing_sporely_taxon_id=accepted_anchor.sporely_taxon_id,
                source=synonym_key[0], namespace=synonym_key[1],
                identifier=synonym_key[2],
                allocated_in_release=release_id,
                first_seen_source_release=(
                    _release_string(synonym_rec.source_release)
                    if synonym_rec else "synonym-resolution"
                ),
            )
        except RegistryError as exc:
            raise CompilerError(str(exc)) from exc
        synonym_alias_applied.add(synonym_key)

    # Persist registry BEFORE emitting outputs, so a crash after registry
    # flush leaves the identity commitment intact and re-runs are idempotent.
    try:
        registry.flush()
    except RegistryError as exc:
        raise CompilerError(f"registry flush failed: {exc}") from exc

    # ----- Build canonical taxa + source_usages listings --------------------
    # ONE canonical taxon record per sporely_taxon_id, sourced from the
    # registry anchor's normalized record. Separate source_usages.jsonl
    # records every binding — anchor and alias — pointing back to the same
    # Sporely identity.
    compiled_taxa: list[dict] = []
    source_usages: list[dict] = []
    seen_sporely_ids: set[int] = set()
    for allocation in registry.all_entries():
        binding_source_usage = (
            allocation.source, allocation.namespace, allocation.identifier,
        )
        record = usage_index.get(binding_source_usage)
        alias_reason = ""
        accepted_ref: dict | None = None
        if binding_source_usage in synonym_to_accepted:
            alias_reason = "synonym_of_accepted"
            accepted_key = synonym_to_accepted[binding_source_usage]
            accepted_ref = {
                "source": accepted_key[0],
                "namespace": accepted_key[1],
                "identifier": accepted_key[2],
            }
        elif binding_source_usage in auto_alias_applied:
            alias_reason = "cross_source_automatic_exact"
        elif binding_source_usage in approved_manual_bridge_usages:
            alias_reason = "manual_approved_exact"
        source_usages.append({
            "sporely_taxon_id": allocation.sporely_taxon_id,
            "source_code": allocation.source,
            "source_release": (
                record.source_release if record else
                {"version": "manual-mapping", "issued_date": ""}
            ),
            "source_usage": {
                "source": allocation.source,
                "namespace": allocation.namespace,
                "identifier": allocation.identifier,
            },
            "core_row_id": (
                {"value": record.core_row_id_value,
                 "namespace": record.core_row_id_namespace}
                if record else None
            ),
            "scientific_name": record.scientific_name if record else "",
            "authorship": record.authorship if record else "",
            "rank": record.rank if record else "",
            "taxonomic_status": record.taxonomic_status if record else "",
            "external_ids": record.external_ids if record else {},
            "identity_binding": allocation.kind,
            "alias_reason": alias_reason,
            "accepted_source_usage": accepted_ref,
            "inclusion_reason": record.inclusion_reason if record else "",
            # A synonym usage is preserved as a searchable name alias.
            "searchable_scientific_name_alias": (
                (record.scientific_name if record else "")
                if alias_reason == "synonym_of_accepted" else ""
            ),
        })
        if allocation.kind != "anchor" or record is None:
            continue
        if allocation.sporely_taxon_id in seen_sporely_ids:
            continue
        seen_sporely_ids.add(allocation.sporely_taxon_id)
        compiled_taxa.append({
            "sporely_taxon_id": allocation.sporely_taxon_id,
            "canonical_source_code": record.source_code,
            "canonical_source_release": record.source_release,
            "canonical_source_usage": {
                "source": record.source_code,
                "namespace": record.taxon_id_namespace,
                "identifier": record.taxon_id_value,
            },
            "scientific_name": record.scientific_name,
            "authorship": record.authorship,
            "rank": record.rank,
            "taxonomic_status": record.taxonomic_status,
            "classification": record.classification,
            "parent_name_usage_id": record.parent_name_usage_id,
            "parent_reference_resolution": record.parent_reference_resolution,
            "external_ids": record.external_ids,
            "inclusion_reason": record.inclusion_reason,
        })

    # Legacy compatibility shim so downstream diagnostics/vernacular logic
    # (which counts one row per Sporely identity) works unchanged.
    compiled_rows = compiled_taxa

    # ----- Write outputs into a temp staging dir, then atomic rename. -------
    staging = Path(tempfile.mkdtemp(
        prefix=f".{output_dir.name}.", suffix=".tmp",
        dir=str(output_dir.parent),
    ))
    committed = False
    try:
        taxa_out = staging / "taxa.jsonl"
        source_usages_out = staging / "source_usages.jsonl"
        mappings_out = staging / "mappings.jsonl"
        vernacular_out = staging / "vernacular.jsonl"
        diagnostics_out = staging / "diagnostics.json"
        manifest_out = staging / "manifest.json"

        compiled_taxa.sort(key=lambda r: r["sporely_taxon_id"])
        with taxa_out.open("w", encoding="utf-8") as handle:
            for row in compiled_taxa:
                handle.write(_canonical_dumps(row) + "\n")

        source_usages.sort(
            key=lambda r: (
                r["sporely_taxon_id"], r["source_code"],
                r["source_usage"]["namespace"], r["source_usage"]["identifier"],
            ),
        )
        with source_usages_out.open("w", encoding="utf-8") as handle:
            for row in source_usages:
                handle.write(_canonical_dumps(row) + "\n")

        mapping_records = _build_mapping_records(
            manual_mappings=manual_mappings,
            registry=registry,
            usage_index=usage_index,
            release_id=release_id,
        )
        # Merge deterministic cross-source proposals. A proposal record is
        # tagged with kind=cross_source_proposal so a reviewer can tell which
        # mappings came from the auto-classifier vs. the manual-mappings file.
        for proposal in cross_source_proposals:
            applied = proposal.source_usage in auto_alias_applied
            mapping_records.append(
                proposal_to_json(proposal, release_id=release_id,
                                 identity_applied=applied)
            )
        mapping_records.sort(
            key=lambda r: (
                r.get("kind", "manual"),
                r["relationship"], r["review_status"],
                r["source_usage"]["source"], r["source_usage"]["namespace"],
                r["source_usage"]["identifier"], r["mapping_id"],
            ),
        )
        with mappings_out.open("w", encoding="utf-8") as handle:
            for row in mapping_records:
                handle.write(_canonical_dumps(row) + "\n")

        # ----- Vernacular pipeline -----------------------------------------
        # Each source's normalized vernacular.jsonl links to its own
        # core_row_id. We resolve that core_row_id through the compiled rows
        # to obtain the correct sporely_taxon_id. Unresolved linkage fails
        # closed — vernaculars without a taxonomic anchor would surface as
        # dangling name results in the runtime search index.
        core_row_id_to_sporely: dict[tuple[str, str, str], int] = {}
        for usage in source_usages:
            if usage.get("core_row_id") is None:
                continue
            key = (
                usage["source_code"],
                usage["core_row_id"]["namespace"],
                usage["core_row_id"]["value"],
            )
            existing = core_row_id_to_sporely.get(key)
            if existing is not None and existing != usage["sporely_taxon_id"]:
                raise CompilerError(
                    f"core_row_id collision within compiled output: {key!r}"
                )
            core_row_id_to_sporely[key] = usage["sporely_taxon_id"]
        compiled_vernaculars: list[dict] = []
        # Track vernaculars whose core_row_id belongs to a source row that
        # was legitimately dropped by the fungal-scope filter. Those are not
        # errors — they are the vernacular rows of NorTaxa plants/animals we
        # deliberately excluded. Vernaculars whose core_row_id is unknown to
        # the source at all remain a hard failure.
        source_known_core_row_ids: dict[str, set[tuple[str, str]]] = {}
        for source_code, records in per_source_records.items():
            source_known_core_row_ids[source_code] = {
                (r.core_row_id_namespace, r.core_row_id_value) for r in records
            }
        vern_dropped_out_of_scope = 0
        vern_dropped_unknown = 0
        for source_dir in normalized_source_dirs:
            report = _read_report(source_dir)
            for entry in _iter_vernacular(source_dir):
                key = (
                    str(entry["source_code"]),
                    str(entry["core_row_id"]["namespace"]),
                    str(entry["core_row_id"]["value"]),
                )
                sporely_id = core_row_id_to_sporely.get(key)
                if sporely_id is None:
                    known = source_known_core_row_ids.get(
                        entry["source_code"], set()
                    )
                    if (key[1], key[2]) in known:
                        # Row exists in the source but was scoped out; drop.
                        vern_dropped_out_of_scope += 1
                        continue
                    raise CompilerError(
                        f"vernacular row references core_row_id {key!r} "
                        f"that does not resolve to any known source taxon"
                    )
                compiled_vernaculars.append({
                    "sporely_taxon_id": sporely_id,
                    "source_code": entry["source_code"],
                    "source_release": entry["source_release"],
                    "core_row_id": entry["core_row_id"],
                    "language": entry["language"],
                    "vernacular_name": entry["vernacular_name"],
                    "is_preferred": bool(entry["is_preferred"]),
                    "provenance": entry.get("provenance", {}),
                })
        compiled_vernaculars.sort(
            key=lambda r: (
                r["sporely_taxon_id"],
                r["source_code"],
                r["language"],
                r["vernacular_name"],
                r["core_row_id"]["namespace"],
                r["core_row_id"]["value"],
            ),
        )
        with vernacular_out.open("w", encoding="utf-8") as handle:
            for row in compiled_vernaculars:
                handle.write(_canonical_dumps(row) + "\n")

        diagnostics = _build_diagnostics(
            source_reports=source_reports,
            compiled_taxa=compiled_taxa,
            source_usages=source_usages,
            mapping_records=mapping_records,
            registry=registry,
            cross_source_proposal_counts=summarize_proposals(cross_source_proposals),
            compiled_vernaculars=compiled_vernaculars,
            scope_diagnostics=scope_diagnostics,
            auto_alias_applied_count=len(auto_alias_applied),
            vern_dropped_out_of_scope=vern_dropped_out_of_scope,
            synonym_diagnostics=synonym_diagnostics,
            synonym_alias_count=len(synonym_alias_applied),
            cross_source_proposals=cross_source_proposals,
        )
        diagnostics_out.write_text(
            json.dumps(diagnostics, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        source_bindings = []
        for source_code in sorted(source_reports):
            report = source_reports[source_code]
            entry = {
                "source_code": source_code,
                "source_release": report.source_release,
                "source_release_id": _release_string(report.source_release,
                                                    prefix=source_code + ":"),
                "identifier_namespaces": report.identifier_namespaces,
                "archive_sha256": report.archive_sha256,
                "record_counts": report.record_counts,
            }
            if source_release_manifests and source_code in source_release_manifests:
                manifest_path = source_release_manifests[source_code]
                if not manifest_path.exists():
                    raise CompilerError(
                        f"source_release manifest not found: {manifest_path}"
                    )
                entry["source_release_manifest"] = {
                    "path": str(manifest_path),
                    "sha256": _sha256_file(manifest_path),
                }
            source_bindings.append(entry)

        manifest = {
            "manifest_schema_version": 1,
            "taxonomy_schema_version": TAXONOMY_SCHEMA_VERSION,
            "content_release_id": release_id,
            "state": "candidate",
            "publication": "none",
            "source_bindings": source_bindings,
            "manual_mappings_sha256": _sha256_file(manual_mappings_path),
            "mapping_policy_sha256": _sha256_file(mapping_policy_path),
            "registry_sha256": _sha256_file(registry_path),
            "outputs": {
                "taxa": {
                    "name": taxa_out.name,
                    "sha256": _sha256_file(taxa_out),
                    "bytes": taxa_out.stat().st_size,
                },
                "source_usages": {
                    "name": source_usages_out.name,
                    "sha256": _sha256_file(source_usages_out),
                    "bytes": source_usages_out.stat().st_size,
                },
                "mappings": {
                    "name": mappings_out.name,
                    "sha256": _sha256_file(mappings_out),
                    "bytes": mappings_out.stat().st_size,
                },
                "vernacular": {
                    "name": vernacular_out.name,
                    "sha256": _sha256_file(vernacular_out),
                    "bytes": vernacular_out.stat().st_size,
                },
                "diagnostics": {
                    "name": diagnostics_out.name,
                    "sha256": _sha256_file(diagnostics_out),
                    "bytes": diagnostics_out.stat().st_size,
                },
            },
            "counts": diagnostics["counts"],
        }
        manifest_out.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        os.replace(staging, output_dir)
        committed = True
        return manifest
    finally:
        if not committed:
            import shutil
            shutil.rmtree(staging, ignore_errors=True)


FUNGI_KINGDOM = "Fungi"


def _count_auto_exact_by_rule(proposals) -> dict[str, int]:
    counts = {"strict": 0, "missing_authorship_classification": 0}
    for p in proposals:
        if p.proposal_class != PROPOSAL_AUTOMATIC_EXACT:
            continue
        reason = p.evidence.get("reason", "")
        if reason == "conservative_exact_rule_satisfied":
            counts["strict"] += 1
        elif reason == "missing_authorship_classification_rule_satisfied":
            counts["missing_authorship_classification"] += 1
    return counts


def _apply_bridge_fungal_scope(
    *,
    per_source_records: dict[str, list[NormalizedTaxonRecord]],
    backbone_source: str,
) -> dict:
    """Filter bridge-source records to (Fungi ∪ navigation-ancestors).

    Backbone records are passed through unchanged (their normalizer applied
    the scope). For each bridge source:

    1. seed set = rows with ``classification.kingdom == "Fungi"``;
    2. ancestor closure = walk ``parent_name_usage_id.value`` transitively
       through the source's own ``taxon_id`` index and add each resolved
       parent to the kept set until no new ancestor appears;
    3. drop everything else.

    Kingdom is read strictly from the Darwin Core term the archive publishes
    (preserved by ``national_source.py``). Kingdom is never inferred from a
    scientific-name display string. A bridge row with an empty kingdom that
    isn't pulled in as an ancestor of a Fungi row is dropped — that's the
    conservative choice: an unclassified row is out of scope until it earns
    inclusion via a resolved fungal descendant.
    """
    kept: list[NormalizedTaxonRecord] = []
    diagnostics: dict = {"backbone": {}, "bridges": {}}
    for source_code, records in per_source_records.items():
        if source_code == backbone_source:
            kept.extend(records)
            diagnostics["backbone"] = {
                "source_code": source_code,
                "kept": len(records),
            }
            continue
        by_taxon_id: dict[str, NormalizedTaxonRecord] = {}
        for record in records:
            by_taxon_id[record.taxon_id_value] = record
        seed_ids: set[str] = {
            record.taxon_id_value for record in records
            if record.kingdom() == FUNGI_KINGDOM
        }
        # Ancestor closure.
        target_ids: set[str] = set(seed_ids)
        ancestor_ids: set[str] = set()
        dangling_parent_pairs: set[tuple[str, str]] = set()
        frontier = list(seed_ids)
        while frontier:
            current = frontier.pop()
            record = by_taxon_id.get(current)
            if record is None:
                continue
            parent_ref = record.parent_name_usage_id
            if not parent_ref:
                continue
            parent_id = str(parent_ref.get("value", ""))
            if not parent_id:
                continue
            if parent_id in target_ids or parent_id in ancestor_ids:
                continue
            if parent_id not in by_taxon_id:
                # Preserved as a warning on the descendant; do not invent.
                dangling_parent_pairs.add((current, parent_id))
                continue
            ancestor_ids.add(parent_id)
            target_ids.add(parent_id)
            frontier.append(parent_id)
        # Filtered pass.
        filtered: list[NormalizedTaxonRecord] = []
        # Rewrite parent_reference_resolution for kept records against the
        # kept set — a parent that was "resolved" in the full source may
        # now sit outside the fungal scope.
        for record in records:
            if record.taxon_id_value not in target_ids:
                continue
            parent_ref = record.parent_name_usage_id
            new_resolution = record.parent_reference_resolution
            if parent_ref:
                parent_id = str(parent_ref.get("value", ""))
                if not parent_id:
                    new_resolution = "absent"
                elif parent_id in target_ids:
                    new_resolution = "resolved"
                else:
                    new_resolution = "unresolved"
            rewritten = NormalizedTaxonRecord(
                source_code=record.source_code,
                source_release=record.source_release,
                core_row_id_value=record.core_row_id_value,
                core_row_id_namespace=record.core_row_id_namespace,
                taxon_id_value=record.taxon_id_value,
                taxon_id_namespace=record.taxon_id_namespace,
                accepted_name_usage_id=record.accepted_name_usage_id,
                parent_name_usage_id=record.parent_name_usage_id,
                parent_reference_resolution=new_resolution,
                scientific_name=record.scientific_name,
                authorship=record.authorship,
                rank=record.rank,
                taxonomic_status=record.taxonomic_status,
                external_ids=record.external_ids,
                classification=record.classification,
                inclusion_reason=("ancestor" if record.taxon_id_value in ancestor_ids
                                  else "fungi"),
                raw=record.raw,
            )
            filtered.append(rewritten)
        kept.extend(filtered)
        diagnostics["bridges"][source_code] = {
            "input": len(records),
            "kept": len(filtered),
            "fungi_seed": len(seed_ids),
            "ancestors_added": len(ancestor_ids),
            "dropped_out_of_scope": len(records) - len(filtered),
            "unresolved_parent_references_after_scope": sum(
                1 for r in filtered
                if r.parent_reference_resolution == "unresolved"
            ),
            "dangling_parent_reference_count": len(dangling_parent_pairs),
        }
    return {"kept": kept, "diagnostics": diagnostics}


_SYNONYM_STATUSES = frozenset({"synonym"})
_ACCEPTED_STATUSES = frozenset({"accepted", "provisionally accepted", "valid"})


def _resolve_intra_source_synonyms(
    *,
    per_source_records: dict[str, list[NormalizedTaxonRecord]],
    per_source_records_unfiltered: dict[str, list[NormalizedTaxonRecord]],
    kept_records: list[NormalizedTaxonRecord],
) -> dict:
    """Walk ``acceptedNameUsageID`` chains within each source.

    For every kept synonym row, resolve to its terminal accepted / valid
    usage. Missing targets, cycles, and self-reference fail closed.

    Returns a mapping ``synonym_to_accepted[(source, ns, id)] = (source, ns,
    id)`` plus deterministic diagnostics.
    """
    kept_ids: dict[str, set[str]] = {}
    for record in kept_records:
        kept_ids.setdefault(record.source_code, set()).add(record.taxon_id_value)

    synonym_to_accepted: dict[tuple[str, str, str], tuple[str, str, str]] = {}
    per_source_counts: dict[str, dict[str, int]] = {}
    missing_targets: list[dict] = []
    cyclic_chains: list[dict] = []
    self_references: list[dict] = []
    pulled_in_records: list[NormalizedTaxonRecord] = []

    MAX_SAMPLES = 10

    for source_code, records in per_source_records.items():
        by_taxon_id = {r.taxon_id_value: r for r in records}
        # For accepted-target pull-in, we need to see the whole (pre-scope)
        # source. This lets a synonym whose accepted lives outside the
        # Fungi kingdom column still resolve.
        unfiltered_by_taxon_id = {
            r.taxon_id_value: r
            for r in per_source_records_unfiltered.get(source_code, [])
        }
        # Merge unfiltered into the walk index so chain resolution can cross
        # in-scope → out-of-scope boundaries.
        for tid, rec in unfiltered_by_taxon_id.items():
            by_taxon_id.setdefault(tid, rec)
        stats = {
            "synonym_rows_in_scope": 0,
            "synonym_rows_resolved": 0,
            "chain_length_over_one": 0,
            "missing_target": 0,
            "cyclic": 0,
            "self_reference": 0,
            "accepted_target_out_of_scope_pulled_back_in": 0,
        }
        for record in records:
            if record.taxon_id_value not in kept_ids.get(source_code, set()):
                continue
            if record.taxonomic_status not in _SYNONYM_STATUSES:
                continue
            stats["synonym_rows_in_scope"] += 1
            accepted_ref = record.accepted_name_usage_id
            if not accepted_ref or not accepted_ref.get("value"):
                stats["missing_target"] += 1
                if len(missing_targets) < MAX_SAMPLES:
                    missing_targets.append({
                        "source_code": source_code,
                        "synonym_taxon_id": record.taxon_id_value,
                        "reason": "empty_acceptedNameUsageID",
                    })
                continue
            chain: list[str] = [record.taxon_id_value]
            visited: set[str] = set(chain)
            current_id = str(accepted_ref["value"])
            failed = False
            while True:
                if current_id == chain[-1]:
                    stats["self_reference"] += 1
                    if len(self_references) < MAX_SAMPLES:
                        self_references.append({
                            "source_code": source_code,
                            "synonym_taxon_id": record.taxon_id_value,
                            "self_referenced_id": current_id,
                        })
                    failed = True
                    break
                if current_id in visited:
                    stats["cyclic"] += 1
                    if len(cyclic_chains) < MAX_SAMPLES:
                        cyclic_chains.append({
                            "source_code": source_code,
                            "synonym_taxon_id": record.taxon_id_value,
                            "chain": chain + [current_id],
                        })
                    failed = True
                    break
                target = by_taxon_id.get(current_id)
                if target is None:
                    stats["missing_target"] += 1
                    if len(missing_targets) < MAX_SAMPLES:
                        missing_targets.append({
                            "source_code": source_code,
                            "synonym_taxon_id": record.taxon_id_value,
                            "unknown_reference": current_id,
                            "walked_chain": chain,
                        })
                    failed = True
                    break
                visited.add(current_id)
                chain.append(current_id)
                if target.taxonomic_status in _ACCEPTED_STATUSES:
                    # Terminal accepted usage.
                    key_synonym = (source_code, record.taxon_id_namespace,
                                   record.taxon_id_value)
                    key_accepted = (source_code, target.taxon_id_namespace,
                                    target.taxon_id_value)
                    synonym_to_accepted[key_synonym] = key_accepted
                    stats["synonym_rows_resolved"] += 1
                    if len(chain) > 2:
                        stats["chain_length_over_one"] += 1
                    if target.taxon_id_value not in kept_ids.get(source_code, set()):
                        # Bring the accepted target into scope.
                        stats["accepted_target_out_of_scope_pulled_back_in"] += 1
                        pulled_in_records.append(target)
                        # And add to kept_ids so a later synonym in this loop
                        # sees the target as in-scope.
                        kept_ids.setdefault(source_code, set()).add(
                            target.taxon_id_value)
                    break
                if target.taxonomic_status not in _SYNONYM_STATUSES:
                    # Some other status (misapplied etc.) — treat as missing
                    # target (never invent an identity binding).
                    stats["missing_target"] += 1
                    if len(missing_targets) < MAX_SAMPLES:
                        missing_targets.append({
                            "source_code": source_code,
                            "synonym_taxon_id": record.taxon_id_value,
                            "terminal_status": target.taxonomic_status,
                            "walked_chain": chain,
                        })
                    failed = True
                    break
                next_ref = target.accepted_name_usage_id
                if not next_ref or not next_ref.get("value"):
                    stats["missing_target"] += 1
                    if len(missing_targets) < MAX_SAMPLES:
                        missing_targets.append({
                            "source_code": source_code,
                            "synonym_taxon_id": record.taxon_id_value,
                            "walked_chain": chain,
                            "reason": "chain_ends_at_synonym_with_no_accepted",
                        })
                    failed = True
                    break
                current_id = str(next_ref["value"])
            if failed:
                pass  # counted above; continue with next record
        per_source_counts[source_code] = stats

    total_failures = sum(
        stats["missing_target"] + stats["cyclic"] + stats["self_reference"]
        for stats in per_source_counts.values()
    )
    if total_failures:
        # Fail closed: the identity contract does NOT allow guessing an
        # accepted target. A source publisher must fix the archive.
        raise CompilerError(
            "synonym resolution failed: "
            f"missing_target={sum(s['missing_target'] for s in per_source_counts.values())}, "
            f"cyclic={sum(s['cyclic'] for s in per_source_counts.values())}, "
            f"self_reference={sum(s['self_reference'] for s in per_source_counts.values())}; "
            f"first samples: missing={missing_targets[:3]!r} "
            f"cyclic={cyclic_chains[:3]!r} self={self_references[:3]!r}"
        )
    return {
        "synonym_to_accepted": synonym_to_accepted,
        "pulled_in_accepted_records": pulled_in_records,
        "diagnostics": {
            "per_source": per_source_counts,
            "missing_target_samples": missing_targets,
            "cyclic_chain_samples": cyclic_chains,
            "self_reference_samples": self_references,
        },
    }


def _release_string(source_release: dict, prefix: str = "") -> str:
    parts = []
    if "version" in source_release:
        parts.append(str(source_release["version"]))
    if "issued_date" in source_release:
        parts.append(str(source_release["issued_date"]))
    value = ":".join(parts) if parts else "unknown"
    return f"{prefix}{value}" if prefix else value


def _build_mapping_records(
    *,
    manual_mappings: list[ManualMapping],
    registry: IdentityRegistry,
    usage_index: dict[tuple[str, str, str], NormalizedTaxonRecord],
    release_id: str,
) -> list[dict]:
    """One record per manual mapping, classified by relationship/state.

    The compiler intentionally does NOT synthesize automatic ``proposed``
    mappings from name-only matches; that is a downstream reviewer tool.
    """
    records: list[dict] = []
    for mapping in manual_mappings:
        target_sporely_taxon_id: int | None = None
        if mapping.target_sporely_taxon_id is not None:
            target_sporely_taxon_id = mapping.target_sporely_taxon_id
        elif mapping.target_source_usage is not None:
            anchor = registry.lookup(*mapping.target_source_usage)
            if anchor is not None:
                target_sporely_taxon_id = anchor.sporely_taxon_id
        records.append({
            "mapping_id": mapping.mapping_id,
            "source_usage": {
                "source": mapping.source_usage[0],
                "namespace": mapping.source_usage[1],
                "identifier": mapping.source_usage[2],
            },
            "target": (
                {"sporely_taxon_id": target_sporely_taxon_id}
                if target_sporely_taxon_id is not None else {"unresolved": True}
            ),
            "relationship": mapping.relationship,
            "review_status": mapping.review_status,
            "applied_in_release": release_id,
        })
    return records


def _build_diagnostics(
    *,
    source_reports: dict[str, NormalizedSourceReport],
    compiled_taxa: list[dict],
    source_usages: list[dict],
    mapping_records: list[dict],
    registry: IdentityRegistry,
    cross_source_proposal_counts: dict[str, int],
    compiled_vernaculars: list[dict],
    scope_diagnostics: dict,
    auto_alias_applied_count: int,
    vern_dropped_out_of_scope: int,
    synonym_diagnostics: dict,
    synonym_alias_count: int,
    cross_source_proposals: list,
) -> dict:
    per_source_usage_count: dict[str, int] = {}
    per_source_unresolved_parents: dict[str, int] = {}
    unresolved_parent_count = 0
    for usage in source_usages:
        per_source_usage_count[usage["source_code"]] = (
            per_source_usage_count.get(usage["source_code"], 0) + 1
        )
    for taxon in compiled_taxa:
        if taxon.get("parent_reference_resolution") == "unresolved":
            unresolved_parent_count += 1
            src = taxon["canonical_source_code"]
            per_source_unresolved_parents[src] = (
                per_source_unresolved_parents.get(src, 0) + 1
            )
    per_relationship: dict[str, int] = {}
    per_review_state: dict[str, int] = {}
    for mapping in mapping_records:
        per_relationship[mapping["relationship"]] = (
            per_relationship.get(mapping["relationship"], 0) + 1
        )
        per_review_state[mapping["review_status"]] = (
            per_review_state.get(mapping["review_status"], 0) + 1
        )
    per_language: dict[str, int] = {}
    for vern in compiled_vernaculars:
        per_language[vern["language"]] = per_language.get(vern["language"], 0) + 1
    return {
        "counts": {
            "compiled_taxa": len(compiled_taxa),
            "compiled_source_usages": len(source_usages),
            "compiled_rows": len(compiled_taxa),
            "distinct_sporely_taxon_ids": len(
                {t["sporely_taxon_id"] for t in compiled_taxa}
            ),
            "registry_anchors": registry.anchor_count(),
            "registry_aliases": registry.alias_count(),
            "manual_mappings": len(mapping_records),
            "per_source_usage_count": per_source_usage_count,
            "mapping_relationship_counts": per_relationship,
            "mapping_review_state_counts": per_review_state,
            "cross_source_proposal_counts": cross_source_proposal_counts,
            "cross_source_automatic_exact": cross_source_proposal_counts.get(
                "automatic_exact", 0),
            "cross_source_automatic_exact_by_rule": _count_auto_exact_by_rule(
                cross_source_proposals
            ),
            "source_synonym_resolved": synonym_alias_count,
            "review_proposed": cross_source_proposal_counts.get(
                "review_proposed", 0),
            "ambiguous": cross_source_proposal_counts.get("ambiguous", 0),
            "accepted_national_only": cross_source_proposal_counts.get(
                "national_only", 0),
            "rejected": cross_source_proposal_counts.get("rejected", 0),
            "automatic_exact_aliases_applied": auto_alias_applied_count,
            "unresolved_parent_references": unresolved_parent_count,
            "unresolved_parent_references_per_source": per_source_unresolved_parents,
            "compiled_vernacular_rows": len(compiled_vernaculars),
            "compiled_vernacular_language_counts": per_language,
            "vernacular_rows_dropped_out_of_scope": vern_dropped_out_of_scope,
            "sporely_scope": scope_diagnostics,
            "synonym_resolution": synonym_diagnostics,
        },
        "sources": {
            code: {
                "source_release": report.source_release,
                "archive_sha256": report.archive_sha256,
                "record_counts": report.record_counts,
                "reference_gaps": report.reference_gaps,
            }
            for code, report in sorted(source_reports.items())
        },
    }


# ----- CLI -----


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, action="append", required=True,
                        dest="sources",
                        help="normalized-source directory (repeatable)")
    parser.add_argument("--manual-mappings", type=Path, required=True)
    parser.add_argument("--mapping-policy", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--source-release-manifest", action="append", default=[],
                        metavar="SOURCE_CODE=PATH",
                        help="bind a source_release manifest, repeatable")
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    manifests: dict[str, Path] = {}
    for spec in args.source_release_manifest:
        if "=" not in spec:
            print(f"error: --source-release-manifest requires SOURCE=PATH: {spec!r}",
                  file=sys.stderr)
            return 2
        code, path = spec.split("=", 1)
        manifests[code] = Path(path)
    try:
        manifest = compile_release(
            normalized_source_dirs=args.sources,
            manual_mappings_path=args.manual_mappings,
            mapping_policy_path=args.mapping_policy,
            registry_path=args.registry,
            output_dir=args.output,
            release_id=args.release_id,
            source_release_manifests=manifests,
        )
    except CompilerError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
