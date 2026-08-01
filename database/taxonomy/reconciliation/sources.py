"""Read-only loaders for the pinned macrofungi release.

The engine consumes:

* ``taxon.jsonl`` — one row per Sporely concept (with rank, scope_state,
  canonical_source_system, canonical_external_id).
* ``taxon_external_id.jsonl`` — one row per namespaced external id;
  ``id_role`` records whether the row is an accepted binding or a
  name-usage / synonym relationship (Level 3 evidence).
* ``taxon_external_id_legacy_integer.jsonl`` — optional; the pinned
  release may keep it empty. Rows follow the same shape but with an
  integer external id.
* ``scientific_name.jsonl`` — one row per (taxon_id, name, is_preferred),
  used for Level-5 candidate generation only.

Additionally, if a ``canonical_registry_path`` is supplied (either a
single JSONL file or a shard directory produced by
``identity_registry.py``), it is loaded read-only and its
``(source, namespace, identifier) -> sporely_taxon_id`` triples are
indexed for Level-2/4 chain resolution. This is optional so tests that
only exercise the pinned release still pass.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator

from database.taxonomy.reconciliation.errors import ReleaseValidationError

logger = logging.getLogger(__name__)


CHUNK_BYTES = 1 * 1024 * 1024


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(CHUNK_BYTES)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _iter_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for line_no, raw in enumerate(handle, start=1):
            stripped = raw.strip()
            if not stripped:
                continue
            try:
                yield json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise ReleaseValidationError(
                    f"{path}:{line_no}: malformed JSONL row: {exc}"
                ) from exc


@dataclass(frozen=True, slots=True)
class TaxonConcept:
    """Minimal projection of a taxonomy_v2 concept row.

    Only the fields the resolver reasons about are stored — everything else
    stays in the release files and is loaded lazily by candidate generation.
    """

    taxon_id: int
    canonical_scientific_name: str
    canonical_source_system: str
    canonical_external_id: str
    taxon_rank: str | None
    scope_state: str
    taxonomic_status: str
    genus: str | None
    specific_epithet: str | None
    family: str | None


@dataclass(frozen=True, slots=True)
class ExternalIdRow:
    """One row from ``taxon_external_id.jsonl`` or its legacy variant."""

    taxon_id: int
    source_system: str
    namespace: str
    external_id: str
    id_role: str
    is_preferred: bool
    external_name: str | None


@dataclass
class PinnedRelease:
    """In-memory read-only view over the pinned macrofungi release.

    Instances are built by :meth:`load` and never mutate afterwards. The
    class holds three indexes:

    * ``taxa_by_id`` — sporely_taxon_id -> ``TaxonConcept``;
    * ``exact_index`` — (source_system, namespace, external_id) ->
      ``ExternalIdRow`` **filtered to id_role == 'accepted'**; the resolver
      uses this for Level 1;
    * ``synonym_index`` — same key but id_role != 'accepted'; the resolver
      uses this for Level 3;
    * ``scientific_name_index`` — lowercase name -> tuple[taxon_id, ...];
    * optional ``registry_index`` — (source, namespace, identifier) ->
      sporely_taxon_id, when a canonical registry was supplied.
    """

    release_dir: Path
    release_id: str
    scope_manifest_sha256: str
    taxa_by_id: dict[int, TaxonConcept] = field(default_factory=dict)
    exact_index: dict[tuple[str, str, str], ExternalIdRow] = field(default_factory=dict)
    synonym_index: dict[tuple[str, str, str], list[ExternalIdRow]] = field(
        default_factory=dict
    )
    scientific_name_index: dict[str, tuple[int, ...]] = field(default_factory=dict)
    registry_index: dict[tuple[str, str, str], int] = field(default_factory=dict)
    registry_identity_hash: str | None = None

    @classmethod
    def load(
        cls,
        release_dir: str | Path,
        *,
        canonical_registry_path: str | Path | None = None,
    ) -> "PinnedRelease":
        release_dir = Path(release_dir)
        if not release_dir.is_dir():
            raise ReleaseValidationError(f"release dir not found: {release_dir}")
        scope_manifest_path = release_dir / "scope-manifest.json"
        taxon_path = release_dir / "taxon.jsonl"
        exid_path = release_dir / "taxon_external_id.jsonl"
        legacy_path = release_dir / "taxon_external_id_legacy_integer.jsonl"
        sciname_path = release_dir / "scientific_name.jsonl"
        release_path = release_dir / "taxonomy_release.jsonl"
        for required in (scope_manifest_path, taxon_path, exid_path, release_path):
            if not required.exists():
                raise ReleaseValidationError(f"required release file missing: {required}")

        release_meta = next(_iter_jsonl(release_path), {})
        release_id = str(release_meta.get("content_release_id") or "")
        if not release_id:
            raise ReleaseValidationError(
                f"taxonomy_release.jsonl missing content_release_id in {release_path}"
            )
        scope_sha = _sha256_file(scope_manifest_path)

        instance = cls(
            release_dir=release_dir,
            release_id=release_id,
            scope_manifest_sha256=scope_sha,
        )

        for row in _iter_jsonl(taxon_path):
            try:
                taxon_id = int(row["taxon_id"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ReleaseValidationError(f"taxon row missing taxon_id: {row!r}") from exc
            concept = TaxonConcept(
                taxon_id=taxon_id,
                canonical_scientific_name=str(row.get("canonical_scientific_name") or ""),
                canonical_source_system=str(row.get("canonical_source_system") or ""),
                canonical_external_id=str(row.get("canonical_external_id") or ""),
                taxon_rank=(str(row["taxon_rank"]) if row.get("taxon_rank") else None),
                scope_state=str(row.get("scope_state") or ""),
                taxonomic_status=str(row.get("taxonomic_status") or ""),
                genus=(str(row["genus"]) if row.get("genus") else None),
                specific_epithet=(
                    str(row["specific_epithet"]) if row.get("specific_epithet") else None
                ),
                family=(str(row["family"]) if row.get("family") else None),
            )
            instance.taxa_by_id[taxon_id] = concept
            # Every taxon row implies a direct (canonical_source_system,
            # 'sporely_taxon_id', int_str) binding. Register that so a
            # `sporely` signal can be verified without walking exid rows.
            key_sporely = ("sporely", "sporely_taxon_id", str(taxon_id))
            instance.exact_index[key_sporely] = ExternalIdRow(
                taxon_id=taxon_id,
                source_system="sporely",
                namespace="sporely_taxon_id",
                external_id=str(taxon_id),
                id_role="accepted",
                is_preferred=True,
                external_name=concept.canonical_scientific_name or None,
            )
            # Also register a canonical row keyed under the source system
            # (e.g. col_xr:col_usage_id:323XQ) using the concept's
            # canonical_external_id. This makes the direct-taxonomy-v2
            # lookup work for observations that carry only the col_usage_id
            # signal without a matching taxon_external_id.jsonl row (which
            # every accepted concept always has, but the redundancy is
            # cheap and prevents surprises).
            if concept.canonical_source_system and concept.canonical_external_id:
                # namespace inferred from the canonical source system —
                # today only col_xr:col_usage_id fires here.
                canonical_namespace = _infer_canonical_namespace(concept.canonical_source_system)
                if canonical_namespace:
                    key_canonical = (
                        concept.canonical_source_system,
                        canonical_namespace,
                        concept.canonical_external_id,
                    )
                    instance.exact_index.setdefault(
                        key_canonical,
                        ExternalIdRow(
                            taxon_id=taxon_id,
                            source_system=concept.canonical_source_system,
                            namespace=canonical_namespace,
                            external_id=concept.canonical_external_id,
                            id_role="accepted",
                            is_preferred=True,
                            external_name=concept.canonical_scientific_name or None,
                        ),
                    )

        for row in _iter_jsonl(exid_path):
            try:
                taxon_id = int(row["taxon_id"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ReleaseValidationError(
                    f"external_id row missing taxon_id: {row!r}"
                ) from exc
            source = str(row.get("source_system") or "")
            ns = str(row.get("namespace") or "")
            ext = str(row.get("external_id") or "")
            role = str(row.get("id_role") or "")
            is_preferred = bool(row.get("is_preferred"))
            ext_row = ExternalIdRow(
                taxon_id=taxon_id,
                source_system=source,
                namespace=ns,
                external_id=ext,
                id_role=role,
                is_preferred=is_preferred,
                external_name=(
                    str(row["external_name"]) if row.get("external_name") else None
                ),
            )
            key = (source, ns, ext)
            if role == "accepted":
                if key in instance.exact_index and instance.exact_index[key].taxon_id != taxon_id:
                    raise ReleaseValidationError(
                        f"duplicate accepted binding for {key!r}: {instance.exact_index[key].taxon_id} vs {taxon_id}"
                    )
                instance.exact_index[key] = ext_row
            else:
                instance.synonym_index.setdefault(key, []).append(ext_row)

        if legacy_path.exists():
            for row in _iter_jsonl(legacy_path):
                try:
                    taxon_id = int(row["taxon_id"])
                except (KeyError, TypeError, ValueError) as exc:
                    raise ReleaseValidationError(
                        f"legacy integer row missing taxon_id: {row!r}"
                    ) from exc
                source = str(row.get("source_system") or "")
                ns = str(row.get("namespace") or "")
                ext = str(row.get("external_id") or "")
                role = str(row.get("id_role") or "accepted")
                ext_row = ExternalIdRow(
                    taxon_id=taxon_id,
                    source_system=source,
                    namespace=ns,
                    external_id=ext,
                    id_role=role,
                    is_preferred=bool(row.get("is_preferred")),
                    external_name=(
                        str(row["external_name"]) if row.get("external_name") else None
                    ),
                )
                key = (source, ns, ext)
                if role == "accepted":
                    instance.exact_index.setdefault(key, ext_row)
                else:
                    instance.synonym_index.setdefault(key, []).append(ext_row)

        if sciname_path.exists():
            for row in _iter_jsonl(sciname_path):
                name = str(row.get("scientific_name") or "").strip()
                if not name:
                    continue
                try:
                    taxon_id = int(row["taxon_id"])
                except (KeyError, TypeError, ValueError):
                    continue
                lc = name.casefold()
                existing = instance.scientific_name_index.get(lc)
                if existing is None:
                    instance.scientific_name_index[lc] = (taxon_id,)
                elif taxon_id not in existing:
                    instance.scientific_name_index[lc] = tuple(sorted(set(existing) | {taxon_id}))
            # Also index the taxon.jsonl canonical name so unaliased releases
            # still support Level-5 candidate lookups.
            for taxon_id, concept in instance.taxa_by_id.items():
                if concept.canonical_scientific_name:
                    lc = concept.canonical_scientific_name.casefold()
                    existing = instance.scientific_name_index.get(lc)
                    if existing is None:
                        instance.scientific_name_index[lc] = (taxon_id,)
                    elif taxon_id not in existing:
                        instance.scientific_name_index[lc] = tuple(sorted(set(existing) | {taxon_id}))

        if canonical_registry_path is not None:
            _load_registry_shards(Path(canonical_registry_path), instance)

        return instance

    # ------------------------------------------------------------------
    # Lookup helpers used by the resolver

    def lookup_exact(
        self,
        source_system: str,
        namespace: str,
        external_id: str,
    ) -> ExternalIdRow | None:
        return self.exact_index.get((source_system, namespace, external_id))

    def lookup_synonym(
        self,
        source_system: str,
        namespace: str,
        external_id: str,
    ) -> list[ExternalIdRow]:
        return list(self.synonym_index.get((source_system, namespace, external_id), ()))

    def lookup_registry(
        self,
        source_system: str,
        namespace: str,
        external_id: str,
    ) -> int | None:
        return self.registry_index.get((source_system, namespace, external_id))

    def concept(self, taxon_id: int) -> TaxonConcept | None:
        return self.taxa_by_id.get(taxon_id)

    def candidates_for_name(self, scientific_name: str) -> tuple[int, ...]:
        return self.scientific_name_index.get(scientific_name.casefold(), ())


def _infer_canonical_namespace(source_system: str) -> str | None:
    """Map a taxon canonical source system to its identifier namespace.

    Only known systems are returned; unknowns yield ``None`` so callers
    silently skip the redundancy registration. Add new mappings here when a
    future release introduces a non-col_xr canonical source.
    """
    if source_system == "col_xr":
        return "col_usage_id"
    return None


def _load_registry_shards(path: Path, target: PinnedRelease) -> None:
    if path.is_dir():
        manifest = path / "manifest.json"
        if not manifest.exists():
            raise ReleaseValidationError(f"registry manifest missing: {manifest}")
        manifest_data = json.loads(manifest.read_text(encoding="utf-8"))
        target.registry_identity_hash = str(
            manifest_data.get("concatenated_sha256") or ""
        )
        for shard in manifest_data.get("shards") or ():
            shard_name = shard.get("name") or shard.get("filename")
            if not shard_name:
                raise ReleaseValidationError(
                    "registry manifest shard missing 'name' (or 'filename')"
                )
            shard_path = path / str(shard_name)
            _index_registry_file(shard_path, target)
    else:
        target.registry_identity_hash = _sha256_file(path)
        _index_registry_file(path, target)


def _index_registry_file(path: Path, target: PinnedRelease) -> None:
    for row in _iter_jsonl(path):
        if row.get("__registry_header__"):
            continue
        try:
            sporely_id = int(row["sporely_taxon_id"])
        except (KeyError, TypeError, ValueError):
            continue
        source = str(row.get("source") or "")
        ns = str(row.get("namespace") or "")
        identifier = str(row.get("identifier") or "")
        if not (source and ns and identifier):
            continue
        key = (source, ns, identifier)
        target.registry_index.setdefault(key, sporely_id)
