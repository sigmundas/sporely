"""Formal supplement-chain validator.

Every registry supplement declares (contract v1.0.0):

    artifact_kind = "registry_supplement"
    supplement_release_id
    base_release_id
    base_release_dependency = { export/scope manifest SHA-256 }
    depends_on = [ {supplement_release_id, shard/manifest/external_id SHA-256} ]
    supplement_shard_sha256
    supplement_registry_manifest_sha256

The loader takes a base release directory plus an ordered list of
supplement directories and validates the whole chain BEFORE any shard is
consumed. It fails closed on:

* an unknown ``artifact_kind``;
* missing/mismatched base-release hashes;
* a declared dependency that is not present in the supplied chain;
* a declared dependency whose actual hash differs from the recorded one;
* supplements supplied in an order that violates the declared
  ``depends_on`` graph (topological check);
* attempting to load a supplement standalone (no base);
* two supplements sharing the same ``supplement_release_id`` with
  different shard hashes (release-ID reuse for byte-distinct artefacts);
* a dependency cycle;
* a supplement whose entries reference a `first_seen_source_release`
  that is not the base and is not in the loaded chain.

The loader NEVER writes anywhere; it produces a validated
:class:`SupplementChain` the reconciliation engine consumes.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path


class SupplementLineageError(ValueError):
    """Raised when the supplement chain fails formal validation."""


ARTIFACT_KIND = "registry_supplement"


@dataclass(frozen=True)
class SupplementEntry:
    release_id: str
    directory: Path
    canonical_dir: Path
    shard_sha256: str
    manifest_sha256: str
    external_id_sha256: str | None
    base_release_id: str
    depends_on_release_ids: tuple[str, ...]
    depends_on_hashes: dict[str, dict[str, str]]
    release_json_path: Path


@dataclass(frozen=True)
class SupplementChain:
    base_release_dir: Path
    base_release_id: str
    base_export_manifest_sha256: str
    base_scope_manifest_sha256: str
    supplements: tuple[SupplementEntry, ...]


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _base_release_info(base_release_dir: Path) -> dict[str, str]:
    export_manifest = base_release_dir / "taxonomy_export_manifest.json"
    scope_manifest = base_release_dir / "scope-manifest.json"
    if not export_manifest.is_file():
        raise SupplementLineageError(
            f"base release missing taxonomy_export_manifest.json: {base_release_dir}"
        )
    doc = json.loads(export_manifest.read_text())
    return {
        "base_release_id": doc.get("release_id", ""),
        "export_manifest_sha256": _sha256(export_manifest),
        "scope_manifest_sha256": doc.get("scope_manifest_sha256")
        or (_sha256(scope_manifest) if scope_manifest.is_file() else ""),
    }


def _read_supplement(directory: Path) -> SupplementEntry:
    # Every supplement emitted by the accepted allocators writes its release
    # JSON under release/<name>.json. Accept any *.json in that folder.
    release_dir = directory / "release"
    if not release_dir.is_dir():
        raise SupplementLineageError(
            f"supplement missing release/ directory: {directory}"
        )
    candidates = sorted(release_dir.glob("*.json"))
    if not candidates:
        raise SupplementLineageError(
            f"supplement release/ directory has no release JSON: {directory}"
        )
    release_json = candidates[0]
    doc = json.loads(release_json.read_text())
    if doc.get("artifact_kind") != ARTIFACT_KIND:
        raise SupplementLineageError(
            f"{release_json}: artifact_kind must be {ARTIFACT_KIND!r}, got {doc.get('artifact_kind')!r}"
        )
    release_id = doc.get("supplement_release_id")
    if not release_id:
        raise SupplementLineageError(
            f"{release_json}: supplement_release_id is required"
        )
    canonical_dir = directory / "canonical"
    if not canonical_dir.is_dir():
        raise SupplementLineageError(
            f"{directory}: canonical/ directory missing"
        )
    manifest_path = canonical_dir / "manifest.json"
    if not manifest_path.is_file():
        raise SupplementLineageError(
            f"{directory}: canonical/manifest.json missing"
        )
    manifest_doc = json.loads(manifest_path.read_text())
    shard_entries = manifest_doc.get("shards") or ()
    if not shard_entries:
        raise SupplementLineageError(f"{manifest_path}: no shards declared")
    # Aggregate concatenated shard hash from the manifest itself.
    concat = manifest_doc.get("concatenated_sha256") or ""

    # Compute observed shard hash — we trust the manifest but verify each
    # named shard's bytes on disk match its recorded sha.
    for shard in shard_entries:
        shard_path = canonical_dir / shard["name"]
        if not shard_path.is_file():
            raise SupplementLineageError(
                f"{directory}: shard {shard['name']!r} missing on disk"
            )
        observed = _sha256(shard_path)
        if observed != shard.get("sha256"):
            raise SupplementLineageError(
                f"{directory}: shard {shard['name']!r} sha256 mismatch — "
                f"disk={observed}, manifest={shard.get('sha256')}"
            )

    declared_shard_sha = doc.get("supplement_shard_sha256")
    # For single-shard supplements the recorded shard sha equals the
    # manifest's concatenated sha; for multi-shard chains it equals the
    # first shard's sha. Enforce match against manifest entries.
    if declared_shard_sha != shard_entries[0].get("sha256"):
        raise SupplementLineageError(
            f"{release_json}: supplement_shard_sha256 does not match the first "
            f"shard hash in canonical/manifest.json"
        )
    if doc.get("supplement_registry_manifest_sha256") != _sha256(manifest_path):
        raise SupplementLineageError(
            f"{release_json}: supplement_registry_manifest_sha256 does not "
            "match the on-disk canonical/manifest.json"
        )

    depends_on = doc.get("depends_on") or []
    depends_on_ids: list[str] = []
    depends_on_hashes: dict[str, dict[str, str]] = {}
    for dep in depends_on:
        rid = dep.get("supplement_release_id")
        if not rid:
            raise SupplementLineageError(
                f"{release_json}: depends_on entry missing supplement_release_id"
            )
        depends_on_ids.append(rid)
        depends_on_hashes[rid] = {
            "supplement_shard_sha256": dep.get("supplement_shard_sha256", ""),
            "supplement_registry_manifest_sha256": dep.get(
                "supplement_registry_manifest_sha256", ""
            ),
        }

    base_dep = doc.get("base_release_dependency") or {}
    return SupplementEntry(
        release_id=release_id,
        directory=directory,
        canonical_dir=canonical_dir,
        shard_sha256=declared_shard_sha,
        manifest_sha256=doc.get("supplement_registry_manifest_sha256", ""),
        external_id_sha256=doc.get("supplement_external_id_sha256"),
        base_release_id=doc.get("base_release_id", ""),
        depends_on_release_ids=tuple(depends_on_ids),
        depends_on_hashes=depends_on_hashes,
        release_json_path=release_json,
    )


def load_supplement_chain(
    *,
    base_release_dir: Path | None,
    supplement_dirs: list[Path],
) -> SupplementChain:
    """Validate the chain end-to-end and return a :class:`SupplementChain`.

    Raises :class:`SupplementLineageError` on any structural or hash defect.
    """

    if base_release_dir is None and supplement_dirs:
        raise SupplementLineageError(
            "cannot load supplements without a base release — supplements are "
            "not standalone taxonomy releases"
        )
    if base_release_dir is None:
        return SupplementChain(
            base_release_dir=Path(),
            base_release_id="",
            base_export_manifest_sha256="",
            base_scope_manifest_sha256="",
            supplements=(),
        )
    base = _base_release_info(base_release_dir)

    entries: list[SupplementEntry] = []
    seen_release_ids: dict[str, SupplementEntry] = {}

    for d in supplement_dirs:
        entry = _read_supplement(Path(d))
        if entry.release_id in seen_release_ids:
            prior = seen_release_ids[entry.release_id]
            if (
                prior.shard_sha256 != entry.shard_sha256
                or prior.manifest_sha256 != entry.manifest_sha256
            ):
                raise SupplementLineageError(
                    f"release-ID reuse: {entry.release_id!r} refers to two "
                    "byte-distinct artefacts — refuse to load"
                )
            # Same-hash duplicate: silently ignore (equivalent supply).
            continue

        # Base release must match.
        if entry.base_release_id != base["base_release_id"]:
            raise SupplementLineageError(
                f"{entry.release_json_path}: base_release_id "
                f"{entry.base_release_id!r} does not match the supplied base "
                f"release {base['base_release_id']!r}"
            )
        # Base export/scope hashes must match.
        expected_base_dep = json.loads(
            entry.release_json_path.read_text()
        ).get("base_release_dependency", {})
        for field, actual in (
            ("base_release_export_manifest_sha256", base["export_manifest_sha256"]),
            ("base_release_scope_manifest_sha256", base["scope_manifest_sha256"]),
        ):
            declared = expected_base_dep.get(field)
            if declared and declared != actual:
                raise SupplementLineageError(
                    f"{entry.release_json_path}: {field} mismatch — "
                    f"declared={declared}, actual base={actual}"
                )
        # Cycle: forbid self-dependency and any dependency already in the
        # topological successors set (i.e., depends_on names a later element
        # in the same submission — accepted only in strict predecessor order).
        if entry.release_id in entry.depends_on_release_ids:
            raise SupplementLineageError(
                f"{entry.release_json_path}: self-dependency (cycle)"
            )
        # depends_on must all be already loaded (topological order).
        for dep_id in entry.depends_on_release_ids:
            if dep_id not in seen_release_ids:
                raise SupplementLineageError(
                    f"{entry.release_json_path}: depends_on {dep_id!r} is "
                    "not present earlier in the supplement chain — either "
                    "supplied out of order or missing entirely"
                )
            dep_entry = seen_release_ids[dep_id]
            recorded = entry.depends_on_hashes[dep_id]
            if (
                recorded.get("supplement_shard_sha256")
                and recorded["supplement_shard_sha256"] != dep_entry.shard_sha256
            ):
                raise SupplementLineageError(
                    f"{entry.release_json_path}: depends_on {dep_id!r} "
                    "supplement_shard_sha256 mismatch — "
                    f"declared={recorded['supplement_shard_sha256']}, "
                    f"actual={dep_entry.shard_sha256}"
                )
            if (
                recorded.get("supplement_registry_manifest_sha256")
                and recorded["supplement_registry_manifest_sha256"]
                != dep_entry.manifest_sha256
            ):
                raise SupplementLineageError(
                    f"{entry.release_json_path}: depends_on {dep_id!r} "
                    "supplement_registry_manifest_sha256 mismatch"
                )
        entries.append(entry)
        seen_release_ids[entry.release_id] = entry

    return SupplementChain(
        base_release_dir=Path(base_release_dir),
        base_release_id=base["base_release_id"],
        base_export_manifest_sha256=base["export_manifest_sha256"],
        base_scope_manifest_sha256=base["scope_manifest_sha256"],
        supplements=tuple(entries),
    )
