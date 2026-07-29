"""Stage 3B.2 taxonomy-v2 runtime: installer + activation resolver.

Behavior:

* Reads the bundled gzip artifact and JSON manifest from
  ``database/reference_data/generated/taxonomy_v2/``.
* Verifies the artifact's SHA-256 against the manifest BEFORE any extraction.
* Extracts to a temporary sibling of the install target in the app-data
  directory, verifies the resulting SQLite SHA-256, then atomically renames.
* Reuses an already-verified installed database on subsequent starts.
* Never overwrites an existing valid install; extraction failures leave the
  previous file intact so rollback is trivial (delete the activation flag).

Activation is intentionally developer-only at this stage. It is gated by
either (a) the ``taxonomy_v2_activation`` key in ``app_settings.json`` or
(b) the ``SPORELY_TAXONOMY_V2`` environment variable. If activation fails
for any reason, callers fall back to the currently bundled taxonomy DB and
the failure is logged.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from database.reference_data_paths import (
    TAXONOMY_V2_DIR,
    TAXONOMY_V2_GZ_PATH,
    TAXONOMY_V2_MANIFEST_PATH,
)


logger = logging.getLogger(__name__)

CHUNK_BYTES = 1 * 1024 * 1024
ACTIVATION_ENV_VAR = "SPORELY_TAXONOMY_V2"
ACTIVATION_SETTINGS_KEY = "taxonomy_v2_activation"


class TaxonomyV2InstallError(RuntimeError):
    """Raised on any verification / extraction / atomic-rename failure."""


@dataclass(frozen=True)
class TaxonomyV2Manifest:
    manifest_schema_version: int
    taxonomy_schema_version: int
    content_release_id: str
    state: str
    publication: str
    gz_artifact: str
    gz_sha256: str
    gz_bytes: int
    sqlite_sha256: str
    sqlite_bytes: int
    registry_concatenated_sha256: str
    compiler_manifest_sha256: str
    install_target_name: str

    @classmethod
    def load(cls, path: Path) -> "TaxonomyV2Manifest":
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise TaxonomyV2InstallError(
                f"cannot read taxonomy-v2 manifest at {path}: {exc}"
            ) from exc
        try:
            return cls(**{k: raw[k] for k in cls.__annotations__})
        except KeyError as exc:
            raise TaxonomyV2InstallError(
                f"taxonomy-v2 manifest missing required field {exc.args[0]!r}"
            ) from exc


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(CHUNK_BYTES)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def load_manifest(manifest_path: Path = TAXONOMY_V2_MANIFEST_PATH) -> TaxonomyV2Manifest:
    return TaxonomyV2Manifest.load(manifest_path)


def is_activation_enabled(app_data_dir: Path) -> bool:
    """Developer-only activation gate.

    Truthy environment variable ``SPORELY_TAXONOMY_V2`` overrides settings.
    Otherwise reads ``app_settings.json[taxonomy_v2_activation]``.
    """
    env_value = os.environ.get(ACTIVATION_ENV_VAR, "").strip().lower()
    if env_value in {"1", "true", "yes", "on"}:
        return True
    if env_value in {"0", "false", "no", "off"}:
        return False
    settings_path = app_data_dir / "app_settings.json"
    if not settings_path.exists():
        return False
    try:
        settings = json.loads(settings_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return bool(settings.get(ACTIVATION_SETTINGS_KEY))


def _install_target(app_data_dir: Path, manifest: TaxonomyV2Manifest) -> Path:
    return app_data_dir / "taxonomy_v2" / manifest.install_target_name


def _extract_gzip_verified(
    *,
    gz_path: Path,
    target_path: Path,
    expected_gz_sha256: str,
    expected_sqlite_sha256: str,
    expected_sqlite_bytes: int,
) -> None:
    """Extract atomically. Verify both compressed and decompressed SHA-256s."""
    if not gz_path.exists():
        raise TaxonomyV2InstallError(f"gzip artifact not found at {gz_path}")
    actual_gz_sha = _sha256_file(gz_path)
    if actual_gz_sha != expected_gz_sha256:
        raise TaxonomyV2InstallError(
            f"gzip artifact SHA-256 mismatch: expected "
            f"{expected_gz_sha256}, got {actual_gz_sha}"
        )
    target_path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_str = tempfile.mkstemp(
        prefix=f".{target_path.name}.", suffix=".tmp",
        dir=str(target_path.parent),
    )
    tmp_path = Path(tmp_str)
    committed = False
    try:
        digest = hashlib.sha256()
        total_bytes = 0
        with os.fdopen(fd, "wb") as raw_out, gzip.open(gz_path, "rb") as gz_in:
            while True:
                chunk = gz_in.read(CHUNK_BYTES)
                if not chunk:
                    break
                total_bytes += len(chunk)
                if total_bytes > expected_sqlite_bytes + CHUNK_BYTES:
                    raise TaxonomyV2InstallError(
                        f"gzip decompression exceeds declared size "
                        f"{expected_sqlite_bytes}"
                    )
                raw_out.write(chunk)
                digest.update(chunk)
        if total_bytes != expected_sqlite_bytes:
            raise TaxonomyV2InstallError(
                f"decompressed byte count mismatch: expected "
                f"{expected_sqlite_bytes}, got {total_bytes}"
            )
        actual_sqlite_sha = digest.hexdigest()
        if actual_sqlite_sha != expected_sqlite_sha256:
            raise TaxonomyV2InstallError(
                f"decompressed SQLite SHA-256 mismatch: expected "
                f"{expected_sqlite_sha256}, got {actual_sqlite_sha}"
            )
        os.replace(tmp_path, target_path)
        committed = True
    finally:
        if not committed:
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass


def ensure_installed(
    *,
    app_data_dir: Path,
    manifest: TaxonomyV2Manifest | None = None,
    gz_path: Path = TAXONOMY_V2_GZ_PATH,
) -> Path:
    """Return the path to a verified, installed taxonomy-v2 SQLite.

    Reuses an existing valid install (SHA-256 matches manifest). Otherwise
    extracts from ``gz_path`` atomically. Every error path leaves any
    previously-valid install intact so a rollback is just clearing the
    activation flag.
    """
    manifest = manifest or load_manifest()
    if manifest.taxonomy_schema_version != 2:
        raise TaxonomyV2InstallError(
            f"unsupported taxonomy_schema_version: "
            f"{manifest.taxonomy_schema_version}"
        )
    target = _install_target(app_data_dir, manifest)
    if target.exists():
        actual = _sha256_file(target)
        if actual == manifest.sqlite_sha256:
            return target
        logger.warning(
            "existing taxonomy-v2 install at %s has SHA-256 %s, expected %s; "
            "re-extracting", target, actual, manifest.sqlite_sha256,
        )
        # Move the mismatched file aside so a partial run doesn't obliterate
        # an operator's diagnostic copy.
        bad_path = target.with_suffix(target.suffix + ".mismatched")
        try:
            os.replace(target, bad_path)
        except OSError:
            logger.exception("could not move mismatched install aside")
    _extract_gzip_verified(
        gz_path=gz_path,
        target_path=target,
        expected_gz_sha256=manifest.gz_sha256,
        expected_sqlite_sha256=manifest.sqlite_sha256,
        expected_sqlite_bytes=manifest.sqlite_bytes,
    )
    return target


def resolve_active_taxonomy_v2_path(app_data_dir: Path) -> Path | None:
    """Public runtime hook: return the v2 DB path when activation is on, else
    ``None``. Any error inside install/verification is logged and returns
    ``None`` so callers fall back to the current bundled taxonomy DB."""
    if not is_activation_enabled(app_data_dir):
        return None
    try:
        return ensure_installed(app_data_dir=app_data_dir)
    except TaxonomyV2InstallError:
        logger.exception("taxonomy-v2 activation failed; falling back")
        return None
    except Exception:  # pragma: no cover — defensive
        logger.exception("unexpected error activating taxonomy-v2")
        return None


def open_taxonomy_v2_readonly(db_path: Path):
    """Open the extracted taxonomy-v2 SQLite in read-only mode with the
    schema-version and release-id guards the compatibility contract
    requires."""
    import sqlite3
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        meta = dict(conn.execute("SELECT key, value FROM taxonomy_meta"))
    except sqlite3.DatabaseError as exc:
        conn.close()
        raise TaxonomyV2InstallError(
            f"taxonomy-v2 SQLite missing taxonomy_meta table: {exc}"
        ) from exc
    if meta.get("taxonomy_schema_version") != "2":
        conn.close()
        raise TaxonomyV2InstallError(
            f"taxonomy_schema_version must be 2, got "
            f"{meta.get('taxonomy_schema_version')!r}"
        )
    if meta.get("state") != "candidate":
        conn.close()
        raise TaxonomyV2InstallError(
            f"unexpected taxonomy state: {meta.get('state')!r}"
        )
    return conn, meta
