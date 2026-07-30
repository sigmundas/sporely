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
    TAXONOMY_V2_MANIFEST_PATH,
)


logger = logging.getLogger(__name__)

CHUNK_BYTES = 1 * 1024 * 1024
ACTIVATION_ENV_VAR = "SPORELY_TAXONOMY_V2"
ACTIVATION_SETTINGS_KEY = "taxonomy_v2_activation"
# Explicit repair/re-verify request from the operator. When truthy, a
# single subsequent path resolution will full-hash the installed SQLite
# even if the receipt is valid. Cleared automatically after the check
# runs so subsequent resolutions revert to the cheap fast path.
VERIFY_ENV_VAR = "SPORELY_TAXONOMY_V2_VERIFY"
RECEIPT_SCHEMA_VERSION = 1
INSTALL_RECEIPT_FILENAME = "install_receipt.json"


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


_ALLOWED_GZ_SUFFIX = ".sqlite3.gz"


def _safe_manifest_artifact_name(name: str) -> str:
    """Validate a manifest ``gz_artifact`` value and return it verbatim.

    The manifest declares only a bare filename that lives beside the
    manifest itself. Anything else is rejected up-front so an attacker or
    a broken build pipeline cannot direct the installer at an arbitrary
    filesystem location.

    Rejects (all raise :class:`TaxonomyV2InstallError`):

    * empty / non-string values
    * absolute paths (POSIX ``/foo`` or Windows drive letters like ``C:``)
    * any directory separator (``/`` or ``\\``)
    * a ``..`` path component
    * any suffix other than ``.sqlite3.gz`` (the canonical release format)
    """
    if not isinstance(name, str) or not name:
        raise TaxonomyV2InstallError(
            f"invalid gzip artifact name in manifest: {name!r}"
        )
    # Reject POSIX-absolute and any embedded separators outright.
    if name.startswith("/") or "/" in name or "\\" in name:
        raise TaxonomyV2InstallError(
            f"invalid gzip artifact name in manifest: {name!r}"
        )
    # Reject a Windows drive-letter absolute path (``C:foo`` / ``C:\\foo``).
    if len(name) >= 2 and name[1] == ":" and name[0].isalpha():
        raise TaxonomyV2InstallError(
            f"invalid gzip artifact name in manifest: {name!r}"
        )
    # Belt-and-braces: PurePath-based checks catch anything the string
    # inspection above missed on the current platform.
    candidate = Path(name)
    if candidate.is_absolute() or candidate.name != name:
        raise TaxonomyV2InstallError(
            f"invalid gzip artifact name in manifest: {name!r}"
        )
    if ".." in candidate.parts or name == "..":
        raise TaxonomyV2InstallError(
            f"invalid gzip artifact name in manifest: {name!r}"
        )
    if not name.endswith(_ALLOWED_GZ_SUFFIX) or name == _ALLOWED_GZ_SUFFIX:
        raise TaxonomyV2InstallError(
            f"invalid gzip artifact suffix in manifest: {name!r} "
            f"(expected *{_ALLOWED_GZ_SUFFIX})"
        )
    return name


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


def _receipt_path(target_path: Path) -> Path:
    """Location of the small install-receipt JSON beside the installed
    SQLite. Deriving the path from the DB (not the manifest) means the
    receipt survives even if the app-data layout changes."""
    return target_path.with_name(INSTALL_RECEIPT_FILENAME)


def _write_install_receipt(
    *, receipt_path: Path, manifest: TaxonomyV2Manifest,
    verified_sha256: str, installed_bytes: int,
) -> None:
    """Persist the receipt atomically. Only called after a successful
    full-hash verification of the just-installed SQLite."""
    receipt = {
        "receipt_schema_version": RECEIPT_SCHEMA_VERSION,
        "content_release_id": manifest.content_release_id,
        "sqlite_sha256": verified_sha256,
        "sqlite_bytes": installed_bytes,
        "manifest_gz_sha256": manifest.gz_sha256,
        "manifest_sqlite_sha256": manifest.sqlite_sha256,
        "taxonomy_schema_version": manifest.taxonomy_schema_version,
    }
    tmp = receipt_path.with_suffix(receipt_path.suffix + ".tmp")
    tmp.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n",
                   encoding="utf-8")
    os.replace(tmp, receipt_path)


def _load_install_receipt(receipt_path: Path) -> dict | None:
    if not receipt_path.exists():
        return None
    try:
        raw = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    return raw


def _receipt_matches(receipt: dict, manifest: TaxonomyV2Manifest,
                     installed_path: Path) -> bool:
    """Cheap check — no full-file SHA. Verifies:

    * receipt schema version;
    * receipt sqlite_sha256 == manifest sqlite_sha256 (so a manifest change
      invalidates the receipt);
    * receipt content_release_id == manifest content_release_id;
    * receipt taxonomy_schema_version == manifest taxonomy_schema_version;
    * installed_path exists and its ``stat().st_size`` matches the receipt.
    """
    try:
        if int(receipt.get("receipt_schema_version") or 0) != RECEIPT_SCHEMA_VERSION:
            return False
        if str(receipt.get("sqlite_sha256")) != manifest.sqlite_sha256:
            return False
        if str(receipt.get("content_release_id")) != manifest.content_release_id:
            return False
        if int(receipt.get("taxonomy_schema_version") or 0) != \
                manifest.taxonomy_schema_version:
            return False
        expected_bytes = int(receipt.get("sqlite_bytes") or -1)
        if expected_bytes != manifest.sqlite_bytes:
            return False
        actual_bytes = installed_path.stat().st_size
        if actual_bytes != expected_bytes:
            return False
    except (OSError, TypeError, ValueError):
        return False
    return True


def _cheap_meta_probe(db_path: Path, manifest: TaxonomyV2Manifest) -> bool:
    """Read-only, indexed lookup of two ``taxonomy_meta`` rows. Fast
    (sub-millisecond on real hardware) and confirms the database opens and
    matches the manifest's schema version + release ID."""
    import sqlite3
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    except sqlite3.DatabaseError:
        return False
    try:
        rows = dict(conn.execute(
            "SELECT key, value FROM taxonomy_meta "
            "WHERE key IN ('taxonomy_schema_version','content_release_id')"))
    except sqlite3.DatabaseError:
        return False
    finally:
        conn.close()
    if rows.get("taxonomy_schema_version") != str(manifest.taxonomy_schema_version):
        return False
    if rows.get("content_release_id") != manifest.content_release_id:
        return False
    return True


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
    gz_path: Path | None = None,
    force_verify: bool = False,
) -> Path:
    """Return the path to a verified, installed taxonomy-v2 SQLite.

    Fast path (no full-file SHA): a valid receipt binds
    ``(content_release_id, sqlite_sha256, sqlite_bytes)`` and the installed
    SQLite's ``stat().st_size`` matches. A cheap ``taxonomy_meta`` probe
    confirms the database still opens.

    Full-verify path (SHA-256 the whole file): triggered only when the
    receipt is missing/inconsistent, the size differs, the manifest changed,
    or ``force_verify`` is set (via ``SPORELY_TAXONOMY_V2_VERIFY=1``).

    Reinstall path (gunzip + full SHA): triggered when the target file is
    absent, or when the full-verify path finds the installed bytes no
    longer match the manifest.
    """
    manifest = manifest or load_manifest()

    if gz_path is None:
        # Derive the artifact path from the manifest so a new release
        # never requires a runtime constant update. TAXONOMY_V2_DIR is
        # read at call time (not at import time) so tests can monkeypatch
        # it to redirect the installer at a temp directory.
        artifact_name = _safe_manifest_artifact_name(manifest.gz_artifact)
        gz_path = TAXONOMY_V2_DIR / artifact_name

    if manifest.taxonomy_schema_version != 2:
        raise TaxonomyV2InstallError(
            f"unsupported taxonomy_schema_version: "
            f"{manifest.taxonomy_schema_version}"
        )
    target = _install_target(app_data_dir, manifest)
    receipt_path = _receipt_path(target)

    if target.exists() and not force_verify:
        receipt = _load_install_receipt(receipt_path)
        if receipt and _receipt_matches(receipt, manifest, target) and \
                _cheap_meta_probe(target, manifest):
            return target

    if target.exists():
        actual = _sha256_file(target)
        if actual == manifest.sqlite_sha256:
            # Receipt was missing/stale but the bytes are still correct —
            # rewrite the receipt so subsequent starts take the fast path.
            _write_install_receipt(
                receipt_path=receipt_path, manifest=manifest,
                verified_sha256=actual, installed_bytes=target.stat().st_size,
            )
            return target
        logger.warning(
            "existing taxonomy-v2 install at %s has SHA-256 %s, expected %s; "
            "re-extracting", target, actual, manifest.sqlite_sha256,
        )
        bad_path = target.with_suffix(target.suffix + ".mismatched")
        try:
            os.replace(target, bad_path)
        except OSError:
            logger.exception("could not move mismatched install aside")
        # Remove the now-stale receipt so it can't survive as a lie.
        try:
            receipt_path.unlink()
        except FileNotFoundError:
            pass
    _extract_gzip_verified(
        gz_path=gz_path,
        target_path=target,
        expected_gz_sha256=manifest.gz_sha256,
        expected_sqlite_sha256=manifest.sqlite_sha256,
        expected_sqlite_bytes=manifest.sqlite_bytes,
    )
    _write_install_receipt(
        receipt_path=receipt_path, manifest=manifest,
        verified_sha256=manifest.sqlite_sha256,
        installed_bytes=target.stat().st_size,
    )
    return target


# Process-global path cache. Populated on the first successful resolution;
# reused verbatim for every subsequent call in the same process. Country /
# language changes MUST NOT invalidate this — the installed database is
# language-neutral. Explicit reinstall / repair operations clear it via
# `invalidate_resolution_cache`.
_RESOLUTION_CACHE: dict[str, Path | None] = {}


def invalidate_resolution_cache() -> None:
    """Clear the process-global cache. Only reinstall / repair paths need
    this; ordinary language / country changes must not call it."""
    _RESOLUTION_CACHE.clear()


def resolve_active_taxonomy_v2_path(
    app_data_dir: Path,
    *,
    force_reresolve: bool = False,
) -> Path | None:
    """Public runtime hook: return the v2 DB path when activation is on,
    else ``None``.

    On the FAST path (default): return a cached resolution when one exists
    for this app-data directory. Ordinary UI code paths (country change,
    lookups, etc.) hit only this branch and pay near-zero cost.

    On the SLOW path (first call, or ``force_reresolve``): re-check
    activation, run ``ensure_installed`` — which itself uses the receipt
    to avoid full-file hashing when the install is still valid — and cache
    the result. Setting ``SPORELY_TAXONOMY_V2_VERIFY=1`` forces the
    installer's full-verify branch on this call.
    """
    key = str(app_data_dir)
    if not force_reresolve and key in _RESOLUTION_CACHE:
        return _RESOLUTION_CACHE[key]

    if not is_activation_enabled(app_data_dir):
        _RESOLUTION_CACHE[key] = None
        return None
    verify_flag = os.environ.get(VERIFY_ENV_VAR, "").strip().lower() in {
        "1", "true", "yes", "on"
    }
    try:
        path = ensure_installed(
            app_data_dir=app_data_dir, force_verify=verify_flag,
        )
    except TaxonomyV2InstallError:
        logger.exception("taxonomy-v2 activation failed; falling back")
        _RESOLUTION_CACHE[key] = None
        return None
    except Exception:  # pragma: no cover — defensive
        logger.exception("unexpected error activating taxonomy-v2")
        _RESOLUTION_CACHE[key] = None
        return None
    # Clear the verify flag so subsequent resolutions in the same process
    # revert to the receipt-fast-path even if the env var is still set —
    # the operator only asked for ONE explicit re-verification.
    if verify_flag:
        try:
            del os.environ[VERIFY_ENV_VAR]
        except KeyError:
            pass
    _RESOLUTION_CACHE[key] = path
    return path


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
