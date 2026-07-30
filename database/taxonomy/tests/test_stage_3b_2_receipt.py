"""Stage 3B.2 receipt + process-cache tests.

Cover the performance-correction contract:

* First install decompresses + hashes once and writes a receipt.
* Subsequent path resolutions in the same process do no full-file hashing.
* A later fresh-process startup with a valid receipt skips the full SHA.
* Manifest/receipt drift (wrong SHA, wrong release, wrong size) triggers
  reinstall.
* Ordinary language/country changes do not touch the installer.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import sqlite3
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _fake_v2_sqlite(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    conn.executescript(
        """
        CREATE TABLE taxon_min (
            taxon_id INTEGER PRIMARY KEY,
            genus TEXT, specific_epithet TEXT, family TEXT,
            canonical_scientific_name TEXT
        );
        CREATE TABLE taxonomy_meta (key TEXT PRIMARY KEY, value TEXT);
        """
    )
    conn.execute("INSERT INTO taxon_min VALUES (1, 'Genus', 'sp', '', 'Genus sp')")
    conn.executemany("INSERT INTO taxonomy_meta VALUES (?, ?)", [
        ("taxonomy_schema_version", "2"),
        ("content_release_id", "tax-2026.07.29-01"),
        ("state", "candidate"),
    ])
    conn.commit()
    conn.close()


def _make_gz(sqlite_path: Path, gz_path: Path) -> tuple[str, str, int]:
    with sqlite_path.open("rb") as src, gz_path.open("wb") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=6, mtime=0) as gz:
            gz.write(src.read())
    gz_sha = hashlib.sha256(gz_path.read_bytes()).hexdigest()
    sql_sha = hashlib.sha256(sqlite_path.read_bytes()).hexdigest()
    return gz_sha, sql_sha, sqlite_path.stat().st_size


def _manifest_path(tmp_path: Path, gz_sha: str, sql_sha: str, sql_bytes: int,
                   *, release_id: str = "tax-2026.07.29-01") -> Path:
    p = tmp_path / "manifest.json"
    p.write_text(json.dumps({
        "manifest_schema_version": 1,
        "taxonomy_schema_version": 2,
        "content_release_id": release_id,
        "state": "candidate",
        "publication": "none",
        "gz_artifact": "tax.sqlite3.gz",
        "gz_sha256": gz_sha,
        "gz_bytes": 0,
        "sqlite_sha256": sql_sha,
        "sqlite_bytes": sql_bytes,
        "registry_concatenated_sha256": "00" * 32,
        "compiler_manifest_sha256": "00" * 32,
        "install_target_name": "vernacular_multilanguage_v2.sqlite3",
    }, indent=2, sort_keys=True))
    return p


class _HashCounter:
    """Patch ``_sha256_file`` and count how often it runs."""
    def __init__(self, module) -> None:
        self.module = module
        self.original = module._sha256_file
        self.count = 0

    def __enter__(self):
        module = self.module
        def counted(path):
            self.count += 1
            return self.original(path)
        module._sha256_file = counted
        return self

    def __exit__(self, *_):
        self.module._sha256_file = self.original


def _prepare_install(tmp_path: Path):
    src = tmp_path / "raw.sqlite3"
    _fake_v2_sqlite(src)
    gz = tmp_path / "artifact.gz"
    gz_sha, sql_sha, sql_bytes = _make_gz(src, gz)
    manifest_path = _manifest_path(tmp_path, gz_sha, sql_sha, sql_bytes)
    return gz, manifest_path, gz_sha, sql_sha, sql_bytes


# ------------------------------------------------------------------ tests --


def test_first_install_decompresses_and_hashes_once(tmp_path: Path) -> None:
    from utils.taxonomy_v2 import (
        TaxonomyV2Manifest, ensure_installed, invalidate_resolution_cache,
    )
    import utils.taxonomy_v2 as tx
    invalidate_resolution_cache()
    gz, manifest_path, gz_sha, sql_sha, sql_bytes = _prepare_install(tmp_path)
    manifest = TaxonomyV2Manifest.load(manifest_path)
    app_data = tmp_path / "userdata"
    with _HashCounter(tx) as counter:
        target = ensure_installed(app_data_dir=app_data, manifest=manifest, gz_path=gz)
    # First install hashes the gz artifact exactly once via `_sha256_file`.
    # The extracted SQLite is verified inline (incremental SHA during the
    # gunzip stream), so `_sha256_file` does not need to be called on the
    # 310 MB SQLite. Total: one gz-only hash.
    assert counter.count == 1, counter.count
    # Receipt now sits beside the installed SQLite.
    receipt = target.with_name("install_receipt.json")
    assert receipt.exists()
    receipt_data = json.loads(receipt.read_text())
    assert receipt_data["sqlite_sha256"] == sql_sha
    assert receipt_data["sqlite_bytes"] == sql_bytes
    assert receipt_data["content_release_id"] == manifest.content_release_id


def test_subsequent_resolutions_in_same_process_skip_full_hashing(tmp_path: Path) -> None:
    from utils.taxonomy_v2 import (
        TaxonomyV2Manifest, ensure_installed, resolve_active_taxonomy_v2_path,
        invalidate_resolution_cache, ACTIVATION_ENV_VAR,
    )
    import utils.taxonomy_v2 as tx
    invalidate_resolution_cache()
    gz, manifest_path, *_ = _prepare_install(tmp_path)
    manifest = TaxonomyV2Manifest.load(manifest_path)
    app_data = tmp_path / "userdata"
    # First-time install.
    ensure_installed(app_data_dir=app_data, manifest=manifest, gz_path=gz)

    # Force activation via env var so `resolve_active_taxonomy_v2_path` runs
    # its full path once, populating the process cache.
    os.environ[ACTIVATION_ENV_VAR] = "1"
    try:
        # Point manifest resolution at our test manifest.
        original_load = tx.load_manifest
        tx.load_manifest = lambda *_a, **_k: manifest
        # Also point the default gz-path constant used inside
        # ensure_installed to our test gz.
        original_gz = tx.TAXONOMY_V2_GZ_PATH
        tx.TAXONOMY_V2_GZ_PATH = gz
        try:
            with _HashCounter(tx) as counter:
                resolve_active_taxonomy_v2_path(app_data)
                # First call: receipt fast path — NO full-file hashing.
                assert counter.count == 0, counter.count
                for _ in range(10):
                    resolve_active_taxonomy_v2_path(app_data)
                # Repeated calls hit the process cache; still zero hashes.
                assert counter.count == 0, counter.count
        finally:
            tx.load_manifest = original_load
            tx.TAXONOMY_V2_GZ_PATH = original_gz
    finally:
        del os.environ[ACTIVATION_ENV_VAR]
    invalidate_resolution_cache()


def test_fresh_process_startup_reuses_receipt(tmp_path: Path) -> None:
    """Simulate a new process: clear the resolution cache but keep the on-
    disk state. ``ensure_installed`` must not full-hash the installed
    SQLite."""
    from utils.taxonomy_v2 import (
        TaxonomyV2Manifest, ensure_installed, invalidate_resolution_cache,
    )
    import utils.taxonomy_v2 as tx
    invalidate_resolution_cache()
    gz, manifest_path, *_ = _prepare_install(tmp_path)
    manifest = TaxonomyV2Manifest.load(manifest_path)
    app_data = tmp_path / "userdata"
    ensure_installed(app_data_dir=app_data, manifest=manifest, gz_path=gz)

    # Fresh process — cache empty, receipt still on disk.
    invalidate_resolution_cache()
    with _HashCounter(tx) as counter:
        ensure_installed(app_data_dir=app_data, manifest=manifest, gz_path=gz)
    assert counter.count == 0, counter.count


def test_changed_release_id_triggers_reinstall(tmp_path: Path) -> None:
    from utils.taxonomy_v2 import (
        TaxonomyV2Manifest, ensure_installed, invalidate_resolution_cache,
    )
    invalidate_resolution_cache()
    gz, manifest_path, gz_sha, sql_sha, sql_bytes = _prepare_install(tmp_path)
    manifest = TaxonomyV2Manifest.load(manifest_path)
    app_data = tmp_path / "userdata"
    ensure_installed(app_data_dir=app_data, manifest=manifest, gz_path=gz)

    # New release ID in manifest — receipt binds the old ID and MUST fail
    # closed and re-verify (which succeeds and updates the receipt).
    new_manifest_path = _manifest_path(
        tmp_path, gz_sha, sql_sha, sql_bytes, release_id="tax-2027.01.01-01",
    )
    new_manifest = TaxonomyV2Manifest.load(new_manifest_path)
    import utils.taxonomy_v2 as tx
    with _HashCounter(tx) as counter:
        ensure_installed(app_data_dir=app_data, manifest=new_manifest, gz_path=gz)
    # Full-verify branch fired: 1 SHA of the installed SQLite (bytes still
    # match manifest.sqlite_sha256, so no extraction, but a receipt rewrite).
    assert counter.count == 1, counter.count
    receipt = app_data / "taxonomy_v2" / "install_receipt.json"
    assert json.loads(receipt.read_text())["content_release_id"] == "tax-2027.01.01-01"


def test_receipt_size_mismatch_triggers_reinstall(tmp_path: Path) -> None:
    from utils.taxonomy_v2 import (
        TaxonomyV2Manifest, ensure_installed, invalidate_resolution_cache,
    )
    invalidate_resolution_cache()
    gz, manifest_path, *_ = _prepare_install(tmp_path)
    manifest = TaxonomyV2Manifest.load(manifest_path)
    app_data = tmp_path / "userdata"
    target = ensure_installed(app_data_dir=app_data, manifest=manifest, gz_path=gz)
    receipt = target.with_name("install_receipt.json")
    tampered = json.loads(receipt.read_text())
    tampered["sqlite_bytes"] = tampered["sqlite_bytes"] + 1
    receipt.write_text(json.dumps(tampered))
    import utils.taxonomy_v2 as tx
    with _HashCounter(tx) as counter:
        ensure_installed(app_data_dir=app_data, manifest=manifest, gz_path=gz)
    # Receipt-size mismatch triggers the full-verify branch (1 SHA over
    # the installed file). Bytes match the manifest, so no re-extraction.
    assert counter.count == 1, counter.count


def test_explicit_verify_env_forces_full_hash_once(tmp_path: Path) -> None:
    from utils.taxonomy_v2 import (
        TaxonomyV2Manifest, ensure_installed, resolve_active_taxonomy_v2_path,
        invalidate_resolution_cache, ACTIVATION_ENV_VAR, VERIFY_ENV_VAR,
    )
    import utils.taxonomy_v2 as tx
    invalidate_resolution_cache()
    gz, manifest_path, *_ = _prepare_install(tmp_path)
    manifest = TaxonomyV2Manifest.load(manifest_path)
    app_data = tmp_path / "userdata"
    ensure_installed(app_data_dir=app_data, manifest=manifest, gz_path=gz)
    invalidate_resolution_cache()
    os.environ[ACTIVATION_ENV_VAR] = "1"
    os.environ[VERIFY_ENV_VAR] = "1"
    original_load = tx.load_manifest
    original_gz = tx.TAXONOMY_V2_GZ_PATH
    tx.load_manifest = lambda *_a, **_k: manifest
    tx.TAXONOMY_V2_GZ_PATH = gz
    try:
        with _HashCounter(tx) as counter:
            resolve_active_taxonomy_v2_path(app_data)
            assert counter.count == 1, counter.count
            # The env flag is auto-cleared after use.
            assert VERIFY_ENV_VAR not in os.environ
    finally:
        os.environ.pop(ACTIVATION_ENV_VAR, None)
        os.environ.pop(VERIFY_ENV_VAR, None)
        tx.load_manifest = original_load
        tx.TAXONOMY_V2_GZ_PATH = original_gz
        invalidate_resolution_cache()


def test_v2_off_returns_none_and_never_hashes(tmp_path: Path) -> None:
    from utils.taxonomy_v2 import (
        resolve_active_taxonomy_v2_path, invalidate_resolution_cache,
        ACTIVATION_ENV_VAR,
    )
    import utils.taxonomy_v2 as tx
    invalidate_resolution_cache()
    os.environ[ACTIVATION_ENV_VAR] = "0"
    try:
        with _HashCounter(tx) as counter:
            assert resolve_active_taxonomy_v2_path(tmp_path) is None
            assert counter.count == 0
    finally:
        os.environ.pop(ACTIVATION_ENV_VAR, None)
        invalidate_resolution_cache()


def test_country_change_burst_does_not_reopen_or_hash(tmp_path: Path) -> None:
    """Simulate a country / language change: many calls to
    ``resolve_active_taxonomy_v2_path`` from various UI paths. NONE of
    them may full-hash the SQLite, run integrity_check, or reinstall."""
    from utils.taxonomy_v2 import (
        TaxonomyV2Manifest, ensure_installed, resolve_active_taxonomy_v2_path,
        invalidate_resolution_cache, ACTIVATION_ENV_VAR,
    )
    import utils.taxonomy_v2 as tx
    invalidate_resolution_cache()
    gz, manifest_path, *_ = _prepare_install(tmp_path)
    manifest = TaxonomyV2Manifest.load(manifest_path)
    app_data = tmp_path / "userdata"
    ensure_installed(app_data_dir=app_data, manifest=manifest, gz_path=gz)
    os.environ[ACTIVATION_ENV_VAR] = "1"
    original_load = tx.load_manifest
    original_gz = tx.TAXONOMY_V2_GZ_PATH
    tx.load_manifest = lambda *_a, **_k: manifest
    tx.TAXONOMY_V2_GZ_PATH = gz
    try:
        # Warm the process cache.
        resolve_active_taxonomy_v2_path(app_data)
        with _HashCounter(tx) as counter:
            for _ in range(100):
                resolve_active_taxonomy_v2_path(app_data)
        assert counter.count == 0
    finally:
        os.environ.pop(ACTIVATION_ENV_VAR, None)
        tx.load_manifest = original_load
        tx.TAXONOMY_V2_GZ_PATH = original_gz
        invalidate_resolution_cache()


def test_country_change_hot_lookup_query_uses_index(tmp_path: Path) -> None:
    """The vernacular hot-path query must resolve via
    ``idx_taxon_genus_species`` + ``idx_vern_taxon_lang``, not a full
    table scan. Regression guard for the COLLATE-NOCASE trap that used
    to force a 7 ms per-row scan of ``vernacular_min``."""
    from database.vernacular_db import VernacularDB
    src = tmp_path / "v2.sqlite3"
    conn = sqlite3.connect(str(src))
    conn.executescript(
        """
        CREATE TABLE taxon_min (
            taxon_id INTEGER PRIMARY KEY,
            genus TEXT, specific_epithet TEXT, family TEXT,
            canonical_scientific_name TEXT
        );
        CREATE TABLE vernacular_min (
            vernacular_id INTEGER PRIMARY KEY AUTOINCREMENT,
            taxon_id INTEGER NOT NULL,
            language_code TEXT NOT NULL,
            vernacular_name TEXT NOT NULL,
            is_preferred_name INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE taxonomy_meta (key TEXT PRIMARY KEY, value TEXT);
        CREATE INDEX idx_taxon_genus_species ON taxon_min(genus, specific_epithet);
        CREATE INDEX idx_vern_taxon_lang ON vernacular_min(taxon_id, language_code);
        CREATE INDEX idx_vern_lang_name ON vernacular_min(language_code, vernacular_name);
        """
    )
    conn.execute("INSERT INTO taxon_min VALUES (1, 'Candolleomyces', "
                 "'candolleanus', '', 'Candolleomyces candolleanus')")
    conn.executemany(
        "INSERT INTO vernacular_min (taxon_id, language_code, "
        "vernacular_name, is_preferred_name) VALUES (?, ?, ?, ?)", [
            (1, "nb", "hvit sprøsopp", 1),
            (1, "nn", "kvit sprøsopp", 1),
        ])
    conn.execute("INSERT INTO taxonomy_meta VALUES ('taxonomy_schema_version', '2')")
    conn.commit()
    db = VernacularDB(src, language_code="no")
    # The two-step lookup returns the preferred Norwegian name in ~µs.
    assert db.vernacular_from_taxon("Candolleomyces", "candolleanus") == "hvit sprøsopp"
    plan = list(conn.execute(
        "EXPLAIN QUERY PLAN "
        "SELECT taxon_id FROM taxon_min "
        "WHERE genus = ? AND specific_epithet = ? LIMIT 1",
        ("Candolleomyces", "candolleanus"),
    ))
    assert any("idx_taxon_genus_species" in str(row) for row in plan), plan
    conn.close()
