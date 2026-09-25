"""The tracked desktop taxonomy-v2 bundle that 0.9.23 ships.

Pins the promoted release so a stale or half-updated bundle (manifest without
its gzip, gzip not allowed by .gitignore, compatibility pins out of step)
fails here rather than in a user's first launch.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

from database.reference_data_paths import TAXONOMY_V2_DIR, TAXONOMY_V2_MANIFEST_PATH
from utils import taxonomy_v2

ROOT = Path(__file__).resolve().parents[1]
SHIPPED_RELEASE = "tax-2026.09.23-01"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def test_bundled_manifest_loads_and_names_the_shipped_release():
    manifest = taxonomy_v2.load_manifest(TAXONOMY_V2_MANIFEST_PATH)
    assert manifest.content_release_id == SHIPPED_RELEASE
    assert manifest.taxonomy_schema_version == 2
    assert manifest.gz_artifact == f"{SHIPPED_RELEASE}.sqlite3.gz"
    assert manifest.install_target_name == "vernacular_multilanguage_v2.sqlite3"


def test_bundled_gzip_is_present_and_matches_the_manifest():
    manifest = taxonomy_v2.load_manifest(TAXONOMY_V2_MANIFEST_PATH)
    gz = TAXONOMY_V2_DIR / manifest.gz_artifact
    assert gz.stat().st_size == manifest.gz_bytes
    assert _sha256(gz) == manifest.gz_sha256
    # Exactly one bundle gzip: a leftover release would ship twice.
    assert sorted(p.name for p in TAXONOMY_V2_DIR.glob("*.sqlite3.gz")) == [manifest.gz_artifact]


def test_bundled_gzip_is_tracked_not_ignored():
    """The release workflow bundles the CI checkout, which has only tracked files."""
    manifest = taxonomy_v2.load_manifest(TAXONOMY_V2_MANIFEST_PATH)
    rel = (TAXONOMY_V2_DIR / manifest.gz_artifact).relative_to(ROOT).as_posix()
    ignored = subprocess.run(["git", "check-ignore", "-q", rel], cwd=ROOT)
    assert ignored.returncode == 1, f"{rel} is ignored by .gitignore"


def test_compatibility_pins_follow_the_bundle():
    manifest = json.loads(TAXONOMY_V2_MANIFEST_PATH.read_text(encoding="utf-8"))
    compat = json.loads((ROOT / "database/taxonomy/desktop-compatibility.json").read_text(encoding="utf-8"))
    assert compat["tested_taxonomy_release"] == manifest["content_release_id"]
    assert compat["bundled_sqlite_sha256"] == manifest["sqlite_sha256"]
    assert compat["bundled_gz_sha256"] == manifest["gz_sha256"]
    assert compat["registry_concatenated_sha256"] == manifest["registry_concatenated_sha256"]
    assert (ROOT / "database/taxonomy/registry/canonical/manifest.json").exists()
    registry = json.loads((ROOT / "database/taxonomy/registry/canonical/manifest.json").read_text(encoding="utf-8"))
    assert registry["concatenated_sha256"] == manifest["registry_concatenated_sha256"]


def test_a_fresh_default_profile_runs_taxonomy_v2_from_the_bundle(tmp_path, monkeypatch):
    """0.9.23 stock behavior: no env var, no setting → the bundled release is
    installed, verified and active."""
    monkeypatch.delenv(taxonomy_v2.ACTIVATION_ENV_VAR, raising=False)
    monkeypatch.delenv(taxonomy_v2.VERIFY_ENV_VAR, raising=False)
    assert taxonomy_v2.is_activation_enabled(tmp_path) is True
    path = taxonomy_v2.resolve_active_taxonomy_v2_path(tmp_path, force_reresolve=True)
    assert path is not None and path.parent == tmp_path / "taxonomy_v2"
    conn, meta = taxonomy_v2.open_taxonomy_v2_readonly(path)
    conn.close()
    assert meta["content_release_id"] == SHIPPED_RELEASE
    receipt = json.loads((path.parent / taxonomy_v2.INSTALL_RECEIPT_FILENAME).read_text(encoding="utf-8"))
    assert receipt["content_release_id"] == SHIPPED_RELEASE


def test_an_explicit_off_setting_keeps_the_legacy_path(tmp_path, monkeypatch):
    monkeypatch.delenv(taxonomy_v2.ACTIVATION_ENV_VAR, raising=False)
    (tmp_path / "app_settings.json").write_text(json.dumps({"taxonomy_v2_activation": False}))
    assert taxonomy_v2.resolve_active_taxonomy_v2_path(tmp_path, force_reresolve=True) is None
    assert not (tmp_path / "taxonomy_v2").exists()
