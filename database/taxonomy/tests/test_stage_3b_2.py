"""Stage 3B.2 tests: taxonomy-v2 install/activation + lookup fan-out +
observation backfill precedence.
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


def _fake_v2_sqlite(path: Path) -> str:
    conn = sqlite3.connect(str(path))
    conn.executescript(
        """
        CREATE TABLE taxon_min (
            taxon_id INTEGER PRIMARY KEY,
            genus TEXT, specific_epithet TEXT, family TEXT,
            canonical_scientific_name TEXT
        );
        CREATE TABLE scientific_name_min (
            scientific_name_id INTEGER PRIMARY KEY AUTOINCREMENT,
            taxon_id INTEGER NOT NULL,
            language_code TEXT NOT NULL,
            scientific_name TEXT NOT NULL,
            is_preferred_name INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE vernacular_min (
            vernacular_id INTEGER PRIMARY KEY AUTOINCREMENT,
            taxon_id INTEGER NOT NULL,
            language_code TEXT NOT NULL,
            vernacular_name TEXT NOT NULL,
            is_preferred_name INTEGER NOT NULL DEFAULT 0,
            source TEXT
        );
        CREATE TABLE taxon_external_id_min (
            external_id_row_id INTEGER PRIMARY KEY,
            taxon_id INTEGER NOT NULL,
            source_system TEXT NOT NULL,
            external_id INTEGER NOT NULL,
            id_role TEXT NOT NULL,
            is_preferred INTEGER NOT NULL DEFAULT 0,
            external_name TEXT,
            note TEXT
        );
        CREATE TABLE taxon_external_id_text_min (
            external_id_row_id INTEGER PRIMARY KEY,
            taxon_id INTEGER NOT NULL,
            source_system TEXT NOT NULL,
            namespace TEXT NOT NULL,
            external_id TEXT NOT NULL,
            id_role TEXT NOT NULL,
            is_preferred INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE taxonomy_meta (key TEXT PRIMARY KEY, value TEXT);
        """
    )
    conn.executemany("INSERT INTO taxon_min VALUES (?, ?, ?, ?, ?)", [
        (133345, "Candolleomyces", "candolleanus", "Psathyrellaceae",
         "Candolleomyces candolleanus"),
        (54995, "Pseudoramonia", "isidiata", "", "Pseudoramonia isidiata"),
    ])
    conn.executemany(
        "INSERT INTO scientific_name_min "
        "(taxon_id, language_code, scientific_name, is_preferred_name) "
        "VALUES (?, ?, ?, ?)",
        [
            (133345, "sci", "Candolleomyces candolleanus", 1),
            (133345, "sci", "Psathyrella candolleana", 0),
        ],
    )
    conn.executemany(
        "INSERT INTO vernacular_min "
        "(taxon_id, language_code, vernacular_name, is_preferred_name, source) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            (133345, "nb", "hvit sprøsopp", 1, "nortaxa"),
            (133345, "nn", "kvit sprøsopp", 1, "nortaxa"),
            (133345, "sma", "test-sma", 0, "nortaxa"),
        ],
    )
    conn.executemany(
        "INSERT INTO taxon_external_id_min "
        "(external_id_row_id, taxon_id, source_system, external_id, id_role, "
        "is_preferred, external_name, note) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (1, 133345, "artsdatabanken", 300190, "accepted", 1, None, None),
            (2, 133345, "artsdatabanken", 54995, "synonym", 0, None, None),
            (3, 133345, "artportalen", 222138, "accepted", 1, None, None),
        ],
    )
    conn.executemany(
        "INSERT INTO taxon_external_id_text_min "
        "(external_id_row_id, taxon_id, source_system, namespace, external_id, "
        "id_role, is_preferred) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (1, 133345, "col_xr", "col_usage_id", "9Z2GC", "accepted", 1),
            # Taxonomy-v2 closeout Stage 2: the authoritative namespaced
            # mapping the observation backfill now resolves against. The
            # ``taxon_external_id_min`` rows above stay in place deliberately,
            # so the backfill tests prove the namespace-lost integer table is
            # no longer consulted.
            (2, 133345, "nortaxa", "nortaxa_taxon_id", "54995", "accepted", 1),
        ],
    )
    conn.executemany("INSERT INTO taxonomy_meta VALUES (?, ?)", [
        ("taxonomy_schema_version", "2"),
        ("content_release_id", "tax-2026.07.29-01"),
        ("state", "candidate"),
        ("publication", "none"),
    ])
    conn.commit()
    conn.close()
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _build_gz(sqlite_path: Path, gz_path: Path) -> tuple[str, str, int]:
    with sqlite_path.open("rb") as src, gz_path.open("wb") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=6, mtime=0) as gz:
            gz.write(src.read())
    gz_sha = hashlib.sha256(gz_path.read_bytes()).hexdigest()
    sql_sha = hashlib.sha256(sqlite_path.read_bytes()).hexdigest()
    return gz_sha, sql_sha, sqlite_path.stat().st_size


def _manifest(tmp_path: Path, gz_sha: str, sql_sha: str, sql_bytes: int,
              install_name: str = "vernacular_multilanguage_v2.sqlite3") -> Path:
    p = tmp_path / "manifest.json"
    p.write_text(json.dumps({
        "manifest_schema_version": 1,
        "taxonomy_schema_version": 2,
        "content_release_id": "tax-2026.07.29-01",
        "state": "candidate",
        "publication": "none",
        "gz_artifact": "tax.sqlite3.gz",
        "gz_sha256": gz_sha,
        "gz_bytes": 0,
        "sqlite_sha256": sql_sha,
        "sqlite_bytes": sql_bytes,
        "registry_concatenated_sha256": "00" * 32,
        "compiler_manifest_sha256": "00" * 32,
        "install_target_name": install_name,
    }, indent=2))
    return p


# ---------------- installer / activation ----------------------------------


def test_install_extracts_verifies_and_atomic_renames(tmp_path: Path) -> None:
    from utils.taxonomy_v2 import TaxonomyV2Manifest, ensure_installed
    src = tmp_path / "raw.sqlite3"
    _fake_v2_sqlite(src)
    gz = tmp_path / "artifact.gz"
    gz_sha, sql_sha, sql_bytes = _build_gz(src, gz)
    manifest_path = _manifest(tmp_path, gz_sha, sql_sha, sql_bytes)
    manifest = TaxonomyV2Manifest.load(manifest_path)
    app_data = tmp_path / "userdata"
    result = ensure_installed(app_data_dir=app_data, manifest=manifest, gz_path=gz)
    assert result.exists()
    assert hashlib.sha256(result.read_bytes()).hexdigest() == sql_sha


def test_install_reuses_existing_valid_install(tmp_path: Path) -> None:
    from utils.taxonomy_v2 import TaxonomyV2Manifest, ensure_installed
    src = tmp_path / "raw.sqlite3"
    _fake_v2_sqlite(src)
    gz = tmp_path / "artifact.gz"
    gz_sha, sql_sha, sql_bytes = _build_gz(src, gz)
    manifest = TaxonomyV2Manifest.load(_manifest(tmp_path, gz_sha, sql_sha, sql_bytes))
    app_data = tmp_path / "userdata"
    ensure_installed(app_data_dir=app_data, manifest=manifest, gz_path=gz)
    target = app_data / "taxonomy_v2" / manifest.install_target_name
    mtime_before = target.stat().st_mtime
    # Second call must NOT touch the file (reuse verified install).
    gz.unlink()  # remove gz to prove the reuse path doesn't extract
    ensure_installed(app_data_dir=app_data, manifest=manifest, gz_path=gz)
    assert target.stat().st_mtime == mtime_before


def test_install_rejects_gz_sha_mismatch(tmp_path: Path) -> None:
    from utils.taxonomy_v2 import TaxonomyV2InstallError, TaxonomyV2Manifest, ensure_installed
    src = tmp_path / "raw.sqlite3"
    _fake_v2_sqlite(src)
    gz = tmp_path / "artifact.gz"
    gz_sha, sql_sha, sql_bytes = _build_gz(src, gz)
    # Manifest declares WRONG gz sha.
    manifest = TaxonomyV2Manifest.load(_manifest(tmp_path, "0" * 64, sql_sha, sql_bytes))
    with pytest.raises(TaxonomyV2InstallError, match="gzip artifact SHA-256 mismatch"):
        ensure_installed(app_data_dir=tmp_path / "userdata",
                         manifest=manifest, gz_path=gz)


def test_install_rejects_decompressed_sha_mismatch(tmp_path: Path) -> None:
    from utils.taxonomy_v2 import TaxonomyV2InstallError, TaxonomyV2Manifest, ensure_installed
    src = tmp_path / "raw.sqlite3"
    _fake_v2_sqlite(src)
    gz = tmp_path / "artifact.gz"
    gz_sha, _sql_sha, sql_bytes = _build_gz(src, gz)
    manifest = TaxonomyV2Manifest.load(_manifest(tmp_path, gz_sha, "0" * 64, sql_bytes))
    target_dir = (tmp_path / "userdata" / "taxonomy_v2")
    with pytest.raises(TaxonomyV2InstallError, match="SQLite SHA-256 mismatch"):
        ensure_installed(app_data_dir=tmp_path / "userdata",
                         manifest=manifest, gz_path=gz)
    # Failure must not leave a partial install.
    if target_dir.exists():
        assert list(target_dir.glob("vernacular_*")) == []


def test_read_only_open_verifies_meta(tmp_path: Path) -> None:
    from utils.taxonomy_v2 import TaxonomyV2InstallError, open_taxonomy_v2_readonly
    src = tmp_path / "raw.sqlite3"
    _fake_v2_sqlite(src)
    conn, meta = open_taxonomy_v2_readonly(src)
    assert meta["taxonomy_schema_version"] == "2"
    assert meta["content_release_id"] == "tax-2026.07.29-01"
    conn.close()
    # Tamper with meta and expect refusal.
    bad = tmp_path / "bad.sqlite3"
    _fake_v2_sqlite(bad)
    c = sqlite3.connect(str(bad))
    c.execute("UPDATE taxonomy_meta SET value='3' WHERE key='taxonomy_schema_version'")
    c.commit(); c.close()
    with pytest.raises(TaxonomyV2InstallError, match="taxonomy_schema_version"):
        open_taxonomy_v2_readonly(bad)


# ---------------- artifact-path derivation --------------------------------
# Regression guard for the runtime bug where the installer looked for a
# hardcoded gzip filename that no longer matched the shipped manifest.


def test_gz_path_defaults_to_manifest_gz_artifact(tmp_path: Path,
                                                  monkeypatch) -> None:
    """When ``gz_path`` is omitted, ``ensure_installed`` derives it from
    ``manifest.gz_artifact`` joined with the module-level
    ``TAXONOMY_V2_DIR``. This prevents the release-rollover regression
    where the runtime kept looking at the previous release's filename."""
    import utils.taxonomy_v2 as tx
    src = tmp_path / "raw.sqlite3"
    _fake_v2_sqlite(src)
    # The gz file is named to match the manifest's declared ``gz_artifact``.
    gz = tmp_path / "tax.sqlite3.gz"
    gz_sha, sql_sha, sql_bytes = _build_gz(src, gz)
    manifest = tx.TaxonomyV2Manifest.load(
        _manifest(tmp_path, gz_sha, sql_sha, sql_bytes))
    # Redirect the module-level directory so the derived path is
    # ``tmp_path / "tax.sqlite3.gz"``.
    monkeypatch.setattr(tx, "TAXONOMY_V2_DIR", tmp_path)
    result = tx.ensure_installed(app_data_dir=tmp_path / "userdata",
                                 manifest=manifest)
    assert result.exists()
    assert hashlib.sha256(result.read_bytes()).hexdigest() == sql_sha


def test_explicit_gz_path_override_is_not_overwritten(tmp_path: Path,
                                                     monkeypatch) -> None:
    """An explicit ``gz_path`` MUST be used verbatim even when the manifest
    declares a different filename — the caller owns the location."""
    import utils.taxonomy_v2 as tx
    src = tmp_path / "raw.sqlite3"
    _fake_v2_sqlite(src)
    explicit_gz = tmp_path / "explicit-name.gz"
    gz_sha, sql_sha, sql_bytes = _build_gz(src, explicit_gz)
    manifest = tx.TaxonomyV2Manifest.load(
        _manifest(tmp_path, gz_sha, sql_sha, sql_bytes))
    # Point TAXONOMY_V2_DIR at an empty directory so the manifest-derived
    # path would fail — proving the explicit gz_path is the one actually
    # opened.
    empty_dir = tmp_path / "nowhere"
    empty_dir.mkdir()
    monkeypatch.setattr(tx, "TAXONOMY_V2_DIR", empty_dir)
    result = tx.ensure_installed(app_data_dir=tmp_path / "userdata",
                                 manifest=manifest, gz_path=explicit_gz)
    assert result.exists()
    assert hashlib.sha256(result.read_bytes()).hexdigest() == sql_sha


def test_missing_artifact_raises_not_found_error(tmp_path: Path,
                                                 monkeypatch) -> None:
    """Message shape must remain ``gzip artifact not found at <path>`` so
    operators can grep for it in logs."""
    import utils.taxonomy_v2 as tx
    src = tmp_path / "raw.sqlite3"
    _fake_v2_sqlite(src)
    gz = tmp_path / "tax.sqlite3.gz"
    gz_sha, sql_sha, sql_bytes = _build_gz(src, gz)
    manifest = tx.TaxonomyV2Manifest.load(
        _manifest(tmp_path, gz_sha, sql_sha, sql_bytes))
    # Delete the artifact so the resolved path does not exist.
    gz.unlink()
    monkeypatch.setattr(tx, "TAXONOMY_V2_DIR", tmp_path)
    with pytest.raises(tx.TaxonomyV2InstallError,
                       match=r"gzip artifact not found at "):
        tx.ensure_installed(app_data_dir=tmp_path / "userdata",
                            manifest=manifest)


@pytest.mark.parametrize("bad_name", [
    "",                                     # empty
    "/etc/passwd",                          # absolute POSIX
    "..",                                   # parent-only
    "../evil.sqlite3.gz",                   # parent traversal
    "sub/tax.sqlite3.gz",                   # embedded separator
    "sub\\tax.sqlite3.gz",                  # windows-style separator
    "tax.sqlite3",                          # wrong suffix (no .gz)
    "tax.gz",                               # wrong suffix (no .sqlite3)
    ".sqlite3.gz",                          # suffix-only, no basename
    "C:tax.sqlite3.gz",                     # windows drive-letter
])
def test_unsafe_manifest_artifact_name_raises(tmp_path: Path, monkeypatch,
                                              bad_name: str) -> None:
    """The manifest's ``gz_artifact`` field MUST be a bare filename with
    the canonical ``.sqlite3.gz`` suffix. Anything else refuses to
    install so a malicious or broken manifest cannot direct the extractor
    at an arbitrary filesystem location."""
    import utils.taxonomy_v2 as tx
    src = tmp_path / "raw.sqlite3"
    _fake_v2_sqlite(src)
    gz = tmp_path / "tax.sqlite3.gz"
    gz_sha, sql_sha, sql_bytes = _build_gz(src, gz)
    # Build a manifest with the offending gz_artifact.
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps({
        "manifest_schema_version": 1,
        "taxonomy_schema_version": 2,
        "content_release_id": "tax-test",
        "state": "candidate",
        "publication": "none",
        "gz_artifact": bad_name,
        "gz_sha256": gz_sha,
        "gz_bytes": 0,
        "sqlite_sha256": sql_sha,
        "sqlite_bytes": sql_bytes,
        "registry_concatenated_sha256": "00" * 32,
        "compiler_manifest_sha256": "00" * 32,
        "install_target_name": "vernacular_multilanguage_v2.sqlite3",
    }))
    manifest = tx.TaxonomyV2Manifest.load(manifest_path)
    monkeypatch.setattr(tx, "TAXONOMY_V2_DIR", tmp_path)
    with pytest.raises(tx.TaxonomyV2InstallError,
                       match=r"invalid gzip artifact"):
        tx.ensure_installed(app_data_dir=tmp_path / "userdata",
                            manifest=manifest)


def test_first_install_writes_receipt_and_second_call_takes_fast_path(
        tmp_path: Path, monkeypatch) -> None:
    """First-install writes the receipt; the second call must reuse the
    installed SQLite without running ``_sha256_file`` over any file
    (fast path) unless ``force_verify`` is set."""
    import utils.taxonomy_v2 as tx
    src = tmp_path / "raw.sqlite3"
    _fake_v2_sqlite(src)
    gz = tmp_path / "tax.sqlite3.gz"
    gz_sha, sql_sha, sql_bytes = _build_gz(src, gz)
    manifest = tx.TaxonomyV2Manifest.load(
        _manifest(tmp_path, gz_sha, sql_sha, sql_bytes))
    monkeypatch.setattr(tx, "TAXONOMY_V2_DIR", tmp_path)
    app_data = tmp_path / "userdata"

    # First install: derives gz_path from the manifest, extracts, hashes.
    result = tx.ensure_installed(app_data_dir=app_data, manifest=manifest)
    receipt = result.with_name("install_receipt.json")
    assert receipt.exists()
    body = json.loads(receipt.read_text())
    assert body["sqlite_sha256"] == sql_sha
    assert body["sqlite_bytes"] == sql_bytes
    assert body["content_release_id"] == manifest.content_release_id

    # Second call: receipt fast path must run zero full-file hashes.
    original = tx._sha256_file
    hash_calls = {"count": 0}

    def counted(path):
        hash_calls["count"] += 1
        return original(path)

    monkeypatch.setattr(tx, "_sha256_file", counted)
    result2 = tx.ensure_installed(app_data_dir=app_data, manifest=manifest)
    assert result2 == result
    assert hash_calls["count"] == 0

    # ``force_verify=True`` MUST full-hash the installed SQLite once.
    hash_calls["count"] = 0
    tx.ensure_installed(app_data_dir=app_data, manifest=manifest,
                       force_verify=True)
    assert hash_calls["count"] == 1


def test_activation_env_and_settings_gate(tmp_path: Path, monkeypatch) -> None:
    from utils.taxonomy_v2 import is_activation_enabled
    monkeypatch.delenv("SPORELY_TAXONOMY_V2", raising=False)
    assert is_activation_enabled(tmp_path) is False
    (tmp_path / "app_settings.json").write_text(
        json.dumps({"taxonomy_v2_activation": True}))
    assert is_activation_enabled(tmp_path) is True
    # Env var wins over settings.
    monkeypatch.setenv("SPORELY_TAXONOMY_V2", "0")
    assert is_activation_enabled(tmp_path) is False


# ---------------- lookup fan-out -----------------------------------------


def test_language_fanout_no_covers_nb_and_nn(tmp_path: Path) -> None:
    from database.vernacular_db import VernacularDB
    src = tmp_path / "v2.sqlite3"
    _fake_v2_sqlite(src)
    db = VernacularDB(src, language_code="no")
    # taxon_from_vernacular exercises _language_clause.
    result = db.taxon_from_vernacular("hvit sprøsopp")
    assert result is not None
    assert result[0] == "Candolleomyces"
    # nn word resolves via umbrella `no` request.
    result = db.taxon_from_vernacular("kvit sprøsopp")
    assert result is not None


def test_nb_and_nn_remain_distinct_when_requested(tmp_path: Path) -> None:
    from database.vernacular_db import VernacularDB
    src = tmp_path / "v2.sqlite3"
    _fake_v2_sqlite(src)
    db_nb = VernacularDB(src, language_code="nb")
    result = db_nb.taxon_from_vernacular("kvit sprøsopp")
    # 'kvit sprøsopp' is stored under 'nn' — an explicit `nb` request must
    # NOT match it.
    assert result is None
    db_nn = VernacularDB(src, language_code="nn")
    assert db_nn.taxon_from_vernacular("kvit sprøsopp") is not None


def test_col_text_identifier_resolution(tmp_path: Path, monkeypatch) -> None:
    src = tmp_path / "v2.sqlite3"
    _fake_v2_sqlite(src)
    # Point resolve_vernacular_db_path at our synthetic v2 DB.
    import utils.vernacular_utils as vu
    monkeypatch.setattr(vu, "resolve_vernacular_db_path",
                        lambda lang_code=None: src)
    from database.models import _resolve_external_taxon_text_id
    result = _resolve_external_taxon_text_id(
        "Candolleomyces", "candolleanus", "col_xr")
    assert result == "9Z2GC"


def test_integer_54995_only_treated_as_sporely_when_it_actually_is(tmp_path: Path) -> None:
    src = tmp_path / "v2.sqlite3"
    _fake_v2_sqlite(src)
    conn = sqlite3.connect(f"file:{src}?mode=ro", uri=True)
    # NBIC-namespaced 54995 → Sporely 133345.
    row = conn.execute(
        "SELECT taxon_id FROM taxon_external_id_min "
        "WHERE source_system='artsdatabanken' AND external_id=?", (54995,)).fetchone()
    assert row[0] == 133345
    # And Sporely 54995 (coincidentally exists) is a different concept.
    row = conn.execute(
        "SELECT canonical_scientific_name FROM taxon_min WHERE taxon_id=?",
        (54995,)).fetchone()
    assert row[0] == "Pseudoramonia isidiata"


# ---------------- observation backfill -----------------------------------


def _make_observations_db(path: Path, rows: list[dict]) -> None:
    conn = sqlite3.connect(str(path))
    conn.executescript(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            genus TEXT, species TEXT,
            artsdata_id INTEGER,
            ai_selected_taxon_id TEXT,
            ai_selected_scientific_name TEXT,
            sporely_taxon_id INTEGER,
            -- Taxonomy-v2 closeout Stage 2 provenance columns, mirroring
            -- database/schema.py.
            taxon_identity_state TEXT,
            taxon_identity_proof TEXT,
            taxon_identity_source_system TEXT,
            taxon_identity_namespace TEXT,
            taxon_identity_external_id TEXT,
            taxon_identity_raw_external_id TEXT,
            taxon_identity_provenance TEXT
        );
        """
    )
    for r in rows:
        conn.execute(
            "INSERT INTO observations "
            "(genus, species, artsdata_id, ai_selected_taxon_id, "
            "ai_selected_scientific_name, sporely_taxon_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (r.get("genus"), r.get("species"), r.get("artsdata_id"),
             r.get("ai_selected_taxon_id"), r.get("ai_selected_scientific_name"),
             r.get("sporely_taxon_id")),
        )
    conn.commit(); conn.close()


def test_backfill_precedence_and_ambiguity(tmp_path: Path) -> None:
    from database.migrate_observations_sporely_id import backfill
    tax = tmp_path / "tax.sqlite3"
    _fake_v2_sqlite(tax)
    # Add a second canonical row for the same name to trigger ambiguity.
    c = sqlite3.connect(str(tax))
    c.execute("INSERT INTO taxon_min (taxon_id, genus, specific_epithet, "
              "family, canonical_scientific_name) VALUES (?, ?, ?, ?, ?)",
              (555, "AmbiG", "ambi", "", "Ambig ambi"))
    c.execute("INSERT INTO taxon_min VALUES (?, ?, ?, ?, ?)",
              (556, "AmbiG", "ambi", "", "Ambig ambi"))
    c.commit(); c.close()
    obs = tmp_path / "obs.sqlite3"
    _make_observations_db(obs, [
        # already-valid sporely_id → kept, promoted to artifact-proven
        {"sporely_taxon_id": 133345},
        # invalid sporely id → cleared to NULL
        {"sporely_taxon_id": 9999},
        # NBIC-prefixed ai_selected_taxon_id → resolved through the
        # authoritative (nortaxa, nortaxa_taxon_id, 54995) mapping
        {"ai_selected_taxon_id": "NBIC:54995"},
        # bare artsdata_id 300190 → reported, NEVER resolved (Stage 2)
        {"artsdata_id": 300190},
        # unique scientific name via ai snapshot → reported, never resolved
        {"ai_selected_scientific_name": "Candolleomyces candolleanus"},
        # ambiguous scientific name → left NULL
        {"genus": "Ambig", "species": "ambi"},
        # unresolvable
        {"genus": "Zzzz", "species": "zzzz"},
    ])
    stats = backfill(observation_db_path=obs, taxonomy_db_path=tax)
    assert stats.already_populated_kept == 1
    assert stats.already_populated_rejected == 1
    assert stats.resolved_by_explicit_nbic == 1
    # Stage 2 flips both of these from "resolved" to "reported only": an
    # Artsobservasjoner sighting id and a scientific-name match are not
    # identity evidence, so neither may write sporely_taxon_id.
    assert stats.artsdata_id_not_resolved == 1
    assert stats.unique_scientific_name_not_resolved == 1
    assert stats.ambiguous_scientific_name_left_null == 1
    # Four rows end with no identity: the artsdata_id row, the unique-name
    # row, the ambiguous-name row and the unresolvable row. The step-3/4
    # counters above report evidence shape; this one reports the outcome.
    assert stats.unresolved_left_null == 4
    conn = sqlite3.connect(str(obs))
    # The resolved row keeps its provider snapshot AND records how the
    # identity was proven, including the verbatim NBIC value.
    row = conn.execute(
        "SELECT sporely_taxon_id, ai_selected_taxon_id, taxon_identity_state, "
        "       taxon_identity_proof, taxon_identity_source_system, "
        "       taxon_identity_namespace, taxon_identity_external_id, "
        "       taxon_identity_raw_external_id, "
        "       taxon_identity_provenance "
        "FROM observations WHERE ai_selected_taxon_id='NBIC:54995'").fetchone()
    assert row == (
        133345, "NBIC:54995", "sporely_v2", "external_id_resolution",
        "nortaxa", "nortaxa_taxon_id", "54995", "NBIC:54995",
        # The release the resolution was performed against, taken from the
        # candidate's `content_release_id`, so the proof names its artifact.
        "sporely_taxonomy_v2_backfill:tax-2026.07.29-01",
    )
    # The artsdata_id and unique-name rows are left without identity.
    assert conn.execute(
        "SELECT sporely_taxon_id, taxon_identity_state FROM observations "
        "WHERE artsdata_id = 300190").fetchone() == (None, None)
    assert conn.execute(
        "SELECT sporely_taxon_id, taxon_identity_state FROM observations "
        "WHERE ai_selected_scientific_name = 'Candolleomyces candolleanus'"
    ).fetchone() == (None, None)
    conn.close()


def test_backfill_never_resolves_a_namespace_lost_integer(tmp_path: Path) -> None:
    """A bare integer carries no namespace, so it cannot resolve.

    Taxonomy-v2 closeout Stage 2 regression. ``54995`` is simultaneously a
    valid Sporely ``taxon_id`` in this fixture and the external identifier
    mapped to Sporely ``133345`` — exactly the numeric collision the plan
    requires the client to survive. Neither reading may be applied to a bare
    integer: the old code accepted one as an NBIC id, and the deployed
    server-side release-membership check cannot catch a collision like this.
    """
    from database.migrate_observations_sporely_id import backfill
    tax = tmp_path / "tax.sqlite3"
    _fake_v2_sqlite(tax)
    obs = tmp_path / "obs.sqlite3"
    _make_observations_db(obs, [
        {"ai_selected_taxon_id": "54995"},
        {"ai_selected_taxon_id": "ZZZZ:54995"},
    ])
    stats = backfill(observation_db_path=obs, taxonomy_db_path=tax)
    assert stats.resolved_by_explicit_nbic == 0
    assert stats.unresolved_left_null == 2
    conn = sqlite3.connect(str(obs))
    assert conn.execute(
        "SELECT DISTINCT sporely_taxon_id, taxon_identity_state "
        "FROM observations").fetchall() == [(None, None)]
    conn.close()


def test_backfill_leaves_ambiguous_external_identifier_null(tmp_path: Path) -> None:
    """Stage 2: resolvers return ambiguity; they never collapse it."""
    from database.migrate_observations_sporely_id import backfill
    tax = tmp_path / "tax.sqlite3"
    _fake_v2_sqlite(tax)
    c = sqlite3.connect(str(tax))
    # A second concept claiming the same namespaced identifier.
    c.execute(
        "INSERT INTO taxon_external_id_text_min "
        "(external_id_row_id, taxon_id, source_system, namespace, external_id, "
        "id_role, is_preferred) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (99, 54995, "nortaxa", "nortaxa_taxon_id", "54995", "synonym", 0),
    )
    c.commit(); c.close()
    obs = tmp_path / "obs.sqlite3"
    _make_observations_db(obs, [{"ai_selected_taxon_id": "NBIC:54995"}])
    stats = backfill(observation_db_path=obs, taxonomy_db_path=tax)
    assert stats.resolved_by_explicit_nbic == 0
    assert stats.ambiguous_external_identifier_left_null == 1
    conn = sqlite3.connect(str(obs))
    assert conn.execute(
        "SELECT sporely_taxon_id FROM observations").fetchone() == (None,)
    conn.close()


def test_backfill_is_idempotent(tmp_path: Path) -> None:
    from database.migrate_observations_sporely_id import backfill
    tax = tmp_path / "tax.sqlite3"
    _fake_v2_sqlite(tax)
    obs = tmp_path / "obs.sqlite3"
    _make_observations_db(obs, [
        {"ai_selected_taxon_id": "NBIC:54995"},
        # Stage 2: this row no longer resolves at all, so only one row is
        # touched by the first pass.
        {"artsdata_id": 300190},
    ])
    stats_a = backfill(observation_db_path=obs, taxonomy_db_path=tax)
    stats_b = backfill(observation_db_path=obs, taxonomy_db_path=tax)
    assert stats_a.rows_touched == 1
    assert stats_b.already_populated_kept == 1
    # Already-proven rows are skipped entirely, so a second pass writes
    # nothing — including no provenance churn.
    assert stats_b.rows_touched == 0


# ---------------- corpus v2 shape ----------------------------------------


def test_regression_corpus_v2_shape_is_valid() -> None:
    corpus = json.loads(Path(_ROOT / "database/taxonomy/evidence/baseline/regression-corpus-v2.json").read_text())
    assert corpus["format"] == "sporely-taxonomy-regression-corpus-v2"
    assert corpus["case_count"] == sum(len(v) for v in corpus["groups"].values())
    for row in corpus["groups"]["scientific_synonym"]:
        assert "expected_sporely_taxon_id" in row
    # Psathyrella candolleana lives in scientific_synonym now.
    ps = [r for r in corpus["groups"]["scientific_synonym"]
          if r.get("query") == "Psathyrella candolleana"]
    assert ps and ps[0]["legacy_nortaxa_taxon_id"] == 54995
    assert ps[0]["expected_sporely_taxon_id"] == 133345
    # Candolleomyces candolleanus was moved out of the missing group.
    missing = [r for r in corpus["groups"]["missing"]
               if r.get("query") == "Candolleomyces candolleanus"]
    assert not missing


def test_backfill_does_not_promote_a_colliding_legacy_integer(tmp_path: Path) -> None:
    """Membership in ``taxon_min`` is existence, not origin.

    Taxonomy-v2 closeout Stage 2 regression, backfill→sync. ``54995`` is a
    real Sporely ``taxon_id`` in this fixture AND, separately, the NorTaxa
    external identifier mapped to Sporely ``133345``. A pre-Stage-2 row whose
    ``sporely_taxon_id`` holds ``54995`` because some legacy path copied an
    external integer therefore LOOKS valid to a membership check.

    An earlier draft of this module promoted exactly that to
    ``taxonomy_v2_artifact`` proof, which handed the collision a proof token
    and would have waved it through the cloud gate — the one residual risk the
    deployed release-membership check in ``set_observation_selected_taxon_v2``
    cannot catch either, since ``54995`` genuinely is in the active release.

    The row must stay legacy-unverified: the integer is kept (it is real
    persisted data) but no proof is written, so cloud sync keeps refusing it.
    """
    from database.migrate_observations_sporely_id import backfill
    from utils.cloud_sync import SporelyCloudClient
    from utils.taxon_identity import TaxonIdentity, proven_sporely_taxon_id

    tax = tmp_path / "tax.sqlite3"
    _fake_v2_sqlite(tax)
    obs = tmp_path / "obs.sqlite3"
    # No provider identifier to re-derive from — the ONLY sound promotion
    # path is unavailable, which is the realistic legacy shape.
    _make_observations_db(obs, [{"sporely_taxon_id": 54995}])

    stats = backfill(observation_db_path=obs, taxonomy_db_path=tax)
    assert stats.already_populated_kept == 1
    assert stats.already_populated_rejected == 0
    assert stats.legacy_kept_unverified == 1
    assert stats.legacy_promoted_by_namespaced_resolution == 0
    assert stats.rows_touched == 0

    conn = sqlite3.connect(str(obs))
    conn.row_factory = sqlite3.Row
    row = dict(conn.execute("SELECT * FROM observations").fetchone())
    conn.close()
    # The integer survives — the backfill does not destroy persisted data.
    assert row["sporely_taxon_id"] == 54995
    # But nothing claims it is proven.
    assert row["taxon_identity_state"] is None
    assert row["taxon_identity_proof"] is None
    identity = TaxonIdentity.from_row(row)
    assert identity.is_legacy_unverified is True
    assert identity.is_proven_sporely is False
    assert proven_sporely_taxon_id(row) is None

    # End-to-end: the cloud gate refuses to emit it.
    client = object.__new__(SporelyCloudClient)
    client.user_id = "00000000-0000-4000-8000-000000000001"
    client._resolve_existing_observation_for_push = lambda _obs, remote_obs=None: "1184"
    client._patch = lambda *_a, **_k: None
    calls: list = []
    client._rpc = lambda name, payload: calls.append((name, payload))
    remote = {"id": 1184, "selected_sporely_taxon_id": None}
    client.push_observation(
        {"id": 1, "cloud_id": "1184", **row}, remote_obs=remote,
    )
    assert calls == []


def test_backfill_promotes_only_when_the_provider_identifier_agrees(tmp_path: Path) -> None:
    """The one sound promotion, and its disagreement case.

    A pre-Stage-2 integer becomes proven only when the row's own namespaced
    provider identifier independently resolves to the SAME value. When the
    authoritative resolution disagrees with the stored integer, the stored
    value is kept but stays unverified rather than being silently rewritten.
    """
    from database.migrate_observations_sporely_id import backfill
    from utils.taxon_identity import TaxonIdentity

    tax = tmp_path / "tax.sqlite3"
    _fake_v2_sqlite(tax)
    obs = tmp_path / "obs.sqlite3"
    _make_observations_db(obs, [
        # Agrees: NBIC:54995 → (nortaxa, nortaxa_taxon_id, 54995) → 133345.
        {"sporely_taxon_id": 133345, "ai_selected_taxon_id": "NBIC:54995"},
        # Disagrees: the stored integer is not what the identifier resolves to.
        {"sporely_taxon_id": 54995, "ai_selected_taxon_id": "NBIC:54995"},
    ])

    stats = backfill(observation_db_path=obs, taxonomy_db_path=tax)
    assert stats.already_populated_kept == 2
    assert stats.legacy_promoted_by_namespaced_resolution == 1
    assert stats.legacy_kept_unverified == 1

    conn = sqlite3.connect(str(obs))
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute(
        "SELECT * FROM observations ORDER BY id").fetchall()]
    conn.close()

    agreed, disagreed = rows
    assert agreed["taxon_identity_state"] == "sporely_v2"
    assert agreed["taxon_identity_proof"] == "external_id_resolution"
    assert agreed["taxon_identity_namespace"] == "nortaxa_taxon_id"
    assert agreed["taxon_identity_raw_external_id"] == "NBIC:54995"
    assert TaxonIdentity.from_row(agreed).is_proven_sporely is True
    # A promoted row must record WHICH taxonomy release re-verified it;
    # "artifact-proven" with no release is an unfalsifiable claim.
    assert agreed["taxon_identity_provenance"].startswith(
        "sporely_taxonomy_v2_backfill"
    )

    # A cleared row keeps no provenance from the identity it lost.
    assert disagreed["taxon_identity_provenance"] is None

    assert disagreed["sporely_taxon_id"] == 54995
    assert disagreed["taxon_identity_state"] is None
    assert TaxonIdentity.from_row(disagreed).is_proven_sporely is False
