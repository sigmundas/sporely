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
        [(1, 133345, "col_xr", "col_usage_id", "9Z2GC", "accepted", 1)],
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
            sporely_taxon_id INTEGER
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
        # already-valid sporely_id → kept
        {"sporely_taxon_id": 133345},
        # invalid sporely id → cleared to NULL
        {"sporely_taxon_id": 9999},
        # NBIC-style ai_selected_taxon_id → resolved via artsdatabanken 54995
        {"ai_selected_taxon_id": "NBIC:54995"},
        # bare artsdata_id 300190
        {"artsdata_id": 300190},
        # unique scientific name via ai snapshot
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
    assert stats.resolved_by_artsdata_id == 1
    assert stats.resolved_by_unique_scientific_name == 1
    assert stats.ambiguous_scientific_name_left_null == 1
    assert stats.unresolved_left_null == 1
    # Preserve snapshots — spot-check.
    conn = sqlite3.connect(str(obs))
    row = conn.execute(
        "SELECT sporely_taxon_id, ai_selected_taxon_id "
        "FROM observations WHERE ai_selected_taxon_id='NBIC:54995'").fetchone()
    assert row == (133345, "NBIC:54995")


def test_backfill_is_idempotent(tmp_path: Path) -> None:
    from database.migrate_observations_sporely_id import backfill
    tax = tmp_path / "tax.sqlite3"
    _fake_v2_sqlite(tax)
    obs = tmp_path / "obs.sqlite3"
    _make_observations_db(obs, [
        {"ai_selected_taxon_id": "NBIC:54995"},
        {"artsdata_id": 300190},
    ])
    stats_a = backfill(observation_db_path=obs, taxonomy_db_path=tax)
    stats_b = backfill(observation_db_path=obs, taxonomy_db_path=tax)
    assert stats_a.rows_touched == 2
    # Second run: both already have valid sporely ids → kept.
    assert stats_b.already_populated_kept == 2
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
