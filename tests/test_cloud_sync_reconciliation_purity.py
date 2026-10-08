"""Purity of the cloud-sync pure reconciliation package (Stage S4 of the extraction).

Every module under ``utils/cloud_sync_impl/reconciliation/`` is pure: no SQLite
or database access, no settings, no cloud client or other I/O, no Qt. The rules
live in ``tools/cloud_sync_reconciliation_purity.py``; this file holds the
allowlists. A symbol that turns out to be stateful moves to a stateful owner --
these allowlists are never widened to admit it.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

from tools.cloud_sync_reconciliation_purity import check_purity

ROOT = Path(__file__).resolve().parents[1]
PURE_PACKAGE = ROOT / "utils" / "cloud_sync_impl" / "reconciliation"

#: Every module of the pure package (discovered, so a new module is checked too).
PURE_MODULES = frozenset(
    "utils.cloud_sync_impl.reconciliation"
    + ("" if path.stem == "__init__" else f".{path.stem}")
    for path in PURE_PACKAGE.glob("*.py")
)

#: Stage S3 leaf owners that are themselves pure and that pure code may import.
PURE_LEAF_OWNERS = frozenset({"utils.cloud_sync_impl.common"})

STDLIB = frozenset({"json", "math", "re", "uuid", "datetime", "dataclasses", "typing"})

#: Value-only names from other project modules. Each is a pure function, class or
#: constant; importing it performs no I/O.
EXTERNAL_NAMES: dict[str, frozenset[str]] = {
    # Structured taxonomy identity value type and its column names.
    "utils.taxon_identity": frozenset({"TaxonIdentity", "IDENTITY_COLUMNS", "STATE_NONE"}),
    # Media key string helpers (no client): normalize_media_key via common,
    # media_variant_key for measurement compare payloads.
    "utils.r2_storage": frozenset({"normalize_media_key", "media_variant_key"}),
    # Only for the static ObservationDB._normalize_location_precision (below).
    "database.models": frozenset({"ObservationDB"}),
}

#: The single allowlisted database attribute.
ALLOWED_DB_ATTRIBUTES = frozenset({"ObservationDB._normalize_location_precision"})


def _client_methods() -> frozenset[str]:
    import utils.cloud_sync as cloud_sync

    names: set[str] = set()
    for klass in cloud_sync.SporelyCloudClient.__mro__:
        if klass is object:
            continue
        names.update(name for name, value in vars(klass).items() if callable(value))
    return frozenset(n for n in names if not (n.startswith("__") and n.endswith("__")))


def _check(root: Path = ROOT, modules: frozenset[str] = PURE_MODULES, **overrides) -> list[str]:
    kwargs = dict(
        pure_dependencies=PURE_LEAF_OWNERS,
        stdlib=STDLIB,
        external_names=EXTERNAL_NAMES,
        allowed_db_attributes=ALLOWED_DB_ATTRIBUTES,
        client_methods=overrides.pop("client_methods", None) or _client_methods(),
    )
    kwargs.update(overrides)
    return check_purity(root, modules, **kwargs)


def test_pure_package_has_its_modules():
    assert {
        "utils.cloud_sync_impl.reconciliation.identity",
        "utils.cloud_sync_impl.reconciliation.asymmetry",
        "utils.cloud_sync_impl.reconciliation.measurements",
        "utils.cloud_sync_impl.reconciliation.images",
    } <= PURE_MODULES


def test_reconciliation_package_is_pure():
    assert _check() == []


def test_pure_modules_are_imported_by_no_stateful_code_path_upward():
    """Nothing pure imports a stateful owner (the purity import rule enforces it)."""
    from tests.test_cloud_sync_impl_import_direction import OWNER_LAYERS

    stateful = {m for m in OWNER_LAYERS if m not in PURE_MODULES | PURE_LEAF_OWNERS}
    assert stateful, "expected stateful owners"
    # Any import of a stateful owner from pure code is reported as a non-allowlisted import.
    assert not any(name in EXTERNAL_NAMES for name in stateful)


# --- synthetic cases ---------------------------------------------------------

def _pkg(tmp_path: Path, source: str) -> Path:
    pkg = tmp_path / "utils" / "cloud_sync_impl" / "reconciliation"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text("")
    (pkg / "mod.py").write_text(textwrap.dedent(source))
    (tmp_path / "utils" / "cloud_sync_impl" / "common.py").write_text("")
    return tmp_path


def _synthetic(tmp_path: Path, source: str) -> list[str]:
    root = _pkg(tmp_path, source)
    return _check(
        root,
        frozenset({"utils.cloud_sync_impl.reconciliation", "utils.cloud_sync_impl.reconciliation.mod"}),
        client_methods=frozenset({"get_observation", "_patch"}),
    )


def test_clean_module_passes(tmp_path):
    assert _synthetic(tmp_path, """
        from __future__ import annotations
        import json
        from datetime import datetime
        from database.models import ObservationDB
        from utils.cloud_sync_impl.common import _safe_int
        from . import mod

        def f(value):
            return ObservationDB._normalize_location_precision(value), json.dumps({}).get
        """) == []


def test_sqlite_import_fails(tmp_path):
    assert any("imports `sqlite3`" in v for v in _synthetic(tmp_path, "import sqlite3\n"))


def test_function_local_qt_import_fails(tmp_path):
    violations = _synthetic(tmp_path, "def f():\n    from PySide6 import QtCore\n    return QtCore\n")
    assert any("from `PySide6`" in v for v in violations)


def test_stateful_owner_import_fails(tmp_path):
    violations = _synthetic(tmp_path, "from utils.cloud_sync_impl.baseline import _store_remote_snapshot\n")
    assert any("_store_remote_snapshot" in v for v in violations)


def test_facade_import_fails(tmp_path):
    violations = _synthetic(tmp_path, "from utils import cloud_sync\n")
    assert any("from `utils`" in v for v in violations)


def test_other_database_name_fails(tmp_path):
    violations = _synthetic(tmp_path, "from database.reverse_location_lookup import normalize_country_code\n")
    assert any("normalize_country_code" in v for v in violations)


def test_get_connection_and_settings_calls_fail(tmp_path):
    violations = _synthetic(tmp_path, """
        def f(get_connection, SettingsDB, get_app_settings, update_app_settings):
            get_connection()
            SettingsDB.get_setting("k")
            get_app_settings()
            update_app_settings({})
        """)
    for needle in ("calls `get_connection`", "uses `SettingsDB.get_setting`", "calls `get_app_settings`",
                   "calls `update_app_settings`", "calls `.get_setting`"):
        assert any(needle in v for v in violations), needle


def test_other_db_method_fails_even_on_allowlisted_class(tmp_path):
    violations = _synthetic(tmp_path, """
        from database.models import ObservationDB

        def f(obs_id):
            return ObservationDB.get_observation(obs_id)
        """)
    assert any("uses `ObservationDB.get_observation`" in v for v in violations)


def test_bare_db_name_fails(tmp_path):
    violations = _synthetic(tmp_path, """
        from database.models import ObservationDB

        def f():
            return ObservationDB
        """)
    assert any("uses `ObservationDB`" in v for v in violations)


def test_client_method_call_fails_on_any_receiver(tmp_path):
    violations = _synthetic(tmp_path, "def f(remote):\n    return remote.get_observation('x')\n")
    assert any("client method `.get_observation`" in v for v in violations)


def test_cursor_and_file_io_fail(tmp_path):
    violations = _synthetic(tmp_path, """
        def f(conn, path):
            conn.execute("select 1")
            open(path)
            return path.read_text()
        """)
    for needle in ("calls `.execute`", "calls `open`", "calls `.read_text`"):
        assert any(needle in v for v in violations), needle
