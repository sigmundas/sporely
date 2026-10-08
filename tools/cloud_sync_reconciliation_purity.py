"""Purity rules for the cloud-sync pure reconciliation code (Stage S4).

Pure modules perform no SQLite or database reads or writes, no settings reads
or writes, no cloud client calls or other I/O, and import no Qt. The rules are
checked on the AST, so they hold for code paths no test executes:

* Imports: ``__future__``, an enumerated set of standard-library modules,
  other pure modules, and an explicit allowlist of names from other project
  modules (each a value-only helper). Anything else -- Qt, ``sqlite3``,
  ``requests``, ``database`` beyond its allowlisted name, the facade, or a
  stateful owner -- is a violation. Function-local imports count.
* Calls: ``get_connection``, ``SettingsDB``, ``get_app_settings``,
  ``update_app_settings``, ``open`` and dynamic machinery are rejected by name.
  Any ``*DB`` name, and any name bound from a ``database`` module (aliases
  included), may be used only as an allowlisted attribute of its imported name
  (only ``ObservationDB._normalize_location_precision``). A call of any attribute
  named like a ``SporelyCloudClient`` method, or of a SQLite/settings method, is
  rejected whatever its receiver.

``tests/test_cloud_sync_reconciliation_purity.py`` holds the module list and the
allowlists. A later stage may extend the module list; it must not weaken the
rules to admit a stateful symbol -- that symbol belongs in a stateful owner.
"""

from __future__ import annotations

import ast
from pathlib import Path

FORBIDDEN_CALL_NAMES = frozenset({
    "get_connection", "SettingsDB", "get_app_settings", "update_app_settings",
    "open", "__import__", "eval", "exec", "compile", "globals", "input", "print",
})
#: SQLite cursor/connection and settings-store methods; rejected on any receiver.
FORBIDDEN_CALL_ATTRS = frozenset({
    "get_connection", "execute", "executemany", "executescript", "cursor", "commit",
    "rollback", "get_setting", "set_setting", "delete_setting", "get_app_settings",
    "update_app_settings", "read_text", "read_bytes", "write_text", "write_bytes",
})


def _module_name(root: Path, path: Path) -> str:
    parts = list(path.relative_to(root).with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _resolve(module: str, is_package: bool, node: ast.ImportFrom) -> str:
    if not node.level:
        return node.module or ""
    package = module.split(".") if is_package else module.split(".")[:-1]
    base = package[: len(package) - (node.level - 1)]
    return ".".join([*base, node.module] if node.module else base)


def check_purity(
    root: Path,
    pure_modules: frozenset[str],
    *,
    pure_dependencies: frozenset[str] = frozenset(),
    stdlib: frozenset[str],
    external_names: dict[str, frozenset[str]],
    allowed_db_attributes: frozenset[str],
    client_methods: frozenset[str],
) -> list[str]:
    """Return violations (empty when every pure module obeys the rules).

    ``pure_modules`` are checked and may import each other; ``pure_dependencies``
    are further pure modules (also checked) they may import.
    """
    checked = pure_modules | pure_dependencies
    violations: list[str] = []
    for module in sorted(checked):
        path = root.joinpath(*module.split("."))
        is_package = (path / "__init__.py").is_file()
        file = path / "__init__.py" if is_package else path.with_suffix(".py")
        if not file.is_file():
            violations.append(f"{module}: pure module not found")
            continue
        tree = ast.parse(file.read_text(encoding="utf-8"), filename=str(file))
        db_names = _database_bindings(tree, module, is_package)

        def _real(name: str) -> str:
            return db_names.get(name, name)

        def _is_db(name: str) -> bool:
            return name in db_names or name.endswith("DB")

        # ``X`` inside an allowlisted ``X.attr`` is not a bare database use. The
        # allowlist is matched on the imported (real) name, so an alias such as
        # ``from database.models import ObservationDB as Obs`` gains nothing.
        allowlisted_receivers = {
            id(node.value)
            for node in ast.walk(tree)
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name)
            and _is_db(node.value.id)
            and f"{_real(node.value.id)}.{node.attr}" in allowed_db_attributes
        }
        for node in ast.walk(tree):
            where = f"{module}:{getattr(node, 'lineno', 0)}"
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.split(".")[0] not in stdlib:
                        violations.append(f"{where}: imports `{alias.name}`")
            elif isinstance(node, ast.ImportFrom):
                target = _resolve(module, is_package, node)
                if target == "__future__" or (not node.level and target.split(".")[0] in stdlib):
                    continue
                if target in checked:
                    continue
                allowed = external_names.get(target, frozenset())
                for alias in node.names:
                    if f"{target}.{alias.name}" in checked:
                        continue
                    if alias.name not in allowed:
                        violations.append(f"{where}: imports `{alias.name}` from `{target}`")
            elif isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Name) and func.id in FORBIDDEN_CALL_NAMES:
                    violations.append(f"{where}: calls `{func.id}`")
                elif isinstance(func, ast.Attribute):
                    if func.attr in FORBIDDEN_CALL_ATTRS:
                        violations.append(f"{where}: calls `.{func.attr}`")
                    elif func.attr in client_methods:
                        violations.append(f"{where}: calls client method `.{func.attr}`")
            elif isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) \
                    and _is_db(node.value.id) and id(node.value) not in allowlisted_receivers:
                violations.append(f"{where}: uses `{_real(node.value.id)}.{node.attr}`")
            elif isinstance(node, ast.Name) and _is_db(node.id) \
                    and id(node) not in allowlisted_receivers:
                violations.append(f"{where}: uses `{_real(node.id)}`")
    return violations


def _database_bindings(tree: ast.Module, module: str, is_package: bool) -> dict[str, str]:
    """Local name -> imported name for every name bound from a ``database`` module.

    Covers aliases and function-local imports, so a renamed database class is
    still recognised as one.
    """
    bindings: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            target = _resolve(module, is_package, node)
            if target.split(".")[0] == "database":
                for alias in node.names:
                    bindings[alias.asname or alias.name] = alias.name
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".")[0] == "database":
                    bindings[alias.asname or alias.name.split(".")[0]] = alias.name
    return bindings
