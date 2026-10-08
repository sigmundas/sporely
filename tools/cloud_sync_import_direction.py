"""Import-direction rules for the cloud-sync owner package.

Owners under ``utils/cloud_sync_impl/`` must not import the facade
(``utils.cloud_sync``) at any nesting level -- function-local and lazy imports
included -- unless the owner is on an explicit, justified allowlist. The one
exception is an ``if TYPE_CHECKING:`` import whose bound names are used only in
annotations of a module with ``from __future__ import annotations``: it never
executes.

Between owners, dependencies must point strictly downward through a declared
layer map, so the owner graph is acyclic. Every owner module must be declared
in the map. ``tests/test_cloud_sync_impl_import_direction.py`` holds the map
and the allowlist; later extraction stages extend them as owners appear.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

FACADE = "utils.cloud_sync"
OWNERS = "utils.cloud_sync_impl"


@dataclass(frozen=True)
class ImportRef:
    module: str  # importing owner module
    target: str  # absolute imported module
    names: tuple[str, ...]  # names bound by the import
    lineno: int
    type_checking: bool  # directly under a module-level ``if TYPE_CHECKING:``
    submodule_guess: bool = False  # ``from pkg import name`` read as ``pkg.name``


def _module_name(root: Path, path: Path) -> str:
    parts = list(path.relative_to(root).with_suffix("").parts)
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def _is_type_checking_test(test: ast.expr) -> bool:
    if isinstance(test, ast.Name):
        return test.id == "TYPE_CHECKING"
    return isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING" \
        and isinstance(test.value, ast.Name) and test.value.id == "typing"


def _resolve(module: str, is_package: bool, node: ast.ImportFrom) -> str:
    if not node.level:
        return node.module or ""
    package = module.split(".") if is_package else module.split(".")[:-1]
    base = package[: len(package) - (node.level - 1)]
    return ".".join([*base, node.module] if node.module else base)


def _imports(module: str, tree: ast.Module, is_package: bool) -> list[ImportRef]:
    guarded: set[int] = set()
    for stmt in tree.body:
        if isinstance(stmt, ast.If) and _is_type_checking_test(stmt.test):
            for inner in stmt.body:
                if isinstance(inner, (ast.Import, ast.ImportFrom)):
                    guarded.add(id(inner))
    refs: list[ImportRef] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                refs.append(ImportRef(module, alias.name, (alias.asname or alias.name.split(".")[0],),
                                      node.lineno, id(node) in guarded))
        elif isinstance(node, ast.ImportFrom):
            target = _resolve(module, is_package, node)
            for alias in node.names:
                # ``from utils import cloud_sync`` imports the submodule.
                full = f"{target}.{alias.name}" if target else alias.name
                refs.append(ImportRef(module, target, (alias.asname or alias.name,), node.lineno,
                                      id(node) in guarded))
                refs.append(ImportRef(module, full, (alias.asname or alias.name,), node.lineno,
                                      id(node) in guarded, submodule_guess=True))
        elif isinstance(node, ast.Call):
            # importlib.import_module("...") / __import__("...") with a literal.
            func = node.func
            name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
            if name in {"import_module", "__import__"} and node.args \
                    and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
                refs.append(ImportRef(module, node.args[0].value, (), node.lineno, False))
        elif isinstance(node, ast.Subscript):
            # sys.modules["utils.cloud_sync"]
            value = node.value
            if isinstance(value, ast.Attribute) and value.attr == "modules" \
                    and isinstance(node.slice, ast.Constant) and isinstance(node.slice.value, str):
                refs.append(ImportRef(module, node.slice.value, (), node.lineno, False))
    return refs


def _runtime_uses(tree: ast.Module, names: set[str], skip: set[int]) -> list[tuple[str, int]]:
    """Uses of ``names`` outside annotations and outside the skipped import nodes."""
    annotation_nodes: set[int] = set()
    for node in ast.walk(tree):
        annotations = []
        if isinstance(node, ast.arg):
            annotations.append(node.annotation)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            annotations.append(node.returns)
        elif isinstance(node, ast.AnnAssign):
            annotations.append(node.annotation)
        for annotation in annotations:
            if annotation is not None:
                annotation_nodes.update(id(sub) for sub in ast.walk(annotation))
    uses = []
    for node in ast.walk(tree):
        if id(node) in skip:
            continue
        if isinstance(node, ast.Name) and node.id in names and id(node) not in annotation_nodes:
            uses.append((node.id, node.lineno))
    return uses


def _has_future_annotations(tree: ast.Module) -> bool:
    return any(
        isinstance(stmt, ast.ImportFrom) and stmt.module == "__future__"
        and any(a.name == "annotations" for a in stmt.names)
        for stmt in tree.body
    )


def check_import_direction(
    root: Path,
    *,
    owners: str = OWNERS,
    facade: str = FACADE,
    layers: dict[str, int],
    facade_allowlist: frozenset[str] = frozenset(),
) -> list[str]:
    """Return human-readable violations (empty when the rules hold)."""
    pkg_dir = root.joinpath(*owners.split("."))
    if not (pkg_dir / "__init__.py").is_file():
        return []
    violations: list[str] = []
    for path in sorted(pkg_dir.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        module = _module_name(root, path)
        is_package = path.name == "__init__.py"
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        if module not in layers and not is_package:
            violations.append(f"{module}: owner module is not declared in the layer map")
        refs = _imports(module, tree, is_package)
        guarded_facade_names: set[str] = set()
        for ref in refs:
            facade_import = ref.target == facade or ref.target.startswith(facade + ".")
            if facade_import:
                if module in facade_allowlist:
                    continue
                if ref.type_checking:
                    guarded_facade_names.update(ref.names)
                    continue
                violations.append(f"{module}:{ref.lineno}: imports the facade `{ref.target}`")
                continue
            if ref.target == owners or ref.target.startswith(owners + "."):
                target = ref.target
                target_path = root.joinpath(*target.split("."))
                if ref.submodule_guess and not (
                    target_path.with_suffix(".py").is_file() or (target_path / "__init__.py").is_file()
                ):
                    continue
                if target == module:
                    continue
                if is_package:
                    violations.append(f"{module}:{ref.lineno}: package __init__ imports owner `{target}`")
                    continue
                if target not in layers:
                    # ``from utils.cloud_sync_impl import x`` also yields the package itself.
                    if target == owners:
                        continue
                    violations.append(f"{module}:{ref.lineno}: imports undeclared owner `{target}`")
                    continue
                if module in layers and layers[target] >= layers[module]:
                    violations.append(
                        f"{module}:{ref.lineno}: imports `{target}` (layer {layers[target]}) "
                        f"from layer {layers[module]}; owner imports must point strictly downward"
                    )
        if guarded_facade_names:
            if not _has_future_annotations(tree):
                violations.append(
                    f"{module}: TYPE_CHECKING facade import without `from __future__ import annotations`"
                )
            guard_nodes = {
                id(sub)
                for stmt in tree.body
                if isinstance(stmt, ast.If) and _is_type_checking_test(stmt.test)
                for sub in ast.walk(stmt)
            }
            for name, lineno in _runtime_uses(tree, guarded_facade_names, guard_nodes):
                violations.append(
                    f"{module}:{lineno}: TYPE_CHECKING facade name `{name}` is used outside annotations"
                )
    return violations
