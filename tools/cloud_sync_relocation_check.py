"""Relocation-equivalence check for the cloud-sync extraction.

For every top-level function, class, method and module-level statement removed
from the facade (``utils/cloud_sync.py``) between a base and a candidate, the
check locates the new definition under the owner package
(``utils/cloud_sync_impl/``) and reports one verdict:

* ``identical``    -- proven equivalent (AST, free-name origins, shared state,
  facade identity and, for client methods, MRO resolution);
* ``differs``      -- the definition or a free-name origin changed (diff shown);
* ``needs review`` -- equivalence cannot be proven automatically;
* ``missing``      -- no relocated definition (or facade export) was found.

The check fails closed: anything it cannot prove is never ``identical``.

Usage (from the repository root)::

    .venv/bin/python tools/cloud_sync_relocation_check.py BASE CANDIDATE

``BASE``/``CANDIDATE`` are git revisions, an existing directory holding a
source tree, or ``WORKTREE`` for the current working tree. Exit status is 0
when every reported entry is ``identical`` (or nothing moved), else 1.

Base and candidate run in separate processes, so free names are compared by
origin: a function or class by module, qualname and an AST hash; a module by
name; an immutable literal by ``repr``; anything else must be the single
shared instance bound by the facade. See the Stage S2 brief in
``docs/plans/active/2026-10-07-cloud-sync-extraction-and-orchestration.md``.
"""

from __future__ import annotations

import argparse
import ast
import copy
import difflib
import hashlib
import json
import os
import subprocess
import sys
import symtable
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

IDENTICAL = "identical"
DIFFERS = "differs"
NEEDS_REVIEW = "needs review"
MISSING = "missing"

DEFAULT_FACADE = "utils.cloud_sync"
DEFAULT_OWNERS = "utils.cloud_sync_impl"
WORKTREE = "WORKTREE"

#: Names whose use makes behavior depend on where the code lives or on
#: dynamic lookup. Their presence always yields ``needs review``.
_DYNAMIC_NAMES = {
    "globals", "locals", "vars", "__import__", "eval", "exec", "importlib",
    "__name__", "__file__", "__spec__", "__package__", "__loader__",
    "__builtins__", "__module__", "__qualname__", "__class__", "inspect",
}
_DYNAMIC_ATTRS = {"modules", "_getframe", "__globals__", "__module__", "__qualname__", "__dict__"}
_ATTR_BUILTINS = {"getattr", "setattr", "delattr", "hasattr"}
_DESCRIPTOR_DUNDERS = {
    "__get__", "__set__", "__delete__", "__set_name__", "__init_subclass__",
    "__class_getitem__", "__getattr__", "__getattribute__", "__new__",
}
#: Class attributes that only record where the class was defined.
_CLASS_METADATA_ATTRS = {
    "__module__", "__qualname__", "__dict__", "__weakref__", "__doc__",
    "__firstlineno__", "__static_attributes__", "__annotations__",
    "__annotate__", "__annotate_func__", "__annotations_cache__", "__orig_bases__",
    "__parameters__",
}


# --------------------------------------------------------------------------
# Static model


@dataclass
class Definition:
    key: str  # e.g. "def:foo", "class:Foo", "method:Client.bar", "assign:X", "import:...", "stmt:<hash>"
    module: str
    node: ast.stmt
    container: str | None = None  # enclosing class name for methods
    names: tuple[str, ...] = ()  # names bound at module level

    @property
    def qualname(self) -> str:
        name = self.key.split(":", 1)[1]
        return name


@dataclass
class ModuleSource:
    name: str
    path: Path
    tree: ast.Module
    future_annotations: bool


@dataclass
class Entry:
    key: str
    verdict: str
    base_module: str
    candidate_module: str | None = None
    candidate_qualname: str | None = None
    reasons: list[str] = field(default_factory=list)
    diff: str = ""
    depends_on: set[str] = field(default_factory=set)

    def to_json(self) -> dict:
        return {
            "key": self.key,
            "verdict": self.verdict,
            "base_module": self.base_module,
            "candidate_module": self.candidate_module,
            "candidate_qualname": self.candidate_qualname,
            "reasons": self.reasons,
            "diff": self.diff,
        }


def _dump(node: ast.AST) -> str:
    return ast.dump(node, annotate_fields=True, include_attributes=False)


def _hash(node: ast.AST) -> str:
    return hashlib.sha256(_dump(node).encode("utf-8")).hexdigest()[:20]


def _has_future_annotations(tree: ast.Module) -> bool:
    for stmt in tree.body:
        if isinstance(stmt, ast.ImportFrom) and stmt.module == "__future__":
            if any(alias.name == "annotations" for alias in stmt.names):
                return True
    return False


def _module_path(root: Path, module: str) -> Path | None:
    base = root.joinpath(*module.split("."))
    if base.with_suffix(".py").is_file():
        return base.with_suffix(".py")
    if (base / "__init__.py").is_file():
        return base / "__init__.py"
    return None


def _load_module(root: Path, module: str) -> ModuleSource | None:
    path = _module_path(root, module)
    if path is None:
        return None
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    return ModuleSource(module, path, tree, _has_future_annotations(tree))


def _owner_modules(root: Path, package: str) -> list[ModuleSource]:
    pkg_dir = root.joinpath(*package.split("."))
    if not (pkg_dir / "__init__.py").is_file():
        return []
    modules = []
    for path in sorted(pkg_dir.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        rel = path.relative_to(root).with_suffix("")
        parts = list(rel.parts)
        if parts[-1] == "__init__":
            parts = parts[:-1]
        source = _load_module(root, ".".join(parts))
        if source is not None:
            modules.append(source)
    return modules


def _bound_names(stmt: ast.stmt) -> tuple[str, ...]:
    names: list[str] = []
    if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        names.append(stmt.name)
    elif isinstance(stmt, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
        targets = stmt.targets if isinstance(stmt, ast.Assign) else [stmt.target]
        for target in targets:
            for node in ast.walk(target):
                if isinstance(node, ast.Name):
                    names.append(node.id)
    elif isinstance(stmt, (ast.Import, ast.ImportFrom)):
        for alias in stmt.names:
            names.append(alias.asname or alias.name.split(".")[0])
    return tuple(names)


def _definitions(source: ModuleSource) -> dict[str, Definition]:
    defs: dict[str, Definition] = {}
    for stmt in source.tree.body:
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
            key = f"def:{stmt.name}"
        elif isinstance(stmt, ast.ClassDef):
            key = f"class:{stmt.name}"
            for item in stmt.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    mkey = f"method:{stmt.name}.{item.name}"
                    defs[mkey] = Definition(mkey, source.name, item, container=stmt.name, names=(item.name,))
        elif isinstance(stmt, (ast.Assign, ast.AnnAssign)) and _bound_names(stmt):
            key = "assign:" + ",".join(_bound_names(stmt))
        elif isinstance(stmt, (ast.Import, ast.ImportFrom)):
            key = "import:" + _hash(stmt)
        else:
            key = "stmt:" + _hash(stmt)
        if key in defs:
            # A rebinding at module level: keep both, distinguished by AST.
            key = f"{key}#{_hash(stmt)}"
        defs[key] = Definition(key, source.name, stmt, names=_bound_names(stmt))
    return defs


def _class_body_without_methods(node: ast.ClassDef) -> str:
    stripped = copy.deepcopy(node)
    stripped.body = [
        item for item in stripped.body
        if not isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]
    return _dump(stripped)


def removed_definitions(base_facade: ModuleSource, cand_facade: ModuleSource | None) -> list[Definition]:
    base_defs = _definitions(base_facade)
    cand_defs = _definitions(cand_facade) if cand_facade is not None else {}
    cand_stmt_dumps = {_dump(d.node) for d in cand_defs.values() if d.key.startswith(("import:", "stmt:"))}
    removed: list[Definition] = []
    moved_classes = {k.split(":", 1)[1] for k in base_defs if k.startswith("class:") and k not in cand_defs}
    for key, definition in base_defs.items():
        if key.startswith(("import:", "stmt:")):
            if _dump(definition.node) not in cand_stmt_dumps:
                removed.append(definition)
            continue
        if key.startswith("method:") and definition.container in moved_classes:
            continue  # covered by the whole-class entry
        if key not in cand_defs:
            removed.append(definition)
    return removed


# --------------------------------------------------------------------------
# Free names


class _StripAnnotations(ast.NodeTransformer):
    def visit_arg(self, node: ast.arg):
        node.annotation = None
        return node

    def visit_FunctionDef(self, node):
        node.returns = None
        self.generic_visit(node)
        return node

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_AnnAssign(self, node: ast.AnnAssign):
        node.annotation = ast.Constant(value=None)
        self.generic_visit(node)
        return node


def _annotation_names(node: ast.AST) -> set[str]:
    names: set[str] = set()
    for sub in ast.walk(node):
        annotations: list[ast.AST | None] = []
        if isinstance(sub, ast.arg):
            annotations.append(sub.annotation)
        elif isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef)):
            annotations.append(sub.returns)
        elif isinstance(sub, ast.AnnAssign):
            annotations.append(sub.annotation)
        for annotation in annotations:
            if annotation is None:
                continue
            for leaf in ast.walk(annotation):
                if isinstance(leaf, ast.Name):
                    names.add(leaf.id)
    return names


def _symtable_reads(node: ast.stmt) -> set[str]:
    """Global/free names the statement reads, via the compiler's own symbol table."""
    module = ast.Module(body=[node], type_ignores=[])
    source = ast.unparse(ast.fix_missing_locations(module))
    table = symtable.symtable(source, "<relocated>", "exec")
    reads: set[str] = set()

    def walk(tab: symtable.SymbolTable, top: bool) -> None:
        for sym in tab.get_symbols():
            if not sym.is_referenced():
                continue
            if top or sym.is_global() or sym.is_declared_global():
                reads.add(sym.get_name())
        for child in tab.get_children():
            walk(child, False)

    walk(table, True)
    return reads


def free_names(node: ast.stmt) -> tuple[set[str], set[str]]:
    """Return (runtime reads, annotation-only names) of a definition."""
    stripped = _StripAnnotations().visit(copy.deepcopy(node))
    runtime = _symtable_reads(stripped)
    annotation_only = _annotation_names(node) - runtime
    return runtime, annotation_only


def dynamic_lookup_reasons(node: ast.stmt, *, relocating_method: bool, facade: str) -> list[str]:
    reasons: list[str] = []
    facade_parent, _, facade_leaf = facade.rpartition(".")
    for sub in ast.walk(node):
        if isinstance(sub, ast.Global):
            reasons.append(f"`global {', '.join(sub.names)}` statement")
        elif isinstance(sub, ast.Name) and sub.id in _DYNAMIC_NAMES:
            reasons.append(f"dynamic or location-sensitive name `{sub.id}`")
        elif isinstance(sub, ast.Attribute) and sub.attr in _DYNAMIC_ATTRS:
            reasons.append(f"dynamic attribute `.{sub.attr}`")
        elif isinstance(sub, ast.Call) and isinstance(sub.func, ast.Name):
            if sub.func.id in _ATTR_BUILTINS and sub.args and isinstance(sub.args[0], ast.Name) \
                    and sub.args[0].id not in {"self", "cls"}:
                reasons.append(f"`{sub.func.id}()` on name `{sub.args[0].id}` (possible module lookup)")
            if sub.func.id == "super" and not sub.args:
                reasons.append("zero-argument `super()` depends on the defining class")
        elif isinstance(sub, ast.Import):
            for alias in sub.names:
                if alias.name == facade or alias.name.startswith(facade + "."):
                    reasons.append(f"late-bound facade import `import {alias.name}`")
        elif isinstance(sub, ast.ImportFrom) and sub.module:
            if sub.module == facade or (
                sub.module == facade_parent and any(a.name == facade_leaf for a in sub.names)
            ):
                reasons.append(f"late-bound facade import `from {sub.module} import ...`")
        elif isinstance(sub, ast.ClassDef):
            if any(kw.arg == "metaclass" for kw in sub.keywords):
                reasons.append(f"class `{sub.name}` sets a metaclass")
            for item in sub.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name in _DESCRIPTOR_DUNDERS:
                    reasons.append(f"descriptor/metaclass-sensitive `{sub.name}.{item.name}`")
        if relocating_method:
            if isinstance(sub, ast.Attribute) and sub.attr.startswith("__") and not sub.attr.endswith("__"):
                reasons.append(f"private name `{sub.attr}` is mangled with the defining class name")
            if isinstance(sub, ast.Name) and sub.id.startswith("__") and not sub.id.endswith("__"):
                reasons.append(f"private name `{sub.id}` is mangled with the defining class name")
    if relocating_method and isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) \
            and node.name in _DESCRIPTOR_DUNDERS:
        reasons.append(f"descriptor/metaclass-sensitive method `{node.name}`")
    return sorted(set(reasons))


# --------------------------------------------------------------------------
# Runtime probe (runs in a separate process per side)

_PROBE = r'''
import ast, builtins, hashlib, importlib, json, re, sys, types
request = json.loads(sys.stdin.read())
sys.path.insert(0, request["root"])
internal = set(request["internal"])
facade = request["facade"]
owners_pkg = request["owners"]

def dump_hash(node):
    return hashlib.sha256(ast.dump(node, annotate_fields=True, include_attributes=False).encode()).hexdigest()[:20]

_trees = {}
def internal_hash(module, qualname):
    mod = sys.modules.get(module)
    path = getattr(mod, "__file__", None)
    if not path:
        return None
    if path not in _trees:
        with open(path, encoding="utf-8") as fh:
            _trees[path] = ast.parse(fh.read())
    body = _trees[path].body
    node = None
    for part in qualname.split("."):
        node = next((n for n in body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and n.name == part), None)
        if node is None:
            return None
        body = node.body
    return dump_hash(node)

modules = {}
errors = []
for name in request["import"]:
    try:
        modules[name] = importlib.import_module(name)
    except Exception as exc:
        errors.append(f"import {name}: {type(exc).__name__}: {exc}")

def internal_modules():
    return [m for n, m in sorted(sys.modules.items()) if n == facade or n == owners_pkg or n.startswith(owners_pkg + ".")]

_IMMUTABLE = (type(None), bool, int, float, complex, str, bytes, type(Ellipsis))
def literal(value):
    if isinstance(value, _IMMUTABLE):
        return True
    if isinstance(value, (tuple, frozenset)):
        return all(literal(v) for v in value)
    return False

def origin(module_name, name):
    mod = sys.modules.get(module_name)
    if mod is None:
        return {"kind": "error", "detail": f"module {module_name} not imported"}
    if name in mod.__dict__:
        obj = mod.__dict__[name]
    elif hasattr(builtins, name):
        return {"kind": "builtin", "name": name}
    else:
        return {"kind": "unbound"}
    if isinstance(obj, types.ModuleType):
        return {"kind": "module", "name": obj.__name__}
    if literal(obj):
        return {"kind": "literal", "repr": repr(obj)}
    if isinstance(obj, re.Pattern):
        return {"kind": "literal", "repr": f"re.compile({obj.pattern!r}, {obj.flags})"}
    if isinstance(obj, (types.FunctionType, type, types.BuiltinFunctionType)):
        mod_of = getattr(obj, "__module__", None)
        qual = getattr(obj, "__qualname__", None)
        if mod_of in internal or (mod_of or "").startswith(owners_pkg + "."):
            h = internal_hash(mod_of, qual) if qual else None
            h = h or "unhashable"
        else:
            try:
                import inspect, textwrap
                h = dump_hash(ast.parse(textwrap.dedent(inspect.getsource(obj))))
            except Exception:
                # No Python source: identify compiled code by the interpreter
                # (built-in modules) or by the extension file's bytes. Anything
                # else stays an unproven placeholder.
                h = "nosource"
                src_mod = sys.modules.get(mod_of or "")
                src_file = getattr(src_mod, "__file__", None) if src_mod else None
                if src_mod is not None and not src_file:
                    h = "interpreter:" + sys.version
                elif src_file and src_file.endswith((".so", ".pyd")):
                    with open(src_file, "rb") as fh:
                        h = "ext:" + hashlib.sha256(fh.read()).hexdigest()[:20]
        return {"kind": "def", "module": mod_of, "qualname": qual, "hash": h}
    # Any other object is state: it must be one shared instance.
    tname = f"{type(obj).__module__}.{type(obj).__qualname__}"
    home = None
    for other_name, other in sorted(sys.modules.items()):
        if other is None or other in internal_modules():
            continue
        try:
            if getattr(other, name, None) is obj:
                home = other_name
                break
        except Exception:
            continue
    result = {"kind": "state", "type": tname, "name": name, "home": home or "<internal>"}
    if home is None:
        fac = sys.modules.get(facade)
        holders = []
        conflicting = []
        for m in internal_modules():
            if name in m.__dict__:
                (holders if m.__dict__[name] is obj else conflicting).append(m.__name__)
        shared = fac is not None and fac.__dict__.get(name, None) is obj and not conflicting
        result["shared"] = shared
        # The initializer of the one module-level assignment that created it.
        inits = []
        for m in internal_modules():
            if m.__dict__.get(name, None) is not obj or not getattr(m, "__file__", None):
                continue
            path = m.__file__
            if path not in _trees:
                with open(path, encoding="utf-8") as fh:
                    _trees[path] = ast.parse(fh.read())
            for stmt in _trees[path].body:
                targets = stmt.targets if isinstance(stmt, ast.Assign) else [stmt.target] if isinstance(stmt, (ast.AnnAssign, ast.AugAssign)) else []
                if any(isinstance(n, ast.Name) and n.id == name for t in targets for n in ast.walk(t)):
                    inits.append(dump_hash(stmt))
        result["init_hash"] = inits[0] if len(inits) == 1 else None
        result["holders"] = holders
        result["conflicting"] = conflicting
        if isinstance(obj, __import__("logging").Logger):
            result["logger_name"] = obj.name
    return result

out = {"errors": errors, "origins": {}, "identity": {}, "classes": {}, "methods": {}}
for module_name, name in request["origins"]:
    out["origins"][f"{module_name}:{name}"] = origin(module_name, name)
for facade_name, owner_module, owner_name in request["identity"]:
    fac = sys.modules.get(facade)
    own = sys.modules.get(owner_module)
    if fac is None or own is None or facade_name not in fac.__dict__ or owner_name not in own.__dict__:
        out["identity"][f"{facade_name}|{owner_module}"] = None
    else:
        out["identity"][f"{facade_name}|{owner_module}"] = fac.__dict__[facade_name] is own.__dict__[owner_name]
def resolution(cls):
    table = {}
    names = set()
    for klass in cls.__mro__:
        names.update(klass.__dict__)
    for attr in sorted(names):
        for klass in cls.__mro__:
            if attr in klass.__dict__:
                table[attr] = f"{klass.__module__}:{klass.__qualname__}"
                break
    return table
for module_name, class_name in request["classes"]:
    mod = sys.modules.get(module_name)
    cls = getattr(mod, class_name, None) if mod else None
    out["classes"][f"{module_name}:{class_name}"] = resolution(cls) if isinstance(cls, type) else None
for module_name, class_name, attr, owner_module, owner_qualname in request["methods"]:
    mod = sys.modules.get(module_name)
    cls = getattr(mod, class_name, None) if mod else None
    own = sys.modules.get(owner_module)
    target = own
    for part in owner_qualname.split("."):
        target = (target.__dict__ if hasattr(target, "__dict__") else {}).get(part) if target is not None else None
    resolved = None
    if isinstance(cls, type):
        for klass in cls.__mro__:
            if attr in klass.__dict__:
                resolved = klass.__dict__[attr]
                break
    out["methods"][f"{class_name}.{attr}"] = target is not None and resolved is target
print(json.dumps(out))
'''


def _probe(root: Path, request: dict) -> dict:
    request = dict(request, root=str(root))
    env = dict(os.environ, PYTHONHASHSEED="0", QT_QPA_PLATFORM=os.environ.get("QT_QPA_PLATFORM", "offscreen"))
    proc = subprocess.run(
        [sys.executable, "-c", _PROBE],
        input=json.dumps(request), capture_output=True, text=True, cwd=str(root), env=env,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"probe failed in {root}:\n{proc.stderr[-4000:]}")
    return json.loads(proc.stdout.strip().splitlines()[-1])


# --------------------------------------------------------------------------
# Comparison


def _locate(definition: Definition, owners: list[ModuleSource]) -> list[tuple[ModuleSource, str, ast.stmt]]:
    """Candidate definitions matching a removed facade definition by kind and name."""
    kind = definition.key.split(":", 1)[0]
    matches: list[tuple[ModuleSource, str, ast.stmt]] = []
    for owner in owners:
        for stmt in owner.tree.body:
            if kind in {"def", "class"} and isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                if stmt.name == definition.node.name and type(stmt) is type(definition.node):
                    matches.append((owner, stmt.name, stmt))
            elif kind == "method":
                if isinstance(stmt, ast.ClassDef):
                    for item in stmt.body:
                        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name == definition.node.name:
                            matches.append((owner, f"{stmt.name}.{item.name}", item))
                elif isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)) and stmt.name == definition.node.name:
                    matches.append((owner, stmt.name, stmt))
            elif kind == "assign" and isinstance(stmt, (ast.Assign, ast.AnnAssign)):
                if _bound_names(stmt) == definition.names:
                    matches.append((owner, ",".join(definition.names), stmt))
            elif kind in {"import", "stmt"} and _dump(stmt) == _dump(definition.node):
                matches.append((owner, definition.key, stmt))
    return matches


def _source_diff(base: ast.stmt, cand: ast.stmt) -> str:
    a = ast.unparse(base).splitlines()
    b = ast.unparse(cand).splitlines()
    diff = "\n".join(difflib.unified_diff(a, b, "base", "candidate", lineterm=""))
    return diff or "(AST differs only in node structure; unparsed source is equal)\n" + "\n".join(
        difflib.unified_diff(_dump(base).split(", "), _dump(cand).split(", "), "base", "candidate", lineterm="")
    )


_PLACEHOLDER_HASHES = {None, "unhashable", "nosource"}


def _same_origin(base: dict, cand: dict, relocations: dict[tuple[str, str], tuple[str, str]]) -> tuple[bool, str | None]:
    """Compare free-name origins. Returns (equal, relocation key it depends on)."""
    if base.get("kind") == "error" or cand.get("kind") == "error":
        return False, None
    if base.get("kind") != cand.get("kind"):
        return False, None
    kind = base["kind"]
    if kind in {"builtin", "module", "literal", "unbound"}:
        return base == cand, None
    if kind == "def":
        if base["hash"] in _PLACEHOLDER_HASHES or cand["hash"] in _PLACEHOLDER_HASHES:
            return False, None
        expected = relocations.get((base["module"], base["qualname"]))
        if expected is not None:
            return (cand["module"], cand["qualname"]) == expected and base["hash"] == cand["hash"], \
                f"{base['module']}:{base['qualname']}"
        return (base["module"], base["qualname"], base["hash"]) == (cand["module"], cand["qualname"], cand["hash"]), None
    if kind == "state":
        if (base["type"], base["name"], base["home"]) != (cand["type"], cand["name"], cand["home"]):
            return False, None
        if base.get("logger_name") != cand.get("logger_name"):
            return False, None
        if cand["home"] == "<internal>":
            # One shared instance created by an unchanged initializer; when the
            # initializer moved in this range, it must itself verify.
            proven = bool(cand.get("shared")) and base.get("init_hash") is not None \
                and base.get("init_hash") == cand.get("init_hash")
            return proven, f"state:{base['name']}"
        return True, None
    return False, None


def run_check(
    base_root: Path,
    cand_root: Path,
    *,
    facade: str = DEFAULT_FACADE,
    owners: str = DEFAULT_OWNERS,
    client_class: str | None = None,
) -> list[Entry]:
    base_facade = _load_module(base_root, facade)
    if base_facade is None:
        raise SystemExit(f"base has no facade module {facade}")
    cand_facade = _load_module(cand_root, facade)
    removed = removed_definitions(base_facade, cand_facade)
    if not removed:
        return []
    cand_owners = _owner_modules(cand_root, owners)
    base_owner_names = [m.name for m in _owner_modules(base_root, owners)]

    entries: dict[str, Entry] = {}
    located: dict[str, tuple[ModuleSource, str, ast.stmt]] = {}
    for definition in removed:
        entry = Entry(definition.key, IDENTICAL, definition.module)
        entries[definition.key] = entry
        matches = _locate(definition, cand_owners)
        if not matches:
            entry.verdict = MISSING
            entry.reasons.append("no relocated definition found under the owner package")
            continue
        if len(matches) > 1:
            entry.verdict = NEEDS_REVIEW
            entry.reasons.append(
                "ambiguous: several owner definitions match: "
                + ", ".join(f"{m.name}:{q}" for m, q, _ in matches)
            )
            continue
        owner, qualname, node = matches[0]
        entry.candidate_module, entry.candidate_qualname = owner.name, qualname
        located[definition.key] = matches[0]
        if _dump(definition.node) != _dump(node):
            entry.verdict = DIFFERS
            entry.diff = _source_diff(definition.node, node)
            entry.reasons.append("definition AST differs")
            continue
        if definition.key.startswith("import:"):
            entry.verdict = NEEDS_REVIEW
            entry.reasons.append("module-level import moved; facade bindings are checked separately")
        elif definition.key.startswith("stmt:"):
            entry.verdict = NEEDS_REVIEW
            entry.reasons.append("module-level side-effect statement moved; cannot be proven automatically")
        method = definition.key.startswith("method:")
        dynamic = dynamic_lookup_reasons(definition.node, relocating_method=method, facade=facade)
        if dynamic:
            entry.verdict = NEEDS_REVIEW
            entry.reasons.extend(dynamic)

    # Removed facade imports: every name they bound must stay bound by the facade.
    import_checks: list[tuple[Definition, str]] = []
    for definition in removed:
        if definition.key.startswith("import:") and definition.key not in located:
            entries[definition.key].verdict = NEEDS_REVIEW
            entries[definition.key].reasons = []
            for name in definition.names:
                import_checks.append((definition, name))

    # Relocation map used to resolve free names that point at other moved defs.
    relocations: dict[tuple[str, str], tuple[str, str]] = {}
    relocation_keys: dict[str, str] = {}
    for key, (owner, qualname, node) in located.items():
        kind, name = key.split(":", 1)
        if kind in {"def", "class", "method"}:
            relocations[(facade, name)] = (owner.name, qualname)
            relocation_keys[f"{facade}:{name}"] = key
    for definition in removed:
        if definition.key.startswith("assign:"):
            for bound in definition.names:
                relocation_keys[f"state:{bound}"] = definition.key

    # Free names.
    base_requests: set[tuple[str, str]] = set()
    cand_requests: set[tuple[str, str]] = set()
    reads: dict[str, set[str]] = {}
    for key, (owner, _qualname, _node) in located.items():
        definition = next(d for d in removed if d.key == key)
        runtime, annotation_only = free_names(definition.node)
        entry = entries[key]
        if annotation_only:
            if not base_facade.future_annotations or not owner.future_annotations:
                entry.verdict = NEEDS_REVIEW if entry.verdict == IDENTICAL else entry.verdict
                side = "owner" if base_facade.future_annotations else "base facade"
                entry.reasons.append(
                    f"annotation-only names {sorted(annotation_only)} are evaluated at runtime: "
                    f"{side} lacks `from __future__ import annotations`"
                )
                runtime = runtime | annotation_only
        reads[key] = runtime
        for name in runtime:
            base_requests.add((facade, name))
            cand_requests.add((owner.name, name))
    for definition, name in import_checks:
        base_requests.add((facade, name))
        cand_requests.add((facade, name))

    identity_requests = []
    for key, (owner, qualname, _node) in located.items():
        kind, name = key.split(":", 1)
        if kind in {"def", "class"}:
            identity_requests.append((name, owner.name, qualname))
        elif kind == "assign":
            for bound in name.split(","):
                identity_requests.append((bound, owner.name, bound))
                cand_requests.add((owner.name, bound))
                base_requests.add((facade, bound))

    method_requests = []
    classes: set[str] = set()
    for key, (owner, qualname, _node) in located.items():
        if key.startswith("method:"):
            cls_name, attr = key.split(":", 1)[1].split(".", 1)
            classes.add(cls_name)
            method_requests.append((facade, cls_name, attr, owner.name, qualname))

    internal_base = [facade, *base_owner_names]
    internal_cand = [facade, owners, *[m.name for m in cand_owners]]
    base_probe = _probe(base_root, {
        "facade": facade, "owners": owners, "internal": internal_base,
        "import": [facade], "origins": sorted(base_requests), "identity": [],
        "classes": [(facade, c) for c in sorted(classes)], "methods": [],
    })
    cand_probe = _probe(cand_root, {
        "facade": facade, "owners": owners, "internal": internal_cand,
        "import": [facade, *[m.name for m in cand_owners]], "origins": sorted(cand_requests),
        "identity": identity_requests, "classes": [(facade, c) for c in sorted(classes)],
        "methods": method_requests,
    })
    probe_errors = base_probe["errors"] + cand_probe["errors"]

    # Origins.
    for key, names in reads.items():
        owner, _qualname, _node = located[key]
        entry = entries[key]
        for name in sorted(names):
            base_origin = base_probe["origins"].get(f"{facade}:{name}", {"kind": "error"})
            cand_origin = cand_probe["origins"].get(f"{owner.name}:{name}", {"kind": "error"})
            equal, dependency = _same_origin(base_origin, cand_origin, relocations)
            if dependency is not None and dependency in relocation_keys:
                entry.depends_on.add(relocation_keys[dependency])
            if not equal:
                if entry.verdict == IDENTICAL:
                    entry.verdict = NEEDS_REVIEW if cand_origin.get("kind") == "state" else DIFFERS
                entry.reasons.append(
                    f"free name `{name}`: base origin {json.dumps(base_origin, sort_keys=True)} "
                    f"!= candidate origin {json.dumps(cand_origin, sort_keys=True)}"
                )

    # Module-level state moved with an assignment: one instance, bound by the facade.
    for key, (owner, qualname, _node) in located.items():
        if not key.startswith("assign:"):
            continue
        entry = entries[key]
        for bound in key.split(":", 1)[1].split(","):
            cand_origin = cand_probe["origins"].get(f"{owner.name}:{bound}", {})
            base_origin = base_probe["origins"].get(f"{facade}:{bound}", {})
            if cand_origin.get("kind") == "state" and not cand_origin.get("shared"):
                entry.verdict = NEEDS_REVIEW if entry.verdict == IDENTICAL else entry.verdict
                entry.reasons.append(
                    f"module-level state `{bound}` is not one shared instance "
                    f"(holders {cand_origin.get('holders')}, conflicting {cand_origin.get('conflicting')})"
                )
            if base_origin.get("kind") != cand_origin.get("kind"):
                entry.verdict = NEEDS_REVIEW if entry.verdict == IDENTICAL else entry.verdict
                entry.reasons.append(f"`{bound}` kind changed: {base_origin} -> {cand_origin}")

    # Candidate-side identity: utils.cloud_sync.X is <owner>.X.
    for name, owner_module, qualname in identity_requests:
        same = cand_probe["identity"].get(f"{name}|{owner_module}")
        key = next(k for k, (o, q, _n) in located.items() if o.name == owner_module and name in q.split(","))
        entry = entries[key]
        if same is None:
            entry.verdict = MISSING
            entry.reasons.append(f"facade no longer binds `{name}`")
        elif not same:
            entry.verdict = NEEDS_REVIEW if entry.verdict == IDENTICAL else entry.verdict
            entry.reasons.append(f"facade `{name}` is not the same object as `{owner_module}.{name}`")

    # Removed facade imports.
    for definition, name in import_checks:
        entry = entries[definition.key]
        base_origin = base_probe["origins"].get(f"{facade}:{name}", {"kind": "error"})
        cand_origin = cand_probe["origins"].get(f"{facade}:{name}", {"kind": "error"})
        equal, _ = _same_origin(base_origin, cand_origin, relocations)
        if not equal:
            entry.reasons.append(f"facade binding `{name}` changed: {base_origin} -> {cand_origin}")
    for definition in {d.key: d for d, _ in import_checks}.values():
        entry = entries[definition.key]
        if not entry.reasons:
            entry.verdict = IDENTICAL
        entry.reasons.insert(0, "import removed from the facade; each name it bound is checked")

    # Client methods: MRO resolution.
    moved_attrs: dict[str, dict[str, str]] = {}
    for module_name, cls_name, attr, owner_module, owner_qualname in method_requests:
        entry = entries[f"method:{cls_name}.{attr}"]
        if not cand_probe["methods"].get(f"{cls_name}.{attr}"):
            entry.verdict = MISSING if entry.verdict == MISSING else NEEDS_REVIEW
            entry.reasons.append(f"`{cls_name}.{attr}` does not resolve through the MRO to the moved function")
        container = owner_qualname.rpartition(".")[0]
        moved_attrs.setdefault(cls_name, {})[attr] = (
            f"{owner_module}:{container}" if container else f"{facade}:{cls_name}"
        )
    for cls_name in sorted(classes):
        base_table = base_probe["classes"].get(f"{facade}:{cls_name}") or {}
        cand_table = cand_probe["classes"].get(f"{facade}:{cls_name}") or {}
        problems = []
        for attr in sorted((set(base_table) | set(cand_table)) - _CLASS_METADATA_ATTRS):
            expected = moved_attrs.get(cls_name, {}).get(attr, base_table.get(attr))
            if attr not in base_table and attr not in moved_attrs.get(cls_name, {}):
                problems.append(f"new attribute `{attr}` resolves to {cand_table.get(attr)}")
            elif cand_table.get(attr) != expected:
                problems.append(f"`{attr}` resolves to {cand_table.get(attr)}, expected {expected}")
        if problems:
            for module_name, c, attr, *_ in method_requests:
                if c == cls_name:
                    entry = entries[f"method:{c}.{attr}"]
                    entry.verdict = NEEDS_REVIEW if entry.verdict == IDENTICAL else entry.verdict
                    entry.reasons.append(f"`{cls_name}` attribute resolution changed: " + "; ".join(problems))

    if probe_errors:
        for entry in entries.values():
            if entry.verdict == IDENTICAL:
                entry.verdict = NEEDS_REVIEW
            entry.reasons.append("probe import errors: " + "; ".join(probe_errors))

    # A free name that points at another relocation is only as good as that relocation.
    changed = True
    while changed:
        changed = False
        for entry in entries.values():
            if entry.verdict != IDENTICAL:
                continue
            for dependency in sorted(entry.depends_on):
                dep = entries.get(dependency)
                if dep is None or dep.verdict != IDENTICAL:
                    entry.verdict = NEEDS_REVIEW
                    entry.reasons.append(f"depends on relocation `{dependency}` which is not identical")
                    changed = True
                    break
    return sorted(entries.values(), key=lambda e: e.key)


# --------------------------------------------------------------------------
# CLI


def _materialize(rev: str, workdir: Path, repo: Path) -> Path:
    if rev == WORKTREE:
        return repo
    as_path = Path(rev)
    if as_path.is_dir():
        return as_path.resolve()
    target = workdir / rev.replace("/", "_")
    target.mkdir(parents=True)
    archive = subprocess.run(["git", "-C", str(repo), "archive", rev], capture_output=True, check=True)
    subprocess.run(["tar", "-x", "-C", str(target)], input=archive.stdout, check=True)
    return target


def format_report(entries: list[Entry]) -> str:
    if not entries:
        return "No definitions were removed from the facade; nothing to check."
    lines = []
    for entry in entries:
        where = f" -> {entry.candidate_module}:{entry.candidate_qualname}" if entry.candidate_module else ""
        lines.append(f"[{entry.verdict}] {entry.key}{where}")
        for reason in entry.reasons:
            lines.append(f"    - {reason}")
        if entry.diff:
            lines.extend("    " + line for line in entry.diff.splitlines())
    counts: dict[str, int] = {}
    for entry in entries:
        counts[entry.verdict] = counts.get(entry.verdict, 0) + 1
    lines.append("Summary: " + ", ".join(f"{v}={counts[v]}" for v in sorted(counts)))
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("base", help="git revision, source directory, or WORKTREE")
    parser.add_argument("candidate", help="git revision, source directory, or WORKTREE")
    parser.add_argument("--facade", default=DEFAULT_FACADE)
    parser.add_argument("--owners", default=DEFAULT_OWNERS)
    parser.add_argument("--json", action="store_true", help="emit JSON instead of text")
    args = parser.parse_args(argv)
    repo = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory(prefix="relocation-check-") as tmp:
        base_root = _materialize(args.base, Path(tmp), repo)
        cand_root = _materialize(args.candidate, Path(tmp), repo)
        entries = run_check(base_root, cand_root, facade=args.facade, owners=args.owners)
    if args.json:
        print(json.dumps([e.to_json() for e in entries], indent=2))
    else:
        print(format_report(entries))
    return 0 if all(e.verdict == IDENTICAL for e in entries) else 1


if __name__ == "__main__":
    sys.exit(main())
