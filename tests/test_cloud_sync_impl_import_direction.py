"""Import-direction rules for ``utils/cloud_sync_impl/`` (Stage S2 of the extraction).

``OWNER_LAYERS`` and ``FACADE_IMPORT_ALLOWLIST`` are the enforced configuration.
Extraction stages extend the layer map as owners appear; an allowlist entry
needs a justification recorded beside it. Neither may be used to weaken the
rules for an existing owner.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

from tools.cloud_sync_import_direction import check_import_direction

ROOT = Path(__file__).resolve().parents[1]

#: Owner module -> layer. An owner may import only owners on a strictly lower layer.
OWNER_LAYERS: dict[str, int] = {
    # Stage S3: leaf and boundary owners.
    "utils.cloud_sync_impl": 0,
    "utils.cloud_sync_impl.common": 0,
    "utils.cloud_sync_impl.capabilities": 0,
    "utils.cloud_sync_impl.errors": 1,
    "utils.cloud_sync_impl.sync_state": 1,
    "utils.cloud_sync_impl.remote_reads": 1,
    "utils.cloud_sync_impl.progress": 2,
    "utils.cloud_sync_impl.pull_only": 2,
    "utils.cloud_sync_impl.transport": 2,
    "utils.cloud_sync_impl.image_policy": 2,
    "utils.cloud_sync_impl.tombstones": 3,
}

#: Owner modules allowed to import the facade at runtime (transitional; each
#: entry needs a justification comment). Empty unless a later stage justifies one.
FACADE_IMPORT_ALLOWLIST: frozenset[str] = frozenset()


def test_cloud_sync_impl_import_direction():
    assert check_import_direction(
        ROOT, layers=OWNER_LAYERS, facade_allowlist=FACADE_IMPORT_ALLOWLIST
    ) == []


# --- synthetic cases ---------------------------------------------------------

LAYERS = {"utils.cloud_sync_impl.base": 0, "utils.cloud_sync_impl.upper": 1}


def _pkg(tmp_path: Path, files: dict[str, str]) -> Path:
    pkg = tmp_path / "utils" / "cloud_sync_impl"
    pkg.mkdir(parents=True)
    (tmp_path / "utils" / "__init__.py").write_text("")
    (tmp_path / "utils" / "cloud_sync.py").write_text("")
    (pkg / "__init__.py").write_text("")
    for name, text in files.items():
        (pkg / name).write_text(textwrap.dedent(text))
    return tmp_path


def _check(tmp_path, files, **kwargs):
    return check_import_direction(_pkg(tmp_path, files), layers=kwargs.pop("layers", LAYERS), **kwargs)


def test_clean_owners_pass(tmp_path):
    assert _check(tmp_path, {
        "base.py": "from __future__ import annotations\nimport json\n",
        "upper.py": "from __future__ import annotations\nfrom utils.cloud_sync_impl.base import json\nfrom . import base\n",
    }) == []


def test_top_level_facade_import_fails(tmp_path):
    violations = _check(tmp_path, {"base.py": "from utils.cloud_sync import SporelyCloudClient\n", "upper.py": ""})
    assert any("imports the facade" in v for v in violations)


def test_function_local_facade_import_fails(tmp_path):
    violations = _check(tmp_path, {
        "base.py": "def f():\n    from utils import cloud_sync\n    return cloud_sync\n",
        "upper.py": "",
    })
    assert any("base:2: imports the facade" in v for v in violations)


def test_lazy_importlib_facade_import_fails(tmp_path):
    violations = _check(tmp_path, {
        "base.py": "import importlib\n\ndef f():\n    return importlib.import_module('utils.cloud_sync')\n",
        "upper.py": "",
    })
    assert any("imports the facade" in v for v in violations)


def test_relative_facade_import_fails(tmp_path):
    violations = _check(tmp_path, {"base.py": "from .. import cloud_sync\n", "upper.py": ""})
    assert any("imports the facade" in v for v in violations)


def test_type_checking_import_used_only_in_annotations_passes(tmp_path):
    assert _check(tmp_path, {
        "base.py": textwrap.dedent("""
            from __future__ import annotations
            from typing import TYPE_CHECKING
            if TYPE_CHECKING:
                from utils.cloud_sync import SporelyCloudClient

            def f(client: SporelyCloudClient) -> SporelyCloudClient:
                return client
            """),
        "upper.py": "",
    }) == []


def test_type_checking_import_used_outside_annotations_fails(tmp_path):
    violations = _check(tmp_path, {
        "base.py": textwrap.dedent("""
            from __future__ import annotations
            from typing import TYPE_CHECKING
            if TYPE_CHECKING:
                from utils.cloud_sync import SporelyCloudClient

            def f(client: SporelyCloudClient):
                return isinstance(client, SporelyCloudClient)
            """),
        "upper.py": "",
    })
    assert any("used outside annotations" in v for v in violations)


def test_type_checking_import_without_future_annotations_fails(tmp_path):
    violations = _check(tmp_path, {
        "base.py": textwrap.dedent("""
            from typing import TYPE_CHECKING
            if TYPE_CHECKING:
                from utils.cloud_sync import SporelyCloudClient

            def f(client: SporelyCloudClient):
                return client
            """),
        "upper.py": "",
    })
    assert any("__future__" in v for v in violations)


def test_allowlisted_owner_may_import_facade(tmp_path):
    assert _check(
        tmp_path,
        {"base.py": "from utils import cloud_sync\n", "upper.py": ""},
        facade_allowlist=frozenset({"utils.cloud_sync_impl.base"}),
    ) == []


def test_upward_owner_import_fails(tmp_path):
    violations = _check(tmp_path, {
        "base.py": "def f():\n    from utils.cloud_sync_impl import upper\n    return upper\n",
        "upper.py": "",
    })
    assert any("strictly downward" in v for v in violations)


def test_same_layer_cycle_fails(tmp_path):
    layers = {"utils.cloud_sync_impl.a": 0, "utils.cloud_sync_impl.b": 0}
    violations = _check(tmp_path, {"a.py": "from . import b\n", "b.py": "from . import a\n"}, layers=layers)
    assert len([v for v in violations if "strictly downward" in v]) == 2


def test_undeclared_owner_fails(tmp_path):
    violations = _check(tmp_path, {"base.py": "", "upper.py": "", "stray.py": ""})
    assert any("stray: owner module is not declared" in v for v in violations)


def test_package_init_importing_owner_fails(tmp_path):
    root = _pkg(tmp_path, {"base.py": "", "upper.py": ""})
    (root / "utils" / "cloud_sync_impl" / "__init__.py").write_text("from . import base\n")
    violations = check_import_direction(root, layers=LAYERS)
    assert any("package __init__ imports owner" in v for v in violations)


def test_aliased_import_module_is_rejected(tmp_path):
    violations = _check(tmp_path, {
        "base.py": "from importlib import import_module as load\n\ndef f():\n    return load('utils.cloud_sync')\n",
        "upper.py": "",
    })
    assert any("dynamic import machinery" in v for v in violations)


def test_sys_modules_lookup_is_rejected(tmp_path):
    violations = _check(tmp_path, {
        "base.py": "import sys\nname = 'utils.' + 'cloud_sync'\n\ndef f():\n    return sys.modules[name]\n",
        "upper.py": "",
    })
    assert any("dynamic import machinery `.modules`" in v for v in violations)


def test_locally_defined_type_checking_guard_is_not_trusted(tmp_path):
    violations = _check(tmp_path, {
        "base.py": textwrap.dedent("""
            from __future__ import annotations
            TYPE_CHECKING = True
            if TYPE_CHECKING:
                from utils.cloud_sync import SporelyCloudClient

            def f(client: SporelyCloudClient) -> SporelyCloudClient:
                return client
            """),
        "upper.py": "",
    })
    assert any("imports the facade" in v for v in violations)


def test_rebound_type_checking_guard_is_not_trusted(tmp_path):
    violations = _check(tmp_path, {
        "base.py": textwrap.dedent("""
            from __future__ import annotations
            from typing import TYPE_CHECKING
            TYPE_CHECKING = True
            if TYPE_CHECKING:
                from utils.cloud_sync import SporelyCloudClient

            def f(client: SporelyCloudClient) -> SporelyCloudClient:
                return client
            """),
        "upper.py": "",
    })
    assert any("imports the facade" in v for v in violations)


def test_typing_attribute_guard_is_trusted_only_unmodified(tmp_path):
    source = textwrap.dedent("""
        from __future__ import annotations
        import typing
        {extra}
        if typing.TYPE_CHECKING:
            from utils.cloud_sync import SporelyCloudClient

        def f(client: SporelyCloudClient) -> SporelyCloudClient:
            return client
        """)
    assert _check(tmp_path / "ok", {"base.py": source.format(extra=""), "upper.py": ""}) == []
    violations = _check(tmp_path / "bad", {
        "base.py": source.format(extra="typing.TYPE_CHECKING = True"), "upper.py": "",
    })
    assert any("imports the facade" in v for v in violations)


def test_wildcard_import_voids_type_checking_guard(tmp_path):
    violations = _check(tmp_path, {
        "base.py": textwrap.dedent("""
            from __future__ import annotations
            from typing import TYPE_CHECKING
            from json import *
            if TYPE_CHECKING:
                from utils.cloud_sync import SporelyCloudClient

            def f(client: SporelyCloudClient) -> SporelyCloudClient:
                return client
            """),
        "upper.py": "",
    })
    assert any("imports the facade" in v for v in violations)
