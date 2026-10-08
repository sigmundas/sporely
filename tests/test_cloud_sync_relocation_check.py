"""Synthetic before/after modules for ``tools/cloud_sync_relocation_check.py``.

Each case builds a base and a candidate source tree with a facade
(``pkg.facade``) and an owner package (``pkg.impl``), then asserts the verdict.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

from tools import cloud_sync_relocation_check as check

FUTURE = "from __future__ import annotations\n"
UTIL = "def _fmt(x):\n    return f'<{x}>'\n"
OTHER = "def _fmt(x):\n    return f'[{x}]'\n"


def _tree(root: Path, files: dict[str, str]) -> Path:
    root.mkdir(parents=True)
    for rel, text in {"pkg/__init__.py": "", "pkg/util.py": UTIL, "pkg/other.py": OTHER, **files}.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(text), encoding="utf-8")
    return root


def _run(tmp_path: Path, base: dict[str, str], cand: dict[str, str]) -> dict[str, check.Entry]:
    entries = check.run_check(
        _tree(tmp_path / "base", base),
        _tree(tmp_path / "cand", cand),
        facade="pkg.facade",
        owners="pkg.impl",
    )
    return {entry.key: entry for entry in entries}


def _owner(text: str) -> dict[str, str]:
    return {"pkg/impl/__init__.py": "", "pkg/impl/a.py": text}


BASE_HELPER = {
    "pkg/facade.py": FUTURE + "from pkg.util import _fmt\n\ndef helper(x):\n    return _fmt(x)\n",
}
CAND_FACADE_HELPER = FUTURE + "from pkg.util import _fmt\nfrom pkg.impl.a import helper\n"


def test_empty_range_reports_nothing(tmp_path):
    assert _run(tmp_path, BASE_HELPER, BASE_HELPER) == {}


def test_identical_relocation(tmp_path):
    entries = _run(tmp_path, BASE_HELPER, {
        "pkg/facade.py": CAND_FACADE_HELPER,
        **_owner(FUTURE + "from pkg.util import _fmt\n\n# a comment\ndef helper(x):\n\n    return _fmt(x)\n"),
    })
    assert entries["def:helper"].verdict == check.IDENTICAL, entries["def:helper"].reasons


def test_method_to_mixin_relocation(tmp_path):
    base = {"pkg/facade.py": FUTURE + textwrap.dedent("""
        CONST = 3

        class Client:
            def keep(self):
                return 1

            def moved(self, other: Client) -> int:
                return CONST + other.keep()
        """)}
    cand = {
        "pkg/facade.py": FUTURE + textwrap.dedent("""
            from pkg.impl.a import ClientMixin
            CONST = 3

            class Client(ClientMixin):
                def keep(self):
                    return 1
            """),
        **_owner(FUTURE + textwrap.dedent("""
            from typing import TYPE_CHECKING
            if TYPE_CHECKING:
                from pkg.facade import Client
            CONST = 3

            class ClientMixin:
                def moved(self, other: Client) -> int:
                    return CONST + other.keep()
            """)),
    }
    entries = _run(tmp_path, base, cand)
    entry = entries["method:Client.moved"]
    assert entry.verdict == check.IDENTICAL, entry.reasons
    assert (entry.candidate_module, entry.candidate_qualname) == ("pkg.impl.a", "ClientMixin.moved")


def test_method_mixin_that_shadows_another_attribute_needs_review(tmp_path):
    base = {"pkg/facade.py": FUTURE + textwrap.dedent("""
        class Client:
            def moved(self):
                return 1
        """)}
    cand = {
        "pkg/facade.py": FUTURE + "from pkg.impl.a import ClientMixin\n\nclass Client(ClientMixin):\n    pass\n",
        **_owner(FUTURE + textwrap.dedent("""
            class ClientMixin:
                extra = 1

                def moved(self):
                    return 1
            """)),
    }
    entry = _run(tmp_path, base, cand)["method:Client.moved"]
    assert entry.verdict == check.NEEDS_REVIEW
    assert any("new attribute `extra`" in reason for reason in entry.reasons)


def test_renamed_free_name_differs(tmp_path):
    entries = _run(tmp_path, BASE_HELPER, {
        "pkg/facade.py": CAND_FACADE_HELPER,
        **_owner(FUTURE + "from pkg.util import _fmt as _fmt2\n\ndef helper(x):\n    return _fmt2(x)\n"),
    })
    entry = entries["def:helper"]
    assert entry.verdict == check.DIFFERS
    assert "_fmt2" in entry.diff


def test_rebound_free_name_differs(tmp_path):
    entries = _run(tmp_path, BASE_HELPER, {
        "pkg/facade.py": CAND_FACADE_HELPER,
        **_owner(FUTURE + "from pkg.other import _fmt\n\ndef helper(x):\n    return _fmt(x)\n"),
    })
    entry = entries["def:helper"]
    assert entry.verdict == check.DIFFERS
    assert any("free name `_fmt`" in reason for reason in entry.reasons)


BASE_CACHE = {"pkg/facade.py": FUTURE + "_CACHE = {}\n\ndef get(k):\n    return _CACHE.get(k)\n"}


def test_duplicated_module_level_state_needs_review(tmp_path):
    entries = _run(tmp_path, BASE_CACHE, {
        "pkg/facade.py": FUTURE + "from pkg.impl.a import get\n_CACHE = {}\n",
        **_owner(FUTURE + "_CACHE = {}\n\ndef get(k):\n    return _CACHE.get(k)\n"),
    })
    assert entries["def:get"].verdict == check.NEEDS_REVIEW
    assert any("_CACHE" in reason for reason in entries["def:get"].reasons)


def test_state_moved_once_and_rebound_by_facade_is_identical(tmp_path):
    entries = _run(tmp_path, BASE_CACHE, {
        "pkg/facade.py": FUTURE + "from pkg.impl.a import _CACHE, get\n",
        **_owner(FUTURE + "_CACHE = {}\n\ndef get(k):\n    return _CACHE.get(k)\n"),
    })
    assert entries["def:get"].verdict == check.IDENTICAL, entries["def:get"].reasons
    assert entries["assign:_CACHE"].verdict == check.IDENTICAL, entries["assign:_CACHE"].reasons


def test_state_moved_into_two_owners_needs_review(tmp_path):
    entries = _run(tmp_path, BASE_CACHE, {
        "pkg/facade.py": FUTURE + "from pkg.impl.a import _CACHE, get\n",
        "pkg/impl/__init__.py": "",
        "pkg/impl/a.py": FUTURE + "_CACHE = {}\n\ndef get(k):\n    return _CACHE.get(k)\n",
        "pkg/impl/b.py": FUTURE + "_CACHE = {}\n",
    })
    # Two owner definitions of the same state: ambiguous, never identical.
    assert entries["assign:_CACHE"].verdict == check.NEEDS_REVIEW
    assert entries["def:get"].verdict == check.NEEDS_REVIEW


def test_global_statement_needs_review(tmp_path):
    source = "COUNTER = 0\n\ndef bump():\n    global COUNTER\n    COUNTER += 1\n"
    entries = _run(tmp_path, {"pkg/facade.py": FUTURE + source}, {
        "pkg/facade.py": FUTURE + "from pkg.impl.a import bump\nCOUNTER = 0\n",
        **_owner(FUTURE + source),
    })
    entry = entries["def:bump"]
    assert entry.verdict == check.NEEDS_REVIEW
    assert any("global COUNTER" in reason for reason in entry.reasons)


def test_logger_name_change_needs_review(tmp_path):
    source = "import logging\nlogger = logging.getLogger(__name__)\n\ndef log():\n    logger.info('x')\n"
    entries = _run(tmp_path, {"pkg/facade.py": FUTURE + source}, {
        "pkg/facade.py": FUTURE + "import logging\nlogger = logging.getLogger(__name__)\nfrom pkg.impl.a import log\n",
        **_owner(FUTURE + source),
    })
    entry = entries["def:log"]
    assert entry.verdict == check.NEEDS_REVIEW
    assert any("logger" in reason for reason in entry.reasons)


def test_moved_logger_statement_needs_review(tmp_path):
    source = "import logging\nlogger = logging.getLogger(__name__)\n"
    entries = _run(tmp_path, {"pkg/facade.py": FUTURE + source}, {
        "pkg/facade.py": FUTURE + "import logging\nfrom pkg.impl.a import logger\n",
        **_owner(FUTURE + source),
    })
    assert entries["assign:logger"].verdict == check.NEEDS_REVIEW
    assert any("__name__" in reason for reason in entries["assign:logger"].reasons)


def test_changed_default_differs(tmp_path):
    entries = _run(tmp_path, {"pkg/facade.py": FUTURE + "def f(x=1):\n    return x\n"}, {
        "pkg/facade.py": FUTURE + "from pkg.impl.a import f\n",
        **_owner(FUTURE + "def f(x=2):\n    return x\n"),
    })
    assert entries["def:f"].verdict == check.DIFFERS
    assert "x=2" in entries["def:f"].diff


def test_changed_decorator_differs(tmp_path):
    base = FUTURE + "import functools\n\n@functools.lru_cache(maxsize=8)\ndef f(x):\n    return x\n"
    cand = FUTURE + "import functools\n\n@functools.lru_cache(maxsize=16)\ndef f(x):\n    return x\n"
    entries = _run(tmp_path, {"pkg/facade.py": base}, {
        "pkg/facade.py": FUTURE + "from pkg.impl.a import f\n",
        **_owner(cand),
    })
    assert entries["def:f"].verdict == check.DIFFERS


ANNOTATED = textwrap.dedent("""
    class Client:
        pass

    def use(client: Client) -> Client:
        return client
    """)


def test_annotation_only_name_with_owner_future_import_is_identical(tmp_path):
    entries = _run(tmp_path, {"pkg/facade.py": FUTURE + ANNOTATED}, {
        "pkg/facade.py": FUTURE + "from pkg.impl.a import use\n\nclass Client:\n    pass\n",
        **_owner(FUTURE + "def use(client: Client) -> Client:\n    return client\n"),
    })
    assert entries["def:use"].verdict == check.IDENTICAL, entries["def:use"].reasons


def test_annotation_only_name_without_owner_future_import_needs_review(tmp_path):
    entries = _run(tmp_path, {"pkg/facade.py": FUTURE + ANNOTATED}, {
        "pkg/facade.py": FUTURE + "from pkg.impl.a import use\n\nclass Client:\n    pass\n",
        # Without the future import this module would fail at import time;
        # a string annotation keeps it importable so the check itself decides.
        **_owner("def use(client: 'Client') -> 'Client':\n    return client\n"),
    })
    assert entries["def:use"].verdict != check.IDENTICAL


def test_annotation_only_name_without_future_import_same_ast_needs_review(tmp_path):
    entries = _run(tmp_path, {"pkg/facade.py": FUTURE + ANNOTATED}, {
        "pkg/facade.py": FUTURE + "from pkg.impl.a import use\n\nclass Client:\n    pass\n",
        **_owner("class Client:\n    pass\n\ndef use(client: Client) -> Client:\n    return client\n"),
    })
    entry = entries["def:use"]
    assert entry.verdict == check.NEEDS_REVIEW
    assert any("__future__" in reason for reason in entry.reasons)


def test_missing_relocation(tmp_path):
    entries = _run(tmp_path, BASE_HELPER, {"pkg/facade.py": FUTURE + "from pkg.util import _fmt\n"})
    assert entries["def:helper"].verdict == check.MISSING


def test_facade_not_reexporting_is_missing(tmp_path):
    entries = _run(tmp_path, BASE_HELPER, {
        "pkg/facade.py": FUTURE + "from pkg.util import _fmt\n",
        **_owner(FUTURE + "from pkg.util import _fmt\n\ndef helper(x):\n    return _fmt(x)\n"),
    })
    assert entries["def:helper"].verdict == check.MISSING


def test_dependency_on_unverified_relocation_is_not_identical(tmp_path):
    base = FUTURE + "def inner():\n    return 1\n\ndef outer():\n    return inner()\n"
    entries = _run(tmp_path, {"pkg/facade.py": base}, {
        "pkg/facade.py": FUTURE + "from pkg.impl.a import inner, outer\n",
        **_owner(FUTURE + "def inner():\n    return 2\n\ndef outer():\n    return inner()\n"),
    })
    assert entries["def:inner"].verdict == check.DIFFERS
    # ``outer`` is AST-equal, but the ``inner`` it reads is a different function.
    assert entries["def:outer"].verdict != check.IDENTICAL


def test_dependency_on_verified_relocation_is_identical(tmp_path):
    base = FUTURE + "def inner():\n    return 1\n\ndef outer():\n    return inner()\n"
    entries = _run(tmp_path, {"pkg/facade.py": base}, {
        "pkg/facade.py": FUTURE + "from pkg.impl.a import inner, outer\n",
        **_owner(base),
    })
    assert entries["def:outer"].verdict == check.IDENTICAL, entries["def:outer"].reasons


def test_late_bound_facade_import_needs_review(tmp_path):
    body = "def f():\n    from pkg import facade\n    return facade\n"
    entries = _run(tmp_path, {"pkg/facade.py": FUTURE + body}, {
        "pkg/facade.py": FUTURE + "from pkg.impl.a import f\n",
        **_owner(FUTURE + body),
    })
    assert entries["def:f"].verdict == check.NEEDS_REVIEW


def test_private_name_mangling_in_moved_method_needs_review(tmp_path):
    base = FUTURE + "class Client:\n    def moved(self):\n        return self.__secret\n"
    cand = {
        "pkg/facade.py": FUTURE + "from pkg.impl.a import ClientMixin\n\nclass Client(ClientMixin):\n    pass\n",
        **_owner(FUTURE + "class ClientMixin:\n    def moved(self):\n        return self.__secret\n"),
    }
    entry = _run(tmp_path, {"pkg/facade.py": base}, cand)["method:Client.moved"]
    assert entry.verdict == check.NEEDS_REVIEW


def test_cli_over_empty_range_reports_nothing(tmp_path, capsys):
    base = _tree(tmp_path / "base", BASE_HELPER)
    assert check.main([str(base), str(base), "--facade", "pkg.facade", "--owners", "pkg.impl"]) == 0
    assert "nothing to check" in capsys.readouterr().out
