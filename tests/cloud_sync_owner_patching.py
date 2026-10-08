"""Test-only: make facade monkeypatches reach code relocated to owners.

The cloud-sync extraction moves definitions from ``utils/cloud_sync.py`` (the
facade) into owners under ``utils/cloud_sync_impl/``. A moved function looks up
its globals in its owner module, so ``monkeypatch.setattr(cloud_sync, "X", f)``
no longer reaches it. ``install_facade_owner_patching`` wraps one test's
``monkeypatch.setattr`` so that patching a facade name also patches every owner
binding of that name that is the *same object* as the facade's binding.

Rules: only same-object owner bindings are patched; a name the facade does not
bind fails even with ``raising=False``; ``monkeypatch`` undoes every patch.
``patch_facade_object`` applies the same rules to ``unittest.mock.patch.object``.
Production code never imports this module.
"""

from __future__ import annotations

import sys
from contextlib import ExitStack, contextmanager
from unittest import mock

FACADE = "utils.cloud_sync"
OWNER_PREFIX = "utils.cloud_sync_impl."
_UNSET = object()


def _resolve(target, name, value):
    """Return (module_object, attribute, value) for both setattr call forms."""
    if isinstance(target, str) and value is _UNSET:
        module_name, _, attr = target.rpartition(".")
        return sys.modules.get(module_name), attr, name
    return target, name, value


def install_facade_owner_patching(monkeypatch, *, facade: str = FACADE, owner_prefix: str = OWNER_PREFIX):
    original_setattr = monkeypatch.setattr

    def setattr_through_owners(target, name, value=_UNSET, raising=True):
        module, attr, new_value = _resolve(target, name, value)
        facade_module = sys.modules.get(facade)
        if facade_module is not None and module is facade_module:
            if attr not in vars(facade_module):
                raise AttributeError(f"{facade} has no attribute {attr!r}")
            current = vars(facade_module)[attr]
            for owner_name, owner in list(sys.modules.items()):
                if owner is None or not owner_name.startswith(owner_prefix):
                    continue
                if vars(owner).get(attr, _UNSET) is current:
                    original_setattr(owner, attr, new_value)
        if value is _UNSET:
            return original_setattr(target, name, raising=raising)
        return original_setattr(target, name, value, raising=raising)

    monkeypatch.setattr = setattr_through_owners
    return setattr_through_owners


@contextmanager
def patch_facade_object(target, attribute, *args, facade: str = FACADE, owner_prefix: str = OWNER_PREFIX, **kwargs):
    """``unittest.mock.patch.object`` that also reaches same-object owner bindings.

    The facade binding is patched with ``patch.object(target, attribute, ...)``;
    every owner binding of ``attribute`` that was the same object as the facade's
    is then bound to the same replacement. A name the facade does not bind fails.
    """
    facade_module = sys.modules.get(facade)
    owners = []
    if facade_module is not None and target is facade_module:
        if attribute not in vars(facade_module):
            raise AttributeError(f"{facade} has no attribute {attribute!r}")
        current = vars(facade_module)[attribute]
        owners = [
            module for name, module in list(sys.modules.items())
            if module is not None and name.startswith(owner_prefix)
            and vars(module).get(attribute, _UNSET) is current
        ]
    with ExitStack() as stack:
        replacement = stack.enter_context(mock.patch.object(target, attribute, *args, **kwargs))
        for module in owners:
            stack.enter_context(mock.patch.object(module, attribute, getattr(target, attribute)))
        yield replacement
