"""Cloud-sync extraction: facade identity and pull-only client classification.

Every module-level name an owner under ``utils/cloud_sync_impl/`` defines is
re-exported by the facade ``utils.cloud_sync`` as the *same object*, and the
facade never redefines it. Every method reachable on ``SporelyCloudClient``
(including methods inherited from owner mixins) is classified for pull-only.
"""

from __future__ import annotations

import ast
import importlib
from pathlib import Path

import pytest

import utils.cloud_sync as cloud_sync

ROOT = Path(__file__).resolve().parents[1]
OWNER_DIR = ROOT / "utils" / "cloud_sync_impl"


def _owner_modules() -> list[str]:
    return sorted(
        f"utils.cloud_sync_impl.{path.stem}"
        for path in OWNER_DIR.glob("*.py")
        if path.stem != "__init__"
    )


def _defined_names(module_name: str) -> list[str]:
    path = OWNER_DIR / f"{module_name.rpartition('.')[2]}.py"
    names: list[str] = []
    for stmt in ast.parse(path.read_text(encoding="utf-8")).body:
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.append(stmt.name)
        elif isinstance(stmt, (ast.Assign, ast.AnnAssign)):
            targets = stmt.targets if isinstance(stmt, ast.Assign) else [stmt.target]
            names.extend(n.id for t in targets for n in ast.walk(t) if isinstance(n, ast.Name))
    return names


def _facade_defined_names() -> set[str]:
    tree = ast.parse((ROOT / "utils" / "cloud_sync.py").read_text(encoding="utf-8"))
    names: set[str] = set()
    for stmt in tree.body:
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(stmt.name)
        elif isinstance(stmt, (ast.Assign, ast.AnnAssign)):
            targets = stmt.targets if isinstance(stmt, ast.Assign) else [stmt.target]
            names.update(n.id for t in targets for n in ast.walk(t) if isinstance(n, ast.Name))
    return names


OWNED = [(module, name) for module in _owner_modules() for name in _defined_names(module)]


def test_owners_define_names():
    assert len(OWNED) > 100


@pytest.mark.parametrize(("module_name", "name"), OWNED, ids=[f"{m.rpartition('.')[2]}.{n}" for m, n in OWNED])
def test_facade_reexports_owner_name_as_same_object(module_name, name):
    owner = importlib.import_module(module_name)
    assert name in vars(cloud_sync), f"facade does not bind {name}"
    assert vars(cloud_sync)[name] is vars(owner)[name]


def test_facade_never_redefines_an_owned_name():
    owned = {name for _module, name in OWNED}
    assert sorted(owned & _facade_defined_names()) == []


def test_owned_names_are_defined_by_exactly_one_owner():
    seen: dict[str, str] = {}
    for module_name, name in OWNED:
        assert name not in seen, f"{name} defined by {seen[name]} and {module_name}"
        seen[name] = module_name


def test_relocated_client_methods_resolve_to_the_mixin():
    from utils.cloud_sync_impl.transport import CloudSyncTransportMixin

    assert issubclass(cloud_sync.SporelyCloudClient, CloudSyncTransportMixin)
    assert cloud_sync.SporelyCloudClient._get_paginated is CloudSyncTransportMixin._get_paginated


# --- pull-only classification -------------------------------------------------

#: Client methods that were in neither pull-only list at the extraction base
#: (``5768fa4``). The registries are frozen at the base during extraction, so
#: these stay unclassified; ``PullOnlyCloudClient`` blocks them fail-closed as
#: "Unrecognized client method". This set may only shrink: a new client method
#: must join one of the two registries.
#:
#: This is NOT exhaustive two-list classification. The S3 brief asks for both
#: "every method in exactly one pull-only list" and "registries are those of the
#: base"; at the base these methods are in neither list, so both cannot hold.
#: Extraction keeps the base registries; classifying these methods (plan
#: invariant 10) is a registry change for a later, explicitly scoped stage.
UNCLASSIFIED_AT_EXTRACTION_BASE = frozenset({
    "_adopt_session_from_values",
    "_build_storage_path",
    "_cloud_image_storage_key",
    "_download_public_media_file",
    "_find_cloud_observation",
    "_get_paginated",
    "_get_r2",
    "_has_column",
    "_maybe_clear_stale_cloud_identity",
    "_measurement_supports_media_keys",
    "_observation_images_support_metadata_purpose",
    "_observation_images_support_original_storage_path",
    "_observation_images_support_storage_exif_safe",
    "_observation_select_columns",
    "_observation_supports_media_keys",
    "_patch_with_precondition",
    "_read_observation_rows",
    "_request_with_refresh",
    "_resolve_existing_image_for_push",
    "_resolve_existing_observation_for_push",
    "_response_indicates_auth_error",
    "_rpc",
    "_set_observation_media_keys",
    "_sync_observation_selected_taxon",
    "_using_default_r2_loader",
    "_verify_identity_clear_landed",
    "clear_credentials",
    "clear_session",
    "community_spore_taxon_summary",
    "count_remote_privacy_slots",
    "fetch_current_user_info",
    "fetch_image_metadata_purpose",
    "fetch_profile",
    "from_stored_credentials",
    "get_community_spore_dataset",
    "login",
    "pull_web_observations",
    "refresh_login",
    "search_community_spore_datasets",
    "search_public_reference_values",
    "update_profile",
    "upload_profile_avatar",
})


def _client_methods() -> set[str]:
    cls = cloud_sync.SporelyCloudClient
    return {
        name for name in dir(cls)
        if not (name.startswith("__") and name.endswith("__")) and callable(getattr(cls, name))
    }


def test_every_reachable_client_method_is_classified_once_or_frozen_unclassified():
    blocked = cloud_sync._PULL_ONLY_BLOCKED_CLIENT_METHODS
    allowed = cloud_sync._PULL_ONLY_ALLOWED_READ_METHODS
    assert blocked & allowed == frozenset()
    unclassified = _client_methods() - blocked - allowed
    assert sorted(unclassified - UNCLASSIFIED_AT_EXTRACTION_BASE) == [], "classify new client methods"
    assert unclassified == UNCLASSIFIED_AT_EXTRACTION_BASE & _client_methods()


def test_methods_from_owner_bases_are_covered():
    cls = cloud_sync.SporelyCloudClient
    inherited = {
        name
        for base in cls.__mro__[1:]
        if base is not object
        for name, value in vars(base).items()
        if callable(value) and not (name.startswith("__") and name.endswith("__"))
    }
    assert inherited, "expected owner mixin methods"
    blocked = cloud_sync._PULL_ONLY_BLOCKED_CLIENT_METHODS
    allowed = cloud_sync._PULL_ONLY_ALLOWED_READ_METHODS
    for name in inherited:
        assert (name in blocked) + (name in allowed) + (name in UNCLASSIFIED_AT_EXTRACTION_BASE) == 1, name


@pytest.mark.parametrize("name", sorted(UNCLASSIFIED_AT_EXTRACTION_BASE))
def test_unclassified_methods_are_blocked_by_the_pull_only_wrapper(name):
    if name in vars(cloud_sync.PullOnlyCloudClient):
        pytest.skip(f"{name} is handled explicitly by PullOnlyCloudClient (e.g. the RPC allowlist)")
    raw = object.__new__(cloud_sync.SporelyCloudClient)
    wrapper = cloud_sync.PullOnlyCloudClient(raw)
    with pytest.raises(cloud_sync.PullOnlyModeError):
        getattr(wrapper, name)()
    assert wrapper.write_attempts
