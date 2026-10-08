import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Test isolation: no test may read or write the developer's real Sporely
# profile (app_settings.json, databases, caches). Point the app data dir at a
# session temp dir before any app module computes its paths at import time.
# Tests that exercise the default resolution clear it with monkeypatch.
import tempfile  # noqa: E402

_TEST_APP_DATA_DIR = Path(tempfile.mkdtemp(prefix="sporely-test-appdata-")).resolve()
os.environ["SPORELY_APP_DATA_DIR"] = str(_TEST_APP_DATA_DIR)
os.environ.pop("SPORELY_PROFILE", None)
# The Artportalen/Artsobservasjoner keychain entries are not profile-scoped;
# a test that forgets to patch keyring must fail, not write the real keychain.
os.environ["PYTHON_KEYRING_BACKEND"] = "keyring.backends.fail.Keyring"

# Taxonomy-v2 is ON by default in the product. Unit tests that resolve the
# vernacular DB must not install the ~320 MB artifact into the developer's
# real app-data profile, so the suite runs with the explicit off-override
# unless a caller opts in (SPORELY_TAXONOMY_V2=1). Tests of the default clear
# it with monkeypatch.delenv.
os.environ.setdefault("SPORELY_TAXONOMY_V2", "0")


import pytest  # noqa: E402

TEST_REFERENCE_DEVICE_ID = "11111111-2222-4333-8444-555555555555"


@pytest.fixture(autouse=True)
def _isolated_reference_device_id(monkeypatch):
    """Never persist a Stage M device id into the developer's real profile.

    Any reference sync write or feed read builds ``p_client_capabilities``;
    the real accessor would create the id in ``app_settings.json``. Tests of
    the accessor import the original function and patch the settings I/O.
    """
    import utils.reference_client_capabilities as capabilities

    monkeypatch.setattr(
        capabilities, "get_reference_device_id", lambda: TEST_REFERENCE_DEVICE_ID
    )


@pytest.fixture(autouse=True)
def _facade_patches_reach_cloud_sync_owners(monkeypatch):
    """A facade monkeypatch also patches same-object owner bindings.

    See ``tests/cloud_sync_owner_patching.py`` (cloud-sync extraction).
    """
    from tests.cloud_sync_owner_patching import install_facade_owner_patching

    install_facade_owner_patching(monkeypatch)


@pytest.fixture(scope="session", autouse=True)
def _initialized_isolated_app_data():
    """Fresh schema in the isolated app data dir.

    Some tests open the default database without their own fixture; they used
    to reach the developer's real ``mushrooms.db``.
    """
    from database import schema

    assert str(schema.SETTINGS_PATH).startswith(str(_TEST_APP_DATA_DIR))
    schema.init_database()
    yield
