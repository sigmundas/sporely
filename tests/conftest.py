import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

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
