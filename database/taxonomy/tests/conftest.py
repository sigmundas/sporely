import os
import tempfile
from pathlib import Path

# Taxonomy-v2 is ON by default in the product; keep these tests from
# installing the bundled artifact into the developer's real app-data profile.
# Tests of the default clear this with monkeypatch.delenv.
os.environ.setdefault("SPORELY_TAXONOMY_V2", "0")

# The same app-data isolation as tests/conftest.py: tests here that reach the
# default database (cloud-row display, sync pushes) must neither touch the
# developer's real profile nor inherit a schema-less database another test
# left current. Run together with tests/, that conftest's directory wins.
if "SPORELY_APP_DATA_DIR" not in os.environ:
    os.environ["SPORELY_APP_DATA_DIR"] = str(
        Path(tempfile.mkdtemp(prefix="sporely-taxonomy-test-appdata-")).resolve())
    os.environ.pop("SPORELY_PROFILE", None)

import pytest  # noqa: E402


@pytest.fixture(scope="session", autouse=True)
def _initialized_isolated_app_data():
    """A schema-complete default database in an isolated temp app data dir.

    The directory is this conftest's, or tests/conftest.py's when both
    directories run together; either way never the real profile.
    """
    from database import schema

    assert Path(schema.SETTINGS_PATH).resolve().is_relative_to(Path(tempfile.gettempdir()).resolve())
    schema.init_database()
    yield
