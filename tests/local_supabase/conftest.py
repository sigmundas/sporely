"""Opt-in local Supabase integration harness (marker ``local_supabase``).

Skipped unless ``SPORELY_LOCAL_SUPABASE=1`` and the local stack answers.
Run it through ``tools/run_local_sync_harness.sh``, which rebuilds the local
database from sporely-web origin/main without the production-deferred
migrations and exports the local URL/keys. Only local URLs are accepted.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path

from urllib.parse import urlsplit

import pytest
import requests

HERE = Path(__file__).resolve().parent
URL = os.environ.get("SPORELY_LOCAL_SUPABASE_URL", "")
SERVICE_KEY = os.environ.get("SPORELY_LOCAL_SUPABASE_SERVICE_KEY", "")
ANON_KEY = os.environ.get("SPORELY_LOCAL_SUPABASE_ANON_KEY", "")
DB_CONTAINER = os.environ.get("SPORELY_LOCAL_SUPABASE_DB_CONTAINER", "supabase_db_zkpjklzfwzefhjluvhfw")


def is_local_url(url: str) -> bool:
    """Parsed host must be 127.0.0.1/localhost; no userinfo, http(s) only."""
    try:
        parts = urlsplit(str(url))
    except ValueError:
        return False
    return (
        parts.scheme in {"http", "https"}
        and parts.hostname in {"127.0.0.1", "localhost"}
        and parts.username is None and parts.password is None
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "local_supabase: needs the local Supabase stack")


def _stack_ready() -> str | None:
    if os.environ.get("SPORELY_LOCAL_SUPABASE") != "1":
        return "set SPORELY_LOCAL_SUPABASE=1 (use tools/run_local_sync_harness.sh)"
    if not is_local_url(URL):
        return "SPORELY_LOCAL_SUPABASE_URL must be a local URL"
    try:
        requests.get(f"{URL}/auth/v1/health", timeout=3)
    except requests.RequestException:
        return "local Supabase stack not reachable"
    return None


def pytest_collection_modifyitems(config, items):
    reason = _stack_ready()
    for item in items:
        if HERE in Path(str(item.fspath)).resolve().parents:
            item.add_marker(pytest.mark.local_supabase)
            if reason:
                item.add_marker(pytest.mark.skip(reason=reason))


def sql(query: str) -> list[list[str]]:
    """Run SQL as postgres on the LOCAL database container (setup/assertions)."""
    out = subprocess.run(
        ["docker", "exec", DB_CONTAINER, "psql", "-U", "postgres", "-tAF", "\t", "-v",
         "ON_ERROR_STOP=1", "-c", query],
        check=True, capture_output=True, text=True,
    ).stdout
    return [line.split("\t") for line in out.splitlines() if line]


@dataclass
class User:
    email: str
    password: str
    id: str

    @property
    def credentials(self) -> dict:
        return {"email": self.email, "password": self.password}


@pytest.fixture(scope="session")
def make_user():
    def _make() -> User:
        email = f"harness-{uuid.uuid4().hex[:10]}@example.test"
        password = uuid.uuid4().hex
        response = requests.post(
            f"{URL}/auth/v1/admin/users",
            headers={"apikey": SERVICE_KEY, "Authorization": f"Bearer {SERVICE_KEY}"},
            json={"email": email, "password": password, "email_confirm": True},
            timeout=10,
        )
        response.raise_for_status()
        user_id = response.json()["id"]
        # Production creates the profile row at sign-up through the web
        # client; local admin-created users need it for observation writes.
        sql(f"INSERT INTO public.profiles (id, username) VALUES ('{user_id}', "
            f"'harness_{user_id[:8]}') ON CONFLICT (id) DO NOTHING")
        return User(email, password, user_id)

    return _make


class Device:
    """A desktop install: its own app data dir, driven in fresh processes."""

    def __init__(self, root: Path, name: str):
        self.name = name
        self.app_dir = root / name
        self.app_dir.mkdir(parents=True)

    def run(self, action: str, **args) -> dict:
        env = {
            **os.environ,
            "SPORELY_APP_DATA_DIR": str(self.app_dir),
            "PYTHON_KEYRING_BACKEND": "keyring.backends.fail.Keyring",
            "QT_QPA_PLATFORM": "offscreen",
        }
        env.pop("SPORELY_PROFILE", None)
        done = subprocess.run(
            [sys.executable, str(HERE / "device.py"), action, json.dumps(args)],
            env=env, capture_output=True, text=True, timeout=300,
        )
        if done.returncode != 0:
            raise AssertionError(f"{self.name} {action} failed:\n{done.stderr[-4000:]}")
        output = json.loads(done.stdout.strip().splitlines()[-1])
        if output.get("non_local_requests"):
            raise AssertionError(
                f"{self.name} {action} attempted non-local requests: {output['non_local_requests']}"
            )
        return output

    @property
    def device_id(self) -> str | None:
        path = self.app_dir / "app_settings.json"
        if not path.exists():
            return None
        return json.loads(path.read_text()).get("reference_client_device_id")


@pytest.fixture()
def device(tmp_path):
    return lambda name: Device(tmp_path, name)
