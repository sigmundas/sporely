"""A settings update cannot write back a snapshot taken before a sign-out."""

from __future__ import annotations

import threading

from database import schema


def test_concurrent_update_does_not_restore_cleared_tokens(monkeypatch, tmp_path):
    monkeypatch.setattr(schema, "SETTINGS_PATH", tmp_path / "app_settings.json")
    schema.save_app_settings({"cloud_access_token": "token-a", "ui_theme": "light"})

    real_load = schema._load_app_settings
    worker_loaded = threading.Event()
    release_worker = threading.Event()

    def slow_load():
        settings = real_load()
        if threading.current_thread().name == "worker":
            worker_loaded.set()
            release_worker.wait(5)
        return settings

    monkeypatch.setattr(schema, "_load_app_settings", slow_load)

    worker = threading.Thread(
        target=lambda: schema.update_app_settings({"ui_theme": "dark"}), name="worker"
    )
    worker.start()
    assert worker_loaded.wait(5)

    sign_out = threading.Thread(
        target=lambda: schema.update_app_settings({"cloud_access_token": None}), name="sign-out"
    )
    sign_out.start()
    sign_out.join(0.2)
    release_worker.set()
    worker.join(5)
    sign_out.join(5)

    final = real_load()
    assert final["cloud_access_token"] is None, "the sign-out is not undone"
    assert final["ui_theme"] == "dark"
