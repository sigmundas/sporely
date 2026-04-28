import pytest

from utils import cloud_sync


class DummyClient:
    def __init__(self, user_id: str):
        self.user_id = user_id

    def fetch_current_user_id(self) -> str:
        return self.user_id


def test_cloud_account_lock_binds_first_sync(monkeypatch):
    settings = {}
    updates = []

    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings))

    def update_app_settings(update):
        updates.append(dict(update))
        settings.update(update)
        return dict(settings)

    monkeypatch.setattr(cloud_sync, "update_app_settings", update_app_settings)

    user_id = cloud_sync.ensure_database_linked_to_cloud_user(DummyClient("user-a"))

    assert user_id == "user-a"
    assert settings["linked_cloud_user_id"] == "user-a"
    assert updates == [{"linked_cloud_user_id": "user-a"}]


def test_cloud_account_lock_allows_same_account(monkeypatch):
    settings = {"linked_cloud_user_id": "user-a"}

    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings))
    monkeypatch.setattr(
        cloud_sync,
        "update_app_settings",
        lambda update: pytest.fail("existing cloud link should not be rewritten"),
    )

    assert cloud_sync.ensure_database_linked_to_cloud_user(DummyClient("user-a")) == "user-a"


def test_cloud_account_lock_blocks_different_account(monkeypatch):
    settings = {"linked_cloud_user_id": "user-a"}

    monkeypatch.setattr(cloud_sync, "get_app_settings", lambda: dict(settings))
    monkeypatch.setattr(
        cloud_sync,
        "update_app_settings",
        lambda update: pytest.fail("mismatched account must not rebind the database"),
    )

    with pytest.raises(cloud_sync.AccountMismatchError) as exc_info:
        cloud_sync.ensure_database_linked_to_cloud_user(DummyClient("user-b"))

    assert str(exc_info.value) == cloud_sync.ACCOUNT_MISMATCH_MESSAGE

