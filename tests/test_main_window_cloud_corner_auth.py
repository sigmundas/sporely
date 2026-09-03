from types import SimpleNamespace

import ui.main_window as main_window


def test_corner_cloud_state_ignores_retained_profile_and_database_link_without_session():
    settings = {
        "cloud_user_email": "user@example.com",
        "linked_cloud_user_id": "linked-user",
    }

    assert main_window._has_active_or_reloadable_cloud_session(settings, None) is False


def test_corner_cloud_state_accepts_active_or_reloadable_session():
    assert main_window._has_active_or_reloadable_cloud_session({}, SimpleNamespace(user_id="user")) is True
    assert main_window._has_active_or_reloadable_cloud_session(
        {"cloud_access_token": "access", "cloud_user_id": "user"}, None
    ) is True
    assert main_window._has_active_or_reloadable_cloud_session(
        {"cloud_refresh_token": "refresh"}, None
    ) is True


def test_logout_clears_cached_corner_avatar():
    owner = SimpleNamespace(_avatar_pixmap_cached=object())

    main_window._clear_cached_cloud_avatar(owner)

    assert not hasattr(owner, "_avatar_pixmap_cached")
