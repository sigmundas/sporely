"""The main window's corner shows the signed-in account's photo, not another's.

The corner fetches the profile photo in the background. A slow reply for
account A, arriving after switching to account B, used to be kept and shown
for B: the reply only checked that someone was signed in. The cached photo
was not tied to an account either. Both are now tied to the user id.

Real ``MainWindow`` in an isolated profile (``tests/test_files_source.py``);
the network manager is a stand-in whose replies finish when the test says.
"""
from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QBuffer, QByteArray, QIODevice, QObject, Signal
from PySide6.QtGui import QColor, QPixmap
from PySide6.QtNetwork import QNetworkReply

import ui.main_window as main_window
from tests.test_files_source import profile, window  # noqa: F401


def _png(color: str) -> QByteArray:
    pixmap = QPixmap(8, 8)
    pixmap.fill(QColor(color))
    data = QByteArray()
    buffer = QBuffer(data)
    buffer.open(QIODevice.WriteOnly)
    pixmap.save(buffer, "PNG")
    return data


class _Reply(QObject):
    finished = Signal()

    def __init__(self, url: str):
        super().__init__()
        self.url = url
        self.body = QByteArray()

    def error(self):
        return QNetworkReply.NoError

    def readAll(self):  # noqa: N802 - Qt API
        return self.body

    def answer(self, color: str) -> None:
        self.body = _png(color)
        self.finished.emit()


class _Network:
    def __init__(self):
        self.replies: list[_Reply] = []

    def get(self, request):
        reply = _Reply(request.url().toString())
        self.replies.append(reply)
        return reply

    def for_user(self, user_id: str) -> list[_Reply]:
        return [reply for reply in self.replies if f"/avatars/{user_id}/" in reply.url]


@pytest.fixture
def corner(window):  # noqa: F811
    network = _Network()
    window._avatar_network = network
    main_window._clear_cached_cloud_avatar(window)
    yield window, network
    window._cloud_client = None
    main_window._clear_cached_cloud_avatar(window)


def _sign_in(window, user_id: str) -> None:
    """The client Preferanser hands the main window; the old photo is dropped."""
    window._cloud_client = SimpleNamespace(user_id=user_id, access_token=f"token-{user_id}")
    main_window._clear_cached_cloud_avatar(window)
    window._update_corner_ui()


def _icon(window) -> int:
    return window._avatar_corner_btn.icon().cacheKey()


def test_a_slow_photo_of_the_previous_account_is_not_shown(corner):
    window, network = corner
    _sign_in(window, "user-A")
    [reply_a] = network.for_user("user-A")
    _sign_in(window, "user-B")  # before A's photo arrived
    shown = _icon(window)

    reply_a.answer("red")

    assert _icon(window) == shown  # still B's initials
    assert not hasattr(window, "_avatar_pixmap_cached")

    [reply_b] = network.for_user("user-B")
    reply_b.answer("blue")

    assert _icon(window) != shown
    assert window._avatar_cached_user_id == "user-B"


def test_a_cached_photo_of_another_account_is_not_reused(corner):
    """Even if a path changes account without dropping the cache."""
    window, network = corner
    _sign_in(window, "user-A")
    network.for_user("user-A")[0].answer("red")
    assert window._avatar_cached_user_id == "user-A"

    window._cloud_client = SimpleNamespace(user_id="user-B", access_token="token-B")  # cache kept
    window._update_corner_ui()

    assert network.for_user("user-B"), "B's photo is fetched"
    assert not hasattr(window, "_avatar_pixmap_cached")  # A's is gone


def test_the_accounts_own_photo_shows_and_is_then_reused(corner):
    window, network = corner
    _sign_in(window, "user-A")
    window._update_corner_ui()  # a second request while the first is out
    first, _second = network.for_user("user-A")
    initials = _icon(window)

    first.answer("red")
    assert _icon(window) != initials and window._avatar_cached_user_id == "user-A"

    requests = len(network.replies)
    window._update_corner_ui()
    assert len(network.replies) == requests  # the cached photo, no new request
