"""F12: the profile avatar is fetched only from the Supabase storage host, with a size cap."""
from __future__ import annotations

import io
from types import SimpleNamespace

import pytest
from PySide6.QtCore import QObject, QUrl, Signal
from PySide6.QtGui import QColor, QImage
from PySide6.QtNetwork import QNetworkReply, QNetworkRequest

from utils.cloud_sync import SUPABASE_URL

GOOD = f"{SUPABASE_URL}/storage/v1/object/public/avatars/abc/avatar.jpg"


class FakeReply(QObject):
    finished = Signal()
    downloadProgress = Signal("qint64", "qint64")

    def __init__(self, url, body=b"", redirect=None):
        super().__init__()
        self._url = QUrl(url)
        self._body = body
        self._redirect = redirect
        self.aborted = False

    def error(self):
        return QNetworkReply.NetworkError.NoError

    def url(self):
        return self._url

    def attribute(self, attr):
        if attr == QNetworkRequest.Attribute.RedirectionTargetAttribute and self._redirect:
            return QUrl(self._redirect)
        return None

    def readAll(self):
        return self._body

    def abort(self):
        self.aborted = True

    def deleteLater(self):
        pass


class FakeNetwork:
    def __init__(self, replies=None):
        self.requests = []
        self.replies = list(replies or [])

    def get(self, request):
        self.requests.append(request.url().toString())
        reply = self.replies.pop(0) if self.replies else FakeReply(request.url().toString())
        return reply


def _jpeg() -> bytes:
    from PySide6.QtCore import QBuffer, QIODevice

    image = QImage(8, 8, QImage.Format.Format_RGB32)
    image.fill(QColor("green"))
    buf = QBuffer()
    buf.open(QIODevice.OpenModeFlag.WriteOnly)
    image.save(buf, "PNG")
    return bytes(buf.data())


def _stub(network):
    applied = []
    stub = SimpleNamespace(
        _profile_avatar_network=network,
        _profile_avatar_url="",
        _profile_avatar_pending_url="",
        _profile_avatar_loaded_url="",
        _profile_avatar_pixmap_cached=None,
        _apply_profile_avatar=lambda pm: applied.append(pm),
        applied=applied,
    )
    # bind the real methods to the stub
    from ui.main_window import SettingsHubDialog as MainWindow

    for name in ("_fetch_and_set_profile_avatar", "_handle_profile_avatar_reply", "_request_profile_avatar"):
        fn = getattr(MainWindow, name, None)
        if fn is not None:
            setattr(stub, name, fn.__get__(stub))
    return stub


@pytest.mark.parametrize(
    "url",
    [
        "https://evil.example/a.jpg",
        "http://" + SUPABASE_URL.split("://", 1)[1] + "/storage/v1/object/public/avatars/a.jpg",
        "https://169.254.169.254/latest/meta-data",
        SUPABASE_URL + ".evil.example/storage/v1/x.jpg",
        "file:///etc/passwd",
    ],
)
def test_other_hosts_are_not_requested(qapp, url):
    net = FakeNetwork()
    stub = _stub(net)
    stub._profile_avatar_url = url
    stub._fetch_and_set_profile_avatar(url)
    assert net.requests == []


def test_supabase_storage_url_is_fetched_and_applied(qapp):
    reply = FakeReply(GOOD, body=_jpeg())
    net = FakeNetwork([reply])
    stub = _stub(net)
    stub._profile_avatar_url = GOOD
    stub._fetch_and_set_profile_avatar(GOOD)
    assert net.requests == [GOOD]
    reply.finished.emit()
    assert len(stub.applied) == 1


def test_oversized_reply_is_aborted_and_refused(qapp):
    big = b"\0" * (6 * 1024 * 1024)
    reply = FakeReply(GOOD, body=big)
    net = FakeNetwork([reply])
    stub = _stub(net)
    stub._profile_avatar_url = GOOD
    stub._fetch_and_set_profile_avatar(GOOD)
    reply.downloadProgress.emit(len(big), len(big))
    assert reply.aborted
    reply.finished.emit()
    assert stub.applied == []


def test_redirect_to_other_host_is_refused(qapp):
    reply = FakeReply(GOOD, redirect="https://evil.example/a.jpg")
    net = FakeNetwork([reply])
    stub = _stub(net)
    stub._profile_avatar_url = GOOD
    stub._fetch_and_set_profile_avatar(GOOD)
    reply.finished.emit()
    assert net.requests == [GOOD]
    assert stub.applied == []


def test_redirect_within_storage_host_is_followed(qapp):
    target = f"{SUPABASE_URL}/storage/v1/object/authenticated/avatars/abc/avatar.jpg"
    first = FakeReply(GOOD, redirect=target)
    second = FakeReply(target, body=_jpeg())
    net = FakeNetwork([first, second])
    stub = _stub(net)
    stub._profile_avatar_url = GOOD
    stub._fetch_and_set_profile_avatar(GOOD)
    first.finished.emit()
    assert net.requests == [GOOD, target]
    second.finished.emit()
    assert len(stub.applied) == 1
