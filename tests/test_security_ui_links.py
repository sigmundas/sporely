"""F11: links from the AI service and the update feed are validated before use."""
from __future__ import annotations

from types import SimpleNamespace

import pytest
from PySide6.QtNetwork import QNetworkReply

pytest.importorskip("PySide6")

GOOD_ADB = "https://artsdatabanken.no/arter/takson/123"
GOOD_INAT = "https://www.inaturalist.org/taxa/47347"
BAD_URLS = [
    "https://evil.example/phish",
    "https://artsdatabanken.no.evil.example/x",
    "http://artsdatabanken.no/arter",
    "javascript:alert(1)",
    "httpfoo://artsdatabanken.no",
    'https://artsdatabanken.no/x"><b>hi</b>',
    "https://creds:pw\x40artsdatabanken.no/x",
    "https://artsdatabanken.no:8443/x",
    "file:///etc/passwd",
]


def _import_dialog_cls():
    from ui.image_import_dialog import ImageImportDialog

    return ImageImportDialog


def _details_dialog_cls():
    from ui.observation_details_dialog import ObservationDetailsDialog

    return ObservationDetailsDialog


@pytest.mark.parametrize("bad", BAD_URLS)
def test_import_dialog_link_falls_back(bad):
    cls = _import_dialog_cls()
    assert cls._ai_prediction_link(None, {"infoURL": bad}, {}) == "https://artsdatabanken.no"
    assert cls._ai_prediction_link(None, {}, {"url": bad}) == "https://artsdatabanken.no"


def test_import_dialog_link_valid_kept():
    cls = _import_dialog_cls()
    assert cls._ai_prediction_link(None, {"infoURL": GOOD_ADB}, {}) == GOOD_ADB
    assert cls._ai_prediction_link(None, {}, {"url": GOOD_INAT}) == GOOD_INAT


def test_import_dialog_label_escapes_url(qapp):
    cls = _import_dialog_cls()
    label = cls._build_adb_link_widget(None, 'https://artsdatabanken.no/x"><b>hi</b>')
    # an unvalidated URL must never reach the rich text unescaped
    assert label is None or '"><b>' not in label.text()


@pytest.mark.parametrize("bad", BAD_URLS)
def test_details_dialog_link_rejects_bad(bad):
    cls = _details_dialog_cls()
    assert cls._ai_prediction_link(None, {"url": bad}, {}) is None


def test_details_dialog_link_valid_kept():
    cls = _details_dialog_cls()
    assert cls._ai_prediction_link(None, {"url": GOOD_ADB}, {}) == GOOD_ADB
    assert cls._ai_prediction_link(None, {}, {"id": "47347"}, source="inat") == GOOD_INAT


def test_details_dialog_click_does_not_open_bad_url(monkeypatch):
    import ui.observation_details_dialog as mod

    opened = []
    monkeypatch.setattr(mod.QDesktopServices, "openUrl", lambda u: opened.append(u.toString()))

    class Item:
        def __init__(self, url):
            self.url = url

        def data(self, role):
            return self.url

    def run(url):
        table = SimpleNamespace(columnCount=lambda: 6, item=lambda r, c: Item(url))
        stub = SimpleNamespace(ai_tables={"arts": table})
        mod.ObservationDetailsDialog._on_ai_table_cell_clicked(stub, "arts", 0, 3)

    run("https://evil.example/x")
    assert opened == []
    run(GOOD_ADB)
    assert opened == [GOOD_ADB]


class _Reply:
    def __init__(self, href):
        self.payload = (
            '<feed xmlns="http://www.w3.org/2005/Atom"><entry><title>v9.9.9</title>'
            f'<link rel="alternate" href="{href}"/></entry></feed>'
        ).encode()

    def error(self):
        return QNetworkReply.NetworkError.NoError

    def readAll(self):
        return self.payload

    def deleteLater(self):
        pass


def _run_atom(href):
    from app_identity import APP_REPOSITORY_URL
    from ui.main_window import MainWindow

    shown = []
    stub = SimpleNamespace(
        _is_newer_version=lambda latest, cur: True,
        _show_update_dialog=lambda version, url: shown.append(url),
    )
    MainWindow._handle_atom_reply(stub, _Reply(href), (0,))
    return shown, APP_REPOSITORY_URL


@pytest.mark.parametrize(
    "href",
    ["https://evil.example/releases/tag/v9", "http://github.com/sigmundas/sporely/releases/tag/v9",
     "https://github.com/other/repo/releases/tag/v9", "javascript:alert(1)"],
)
def test_update_feed_bad_href_falls_back(href):
    shown, repo = _run_atom(href)
    assert shown == [f"{repo}/releases/latest"]


def test_update_feed_valid_href_kept():
    from app_identity import APP_REPOSITORY_URL

    href = f"{APP_REPOSITORY_URL}/releases/tag/v9.9.9"
    shown, _ = _run_atom(href)
    assert shown == [href]


@pytest.mark.parametrize(
    "bad",
    [
        "https://github.com/sigmundas/sporely/releases/../../other/repo/releases/tag/v9",
        "https://github.com/sigmundas/sporely/releases/%2e%2e/%2e%2e/other",
        "https://github.com/sigmundas/sporely/releases/..%2fx",
    ],
)
def test_update_feed_dot_segments_fall_back(bad):
    shown, repo = _run_atom(bad)
    assert shown == [f"{repo}/releases/latest"]


def test_dot_segments_rejected_for_ai_and_avatar_links():
    from utils.cloud_sync import SUPABASE_URL
    from utils.safe_links import safe_ai_link, safe_avatar_url

    assert safe_ai_link("https://artsdatabanken.no/arter/../x") is None
    assert safe_avatar_url(f"{SUPABASE_URL}/storage/v1/../rest/v1/profiles") is None
    assert safe_avatar_url(f"{SUPABASE_URL}/storage/v1/object/public/avatars/a/avatar.jpg")
