"""Focused tests for the Sporely-Worker Artsorakel integration."""
from __future__ import annotations

import os
import sys
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication

import ui.image_import_dialog as image_import_dialog
import utils.ai_image_prep as ai_image_prep


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


class _FakeResponse:
    def __init__(self, payload: dict, status_code: int = 200) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = ""
        self.closed = False

    def json(self):
        return self._payload

    def close(self) -> None:
        self.closed = True


def _make_worker(
    tmp_path: Path,
    *,
    access_token: str = "supabase-access-token",
    latitude=None,
    longitude=None,
    image_count: int = 1,
):
    source_paths = [tmp_path / f"source-{idx}.jpg" for idx in range(image_count)]
    for path in source_paths:
        path.write_bytes(b"source")
    temp_dir = tmp_path / "ai"
    temp_dir.mkdir(parents=True, exist_ok=True)

    prepared_paths: list[Path] = []

    def fake_prepare(self, image_path, crop_box):
        prepared_path = temp_dir / f"prepared-{len(prepared_paths)}.jpg"
        prepared_path.write_bytes(f"prepared:{image_path}".encode("utf-8"))
        prepared_paths.append(prepared_path)
        return SimpleNamespace(
            path=prepared_path,
            original_size=(10, 10),
            crop_box=crop_box,
            crop_pixels=None,
            final_size=(10, 10),
            sha256="a" * 64,
            byte_size=prepared_path.stat().st_size,
        )

    return (
        image_import_dialog.AIGuessWorker(
            [
                {"index": idx, "image_path": str(path), "crop_box": None}
                for idx, path in enumerate(source_paths)
            ],
            temp_dir,
            max_dim=ai_image_prep.DEFAULT_ARTSORAKEL_MAX_DIM,
            access_token=access_token,
            latitude=latitude,
            longitude=longitude,
        ),
        fake_prepare,
        prepared_paths,
        source_paths,
    )


def _stub_requests(monkeypatch, captured: dict, *, payload=None):
    payload = payload if payload is not None else {"predictions": []}

    def fake_post(url, *, files, headers, data, timeout):
        captured["url"] = url
        captured["files"] = list(files)
        captured["headers"] = dict(headers)
        captured["data"] = dict(data)
        captured["timeout"] = timeout
        return _FakeResponse(payload)

    monkeypatch.setitem(sys.modules, "requests", SimpleNamespace(post=fake_post))


def test_worker_posts_to_sporely_worker_with_supabase_bearer(monkeypatch, qapp, tmp_path):
    worker, fake_prepare, _prepared, _sources = _make_worker(tmp_path)
    monkeypatch.setattr(
        image_import_dialog.AIGuessWorker, "_prepare_image", fake_prepare, raising=False
    )
    monkeypatch.setattr(
        ai_image_prep, "debug_log_prepared_ai_request_image", lambda *a, **k: None
    )

    captured: dict = {}
    _stub_requests(monkeypatch, captured)

    worker.run()

    assert captured["url"] == "https://upload.sporely.no/artsorakel"
    assert not captured["url"].startswith("https://ai.artsdatabanken.no")
    assert captured["headers"]["Authorization"] == "Bearer supabase-access-token"
    assert captured["headers"]["User-Agent"] == "Sporely/AI"


def test_worker_forwards_image_and_application_multipart(monkeypatch, qapp, tmp_path):
    worker, fake_prepare, prepared_paths, _sources = _make_worker(
        tmp_path, image_count=2
    )
    monkeypatch.setattr(
        image_import_dialog.AIGuessWorker, "_prepare_image", fake_prepare, raising=False
    )
    monkeypatch.setattr(
        ai_image_prep, "debug_log_prepared_ai_request_image", lambda *a, **k: None
    )

    captured: dict = {}
    _stub_requests(monkeypatch, captured)

    worker.run()

    assert captured["data"]["application"] == "Sporely"
    field_names = [field_name for field_name, _part in captured["files"]]
    assert field_names == ["image", "image"]
    file_names = [part[0] for _field_name, part in captured["files"]]
    assert file_names == [path.name for path in prepared_paths]


def test_worker_forwards_lat_lon_rounded_to_one_decimal(monkeypatch, qapp, tmp_path):
    worker, fake_prepare, _prepared, _sources = _make_worker(
        tmp_path,
        latitude=59.123456,
        longitude=10.987654,
    )
    monkeypatch.setattr(
        image_import_dialog.AIGuessWorker, "_prepare_image", fake_prepare, raising=False
    )
    monkeypatch.setattr(
        ai_image_prep, "debug_log_prepared_ai_request_image", lambda *a, **k: None
    )

    captured: dict = {}
    _stub_requests(monkeypatch, captured)

    worker.run()

    assert captured["data"]["latitude"] == "59.1"
    assert captured["data"]["longitude"] == "11.0"


@pytest.mark.parametrize(
    ("lat", "lon"),
    [
        (None, 10.0),
        (59.0, None),
        (None, None),
        (float("nan"), 10.0),
        (91.0, 10.0),
        (59.0, 181.0),
        ("not-a-number", 10.0),
    ],
)
def test_worker_omits_partial_or_invalid_coordinates(monkeypatch, qapp, tmp_path, lat, lon):
    worker, fake_prepare, _prepared, _sources = _make_worker(
        tmp_path, latitude=lat, longitude=lon
    )
    monkeypatch.setattr(
        image_import_dialog.AIGuessWorker, "_prepare_image", fake_prepare, raising=False
    )
    monkeypatch.setattr(
        ai_image_prep, "debug_log_prepared_ai_request_image", lambda *a, **k: None
    )

    captured: dict = {}
    _stub_requests(monkeypatch, captured)

    worker.run()

    assert "latitude" not in captured["data"]
    assert "longitude" not in captured["data"]


def test_worker_without_access_token_reports_error(monkeypatch, qapp, tmp_path):
    worker, fake_prepare, _prepared, _sources = _make_worker(tmp_path, access_token="")
    monkeypatch.setattr(
        image_import_dialog.AIGuessWorker, "_prepare_image", fake_prepare, raising=False
    )

    called: dict = {"posted": False}

    def fake_post(*_a, **_k):
        called["posted"] = True
        raise AssertionError("worker must not POST without an access token")

    monkeypatch.setitem(sys.modules, "requests", SimpleNamespace(post=fake_post))

    errors: list[tuple[list, str]] = []
    worker.error.connect(lambda indices, message: errors.append((list(indices), message)))

    worker.run()

    assert called["posted"] is False
    assert errors, "worker should emit an error when access_token is missing"


def test_worker_parses_current_prediction_shape(monkeypatch, qapp, tmp_path):
    worker, fake_prepare, _prepared, _sources = _make_worker(tmp_path)
    monkeypatch.setattr(
        image_import_dialog.AIGuessWorker, "_prepare_image", fake_prepare, raising=False
    )
    monkeypatch.setattr(
        ai_image_prep, "debug_log_prepared_ai_request_image", lambda *a, **k: None
    )

    payload = {
        "predictions": [
            {
                "taxa": {
                    "items": [
                        {
                            "scientific_name_id": "NBIC:1",
                            "scientificName": "Amanita regalis",
                            "probability": 0.9,
                            "taxon": {"vernacularName": "Kongefluesopp"},
                        },
                        {
                            "scientific_name_id": "NBIC:2",
                            "scientificName": "Amanita muscaria",
                            "probability": 0.4,
                            "taxon": {"vernacularName": "Rød fluesopp"},
                        },
                    ]
                }
            }
        ]
    }
    captured: dict = {}
    _stub_requests(monkeypatch, captured, payload=payload)

    emitted: list = []
    worker.resultReady.connect(lambda *args: emitted.append(args))

    worker.run()

    assert emitted, "expected one result batch"
    _indices, predictions, _box, _warnings, _paths = emitted[0]
    assert [pred["scientific_name_id"] for pred in predictions] == ["NBIC:1", "NBIC:2"]


def test_no_artsdatabanken_service_token_in_client(monkeypatch):
    """Ensure the Artsdatabanken bearer token is never embedded client-side."""
    root = Path(__file__).resolve().parents[1]

    forbidden_tokens = (
        "ARTSORAKEL_API_TOKEN",
        "ARTSORAKEL_BEARER",
        "ARTSDATABANKEN_TOKEN",
        "ARTSDATABANKEN_API_TOKEN",
    )
    scanned_files = 0
    _skip_prefixes = ("tests/", "__pycache__/", ".claude/", ".git/", "references/")
    for path in root.rglob("*.py"):
        rel = path.relative_to(root).as_posix()
        if rel.startswith(_skip_prefixes):
            continue
        if "/__pycache__/" in rel:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        scanned_files += 1
        for needle in forbidden_tokens:
            assert needle not in text, (
                f"Artsdatabanken service token reference {needle!r} found in {rel} — "
                "the desktop app must proxy through the Sporely Worker."
            )
        assert "ai.artsdatabanken.no" not in text, (
            f"Direct Artsdatabanken root endpoint reference in {rel} — "
            "must go through the Sporely Worker's /artsorakel proxy."
        )
    assert scanned_files > 0
