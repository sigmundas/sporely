from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PIL import Image
from PySide6.QtWidgets import QApplication

import ui.image_import_dialog as image_import_dialog
import ui.observations_tab as observations_tab
from ui.image_import_dialog import ImageImportResult
import utils.ai_image_prep as ai_image_prep
import utils.inat_oauth as inat_oauth


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


def _make_oriented_image(path: Path) -> Path:
    image = Image.new("RGB", (4, 2))
    for x in range(4):
        for y in range(2):
            image.putpixel((x, y), (x * 50, y * 120, 40))
    exif = image.getexif()
    exif[274] = 6
    image.save(path, exif=exif, quality=95)
    return path


def _make_dialog_patches(monkeypatch) -> None:
    fake_client = SimpleNamespace(
        user_id="user-123",
        fetch_cloud_plan_profile=lambda: {"cloud_plan": "free", "is_pro": False},
        count_remote_privacy_slots=lambda: 0,
        list_remote_observations=lambda: [],
    )
    monkeypatch.setattr(
        observations_tab.SettingsDB,
        "get_setting",
        lambda key, default=None: "en" if key == "vernacular_language" else default,
    )
    monkeypatch.setattr(observations_tab, "resolve_vernacular_db_path", lambda _lang: None)
    monkeypatch.setattr(
        observations_tab.ObservationDetailsDialog,
        "_load_objectives",
        lambda self: {"default": {"is_default": True}},
    )
    monkeypatch.setattr(
        observations_tab.ObservationDetailsDialog,
        "_load_tag_options",
        lambda self, category: [f"{category}-default"],
    )
    monkeypatch.setattr(
        observations_tab.ObservationDetailsDialog,
        "_load_habitat_tree",
        lambda self, filename: [],
    )
    monkeypatch.setattr(observations_tab.ObservationDetailsDialog, "_apply_primary_metadata", lambda self: None)
    monkeypatch.setattr(observations_tab.ObservationDetailsDialog, "_apply_suggested_taxon", lambda self: None)
    monkeypatch.setattr(observations_tab.ObservationDetailsDialog, "_sync_taxon_cache", lambda self: None)
    monkeypatch.setattr(
        observations_tab.ObservationDetailsDialog,
        "_complete_deferred_dialog_setup",
        lambda self: None,
    )
    monkeypatch.setattr(observations_tab.SporelyCloudClient, "from_stored_credentials", lambda: fake_client)


def _build_dialog(
    monkeypatch,
    qapp,
    *,
    image_path: Path,
    image_results: list[ImageImportResult] | None = None,
    observation: dict | None = None,
    draft_data: dict | None = None,
):
    _make_dialog_patches(monkeypatch)
    dialog = observations_tab.ObservationDetailsDialog(
        parent=None,
        observation=observation,
        draft_data=draft_data,
        image_results=image_results
        if image_results is not None
        else [ImageImportResult(filepath=str(image_path), image_type="field")],
    )
    qapp.processEvents()
    return dialog


class _FakeSignal:
    def connect(self, *_args, **_kwargs) -> None:
        return None


def _recording_worker_class():
    class RecordingWorker:
        instances: list["RecordingWorker"] = []

        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
            self.started = False
            self.resultReady = _FakeSignal()
            self.error = _FakeSignal()
            self.finished = _FakeSignal()
            RecordingWorker.instances.append(self)

        def start(self) -> None:
            self.started = True

        def deleteLater(self) -> None:
            return None

        def quit(self) -> None:
            return None

        def wait(self, _timeout: int = 0) -> bool:
            return True

        def isRunning(self) -> bool:
            return False

        def requestInterruption(self) -> None:
            return None

    return RecordingWorker


class _FailWorker:
    def __init__(self, *_args, **_kwargs):
        raise AssertionError("unexpected worker creation")


def test_prepare_ai_request_image_transposes_crops_and_resizes(tmp_path: Path) -> None:
    source = _make_oriented_image(tmp_path / "source.jpg")
    temp_dir = tmp_path / "ai"

    prepared = ai_image_prep.prepare_ai_request_image(
        source,
        crop_box=(0.25, 0.25, 0.75, 0.75),
        temp_dir=temp_dir,
        prefix="specimen",
        max_dim=1,
        jpeg_quality=90,
    )
    prepared_again = ai_image_prep.prepare_ai_request_image(
        source,
        crop_box=(0.25, 0.25, 0.75, 0.75),
        temp_dir=temp_dir,
        prefix="specimen",
        max_dim=1,
        jpeg_quality=90,
    )

    assert prepared.original_size == (2, 4)
    assert prepared.crop_box == (0.25, 0.25, 0.75, 0.75)
    assert prepared.crop_pixels == (0, 1, 2, 3)
    assert prepared.final_size == (1, 1)
    assert prepared.path.exists()
    assert prepared.byte_size == prepared.path.stat().st_size
    assert len(prepared.sha256) == 64
    assert prepared_again.original_size == prepared.original_size
    assert prepared_again.crop_pixels == prepared.crop_pixels
    assert prepared_again.final_size == prepared.final_size
    assert prepared_again.byte_size == prepared.byte_size
    assert prepared_again.sha256 == prepared.sha256


def test_default_ai_crop_rect_matches_sporely_web() -> None:
    rect = ai_image_prep.get_default_ai_crop_rect(1000, 500)

    assert rect == pytest.approx((0.31, 0.12, 0.69, 0.88))


def test_ai_workers_delegate_to_shared_preparation_helper(monkeypatch, tmp_path: Path) -> None:
    calls: list[tuple[str, tuple[float, float, float, float] | None, Path, str, int, int]] = []

    def fake_prepare(
        image_path,
        crop_box,
        temp_dir,
        prefix,
        max_dim=ai_image_prep.DEFAULT_ARTSORAKEL_MAX_DIM,
        jpeg_quality=90,
    ):
        calls.append((str(image_path), crop_box, Path(temp_dir), prefix, max_dim, jpeg_quality))
        return SimpleNamespace(
            path=Path(temp_dir) / f"{prefix}.jpg",
            original_size=(10, 20),
            crop_box=crop_box,
            crop_pixels=None,
            final_size=(10, 20),
            sha256="a" * 64,
            byte_size=123,
        )

    monkeypatch.setattr(ai_image_prep, "prepare_ai_request_image", fake_prepare)

    import_worker = SimpleNamespace(
        temp_dir=tmp_path / "import",
        max_dim=ai_image_prep.DEFAULT_ARTSORAKEL_MAX_DIM,
    )
    inat_worker = SimpleNamespace(temp_dir=tmp_path / "inat", max_dim=777)

    import_result = image_import_dialog.AIGuessWorker._prepare_image(
        import_worker,
        "source-a.jpg",
        (0.1, 0.2, 0.3, 0.4),
    )
    inat_result = observations_tab.INatAIGuessWorker._prepare_image(
        inat_worker,
        "source-b.jpg",
        None,
    )

    assert import_result.path == tmp_path / "import" / "ai_guess.jpg"
    assert inat_result.path == tmp_path / "inat" / "inat_ai_guess.jpg"
    assert calls == [
        (
            "source-a.jpg",
            (0.1, 0.2, 0.3, 0.4),
            tmp_path / "import",
            "ai_guess",
            ai_image_prep.DEFAULT_ARTSORAKEL_MAX_DIM,
            90,
        ),
        ("source-b.jpg", None, tmp_path / "inat", "inat_ai_guess", 777, 90),
    ]


def test_artsorakel_worker_batches_selected_images_and_flattens_combined_response(
    monkeypatch,
    qapp,
    tmp_path: Path,
) -> None:
    reference_path = Path(__file__).resolve().parents[1] / "database" / "reference_data" / "sources" / "artsorakel_3images.txt"
    reference_text = reference_path.read_text()
    reference_payload = json.loads(reference_text[reference_text.index("{"):])

    source_paths = [tmp_path / f"source-{idx}.jpg" for idx in range(3)]
    for path in source_paths:
        path.write_bytes(b"source")

    temp_dir = tmp_path / "ai"
    prepared_paths: list[Path] = []

    def fake_prepare(self, image_path, crop_box):
        prepared_path = temp_dir / f"prepared-{len(prepared_paths)}.jpg"
        prepared_path.parent.mkdir(parents=True, exist_ok=True)
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

    monkeypatch.setattr(image_import_dialog.AIGuessWorker, "_prepare_image", fake_prepare, raising=False)
    monkeypatch.setattr(ai_image_prep, "debug_log_prepared_ai_request_image", lambda *args, **kwargs: None)

    captured: dict[str, object] = {}

    class FakeResponse:
        def __init__(self, payload: dict) -> None:
            self.status_code = 200
            self._payload = payload
            self.text = ""
            self.closed = False

        def json(self):
            return self._payload

        def close(self) -> None:
            self.closed = True

    def fake_post(url, *, files, headers, data, timeout):
        captured["url"] = url
        captured["files"] = files
        captured["headers"] = headers
        captured["data"] = data
        captured["timeout"] = timeout
        return FakeResponse(reference_payload)

    monkeypatch.setitem(sys.modules, "requests", SimpleNamespace(post=fake_post))

    worker = image_import_dialog.AIGuessWorker(
        [
            {"index": idx, "image_path": str(path), "crop_box": None}
            for idx, path in enumerate(source_paths)
        ],
        temp_dir,
        max_dim=ai_image_prep.DEFAULT_ARTSORAKEL_MAX_DIM,
    )

    emitted: list[tuple[list, list, object, object, list]] = []
    worker.resultReady.connect(lambda *args: emitted.append(args))

    worker.run()

    assert captured["url"] == "https://ai.artsdatabanken.no"
    assert captured["data"] == {"application": "Sporely"}
    assert captured["headers"] == {"User-Agent": "Sporely/AI"}
    assert isinstance(captured["files"], list)
    assert len(captured["files"]) == 3
    assert [field_name for field_name, _part in captured["files"]] == ["image", "image", "image"]
    assert [part[0] for _field_name, part in captured["files"]] == [path.name for path in prepared_paths]

    assert len(emitted) == 1
    indices, predictions, _box, warnings, temp_paths = emitted[0]
    assert indices == [0, 1, 2]
    assert [pred["scientificName"] for pred in predictions[:3]] == [
        "Entoloma",
        "Entoloma asprellum",
        "Entoloma sericeum",
    ]
    assert [pred["scientific_name_id"] for pred in predictions[:3]] == [
        "NBIC:53377",
        "NBIC:53442",
        "NBIC:53669",
    ]
    assert warnings is not None
    assert isinstance(warnings, dict)
    assert isinstance(temp_paths, list)
    assert temp_paths == [str(path) for path in prepared_paths]
    assert all(not path.exists() for path in prepared_paths)


def test_image_import_dialog_normalizes_ai_prediction_taxon_from_raw_payload() -> None:
    prediction = {
        "scientific_name": "Amanita regalis",
        "vernacularName": "Royal fly agaric",
        "vernacularNames": {"no": "Kongefluesopp", "en": "Royal fly agaric"},
        "taxon": {"id": 123, "vernacularName": "Royal fly agaric"},
        "probability": 0.84,
    }

    taxon = image_import_dialog.ImageImportDialog._normalized_ai_prediction_taxon(SimpleNamespace(), prediction)

    assert taxon["scientificName"] == "Amanita regalis"
    assert taxon["scientific_name"] == "Amanita regalis"
    assert taxon["vernacularNames"]["no"] == "Kongefluesopp"
    assert taxon["vernacularName"] == "Royal fly agaric"
    assert taxon["id"] == 123


@pytest.mark.parametrize(
    ("source", "expected_attr", "unexpected_attr"),
    [
        ("arts", "_ai_thread", "_inat_ai_thread"),
        ("inat", "_inat_ai_thread", "_ai_thread"),
    ],
)
def test_provider_specific_guess_only_starts_requested_worker(
    monkeypatch,
    qapp,
    tmp_path: Path,
    source: str,
    expected_attr: str,
    unexpected_attr: str,
) -> None:
    image_path = _make_oriented_image(tmp_path / f"{source}.jpg")
    dialog = _build_dialog(monkeypatch, qapp, image_path=image_path)

    expected_worker = _recording_worker_class()
    monkeypatch.setattr(observations_tab, "AIGuessWorker", expected_worker if source == "arts" else _FailWorker)
    monkeypatch.setattr(observations_tab, "INatAIGuessWorker", expected_worker if source == "inat" else _FailWorker)

    if source == "arts":
        class FailOAuth:
            def __init__(self, *_args, **_kwargs):
                raise AssertionError("iNaturalist should not be started from the Artsorakel button")

        monkeypatch.setattr(inat_oauth, "INatOAuthClient", FailOAuth)
        monkeypatch.setattr(dialog, "_inat_credentials", lambda: ("client-id", "secret", "uri"))
    else:
        class LoggedInOAuth:
            def __init__(self, *_args, **_kwargs):
                self.logged_in = True

            def is_logged_in(self) -> bool:
                return True

        monkeypatch.setattr(inat_oauth, "INatOAuthClient", LoggedInOAuth)
        monkeypatch.setattr(dialog, "_inat_credentials", lambda: ("client-id", "secret", "uri"))
        monkeypatch.setattr(dialog, "_inat_locale", lambda: "en")
        monkeypatch.setattr(dialog, "_inat_token_file", lambda: tmp_path / "inat_tokens.json")

    dialog._on_ai_guess_clicked(source)

    expected_thread = getattr(dialog, expected_attr)
    unexpected_thread = getattr(dialog, unexpected_attr)
    assert expected_thread is not None
    assert unexpected_thread is None

    expected_instances = getattr(expected_worker, "instances", [])
    assert len(expected_instances) == 1
    assert expected_instances[0].started is True
    expected_max_dim = (
        ai_image_prep.DEFAULT_ARTSORAKEL_MAX_DIM
        if source == "arts"
        else 1600
    )
    assert expected_instances[0].kwargs.get("max_dim") == expected_max_dim

    dialog._cleanup_dialog_threads()
    dialog.deleteLater()


def test_guess_uses_all_field_images_when_no_thumbnail_is_selected(monkeypatch, qapp, tmp_path: Path) -> None:
    first_path = _make_oriented_image(tmp_path / "first.jpg")
    second_path = _make_oriented_image(tmp_path / "second.jpg")
    third_path = _make_oriented_image(tmp_path / "third.jpg")
    dialog = _build_dialog(
        monkeypatch,
        qapp,
        image_path=first_path,
        image_results=[
            ImageImportResult(filepath=str(first_path), image_type="microscope"),
            ImageImportResult(filepath=str(second_path), image_type="field"),
            ImageImportResult(filepath=str(third_path), image_type="field"),
        ],
    )
    dialog._refresh_image_gallery_summary()
    qapp.processEvents()

    dialog.image_gallery.select_paths([])
    dialog._update_ai_controls_state()

    assert dialog.ai_guess_buttons["arts"].isEnabled() is True

    recording_worker = _recording_worker_class()
    monkeypatch.setattr(observations_tab, "AIGuessWorker", recording_worker)

    dialog._on_ai_guess_clicked("arts")

    assert dialog.image_gallery.selected_paths() == [str(second_path), str(third_path)]
    assert len(recording_worker.instances) == 1
    assert recording_worker.instances[0].kwargs.get("max_dim") == ai_image_prep.DEFAULT_ARTSORAKEL_MAX_DIM
    requests = recording_worker.instances[0].args[0]
    assert [request["index"] for request in requests] == [1, 2]
    assert [request["image_path"] for request in requests] == [str(second_path), str(third_path)]

    dialog._cleanup_dialog_threads()
    dialog.deleteLater()


def test_explicit_all_source_starts_both_workers(monkeypatch, qapp, tmp_path: Path) -> None:
    image_path = _make_oriented_image(tmp_path / "all.jpg")
    dialog = _build_dialog(monkeypatch, qapp, image_path=image_path)

    recording_worker = _recording_worker_class()
    monkeypatch.setattr(observations_tab, "AIGuessWorker", recording_worker)
    monkeypatch.setattr(observations_tab, "INatAIGuessWorker", recording_worker)

    class LoggedInOAuth:
        def __init__(self, *_args, **_kwargs):
            self.logged_in = True

        def is_logged_in(self) -> bool:
            return True

    monkeypatch.setattr(inat_oauth, "INatOAuthClient", LoggedInOAuth)
    monkeypatch.setattr(dialog, "_inat_credentials", lambda: ("client-id", "secret", "uri"))
    monkeypatch.setattr(dialog, "_inat_locale", lambda: "en")
    monkeypatch.setattr(dialog, "_inat_token_file", lambda: tmp_path / "inat_tokens.json")

    dialog._on_ai_guess_clicked("all")

    assert dialog._ai_thread is not None
    assert dialog._inat_ai_thread is not None
    assert len(recording_worker.instances) == 2
    assert all(instance.started for instance in recording_worker.instances)

    dialog._cleanup_dialog_threads()
    dialog.deleteLater()


def test_copying_species_ai_selection_updates_get_data_and_grows_on_does_not(
    monkeypatch,
    qapp,
    tmp_path: Path,
) -> None:
    image_path = _make_oriented_image(tmp_path / "copy.jpg")
    dialog = _build_dialog(
        monkeypatch,
        qapp,
        image_path=image_path,
        draft_data={
            "ai_selected_service": "legacy-service",
            "ai_selected_taxon_id": "old-1",
            "ai_selected_scientific_name": "Agaricus bisporus",
            "ai_selected_probability": 0.11,
            "ai_selected_at": "2026-01-01T00:00:00Z",
        },
    )

    seen_predictions = iter(
        [
            {
                "scientific_name": "Amanita regalis",
                "vernacularName": "Royal fly agaric",
                "vernacularNames": {"no": "Kongefluesopp", "en": "Royal fly agaric"},
                "taxon": {"id": 123, "vernacularName": "Royal fly agaric"},
                "probability": 0.84,
            },
            {
                "scientific_name": "Lentinus tigrinus",
                "vernacularName": "Tiger sawgill",
                "vernacularNames": {"no": "Tigersopp", "en": "Tiger sawgill"},
                "taxon": {"id": 456, "vernacularName": "Tiger sawgill"},
                "probability": 0.61,
            },
        ]
    )
    monkeypatch.setattr(dialog, "_selected_ai_prediction", lambda _source: next(seen_predictions))
    monkeypatch.setattr(observations_tab, "_current_utc_timestamp_text", lambda: "2026-06-01T12:00:00Z")
    monkeypatch.setattr(
        observations_tab.SettingsDB,
        "get_setting",
        lambda key, default=None: "no" if key == "vernacular_language" else default,
    )

    initial_data = dialog.get_data()
    assert initial_data["ai_selected_service"] == "legacy-service"
    assert initial_data["ai_selected_taxon_id"] == "old-1"
    assert initial_data["ai_selected_scientific_name"] == "Agaricus bisporus"
    assert initial_data["ai_selected_probability"] == 0.11
    assert initial_data["ai_selected_at"] == "2026-01-01T00:00:00Z"

    dialog.taxonomy_tabs.setCurrentWidget(dialog.species_tab)
    dialog._on_ai_copy_to_taxonomy("arts")
    species_data = dialog.get_data()

    assert species_data["ai_selected_service"] == "artsorakel"
    assert species_data["ai_selected_taxon_id"] == "123"
    assert species_data["ai_selected_scientific_name"] == "Amanita regalis"
    assert species_data["ai_selected_probability"] == 0.84
    assert species_data["ai_selected_at"] == "2026-06-01T12:00:00Z"
    assert dialog.genus_input.text() == "Amanita"
    assert dialog.species_input.text() == "regalis"
    assert dialog.vernacular_input.text() == "Kongefluesopp"
    assert "Amanita regalis" in dialog.ai_selected_summary_label.text()
    assert "p=84%" in dialog.ai_selected_summary_label.text()

    dialog.taxonomy_tabs.setCurrentWidget(dialog.grows_tab)
    dialog._on_ai_copy_to_taxonomy("arts")
    grows_data = dialog.get_data()

    assert grows_data["ai_selected_service"] == "artsorakel"
    assert grows_data["ai_selected_taxon_id"] == "123"
    assert grows_data["ai_selected_scientific_name"] == "Amanita regalis"
    assert grows_data["ai_selected_probability"] == 0.84
    assert grows_data["ai_selected_at"] == "2026-06-01T12:00:00Z"
    assert dialog.host_genus_input.text() == "Lentinus"
    assert dialog.host_species_input.text() == "tigrinus"

    dialog._cleanup_dialog_threads()
    dialog.deleteLater()


def test_inat_ai_prediction_scores_render_as_percentages() -> None:
    dialog = SimpleNamespace(
        _ai_prediction_score=observations_tab.ObservationDetailsDialog._ai_prediction_score,
    )

    assert observations_tab.ObservationDetailsDialog._format_ai_prediction_score(
        dialog,
        {"probability": 0.6},
        source="inat",
    ) == "60%"
    assert observations_tab.ObservationDetailsDialog._format_ai_prediction_score(
        dialog,
        {"probability": 0.581},
        source="inat",
    ) == "58.1%"


def test_existing_observation_hydrates_taxonomy_from_species_guess(
    monkeypatch,
    qapp,
    tmp_path: Path,
) -> None:
    image_path = _make_oriented_image(tmp_path / "existing.jpg")
    dialog = _build_dialog(
        monkeypatch,
        qapp,
        image_path=image_path,
        observation={
            "id": 463,
            "species_guess": "Amanita regalis",
            "common_name": "",
            "genus": "",
            "species": "",
        },
    )

    assert dialog.genus_input.text() == "Amanita"
    assert dialog.species_input.text() == "regalis"

    data = dialog.get_data()
    assert data["genus"] == "Amanita"
    assert data["species"] == "regalis"
    assert data["species_guess"] == "Amanita regalis"

    dialog._cleanup_dialog_threads()
    dialog.deleteLater()
