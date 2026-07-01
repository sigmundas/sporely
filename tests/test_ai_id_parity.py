from __future__ import annotations

import os
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
    draft_data: dict | None = None,
):
    _make_dialog_patches(monkeypatch)
    dialog = observations_tab.ObservationDetailsDialog(
        parent=None,
        observation=None,
        draft_data=draft_data,
        image_results=[ImageImportResult(filepath=str(image_path), image_type="field")],
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


def test_ai_workers_delegate_to_shared_preparation_helper(monkeypatch, tmp_path: Path) -> None:
    calls: list[tuple[str, tuple[float, float, float, float] | None, Path, str, int, int]] = []

    def fake_prepare(image_path, crop_box, temp_dir, prefix, max_dim=1600, jpeg_quality=90):
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

    import_worker = SimpleNamespace(temp_dir=tmp_path / "import", max_dim=1600)
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
        ("source-a.jpg", (0.1, 0.2, 0.3, 0.4), tmp_path / "import", "ai_guess", 1600, 90),
        ("source-b.jpg", None, tmp_path / "inat", "inat_ai_guess", 777, 90),
    ]


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
            "ai_selected_scientific_name": "Old name",
            "ai_selected_probability": 0.11,
            "ai_selected_at": "2026-01-01T00:00:00Z",
        },
    )

    seen_predictions = iter(
        [
            {
                "taxon": {
                    "id": 123,
                    "genus": "Agaricus",
                    "species": "bisporus",
                    "vernacularName": "Button mushroom",
                },
                "probability": 0.84,
            },
            {
                "taxon": {
                    "id": 456,
                    "genus": "Lentinus",
                    "species": "tigrinus",
                    "vernacularName": "Tiger sawgill",
                },
                "probability": 0.61,
            },
        ]
    )
    monkeypatch.setattr(dialog, "_selected_ai_prediction", lambda _source: next(seen_predictions))
    monkeypatch.setattr(observations_tab, "_current_utc_timestamp_text", lambda: "2026-06-01T12:00:00Z")

    initial_data = dialog.get_data()
    assert initial_data["ai_selected_service"] == "legacy-service"
    assert initial_data["ai_selected_taxon_id"] == "old-1"
    assert initial_data["ai_selected_scientific_name"] == "Old name"
    assert initial_data["ai_selected_probability"] == 0.11
    assert initial_data["ai_selected_at"] == "2026-01-01T00:00:00Z"

    dialog.taxonomy_tabs.setCurrentWidget(dialog.species_tab)
    dialog._on_ai_copy_to_taxonomy("arts")
    species_data = dialog.get_data()

    assert species_data["ai_selected_service"] == "artsorakel"
    assert species_data["ai_selected_taxon_id"] == "123"
    assert species_data["ai_selected_scientific_name"] == "Agaricus bisporus"
    assert species_data["ai_selected_probability"] == 0.84
    assert species_data["ai_selected_at"] == "2026-06-01T12:00:00Z"
    assert "Agaricus bisporus" in dialog.ai_selected_summary_label.text()

    dialog.taxonomy_tabs.setCurrentWidget(dialog.grows_tab)
    dialog._on_ai_copy_to_taxonomy("arts")
    grows_data = dialog.get_data()

    assert grows_data["ai_selected_service"] == "artsorakel"
    assert grows_data["ai_selected_taxon_id"] == "123"
    assert grows_data["ai_selected_scientific_name"] == "Agaricus bisporus"
    assert grows_data["ai_selected_probability"] == 0.84
    assert grows_data["ai_selected_at"] == "2026-06-01T12:00:00Z"
    assert dialog.host_genus_input.text() == "Lentinus"
    assert dialog.host_species_input.text() == "tigrinus"

    dialog._cleanup_dialog_threads()
    dialog.deleteLater()
