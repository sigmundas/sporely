from __future__ import annotations

import copy
import base64
import json
import os
import time
from types import SimpleNamespace
from unittest.mock import Mock

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QByteArray, Qt
from PySide6.QtGui import QCloseEvent
from PySide6.QtWidgets import QApplication, QMessageBox

import ui.cloud_conflict_dialog as conflict_ui
import utils.cloud_sync as cloud_sync


@pytest.fixture(scope="module")
def app():
    return QApplication.instance() or QApplication([])


def _measurement_conflict():
    return {
        "status": "values_differ",
        "local_id": 31,
        "cloud_id": "measurement-cloud-31",
        "local_image_id": 8,
        "cloud_image_id": "image-cloud-8",
        "fields": ["length_um", "p1_x", "p2_x"],
        "local_values": {"length_um": 8.72, "gallery_rotation": 90, "p1_x": 3, "p2_x": 9},
        "remote_values": {"length_um": 8.46, "gallery_rotation": 0, "p1_x": 2, "p2_x": 8},
        "baseline_values": {"length_um": 8.46, "gallery_rotation": 0, "p1_x": 2, "p2_x": 8},
        "baseline_available": True,
        "change_origin": "local",
        "geometry_summary": ["length axis moved"],
        "geometry_baseline": "Original geometry",
        "geometry_local": "Length axis moved",
        "geometry_cloud": "Unchanged",
    }


def _fixed_token(*, expires_in=3600):
    def part(value):
        return base64.urlsafe_b64encode(json.dumps(value).encode()).decode().rstrip("=")
    return f"{part({'alg': 'none'})}.{part({'sub': 'user-1', 'exp': time.time() + expires_in})}.signature"


def _detail(*, fields=False, measurement=True, possible=False):
    measurement_conflicts = [_measurement_conflict()] if measurement else []
    pairs = [
        {
            "pairing": "authoritative",
            "match_basis": "cloud_id",
            "status": "measurements_differ" if measurement else "same",
            "local": {
                "local_id": 8, "cloud_id": "image-cloud-8", "image_type": "microscope",
                "sort_order": 8, "measurement_count": 3, "thumbnail_source": "missing-local.jpg",
            },
            "remote": {
                "local_id": 8, "cloud_id": "image-cloud-8", "image_type": "microscope",
                "sort_order": 8, "measurement_count": 3, "thumbnail_source": "cloud/key.webp",
            },
            "measurement_conflicts": measurement_conflicts,
            "measurement_pairs": measurement_conflicts,
        },
        {
            "pairing": "unpaired", "status": "possible_match" if possible else "local_only",
            "local": {"local_id": 9, "image_type": "microscope", "sort_order": 9,
                      "measurement_count": 0, "thumbnail_source": "missing-local-2.jpg"},
            "remote": None,
            "possible_counterpart": {"cloud_id": "possible-cloud"} if possible else None,
        },
        {
            "pairing": "unpaired", "status": "cloud_only", "local": None,
            "remote": {"cloud_id": "cloud-only", "image_type": "field", "sort_order": 1,
                       "measurement_count": 0, "thumbnail_source": "cloud/missing.webp"},
        },
    ]
    return {
        "local_id": 593,
        "cloud_id": "902",
        "local_observation": {"genus": "Paxillus", "species": "involutus", "date": "2026-07-19", "location": "Oslo"},
        "remote_observation": {"genus": "Paxillus", "species": "involutus", "date": "2026-07-19", "location": "Oslo"},
        "field_rows": ([{"field": "notes", "label": "Notes", "baseline": "a", "local": "b",
                         "remote": "c", "local_changed": True, "remote_changed": True}] if fields else []),
        "image_pairs": pairs,
        "image_mismatches": [],
        "measurement_conflicts": measurement_conflicts,
        "measurement_pairs": measurement_conflicts,
        "baseline_available": True,
    }


@pytest.fixture
def dialog(app, monkeypatch):
    detail = _detail()
    monkeypatch.setattr(conflict_ui, "get_app_settings", lambda: {
        "cloud_access_token": _fixed_token(), "cloud_user_id": "user-1",
        "cloud_refresh_token": "must-not-be-read",
    })
    monkeypatch.setattr(conflict_ui, "get_conflict_detail", lambda *args, **kwargs: copy.deepcopy(detail))
    monkeypatch.setattr(conflict_ui.ConflictDetailWorker, "start", lambda self: self.run())
    monkeypatch.setattr(conflict_ui.ConflictThumbnailWorker, "start", lambda self: self.failed.emit(
        self.generation, self.cache_key, "unavailable"
    ))
    instance = conflict_ui.CloudConflictDialog(
        conflicts=[{"local_id": 593, "cloud_id": "902"}, {"local_id": 671, "cloud_id": "903"}]
    )
    app.processEvents()
    yield instance
    instance.close()
    app.processEvents()


def test_review_later_escape_and_close_perform_no_writes_when_idle(dialog, monkeypatch):
    """Turn B: Review-later, Escape, and close never invoke the resolver.

    Successful applies committed BEFORE the user hits Review-later are already
    on disk and remain in ``dialog.decisions`` as an audit log; only the
    currently-pending (unfired) plan for the visible conflict is discarded.
    """
    calls = []
    monkeypatch.setattr(conflict_ui, "resolve_conflict_keep_local", lambda *args, **kwargs: calls.append("local"))
    monkeypatch.setattr(conflict_ui, "resolve_conflict_keep_cloud", lambda *args, **kwargs: calls.append("cloud"))
    monkeypatch.setattr(conflict_ui, "resolve_conflict_merge", lambda *args, **kwargs: calls.append("merge"))
    monkeypatch.setattr(conflict_ui, "resolve_conflict_plan", lambda *a, **k: calls.append("plan"))
    dialog._list.setCurrentRow(1)
    dialog.reject()
    assert calls == []
    dialog.closeEvent(QCloseEvent())
    assert calls == []


def test_resolution_disabled_during_loading_and_empty_fields_hidden(dialog):
    dialog._show_loading()
    assert not dialog._keep_local_btn.isEnabled()
    assert not dialog._keep_remote_btn.isEnabled()
    assert not dialog._merge_btn.isEnabled()
    assert dialog._review_later_btn.isEnabled()
    dialog._populate_detail(_detail())
    assert not dialog._compare_table.isVisible()
    assert dialog._field_status.text() == "No observation fields differ"


def test_unpaired_and_authoritatively_paired_photo_cards_render(dialog):
    dialog._populate_detail(_detail())
    groups = [dialog._photos_layout.itemAt(index).widget() for index in range(dialog._photos_layout.count())]
    photo_groups = [group for group in groups if len(group.findChildren(conflict_ui.PhotoCard)) == 2]
    assert len(photo_groups) == 3
    first_cards = photo_groups[0].findChildren(conflict_ui.PhotoCard)
    assert first_cards[0].image["local_id"] == 8
    assert first_cards[1].image["cloud_id"] == "image-cloud-8"
    assert any(card.image and card.image.get("local_id") == 9 for group in photo_groups for card in group.findChildren(conflict_ui.PhotoCard))
    assert any(card.image and card.image.get("cloud_id") == "cloud-only" for group in photo_groups for card in group.findChildren(conflict_ui.PhotoCard))


def test_measurement_details_include_owner_identities_fields_values_and_geometry(dialog):
    dialog._populate_detail(_detail())
    text = " ".join(label.text() for label in dialog._photos_container.findChildren(conflict_ui.QLabel))
    assert "Microscope image 8" in text
    assert "Measurement · local #31 · cloud measurement-cloud-31" in text
    assert "Changed only on this device" in text
    tables = dialog._photos_container.findChildren(conflict_ui.QTableWidget)
    cell_text = {tables[0].item(row, column).text() for row in range(tables[0].rowCount()) for column in range(4)}
    assert {"Length", "8.72 µm", "8.46 µm", "Length axis moved", "Unchanged"} <= cell_text
    assert any("p1_x" in (tables[0].item(row, 0).toolTip() or "") for row in range(tables[0].rowCount()))


def test_presets_do_not_write_and_apply_all_is_disabled(dialog, monkeypatch):
    dialog._populate_detail(_detail())
    writes = []
    monkeypatch.setattr(conflict_ui, "resolve_conflict_plan", lambda *a, **k: writes.append((a, k)))
    dialog._keep_local_btn.click()
    assert writes == []
    assert dialog._selected_choice("measurement:31") == "local"
    assert not dialog._apply_all_check.isEnabled()
    assert "cannot be applied blindly" in dialog._apply_all_check.toolTip()


def test_apply_all_remains_disabled_for_per_item_metadata_plans(dialog):
    detail = _detail(fields=True, measurement=False)
    detail["image_pairs"] = [{"status": "same", "local": {"local_id": 1}, "remote": {"cloud_id": "a"}}]
    dialog._populate_detail(detail)
    assert not dialog._apply_all_check.isEnabled()
    assert not dialog._compare_table.isHidden()
    assert dialog._merge_btn.isEnabled()


def test_field_only_conflict_requires_choice_and_sizes_table_to_content(dialog):
    detail = _detail(fields=True, measurement=False)
    detail["image_pairs"] = []
    dialog._populate_detail(detail)

    assert dialog._compare_table.rowCount() == 1
    expected_height = dialog._compare_table.horizontalHeader().height() + sum(
        dialog._compare_table.rowHeight(row)
        for row in range(dialog._compare_table.rowCount())
    ) + dialog._compare_table.frameWidth() * 2 + 6
    assert dialog._compare_table.height() == max(58, expected_height)
    assert dialog._selected_choice("field:notes") is None
    assert not dialog._apply_btn.isEnabled()

    dialog._set_choice("field:notes", "cloud")
    dialog._update_apply_enabled()
    assert dialog._apply_btn.isEnabled()
    plan_items = dialog._build_selected_plan()["items"]
    assert len(plan_items) == 1
    assert {
        key: plan_items[0][key] for key in ("kind", "field", "choice")
    } == {"kind": "field", "field": "notes", "choice": "cloud"}


def test_possible_match_warning_does_not_pair_cards(dialog):
    detail = _detail(measurement=False, possible=True)
    dialog._populate_detail(detail)
    text = " ".join(label.text() for label in dialog._photos_container.findChildren(conflict_ui.QLabel))
    assert "Possible counterpart — identity is not confirmed" in text
    groups = [dialog._photos_layout.itemAt(index).widget() for index in range(dialog._photos_layout.count())]
    possible_groups = [group for group in groups if "Possible counterpart" in " ".join(
        label.text() for label in group.findChildren(conflict_ui.QLabel)
    )]
    cards = possible_groups[0].findChildren(conflict_ui.PhotoCard)
    assert bool(cards[0].image) != bool(cards[1].image)


def test_missing_thumbnails_do_not_disable_review(dialog):
    dialog._populate_detail(_detail())
    texts = [card.thumbnail.text() for card in dialog._photos_container.findChildren(conflict_ui.PhotoCard)]
    assert "Local file unavailable" in texts
    assert "Cloud thumbnail unavailable" in texts
    assert dialog._review_later_btn.isEnabled()


def test_stale_thumbnail_result_is_cached_but_not_rendered(dialog, monkeypatch):
    dialog._populate_detail(_detail())
    cards = dialog._photos_container.findChildren(conflict_ui.PhotoCard)
    called = []
    monkeypatch.setattr(cards[0], "show_image", lambda data: called.append(data))
    dialog._thumbnail_loaded(dialog._selection_generation - 1, "local:8", QByteArray(b"old"))
    assert called == []
    assert dialog._thumbnail_cache["local:8"] == QByteArray(b"old")


def test_refresh_and_detail_loading_do_not_call_write_apis(dialog, monkeypatch):
    writes = []
    monkeypatch.setattr(conflict_ui, "resolve_conflict_keep_local", lambda *args, **kwargs: writes.append("local"))
    monkeypatch.setattr(conflict_ui, "resolve_conflict_keep_cloud", lambda *args, **kwargs: writes.append("cloud"))
    monkeypatch.setattr(conflict_ui, "resolve_conflict_merge", lambda *args, **kwargs: writes.append("merge"))
    dialog._refresh_current_detail()
    assert writes == []


def test_safe_additions_preset_leaves_scientific_conflict_unresolved(dialog):
    dialog._populate_detail(_detail())
    dialog._merge_btn.click()
    assert dialog._selected_choice("image:9") == "upload"
    assert dialog._selected_choice("image:cloud-only") == "download"
    assert dialog._selected_choice("measurement:31") is None
    assert not dialog._apply_btn.isEnabled()


def _patch_inline_apply(monkeypatch, *, resolver=None):
    """Run the per-conflict apply worker synchronously and route resolve_conflict_plan."""
    monkeypatch.setattr(
        conflict_ui.ConflictPlanApplyWorker, "start",
        lambda self: self.run(),
    )
    monkeypatch.setattr(
        conflict_ui.SporelyCloudClient, "from_stored_credentials",
        lambda: SimpleNamespace(get_observation=lambda _id: {}),
    )
    if resolver is not None:
        monkeypatch.setattr(conflict_ui, "resolve_conflict_plan", resolver)


def test_apply_directly_accumulates_selected_plan_without_confirmation_dialog(dialog, monkeypatch):
    dialog._populate_detail(_detail())
    dialog._keep_local_btn.click()
    assert dialog._apply_btn.isEnabled()

    def _fail_on_question(*args, **kwargs):
        raise AssertionError("Apply must not open a second confirmation dialog")

    monkeypatch.setattr(QMessageBox, "question", _fail_on_question)
    monkeypatch.setattr(QMessageBox, "warning", _fail_on_question)
    monkeypatch.setattr(QMessageBox, "information", _fail_on_question)
    monkeypatch.setattr(QMessageBox, "critical", _fail_on_question)
    _patch_inline_apply(monkeypatch, resolver=lambda *a, **k: {
        'plan_applied': True, 'operations': [], 'presentation_warnings': [],
    })
    dialog._apply_selected_changes()
    assert dialog.decisions[0]["action"] == "plan"
    assert dialog.decisions[0]["plan"]["allow_media_deletion"] is False
    # A rapid second click after a successful apply advances to the next
    # conflict; the resolved one is no longer present.
    assert dialog._current_conflict() is None or (
        dialog._current_conflict() != {"local_id": 593, "cloud_id": "902"}
    )


def test_apply_is_disabled_and_ignored_when_plan_incomplete(dialog, monkeypatch):
    def _fail_on_question(*args, **kwargs):
        raise AssertionError("Apply must remain disabled and cannot open dialogs")

    monkeypatch.setattr(QMessageBox, "question", _fail_on_question)
    monkeypatch.setattr(QMessageBox, "warning", _fail_on_question)
    monkeypatch.setattr(QMessageBox, "information", _fail_on_question)
    monkeypatch.setattr(QMessageBox, "critical", _fail_on_question)
    _patch_inline_apply(monkeypatch)
    dialog._merge_btn.click()  # safe additions preset leaves scientific conflict unresolved
    assert not dialog._apply_btn.isEnabled()
    dialog._apply_selected_changes()
    assert dialog.decisions == []


def test_apply_failure_keeps_conflict_and_choices(dialog, monkeypatch):
    """B1: failed apply keeps the conflict visible with the same selection intact."""
    dialog._populate_detail(_detail())
    dialog._keep_local_btn.click()
    initial_choice = dialog._selected_choice("measurement:31")
    assert initial_choice == "local"

    def _boom(*a, **k):
        raise conflict_ui.CloudSyncError("simulated cloud failure")

    _patch_inline_apply(monkeypatch, resolver=_boom)
    dialog._apply_selected_changes()
    # Conflict remains in the dialog list.
    assert dialog._current_conflict() is not None
    # No decision recorded.
    assert dialog.decisions == []
    # Choice preserved so retry can proceed.
    assert dialog._selected_choice("measurement:31") == "local"
    # Status label surfaces the error inline.
    assert "Apply failed" in dialog._status_label.text()


def test_apply_partial_failure_carries_prior_result_into_retry(dialog, monkeypatch):
    """B2: on retry, the resolver receives prior_result so completed ops are skipped."""
    dialog._populate_detail(_detail())
    dialog._keep_local_btn.click()
    seen_prior = []

    def _first_call(*a, **k):
        raise conflict_ui.PartialConflictPlanError(
            "half-done",
            partial_result={
                'local_id': 593, 'cloud_id': '902', 'plan_applied': False,
                'operations': [
                    {'op': 'push_field', 'field': 'notes', 'status': 'completed'},
                ],
            },
        )

    def _second_call(*a, **k):
        seen_prior.append(k.get('prior_result'))
        return {'plan_applied': True, 'operations': [], 'presentation_warnings': []}

    resolver = {'n': 0}

    def _resolver(*a, **k):
        resolver['n'] += 1
        return _first_call(*a, **k) if resolver['n'] == 1 else _second_call(*a, **k)

    _patch_inline_apply(monkeypatch, resolver=_resolver)
    dialog._apply_selected_changes()  # first — fails
    assert dialog.decisions == []
    assert dialog._pending_prior_result is not None
    # Retry.
    dialog._apply_selected_changes()
    assert dialog.decisions and dialog.decisions[0]["action"] == "plan"
    assert seen_prior and seen_prior[0].get('operations') == [
        {'op': 'push_field', 'field': 'notes', 'status': 'completed'},
    ]


def test_detail_model_pairs_only_stable_identity_and_exposes_measurement_values(monkeypatch):
    local_obs = {"id": 1, "cloud_id": "obs-cloud", "date": "2026-01-01", "genus": "Mycena", "species": "haematopus"}
    remote_obs = dict(local_obs)
    local_images = [
        {"id": 8, "cloud_id": "image-cloud-8", "filepath": "/tmp/eight.jpg", "image_type": "microscope", "sort_order": 8, "notes": "local note"},
        {"id": 9, "filepath": "/tmp/same-name.jpg", "image_type": "microscope", "sort_order": 9},
    ]
    remote_images = [
        {"id": "image-cloud-8", "desktop_id": 8, "original_filename": "eight.jpg", "image_type": "microscope", "sort_order": 8, "storage_path": "cloud/eight.webp", "notes": "cloud note"},
        {"id": "fallback-cloud", "original_filename": "same-name.jpg", "image_type": "microscope", "sort_order": 9, "storage_path": "cloud/fallback.webp"},
    ]
    local_measurement = {"id": 31, "cloud_id": "measurement-cloud-31", "image_id": 8, "length_um": 8.72, "width_um": 5.67, "p1_x": 3, "p1_y": 1, "p2_x": 9, "p2_y": 1}
    local_only_measurement = {"id": 32, "image_id": 8, "length_um": 7.1, "width_um": 4.2}
    remote_measurement = {"id": "measurement-cloud-31", "desktop_id": 31, "image_id": "image-cloud-8", "length_um": 8.46, "width_um": 5.67, "p1_x": 4, "p1_y": 1, "p2_x": 10, "p2_y": 1}
    cloud_only_measurement = {"id": "measurement-cloud-only", "image_id": "image-cloud-8", "length_um": 9.1, "width_um": 6.0}
    snapshot_measurement = {**remote_measurement, "p1_x": 2, "p2_x": 8}
    snapshot = {"observation": remote_obs, "images": [{"id": "image-cloud-8", "desktop_id": 8, "image_type": "microscope", "sort_order": 8, "notes": "baseline note"}], "measurements": [snapshot_measurement]}

    monkeypatch.setattr(cloud_sync.ObservationDB, "get_observation", lambda _id: dict(local_obs))
    monkeypatch.setattr(cloud_sync.ImageDB, "get_images_for_observation", lambda _id: copy.deepcopy(local_images))
    monkeypatch.setattr(cloud_sync.MeasurementDB, "get_measurements_for_observation", lambda _id: [dict(local_measurement), dict(local_only_measurement)])
    monkeypatch.setattr(cloud_sync, "_load_local_measurement_lookup", lambda _id: ({}, {31: dict(local_measurement), 32: dict(local_only_measurement)}))
    monkeypatch.setattr(cloud_sync, "_pull_remote_measurements_for_images", lambda *args: [dict(remote_measurement), dict(cloud_only_measurement)])
    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda _id: __import__("json").dumps(snapshot))

    class Client:
        def get_observation(self, _id):
            return dict(remote_obs)

        def pull_image_metadata(self, _id, include_deleted_for_sync=False):
            return copy.deepcopy(remote_images)

    detail = cloud_sync.get_conflict_detail(Client(), 1, "obs-cloud")
    matched = next(pair for pair in detail["image_pairs"] if pair["local"] and pair["local"].get("local_id") == 8)
    assert matched["pairing"] == "authoritative"
    assert matched["match_basis"] == "cloud_id"
    assert [row["field"] for row in matched["metadata_diff_details"]] == ["notes"]
    assert "storage_path" not in matched["metadata_diff_fields"]
    fallback_local = next(pair for pair in detail["image_pairs"] if pair["local"] and pair["local"].get("local_id") == 9)
    fallback_cloud = next(pair for pair in detail["image_pairs"] if pair["remote"] and pair["remote"].get("cloud_id") == "fallback-cloud")
    assert fallback_local["remote"] is None and fallback_cloud["local"] is None
    assert fallback_local["status"] == fallback_cloud["status"] == "possible_match"
    measurement = detail["measurement_conflicts"][0]
    assert measurement["local_image_id"] == 8
    assert measurement["cloud_image_id"] == "image-cloud-8"
    assert "length_um" in measurement["fields"]
    assert measurement["local_values"]["length_um"] == 8.72
    assert measurement["remote_values"]["length_um"] == 8.46
    assert measurement["geometry_local"] == "Length axis moved"
    assert measurement["geometry_cloud"] == "Length axis moved differently"
    # Simplified sync model: local-only and cloud-only measurements are
    # additive and auto-resolved by ordinary sync — they no longer appear in
    # measurement_pairs.  They show up under automatic_decisions.media
    # instead.  Only two-sided-differ measurements remain in the pairs list.
    assert {row["status"] for row in detail["measurement_pairs"]} == {"values_differ"}
    auto_media = detail["automatic_decisions"]["media"]
    kinds_sides = {(d["kind"], d["side"]) for d in auto_media}
    assert ("measurement", "local_only") in kinds_sides
    assert ("measurement", "cloud_only") in kinds_sides


def test_gallery_rotation_only_is_presentation_not_scientific_conflict():
    local = {"id": 31, "cloud_id": "m31", "image_id": 8, "gallery_rotation": 90}
    remote = {"id": "m31", "desktop_id": 31, "image_id": "image-8", "gallery_rotation": 0}
    assert cloud_sync._measurement_push_diff_fields(
        local, remote, cloud_image_id="image-8"
    ) == []
    assert cloud_sync._measurement_payloads_match(
        local, remote, cloud_image_id="image-8"
    )


def test_scientific_measurement_difference_still_blocks_for_choice():
    local = {"id": 31, "cloud_id": "m31", "image_id": 8, "length_um": 8.72}
    remote = {"id": "m31", "desktop_id": 31, "image_id": "image-8", "length_um": 8.46}
    assert cloud_sync._measurement_push_diff_fields(
        local, remote, cloud_image_id="image-8"
    ) == ["length_um"]


def test_image_order_only_is_informational_and_nonblocking(dialog):
    detail = _detail(measurement=False)
    detail["image_pairs"] = [{
        "status": "same", "pairing": "authoritative",
        "local": {"local_id": 8, "cloud_id": "image-8", "image_type": "microscope",
                  "sort_order": 5, "measurement_count": 0},
        "remote": {"local_id": 8, "cloud_id": "image-8", "image_type": "microscope",
                   "sort_order": 3, "measurement_count": 0},
        "metadata_diff_details": [], "measurement_pairs": [],
        "presentation_differences": [{"field": "sort_order", "local": 5, "remote": 3,
                                      "automatic_policy": "local_desktop"}],
    }]
    dialog._show_matching_check.setChecked(True)
    dialog._populate_detail(detail)
    assert not any(spec["kind"] == "image_metadata" for spec in dialog._choice_specs.values())
    text = " ".join(label.text() for label in dialog._photos_container.findChildren(conflict_ui.QLabel))
    assert "Informational only" in text and "Desktop order 5" in text


def test_spore_statistics_is_derived_and_not_an_independent_field_choice(dialog):
    detail = _detail(fields=True)
    detail["field_rows"].append({
        "field": "spore_statistics", "label": "Spore statistics",
        "baseline": "old", "local": "local", "remote": "cloud",
    })
    # get_conflict_detail removes this row; exercise the UI contract it consumes.
    detail["field_rows"] = [row for row in detail["field_rows"] if row["field"] != "spore_statistics"]
    detail["derived_statistics"] = {"status": "recompute_from_measurements", "rows": [{}]}
    dialog._populate_detail(detail)
    assert "field:spore_statistics" not in dialog._choice_specs
    assert "recomputed automatically" in dialog._status_label.text()


def test_mixed_cloud_field_and_local_measurement_plan_recomputes_statistics(dialog):
    detail = _detail(fields=True)
    detail["field_rows"][0]["field"] = "common_name"
    detail["field_rows"][0]["label"] = "Common name"
    detail["derived_statistics"] = {"status": "recompute_from_measurements", "rows": [{}]}
    dialog._populate_detail(detail)
    dialog._set_choice("field:common_name", "cloud")
    dialog._set_choice("measurement:31", "local")
    dialog._set_choice("image:9", "upload")
    dialog._set_choice("image:cloud-only", "keep_cloud")
    dialog._update_apply_enabled()
    plan = dialog._build_selected_plan()
    assert dialog._apply_btn.isEnabled()
    assert {
        (item["kind"], item.get("field"), item.get("side"), item["choice"])
        for item in plan["items"]
    } >= {
        ("field", "common_name", None, "cloud"),
        ("measurement", None, "matched", "local"),
        ("image", None, "local_only", "upload"),
        ("image", None, "cloud_only", "keep_cloud"),
    }
    assert plan["derived_statistics"] == "recompute_from_measurements"
    assert plan["presentation_policy"] == {
        "gallery_rotation": "local_desktop", "image_order": "local_desktop"
    }
    assert plan["allow_media_deletion"] is False


def test_single_property_measurement_table_is_compact(dialog):
    detail = _detail()
    comparison = detail["measurement_pairs"][0]
    comparison["fields"] = ["length_um"]
    comparison["geometry_summary"] = []
    dialog._populate_detail(detail)
    table = dialog._photos_container.findChildren(conflict_ui.QTableWidget)[0]
    assert table.rowCount() <= 2
    assert table.maximumHeight() < 180


def test_fixed_token_client_cannot_login_refresh_or_persist(monkeypatch):
    forbidden = []
    for name in ("refresh_login", "login", "_refresh_session_if_possible", "save_credentials", "clear_session", "clear_credentials"):
        monkeypatch.setattr(
            conflict_ui.SporelyCloudClient,
            name,
            lambda *args, _name=name, **kwargs: forbidden.append(_name),
        )
    monkeypatch.setattr(conflict_ui, "get_app_settings", lambda: {
        "cloud_access_token": _fixed_token(), "cloud_user_id": "user-1",
        "cloud_refresh_token": "refresh-token-that-must-not-be-loaded",
    })
    client = conflict_ui._read_only_cloud_client()
    assert client is not None
    assert isinstance(client, cloud_sync.SporelyReadOnlyCloudClient)
    assert client.refresh_token is None
    # _get on the subclass routes to get_read_only.
    assert client._get.__self__ is client
    assert client._get.__func__ is cloud_sync.SporelyReadOnlyCloudClient._get
    # Belt-and-braces: even calling these directly on the instance is a no-op
    # or raises; nothing observable happens through parent forbidden hooks.
    assert client._refresh_session_if_possible() is False
    assert client.save_credentials() is None
    with pytest.raises(cloud_sync.CloudSyncError):
        client.clear_session()
    with pytest.raises(cloud_sync.CloudSyncError):
        client.login("user", "pw")
    with pytest.raises(cloud_sync.CloudSyncError):
        cloud_sync.SporelyReadOnlyCloudClient.refresh_login("stale-token")
    assert forbidden == []


def test_actual_detail_worker_uses_fixed_token_client(monkeypatch):
    forbidden = []
    monkeypatch.setattr(conflict_ui, "get_app_settings", lambda: {
        "cloud_access_token": _fixed_token(), "cloud_user_id": "user-1",
    })
    for name in ("refresh_login", "login", "_refresh_session_if_possible", "save_credentials", "clear_session", "clear_credentials"):
        monkeypatch.setattr(conflict_ui.SporelyCloudClient, name, lambda *a, _name=name, **k: forbidden.append(_name))
    seen = []
    monkeypatch.setattr(conflict_ui, "get_conflict_detail", lambda client, *args: seen.append(client) or _detail())
    worker = conflict_ui.ConflictDetailWorker(4, "1::cloud", {"local_id": 1, "cloud_id": "cloud"})
    worker.run()
    assert len(seen) == 1 and seen[0].refresh_token is None
    assert isinstance(seen[0], cloud_sync.SporelyReadOnlyCloudClient)
    assert forbidden == []


def test_expired_fixed_token_fails_closed_without_login(monkeypatch):
    calls = []
    monkeypatch.setattr(conflict_ui, "get_app_settings", lambda: {
        "cloud_access_token": _fixed_token(expires_in=-10), "cloud_user_id": "user-1",
    })
    monkeypatch.setattr(conflict_ui.SporelyCloudClient, "login", lambda *a, **k: calls.append("login"))
    monkeypatch.setattr(conflict_ui.SporelyCloudClient, "refresh_login", lambda *a, **k: calls.append("refresh"))
    with pytest.raises(conflict_ui.CloudSyncError, match="Authentication expired"):
        conflict_ui._read_only_cloud_client()
    assert calls == []


def test_actual_cloud_thumbnail_worker_uses_fixed_token_without_refresh(monkeypatch):
    forbidden = []
    monkeypatch.setattr(conflict_ui, "get_app_settings", lambda: {
        "cloud_access_token": _fixed_token(), "cloud_user_id": "user-1",
    })
    for name in ("refresh_login", "login", "_refresh_session_if_possible", "save_credentials", "clear_session", "clear_credentials"):
        monkeypatch.setattr(conflict_ui.SporelyCloudClient, name, lambda *a, _name=name, **k: forbidden.append(_name))

    def download(_client, _source, destination):
        from pathlib import Path
        Path(destination).write_bytes(b"thumbnail-bytes")
        return Path(destination)

    monkeypatch.setattr(conflict_ui.SporelyCloudClient, "download_image_file", download)
    loaded = []
    worker = conflict_ui.ConflictThumbnailWorker(3, "cloud:x", "cloud", "cloud/key.webp")
    worker.loaded.connect(lambda generation, key, data: loaded.append((generation, key, bytes(data))))
    worker.run()
    assert loaded == [(3, "cloud:x", b"thumbnail-bytes")]
    assert forbidden == []


def test_stale_detail_success_and_failure_cannot_replace_current_result(dialog):
    dialog._selection_generation = 20
    dialog._current_detail = _detail(fields=True)
    current_title = dialog._title_label.text()
    dialog._detail_loaded(19, dialog._key(dialog._current_conflict()), _detail(fields=False))
    dialog._detail_failed(19, dialog._key(dialog._current_conflict()), "stale failure")
    assert dialog._current_detail["field_rows"]
    assert dialog._title_label.text() == current_title
    assert not dialog._status_label.isVisible()


def test_closing_invalidates_active_workers_without_waiting(dialog):
    detail_worker = Mock()
    thumbnail_worker = Mock()
    dialog._detail_workers.add(detail_worker)
    dialog._thumbnail_workers.add(thumbnail_worker)
    dialog.reject()
    detail_worker.requestInterruption.assert_called_once_with()
    thumbnail_worker.requestInterruption.assert_called_once_with()
    assert dialog._closing is True


def test_near_identical_gps_drift_is_not_a_conflict():
    assert cloud_sync._observation_field_values_match("gps_latitude", 59.9139001, 59.9139007)
    assert cloud_sync._observation_field_values_match("gps_longitude", 10.7522001, 10.7522007)
    assert not cloud_sync._observation_field_values_match("gps_latitude", 59.9139, 59.913905)


def test_duplicate_local_cloud_identity_is_reported_and_disables_actions(monkeypatch, dialog):
    local_obs = {"id": 1, "cloud_id": "obs", "date": "2026-01-01"}
    local_images = [
        {"id": 1, "cloud_id": "shared", "filepath": "/tmp/a.jpg", "image_type": "field"},
        {"id": 2, "cloud_id": "shared", "filepath": "/tmp/b.jpg", "image_type": "field"},
    ]
    remote_images = [{"id": "shared", "desktop_id": 1, "image_type": "field", "storage_path": "cloud/a"}]
    monkeypatch.setattr(cloud_sync.ObservationDB, "get_observation", lambda _id: dict(local_obs))
    monkeypatch.setattr(cloud_sync.ImageDB, "get_images_for_observation", lambda _id: copy.deepcopy(local_images))
    monkeypatch.setattr(cloud_sync.MeasurementDB, "get_measurements_for_observation", lambda _id: [])
    monkeypatch.setattr(cloud_sync, "_load_local_measurement_lookup", lambda _id: ({}, {}))
    monkeypatch.setattr(cloud_sync, "_pull_remote_measurements_for_images", lambda *args: [])
    monkeypatch.setattr(cloud_sync, "_load_cloud_observation_snapshot", lambda _id: "")

    class Client:
        def get_observation(self, _id): return dict(local_obs)
        def pull_image_metadata(self, _id, include_deleted_for_sync=False): return copy.deepcopy(remote_images)

    detail = cloud_sync.get_conflict_detail(Client(), 1, "obs")
    assert detail["identity_conflicts"]
    assert any("multiple local images" in reason for pair in detail["identity_conflicts"] for reason in pair["identity_conflict_reasons"])
    dialog._closing = False
    dialog._populate_detail(detail)
    assert not dialog._keep_local_btn.isEnabled()
    assert not dialog._keep_remote_btn.isEnabled()
    assert not dialog._merge_btn.isEnabled()
    assert dialog._review_later_btn.isEnabled()


def test_matching_photos_hidden_by_default_and_revealed_by_toggle(dialog):
    detail = _detail(measurement=False)
    detail["image_pairs"] = [
        {"status": "same", "pairing": "authoritative",
         "local": {"local_id": 1, "image_type": "field", "measurement_count": 0},
         "remote": {"cloud_id": "one", "image_type": "field", "measurement_count": 0},
         "measurement_pairs": []}
    ]
    dialog._show_matching_check.setChecked(False)
    dialog._populate_detail(detail)
    assert dialog._photos_layout.count() == 0
    assert dialog._photos_heading.isHidden()
    assert dialog._photo_headings.isHidden()
    dialog._show_matching_check.setChecked(True)
    assert dialog._photos_layout.count() == 1
    assert not dialog._photos_heading.isHidden()
    assert "Same" in " ".join(
        label.text() for label in dialog._photos_container.findChildren(conflict_ui.QLabel)
    )


def test_metadata_details_show_exact_user_fields_not_storage_diagnostics(dialog):
    detail = _detail(measurement=False)
    detail["image_pairs"] = [{
        "status": "metadata_differs", "pairing": "authoritative",
        "local": {"local_id": 1, "cloud_id": "one", "image_type": "microscope", "sort_order": 5, "measurement_count": 0},
        "remote": {"cloud_id": "one", "local_id": 1, "image_type": "microscope", "sort_order": 3, "measurement_count": 0},
        "metadata_diff_details": [
            {"field": "crop_mode", "local": "Fit", "remote": "Fill", "baseline": "Fit", "change_origin": "cloud"},
        ],
        "presentation_differences": [
            {"field": "sort_order", "local": 5, "remote": 3, "automatic_policy": "local_desktop"}
        ],
    }]
    dialog._show_matching_check.setChecked(False)
    dialog._populate_detail(detail)
    text = " ".join(label.text() for label in dialog._photos_container.findChildren(conflict_ui.QLabel))
    assert "image order differs" in text
    assert "Crop mode: this device Fit · cloud Fill" in text
    assert "Changed only on this device" in text
    assert "Changed only on Sporely Cloud" in text
    assert "storage" not in text.lower()
    assert dialog._merge_btn.isEnabled()


def test_local_and_cloud_only_cards_do_not_show_old_consequence_wording(dialog):
    """Simplified sync model: additive one-sided images auto-sync and never
    surface in the dialog.  The old "Use Sporely Cloud" / "Use this device"
    observation-wide consequence wording is gone.
    """
    dialog._populate_detail(_detail(measurement=False))
    text = " ".join(label.text() for label in dialog._photos_container.findChildren(conflict_ui.QLabel))
    assert "this photo remains in the local database" not in text
    assert "Use Sporely Cloud" not in text
    assert "Use this device" not in text


def test_geometry_columns_describe_each_side_separately(dialog):
    detail = _detail()
    comparison = detail["measurement_pairs"][0]
    comparison["change_origin"] = "both"
    comparison["geometry_local"] = "Length and width axes moved"
    comparison["geometry_cloud"] = "Length axis moved differently"
    dialog._populate_detail(detail)
    tables = dialog._photos_container.findChildren(conflict_ui.QTableWidget)
    values = {
        table.item(row, column).text()
        for table in tables
        for row in range(table.rowCount()) for column in range(4)
    }
    assert "Original geometry" in values
    assert "Length and width axes moved" in values
    assert "Length axis moved differently" in values


@pytest.mark.parametrize("dark", [False, True])
def test_changed_cell_colors_have_readable_contrast(app, dialog, dark):
    palette = dialog.palette()
    palette.setColor(conflict_ui.QPalette.Window, conflict_ui.QColor("#202124" if dark else "#f8fafc"))
    dialog.setPalette(palette)
    dialog._compare_table.setPalette(palette)
    dialog._populate_fields([{
        "field": "notes", "label": "Notes", "baseline": "before",
        "local": "device", "remote": "cloud", "local_changed": True, "remote_changed": True,
    }])
    background, foreground = conflict_ui._changed_cell_colors(dialog, "local")
    def luminance(color):
        return 0.2126 * color.redF() + 0.7152 * color.greenF() + 0.0722 * color.blueF()
    assert abs(luminance(background) - luminance(foreground)) > 0.45
    local_item = dialog._compare_table.item(0, 2)
    cloud_item = dialog._compare_table.item(0, 3)
    assert abs(luminance(local_item.background().color()) - luminance(local_item.foreground().color())) > 0.45
    assert abs(luminance(cloud_item.background().color()) - luminance(cloud_item.foreground().color())) > 0.45


def test_every_displayed_coordinate_difference_is_visible():
    local = 59.9139000
    cloud = 59.9139050
    assert not cloud_sync._observation_field_values_match("gps_latitude", local, cloud)
    assert conflict_ui._format_compare_value("gps_latitude", local) != conflict_ui._format_compare_value("gps_latitude", cloud)


def test_keep_cloud_disables_deletion_preserves_local_file_and_overwrites_remote_measurements(monkeypatch, tmp_path):
    source = tmp_path / "local-only.jpg"
    source.write_bytes(b"local source")
    local_obs = {"id": 1, "cloud_id": "obs-cloud", "sync_status": "dirty"}
    remote_obs = {"id": "obs-cloud", "desktop_id": 1}
    remote_images = [{"id": "remote-image", "image_type": "microscope"}]
    calls = {}

    monkeypatch.setattr(cloud_sync.ObservationDB, "get_observation", lambda _id: dict(local_obs))
    monkeypatch.setattr(cloud_sync, "_pull_remote_images_for_sync", lambda *args: copy.deepcopy(remote_images))
    monkeypatch.setattr(cloud_sync, "_record_remote_image_tombstones", lambda *args, **kwargs: None)
    monkeypatch.setattr(cloud_sync, "_apply_remote_observation_fields", lambda *args, **kwargs: None)
    def apply_images(client, local_id, images, *, allow_delete=True, **kwargs):
        calls["allow_delete"] = allow_delete
        return []
    monkeypatch.setattr(cloud_sync, "_apply_remote_images_to_local", apply_images)
    monkeypatch.setattr(cloud_sync, "_pull_remote_measurements_for_images", lambda *args: [{"id": "remote-measurement"}])
    def import_measurements(*args, **kwargs):
        calls["overwrite_conflicts"] = kwargs.get("overwrite_conflicts")
        return {"warnings": [], "failed": 0}
    monkeypatch.setattr(cloud_sync, "_import_remote_measurements_for_observation", import_measurements)
    monkeypatch.setattr(cloud_sync, "_stamp_observation_synced", lambda *args: calls.setdefault("stamped", True))
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature", lambda *args: calls.setdefault("signature", True))
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *args, **kwargs: calls.setdefault("snapshot", kwargs))

    class Client:
        def get_observation(self, _id): return dict(remote_obs)

    result = cloud_sync.resolve_conflict_keep_cloud(Client(), 1, allow_delete=False)
    assert result["cloud_id"] == "obs-cloud"
    assert calls["allow_delete"] is False
    assert calls["overwrite_conflicts"] is True
    assert calls["stamped"] and calls["signature"]
    assert source.read_bytes() == b"local source"


def test_resolution_plan_applies_mixed_cloud_field_local_measurement_and_recomputes(monkeypatch):
    calls = []
    local_obs = {"id": 1, "cloud_id": "obs-cloud", "common_name": "local"}
    remote_obs = {"id": "obs-cloud", "common_name": "cloud"}
    monkeypatch.setattr(cloud_sync.ObservationDB, "get_observation", lambda _id: dict(local_obs))
    monkeypatch.setattr(cloud_sync, "_pull_remote_images_for_sync", lambda *a: [])
    monkeypatch.setattr(cloud_sync, "_pull_remote_measurements_for_images", lambda *a: [])
    monkeypatch.setattr(cloud_sync, "_apply_remote_observation_fields",
                        lambda *a, **k: calls.append(("cloud_fields", k["fields"])))
    monkeypatch.setattr(cloud_sync, "_push_measurements_for_observation",
                        lambda *a, **k: calls.append(("push_measurements", k["measurement_ids"])))
    monkeypatch.setattr(cloud_sync, "_format_recomputed_spore_statistics", lambda _id: "recomputed")
    monkeypatch.setattr(cloud_sync.ObservationDB, "update_spore_statistics",
                        lambda _id, value: calls.append(("statistics_local", value)))
    monkeypatch.setattr(cloud_sync, "_stamp_observation_synced", lambda *a: calls.append(("stamp",)))
    monkeypatch.setattr(cloud_sync, "_refresh_local_cloud_media_signature", lambda *a: None)
    monkeypatch.setattr(cloud_sync, "_store_remote_snapshot", lambda *a, **k: None)

    class Client:
        def get_observation(self, _id): return dict(remote_obs)
        def _patch(self, path, payload): calls.append(("patch", path, payload))

    monkeypatch.setattr(cloud_sync.ImageDB, "get_images_for_observation", lambda _id: [])
    local_meas = [{"id": 31, "cloud_id": "m31", "image_id": 0, "length_um": 5.0}]
    remote_meas = [{"id": "m31", "desktop_id": 31, "image_id": None, "length_um": 5.0}]
    monkeypatch.setattr(cloud_sync.MeasurementDB, "get_measurements_for_observation",
                        lambda _id: list(local_meas))
    monkeypatch.setattr(cloud_sync, "_pull_remote_measurements_for_images",
                        lambda *a: list(remote_meas))
    baseline = cloud_sync.build_conflict_plan_baseline(
        local_obs=local_obs, remote_obs=remote_obs,
        local_images=[], remote_images=[],
        local_measurements=local_meas, remote_measurements=remote_meas,
    )
    result = cloud_sync.resolve_conflict_plan(Client(), 1, plan={
        "allow_media_deletion": False,
        "derived_statistics": "recompute_from_measurements",
        "baseline": baseline,
        "items": [
            {"kind": "field", "field": "common_name", "choice": "cloud"},
            {"kind": "measurement", "side": "matched", "local_id": 31,
             "cloud_id": "m31", "choice": "local"},
        ],
    })
    assert ("cloud_fields", {"common_name"}) in calls
    assert ("push_measurements", {31}) in calls
    assert ("statistics_local", "recomputed") in calls
    assert any(call[0] == "patch" and call[2] == {"spore_statistics": "recomputed"} for call in calls)
    assert result["media_deleted"] is False


def test_resolution_plan_requires_image_preparer_and_never_deletes(monkeypatch):
    monkeypatch.setattr(cloud_sync.ObservationDB, "get_observation",
                        lambda _id: {"id": 1, "cloud_id": "obs-cloud"})
    monkeypatch.setattr(cloud_sync, "_pull_remote_images_for_sync", lambda *a: [])
    monkeypatch.setattr(cloud_sync, "_pull_remote_measurements_for_images", lambda *a: [])

    class Client:
        def get_observation(self, _id): return {"id": "obs-cloud"}

    monkeypatch.setattr(cloud_sync.ImageDB, "get_images_for_observation",
                        lambda _id: [{"id": 9, "image_type": "microscope"}])
    monkeypatch.setattr(cloud_sync.MeasurementDB, "get_measurements_for_observation",
                        lambda _id: [])
    baseline = cloud_sync.build_conflict_plan_baseline(
        local_obs={"id": 1, "cloud_id": "obs-cloud"},
        remote_obs={"id": "obs-cloud"},
        local_images=[{"id": 9, "image_type": "microscope"}],
        remote_images=[], local_measurements=[], remote_measurements=[],
    )
    with pytest.raises(cloud_sync.CloudSyncError, match="image preparer"):
        cloud_sync.resolve_conflict_plan(Client(), 1, plan={
            "baseline": baseline,
            "items": [{"kind": "image", "side": "local_only", "local_id": 9,
                       "choice": "upload"}],
            "allow_media_deletion": False,
        })
    with pytest.raises(cloud_sync.CloudSyncError, match="cannot authorize media deletion"):
        cloud_sync.resolve_conflict_plan(Client(), 1, plan={"allow_media_deletion": True})
