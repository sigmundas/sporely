"""Conflict-dialog review states and their sync-specific mocked fixtures."""
from __future__ import annotations

import base64
import copy
import json

from unittest.mock import patch

import ui.cloud_conflict_dialog as conflict_ui

from ..context import ReviewContext
from ..registry import ReviewScenario, ScenarioRegistry


VIEWPORT = (1240, 820)


def _fixed_token() -> str:
    def part(value) -> str:
        return base64.urlsafe_b64encode(json.dumps(value).encode()).decode().rstrip("=")

    return (
        f"{part({'alg': 'none'})}."
        f"{part({'sub': 'user-1', 'exp': 4_102_444_800})}.signature"
    )


def _measurement_conflict() -> dict:
    return {
        "status": "values_differ",
        "local_id": 31,
        "cloud_id": "measurement-cloud-31",
        "local_image_id": 8,
        "cloud_image_id": "image-cloud-8",
        "fields": ["length_um", "p1_x", "p2_x"],
        "local_values": {
            "length_um": 8.72,
            "gallery_rotation": 90,
            "p1_x": 3,
            "p2_x": 9,
        },
        "remote_values": {
            "length_um": 8.46,
            "gallery_rotation": 0,
            "p1_x": 2,
            "p2_x": 8,
        },
        "baseline_values": {
            "length_um": 8.46,
            "gallery_rotation": 0,
            "p1_x": 2,
            "p2_x": 8,
        },
        "baseline_available": True,
        "change_origin": "local",
        "geometry_summary": ["length axis moved"],
        "geometry_baseline": "Original geometry",
        "geometry_local": "Length axis moved",
        "geometry_cloud": "Unchanged",
    }


def _default_detail(
    *,
    fields: bool = False,
    measurement: bool = True,
    possible: bool = False,
    identity: bool = False,
) -> dict:
    measurement_conflicts = [_measurement_conflict()] if measurement else []
    pairs = [
        {
            "pairing": "identity_conflict" if identity else "authoritative",
            "match_basis": "cloud_id",
            "status": (
                "identity_conflict"
                if identity
                else ("measurements_differ" if measurement else "same")
            ),
            "identity_conflict_reasons": (
                ["multiple local images share the same cloud ID"] if identity else []
            ),
            "local": {
                "local_id": 8,
                "cloud_id": "image-cloud-8",
                "image_type": "microscope",
                "sort_order": 8,
                "measurement_count": 3,
                "thumbnail_source": "missing.jpg",
            },
            "remote": {
                "local_id": 8,
                "cloud_id": "image-cloud-8",
                "image_type": "microscope",
                "sort_order": 8,
                "measurement_count": 3,
                "thumbnail_source": "cloud/key.webp",
            },
            "measurement_conflicts": measurement_conflicts,
            "measurement_pairs": measurement_conflicts,
        },
        {
            "pairing": "unpaired",
            "status": "possible_match" if possible else "local_only",
            "local": {
                "local_id": 9,
                "image_type": "microscope",
                "sort_order": 9,
                "measurement_count": 0,
                "thumbnail_source": "missing.jpg",
            },
            "remote": None,
            "possible_counterpart": (
                {"cloud_id": "possible-cloud"} if possible else None
            ),
        },
        {
            "pairing": "unpaired",
            "status": "cloud_only",
            "local": None,
            "remote": {
                "cloud_id": "cloud-only",
                "image_type": "field",
                "sort_order": 1,
                "measurement_count": 0,
                "thumbnail_source": "cloud/missing.webp",
            },
        },
    ]
    return {
        "local_id": 593,
        "cloud_id": "902",
        "local_observation": {
            "genus": "Mycena",
            "species": "haematopus",
            "date": "2026-07-19",
            "location": "Oslo",
            "common_name": "",
        },
        "remote_observation": {
            "genus": "Mycena",
            "species": "haematopus",
            "date": "2026-07-19",
            "location": "Oslo",
            "common_name": "Bleeding fairy helmet",
        },
        "field_rows": (
            [
                {
                    "field": "common_name",
                    "label": "Common name",
                    "baseline": "",
                    "local": "",
                    "remote": "Bleeding fairy helmet",
                    "local_changed": False,
                    "remote_changed": True,
                }
            ]
            if fields
            else []
        ),
        "image_pairs": pairs,
        "image_mismatches": [],
        "identity_conflicts": [
            pair for pair in pairs if pair.get("status") == "identity_conflict"
        ],
        "measurement_conflicts": measurement_conflicts,
        "measurement_pairs": measurement_conflicts,
        "baseline_available": True,
        "derived_statistics": (
            {"status": "recompute_from_measurements", "rows": [{}]}
            if measurement
            else None
        ),
        "plan_baseline": {
            "schema_version": 1,
            "local_observation": {},
            "remote_observation": {},
            "local_images": [],
            "remote_images": [],
            "local_measurements": [],
            "remote_measurements": [],
        },
    }


def _install_fixture(context: ReviewContext) -> None:
    if context.state.get("conflict.fixture-installed"):
        return
    context.enter_fixture(
        patch.object(
            conflict_ui,
            "get_app_settings",
            lambda: {"cloud_access_token": _fixed_token(), "cloud_user_id": "user-1"},
        )
    )
    context.enter_fixture(
        patch.object(
            conflict_ui.ConflictDetailWorker,
            "start",
            lambda self: self.run(),
        )
    )
    context.enter_fixture(
        patch.object(
            conflict_ui.ConflictThumbnailWorker,
            "start",
            lambda self: self.failed.emit(
                self.generation, self.cache_key, "unavailable"
            ),
        )
    )
    context.state["conflict.fixture-installed"] = True


def _dialog(context: ReviewContext, detail: dict):
    _install_fixture(context)
    # The dialog defers its worker start through Qt. Keep this scenario's detail
    # provider alive beyond construction so the queued start sees the fixture.
    context.enter_fixture(
        patch.object(
            conflict_ui,
            "get_conflict_detail",
            lambda *_args, **_kwargs: copy.deepcopy(detail),
        )
    )
    conflict = {"local_id": 593, "cloud_id": "902"}
    dialog = conflict_ui.CloudConflictDialog(conflicts=[conflict])
    context.app.processEvents()
    # The production worker is asynchronous. The legacy renderer replaced
    # QThread.start with a synchronous run, but the dialog now sets its loading
    # state immediately after start returns. Deliver the deterministic result
    # through the production completion handler to establish the intended UI.
    dialog._detail_loaded(
        dialog._selection_generation,
        dialog._key(conflict),
        copy.deepcopy(detail),
    )
    context.app.processEvents()
    return dialog


def _mixed(context: ReviewContext):
    dialog = _dialog(context, _default_detail(fields=True, measurement=True))
    dialog._set_choice("field:common_name", "cloud")
    dialog._set_choice("measurement:31", "local")
    dialog._set_choice("image:9", "upload")
    dialog._set_choice("image:cloud-only", "keep_cloud")
    dialog._update_apply_enabled()
    context.app.processEvents()
    return dialog


def _field_only(context: ReviewContext):
    detail = _default_detail(fields=True, measurement=False)
    detail["image_pairs"] = []
    return _dialog(context, detail)


def _local_cloud_images(context: ReviewContext):
    return _dialog(context, _default_detail(measurement=False))


def _geometry(context: ReviewContext):
    return _dialog(context, _default_detail())


def _possible_match(context: ReviewContext):
    return _dialog(context, _default_detail(measurement=False, possible=True))


def _identity(context: ReviewContext):
    return _dialog(context, _default_detail(measurement=False, identity=True))


def _incomplete(context: ReviewContext):
    dialog = _dialog(context, _default_detail())
    dialog._merge_btn.click()
    context.app.processEvents()
    return dialog


def _progress(context: ReviewContext):
    dialog = _dialog(context, _default_detail(fields=True))
    dialog._keep_local_btn.click()
    dialog._show_status("Applying selected changes…", "info")
    context.app.processEvents()
    return dialog


def _image_order(context: ReviewContext):
    detail = _default_detail(measurement=False)
    detail["image_pairs"] = [
        {
            "status": "same",
            "pairing": "authoritative",
            "local": {
                "local_id": 8,
                "cloud_id": "image-cloud-8",
                "image_type": "microscope",
                "sort_order": 5,
                "measurement_count": 0,
            },
            "remote": {
                "local_id": 8,
                "cloud_id": "image-cloud-8",
                "image_type": "microscope",
                "sort_order": 3,
                "measurement_count": 0,
            },
            "metadata_diff_details": [],
            "measurement_pairs": [],
            "presentation_differences": [
                {
                    "field": "sort_order",
                    "local": 5,
                    "remote": 3,
                    "automatic_policy": "local_desktop",
                }
            ],
        }
    ]
    dialog = _dialog(context, detail)
    dialog._show_matching_check.setChecked(True)
    context.app.processEvents()
    return dialog


def register_conflict_scenarios(registry: ScenarioRegistry) -> None:
    scenarios = (
        ReviewScenario(
            id="conflict.local-changes",
            group="conflict",
            title="Mixed field and measurement choices",
            description="Cloud field changes and local measurement changes are resolved together.",
            viewport=VIEWPORT,
            build=_mixed,
        ),
        ReviewScenario(
            id="conflict.field",
            group="conflict",
            title="Observation field conflict",
            description="A field-only conflict requires an explicit per-field choice.",
            viewport=VIEWPORT,
            build=_field_only,
        ),
        ReviewScenario(
            id="conflict.local-cloud-images",
            group="conflict",
            title="Local-only and cloud-only images",
            description="Unpaired local and cloud media remain visible as explicit choices.",
            viewport=VIEWPORT,
            build=_local_cloud_images,
        ),
        ReviewScenario(
            id="conflict.geometry",
            group="conflict",
            title="Scientific geometry difference",
            description="A measurement axis geometry conflict exposes the scientific detail.",
            viewport=VIEWPORT,
            build=_geometry,
        ),
        ReviewScenario(
            id="conflict.possible-match",
            group="conflict",
            title="Possible image match warning",
            description="An uncertain unpaired image match is presented without asserting identity.",
            viewport=VIEWPORT,
            build=_possible_match,
        ),
        ReviewScenario(
            id="conflict.identity",
            group="conflict",
            title="Identity conflict with Apply disabled",
            description="Ambiguous cloud identity blocks applying an unsafe merge plan.",
            viewport=VIEWPORT,
            build=_identity,
        ),
        ReviewScenario(
            id="conflict.incomplete-plan",
            group="conflict",
            title="Incomplete plan with Apply disabled",
            description="The safe-additions preset leaves a measurement decision unresolved.",
            viewport=VIEWPORT,
            build=_incomplete,
        ),
        ReviewScenario(
            id="conflict.progress",
            group="conflict",
            title="Active apply progress",
            description="The dialog reports active application without a confirmation popup.",
            viewport=VIEWPORT,
            build=_progress,
        ),
        ReviewScenario(
            id="conflict.image-order",
            group="conflict",
            title="Image-order-only informational state",
            description="Presentation-only image ordering remains informational and nonblocking.",
            viewport=VIEWPORT,
            build=_image_order,
        ),
        ReviewScenario(
            id="conflict.light",
            group="conflict",
            title="Conflict dialog in light mode",
            description="The mixed conflict state uses the real application light theme.",
            viewport=VIEWPORT,
            build=_mixed,
            theme="light",
        ),
        ReviewScenario(
            id="conflict.dark",
            group="conflict",
            title="Conflict dialog in dark mode",
            description="The mixed conflict state uses the real application dark theme.",
            viewport=VIEWPORT,
            build=_mixed,
            theme="dark",
        ),
    )
    for scenario in scenarios:
        registry.register(scenario)
