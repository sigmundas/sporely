"""Render offscreen PNGs of the conflict-dialog states.

Uses the same monkey-patched fixtures the test suite uses so no live
Sporely Cloud call, no live sync, and no real database access occurs.
Intended for review only — the produced PNGs are throwaway artifacts
under /tmp/sporely-screens/ by default.

Usage:

    QT_QPA_PLATFORM=offscreen \\
      python -m tools.render_conflict_screenshots [output_dir]
"""
from __future__ import annotations

import base64
import copy
import json
import os
import sys
import time
from pathlib import Path
from unittest.mock import Mock

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from PySide6.QtCore import QByteArray, Qt
from PySide6.QtGui import QPalette, QColor
from PySide6.QtWidgets import QApplication

import ui.cloud_conflict_dialog as conflict_ui


OUT_DIR = Path(sys.argv[1] if len(sys.argv) > 1 else "/tmp/sporely-screens")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def _fixed_token(expires_in: int = 3600) -> str:
    def part(value):
        return (
            base64.urlsafe_b64encode(json.dumps(value).encode()).decode().rstrip("=")
        )
    return f"{part({'alg': 'none'})}.{part({'sub': 'user-1', 'exp': time.time() + expires_in})}.signature"


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


def _default_detail(*, fields=False, measurement=True, possible=False, identity=False):
    measurement_conflicts = [_measurement_conflict()] if measurement else []
    pairs = [
        {
            "pairing": "identity_conflict" if identity else "authoritative",
            "match_basis": "cloud_id",
            "status": "identity_conflict" if identity
                       else ("measurements_differ" if measurement else "same"),
            "identity_conflict_reasons": (
                ["multiple local images share the same cloud ID"] if identity else []
            ),
            "local": {
                "local_id": 8, "cloud_id": "image-cloud-8", "image_type": "microscope",
                "sort_order": 8, "measurement_count": 3,
                "thumbnail_source": "missing.jpg",
            },
            "remote": {
                "local_id": 8, "cloud_id": "image-cloud-8", "image_type": "microscope",
                "sort_order": 8, "measurement_count": 3,
                "thumbnail_source": "cloud/key.webp",
            },
            "measurement_conflicts": measurement_conflicts,
            "measurement_pairs": measurement_conflicts,
        },
        {
            "pairing": "unpaired",
            "status": "possible_match" if possible else "local_only",
            "local": {
                "local_id": 9, "image_type": "microscope", "sort_order": 9,
                "measurement_count": 0, "thumbnail_source": "missing.jpg",
            },
            "remote": None,
            "possible_counterpart": {"cloud_id": "possible-cloud"} if possible else None,
        },
        {
            "pairing": "unpaired", "status": "cloud_only", "local": None,
            "remote": {
                "cloud_id": "cloud-only", "image_type": "field", "sort_order": 1,
                "measurement_count": 0, "thumbnail_source": "cloud/missing.webp",
            },
        },
    ]
    return {
        "local_id": 593,
        "cloud_id": "902",
        "local_observation": {"genus": "Mycena", "species": "haematopus",
                              "date": "2026-07-19", "location": "Oslo",
                              "common_name": ""},
        "remote_observation": {"genus": "Mycena", "species": "haematopus",
                               "date": "2026-07-19", "location": "Oslo",
                               "common_name": "Bleeding fairy helmet"},
        "field_rows": ([{"field": "common_name", "label": "Common name",
                         "baseline": "", "local": "",
                         "remote": "Bleeding fairy helmet",
                         "local_changed": False, "remote_changed": True}] if fields else []),
        "image_pairs": pairs,
        "image_mismatches": [],
        "identity_conflicts": [pair for pair in pairs if pair.get("status") == "identity_conflict"],
        "measurement_conflicts": measurement_conflicts,
        "measurement_pairs": measurement_conflicts,
        "baseline_available": True,
        "derived_statistics": ({"status": "recompute_from_measurements",
                                "rows": [{}]} if measurement else None),
        "plan_baseline": {
            "schema_version": 1,
            "local_observation": {}, "remote_observation": {},
            "local_images": [], "remote_images": [],
            "local_measurements": [], "remote_measurements": [],
        },
    }


def _prepare_dialog(app, detail):
    conflict_ui.get_app_settings = lambda: {
        "cloud_access_token": _fixed_token(),
        "cloud_user_id": "user-1",
    }
    conflict_ui.get_conflict_detail = lambda *args, **kwargs: copy.deepcopy(detail)
    conflict_ui.ConflictDetailWorker.start = lambda self: self.run()
    conflict_ui.ConflictThumbnailWorker.start = lambda self: self.failed.emit(
        self.generation, self.cache_key, "unavailable"
    )
    dialog = conflict_ui.CloudConflictDialog(
        conflicts=[{"local_id": 593, "cloud_id": "902"}],
    )
    dialog.resize(1240, 820)
    app.processEvents()
    return dialog


def _shot(dialog, app, name, *, palette=None):
    if palette is not None:
        dialog.setPalette(palette)
        app.processEvents()
    pm = dialog.grab()
    path = OUT_DIR / f"{name}.png"
    pm.save(str(path))
    print(f"saved {path}")


def _dark_palette():
    p = QPalette()
    p.setColor(QPalette.Window, QColor("#202124"))
    p.setColor(QPalette.WindowText, QColor("#e8eaed"))
    p.setColor(QPalette.Base, QColor("#2b2c30"))
    p.setColor(QPalette.Text, QColor("#e8eaed"))
    p.setColor(QPalette.Button, QColor("#2b2c30"))
    p.setColor(QPalette.ButtonText, QColor("#e8eaed"))
    return p


def main():
    app = QApplication.instance() or QApplication([])

    # 1. Mixed cloud-field / local-measurement plan
    d1 = _prepare_dialog(app, _default_detail(fields=True, measurement=True))
    _shot(d1, app, "01_mixed_cloud_field_local_measurement")
    d1.close()

    # 2. Local-only image (default detail has one)
    d2 = _prepare_dialog(app, _default_detail(measurement=False))
    _shot(d2, app, "02_local_and_cloud_only_images")
    d2.close()

    # 3. Scientific geometry difference (default measurement conflict)
    d3 = _prepare_dialog(app, _default_detail())
    _shot(d3, app, "03_scientific_geometry_difference")
    d3.close()

    # 4. Possible-match (unpaired)
    d4 = _prepare_dialog(app, _default_detail(measurement=False, possible=True))
    _shot(d4, app, "04_possible_match_warning")
    d4.close()

    # 5. Identity conflict — apply must be disabled
    d5 = _prepare_dialog(app, _default_detail(measurement=False, identity=True))
    _shot(d5, app, "05_identity_conflict_apply_disabled")
    d5.close()

    # 6. Incomplete plan (safe additions preset leaves measurement unresolved)
    d6 = _prepare_dialog(app, _default_detail())
    d6._merge_btn.click()
    app.processEvents()
    _shot(d6, app, "06_incomplete_plan_apply_disabled")
    d6.close()

    # 7. Active apply progress (no confirmation popup)
    d7 = _prepare_dialog(app, _default_detail(fields=True))
    d7._keep_local_btn.click()
    d7._show_status("Applying selected changes…", "info")
    app.processEvents()
    _shot(d7, app, "07_active_apply_no_popup")
    d7.close()

    # 8. Rotation-only difference (presentation-only, nonblocking)
    detail_rot = _default_detail(measurement=False)
    detail_rot["image_pairs"] = [{
        "status": "same", "pairing": "authoritative",
        "local": {"local_id": 8, "cloud_id": "image-cloud-8", "image_type": "microscope",
                   "sort_order": 5, "measurement_count": 0},
        "remote": {"local_id": 8, "cloud_id": "image-cloud-8", "image_type": "microscope",
                    "sort_order": 3, "measurement_count": 0},
        "metadata_diff_details": [],
        "measurement_pairs": [],
        "presentation_differences": [{"field": "sort_order", "local": 5, "remote": 3,
                                       "automatic_policy": "local_desktop"}],
    }]
    d8 = _prepare_dialog(app, detail_rot)
    d8._show_matching_check.setChecked(True)
    app.processEvents()
    _shot(d8, app, "08_image_order_only_informational")
    d8.close()

    # 9. Light + dark of the mixed conflict view
    d9 = _prepare_dialog(app, _default_detail(fields=True, measurement=True))
    _shot(d9, app, "09_light_mode_conflict")
    _shot(d9, app, "10_dark_mode_conflict", palette=_dark_palette())
    d9.close()

    print(f"screenshots written under {OUT_DIR}")


if __name__ == "__main__":
    main()
