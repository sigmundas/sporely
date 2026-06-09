from __future__ import annotations

from copy import deepcopy

from utils.image_metadata_merge import merge_image_lab_metadata


def test_merge_image_lab_metadata_deep_merges_nested_metadata_without_mutating_inputs():
    caller_metadata = {
        "image_type": "microscope",
        "contrast": "phase",
        "raw_processing": {
            "source": {
                "kind": "camera_raw",
                "path": "/raw/file.orf",
            },
            "settings": {
                "white_balance_mode": "camera",
                "auto_levels": True,
            },
        },
        "image_processing": {
            "steps": {
                "denoise": {
                    "enabled": False,
                    "strength": 0.25,
                }
            }
        },
    }
    ingest_metadata = {
        "image_type": "microscope",
        "objective_name": "40x",
        "mount_medium": "water",
        "stain": "none",
        "sample_type": "spore",
        "raw_processing": {
            "source": {
                "mime_type": "image/x-raw",
            },
            "settings": {
                "auto_levels": False,
                "tone_curve_enabled": True,
            },
        },
        "image_processing": {
            "steps": {
                "denoise": {
                    "enabled": True,
                },
                "resize": {
                    "factor": 0.75,
                },
            }
        },
    }
    caller_snapshot = deepcopy(caller_metadata)
    ingest_snapshot = deepcopy(ingest_metadata)

    merged = merge_image_lab_metadata(None, caller_metadata, "skip-me", ingest_metadata)

    assert caller_metadata == caller_snapshot
    assert ingest_metadata == ingest_snapshot
    assert merged["image_type"] == "microscope"
    assert merged["contrast"] == "phase"
    assert merged["objective_name"] == "40x"
    assert merged["mount_medium"] == "water"
    assert merged["stain"] == "none"
    assert merged["sample_type"] == "spore"
    assert merged["raw_processing"]["source"]["kind"] == "camera_raw"
    assert merged["raw_processing"]["source"]["path"] == "/raw/file.orf"
    assert merged["raw_processing"]["source"]["mime_type"] == "image/x-raw"
    assert merged["raw_processing"]["settings"]["white_balance_mode"] == "camera"
    assert merged["raw_processing"]["settings"]["auto_levels"] is False
    assert merged["raw_processing"]["settings"]["tone_curve_enabled"] is True
    assert merged["image_processing"]["steps"]["denoise"]["enabled"] is True
    assert merged["image_processing"]["steps"]["denoise"]["strength"] == 0.25
    assert merged["image_processing"]["steps"]["resize"]["factor"] == 0.75


def test_merge_image_lab_metadata_prefers_later_scalar_values_and_ignores_none_values():
    merged = merge_image_lab_metadata(
        {
            "contrast": "phase",
            "file_purpose": "microscope",
            "raw_processing": {
                "settings": {
                    "white_balance_mode": "camera",
                    "auto_levels": True,
                }
            },
        },
        {
            "contrast": "dic",
            "file_purpose": None,
            "raw_processing": {
                "settings": {
                    "auto_levels": False,
                }
            },
        },
    )

    assert merged["contrast"] == "dic"
    assert merged["file_purpose"] == "microscope"
    assert merged["raw_processing"]["settings"]["white_balance_mode"] == "camera"
    assert merged["raw_processing"]["settings"]["auto_levels"] is False
