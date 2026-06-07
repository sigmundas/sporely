from utils.raw_presets import build_raw_preset_key, normalize_raw_context


def test_raw_preset_key_is_deterministic_and_ignores_noise():
    context = {
        "camera_name": "Canon EOS R5",
        "microscope_model": "BX53",
        "contrast_label": "Phase Contrast",
        "stain_label": "H&E",
        "mount_medium_label": "Water",
        "sample_label": "Spore print",
        "objective_label": "20x",
        "objective_power": "20x",
        "session_id": "session-123",
        "observation_id": 99,
    }

    normalized = normalize_raw_context(context)
    assert normalized["camera_model"] == "Canon EOS R5"
    assert normalized["microscope"] == "BX53"
    assert normalized["contrast_mode"] == "Phase Contrast"
    assert normalized["stain"] == "H&E"
    assert normalized["mountant"] == "Water"
    assert normalized["sample_type"] == "Spore print"
    assert normalized["objective"] == "20x"
    assert normalized["magnification"] == "20x"

    key_one = build_raw_preset_key(context)
    key_two = build_raw_preset_key({**context, "session_id": "session-999", "observation_id": 1})

    assert key_one == key_two
    assert key_one.startswith("raw-preset:v1:")
    assert "session" not in key_one
