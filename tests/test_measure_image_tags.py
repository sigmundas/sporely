from types import SimpleNamespace

from PySide6.QtGui import QColor

from ui.main_window import MainWindow


def test_measure_image_tags_include_context_and_append_contrast():
    captured = []
    captured_keys = []
    image_label = SimpleNamespace(
        objective_text="40x",
        objective_color=QColor("#3498db"),
        set_top_left_tags=lambda tags, keys: (captured.extend(tags), captured_keys.extend(keys)),
    )
    window = SimpleNamespace(image_label=image_label)

    MainWindow._set_measure_image_tags(
        window,
        {
            "contrast": "DIC",
            "mount_medium": "Water",
            "stain": "Congo_Red",
            "sample_type": "Fresh",
            "sample_source": "Hymenium",
        },
    )

    assert [text for text, _color in captured] == [
        "40x DIC",
        "Water",
        "Congo Red",
        "Fresh",
        "Hymenium",
    ]
    assert QColor(captured[2][1]).name() == "#c0392b"
    assert captured_keys == ["microscope", "mount", "stain", "sample", "sample_source"]


def test_measure_image_tags_omit_unset_values_and_use_lab_metadata_fallback():
    captured = []
    captured_keys = []
    image_label = SimpleNamespace(
        objective_text="",
        objective_color=QColor("#3498db"),
        set_top_left_tags=lambda tags, keys: (captured.extend(tags), captured_keys.extend(keys)),
    )
    window = SimpleNamespace(image_label=image_label)

    MainWindow._set_measure_image_tags(
        window,
        {
            "mount_medium": "Not_set",
            "lab_metadata": {"stain": "Cotton_Blue", "sample_type": "Dried"},
        },
    )

    assert [text for text, _color in captured] == ["Cotton Blue", "Dried"]
    assert captured_keys == ["stain", "sample"]


def test_measure_image_tags_show_no_stain_when_stain_is_unset():
    captured = []
    image_label = SimpleNamespace(
        objective_text="40x",
        objective_color=QColor("#3498db"),
        set_top_left_tags=lambda tags, _keys: captured.extend(tags),
    )
    window = SimpleNamespace(image_label=image_label, tr=lambda text: text)

    MainWindow._set_measure_image_tags(window, {"stain": "Not_set"})

    assert [text for text, _color in captured] == ["40x", "No stain"]
