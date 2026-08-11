from types import SimpleNamespace

import ui.observations_tab as observations_tab


def test_observation_image_browser_builds_same_metadata_tag_row(monkeypatch):
    captured = {}
    browser = SimpleNamespace(
        current_image_id=lambda: 7,
        image_label=SimpleNamespace(
            set_top_left_tags=lambda tags, keys=None: captured.update(tags=tags, keys=keys)
        ),
        tr=lambda text: text,
    )
    monkeypatch.setattr(
        observations_tab.ImageDB,
        "get_image",
        lambda _image_id: {
            "objective_name": "40x",
            "contrast": "DIC",
            "mount_medium": "Water",
            "stain": "Not_set",
            "sample_type": "Fresh",
            "sample_source": "Hymenium",
        },
    )
    monkeypatch.setattr(
        observations_tab,
        "load_objectives",
        lambda: {"40x": {"magnification": 40, "name": "40x"}},
    )

    observations_tab._ObservationImageBrowser._set_current_image_tags(browser)

    assert [text for text, _color in captured["tags"]] == [
        "40x DIC",
        "Water",
        "No stain",
        "Fresh",
        "Hymenium",
    ]
    assert captured["keys"] == ["microscope", "mount", "stain", "sample", "sample_source"]
