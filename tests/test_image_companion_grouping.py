from utils.image_companion_grouping import (
    group_companion_paths,
    select_preferred_companion_path,
)


def test_companion_grouping_respects_source_preference(monkeypatch, tmp_path):
    raw_path = tmp_path / "P070020_1.ORF"
    jpeg_path = tmp_path / "P070020_1.JPG"
    raw_path.write_bytes(b"raw-bytes")
    jpeg_path.write_bytes(b"jpeg-bytes")
    monkeypatch.setattr("utils.image_companion_grouping.get_image_datetime", lambda _path: None)
    monkeypatch.setattr("utils.image_companion_grouping.read_rawpy_capture_datetime", lambda _path: None)

    assert select_preferred_companion_path([raw_path, jpeg_path]) == str(raw_path.resolve())
    assert select_preferred_companion_path(
        [raw_path, jpeg_path],
        source_preference="camera_jpeg",
    ) == str(jpeg_path.resolve())

    default_groups = group_companion_paths([raw_path, jpeg_path])
    jpeg_groups = group_companion_paths([raw_path, jpeg_path], source_preference="camera_jpeg")

    assert len(default_groups) == 1
    assert len(jpeg_groups) == 1
    assert default_groups[0].preferred_path == str(raw_path.resolve())
    assert jpeg_groups[0].preferred_path == str(jpeg_path.resolve())
    assert default_groups[0].has_raw is True
    assert jpeg_groups[0].has_raw is True
