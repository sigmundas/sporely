from ui.image_gallery_widget import ImageGalleryWidget


def test_build_raw_source_badges_marks_raw_backed_derivatives():
    metadata = {
        "raw_processing": {
            "source": {
                "kind": "camera_raw",
                "path": "/tmp/P070020_1.ORF",
                "mime_type": "image/x-raw",
            }
        }
    }

    assert ImageGalleryWidget.build_raw_source_badges(metadata) == ["RAW-derived"]
    assert ImageGalleryWidget.build_raw_source_badges({"raw_processing": {"source": {"kind": "local_derivative"}}}) == []
    assert ImageGalleryWidget.build_raw_source_badges({"image_type": "microscope"}) == []
