from utils import exif_reader


def test_get_gps_coordinates_ignores_raw_gps_pointer(monkeypatch):
    monkeypatch.setattr(exif_reader, "get_exif_data", lambda _path: {"GPSInfo": 26})

    assert exif_reader.get_gps_coordinates("image.jpg") == (None, None)


def test_get_gps_coordinates_decodes_gps_ifd(monkeypatch):
    monkeypatch.setattr(
        exif_reader,
        "get_exif_data",
        lambda _path: {
            "GPSInfo": {
                1: "N",
                2: (59, 54, 0),
                3: "E",
                4: (10, 45, 0),
            }
        },
    )

    assert exif_reader.get_gps_coordinates("image.jpg") == (59.9, 10.75)
