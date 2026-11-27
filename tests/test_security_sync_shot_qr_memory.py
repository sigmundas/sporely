"""F23: Sync Shot QR decoding must keep every working array within a pixel budget."""
from datetime import datetime, timezone

from PIL import Image

from utils import sync_shot_qr

PIXEL_BUDGET = 16_500_000


def _make_large_photo(path, size=(6000, 4500), box_size=10):
    payload = sync_shot_qr.build_sync_shot_payload(
        datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc), "dummy-session"
    )
    qr = sync_shot_qr.render_sync_shot_qr(payload, box_size=box_size)
    canvas = Image.new("RGB", size, (255, 255, 255))
    canvas.paste(qr, (size[0] // 3, size[1] // 3))
    canvas.save(path, format="JPEG", quality=95)
    return payload


def test_candidate_arrays_stay_within_the_pixel_budget(tmp_path):
    path = tmp_path / "large.jpg"
    _make_large_photo(path)
    image = sync_shot_qr._load_decode_image(path)
    assert image.shape[0] * image.shape[1] <= PIXEL_BUDGET
    seen = 0
    for candidate in sync_shot_qr._collect_candidate_arrays(image):
        seen += 1
        assert candidate.shape[0] * candidate.shape[1] <= PIXEL_BUDGET
    assert seen > 0


def test_large_photo_still_decodes(tmp_path):
    path = tmp_path / "large.jpg"
    payload = _make_large_photo(path)
    result = sync_shot_qr.decode_sync_shot_qr(path)
    assert [m["payload"] for m in result["matches"]] == [payload]


def test_small_photo_still_decodes(tmp_path):
    path = tmp_path / "small.png"
    payload = _make_large_photo(path, size=(800, 600))
    result = sync_shot_qr.decode_sync_shot_qr(path)
    assert [m["payload"] for m in result["matches"]] == [payload]


def test_small_qr_in_a_camera_sized_photo_still_decodes(tmp_path):
    # A QR roughly 117 px wide in a 12 MP photo was readable before and must stay so.
    path = tmp_path / "camera.jpg"
    payload = _make_large_photo(path, size=(4000, 3000), box_size=3)
    result = sync_shot_qr.decode_sync_shot_qr(path)
    assert [m["payload"] for m in result["matches"]] == [payload]
