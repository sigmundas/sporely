from __future__ import annotations

import os
import time
from pathlib import Path

from PIL import Image

from utils.publish_media_cache import (
    PublishMediaCache,
    publish_media_signature,
    validate_cached_image,
)


def _write_png(path: Path, color: tuple[int, int, int] = (12, 34, 56)) -> None:
    Image.new("RGB", (8, 6), color).save(path, "PNG")


def test_signature_is_deterministic_and_renderer_versioned():
    first = publish_media_signature(
        "mosaic",
        "3",
        {"measurements": [{"id": 2}, {"id": 7}], "excluded": {9, 4}},
    )
    second = publish_media_signature(
        "mosaic",
        "3",
        {"excluded": {4, 9}, "measurements": [{"id": 2}, {"id": 7}]},
    )
    changed = publish_media_signature(
        "mosaic",
        "4",
        {"measurements": [{"id": 2}, {"id": 7}], "excluded": {9, 4}},
    )

    assert first == second
    assert first != changed


def test_cache_survives_new_cache_object_and_rejects_corruption(tmp_path):
    source = tmp_path / "render.png"
    _write_png(source)
    signature = publish_media_signature("mosaic", "1", {"observation_id": 7})

    first_cache = PublishMediaCache(tmp_path / "cache")
    cached_path = first_cache.store_file(
        "mosaic",
        signature,
        "png",
        source,
        validator=validate_cached_image,
    )

    second_cache = PublishMediaCache(tmp_path / "cache")
    assert second_cache.lookup(
        "mosaic",
        signature,
        "png",
        validator=validate_cached_image,
    ) == cached_path

    cached_path.write_bytes(b"not an image")
    assert second_cache.lookup(
        "mosaic",
        signature,
        "png",
        validator=validate_cached_image,
    ) is None
    assert not cached_path.exists()


def test_failed_store_does_not_leave_valid_looking_or_temporary_entry(tmp_path):
    source = tmp_path / "broken.png"
    source.write_bytes(b"broken")
    cache = PublishMediaCache(tmp_path / "cache")
    signature = publish_media_signature("mosaic", "1", {"observation_id": 8})

    try:
        cache.store_file(
            "mosaic",
            signature,
            "png",
            source,
            validator=validate_cached_image,
        )
    except ValueError:
        pass
    else:
        raise AssertionError("invalid render should not be cached")

    assert cache.lookup("mosaic", signature, "png") is None
    assert list((tmp_path / "cache").rglob("*.tmp-*")) == []


def test_cleanup_removes_stale_assets_and_abandoned_temporary_files(tmp_path):
    cache = PublishMediaCache(tmp_path / "cache")
    signature = publish_media_signature("variant", "1", {"target": "inat"})
    asset = cache.path_for("variant", signature, "jpg")
    asset.parent.mkdir(parents=True)
    asset.write_bytes(b"asset")
    temporary = asset.parent / f".{asset.stem}.tmp-old.jpg"
    temporary.write_bytes(b"temporary")
    old = time.time() - (3 * 86400)
    os.utime(asset, (old, old))
    os.utime(temporary, (old, old))

    result = cache.cleanup(max_age_days=1, temporary_max_age_hours=1)

    assert result == {"assets": 1, "temporary": 1}
    assert not asset.exists()
    assert not temporary.exists()
