"""Local image file helpers shared by image push and pull: content signature,
detected extension and new Worker storage keys.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S6 of the cloud-sync extraction.
"""

from __future__ import annotations

import hashlib
import uuid
from pathlib import Path

from PIL import Image

from utils.cloud_sync_impl.common import _normalize_cloud_media_key, _safe_int


def _new_image_storage_suffix() -> str:
    """Return a random, non-time-derived unique suffix for a new image key.

    Storage keys are public CDN paths; they must not leak capture or creation
    time. The key is persisted on the remote ``observation_images`` row
    (reserved before any bytes are sent), and later syncs always reuse the
    row's existing ``storage_path``, so a random suffix never causes a second
    upload on retry. Existing keys are never rewritten.
    """
    return uuid.uuid4().hex


def _build_worker_storage_path(
    user_id: str,
    obs_cloud_id: str,
    image_row: dict | None,
    upload_path: str,
) -> str:
    path = Path(str(upload_path or '').strip())
    suffix = path.suffix.lower() or _detected_image_extension(path)
    extension = suffix if suffix.startswith('.') else f'.{suffix or "jpg"}'
    sort_order = _safe_int((image_row or {}).get('sort_order'))
    if sort_order < 0:
        sort_order = 0
    unique_suffix = _new_image_storage_suffix()
    return _normalize_cloud_media_key(
        f'{str(user_id or "").strip()}/{str(obs_cloud_id or "").strip()}/{sort_order}_{unique_suffix}{extension}'
    )


def _file_content_signature(path: str | Path) -> str:
    file_path = Path(path)
    if not file_path.exists() or not file_path.is_file():
        return ''
    digest = hashlib.sha1()
    with open(file_path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _detected_image_extension(path: str | Path) -> str:
    try:
        with Image.open(path) as img:
            fmt = str(img.format or '').strip().upper()
    except Exception:
        return Path(path).suffix.lower() or '.jpg'
    if fmt == 'WEBP':
        return '.webp'
    if fmt == 'AVIF':
        return '.avif'
    if fmt in {'JPEG', 'JPG'}:
        return '.jpg'
    if fmt == 'PNG':
        return '.png'
    if fmt == 'TIFF':
        return '.tif'
    return Path(path).suffix.lower() or '.jpg'


def _rename_to_detected_image_extension(path: Path) -> Path:
    detected_ext = _detected_image_extension(path)
    if not detected_ext or path.suffix.lower() == detected_ext:
        return path
    target = path.with_suffix(detected_ext)
    counter = 1
    while target.exists() and target != path:
        target = path.with_name(f"{path.stem}_{counter}{detected_ext}")
        counter += 1
    path.rename(target)
    return target
