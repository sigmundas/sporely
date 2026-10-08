"""Local image asset path resolution (filesystem reads).

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S4 of the cloud-sync extraction.
"""

from __future__ import annotations

from pathlib import Path

from database.schema import get_images_dir


def _is_readable_local_file(path: Path) -> bool:
    try:
        if not path.exists() or not path.is_file():
            return False
        with path.open('rb') as handle:
            handle.read(1)
        return True
    except Exception:
        return False


def _resolve_existing_local_image_asset_path(path_value: str | None) -> Path | None:
    text = str(path_value or '').strip()
    if not text:
        return None
    try:
        raw_path = Path(text).expanduser()
    except Exception:
        return None

    candidates: list[Path] = []
    if raw_path.is_absolute():
        candidates.append(raw_path)
    else:
        images_dir = get_images_dir()
        if raw_path.parts and raw_path.parts[0] == images_dir.name:
            candidates.append(images_dir.parent / raw_path)
        candidates.append(images_dir / raw_path)
        candidates.append(raw_path)

    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        if _is_readable_local_file(candidate):
            return candidate
    return None
