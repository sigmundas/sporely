"""New photos on a camera's SD card (redesign stage 9i, owner 2026-11-12).

Bilder › Files and SD card's “From SD card (new photos)”:

- A card is a mounted volume with a ``DCIM`` folder at its root.
- A photo is new when it is not in Sporely yet: its file name and capture
  time match no saved image of any observation (its stored copy or its
  original), the rule Camera import uses for one observation. An image saved
  without a capture time matches the file name alone.
- The photos are read with Camera import's scanner (RAW and camera JPEG
  pairs are one photo, the RAW companion preference applies).
"""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Iterable

from config import RAW_COMPANION_SOURCE_PREFERENCE_PREFER_RAW, SETTING_RAW_COMPANION_SOURCE_PREFERENCE
from database.schema import get_connection

SETTING_CARD_FOLDER = "files_sd_card_folder"  # the folder chosen when no card was found


def _volume_roots() -> list[Path]:
    from PySide6.QtCore import QStorageInfo

    roots = []
    for volume in QStorageInfo.mountedVolumes():
        if not volume.isValid() or not volume.isReady():
            continue
        root = Path(volume.rootPath())
        if str(root) in {"", "/"}:
            continue  # the system disk
        roots.append(root)
    return roots


def find_camera_cards(volume_roots: Iterable[Path] | None = None) -> list[Path]:
    """Mounted volumes with a ``DCIM`` folder at their root, sorted by name."""
    roots = _volume_roots() if volume_roots is None else [Path(root) for root in volume_roots]
    cards = []
    for root in roots:
        try:
            if (root / "DCIM").is_dir():
                cards.append(root)
        except OSError:
            continue  # an unreadable volume is no card
    return sorted(set(cards), key=lambda root: str(root).casefold())


def _parse_datetime(text: str) -> datetime | None:
    try:
        return datetime.fromisoformat(text)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def capture_key(value) -> str | None:
    """A capture time as compared for “already imported” (to the second; text kept if unparsed)."""
    if isinstance(value, datetime):
        return value.replace(microsecond=0).isoformat(sep=" ")
    text = str(value or "").strip()
    if not text:
        return None
    parsed = _parse_datetime(text)
    if parsed is None:
        return text
    return parsed.replace(microsecond=0).isoformat(sep=" ")


def imported_signatures() -> set[tuple[str, str | None]]:
    """(file name, capture key) of every saved image, for its stored copy and its original."""
    signatures: set[tuple[str, str | None]] = set()
    conn = get_connection()
    try:
        rows = conn.execute("SELECT filepath, original_filepath, captured_at FROM images").fetchall()
    finally:
        conn.close()
    for filepath, original_filepath, captured_at in rows:
        key = capture_key(captured_at)
        for path in (filepath, original_filepath):
            name = Path(str(path or "")).name.casefold()
            if name:
                signatures.add((name, key))
    return signatures


def is_imported(filename: str, captured_at, signatures: set[tuple[str, str | None]]) -> bool:
    name = Path(str(filename or "")).name.casefold()
    return (name, capture_key(captured_at)) in signatures or (name, None) in signatures


def scan_folder(folder: Path) -> Path:
    """The folder to read: a card's ``DCIM``, or the folder itself."""
    folder = Path(folder)
    dcim = folder / "DCIM"
    return dcim if dcim.is_dir() else folder


def new_photo_rows(folder: Path, *, source_preference: str | None = None) -> list[dict]:
    """The photos under ``folder`` (its DCIM) that are not in Sporely yet, oldest first."""
    from database.models import SettingsDB
    from utils.image_companion_grouping import normalize_raw_companion_source_preference
    from utils.temporal_matcher import TemporalMatcher

    if source_preference is None:
        source_preference = normalize_raw_companion_source_preference(
            SettingsDB.get_setting(SETTING_RAW_COMPANION_SOURCE_PREFERENCE, RAW_COMPANION_SOURCE_PREFERENCE_PREFER_RAW)
        )
    root = scan_folder(folder)
    files = sorted(str(path) for path in root.rglob("*") if path.is_file() and not path.name.startswith("."))
    rows = TemporalMatcher(session_kind="offline").prepare_image_rows(files, source_preference=source_preference)
    signatures = imported_signatures()
    return [row for row in rows if not row_is_imported(row, signatures)]


def row_is_imported(row: dict, signatures: set[tuple[str, str | None]]) -> bool:
    """Any file of the photo (a RAW or its camera JPEG) saved with its capture time already."""
    names = [row.get("filename"), *paths_for_row(row)]
    return any(is_imported(name, row.get("captured_at"), signatures) for name in names if name)


def observation_day(value) -> date | None:
    """The local day of an observation's stored date, or None.

    Read as the scanner reads photo times (local and naive): a date stored
    with an offset (a cloud observation's UTC time) is converted first.
    """
    from utils.temporal_matcher import normalize_timestamp

    parsed = normalize_timestamp(value) if value else None
    return parsed.date() if parsed is not None else None


def photo_day(row: dict) -> date | None:
    captured = row.get("captured_at")
    return captured.date() if isinstance(captured, datetime) else None


def newest_day(rows: Iterable[dict]) -> date | None:
    days = [day for day in (photo_day(row) for row in rows) if day is not None]
    return max(days) if days else None


def rows_on_day(rows: Iterable[dict], day: date | None) -> list[dict]:
    return [row for row in rows if day is not None and photo_day(row) == day]


def paths_for_row(row: dict) -> list[str]:
    """Every file of one photo (a RAW and its camera JPEG), for the editor to pair again."""
    paths: list[str] = []
    for value in (row.get("source_path"), row.get("selected_path"), row.get("filepath"), row.get("raw_path"),
                  row.get("camera_jpeg_path"), *(row.get("companion_paths") or [])):
        text = str(value or "").strip()
        if text and text not in paths:
            paths.append(text)
    return paths
