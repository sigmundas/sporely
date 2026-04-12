"""QR generation and decoding helpers for Sync Shot clock calibration."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from PIL import Image, ImageOps


SYNC_SHOT_QR_INTERVAL_MS = 2000
SYNC_SHOT_QR_BLANK_MS = 100
SYNC_SHOT_QR_VISIBLE_MS = SYNC_SHOT_QR_INTERVAL_MS - SYNC_SHOT_QR_BLANK_MS

_SYNC_SHOT_PREFIX = "SPORELY_SYNC_SHOT"
_SYNC_SHOT_VERSION = "v1"


def current_sync_shot_utc() -> datetime:
    """Return the current UTC time rounded to whole seconds."""
    return datetime.now(timezone.utc).replace(microsecond=0)


def format_sync_shot_utc(utc_dt: datetime) -> str:
    """Format a UTC datetime for QR payloads and UI labels."""
    return utc_dt.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_sync_shot_payload(utc_dt: datetime, session_id: str) -> str:
    """Encode the Sync Shot session id and UTC timestamp into one QR payload."""
    utc_text = format_sync_shot_utc(utc_dt)
    return f"{_SYNC_SHOT_PREFIX}|{_SYNC_SHOT_VERSION}|{utc_text}|{str(session_id or '').strip()}"


def parse_sync_shot_payload(payload: str | None) -> dict | None:
    """Parse and validate a Sync Shot QR payload."""
    text = str(payload or "").strip()
    if not text:
        return None
    parts = text.split("|")
    if len(parts) != 4:
        return None
    prefix, version, utc_text, session_id = parts
    if prefix != _SYNC_SHOT_PREFIX or version != _SYNC_SHOT_VERSION:
        return None
    session_text = str(session_id or "").strip()
    if not session_text:
        return None
    try:
        utc_dt = datetime.strptime(utc_text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return {
        "payload": text,
        "prefix": prefix,
        "version": version,
        "session_id": session_text,
        "utc_dt": utc_dt,
        "utc_text": format_sync_shot_utc(utc_dt),
    }


def render_sync_shot_qr(payload: str, *, box_size: int = 12, border: int = 3) -> Image.Image:
    """Render a QR code image for the given payload."""
    import qrcode
    from qrcode.constants import ERROR_CORRECT_M

    qr = qrcode.QRCode(
        version=None,
        error_correction=ERROR_CORRECT_M,
        box_size=max(2, int(box_size)),
        border=max(1, int(border)),
    )
    qr.add_data(str(payload or "").strip())
    qr.make(fit=True)
    image = qr.make_image(fill_color="black", back_color="white")
    return image.convert("RGB")


def _load_decode_image(filepath: str | Path) -> np.ndarray:
    path = Path(filepath)
    suffix = path.suffix.lower()
    if suffix in {".heic", ".heif"}:
        try:
            import pillow_heif

            pillow_heif.register_heif_opener()
        except ImportError:
            pass
    with Image.open(path) as image:
        normalized = ImageOps.exif_transpose(image).convert("RGB")
        return np.array(normalized)


def _collect_candidate_arrays(rgb_image: np.ndarray) -> list[np.ndarray]:
    import cv2

    bgr_image = cv2.cvtColor(rgb_image, cv2.COLOR_RGB2BGR)
    gray_image = cv2.cvtColor(bgr_image, cv2.COLOR_BGR2GRAY)
    scaled_gray = cv2.resize(gray_image, None, fx=2.0, fy=2.0, interpolation=cv2.INTER_CUBIC)
    _, threshold_gray = cv2.threshold(gray_image, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    _, threshold_scaled = cv2.threshold(scaled_gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    return [bgr_image, gray_image, scaled_gray, threshold_gray, threshold_scaled]


def _decode_payloads_from_candidate(candidate: np.ndarray) -> list[str]:
    import cv2

    detector = cv2.QRCodeDetector()
    payloads: list[str] = []

    try:
        multi_ok, decoded_info, _points, _straight = detector.detectAndDecodeMulti(candidate)
        if multi_ok and decoded_info:
            for value in decoded_info:
                text = str(value or "").strip()
                if text:
                    payloads.append(text)
    except Exception:
        pass

    if not payloads:
        try:
            single_value, _points, _straight = detector.detectAndDecode(candidate)
            text = str(single_value or "").strip()
            if text:
                payloads.append(text)
        except Exception:
            pass
    return payloads


def decode_sync_shot_qr(filepath: str | Path) -> dict:
    """Decode Sync Shot QR payloads from an image file."""
    image = _load_decode_image(filepath)
    seen_payloads: set[str] = set()
    matches: list[dict] = []
    for candidate in _collect_candidate_arrays(image):
        for payload in _decode_payloads_from_candidate(candidate):
            if payload in seen_payloads:
                continue
            seen_payloads.add(payload)
            parsed = parse_sync_shot_payload(payload)
            if parsed:
                matches.append(parsed)
    unique_matches: list[dict] = []
    seen_keys: set[tuple[str, str]] = set()
    for parsed in matches:
        key = (str(parsed.get("session_id") or ""), str(parsed.get("utc_text") or ""))
        if key in seen_keys:
            continue
        seen_keys.add(key)
        unique_matches.append(parsed)
    return {
        "matches": unique_matches,
        "multiple": len(unique_matches) > 1,
    }


def choose_sync_shot_offset(captured_at: datetime, qr_utc_dt: datetime) -> dict:
    """Choose the most plausible EXIF basis for a QR-calibrated capture time."""
    local_dt = qr_utc_dt.astimezone().replace(tzinfo=None)
    utc_dt = qr_utc_dt.astimezone(timezone.utc).replace(tzinfo=None)
    local_offset = (local_dt - captured_at).total_seconds()
    utc_offset = (utc_dt - captured_at).total_seconds()
    if abs(local_offset) <= abs(utc_offset):
        return {
            "basis": "local",
            "display_dt": local_dt,
            "offset_seconds": local_offset,
        }
    return {
        "basis": "utc",
        "display_dt": utc_dt,
        "offset_seconds": utc_offset,
    }
