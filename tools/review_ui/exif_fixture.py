"""Synthetic JPEGs with EXIF time, camera and GPS (review scenarios and tests).

Bilder › Files and SD card shows a field photo's time and GPS on the image and
its EXIF in an Info drawer (stage 9f2); the plain fixture JPEGs carry no EXIF.
Written with Pillow only (no extra dependency).
"""
from __future__ import annotations

from pathlib import Path

from PIL import Image
from PIL.TiffImagePlugin import IFDRational

from .live_lab_harness import synthetic_frame

_EXIF_IFD = 0x8769
_GPS_IFD = 0x8825


def _dms(value: float) -> tuple[IFDRational, IFDRational, IFDRational]:
    value = abs(float(value))
    degrees = int(value)
    minutes_full = (value - degrees) * 60.0
    minutes = int(minutes_full)
    seconds = round((minutes_full - minutes) * 60.0, 3)
    return IFDRational(degrees, 1), IFDRational(minutes, 1), IFDRational(int(seconds * 1000), 1000)


def jpeg_with_exif(
    path: Path,
    *,
    seed: int = 7,
    taken: str = "2026:11:07 09:21:00",
    latitude: float | None = 63.42,
    longitude: float | None = 10.3107,
    model: str = "TG-6",
) -> Path:
    """Write ``path`` (once) with DateTimeOriginal, camera model, ISO and GPS."""
    path = Path(path)
    if path.exists():
        return path
    exif = Image.Exif()
    exif[0x010F] = "OM System"  # Make
    exif[0x0110] = model  # Model
    details = exif.get_ifd(_EXIF_IFD)
    details[0x9003] = taken  # DateTimeOriginal
    details[0x8827] = 200  # ISOSpeedRatings
    details[0x829A] = IFDRational(1, 125)  # ExposureTime
    details[0x829D] = IFDRational(28, 10)  # FNumber
    if latitude is not None and longitude is not None:
        gps = exif.get_ifd(_GPS_IFD)
        gps[1] = "N" if latitude >= 0 else "S"
        gps[2] = _dms(latitude)
        gps[3] = "E" if longitude >= 0 else "W"
        gps[4] = _dms(longitude)
    Image.fromarray(synthetic_frame(seed=seed)).save(path, "JPEG", quality=90, exif=exif.tobytes())
    return path
