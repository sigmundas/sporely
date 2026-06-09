"""HEIC/HEIF conversion helper."""
import os
import mimetypes
from pathlib import Path

from utils.raw_detection import SUPPORTED_RAW_SUFFIXES, raw_mime_type_for_path

WEBP_QUALITY = 65
WEBP_METHOD = 4
JPEG_QUALITY = 90

_KNOWN_IMAGE_MIME_BY_SUFFIX = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".tif": "image/tiff",
    ".tiff": "image/tiff",
    ".webp": "image/webp",
    ".heic": "image/heic",
    ".heif": "image/heif",
}


def _unique_output_path(output_dir: Path, base_name: str, suffix: str) -> Path:
    output_path = output_dir / f"{base_name}{suffix}"
    counter = 1
    while output_path.exists():
        output_path = output_dir / f"{base_name}_{counter}{suffix}"
        counter += 1
    return output_path


def save_image_as_webp(image, output_path: str | Path, *, exif_bytes: bytes | None = None) -> None:
    """Save a Pillow image as WebP using the app's local-image quality setting."""
    save_kwargs = {
        "quality": WEBP_QUALITY,
        "method": WEBP_METHOD,
    }
    if exif_bytes:
        save_kwargs["exif"] = exif_bytes
    image.save(output_path, "WEBP", **save_kwargs)


def save_image_as_jpeg(image, output_path: str | Path, *, exif_bytes: bytes | None = None) -> None:
    """Save a Pillow image as JPEG using the app's local working-copy quality."""
    save_kwargs = {
        "quality": JPEG_QUALITY,
    }
    if exif_bytes:
        save_kwargs["exif"] = exif_bytes
    image.save(output_path, "JPEG", **save_kwargs)


def guess_local_image_mime_type(filepath) -> str | None:
    """Best-effort MIME detection for local image files."""
    if not filepath:
        return None
    path = Path(filepath)
    suffix = path.suffix.lower()
    raw_mime = raw_mime_type_for_path(path)
    if raw_mime != "application/octet-stream":
        return raw_mime
    mime, _encoding = mimetypes.guess_type(str(path))
    if mime:
        return mime
    return _KNOWN_IMAGE_MIME_BY_SUFFIX.get(suffix)


def build_local_image_provenance(
    source_path,
    working_path,
    *,
    image_type: str | None = None,
) -> dict[str, str | None]:
    """Return provenance fields for a local import or HEIC conversion."""
    source_text = str(source_path or "").strip()
    working_text = str(working_path or source_text or "").strip()
    source = Path(source_text) if source_text else None
    working = Path(working_text) if working_text else None
    source_suffix = source.suffix.lower() if source else ""
    working_suffix = working.suffix.lower() if working else ""
    source_mime = guess_local_image_mime_type(source) if source else None
    working_mime = guess_local_image_mime_type(working) if working else None

    source_role = "local_canonical"
    converted_suffixes = {".heic", ".heif"} | set(SUPPORTED_RAW_SUFFIXES)
    if source and source_suffix in converted_suffixes and working_text and working_text != source_text:
        source_role = "converted_local"

    normalized_type = str(image_type or "").strip().lower()
    file_purpose = (
        normalized_type
        if normalized_type in {
            "field",
            "microscope",
            "calibration",
            "reference",
            "plot",
            "thumbnail",
            "spore_crop",
            "cache",
        }
        else None
    )

    return {
        "source_role": source_role,
        "file_purpose": file_purpose,
        "original_mime_type": source_mime,
        "working_mime_type": working_mime,
    }


def convert_heic_to_jpeg(filepath, output_dir):
    """Convert a HEIC/HEIF image to JPEG in output_dir.

    Returns the converted JPEG path as a string, or None on failure.
    """
    output_path = None
    try:
        import pillow_heif
        from PIL import Image, ImageOps
    except ImportError:
        return None

    try:
        pillow_heif.register_heif_opener()
        try:
            heif_file = pillow_heif.open_heif(filepath)
            image = heif_file.to_pillow()
        except Exception:
            image = Image.open(filepath)

        # Apply EXIF orientation to pixels so portrait photos remain portrait
        # on services that ignore EXIF orientation during display.
        image = ImageOps.exif_transpose(image).convert("RGB")

        exif_bytes = None
        try:
            exif = image.getexif()
            if exif:
                # Orientation tag: reset after transposing.
                exif[274] = 1
                exif_bytes = exif.tobytes()
        except Exception:
            exif_bytes = None

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        base_name = Path(filepath).stem
        output_path = _unique_output_path(output_dir, base_name, ".jpg")
        save_image_as_jpeg(image, output_path, exif_bytes=exif_bytes)
        try:
            source_stat = Path(filepath).stat()
            os.utime(output_path, (source_stat.st_atime, source_stat.st_mtime))
        except Exception:
            pass
        return str(output_path)
    except Exception:
        if output_path is not None:
            try:
                output_path.unlink(missing_ok=True)
            except Exception:
                pass
        return None


def maybe_convert_heic(filepath, output_dir):
    """Convert HEIC/HEIF files to JPEG, otherwise return original path."""
    suffix = Path(filepath).suffix.lower()
    if suffix in (".heic", ".heif"):
        return convert_heic_to_jpeg(filepath, output_dir)
    return filepath
