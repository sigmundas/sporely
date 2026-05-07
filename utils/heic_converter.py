"""HEIC/HEIF conversion helper."""
import os
from pathlib import Path

WEBP_QUALITY = 65
WEBP_METHOD = 4


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


def convert_heic_to_webp(filepath, output_dir):
    """Convert a HEIC/HEIF image to WebP in output_dir.

    Returns the converted WebP path as a string, or None on failure.
    """
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
        output_path = _unique_output_path(output_dir, base_name, ".webp")
        save_image_as_webp(image, output_path, exif_bytes=exif_bytes)
        try:
            source_stat = Path(filepath).stat()
            os.utime(output_path, (source_stat.st_atime, source_stat.st_mtime))
        except Exception:
            pass
        return str(output_path)
    except Exception:
        return None


def convert_heic_to_jpeg(filepath, output_dir):
    """Backward-compatible wrapper. HEIC/HEIF is now converted to WebP."""
    return convert_heic_to_webp(filepath, output_dir)


def maybe_convert_heic(filepath, output_dir):
    """Convert HEIC/HEIF files to WebP, otherwise return original path."""
    suffix = Path(filepath).suffix.lower()
    if suffix in (".heic", ".heif"):
        return convert_heic_to_webp(filepath, output_dir)
    return filepath
