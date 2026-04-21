"""Image processing utilities."""
from pathlib import Path
from PIL import Image
from PySide6.QtGui import QPixmap
from typing import Optional


def load_image(image_path: str) -> Optional[Image.Image]:
    """
    Load an image from disk.

    Args:
        image_path: Path to the image file

    Returns:
        PIL Image object or None if loading fails
    """
    try:
        return Image.open(image_path)
    except Exception as e:
        print(f"Error loading image {image_path}: {e}")
        return None


def scale_image(pixmap: QPixmap, max_width: int, max_height: int) -> QPixmap:
    """
    Scale a QPixmap to fit within specified dimensions while maintaining aspect ratio.

    Args:
        pixmap: The QPixmap to scale
        max_width: Maximum width
        max_height: Maximum height

    Returns:
        Scaled QPixmap
    """
    from PySide6.QtCore import Qt
    return pixmap.scaled(max_width, max_height, Qt.KeepAspectRatio)

def cleanup_import_temp_file(
    source_path: str,
    converted_path: str,
    stored_path: str,
    imports_dir: Path,
) -> None:
    """Remove temporary converted files once a stored copy exists."""
    try:
        source = Path(source_path).resolve()
        converted = Path(converted_path).resolve()
        stored = Path(stored_path).resolve()
        imports_dir = Path(imports_dir).resolve()
    except Exception:
        return

    if converted == source or converted == stored:
        return
    if not converted.exists():
        return
    try:
        if not converted.is_relative_to(imports_dir):
            return
    except Exception:
        return

    try:
        converted.unlink()
    except Exception as e:
        print(f"Warning: Could not remove temporary import file: {e}")
