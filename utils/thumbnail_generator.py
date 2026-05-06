"""Thumbnail generator for efficient loading and ML training."""
from pathlib import Path
from PIL import Image, ImageOps, features
import sqlite3
from database.schema import get_connection, DATABASE_PATH

# Thumbnail output directory
THUMBNAIL_DIR = DATABASE_PATH.parent / "thumbnails"

# Size presets for thumbnails. Keep one UI thumbnail only; ML training uses
# spore crops exported on-demand via "Export for ML".
SIZE_PRESETS = {
    '224x224': (224, 224),
}

SIZE_PRESET_ALIASES = {
    'small': ('small', '224x224', 'thumb'),
    'thumb': ('thumb', '224x224', 'small'),
    '224x224': ('224x224', 'thumb', 'small'),
}

# These thumbnails are read directly by Qt/PySide in the desktop UI. Pillow can
# write AVIF here, but the packaged Qt image plugins do not reliably read it.
THUMBNAIL_FORMAT = 'WEBP' if features.check('webp') else 'JPEG'
THUMBNAIL_EXTENSION = '.webp' if THUMBNAIL_FORMAT == 'WEBP' else '.jpg'
THUMBNAIL_SAVE_OPTIONS = (
    {'quality': 58, 'method': 4}
    if THUMBNAIL_FORMAT == 'WEBP'
    else {'quality': 78}
)


def ensure_thumbnail_dir():
    """Ensure the thumbnail directory exists."""
    THUMBNAIL_DIR.mkdir(parents=True, exist_ok=True)


def generate_thumbnail(image_path: str, size: tuple, output_path: Path) -> bool:
    """Generate a single thumbnail at the specified size.

    Args:
        image_path: Path to the source image
        size: Tuple of (width, height) for the thumbnail
        output_path: Path where thumbnail should be saved

    Returns:
        True if successful, False otherwise
    """
    try:
        suffix = Path(image_path).suffix.lower()
        if suffix in ('.heic', '.heif'):
            try:
                import pillow_heif
                pillow_heif.register_heif_opener()
            except ImportError as exc:
                raise RuntimeError("HEIC import requires pillow-heif") from exc

        with Image.open(image_path) as img:
            img = ImageOps.exif_transpose(img)
            # Convert to RGB if necessary (handles RGBA, grayscale, etc.)
            if img.mode in ('RGBA', 'LA'):
                # Create white background for transparent images
                background = Image.new('RGB', img.size, (255, 255, 255))
                if img.mode == 'RGBA':
                    background.paste(img, mask=img.split()[3])
                else:
                    background.paste(img, mask=img.split()[1])
                img = background
            elif img.mode != 'RGB':
                img = img.convert('RGB')

            # Calculate aspect-ratio-preserving resize
            target_w, target_h = size
            orig_w, orig_h = img.size

            # Calculate scale to fit the longer dimension
            scale = max(target_w / orig_w, target_h / orig_h)
            new_w = int(orig_w * scale)
            new_h = int(orig_h * scale)

            # Resize with high-quality resampling
            img_resized = img.resize((new_w, new_h), Image.Resampling.LANCZOS)

            # Center crop to exact target size
            left = (new_w - target_w) // 2
            top = (new_h - target_h) // 2
            right = left + target_w
            bottom = top + target_h

            img_cropped = img_resized.crop((left, top, right, bottom))

            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            img_cropped.save(output_path, THUMBNAIL_FORMAT, **THUMBNAIL_SAVE_OPTIONS)
            return True

    except Exception as e:
        print(f"Error generating thumbnail for {image_path}: {e}")
        return False


def generate_all_sizes(image_path: str, image_id: int) -> dict:
    """Generate thumbnails at all preset sizes for an image.

    Args:
        image_path: Path to the source image
        image_id: Database ID of the image

    Returns:
        Dictionary mapping size_preset names to thumbnail filepaths
    """
    ensure_thumbnail_dir()

    results = {}
    source_path = Path(image_path)

    if not source_path.exists():
        print(f"Source image not found: {image_path}")
        return results

    conn = get_connection()
    cursor = conn.cursor()

    for preset_name, size in SIZE_PRESETS.items():
        # Generate unique filename using image_id and preset
        thumbnail_filename = f"img_{image_id}_{preset_name}{THUMBNAIL_EXTENSION}"
        thumbnail_path = THUMBNAIL_DIR / thumbnail_filename

        # Generate the thumbnail
        if generate_thumbnail(image_path, size, thumbnail_path):
            # Save to database
            try:
                cursor.execute('''
                    INSERT OR REPLACE INTO thumbnails (image_id, size_preset, filepath)
                    VALUES (?, ?, ?)
                ''', (image_id, preset_name, str(thumbnail_path)))
                results[preset_name] = str(thumbnail_path)
            except sqlite3.Error as e:
                print(f"Database error saving thumbnail record: {e}")

    conn.commit()
    conn.close()

    return results


def get_thumbnail_path(image_id: int, size_preset: str) -> str | None:
    """Get the filepath for a specific thumbnail.

    Args:
        image_id: Database ID of the image
        size_preset: Size preset name (e.g., '224x224')

    Returns:
        Filepath string if exists, None otherwise
    """
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    preset_names = SIZE_PRESET_ALIASES.get(str(size_preset or '').strip(), (size_preset,))
    row = None
    for preset_name in preset_names:
        cursor.execute('''
            SELECT filepath FROM thumbnails
            WHERE image_id = ? AND size_preset = ?
        ''', (image_id, preset_name))
        row = cursor.fetchone()
        if row:
            break
    conn.close()

    if row:
        return row['filepath']
    return None


def get_all_thumbnails(image_id: int) -> dict:
    """Get all thumbnail paths for an image.

    Args:
        image_id: Database ID of the image

    Returns:
        Dictionary mapping size_preset names to filepaths
    """
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute('''
        SELECT size_preset, filepath FROM thumbnails
        WHERE image_id = ?
    ''', (image_id,))

    rows = cursor.fetchall()
    conn.close()

    return {row['size_preset']: row['filepath'] for row in rows}


def delete_thumbnails(image_id: int):
    """Delete all thumbnails for an image.

    Args:
        image_id: Database ID of the image
    """
    # Get thumbnail paths first
    thumbnails = get_all_thumbnails(image_id)

    # Delete files
    for filepath in thumbnails.values():
        try:
            Path(filepath).unlink(missing_ok=True)
        except Exception as e:
            print(f"Error deleting thumbnail file {filepath}: {e}")

    # Delete database records
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM thumbnails WHERE image_id = ?', (image_id,))
    conn.commit()
    conn.close()


def regenerate_thumbnails_for_image(image_id: int, image_path: str) -> dict:
    """Regenerate all thumbnails for an image (useful after updates).

    Args:
        image_id: Database ID of the image
        image_path: Path to the source image

    Returns:
        Dictionary mapping size_preset names to thumbnail filepaths
    """
    delete_thumbnails(image_id)
    return generate_all_sizes(image_path, image_id)
