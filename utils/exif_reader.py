"""EXIF metadata reader for extracting date/time and GPS from images."""
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple, Dict, Any
from PIL import Image
from PIL import ExifTags

"""EXIF metadata reader for extracting date/time and GPS from images."""
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple, Dict, Any
from PIL import Image
from PIL import ExifTags


def get_exif_data(image_path: str) -> Dict[str, Any]:
    """
    Extract EXIF data from an image file.

    Args:
        image_path: Path to the image file

    Returns:
        Dictionary with decoded EXIF tags
    """
    try:
        if not image_path or not Path(image_path).exists():
            return {}
        suffix = Path(image_path).suffix.lower()

        # Handle HEIC/HEIF files with pillow_heif
        if suffix in ('.heic', '.heif'):
            try:
                import pillow_heif
                heif_file = pillow_heif.open_heif(image_path)

                # Get EXIF from pillow_heif metadata
                exif_data = heif_file.info.get('exif')
                if exif_data:
                    # Create a temporary image to parse EXIF
                    img = heif_file.to_pillow()
                    exif = img.getexif()
                    if exif:
                        decoded = {}
                        for tag_id, value in exif.items():
                            tag = ExifTags.TAGS.get(tag_id, tag_id)
                            decoded[tag] = value

                        # Handle EXIF IFD separately (camera settings)
                        try:
                            exif_ifd = exif.get_ifd(0x8769)  # EXIF IFD tag
                            if exif_ifd:
                                for tag_id, value in exif_ifd.items():
                                    tag = ExifTags.TAGS.get(tag_id, tag_id)
                                    decoded[tag] = value
                        except (KeyError, AttributeError):
                            pass

                        # Handle GPS IFD separately
                        try:
                            gps_ifd = exif.get_ifd(0x8825)  # GPSInfo tag
                            if gps_ifd:
                                decoded['GPSInfo'] = dict(gps_ifd)
                        except (KeyError, AttributeError):
                            pass

                        return decoded
                return {}
            except ImportError:
                print("pillow-heif not installed, cannot read HEIC files")
                return {}
            except Exception as e:
                print(f"Error reading HEIC EXIF from {image_path}: {e}")
                return {}

        # Standard image formats
        with Image.open(image_path) as img:
            exif = img.getexif()
            if not exif:
                # Try legacy method for older PIL versions
                exif_data = getattr(img, '_getexif', lambda: None)()
                if not exif_data:
                    return {}
                decoded = {}
                for tag_id, value in exif_data.items():
                    tag = ExifTags.TAGS.get(tag_id, tag_id)
                    decoded[tag] = value
                # Check for GPS info in legacy exif data
                gps_tag_id = 34853  # GPSInfo tag ID
                if gps_tag_id in exif_data:
                    decoded['GPSInfo'] = exif_data[gps_tag_id]
                return decoded

            decoded = {}
            for tag_id, value in exif.items():
                tag = ExifTags.TAGS.get(tag_id, tag_id)
                decoded[tag] = value

            # Handle EXIF IFD separately (camera settings like ISO, shutter, f-stop)
            try:
                exif_ifd = exif.get_ifd(0x8769)  # EXIF IFD tag
                if exif_ifd:
                    for tag_id, value in exif_ifd.items():
                        tag = ExifTags.TAGS.get(tag_id, tag_id)
                        decoded[tag] = value
            except (KeyError, AttributeError):
                pass

            # Handle GPS IFD separately
            try:
                gps_ifd = exif.get_ifd(0x8825)  # GPSInfo tag
                if gps_ifd:
                    decoded['GPSInfo'] = dict(gps_ifd)
            except (KeyError, AttributeError):
                pass

            return decoded
    except Exception as e:
        print(f"Error reading EXIF from {image_path}: {e}")
        return {}


def get_camera_settings(image_path: str) -> Dict[str, Any]:
    """
    Extract camera settings (ISO, shutter speed, f-stop) from an image.

    Args:
        image_path: Path to the image file

    Returns:
        Dictionary with 'iso', 'shutter_speed', 'f_number', 'focal_length'
    """
    exif = get_exif_data(image_path)
    
    # ISO - try multiple tag names
    iso = (exif.get('ISOSpeedRatings') or 
           exif.get('PhotographicSensitivity') or 
           exif.get('ISO'))
    
    # Shutter speed - prefer ExposureTime over ShutterSpeedValue
    shutter = exif.get('ExposureTime')
    if shutter is None:
        # ShutterSpeedValue is in APEX format, need to convert
        shutter_apex = exif.get('ShutterSpeedValue')
        if shutter_apex is not None:
            try:
                shutter = 1 / (2 ** float(shutter_apex))
            except (ValueError, TypeError, ZeroDivisionError):
                shutter = None
    
    # F-number (aperture)
    f_number = exif.get('FNumber')
    if f_number is None:
        # ApertureValue is in APEX format, need to convert
        aperture_apex = exif.get('ApertureValue')
        if aperture_apex is not None:
            try:
                f_number = 2 ** (float(aperture_apex) / 2)
            except (ValueError, TypeError):
                f_number = None
    
    # Focal length
    focal_length = exif.get('FocalLength')
    
    return {
        'iso': iso,
        'shutter_speed': shutter,
        'f_number': f_number,
        'focal_length': focal_length
    }


def _clean_exif_text(value: Any) -> str:
    """Normalize EXIF text fields to a trimmed Unicode string."""
    if value is None:
        return ""
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8", errors="ignore")
        except Exception:
            value = str(value)
    return str(value).strip()


def get_camera_model(exif_or_path: Dict[str, Any] | str | Path | None) -> Optional[str]:
    """
    Return the camera model text shown by the app.

    Prefer the EXIF Model field to match Finder's "Device model" display,
    and fall back to Make only if Model is unavailable.
    """
    if isinstance(exif_or_path, dict):
        exif = exif_or_path
    else:
        exif = get_exif_data(str(exif_or_path)) if exif_or_path else {}
    model = _clean_exif_text(exif.get("Model"))
    if model:
        return model
    make = _clean_exif_text(exif.get("Make"))
    return make or None



def get_image_datetime(image_path: str) -> Optional[datetime]:
    """
    Extract the date/time when the photo was taken.

    Args:
        image_path: Path to the image file

    Returns:
        datetime object or None if not available
    """
    exif = get_exif_data(image_path)

    # Try different date fields in order of preference
    date_fields = ['DateTimeOriginal', 'DateTimeDigitized', 'DateTime']

    for field in date_fields:
        if field in exif:
            try:
                date_str = exif[field]
                # EXIF date format is typically "YYYY:MM:DD HH:MM:SS"
                return datetime.strptime(date_str, "%Y:%m:%d %H:%M:%S")
            except (ValueError, TypeError):
                continue

    return None


def _convert_to_degrees(value) -> float:
    """Convert EXIF GPS coordinates to degrees."""
    try:
        d = float(value[0])
        m = float(value[1])
        s = float(value[2])
        return d + (m / 60.0) + (s / 3600.0)
    except (TypeError, IndexError, ZeroDivisionError):
        return 0.0


def get_gps_coordinates(image_path: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Extract GPS coordinates from an image.

    Args:
        image_path: Path to the image file

    Returns:
        Tuple of (latitude, longitude) or (None, None) if not available
    """
    exif = get_exif_data(image_path)

    if 'GPSInfo' not in exif:
        return None, None

    gps_info = exif['GPSInfo']

    # Decode GPS tags
    gps_data = {}
    for tag_id, value in gps_info.items():
        tag = ExifTags.GPSTAGS.get(tag_id, tag_id)
        gps_data[tag] = value

    try:
        lat = _convert_to_degrees(gps_data.get('GPSLatitude', []))
        lat_ref = gps_data.get('GPSLatitudeRef', 'N')
        if lat_ref == 'S':
            lat = -lat

        lon = _convert_to_degrees(gps_data.get('GPSLongitude', []))
        lon_ref = gps_data.get('GPSLongitudeRef', 'E')
        if lon_ref == 'W':
            lon = -lon

        if lat == 0.0 and lon == 0.0:
            return None, None

        return lat, lon
    except Exception:
        return None, None


def get_image_metadata(image_path: str) -> Dict[str, Any]:
    """
    Get all relevant metadata from an image.

    Args:
        image_path: Path to the image file

    Returns:
        Dictionary with 'datetime', 'latitude', 'longitude', 'filename'
    """
    path = Path(image_path) if image_path else None
    if not path or not path.exists():
        return {
            'missing': True,
            'datetime': None,
            'latitude': None,
            'longitude': None,
            'filename': path.name if path else "",
            'filepath': image_path,
        }

    dt = get_image_datetime(image_path)
    lat, lon = get_gps_coordinates(image_path)

    return {
        'missing': False,
        'datetime': dt,
        'latitude': lat,
        'longitude': lon,
        'filename': path.name,
        'filepath': image_path
    }
