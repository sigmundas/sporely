"""Application configuration settings."""
from pathlib import Path

from app_identity import APP_NAME, app_data_dir
from utils.raw_detection import SUPPORTED_RAW_SUFFIXES

# Database settings
DB_NAME = "mushrooms.db"
_app_dir = app_data_dir()
DB_PATH = _app_dir / DB_NAME

# UI settings
WINDOW_TITLE = APP_NAME
WINDOW_WIDTH = 1200
WINDOW_HEIGHT = 800
IMAGE_DISPLAY_WIDTH = 1100
IMAGE_DISPLAY_HEIGHT = 700

# Measurement defaults
DEFAULT_SCALE = 0.5  # microns per pixel

# Supported image formats
def _format_image_filter(suffixes: tuple[str, ...]) -> str:
    return " ".join(f"*{suffix}" for suffix in suffixes)


_RASTER_IMAGE_SUFFIXES = (".png", ".jpg", ".jpeg", ".tif", ".tiff")
_LOCAL_IMPORT_IMAGE_SUFFIXES = _RASTER_IMAGE_SUFFIXES + (".webp", ".heic", ".heif")
_RAW_FORMAT_FILTER = _format_image_filter(tuple(sorted(SUPPORTED_RAW_SUFFIXES)))

RASTER_IMAGE_FILTER = f"Images ({_format_image_filter(_RASTER_IMAGE_SUFFIXES)})"
# Only use this for paths that route through prepare_local_ingest_image().
LOCAL_IMPORT_IMAGE_FILTER = (
    f"Images ({_format_image_filter(_LOCAL_IMPORT_IMAGE_SUFFIXES)} {_RAW_FORMAT_FILTER})"
)
# RAW companion preference is a user-level setting shared by Live Lab and folder import.
SETTING_RAW_COMPANION_SOURCE_PREFERENCE = "raw_companion_source_preference"
RAW_COMPANION_SOURCE_PREFERENCE_PREFER_RAW = "prefer_raw"
RAW_COMPANION_SOURCE_PREFERENCE_CAMERA_JPEG = "camera_jpeg"
# Advanced RAW processing preferences used by Live Lab and the curve inspector.
SETTING_RAW_PROCESSING_DARK_CUTOFF = "raw_processing_dark_cutoff"
SETTING_RAW_PROCESSING_BRIGHT_CUTOFF = "raw_processing_bright_cutoff"
SETTING_RAW_PROCESSING_SHADOW_LIFT_ENABLED = "raw_processing_shadow_lift_enabled"
SETTING_RAW_PROCESSING_SHADOW_LIFT_MAX = "raw_processing_shadow_lift_max"
# Legacy alias for direct-open callers.
SUPPORTED_FORMATS = RASTER_IMAGE_FILTER
RAW_FORMATS = tuple(sorted(SUPPORTED_RAW_SUFFIXES))
