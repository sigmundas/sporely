"""Application configuration settings."""
from pathlib import Path

from app_identity import APP_NAME, app_data_dir

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
SUPPORTED_FORMATS = "Images (*.png *.jpg *.jpeg *.tif *.tiff *.NEF *.ORF)"
RAW_FORMATS = ('.nef', '.orf', '.cr2', '.arw')
