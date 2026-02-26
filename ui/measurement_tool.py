"""Measurement tool widget for spore analysis."""
from PySide6.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QLineEdit, QFileDialog
from PySide6.QtGui import QPixmap
from PySide6.QtCore import Qt, QPoint, Signal
import numpy as np

from config import DEFAULT_SCALE, IMAGE_DISPLAY_WIDTH, IMAGE_DISPLAY_HEIGHT, SUPPORTED_FORMATS, RAW_FORMATS
from database import MeasurementRepository
from utils.stats import calculate_statistics


class MeasurementTool(QWidget):
    """Widget for loading images and measuring spores."""

    measurement_added = Signal(float)  # Emits when a new measurement is made

    def __init__(self):
        super().__init__()
        self.image_path = None
        self.scale = DEFAULT_SCALE
        self.points = []
        self.measurements = []
        self.measuring = False
        self.current_pixmap = None

        self.repository = MeasurementRepository()
        self.init_ui()

    def init_ui(self):
        """Initialize the user interface."""
        layout = QVBoxLayout(self)

        # Controls
        controls = QHBoxLayout()

        load_btn = QPushButton(self.tr("Load Image"))
        load_btn.clicked.connect(self.load_image)
        controls.addWidget(load_btn)

        controls.addWidget(QLabel("Scale (μm/pixel):"))
        self.scale_input = QLineEdit(str(DEFAULT_SCALE))
        self.scale_input.setMaximumWidth(100)
        controls.addWidget(self.scale_input)

        measure_btn = QPushButton(self.tr("Measure (click 2 points)"))
        measure_btn.clicked.connect(self.start_measurement)
        controls.addWidget(measure_btn)

        self.result_label = QLabel("No measurements")
        controls.addWidget(self.result_label)

        layout.addLayout(controls)

        # Image display
        self.image_label = QLabel()
        self.image_label.setAlignment(Qt.AlignCenter)
        self.image_label.mousePressEvent = self.image_clicked
        layout.addWidget(self.image_label)

    def load_image(self):
        """Load an image file for measurement."""
        path, _ = QFileDialog.getOpenFileName(
            self, "Open Microscope Image", "", SUPPORTED_FORMATS
        )
        if path:
            self.image_path = path

            # Handle RAW files
            if path.lower().endswith(RAW_FORMATS):
                self.result_label.setText("RAW file - convert to TIFF first")
                return

            # Load and display
            pixmap = QPixmap(path)
            scaled = pixmap.scaled(
                IMAGE_DISPLAY_WIDTH,
                IMAGE_DISPLAY_HEIGHT,
                Qt.KeepAspectRatio
            )
            self.image_label.setPixmap(scaled)
            self.current_pixmap = pixmap

            # Reset measurements for new image
            self.measurements = []
            self.result_label.setText("Image loaded - ready to measure")

    def start_measurement(self):
        """Start a new measurement."""
        if not self.image_path:
            self.result_label.setText("Please load an image first")
            return

        self.points = []
        self.measuring = True
        self.result_label.setText("Click 2 points to measure...")

    def image_clicked(self, event):
        """Handle mouse clicks on the image."""
        if not self.measuring:
            return

        # Get click position relative to original image size
        label_size = self.image_label.size()
        pixmap_size = self.image_label.pixmap().size()

        # Calculate scale factor
        scale_x = self.current_pixmap.width() / pixmap_size.width()
        scale_y = self.current_pixmap.height() / pixmap_size.height()

        # Adjust click position
        pos = event.position()
        x = int((pos.x() - (label_size.width() - pixmap_size.width()) / 2) * scale_x)
        y = int((pos.y() - (label_size.height() - pixmap_size.height()) / 2) * scale_y)

        self.points.append(QPoint(x, y))

        if len(self.points) == 2:
            self._complete_measurement()

    def _complete_measurement(self):
        """Complete the measurement and save to database."""
        # Calculate distance
        dx = self.points[1].x() - self.points[0].x()
        dy = self.points[1].y() - self.points[0].y()
        distance_pixels = np.sqrt(dx**2 + dy**2)

        # Convert to microns
        scale = float(self.scale_input.text().replace(",", "."))
        distance_microns = distance_pixels * scale

        # Save to database
        self.repository.add_measurement(
            self.image_path,
            distance_microns,
            scale
        )

        self.measurements.append(distance_microns)
        self.measurement_added.emit(distance_microns)

        # Show statistics
        if len(self.measurements) > 1:
            stats = calculate_statistics(self.measurements)
            self.result_label.setText(
                f"Last: {distance_microns:.2f} μm | "
                f"Mean: {stats['mean']:.2f} ± {stats['std']:.2f} μm "
                f"(n={stats['count']})"
            )
        else:
            self.result_label.setText(f"Measured: {distance_microns:.2f} μm")

        self.measuring = False
        self.points = []
