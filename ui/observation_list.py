"""Observation list widget for browsing measurements."""
from PySide6.QtWidgets import QWidget, QVBoxLayout, QTableWidget, QTableWidgetItem, QPushButton
from PySide6.QtCore import Signal
from database import MeasurementRepository


class ObservationList(QWidget):
    """Widget for viewing and browsing spore measurements."""

    observation_selected = Signal(int)  # Emits observation ID when selected

    def __init__(self):
        super().__init__()
        self.repository = MeasurementRepository()
        self.init_ui()

    def init_ui(self):
        """Initialize the user interface."""
        layout = QVBoxLayout(self)

        # Refresh button
        refresh_btn = QPushButton(self.tr("Refresh List"))
        refresh_btn.clicked.connect(self.load_observations)
        layout.addWidget(refresh_btn)

        # Table for displaying observations
        self.table = QTableWidget()
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels([
            self.tr("ID"),
            self.tr("Image Path"),
            self.tr("Length (μm)"),
            self.tr("Scale"),
            self.tr("Timestamp"),
        ])
        self.table.itemSelectionChanged.connect(self._on_selection_changed)
        layout.addWidget(self.table)

        # Load initial data
        self.load_observations()

    def load_observations(self):
        """Load observations from the database."""
        measurements = self.repository.get_measurements()
        self.table.setRowCount(len(measurements))

        for row, measurement in enumerate(measurements):
            measurement_id, image_path, length_um, width_um, scale, timestamp = measurement

            self.table.setItem(row, 0, QTableWidgetItem(str(measurement_id)))
            self.table.setItem(row, 1, QTableWidgetItem(image_path or ""))
            self.table.setItem(row, 2, QTableWidgetItem(f"{length_um:.2f}"))
            self.table.setItem(row, 3, QTableWidgetItem(f"{scale:.2f}"))
            self.table.setItem(row, 4, QTableWidgetItem(timestamp or ""))

        self.table.resizeColumnsToContents()

    def _on_selection_changed(self):
        """Handle selection changes in the table."""
        selected_items = self.table.selectedItems()
        if selected_items:
            row = selected_items[0].row()
            measurement_id = int(self.table.item(row, 0).text())
            self.observation_selected.emit(measurement_id)
