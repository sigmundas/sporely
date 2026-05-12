﻿"""Compact statistics table widget."""
from PySide6.QtWidgets import QWidget, QVBoxLayout, QTableWidget, QTableWidgetItem, QHeaderView


class StatsTableWidget(QWidget):
    """Compact widget with statistics table."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.current_stats = None

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(2)

        # Stats table
        self.table = QTableWidget()
        self.table.setFocusPolicy(Qt.NoFocus)
        self.table.setColumnCount(2)
        self.table.setRowCount(3)
        self.table.setHorizontalHeaderLabels(["Mean +/- SD (um)", "Range (um)"])
        self.table.setVerticalHeaderLabels(["Length", "Width", "Q"])

        # Make it compact
        self.table.setMaximumHeight(110)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setSelectionMode(QTableWidget.NoSelection)

        # Column sizing
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.Stretch)
        header.setSectionResizeMode(1, QHeaderView.Stretch)

        layout.addWidget(self.table)

        # Initialize with empty values
        self.clear_stats()

    def clear_stats(self):
        """Clear all statistics."""
        self.current_stats = None
        for row in range(3):
            self.table.setItem(row, 0, QTableWidgetItem("-"))
            self.table.setItem(row, 1, QTableWidgetItem("-"))

    def update_stats(self, stats):
        """Update the statistics display."""
        self.current_stats = stats
        if not stats:
            self.clear_stats()
            return

        # Length row
        self.table.setItem(0, 0, QTableWidgetItem(
            f"{stats['length_mean']:.2f} +/- {stats['length_std']:.2f}"
        ))
        self.table.setItem(0, 1, QTableWidgetItem(
            f"{stats['length_min']:.2f} - {stats['length_max']:.2f}"
        ))

        # Width row
        if 'width_mean' in stats and stats.get('width_mean', 0) > 0:
            self.table.setItem(1, 0, QTableWidgetItem(
                f"{stats['width_mean']:.2f} +/- {stats['width_std']:.2f}"
            ))
            self.table.setItem(1, 1, QTableWidgetItem(
                f"{stats['width_min']:.2f} - {stats['width_max']:.2f}"
            ))

            # Q row - no units
            self.table.setItem(2, 0, QTableWidgetItem(f"{stats['ratio_mean']:.1f}"))
            self.table.setItem(2, 1, QTableWidgetItem(f"{stats['ratio_min']:.1f} - {stats['ratio_max']:.1f}"))
        else:
            self.table.setItem(1, 0, QTableWidgetItem("-"))
            self.table.setItem(1, 1, QTableWidgetItem("-"))

            self.table.setItem(2, 0, QTableWidgetItem("-"))
            self.table.setItem(2, 1, QTableWidgetItem("-"))
