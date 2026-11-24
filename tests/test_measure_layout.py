"""The Measure tab's layout (redesign plan stage 6), on a real ``MainWindow``.

Skala is a collapsed section with a summary line, the measuring tools are a
toolbar above the image, Visning and Info are pages of a right drawer, and
the left column is gone. The window runs against an isolated profile under
``tmp_path`` (``tools/review_ui/live_lab_harness.py``): no network, no live data.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QPoint, QPointF, Qt
from PySide6.QtWidgets import QWidget

from database.models import ImageDB, MeasurementDB, SettingsDB
from tools.review_ui.live_lab_harness import FakeRawpy, create_observation, isolation_patches, prepare_profile


@pytest.fixture
def profile(tmp_path, monkeypatch, qtbot):
    rawpy = FakeRawpy()
    for owner, attribute, value in isolation_patches(tmp_path, rawpy):
        monkeypatch.setattr(owner, attribute, value)
    yield prepare_profile(tmp_path, rawpy)


def _window(qtbot, profile, *, note=None):
    from ui.main_window import MainWindow

    observation_id = create_observation(profile)
    image_id = ImageDB.add_image(
        observation_id, str(profile.jpeg("M0001.JPG", seed=3)), "microscope",
        scale=0.1, objective_name="100X", contrast="DIC", notes=note,
    )
    MeasurementDB.add_measurement(
        image_id, 2.8, 1.8, measurement_type="Spores",
        points=[QPointF(100, 90), QPointF(128, 90), QPointF(114, 81), QPointF(114, 99)],
    )
    window = MainWindow()
    qtbot.addWidget(window)
    window.resize(1500, 950)
    window.show()
    qtbot.waitExposed(window)
    window.activateWindow()  # QShortcuts (M, Esc) act only in the active window
    qtbot.waitActive(window)
    window.on_observation_selected(observation_id, "Inocybe geophylla", switch_tab=False)
    window.tab_widget.setCurrentWidget(window.measure_tab)
    qtbot.waitUntil(lambda: window.current_image_id == image_id, timeout=5000)
    qtbot.wait(400)  # the layout settles a pixel after the image loads
    return window


def _activate(qtbot, window):
    """Offscreen windows lose activation while loading; QShortcuts need it."""
    window.activateWindow()
    qtbot.waitActive(window)


def _inside(widget, ancestor) -> bool:
    return ancestor is widget or ancestor.isAncestorOf(widget)


def test_no_left_column_tools_above_the_image(qtbot, profile):
    window = _window(qtbot, profile)
    splitter = window.measure_main_splitter
    assert splitter.count() == 2  # image area and the Measurements panel
    image_top = window.image_label.mapTo(window, window.image_label.rect().topLeft()).y()
    for above in (window.measure_scale_section, window.measure_toolbar):
        assert above.isVisible()
        assert above.mapTo(window, above.rect().bottomLeft()).y() < image_top
    for tool in (window.measure_category_combo, window.mode_lines, window.mode_rect, window.mode_multiline,
                 window.measure_button, window.measure_status_label):
        assert _inside(tool, window.measure_toolbar) and tool.isVisible()
    # The image gets the width the left column had.
    assert window.image_label.width() > 1500 - 340 - 200


def test_every_view_option_is_in_the_view_drawer_and_the_buttons_toggle_it(qtbot, profile):
    window = _window(qtbot, profile)
    drawer = window.measure_drawer
    assert not drawer.isVisible()
    qtbot.mouseClick(window.measure_view_drawer_btn, Qt.LeftButton)
    qtbot.waitUntil(lambda: drawer.isVisible() and drawer.geometry().right() <= window.measure_image_area.width(), timeout=2000)
    assert drawer.current_page() == "view" and window.measure_view_drawer_btn.isChecked()
    for option in (window.show_measures_checkbox, window.show_rectangles_checkbox, window.show_scale_bar_checkbox,
                   window.show_copyright_checkbox, window.measure_color_menu_button, window.rectangle_style_selector,
                   window.rectangle_thickness_selector, window.scale_bar_input):
        assert _inside(option, window.measure_view_page)
    for option in (window.show_measures_checkbox, window.show_copyright_checkbox, window.measure_color_menu_button):
        assert option.isVisible()
    # Still working from the drawer.
    before = window.show_copyright_checkbox.isChecked()
    box = window.show_copyright_checkbox
    qtbot.mouseClick(box, Qt.LeftButton, pos=QPoint(8, box.height() // 2))  # on the box, not past its text
    assert window.show_copyright_checkbox.isChecked() is not before

    qtbot.mouseClick(window.measure_info_drawer_btn, Qt.LeftButton)  # one drawer, another page
    assert drawer.current_page() == "info"
    assert window.measure_info_drawer_btn.isChecked() and not window.measure_view_drawer_btn.isChecked()
    assert window.exif_info_label.isVisible() and window.measure_image_note_input.isVisible()
    qtbot.mouseClick(window.measure_info_drawer_btn, Qt.LeftButton)  # the same button closes it
    qtbot.waitUntil(lambda: not drawer.isVisible(), timeout=2000)
    assert not window.measure_info_drawer_btn.isChecked()


def test_esc_closes_the_drawer_first_and_the_close_button_closes_it(qtbot, profile):
    window = _window(qtbot, profile)
    drawer = window.measure_drawer
    drawer.open_page("view")
    _activate(qtbot, window)
    window.image_label.setFocus()
    measuring = window.measurement_active
    qtbot.keyClick(window.image_label, Qt.Key_Escape)
    qtbot.waitUntil(lambda: not drawer.isVisible(), timeout=2000)
    assert window.measurement_active == measuring  # Esc only closed the drawer
    drawer.open_page("info")
    qtbot.mouseClick(drawer.close_button, Qt.LeftButton)
    qtbot.waitUntil(lambda: not drawer.isVisible(), timeout=2000)


def test_the_drawer_overlays_the_image_without_resizing_it(qtbot, profile):
    window = _window(qtbot, profile)
    size = window.image_label.size()
    window.measure_drawer.open_page("view")
    qtbot.wait(300)
    assert window.image_label.size() == size
    window.resize(1200, 800)
    qtbot.waitUntil(lambda: window.measure_drawer.geometry().right() == window.measure_image_area.width() - 1, timeout=2000)
    assert window.measure_drawer.height() == window.measure_image_area.height()


def test_skala_summary_names_objective_and_calibration_and_warns(qtbot, profile):
    window = _window(qtbot, profile)
    section = window.measure_scale_section
    assert not section.is_expanded()  # set once per session: closed by default
    summary = section.summary()
    assert window.scale_combo.currentText() in summary
    assert "Calibration" in summary
    window.scale_warning_label.setText("Warning: Image resolution mismatch!")
    window.scale_warning_label.setVisible(True)
    assert section.summary_alert() == "Warning: Image resolution mismatch!"
    window.scale_warning_label.setVisible(False)
    assert section.summary_alert() == ""


def test_skala_open_state_and_splitter_survive_a_restart(qtbot, profile):
    window = _window(qtbot, profile)
    qtbot.mouseClick(window.measure_scale_section.header, Qt.LeftButton)
    qtbot.waitUntil(window.measure_scale_section.is_expanded, timeout=1000)
    window.close()
    assert SettingsDB.get_setting("measure_main_splitter_sizes", None) is None  # the old key is left alone
    restarted = _window(qtbot, profile)
    assert restarted.measure_scale_section.is_expanded()
    assert restarted.scale_combo.isVisible()


def test_the_info_button_marks_an_image_with_a_note(qtbot, profile):
    window = _window(qtbot, profile, note="Clamp connections")
    assert window.measure_info_drawer_btn.text().endswith("•")
    window.measure_image_note_input.setPlainText("")
    assert window.measure_info_drawer_btn.text() == "Info"


def test_a_long_status_is_elided_on_one_line_with_the_full_text_as_tooltip(qtbot, profile):
    window = _window(qtbot, profile)
    label = window.measure_status_label
    text = "Warning: a very long status message " * 8
    label.setText(text)
    assert label.text() == text and label.toolTip() == text
    assert label.height() < 2 * label.fontMetrics().height()


def test_a_narrowed_image_pane_keeps_the_toolbar_from_overlapping(qtbot, profile):
    window = _window(qtbot, profile)
    splitter = window.measure_main_splitter
    splitter.setSizes([200, 2000])  # as far left as the splitter lets it go
    qtbot.wait(100)
    tools = [window.measure_category_combo, window.measure_mode_selector, window.measure_button,
             window.measure_view_drawer_btn, window.measure_info_drawer_btn]
    rects = [tool.geometry() for tool in tools]
    for first, second in zip(rects, rects[1:]):
        assert first.right() < second.left(), (first, second)
    assert window.measure_toolbar.width() >= window.measure_toolbar.minimumSizeHint().width()


def test_the_drawer_widens_to_fit_its_page_with_a_large_font(qtbot, profile):
    window = _window(qtbot, profile)
    page = window.measure_view_page
    font = page.font()
    font.setPointSize(font.pointSize() + 6)
    page.setFont(font)
    for child in page.findChildren(QWidget):
        child.setFont(font)
    window.measure_drawer.open_page("view")
    qtbot.wait(300)
    thick = window.rectangle_thickness_thick_radio
    right_edge = thick.mapTo(window.measure_drawer, thick.rect().topRight()).x()
    assert right_edge < window.measure_drawer.width()  # the last View control is inside the drawer
