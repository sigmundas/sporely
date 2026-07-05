import os
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QEvent, Qt
from PySide6.QtGui import QKeyEvent
from PySide6.QtWidgets import QApplication

import ui.observations_tab as observations_tab_module
from ui.observations_tab import ObservationsTab


def _qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


class _FakeTable:
    def __init__(self) -> None:
        self._stylesheet = ""
        self.selected_rows: list[int] = []

    def styleSheet(self) -> str:
        return self._stylesheet

    def setStyleSheet(self, stylesheet: str) -> None:
        self._stylesheet = str(stylesheet)

    def selectRow(self, row: int) -> None:
        self.selected_rows.append(int(row))


def _build_move_state():
    state = SimpleNamespace()
    state.tr = lambda text: text
    state.selected_observation_id = 5
    state.table = _FakeTable()
    state.hint_messages: list[tuple[str | None, str]] = []
    state.status_messages: list[tuple[str, str]] = []
    state.refresh_calls: list[bool] = []
    state.find_row_calls: list[int] = []
    state._pending_gallery_move_image_ids = []
    state._pending_gallery_move_source_observation_id = None
    state._pending_gallery_move_previous_table_stylesheet = ""
    state._set_hint = lambda text, tone="info": state.hint_messages.append((text, tone))
    state.refresh_observations = lambda restore_selection=False: state.refresh_calls.append(bool(restore_selection))
    state._find_table_row_for_observation = lambda observation_id: state.find_row_calls.append(int(observation_id)) or 2
    state.set_status_message = lambda message, level="info", auto_clear_ms=0: state.status_messages.append((str(message), str(level)))
    state._clear_pending_gallery_move = (
        lambda restore_hint=True: (
            setattr(state, "_pending_gallery_move_image_ids", []),
            setattr(state, "_pending_gallery_move_source_observation_id", None),
            setattr(state, "_pending_gallery_move_previous_table_stylesheet", ""),
            state.table.setStyleSheet(""),
            state._set_hint("Ready.") if restore_hint else None,
        )
    )
    return state


def test_pending_gallery_move_can_be_cancelled_with_escape(qapp):
    state = _build_move_state()

    ObservationsTab._begin_move_selected_gallery_images(state, [11, 12])
    assert state._pending_gallery_move_image_ids == [11, 12]
    assert "border: 2px solid #e74c3c" in state.table.styleSheet()

    event = QKeyEvent(QEvent.KeyPress, Qt.Key_Escape, Qt.NoModifier)
    assert ObservationsTab.eventFilter(state, object(), event) is True

    assert state._pending_gallery_move_image_ids == []
    assert state._pending_gallery_move_source_observation_id is None
    assert state.table.styleSheet() == ""
    assert state.hint_messages[-1] == ("Ready.", "info")


def test_pending_gallery_move_updates_image_rows_for_target_observation(monkeypatch, qapp):
    state = _build_move_state()
    state._pending_gallery_move_image_ids = [101, 102]
    state._pending_gallery_move_source_observation_id = 5

    class _FakeCursor:
        def __init__(self) -> None:
            self.executed: list[tuple[str, tuple[object, ...]]] = []
            self._fetchone = (4,)

        def execute(self, sql, params=()):
            self.executed.append((str(sql), tuple(params)))
            if "COALESCE(MAX(sort_order), -1) + 1" in str(sql):
                self._fetchone = (4,)
            return self

        def fetchone(self):
            return self._fetchone

    class _FakeConn:
        def __init__(self) -> None:
            self.cursor_obj = _FakeCursor()
            self.row_factory = None
            self.committed = False
            self.closed = False

        def cursor(self):
            return self.cursor_obj

        def commit(self):
            self.committed = True

        def close(self):
            self.closed = True

    fake_conn = _FakeConn()
    monkeypatch.setattr(observations_tab_module, "get_connection", lambda: fake_conn)

    ObservationsTab._complete_pending_gallery_move(state, 7)

    assert fake_conn.committed is True
    assert fake_conn.closed is True
    assert fake_conn.cursor_obj.executed[0][0].startswith("SELECT COALESCE(MAX(sort_order), -1) + 1")
    assert fake_conn.cursor_obj.executed[1] == (
        "UPDATE images SET observation_id = ?, sort_order = ? WHERE id = ?",
        (7, 4, 101),
    )
    assert fake_conn.cursor_obj.executed[2] == (
        "UPDATE images SET observation_id = ?, sort_order = ? WHERE id = ?",
        (7, 5, 102),
    )
    assert state.refresh_calls == [False]
    assert state.table.selected_rows == [2]
    assert state._pending_gallery_move_image_ids == []
    assert state._pending_gallery_move_source_observation_id is None
    assert state.status_messages[-1][0] == "Moved 2 images to observation 7."


def test_delete_selected_images_uses_light_refresh(monkeypatch, qapp):
    state = SimpleNamespace()
    state.tr = lambda text: text
    state._question_yes_no = lambda *args, **kwargs: True
    state.set_status_message = lambda *args, **kwargs: None
    state.selected_observation_id = 5
    state.refresh_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    state.light_refresh_calls: list[bool] = []
    state.refresh_observations = lambda *args, **kwargs: state.refresh_calls.append((args, kwargs))
    state._refresh_current_observation_after_image_delete = lambda: state.light_refresh_calls.append(True)
    state._get_measurements_for_image = lambda image_id: []

    deleted_ids: list[int] = []
    monkeypatch.setattr(
        observations_tab_module.ImageDB,
        "get_image",
        lambda image_id: {"filepath": f"/tmp/image-{int(image_id)}.jpg"},
    )
    monkeypatch.setattr(
        observations_tab_module.ImageDB,
        "delete_image",
        lambda image_id: deleted_ids.append(int(image_id)),
    )
    monkeypatch.setattr(
        observations_tab_module.MeasurementDB,
        "get_measurements_for_image",
        lambda image_id: [],
    )

    ObservationsTab._confirm_delete_selected_images(state, [11, 12])

    assert deleted_ids == [11, 12]
    assert state.light_refresh_calls == [True]
    assert state.refresh_calls == []


def test_delete_single_image_uses_light_refresh(monkeypatch, qapp):
    state = SimpleNamespace()
    state.tr = lambda text: text
    state._question_yes_no = lambda *args, **kwargs: True
    state.set_status_message = lambda *args, **kwargs: None
    state.selected_observation_id = 5
    state.refresh_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    state.light_refresh_calls: list[bool] = []
    state.refresh_observations = lambda *args, **kwargs: state.refresh_calls.append((args, kwargs))
    state._refresh_current_observation_after_image_delete = lambda: state.light_refresh_calls.append(True)
    state._get_measurements_for_image = lambda image_id: []

    deleted_ids: list[int] = []
    monkeypatch.setattr(
        observations_tab_module.ImageDB,
        "delete_image",
        lambda image_id: deleted_ids.append(int(image_id)),
    )
    monkeypatch.setattr(
        observations_tab_module.MeasurementDB,
        "get_measurements_for_image",
        lambda image_id: [],
    )

    ObservationsTab._confirm_delete_image(state, 11)

    assert deleted_ids == [11]
    assert state.light_refresh_calls == [True]
    assert state.refresh_calls == []
