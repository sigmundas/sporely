import os
import sqlite3
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtCore import QEvent, Qt
from PySide6.QtGui import QKeyEvent, QImage, QColor
from PySide6.QtWidgets import QApplication

import ui.observations_tab as observations_tab_module
import database.models as models_module
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
        self.hover_updates = 0
        self._viewport = SimpleNamespace(update=self._on_update)

    def styleSheet(self) -> str:
        return self._stylesheet

    def setStyleSheet(self, stylesheet: str) -> None:
        self._stylesheet = str(stylesheet)

    def selectRow(self, row: int) -> None:
        self.selected_rows.append(int(row))

    def viewport(self):
        return self._viewport

    def rowAt(self, y: int) -> int:
        if y < 0:
            return -1
        return int(y // 20)

    def _on_update(self) -> None:
        self.hover_updates += 1


class _FakeSplitter:
    def __init__(self, width: int = 1200, sizes: list[int] | None = None) -> None:
        self._width = int(width)
        self._sizes = list(sizes or [280, 920])
        self.set_sizes_calls: list[list[int]] = []

    def width(self) -> int:
        return self._width

    def sizes(self) -> list[int]:
        return list(self._sizes)

    def setSizes(self, sizes) -> None:  # noqa: N802 - Qt-style name
        values = [int(value) for value in sizes]
        self._sizes = list(values)
        self.set_sizes_calls.append(values)


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

    move_calls = []
    monkeypatch.setattr(
        observations_tab_module.ImageDB,
        "move_images_to_observation",
        lambda image_ids, source_id, target_id: move_calls.append(
            (list(image_ids), source_id, target_id)
        ) or len(image_ids),
    )

    ObservationsTab._complete_pending_gallery_move(state, 7)

    assert move_calls == [([101, 102], 5, 7)]
    assert state.refresh_calls == [False]
    assert state.table.selected_rows == [2]
    assert state._pending_gallery_move_image_ids == []
    assert state._pending_gallery_move_source_observation_id is None
    assert state.status_messages[-1][0] == "Moved 2 images to observation 7."


def test_pending_gallery_move_tracks_hovered_table_row(qapp):
    state = _build_move_state()
    state._pending_gallery_move_image_ids = [101]
    state._pending_gallery_move_source_observation_id = 5
    state._pending_gallery_move_hover_row = -1

    class _MouseEvent:
        def __init__(self, event_type, y):
            self._event_type = event_type
            self._y = y

        def type(self):
            return self._event_type

        def position(self):
            return SimpleNamespace(toPoint=lambda: SimpleNamespace(y=lambda: self._y))

    viewport = state.table.viewport()
    hover_event = _MouseEvent(QEvent.MouseMove, 45)
    assert ObservationsTab.eventFilter(state, viewport, hover_event) is False
    assert state._pending_gallery_move_hover_row == 2
    assert state.table.hover_updates == 1

    leave_event = _MouseEvent(QEvent.Leave, -1)
    assert ObservationsTab.eventFilter(state, viewport, leave_event) is False
    assert state._pending_gallery_move_hover_row == -1
    assert state.table.hover_updates == 2


def test_move_cloud_image_preserves_measurements_and_resets_remote_links(monkeypatch, tmp_path):
    db_path = tmp_path / "move.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY, cloud_id TEXT, sync_status TEXT, updated_at TEXT
        );
        CREATE TABLE images (
            id INTEGER PRIMARY KEY, observation_id INTEGER, cloud_id TEXT,
            synced_at TEXT, sort_order INTEGER, image_type TEXT,
            filepath TEXT, original_filepath TEXT
        );
        CREATE TABLE spore_measurements (
            id INTEGER PRIMARY KEY, image_id INTEGER, cloud_id TEXT
        );
        CREATE TABLE image_tombstones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            deleted_cloud_id TEXT NOT NULL UNIQUE,
            deleted_at TEXT NOT NULL,
            delete_synced_at TEXT,
            deleted_storage_path TEXT,
            deleted_observation_cloud_id TEXT,
            local_observation_id INTEGER,
            local_image_id INTEGER,
            image_type TEXT,
            filepath TEXT,
            original_filepath TEXT
        );
        INSERT INTO observations VALUES (5, 'cloud-obs-5', 'synced', NULL);
        INSERT INTO observations VALUES (7, 'cloud-obs-7', 'synced', NULL);
        INSERT INTO images VALUES (
            101, 5, 'cloud-image-101', '2026-07-01', 2,
            'microscope', '/tmp/micro.jpg', NULL
        );
        INSERT INTO images VALUES (201, 7, NULL, NULL, 3, 'field', '/tmp/field.jpg', NULL);
        INSERT INTO spore_measurements VALUES (301, 101, 'cloud-measurement-301');
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(
        models_module,
        "get_connection",
        lambda: sqlite3.connect(db_path),
    )

    moved = models_module.ImageDB.move_images_to_observation([101], 5, 7)
    assert moved == 1

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    image = dict(conn.execute("SELECT * FROM images WHERE id = 101").fetchone())
    measurement = dict(conn.execute("SELECT * FROM spore_measurements WHERE id = 301").fetchone())
    tombstone = dict(conn.execute("SELECT * FROM image_tombstones").fetchone())
    statuses = dict(conn.execute("SELECT id, sync_status FROM observations").fetchall())
    conn.close()

    assert image["observation_id"] == 7
    assert image["sort_order"] == 4
    assert image["cloud_id"] is None
    assert image["synced_at"] is None
    assert measurement["image_id"] == 101
    assert measurement["cloud_id"] is None
    assert tombstone["deleted_cloud_id"] == "cloud-image-101"
    assert tombstone["deleted_observation_cloud_id"] == "cloud-obs-5"
    assert tombstone["local_observation_id"] == 5
    assert tombstone["local_image_id"] is None
    assert statuses == {5: "dirty", 7: "dirty"}


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


def test_gallery_double_click_switches_to_image_mode_and_shows_path(qapp):
    shown_paths: list[str] = []
    state = SimpleNamespace()
    state.VIEW_MODE_IMAGES = "images"
    state._apply_view_mode = lambda mode, persist=True: shown_paths.append(f"mode:{mode}")
    state._refresh_image_browser_for_current_selection = lambda: shown_paths.append("refresh")
    state.image_browser = SimpleNamespace(show_image_for_path=lambda path: shown_paths.append(path) or True)

    ObservationsTab._on_gallery_image_double_clicked(state, 11, "/tmp/example.jpg")

    assert shown_paths == ["mode:images", "/tmp/example.jpg"]


def test_observation_image_browser_force_first_item_ignores_previous_selection(tmp_path, qapp):
    image1 = tmp_path / "image-1.png"
    image2 = tmp_path / "image-2.png"
    pixmap = QImage(24, 24, QImage.Format_ARGB32)
    pixmap.fill(QColor("#ffffff"))
    assert pixmap.save(str(image1))
    assert pixmap.save(str(image2))

    browser = observations_tab_module._ObservationImageBrowser()
    browser.set_items(
        [
            {"id": 1, "path": str(image1)},
            {"id": 2, "path": str(image2)},
        ]
    )
    assert browser.current_image_path() == str(image1)

    browser.show_image_for_path(str(image2))
    assert browser.current_image_path() == str(image2)

    browser.set_items(
        [
            {"id": 1, "path": str(image1)},
            {"id": 2, "path": str(image2)},
        ],
        force_first=True,
    )

    assert browser.current_image_path() == str(image1)


def test_refresh_image_browser_for_current_selection_forces_first_image_on_observation_change(monkeypatch, tmp_path, qapp):
    image1 = tmp_path / "image-1.png"
    image2 = tmp_path / "image-2.png"
    pixmap = QImage(24, 24, QImage.Format_ARGB32)
    pixmap.fill(QColor("#ffffff"))
    assert pixmap.save(str(image1))
    assert pixmap.save(str(image2))

    calls: list[tuple[str, object]] = []
    state = SimpleNamespace()
    state.selected_observation_id = 9
    state._image_browser_observation_id = 3
    state._sync_image_browser_publish_state = lambda: calls.append(("publish", None))
    state.image_browser = SimpleNamespace(
        set_items=lambda items, force_first=False: calls.append(("set_items", force_first, [dict(item) for item in items])),
        clear=lambda: calls.append(("clear", None)),
    )

    monkeypatch.setattr(
        observations_tab_module.ImageDB,
        "get_images_for_observation",
        lambda observation_id: [
            {"id": 1, "filepath": str(image1)},
            {"id": 2, "filepath": str(image2)},
        ],
    )

    ObservationsTab._refresh_image_browser_for_current_selection(state)

    assert calls[0][0] == "set_items"
    assert calls[0][1] is True
    assert calls[0][2] == [
        {"id": 1, "path": str(image1)},
        {"id": 2, "path": str(image2)},
    ]
    assert calls[1] == ("publish", None)
    assert state._image_browser_observation_id == 9


def test_sync_gallery_selection_tracks_browser_image_and_skips_multiselect(qapp):
    selected_calls: list[tuple[str, bool]] = []
    state = SimpleNamespace()
    state.image_browser = SimpleNamespace(
        current_image_id=lambda: 7,
        current_image_path=lambda: "/tmp/example.jpg",
    )
    state.gallery_widget = SimpleNamespace(
        selected_image_keys=lambda: set(),
        select_image=lambda image_id, center=True: selected_calls.append((f"id:{image_id}", bool(center))),
        select_paths=lambda paths, center=True: selected_calls.append((f"path:{paths[0]}", bool(center))),
    )

    ObservationsTab._sync_gallery_selection_to_current_browser_image(state)

    assert selected_calls == [("id:7", True)]

    selected_calls.clear()
    state.gallery_widget.selected_image_keys = lambda: {1, 2}

    ObservationsTab._sync_gallery_selection_to_current_browser_image(state)

    assert selected_calls == []


def test_image_mode_uses_wider_table_splitter_default(qapp):
    state = SimpleNamespace()
    state.VIEW_MODE_TABLE = "table"
    state.VIEW_MODE_IMAGES = "images"
    selector_calls: list[tuple[str, object]] = []
    state.view_mode_selector = SimpleNamespace(
        set_selected_value=lambda value: selector_calls.append(("set", value))
    )
    state.table = SimpleNamespace(
        columnCount=lambda: 10,
        setColumnHidden=lambda *args, **kwargs: None,
        viewport=lambda: SimpleNamespace(update=lambda: None),
    )
    state._IMAGE_MODE_VISIBLE_COLUMNS = (0, 1)
    state.image_browser = SimpleNamespace(setVisible=lambda *_args, **_kwargs: None)
    state.view_splitter = _FakeSplitter(width=1200, sizes=[0, 0])
    state._view_splitter_table_width = 0
    state._shortcut_image_prev = None
    state._shortcut_image_next = None
    state._shortcut_image_row_up = None
    state._shortcut_image_row_down = None
    state._redistribute_taxonomy_columns = lambda: None
    state._refresh_image_browser_for_current_selection = lambda: None

    ObservationsTab._apply_view_mode(state, state.VIEW_MODE_IMAGES, persist=False)

    assert selector_calls == [("set", "images")]
    assert state.view_splitter.set_sizes_calls
    assert state.view_splitter.set_sizes_calls[-1][0] >= 320
    assert state.view_splitter.set_sizes_calls[-1][0] > 280


def test_thumbnail_row_navigation_shortcuts_stay_enabled_in_table_mode_and_move_selection(qapp):
    class _ShortcutSpy:
        def __init__(self) -> None:
            self.enabled = None

        def setEnabled(self, enabled) -> None:  # noqa: N802 - Qt-style name
            self.enabled = bool(enabled)

    moves: list[int] = []
    state = SimpleNamespace()
    state.VIEW_MODE_TABLE = "table"
    state.VIEW_MODE_IMAGES = "images"
    state.table = SimpleNamespace(
        columnCount=lambda: 10,
        setColumnHidden=lambda *args, **kwargs: None,
        viewport=lambda: SimpleNamespace(update=lambda: None),
    )
    state._IMAGE_MODE_VISIBLE_COLUMNS = (0, 1)
    state.image_browser = SimpleNamespace(setVisible=lambda *_args, **_kwargs: None)
    state.view_splitter = _FakeSplitter(width=1200, sizes=[0, 0])
    state._view_splitter_table_width = 0
    state._shortcut_image_prev = _ShortcutSpy()
    state._shortcut_image_next = _ShortcutSpy()
    state._shortcut_image_row_up = _ShortcutSpy()
    state._shortcut_image_row_down = _ShortcutSpy()
    state._redistribute_taxonomy_columns = lambda: None
    state._refresh_image_browser_for_current_selection = lambda: None
    state._shortcut_blocked_by_text_input = lambda: False
    state._move_table_selection = lambda delta: moves.append(int(delta))

    ObservationsTab._apply_view_mode(state, state.VIEW_MODE_TABLE, persist=False)
    assert state._shortcut_image_prev.enabled is False
    assert state._shortcut_image_next.enabled is False
    assert state._shortcut_image_row_up.enabled is True
    assert state._shortcut_image_row_down.enabled is True

    ObservationsTab._apply_view_mode(state, state.VIEW_MODE_IMAGES, persist=False)
    assert state._shortcut_image_prev.enabled is True
    assert state._shortcut_image_next.enabled is True
    assert state._shortcut_image_row_up.enabled is True
    assert state._shortcut_image_row_down.enabled is True

    ObservationsTab._on_image_row_up_shortcut(state)
    ObservationsTab._on_image_row_down_shortcut(state)

    assert moves == [-1, 1]


def test_table_thumbnail_double_click_switches_to_image_mode_and_shows_full_image(monkeypatch, qapp):
    calls: list[tuple[str, object]] = []

    class _FakeSelectionModel:
        def selectedRows(self):
            return [SimpleNamespace(row=lambda: 0)]

    class _FakeTable:
        def selectionModel(self):
            return _FakeSelectionModel()

    class _FakeItem:
        def column(self):
            return 0

    state = SimpleNamespace()
    state.table = _FakeTable()
    state._show_observation_table_thumbnails = lambda: True
    state._observation_row_data_from_item = lambda item: {"thumbnail_image_id": 321}
    state.VIEW_MODE_IMAGES = "images"
    state._apply_view_mode = lambda mode, persist=True: calls.append(("mode", mode, bool(persist)))
    state._refresh_image_browser_for_current_selection = lambda: calls.append(("refresh", None))
    state.image_browser = SimpleNamespace(
        show_image_for_path=lambda path: calls.append(("show", path)) or True,
    )
    state.edit_observation = lambda: calls.append(("edit", None))

    monkeypatch.setattr(
        observations_tab_module.ImageDB,
        "get_image",
        lambda image_id: {"filepath": "/tmp/full-size-image.jpg"} if int(image_id) == 321 else None,
    )

    ObservationsTab.on_row_double_clicked(state, _FakeItem())

    assert calls == [
        ("mode", "images", True),
        ("show", "/tmp/full-size-image.jpg"),
    ]
