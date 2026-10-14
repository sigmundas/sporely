"""Stage 4: images, measurements and anchors on the sync-key state machine (U tier).

Two SQLite databases and one shared fake cloud. The fake sits below the real
``SporelyCloudClient`` at the HTTP-session layer, for both the Data API and the
media worker, so the client's own request, error-parsing, upload and refusal
code runs unchanged. It enforces the server rules the desktop depends on,
with their real names:

- per-owner ``*_user_sync_key_uidx`` on every synced table and the legacy
  ``*_desktop_id_user_unique``, all answering ``23505``;
- child foreign keys (an image's observation, a measurement's image) answering
  ``23503``;
- the identity guard (``P0001`` / ``sporely_sync_identity_refused``): a key
  never changes, and a row never carries both ``sync_key`` and ``desktop_id``;
- ``updated_at`` bumped on every UPDATE, and soft-deleted images.

The media worker stores objects by key, so a second object for one image is
visible. The only test seams are the crash itself (the fault hook's process
exit becomes an exception) and the transient-retry backoff sleep.
"""
from __future__ import annotations

import copy
import json
import shutil
import sqlite3
import uuid
from pathlib import Path
from urllib.parse import parse_qsl, unquote, urlsplit

import pytest
import requests
from PIL import Image

from database import schema
from database.models import ImageDB, MeasurementDB, ObservationDB
from utils import cloud_sync
from utils.cloud_sync_impl import fault_injection
from tests.test_sync_key_observation_identity import (
    USER_ID,
    _FakeResponse,
    _Killed,
    _error,
    _now,
    two_desktops,  # noqa: F401  (fixture)
)

_TABLES = ("observations", "observation_images", "spore_measurements")
_PARENTS = {"observation_images": ("observation_id", "observations"),
            "spore_measurements": ("image_id", "observation_images")}
_MEDIA_PREFIX = "/upload/"


class ChildCloud:
    """Observations, images and measurements, plus media objects, for every client."""

    def __init__(self):
        self.tables: dict[str, list[dict]] = {name: [] for name in _TABLES}
        self.objects: dict[str, int] = {}
        self.requests: list[dict] = []
        self.media_requests: list[tuple[str, str]] = []
        self.inject_post_error: dict[str, _FakeResponse] = {}
        self.refuse_posts: dict[str, _FakeResponse] = {}
        self.timeout_after_next_insert: set[str] = set()
        self.kill_on_media_put = False
        # "before" (nothing stored) or "after" (stored, answer lost): a
        # requests.Timeout on the next media PUT.
        self.timeout_on_media_put: str | None = None
        self.after_get = None  # callable(table, filters) run after answering a GET
        self.media_upgrade_required = False  # every media PUT answers 426
        self.fail_get = None  # callable(table, filters) -> error response or None
        self._next_id = 5000

    # ── wiring ──────────────────────────────────────────────────────────
    def client(self, user_id: str = USER_ID) -> cloud_sync.SporelyCloudClient:
        client = cloud_sync.SporelyCloudClient("token", user_id)
        session = client._s

        def request(method, url, **kwargs):
            headers = {**dict(session.headers), **dict(kwargs.get("headers") or {})}
            return self.handle(method, url, headers=headers, payload=kwargs.get("json"))

        session.request = request
        worker = client._get_media_worker()
        worker._session.request = self.media
        return client

    # ── views for assertions ───────────────────────────────────────────
    def rows(self, table: str) -> list[dict]:
        return [copy.deepcopy(row) for row in self.tables[table] if row["user_id"] == USER_ID]

    def writes(self, table: str, method: str | None = None) -> list[dict]:
        return [
            r for r in self.requests
            if r["table"] == table and r["method"] in ({method} if method else {"POST", "PATCH", "DELETE"})
        ]

    def edit_elsewhere(self, table: str, row_id: int, **values) -> None:
        for row in self.tables[table]:
            if row["id"] == row_id:
                row.update(values)
                row["updated_at"] = _now()

    def insert_legacy_row(self, table: str, **values) -> dict:
        """A row written by a released desktop: ``desktop_id``, no key."""
        row = self._new_row({"user_id": USER_ID, **values})
        self.tables[table].append(row)
        return copy.deepcopy(row)

    # ── media worker ────────────────────────────────────────────────────
    def media(self, method, url, **kwargs):
        key = unquote(urlsplit(url).path.split(_MEDIA_PREFIX, 1)[-1])
        self.media_requests.append((method.upper(), key))
        if method.upper() == "PUT":
            if self.kill_on_media_put:
                self.kill_on_media_put = False
                raise _Killed()
            if self.media_upgrade_required:
                return _FakeResponse(426, {"error": "upgrade"},
                                     {"X-Sporely-Error-Code": "sporely_upgrade_required"})
            timeout, self.timeout_on_media_put = self.timeout_on_media_put, None
            if timeout == "before":
                raise requests.Timeout("media upload timed out before the bytes landed")
            data = kwargs.get("data")
            body = data.read() if hasattr(data, "read") else bytes(data or b"")
            self.objects[key] = len(body)
            if timeout == "after":
                raise requests.Timeout("media upload answer lost after the bytes landed")
            return _FakeResponse(200, {"key": key})
        if method.upper() == "DELETE":
            self.objects.pop(key, None)
            return _FakeResponse(200, {"deleted": True})
        return _FakeResponse(404, {"error": "not found"})

    # ── PostgREST emulation ────────────────────────────────────────────
    def handle(self, method: str, url: str, *, headers: dict, payload) -> _FakeResponse:
        parts = urlsplit(url)
        path = unquote(parts.path.split("/rest/v1/", 1)[-1])
        params = parse_qsl(parts.query, keep_blank_values=True)
        table = path.split("/", 1)[0] if not path.startswith("rpc/") else "rpc"
        method = method.upper()
        self.requests.append({
            "method": method, "table": table, "path": path,
            "params": params, "headers": dict(headers), "payload": copy.deepcopy(payload),
        })
        if table not in _TABLES:
            if method == "GET":
                return _FakeResponse(200, [])
            if table == "rpc":
                return _FakeResponse(200, None)
            return _FakeResponse(201, [])
        filters = [(k, v) for k, v in params if k not in {"select", "order", "limit", "offset"}]
        if method == "GET":
            failure = self.fail_get(table, filters) if self.fail_get is not None else None
            if failure is not None:
                return failure
            response = self._get(table, params, filters)
            if self.after_get is not None:
                self.after_get(table, filters)
            return response
        if method == "POST":
            return self._post(table, dict(payload or {}))
        if method == "PATCH":
            return self._patch(table, filters, dict(payload or {}), headers)
        if method == "DELETE":
            matched = self._match(table, filters)
            self.tables[table] = [r for r in self.tables[table] if r not in matched]
            return _FakeResponse(204)
        return _error(405, "PGRST000", "unsupported")

    def _match(self, table: str, filters) -> list[dict]:
        matched = list(self.tables[table])
        for column, expression in filters:
            op, _, value = expression.partition(".")
            if op == "eq":
                matched = [r for r in matched if r.get(column) is not None and str(r.get(column)) == value]
            elif op == "is" and value == "null":
                matched = [r for r in matched if r.get(column) is None]
            elif op == "in":
                wanted = set(value.strip("()").split(","))
                matched = [r for r in matched if str(r.get(column)) in wanted]
            elif op in {"gte", "gt"}:
                matched = [r for r in matched if str(r.get(column) or "") >= value]
            else:
                raise AssertionError(f"fake PostgREST: unsupported filter {column}={expression}")
        return matched

    def _get(self, table, params, filters) -> _FakeResponse:
        rows = sorted(self._match(table, filters), key=lambda r: r["id"])
        query = dict(params)
        rows = rows[int(query.get("offset") or 0):]
        if "limit" in query:
            rows = rows[: int(query["limit"])]
        return _FakeResponse(200, copy.deepcopy(rows))

    def _new_row(self, values: dict) -> dict:
        self._next_id += 1
        now = _now()
        row = {"id": self._next_id, "desktop_id": None, "sync_key": None,
               "created_at": now, "updated_at": now}
        row.update(values)
        return row

    def _refusal(self, table: str, row: dict, before: dict | None) -> _FakeResponse | None:
        if before is not None and before.get("sync_key") != row.get("sync_key"):
            return _error(400, "P0001", "sync_key is immutable", "sporely_sync_identity_refused")
        if row.get("sync_key") is not None and row.get("desktop_id") is not None:
            return _error(400, "P0001", "a row with a sync_key cannot carry desktop_id",
                          "sporely_sync_identity_refused")
        parent = _PARENTS.get(table)
        if parent is not None:
            column, parent_table = parent
            if not any(str(p["id"]) == str(row.get(column)) for p in self.tables[parent_table]):
                return _error(409, "23503", f'insert or update on table "{table}" violates '
                                            f'foreign key constraint "{table}_{column}_fkey"')
        for other in self.tables[table]:
            if other is before or other["user_id"] != row["user_id"]:
                continue
            if row.get("sync_key") is not None and other.get("sync_key") == row.get("sync_key"):
                return _error(409, "23505", f'duplicate key value violates unique constraint '
                                            f'"{table}_user_sync_key_uidx"')
            if row.get("desktop_id") is not None and other.get("desktop_id") == row.get("desktop_id"):
                return _error(409, "23505", f'duplicate key value violates unique constraint '
                                            f'"{table}_desktop_id_user_unique"')
        return None

    def _post(self, table: str, payload: dict) -> _FakeResponse:
        injected = self.inject_post_error.pop(table, None) or self.refuse_posts.get(table)
        if injected is not None:
            return injected
        row = self._new_row(payload)
        refusal = self._refusal(table, row, None)
        if refusal is not None:
            self._next_id -= 1
            return refusal
        self.tables[table].append(row)
        if table in self.timeout_after_next_insert:
            self.timeout_after_next_insert.discard(table)
            raise requests.Timeout("read timed out after the INSERT landed")
        return _FakeResponse(201, [copy.deepcopy(row)])

    def _patch(self, table, filters, payload: dict, headers: dict) -> _FakeResponse:
        updated = []
        for row in self._match(table, filters):
            candidate = {**row, **payload}
            refusal = self._refusal(table, candidate, row)
            if refusal is not None:
                return refusal
            row.update(payload)
            row["updated_at"] = _now()
            updated.append(copy.deepcopy(row))
        prefer = str(headers.get("Prefer") or "")
        return _FakeResponse(200, updated) if "representation" in prefer else _FakeResponse(204)


# ── Local helpers ────────────────────────────────────────────────────────


def _photo(directory: Path, name: str, color=(120, 80, 40)) -> str:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    Image.new("RGB", (64, 48), color).save(path, format="JPEG")
    return str(path)


def _row(table: str, local_id: int) -> dict:
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.row_factory = sqlite3.Row
        return dict(connection.execute(f"SELECT * FROM {table} WHERE id=?", (local_id,)).fetchone())


def _set(table: str, local_id: int, **values) -> None:
    assignments = ", ".join(f"{column} = ?" for column in values)
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute(
            f"UPDATE {table} SET {assignments} WHERE id = ?", (*values.values(), local_id),
        )


def _observation(**values) -> int:
    return ObservationDB.create_observation(
        date=values.pop("date", "2026-10-01"),
        genus=values.pop("genus", "Cortinarius"),
        species=values.pop("species", "violaceus"),
        **values,
    )


def _image(obs_id: int, tmp: Path, *, image_type: str = "field", name: str = "a.jpg", **values) -> int:
    return ImageDB.add_image(
        obs_id, _photo(tmp, name), image_type, copy_to_folder=False, **values,
    )


class _Point:
    def __init__(self, x: float, y: float):
        self._x, self._y = x, y

    def x(self) -> float:
        return self._x

    def y(self) -> float:
        return self._y


def _measurement(image_id: int, length: float = 10.0, width: float = 6.0) -> int:
    return MeasurementDB.add_measurement(
        image_id, length, width, "manual",
        points=[_Point(1.0, 2.0), _Point(3.0, 4.0), _Point(5.0, 6.0), _Point(7.0, 8.0)],
    )


def _prepare_all(observation, progress_cb=None):
    """The desktop's prepare step, minus WebP encoding: every image not skipped."""
    skip = set(observation.get(cloud_sync.CLOUD_SYNC_SKIP_PREPARE_IMAGE_IDS_KEY) or [])
    items = [
        {"image_row": row, "upload_path": row["filepath"]}
        for row in ImageDB.get_images_for_observation(int(observation["id"]))
        if int(row["id"]) not in skip
    ]
    return items, None, []


def _push(client, *, images: bool = True) -> dict:
    return cloud_sync.push_all(
        client,
        sync_images=images,
        prepare_images_cb=_prepare_all if images else None,
        remote_obs=client.list_remote_observations(),
        sync_calibrations=False,
        full_pull=False,
        verify_stamped_measurements=False,
    )


def _push_summary(client) -> tuple[dict, dict]:
    summary = cloud_sync._new_sync_summary()
    with cloud_sync._cloud_sync_summary_scope(summary):
        result = _push(client)
    return result, summary


def _no_desktop_id_and_no_key_patches(cloud: ChildCloud) -> None:
    for table in ("observation_images", "spore_measurements"):
        for write in cloud.writes(table):
            body = write["payload"] or {}
            assert "desktop_id" not in body, write
            if write["method"] == "PATCH":
                assert "sync_key" not in body, write


def _create_full(tmp: Path, *, image_type: str = "microscope") -> tuple[int, int, int]:
    obs_id = _observation()
    image_id = _image(obs_id, tmp, image_type=image_type)
    measurement_id = _measurement(image_id)
    return obs_id, image_id, measurement_id


def _create_for(table: str, tmp: Path) -> tuple[str, int]:
    """The local row a crash test targets: a field image, or a measurement."""
    if table == "observation_images":
        obs_id = _observation()
        return "images", _image(obs_id, tmp, image_type="field")
    _obs_id, _image_id, measurement_id = _create_full(tmp)
    return "spore_measurements", measurement_id


def _fault(monkeypatch, value: str | None) -> None:
    if value is None:
        monkeypatch.delenv(fault_injection.SYNC_FAULT_ENV_VAR, raising=False)
    else:
        monkeypatch.setenv(fault_injection.SYNC_FAULT_ENV_VAR, value)


# ── Creation with keys; payloads never carry desktop_id ──────────────────


def test_children_are_created_with_their_keys_and_never_send_desktop_id(two_desktops, tmp_path):
    cloud = ChildCloud()
    obs_id, image_id, measurement_id = _create_full(tmp_path)

    result = _push(cloud.client())

    assert result["errors"] == []
    [image_row] = cloud.rows("observation_images")
    [measurement_row] = cloud.rows("spore_measurements")
    assert image_row["sync_key"] == _row("images", image_id)["sync_key"]
    assert measurement_row["sync_key"] == _row("spore_measurements", measurement_id)["sync_key"]
    assert image_row["desktop_id"] is None and measurement_row["desktop_id"] is None
    assert str(_row("images", image_id)["cloud_id"]) == str(image_row["id"])
    assert str(_row("spore_measurements", measurement_id)["cloud_id"]) == str(measurement_row["id"])
    assert set(cloud.objects) == {image_row["storage_path"], cloud_sync.media_variant_key(image_row["storage_path"], "thumb")}
    _no_desktop_id_and_no_key_patches(cloud)

    # A second push changes nothing: no echo PATCH from a compared desktop_id.
    cloud.requests.clear()
    ObservationDB.update_observation(obs_id, notes="edited")
    assert _push(cloud.client())["errors"] == []
    assert cloud.writes("observation_images") == []
    assert cloud.writes("spore_measurements") == []


# ── Matrix rows 6–8: crash and timeout around the create ─────────────────


@pytest.mark.parametrize("table", ["observation_images", "spore_measurements"])
def test_kill_after_the_child_insert_adopts_it_on_retry(two_desktops, monkeypatch, tmp_path, table):
    cloud = ChildCloud()
    local_table, local_id = _create_for(table, tmp_path)
    _fault(monkeypatch, f"after_insert:{table}")
    with pytest.raises(_Killed):
        _push(cloud.client())
    assert _row(local_table, local_id)["cloud_id"] is None
    assert len(cloud.rows(table)) == 1
    writes_before = len(cloud.writes(table))

    # A fresh client, as after a restart: the retry repeats the same create.
    _fault(monkeypatch, None)
    result, summary = _push_summary(cloud.client())

    assert result["errors"] == []
    [row] = cloud.rows(table)
    assert str(_row(local_table, local_id)["cloud_id"]) == str(row["id"])
    retry_writes = cloud.writes(table)[writes_before:]
    assert retry_writes[0]["method"] == "POST"
    assert row["sync_key"] == _row(local_table, local_id)["sync_key"]
    assert summary[f"{cloud_sync.CHILD_TABLES[table].summary_prefix}_adopted"] == 1
    if table == "observation_images":
        assert [w["method"] for w in retry_writes] == ["POST"], "adoption never PATCHes"
        assert set(cloud.objects) == {
            row["storage_path"], cloud_sync.media_variant_key(row["storage_path"], "thumb"),
        }, "the adopted image's bytes went to its reserved key: one object"
    else:
        assert [w["method"] for w in retry_writes] == ["POST"], "adoption never PATCHes"
        assert str(row["image_id"]) == str(cloud.rows("observation_images")[0]["id"])


@pytest.mark.parametrize("table", ["observation_images", "spore_measurements"])
def test_timeout_after_a_successful_child_insert_adopts_without_a_duplicate(two_desktops, tmp_path, table):
    cloud = ChildCloud()
    local_table, local_id = _create_for(table, tmp_path)
    cloud.timeout_after_next_insert.add(table)

    assert _push(cloud.client())["errors"] == []

    [row] = cloud.rows(table)
    assert str(_row(local_table, local_id)["cloud_id"]) == str(row["id"])
    posts = [w for w in cloud.writes(table, "POST")]
    assert len(posts) == 2 and posts[0]["payload"]["sync_key"] == posts[1]["payload"]["sync_key"]
    if table == "observation_images":
        assert len(cloud.objects) == 2


@pytest.mark.parametrize("table", ["observation_images", "spore_measurements"])
def test_kill_before_the_child_request_lands_inserts_once_on_retry(two_desktops, monkeypatch, tmp_path, table):
    cloud = ChildCloud()
    local_table, local_id = _create_for(table, tmp_path)
    _fault(monkeypatch, f"before_request:{table}")
    with pytest.raises(_Killed):
        _push(cloud.client())
    assert cloud.rows(table) == [] and cloud.writes(table, "POST") == []

    _fault(monkeypatch, None)
    assert _push(cloud.client())["errors"] == []
    [row] = cloud.rows(table)
    assert len(cloud.writes(table, "POST")) == 1
    assert str(_row(local_table, local_id)["cloud_id"]) == str(row["id"])


# ── Matrix row 9: the reserved storage key is reused ─────────────────────


def test_kill_mid_upload_reuses_the_reserved_key_on_retry(two_desktops, tmp_path):
    cloud = ChildCloud()
    _obs_id, image_id, _m = _create_full(tmp_path, image_type="field")
    cloud.kill_on_media_put = True
    with pytest.raises(_Killed):
        _push(cloud.client())
    [reserved] = cloud.rows("observation_images")
    assert reserved["storage_path"] and cloud.objects == {}

    assert _push(cloud.client())["errors"] == []
    [row] = cloud.rows("observation_images")
    assert row["id"] == reserved["id"] and row["storage_path"] == reserved["storage_path"]
    assert set(cloud.objects) == {row["storage_path"], cloud_sync.media_variant_key(row["storage_path"], "thumb")}
    assert str(_row("images", image_id)["cloud_id"]) == str(row["id"])


def test_kill_after_upload_before_confirmation_reuses_the_key(two_desktops, monkeypatch, tmp_path):
    cloud = ChildCloud()
    _obs_id, image_id, _m = _create_full(tmp_path, image_type="field")
    _fault(monkeypatch, "after_upload")
    with pytest.raises(_Killed):
        _push(cloud.client())
    [reserved] = cloud.rows("observation_images")
    assert set(cloud.objects) == {reserved["storage_path"]}, "bytes landed, thumbnail and confirmation did not"
    assert _row("images", image_id)["cloud_id"] is None

    _fault(monkeypatch, None)
    assert _push(cloud.client())["errors"] == []
    [row] = cloud.rows("observation_images")
    assert row["id"] == reserved["id"]
    assert set(cloud.objects) == {row["storage_path"], cloud_sync.media_variant_key(row["storage_path"], "thumb")}
    # First run: the create. Retry: the same create, adopted; no PATCH of it.
    assert [w["method"] for w in cloud.writes("observation_images")] == ["POST", "POST"]
    assert str(_row("images", image_id)["cloud_id"]) == str(row["id"])


# ── Matrix row 10: children wait for their parent ────────────────────────


def test_children_are_deferred_until_the_parent_cloud_id_is_committed(two_desktops, tmp_path):
    cloud = ChildCloud()
    obs_id, image_id, measurement_id = _create_full(tmp_path)
    client = cloud.client()

    with pytest.raises(cloud_sync.ChildParentNotLinkedError):
        client.push_image_metadata(_row("images", image_id), "123456", "")
    assert cloud.writes("observation_images") == []

    cloud.inject_post_error["observations"] = _error(400, "22P02", "invalid input syntax")
    _push(client)
    assert cloud.rows("observation_images") == [] and cloud.rows("spore_measurements") == []
    assert _row("images", image_id)["cloud_id"] is None

    assert _push(cloud.client())["errors"] == []
    [obs_row] = cloud.rows("observations")
    [image_row] = cloud.rows("observation_images")
    [measurement_row] = cloud.rows("spore_measurements")
    assert str(image_row["observation_id"]) == str(obs_row["id"])
    assert str(measurement_row["image_id"]) == str(image_row["id"])
    assert str(_row("observations", obs_id)["cloud_id"]) == str(obs_row["id"])
    assert _row("spore_measurements", measurement_id)["cloud_id"] is not None


def test_a_measurement_waits_for_its_image(two_desktops, tmp_path):
    cloud = ChildCloud()
    _obs_id, image_id, measurement_id = _create_full(tmp_path)
    cloud.refuse_posts["observation_images"] = _error(400, "22P02", "invalid input syntax")

    result = _push(cloud.client())

    assert result["errors"]
    assert cloud.writes("spore_measurements") == []
    assert _row("spore_measurements", measurement_id)["cloud_id"] is None
    cloud.refuse_posts.clear()
    assert _push(cloud.client())["errors"] == []
    assert str(cloud.rows("spore_measurements")[0]["image_id"]) == str(cloud.rows("observation_images")[0]["id"])


# ── Matrix row 12 and image move: forks are new rows ─────────────────────


def _copy_database(source, target) -> None:
    with sqlite3.connect(source.root / "mushrooms.db") as connection:
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    for suffix in ("-wal", "-shm"):
        (target.root / f"mushrooms.db{suffix}").unlink(missing_ok=True)
    shutil.copyfile(source.root / "mushrooms.db", target.root / "mushrooms.db")


def test_imported_children_with_fresh_keys_are_new_rows(two_desktops, monkeypatch, tmp_path):
    # The importers' fork (fresh keys, no cloud_id) is unit-tested with the
    # importers (tests/test_sync_key_local.py); this is its cloud effect.
    cloud = ChildCloud()
    _obs_id, image_id, measurement_id = _create_full(tmp_path)
    assert _push(cloud.client())["errors"] == []
    a_rows = {table: cloud.rows(table) for table in _TABLES}

    _copy_database(two_desktops["A"], two_desktops["B"])
    two_desktops["B"].activate(monkeypatch)
    for table in ("observations", "images", "spore_measurements"):
        with sqlite3.connect(schema.get_database_path()) as connection:
            for (local_id,) in connection.execute(f"SELECT id FROM {table}").fetchall():
                connection.execute(
                    f"UPDATE {table} SET cloud_id = NULL, sync_key = ? WHERE id = ?",
                    (schema.new_sync_key(), local_id),
                )

    assert _push(cloud.client())["errors"] == []
    for table in _TABLES:
        rows = cloud.rows(table)
        assert len(rows) == 2
        assert a_rows[table][0] in rows, "A's row is untouched"
    assert str(_row("images", image_id)["cloud_id"]) != str(a_rows["observation_images"][0]["id"])
    assert str(_row("spore_measurements", measurement_id)["cloud_id"]) != str(a_rows["spore_measurements"][0]["id"])


def test_a_moved_image_is_a_new_cloud_row_and_the_old_one_is_tombstoned(two_desktops, tmp_path):
    cloud = ChildCloud()
    source_obs, image_id, measurement_id = _create_full(tmp_path)
    target_obs = _observation(genus="Russula", species="emetica")
    assert _push(cloud.client())["errors"] == []
    [old_image] = cloud.rows("observation_images")
    [old_measurement] = cloud.rows("spore_measurements")
    old_key = _row("images", image_id)["sync_key"]

    assert ImageDB.move_images_to_observation([image_id], source_obs, target_obs) == 1
    moved = _row("images", image_id)
    assert moved["cloud_id"] is None and moved["sync_key"] != old_key
    assert _row("spore_measurements", measurement_id)["cloud_id"] is None

    cloud.requests.clear()
    assert _push(cloud.client())["errors"] == []
    images = {row["id"]: row for row in cloud.rows("observation_images")}
    target_cloud = str(_row("observations", target_obs)["cloud_id"])
    new_image = images[int(_row("images", image_id)["cloud_id"])]
    assert str(new_image["observation_id"]) == target_cloud
    assert new_image["sync_key"] == moved["sync_key"]
    assert images[old_image["id"]]["deleted_at"], "the old cloud image is tombstoned"
    assert str(images[old_image["id"]]["observation_id"]) == str(old_image["observation_id"]), "never re-parented"
    patches = cloud.writes("observation_images", "PATCH")
    assert all("observation_id" not in (p["payload"] or {}) or p["params"][0] != ("id", f"eq.{old_image['id']}")
               for p in patches)
    measurements = {row["id"]: row for row in cloud.rows("spore_measurements")}
    new_measurement = measurements[int(_row("spore_measurements", measurement_id)["cloud_id"])]
    assert str(new_measurement["image_id"]) == str(new_image["id"])
    assert str(measurements[old_measurement["id"]]["image_id"]) == str(old_image["id"])


def test_republishing_a_deleted_image_forks_its_keys(two_desktops, tmp_path):
    _obs_id, image_id, measurement_id = _create_full(tmp_path)
    _set("images", image_id, cloud_id="5551", legacy_recovery_pending=1)
    _set("spore_measurements", measurement_id, cloud_id="5552", legacy_recovery_pending=1)
    keys = (_row("images", image_id)["sync_key"], _row("spore_measurements", measurement_id)["sync_key"])

    assert ImageDB.clear_image_cloud_sync_state(image_id)

    image, measurement = _row("images", image_id), _row("spore_measurements", measurement_id)
    assert image["cloud_id"] is None and measurement["cloud_id"] is None
    assert image["sync_key"] != keys[0] and measurement["sync_key"] != keys[1]
    assert image["legacy_recovery_pending"] == 0 and measurement["legacy_recovery_pending"] == 0


# ── Matrix rows 13–13d: copied databases and other refusals ──────────────


def test_copied_database_with_unchanged_children_adopts_them_silently(two_desktops, monkeypatch, tmp_path):
    cloud = ChildCloud()
    _obs_id, image_id, measurement_id = _create_full(tmp_path)
    _copy_database(two_desktops["A"], two_desktops["B"])
    assert _push(cloud.client())["errors"] == []
    before = {table: cloud.rows(table) for table in _TABLES}
    objects_before = dict(cloud.objects)

    two_desktops["B"].activate(monkeypatch)
    cloud.requests.clear()
    result, summary = _push_summary(cloud.client())

    assert result["errors"] == []
    for table in _TABLES:
        assert len(cloud.rows(table)) == 1
    # The anchor step and the upload each meet the same keyed row (23505);
    # neither writes it.
    assert {w["method"] for w in cloud.writes("observation_images")} == {"POST"}
    assert [w["method"] for w in cloud.writes("spore_measurements")] == ["POST"]
    assert cloud.rows("observation_images") == before["observation_images"]
    assert cloud.rows("spore_measurements") == before["spore_measurements"]
    assert set(cloud.objects) == set(objects_before), "bytes go to the adopted row's key"
    assert str(_row("images", image_id)["cloud_id"]) == str(before["observation_images"][0]["id"])
    assert str(_row("spore_measurements", measurement_id)["cloud_id"]) == str(before["spore_measurements"][0]["id"])
    assert summary["images_adopted"] == 1 and summary["measurements_adopted"] == 1


@pytest.mark.parametrize(
    "table, edit",
    [("observation_images", {"notes": "edited on the web"}),
     ("spore_measurements", {"length_um": 12.5})],
)
def test_copied_children_edited_elsewhere_raise_a_conflict_and_never_patch(
    two_desktops, monkeypatch, tmp_path, table, edit,
):
    cloud = ChildCloud()
    _obs_id, image_id, measurement_id = _create_full(tmp_path)
    _copy_database(two_desktops["A"], two_desktops["B"])
    assert _push(cloud.client())["errors"] == []
    cloud.edit_elsewhere(table, cloud.rows(table)[0]["id"], **edit)
    expected = {name: cloud.rows(name) for name in _TABLES}

    two_desktops["B"].activate(monkeypatch)
    for _attempt in range(2):
        cloud.requests.clear()
        result = _push(cloud.client())
        assert any("differs from the local row" in str(error) for error in result["errors"]), result["errors"]
        assert {w["method"] for w in cloud.writes(table)} == {"POST"}, "a differing copy is never PATCHed"
        for name in ("observation_images", "spore_measurements"):
            assert cloud.rows(name) == expected[name]
    local_table, local_id = (("images", image_id) if table == "observation_images"
                             else ("spore_measurements", measurement_id))
    assert _row(local_table, local_id)["cloud_id"] is None
    assert _row("observations", _row("images", image_id)["observation_id"])["sync_status"] == "dirty"


def test_copied_image_deleted_in_the_cloud_is_never_resurrected(two_desktops, monkeypatch, tmp_path):
    cloud = ChildCloud()
    _obs_id, image_id, _m = _create_full(tmp_path, image_type="field")
    _copy_database(two_desktops["A"], two_desktops["B"])
    assert _push(cloud.client())["errors"] == []
    cloud.edit_elsewhere("observation_images", cloud.rows("observation_images")[0]["id"],
                         deleted_at=_now())
    expected = cloud.rows("observation_images")

    two_desktops["B"].activate(monkeypatch)
    cloud.requests.clear()
    result = _push(cloud.client())
    assert any("deleted in the cloud" in str(error) for error in result["errors"]), result["errors"]
    assert cloud.rows("observation_images") == expected
    assert [w["method"] for w in cloud.writes("observation_images")] == ["POST"]
    assert _row("images", image_id)["cloud_id"] is None


def test_a_linked_measurement_whose_cloud_copy_was_purged_is_never_recreated(two_desktops, tmp_path):
    cloud = ChildCloud()
    _obs_id, _image_id, measurement_id = _create_full(tmp_path)
    assert _push(cloud.client())["errors"] == []
    cloud.tables["spore_measurements"].clear()
    _set("spore_measurements", measurement_id, length_um=11.0)
    ObservationDB.update_observation(_row("images", _image_id)["observation_id"], notes="touched")

    cloud.requests.clear()
    result = _push(cloud.client())
    assert any("was not found" in str(error) for error in result["errors"]), result["errors"]
    assert cloud.rows("spore_measurements") == []
    assert cloud.writes("spore_measurements") == []
    assert _row("spore_measurements", measurement_id)["cloud_id"] is not None


@pytest.mark.parametrize(
    "table, code, constraint",
    [
        ("observation_images", "23503", "observation_images_observation_id_fkey"),
        ("observation_images", "23505", "observation_images_desktop_id_user_unique"),
        ("spore_measurements", "23503", "spore_measurements_image_id_fkey"),
        ("spore_measurements", "23505", "spore_measurements_desktop_id_user_unique"),
    ],
)
def test_other_refusals_are_plain_errors_not_adoption(two_desktops, tmp_path, table, code, constraint):
    cloud = ChildCloud()
    local_table, local_id = _create_for(table, tmp_path)
    cloud.refuse_posts[table] = _error(409, code, f'violates constraint "{constraint}"')

    result = _push(cloud.client())

    assert any(code in str(error) for error in result["errors"]), result["errors"]
    local = _row(local_table, local_id)
    assert local["cloud_id"] is None
    assert not any(
        ("sync_key", f"eq.{local['sync_key']}") in r["params"] for r in cloud.requests
    ), "only the named sync-key index triggers the adoption lookup"


def test_adoption_of_an_image_under_another_observation_is_a_conflict(two_desktops, tmp_path):
    cloud = ChildCloud()
    obs_id, image_id, _m = _create_full(tmp_path, image_type="field")
    other = cloud.insert_legacy_row("observations", genus="Other")
    cloud.insert_legacy_row(
        "observation_images", observation_id=other["id"],
        sync_key=_row("images", image_id)["sync_key"], storage_path=f"{USER_ID}/{other['id']}/x.webp",
        image_type="field", sort_order=0,
    )

    result = _push(cloud.client())

    assert any("never re-parented" in str(error) for error in result["errors"]), result["errors"]
    assert _row("images", image_id)["cloud_id"] is None
    assert [str(r["observation_id"]) for r in cloud.rows("observation_images")] == [str(other["id"])]
    assert cloud.writes("observation_images", "PATCH") == []


# ── Matrix row 22: one-time legacy recovery for children ─────────────────


def _mark_legacy(table: str, local_id: int) -> None:
    _set(table, local_id, legacy_recovery_pending=1)


def _legacy_image_copy(image_id: int, obs_cloud_id, **overrides) -> dict:
    """What an older desktop's create of this image stored: its own content."""
    image = ImageDB.get_image(image_id)
    storage_path = f"{USER_ID}/{obs_cloud_id}/legacy.webp"
    payload = cloud_sync._prepared_item_remote_payload(
        image, image["filepath"], storage_path, include_upload_meta=False,
    )
    payload.pop("id", None)
    return {**payload, "observation_id": obs_cloud_id, "desktop_id": image_id,
            "storage_path": storage_path, "deleted_at": None, **overrides}


def _legacy_measurement_copy(measurement_id: int, legacy_image: dict, **overrides) -> dict:
    """What an older desktop's create of this measurement stored."""
    payload = cloud_sync._measurement_sync_payload(
        _row("spore_measurements", measurement_id), local=True,
        cloud_image_id=str(legacy_image["id"]),
        image_storage_key=legacy_image["storage_path"], include_media_keys=True,
    )
    return {**payload, "image_id": legacy_image["id"], "desktop_id": measurement_id, **overrides}


def _push_parent_only(tmp_path) -> tuple[ChildCloud, int, int, int]:
    """A linked observation whose image and measurement predate the upgrade."""
    cloud = ChildCloud()
    obs_id, image_id, measurement_id = _create_full(tmp_path)
    cloud.refuse_posts["observation_images"] = _error(400, "22P02", "first run fails")
    _push(cloud.client())
    cloud.refuse_posts.clear()
    assert _row("observations", obs_id)["cloud_id"] is not None
    assert _row("images", image_id)["cloud_id"] is None
    cloud.requests.clear()
    return cloud, obs_id, image_id, measurement_id


def test_legacy_recovery_links_equal_keyless_children_without_any_cloud_write(two_desktops, tmp_path):
    cloud, obs_id, image_id, measurement_id = _push_parent_only(tmp_path)
    obs_cloud = int(_row("observations", obs_id)["cloud_id"])
    legacy_image = cloud.insert_legacy_row("observation_images", **_legacy_image_copy(image_id, obs_cloud))
    legacy_measurement = cloud.insert_legacy_row(
        "spore_measurements", **_legacy_measurement_copy(measurement_id, legacy_image),
    )
    _mark_legacy("images", image_id)
    _mark_legacy("spore_measurements", measurement_id)
    rows_before = {table: cloud.rows(table) for table in ("observation_images", "spore_measurements")}

    result, summary = _push_summary(cloud.client())

    assert result["errors"] == []
    assert cloud.writes("observation_images") == [] and cloud.writes("spore_measurements") == []
    for table, rows in rows_before.items():
        assert cloud.rows(table) == rows, "no key is added and desktop_id is never written"
    assert str(_row("images", image_id)["cloud_id"]) == str(legacy_image["id"])
    assert str(_row("spore_measurements", measurement_id)["cloud_id"]) == str(legacy_measurement["id"])
    assert _row("images", image_id)["legacy_recovery_pending"] == 0
    assert _row("spore_measurements", measurement_id)["legacy_recovery_pending"] == 0
    assert summary["images_adopted"] == 1 and summary["measurements_adopted"] == 1
    assert set(cloud.objects) == {legacy_image["storage_path"],
                                  cloud_sync.media_variant_key(legacy_image["storage_path"], "thumb")}, (
        "the found row's bytes go to its own key; no second object"
    )


def test_legacy_recovery_with_different_content_posts_new_children_and_reports_duplicates(two_desktops, tmp_path):
    cloud, obs_id, image_id, measurement_id = _push_parent_only(tmp_path)
    obs_cloud = int(_row("observations", obs_id)["cloud_id"])
    other_image = cloud.insert_legacy_row(
        "observation_images", **_legacy_image_copy(image_id, obs_cloud, notes="another desktop's image"),
    )
    _mark_legacy("images", image_id)
    _mark_legacy("spore_measurements", measurement_id)

    result, summary = _push_summary(cloud.client())

    assert result["errors"] == []
    images = {row["id"]: row for row in cloud.rows("observation_images")}
    assert images[other_image["id"]] == other_image, "the matched row is never PATCHed"
    new_image_id = int(_row("images", image_id)["cloud_id"])
    assert new_image_id != other_image["id"]
    assert images[new_image_id]["sync_key"] == _row("images", image_id)["sync_key"]
    assert summary["images_possible_duplicate"] == 1
    assert summary["measurements_possible_duplicate"] == 1, "no keyless measurement under the new image"
    assert "possible duplicate of an earlier upload" in cloud_sync.format_sync_summary(summary)
    assert _row("images", image_id)["legacy_recovery_pending"] == 0


def test_legacy_lookup_is_restricted_to_the_linked_parent(two_desktops, tmp_path):
    cloud, obs_id, image_id, _measurement_id = _push_parent_only(tmp_path)
    other = cloud.insert_legacy_row("observations", genus="Other")
    elsewhere = cloud.insert_legacy_row("observation_images", **_legacy_image_copy(image_id, other["id"]))
    _mark_legacy("images", image_id)

    assert _push(cloud.client())["errors"] == []

    lookups = [r for r in cloud.requests
               if r["table"] == "observation_images" and ("desktop_id", f"eq.{image_id}") in r["params"]]
    obs_cloud = str(_row("observations", obs_id)["cloud_id"])
    assert lookups and all(("observation_id", f"eq.{obs_cloud}") in r["params"] for r in lookups)
    assert int(_row("images", image_id)["cloud_id"]) != elsewhere["id"]
    assert cloud.rows("observation_images")[0] == elsewhere


# ── Matrix row 24: ownership after two-desktop activity ──────────────────


def test_two_desktops_keep_every_child_under_its_own_parent(two_desktops, monkeypatch, tmp_path):
    cloud = ChildCloud()
    created = {}
    for name in ("A", "B"):
        two_desktops[name].activate(monkeypatch)
        created[name] = _create_full(tmp_path / name)
        assert _push(cloud.client())["errors"] == []
    assert created["A"] == created["B"], "the fixture needs overlapping local id sequences"

    for name in ("A", "B"):
        two_desktops[name].activate(monkeypatch)
        obs_id, image_id, measurement_id = created[name]
        assert _push(cloud.client())["errors"] == []
        obs_cloud = int(_row("observations", obs_id)["cloud_id"])
        image = next(r for r in cloud.rows("observation_images")
                     if str(r["id"]) == str(_row("images", image_id)["cloud_id"]))
        measurement = next(r for r in cloud.rows("spore_measurements")
                           if str(r["id"]) == str(_row("spore_measurements", measurement_id)["cloud_id"]))
        assert str(image["observation_id"]) == str(obs_cloud)
        assert str(measurement["image_id"]) == str(image["id"])
        assert image["storage_path"].startswith(f"{USER_ID}/{obs_cloud}/")
    assert len(cloud.rows("observation_images")) == 2
    assert len(cloud.rows("spore_measurements")) == 2
    assert {r["desktop_id"] for table in _TABLES for r in cloud.rows(table)} == {None}
    _no_desktop_id_and_no_key_patches(cloud)


# ── Review round 1: uncertain uploads, deleted anchors, judged baselines ─


@pytest.mark.parametrize("when", ["before", "after"])
def test_an_upload_timeout_keeps_the_reserved_row_and_key_for_the_retry(two_desktops, tmp_path, when):
    cloud = ChildCloud()
    obs_id = _observation()
    image_id = _image(obs_id, tmp_path, image_type="field")
    cloud.timeout_on_media_put = when

    # Classified as transient (the push stops) or as an image failure (the
    # observation stays dirty); either way it is surfaced, never swallowed.
    try:
        result = _push(cloud.client())
    except cloud_sync.CloudSyncError as exc:
        assert "Media upload failed" in str(exc)
    else:
        assert any("Media upload failed" in str(error) for error in result["errors"]), result["errors"]

    [reserved] = cloud.rows("observation_images")
    assert reserved["storage_path"]
    assert cloud.writes("observation_images", "DELETE") == [], "the reservation is never rolled back"
    assert set(cloud.objects) == ({reserved["storage_path"]} if when == "after" else set())
    assert _row("images", image_id)["cloud_id"] is None

    assert _push(cloud.client())["errors"] == []
    [row] = cloud.rows("observation_images")
    assert (row["id"], row["storage_path"]) == (reserved["id"], reserved["storage_path"])
    assert set(cloud.objects) == {row["storage_path"], cloud_sync.media_variant_key(row["storage_path"], "thumb")}
    assert str(_row("images", image_id)["cloud_id"]) == str(row["id"])


def test_a_definite_upload_refusal_still_rolls_back_the_new_row(two_desktops, tmp_path):
    cloud = ChildCloud()
    obs_id = _observation()
    _image(obs_id, tmp_path, image_type="field")
    real_media = cloud.media

    def refuse_put(method, url, **kwargs):
        if method.upper() == "PUT":
            return _FakeResponse(400, {"error": "bad_request", "message": "rejected"})
        return real_media(method, url, **kwargs)

    client = cloud.client()
    client._get_media_worker()._session.request = refuse_put
    assert _push(client)["errors"]
    assert cloud.rows("observation_images") == []
    assert [w["method"] for w in cloud.writes("observation_images")] == ["POST", "DELETE"]


def test_a_linked_anchor_deleted_in_the_cloud_is_never_resurrected(two_desktops, tmp_path):
    cloud = ChildCloud()
    obs_id, image_id, _measurement_id = _create_full(tmp_path)
    assert _push(cloud.client())["errors"] == []
    [image_row] = cloud.rows("observation_images")
    cloud.edit_elsewhere("observation_images", image_row["id"], deleted_at=_now())
    deleted = cloud.rows("observation_images")
    ObservationDB.update_observation(obs_id, notes="edited after the web delete")

    cloud.requests.clear()
    # The anchor step itself refuses, whoever reaches it first ...
    client = cloud.client()
    anchors = cloud_sync._ensure_metadata_only_microscope_images_for_observation(
        client, obs_id, str(_row("observations", obs_id)["cloud_id"]),
    )
    assert anchors["failed"] == 1
    assert any("never resurrected" in failure for failure in anchors["failures"])
    # ... and a full push holds the observation for review.
    result = _push(client)
    assert result["errors"]

    assert cloud.rows("observation_images") == deleted
    assert not [w for w in cloud.writes("observation_images", "PATCH")
                if "deleted_at" in (w["payload"] or {})]
    assert str(_row("images", image_id)["cloud_id"]) == str(image_row["id"])


def _stored_children(obs_id: int) -> tuple[list[dict], list[dict]]:
    cloud_id = str(_row("observations", obs_id)["cloud_id"])
    parsed = cloud_sync._parse_cloud_observation_snapshot(
        cloud_sync._load_cloud_observation_snapshot(cloud_id)
    )
    return list(parsed.get("images") or []), list(parsed.get("measurements") or [])


@pytest.mark.parametrize(
    "table, edit, field",
    [("observation_images", {"notes": "edited on the web"}, "notes"),
     ("spore_measurements", {"length_um": 12.5}, "length_um")],
)
def test_an_adopted_child_keeps_its_fetched_row_as_baseline(
    two_desktops, monkeypatch, tmp_path, table, edit, field,
):
    cloud = ChildCloud()
    if table == "observation_images":
        obs_id = _observation()
        _image(obs_id, tmp_path, image_type="field")
    else:
        obs_id, _image_id, _measurement_id = _create_full(tmp_path)
    _copy_database(two_desktops["A"], two_desktops["B"])
    assert _push(cloud.client())["errors"] == []
    judged = cloud.rows(table)[0][field]

    def edit_after_adoption_read(read_table, filters):
        if read_table == table and any(column == "sync_key" and value.startswith("eq.")
                                       for column, value in filters):
            cloud.after_get = None
            cloud.edit_elsewhere(table, cloud.rows(table)[0]["id"], **edit)

    two_desktops["B"].activate(monkeypatch)
    cloud.after_get = edit_after_adoption_read
    assert _push(cloud.client())["errors"] == []

    assert cloud.rows(table)[0][field] == edit[field], "the edit landed after the adoption read"
    images, measurements = _stored_children(obs_id)
    stored = images if table == "observation_images" else measurements
    assert len(stored) == 1
    assert stored[0][field] == judged, "the baseline is the judged row, not a later read"


# ── Review round 2: promotion timeouts, vanished judged rows, push scope ─


@pytest.mark.parametrize("when", ["before", "after"])
def test_a_promotion_upload_timeout_keeps_the_reserved_key_for_the_retry(two_desktops, tmp_path, when):
    cloud = ChildCloud()
    _obs_id, image_id, _measurement_id = _create_full(tmp_path)  # anchor first, then promotion
    cloud.timeout_on_media_put = when
    try:
        _push(cloud.client())
    except cloud_sync.CloudSyncError as exc:
        assert "Media upload failed" in str(exc)

    [anchor] = cloud.rows("observation_images")
    reserved_key = anchor["storage_path"]
    assert reserved_key, "the promotion's reservation is kept"
    assert str(_row("images", image_id)["cloud_id"]) == str(anchor["id"])
    assert set(cloud.objects) == ({reserved_key} if when == "after" else set())

    assert _push(cloud.client())["errors"] == []
    [row] = cloud.rows("observation_images")
    assert (row["id"], row["storage_path"]) == (anchor["id"], reserved_key), "same row, same key"
    assert set(cloud.objects) == {reserved_key, cloud_sync.media_variant_key(reserved_key, "thumb")}


@pytest.mark.parametrize("vanish", ["deleted", "purged"])
def test_a_judged_image_that_vanishes_before_the_baseline_stays_in_it(
    two_desktops, monkeypatch, tmp_path, vanish,
):
    cloud = ChildCloud()
    obs_id = _observation()
    _image(obs_id, tmp_path, image_type="field")
    _copy_database(two_desktops["A"], two_desktops["B"])
    assert _push(cloud.client())["errors"] == []
    judged_id = cloud.rows("observation_images")[0]["id"]

    def vanish_after_adoption_read(read_table, filters):
        if read_table == "observation_images" and any(
            column == "sync_key" and value.startswith("eq.") for column, value in filters
        ):
            cloud.after_get = None
            if vanish == "deleted":
                cloud.edit_elsewhere("observation_images", judged_id, deleted_at=_now())
            else:
                cloud.tables["observation_images"].clear()

    two_desktops["B"].activate(monkeypatch)
    cloud.after_get = vanish_after_adoption_read
    _push(cloud.client())

    images, _measurements = _stored_children(obs_id)
    assert [str(image["id"]) for image in images] == [str(judged_id)], (
        "the removal after the adoption read stays a remote change against the judged baseline"
    )


def test_judged_rows_never_outlive_an_interrupted_push(two_desktops, monkeypatch, tmp_path):
    cloud = ChildCloud()
    obs_id = _observation()
    _image(obs_id, tmp_path, image_type="field")
    _copy_database(two_desktops["A"], two_desktops["B"])
    assert _push(cloud.client())["errors"] == []

    two_desktops["B"].activate(monkeypatch)
    adopted = {}

    def note_adoption_read(read_table, filters):
        if read_table == "observation_images" and any(
            column == "sync_key" and value.startswith("eq.") for column, value in filters
        ):
            adopted["read"] = True

    def fail_listing_after_adoption(read_table, filters):
        # The baseline's child read fails after the adoption judged a row.
        if adopted and read_table == "observation_images" and any(
            column == "observation_id" for column, _ in filters
        ):
            return _error(400, "22P02", "listing refused")
        return None

    cloud.after_get = note_adoption_read
    cloud.fail_get = fail_listing_after_adoption
    client = cloud.client()
    result = _push(client)
    assert any("listing refused" in str(error) for error in result["errors"]), result["errors"]
    assert adopted, "the adoption judged a row before the push stopped short of its baseline"
    assert not getattr(client, "_judged_child_rows", {}), "dropped when the push ends"

    cloud.fail_get = None
    cloud.media_upgrade_required = True
    _image(obs_id, tmp_path / "later", image_type="field", name="b.jpg")

    # sync_all catches the refusal and goes on to pull with the same client.
    result = cloud_sync.sync_all(
        client, sync_images=True, prepare_images_cb=_prepare_all,
        materialize_remote_images=False, full_pull=False,
    )
    assert result.get("upload_blocked") == "upgrade_required"
    assert not getattr(client, "_judged_child_rows", {})


# ── Review round 3: a crashed anchor promotion is rescheduled ────────────


@pytest.mark.parametrize("crash", ["after_reservation", "after_bytes"])
def test_a_crashed_anchor_promotion_resumes_on_the_next_ordinary_sync(
    two_desktops, monkeypatch, tmp_path, crash,
):
    cloud = ChildCloud()
    obs_id, image_id, _measurement_id = _create_full(tmp_path)  # anchor, then promotion
    if crash == "after_reservation":
        cloud.kill_on_media_put = True
    else:
        _fault(monkeypatch, "after_upload")
    with pytest.raises(_Killed):
        _push(cloud.client())
    _fault(monkeypatch, None)

    [anchor] = cloud.rows("observation_images")
    reserved_key = anchor["storage_path"]
    assert reserved_key, "the promotion reserved its key before the crash"
    assert str(_row("images", image_id)["cloud_id"]) == str(anchor["id"]), "the anchor is linked"
    assert _row("observations", obs_id)["sync_status"] == "synced", "stamped before the images"
    due, reason = cloud_sync._cloud_pending_image_repair_scan_due()
    assert not due, f"the periodic scan is not due ({reason}); it cannot be what recovers this"

    # A fresh client, as after a restart; no user edit, no forced scan.
    assert _push(cloud.client())["errors"] == []

    [row] = cloud.rows("observation_images")
    assert (row["id"], row["storage_path"]) == (anchor["id"], reserved_key), "same row, same key"
    assert set(cloud.objects) == {reserved_key, cloud_sync.media_variant_key(reserved_key, "thumb")}
    assert _row("observations", obs_id)["sync_status"] == "synced"
    assert not cloud_sync._load_pending_image_promotion_key(obs_id, image_id), "promotion confirmed"

    # Settled: the next sync uploads nothing and rewrites nothing.
    cloud.requests.clear()
    media_before = list(cloud.media_requests)
    assert _push(cloud.client())["errors"] == []
    assert cloud.media_requests == media_before
    assert cloud.writes("observation_images") == []
