"""Stage 5: pull and conflict pairing by cloud id, then ``sync_key`` (U tier).

Two SQLite databases and one shared fake cloud (the Stage 4 harness, with a
media worker that also serves the stored bytes back). The real sync functions
run unchanged; the only seams are the transient-retry backoff sleep and the
fault hook. Integer local ids overlap between the two databases on purpose:
pull must never pair a cloud row with a local row by an integer.
"""
from __future__ import annotations

import sqlite3
import uuid
from urllib.parse import unquote, urlsplit


from database import schema
from database.models import ImageDB, ObservationDB
from utils import cloud_sync
from tests.test_sync_key_child_identity import (
    _MEDIA_PREFIX,
    ChildCloud,
    _FakeResponse,
    _image,
    _measurement,
    _observation,
    _prepare_all,
    _row,
    _set,
    USER_ID,
    two_desktops,  # noqa: F401  (fixture)
)


class _BytesResponse(_FakeResponse):
    def __init__(self, body: bytes):
        super().__init__(200)
        self.content = body
        self.text = ""

    def iter_content(self, chunk_size=1):
        yield self.content


class BytesCloud(ChildCloud):
    """``ChildCloud`` whose media worker keeps the bytes and serves them back."""

    def __init__(self):
        super().__init__()
        self.blobs: dict[str, bytes] = {}

    def media(self, method, url, **kwargs):
        key = unquote(urlsplit(url).path.split(_MEDIA_PREFIX, 1)[-1])
        if method.upper() == "PUT":
            data = kwargs.get("data")
            body = data.read() if hasattr(data, "read") else bytes(data or b"")
            if hasattr(data, "seek"):
                data.seek(0)
            self.blobs[key] = body
        if method.upper() == "GET":
            self.media_requests.append(("GET", key))
            if key in self.blobs:
                return _BytesResponse(self.blobs[key])
            return _FakeResponse(404, {"error": "NoSuchKey"})
        return super().media(method, url, **kwargs)

    def identity_writes(self) -> list[dict]:
        """Every write that would put an identity onto an existing cloud row."""
        return [
            write for table in ("observations", "observation_images", "spore_measurements")
            for write in self.writes(table, "PATCH")
            if {"desktop_id", "sync_key"} & set(write["payload"] or {})
        ]


def _sync(client, *, materialize: bool = True) -> dict:
    return cloud_sync.sync_all(
        client,
        sync_images=True,
        prepare_images_cb=_prepare_all,
        materialize_remote_images=materialize,
        full_pull=False,
    )


def _errors(result: dict) -> list[str]:
    # The fake answers reference-library reads with nothing; those errors are
    # outside this stage.
    return [e for e in result.get("errors") or [] if not str(e).startswith("reference ")]


def _local_rows(table: str) -> list[dict]:
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.row_factory = sqlite3.Row
        return [dict(r) for r in connection.execute(f"SELECT * FROM {table} ORDER BY id")]


def _cloud_state(cloud: ChildCloud) -> dict:
    return {table: cloud.rows(table) for table in cloud.tables}


def _full_graph(tmp, genus: str, *, name: str) -> tuple[int, int, int]:
    obs_id = _observation(genus=genus, species="sp")
    image_id = _image(obs_id, tmp / name, image_type="microscope", name=f"{name}.jpg")
    measurement_id = _measurement(image_id, length=10.0 if genus < "B" else 20.0)
    return obs_id, image_id, measurement_id


# ── Regression: overlapping local ids on two desktops (fails on ee6f2af) ──


def test_overlapping_local_ids_insert_on_b_and_leave_a_untouched(two_desktops, monkeypatch, tmp_path):
    cloud = BytesCloud()
    two_desktops["A"].activate(monkeypatch)
    a_ids = _full_graph(tmp_path, "Amanita", name="a")
    assert _errors(_sync(cloud.client())) == []
    a_cloud = _cloud_state(cloud)
    assert [len(rows) for rows in a_cloud.values()] == [1, 1, 1]

    two_desktops["B"].activate(monkeypatch)
    b_ids = _full_graph(tmp_path, "Boletus", name="b")
    assert b_ids == a_ids, "the fixture needs overlapping local id sequences"
    assert _errors(_sync(cloud.client())) == []

    # B's rows are new cloud rows; A's rows are exactly as A left them.
    for table, before in a_cloud.items():
        rows = {row["id"]: row for row in cloud.rows(table)}
        assert len(rows) == 2, table
        assert rows[before[0]["id"]] == before[0], f"B's sync changed A's {table} row"
    b_obs = _row("observations", b_ids[0])
    assert b_obs["genus"] == "Boletus" and b_obs["cloud_id"] not in (None, str(a_cloud["observations"][0]["id"]))
    assert _row("spore_measurements", b_ids[2])["length_um"] == 20.0

    # B also pulled A's graph as new local rows carrying A's keys.
    pulled = [row for row in _local_rows("observations") if row["id"] != b_ids[0]]
    assert [row["genus"] for row in pulled] == ["Amanita"]
    assert pulled[0]["sync_key"] == a_cloud["observations"][0]["sync_key"]
    pulled_images = [row for row in _local_rows("images") if row["id"] != b_ids[1]]
    assert [row["sync_key"] for row in pulled_images] == [a_cloud["observation_images"][0]["sync_key"]]
    pulled_measurements = [row for row in _local_rows("spore_measurements") if row["id"] != b_ids[2]]
    assert [(row["length_um"], row["sync_key"]) for row in pulled_measurements] == [
        (10.0, a_cloud["spore_measurements"][0]["sync_key"]),
    ]
    assert cloud.identity_writes() == []

    # A pulls B's graph the same way; A's own rows stay A's.
    two_desktops["A"].activate(monkeypatch)
    assert _errors(_sync(cloud.client())) == []
    assert _row("observations", a_ids[0])["genus"] == "Amanita"
    assert _row("spore_measurements", a_ids[2])["length_um"] == 10.0
    assert sorted(row["genus"] for row in _local_rows("observations")) == ["Amanita", "Boletus"]
    assert cloud.identity_writes() == []


def test_a_released_desktop_row_with_an_overlapping_desktop_id_pulls_as_a_new_row(
    two_desktops, monkeypatch, tmp_path,
):
    cloud = BytesCloud()
    two_desktops["B"].activate(monkeypatch)
    b_obs, b_image, b_measurement = _full_graph(tmp_path, "Boletus", name="b")
    assert _errors(_sync(cloud.client())) == []
    b_cloud = _cloud_state(cloud)

    # A released desktop wrote its own keyless rows whose desktop_id values are
    # B's local ids (its own SQLite counted from 1 too).
    legacy_obs = cloud.insert_legacy_row(
        "observations", desktop_id=b_obs, genus="Lactarius", species="deliciosus",
        date="2026-09-01", visibility="private",
    )
    ObservationDB.update_observation(b_obs, notes="B's own note")
    cloud.requests.clear()
    assert _errors(_sync(cloud.client())) == []

    by_genus = {row["genus"]: row for row in _local_rows("observations")}
    assert set(by_genus) == {"Boletus", "Lactarius"}
    assert by_genus["Boletus"]["id"] == b_obs and by_genus["Boletus"]["notes"] == "B's own note"
    assert str(by_genus["Lactarius"]["cloud_id"]) == str(legacy_obs["id"])
    assert by_genus["Lactarius"]["sync_key"] is None, "a keyless cloud row is never given a key"
    # Nothing was written onto the released desktop's row.
    assert [w for w in cloud.writes("observations") if str(legacy_obs["id"]) in str(w["params"])] == []
    assert cloud.rows("observations")[-1] == legacy_obs
    assert cloud.identity_writes() == []
    # B's children kept their own cloud rows.
    assert cloud.rows("observation_images") == b_cloud["observation_images"]
    assert cloud.rows("spore_measurements") == b_cloud["spore_measurements"]


# ── Matrix row 2: both download, alternating pulls ───────────────────────


def test_alternating_pulls_make_no_identity_writes_and_no_updated_at_echo(two_desktops, monkeypatch, tmp_path):
    cloud = BytesCloud()
    two_desktops["A"].activate(monkeypatch)
    _full_graph(tmp_path, "Amanita", name="a")
    assert _errors(_sync(cloud.client())) == []
    two_desktops["B"].activate(monkeypatch)
    _full_graph(tmp_path, "Boletus", name="b")
    assert _errors(_sync(cloud.client())) == []
    two_desktops["A"].activate(monkeypatch)
    assert _errors(_sync(cloud.client())) == []

    settled = _cloud_state(cloud)
    cloud.requests.clear()
    for _round in range(3):
        for name in ("A", "B"):
            two_desktops[name].activate(monkeypatch)
            assert _errors(_sync(cloud.client())) == []
    assert [w for table in cloud.tables for w in cloud.writes(table)] == []
    assert _cloud_state(cloud) == settled, "a pull wrote to the cloud (updated_at echo)"
    for name in ("A", "B"):
        two_desktops[name].activate(monkeypatch)
        assert sorted(row["genus"] for row in _local_rows("observations")) == ["Amanita", "Boletus"]
        assert len(_local_rows("images")) == 2 and len(_local_rows("spore_measurements")) == 2


# ── Matrix row 3: A edits, B pulls ──────────────────────────────────────


def test_an_edit_on_a_changes_only_the_linked_rows_on_b(two_desktops, monkeypatch, tmp_path):
    cloud = BytesCloud()
    two_desktops["A"].activate(monkeypatch)
    a_obs, a_image, a_measurement = _full_graph(tmp_path, "Amanita", name="a")
    assert _errors(_sync(cloud.client())) == []
    two_desktops["B"].activate(monkeypatch)
    b_obs, b_image, b_measurement = _full_graph(tmp_path, "Boletus", name="b")
    assert _errors(_sync(cloud.client())) == []
    b_own = (_row("observations", b_obs), _row("images", b_image), _row("spore_measurements", b_measurement))

    two_desktops["A"].activate(monkeypatch)
    ObservationDB.update_observation(a_obs, notes="edited on A")
    ImageDB.update_image(a_image, notes="image note from A")
    ObservationDB.update_observation(a_obs, notes="edited on A")
    assert _errors(_sync(cloud.client())) == []

    two_desktops["B"].activate(monkeypatch)
    cloud.requests.clear()
    assert _errors(_sync(cloud.client())) == []
    copy_of_a = [row for row in _local_rows("observations") if row["id"] != b_obs]
    assert [row["notes"] for row in copy_of_a] == ["edited on A"]
    copy_images = [row for row in _local_rows("images") if row["id"] != b_image]
    assert [row["notes"] for row in copy_images] == ["image note from A"]
    copy_measurements = [row for row in _local_rows("spore_measurements") if row["id"] != b_measurement]
    assert [row["length_um"] for row in copy_measurements] == [10.0]
    # B's own rows, which share A's local ids, are untouched.
    assert _row("observations", b_obs)["notes"] == b_own[0]["notes"]
    assert _row("images", b_image)["notes"] == b_own[1]["notes"]
    assert _row("spore_measurements", b_measurement)["length_um"] == b_own[2]["length_um"]
    assert [w for table in cloud.tables for w in cloud.writes(table)] == []


# ── Matrix rows 4/5: conflict, keep-device / take-cloud ──────────────────


def _conflicted_pair(two_desktops, monkeypatch, tmp_path) -> tuple[BytesCloud, int, int, dict]:
    """A's observation edited on both A and B; B holds its own row with A's ids."""
    cloud = BytesCloud()
    two_desktops["A"].activate(monkeypatch)
    a_obs, _a_image, _a_measurement = _full_graph(tmp_path, "Amanita", name="a")
    assert _errors(_sync(cloud.client())) == []
    two_desktops["B"].activate(monkeypatch)
    b_obs, _b_image, _b_measurement = _full_graph(tmp_path, "Boletus", name="b")
    assert _errors(_sync(cloud.client())) == []
    copy_id = next(row["id"] for row in _local_rows("observations") if row["id"] != b_obs)

    two_desktops["A"].activate(monkeypatch)
    ObservationDB.update_observation(a_obs, notes="A's version")
    assert _errors(_sync(cloud.client())) == []

    two_desktops["B"].activate(monkeypatch)
    ObservationDB.update_observation(copy_id, notes="B's version")
    result = _sync(cloud.client())
    assert any("review" in str(error).lower() for error in _errors(result)), result["errors"]
    return cloud, b_obs, copy_id, _cloud_state(cloud)


def _b_row_unchanged(cloud: ChildCloud, before: dict, b_cloud_id: str) -> None:
    for table in ("observations",):
        rows = {str(row["id"]): row for row in cloud.rows(table)}
        old = {str(row["id"]): row for row in before[table]}
        assert rows[b_cloud_id] == old[b_cloud_id]


def test_keep_device_changes_only_the_paired_rows(two_desktops, monkeypatch, tmp_path):
    cloud, b_obs, copy_id, before = _conflicted_pair(two_desktops, monkeypatch, tmp_path)
    b_own = _row("observations", b_obs)
    copy_cloud_id = str(_row("observations", copy_id)["cloud_id"])
    cloud.requests.clear()

    cloud_sync.resolve_conflict_keep_local(cloud.client(), copy_id, prepare_images_cb=_prepare_all)

    obs_patches = cloud.writes("observations", "PATCH")
    assert obs_patches and all(p["params"][0] == ("id", f"eq.{copy_cloud_id}") for p in obs_patches)
    assert cloud.writes("observations", "POST") == []
    rows = {str(row["id"]): row for row in cloud.rows("observations")}
    assert rows[copy_cloud_id]["notes"] == "B's version"
    _b_row_unchanged(cloud, before, str(b_own["cloud_id"]))
    assert _row("observations", b_obs) == b_own
    assert cloud.identity_writes() == []


def test_take_cloud_changes_only_the_paired_local_row(two_desktops, monkeypatch, tmp_path):
    cloud, b_obs, copy_id, before = _conflicted_pair(two_desktops, monkeypatch, tmp_path)
    b_own = _row("observations", b_obs)
    b_images = [dict(r) for r in _local_rows("images")]
    cloud.requests.clear()

    cloud_sync.resolve_conflict_keep_cloud(
        cloud.client(), copy_id, cloud_id=str(_row("observations", copy_id)["cloud_id"]),
    )

    assert _row("observations", copy_id)["notes"] == "A's version"
    assert _row("observations", b_obs) == b_own
    assert [r["id"] for r in _local_rows("images")] == [r["id"] for r in b_images]
    assert [w for table in cloud.tables for w in cloud.writes(table)] == []
    assert _cloud_state(cloud) == before


def test_conflict_detail_pairs_children_by_cloud_id_then_by_key(two_desktops, monkeypatch, tmp_path):
    cloud, b_obs, copy_id, _before = _conflicted_pair(two_desktops, monkeypatch, tmp_path)
    copy_image = next(r for r in _local_rows("images") if r["observation_id"] == copy_id)
    copy_measurement = next(
        r for r in _local_rows("spore_measurements") if r["image_id"] == copy_image["id"]
    )
    # A lost local link on the measurement: it still pairs, by its key.
    _set("spore_measurements", copy_measurement["id"], cloud_id=None)

    detail = cloud_sync.get_conflict_detail(cloud.client(), copy_id)

    [image_pair] = [p for p in detail["image_pairs"] if p.get("local")]
    assert image_pair["match_basis"] == "cloud_id"
    assert image_pair["local"]["local_id"] == copy_image["id"]
    assert image_pair["remote"]["local_id"] is None, "a cloud row never names a local row"
    [measurement_pair] = image_pair["measurement_pairs"]
    assert measurement_pair["pairing"] == "sync_key"
    assert measurement_pair["local_id"] == copy_measurement["id"]
    assert str(measurement_pair["cloud_id"]) == str(copy_measurement["cloud_id"])


# ── Matrix row 10: a child whose parent is unknown ───────────────────────


def test_a_pulled_measurement_waits_for_its_image_and_never_attaches_by_integer(
    two_desktops, monkeypatch, tmp_path,
):
    cloud = BytesCloud()
    two_desktops["A"].activate(monkeypatch)
    _full_graph(tmp_path, "Amanita", name="a")
    assert _errors(_sync(cloud.client())) == []
    a_measurement = cloud.rows("spore_measurements")[0]

    two_desktops["B"].activate(monkeypatch)
    b_obs, b_image, b_measurement = _full_graph(tmp_path, "Boletus", name="b")
    # Let the cloud measurement carry B's local id as a stale desktop_id.
    cloud.edit_elsewhere("spore_measurements", a_measurement["id"], desktop_id=b_measurement, sync_key=None)
    assert _errors(_sync(cloud.client(), materialize=False)) == []
    # The image was not materialized, so its measurement is deferred: nothing
    # was attached to B's own measurement or image.
    assert _row("spore_measurements", b_measurement)["length_um"] == 20.0
    assert str(_row("spore_measurements", b_measurement)["cloud_id"]) != str(a_measurement["id"])
    assert [r["id"] for r in _local_rows("spore_measurements")] == [b_measurement]

    assert _errors(_sync(cloud.client(), materialize=True)) == []
    pulled_image = next(r for r in _local_rows("images") if r["id"] != b_image)
    pulled = next(r for r in _local_rows("spore_measurements") if r["id"] != b_measurement)
    assert pulled["image_id"] == pulled_image["id"]
    assert str(pulled["cloud_id"]) == str(a_measurement["id"]) and pulled["length_um"] == 10.0
    assert _row("spore_measurements", b_measurement)["length_um"] == 20.0
    assert cloud.identity_writes() == []


def test_a_pushed_child_waits_for_its_parent_and_is_created_under_it(two_desktops, tmp_path):
    cloud = BytesCloud()
    obs_id, image_id, measurement_id = _full_graph(tmp_path, "Amanita", name="a")
    assert _errors(_sync(cloud.client())) == []
    posts = [w["table"] for w in cloud.requests if w["method"] == "POST"]
    assert posts.index("observations") < posts.index("observation_images") < posts.index("spore_measurements")
    [image_row] = cloud.rows("observation_images")
    [measurement_row] = cloud.rows("spore_measurements")
    assert str(image_row["observation_id"]) == str(_row("observations", obs_id)["cloud_id"])
    assert str(measurement_row["image_id"]) == str(_row("images", image_id)["cloud_id"])


# ── Pull never duplicates a row this database already holds by key ───────


def test_download_from_cloud_never_duplicates_an_unlinked_keyed_row(two_desktops, tmp_path):
    cloud = BytesCloud()
    obs_id = _observation(genus="Amanita", species="sp")
    local = _row("observations", obs_id)
    # This database's create landed; its answer was lost (no local cloud_id).
    landed = cloud._new_row({
        "user_id": USER_ID, "sync_key": local["sync_key"], "genus": "Amanita", "species": "sp",
        "date": local["date"], "visibility": "private",
    })
    cloud.tables["observations"].append(landed)

    result = cloud_sync.sync_all(cloud.client(), pull_only=True, materialize_remote_images=False)
    assert _errors(result) == []
    assert [r["id"] for r in _local_rows("observations")] == [obs_id]
    assert _row("observations", obs_id)["cloud_id"] is None
    assert [w for table in cloud.tables for w in cloud.writes(table)] == []


def test_pulled_rows_store_the_cloud_key_and_keyless_rows_stay_keyless(two_desktops, tmp_path):
    cloud = BytesCloud()
    keyed = cloud._new_row({"user_id": USER_ID, "sync_key": str(uuid.uuid4()), "genus": "Russula",
                            "date": "2026-09-30", "visibility": "private"})
    keyless = cloud._new_row({"user_id": USER_ID, "genus": "Lactarius",
                              "date": "2026-09-29", "visibility": "private"})
    cloud.tables["observations"] += [keyed, keyless]
    assert _errors(_sync(cloud.client())) == []
    keys = {row["genus"]: row["sync_key"] for row in _local_rows("observations")}
    assert keys == {"Russula": keyed["sync_key"], "Lactarius": None}
    assert cloud.identity_writes() == []


def test_remote_observations_not_on_this_desktop_drop_rows_held_by_id_or_key(two_desktops, tmp_path):
    linked = _observation(genus="Amanita", species="sp")
    unlinked = _observation(genus="Boletus", species="sp")
    _set("observations", linked, cloud_id="5001")
    rows = [
        {"id": 5001, "sync_key": None},
        {"id": 5002, "sync_key": _row("observations", unlinked)["sync_key"]},
        {"id": 5003, "sync_key": str(uuid.uuid4()), "desktop_id": unlinked},
    ]
    assert [r["id"] for r in cloud_sync.remote_observations_not_on_this_desktop(rows)] == [5003]


# ── Portable-pending rows upload through the state machine ───────────────


def test_a_portable_pending_graph_uploads_with_keys_and_never_reads_or_writes_desktop_id(
    two_desktops, tmp_path,
):
    cloud = BytesCloud()
    obs_id, image_id, measurement_id = _full_graph(tmp_path, "Amanita", name="a")
    _set("observations", obs_id, portable_cloud_identity_pending=1)

    assert _errors(_sync(cloud.client())) == []

    for table, local_table, local_id in (
        ("observations", "observations", obs_id),
        ("observation_images", "images", image_id),
        ("spore_measurements", "spore_measurements", measurement_id),
    ):
        [row] = cloud.rows(table)
        assert row["sync_key"] == _row(local_table, local_id)["sync_key"]
        assert str(_row(local_table, local_id)["cloud_id"]) == str(row["id"])
        assert row["desktop_id"] is None
    for request in cloud.requests:
        assert "desktop_id" not in str(request["params"]), request
        assert "desktop_id" not in (request["payload"] or {}), request
    # The flag no longer gates anything; it stays until the deferred cleanup.
    assert _row("observations", obs_id)["portable_cloud_identity_pending"] == 1


# ── Reviewed key-paired children: both conflict choices apply ────────────


def _lost_link_child_conflict(tmp_path) -> tuple[BytesCloud, int, int, int, dict, dict]:
    """This database's image and measurement exist in the cloud, their local
    links were lost, and both sides edited them since: a key-paired conflict."""
    cloud = BytesCloud()
    obs_id, image_id, measurement_id = _full_graph(tmp_path, "Amanita", name="a")
    assert _errors(_sync(cloud.client())) == []
    [cloud_image] = cloud.rows("observation_images")
    [cloud_measurement] = cloud.rows("spore_measurements")
    _set("images", image_id, cloud_id=None)
    _set("spore_measurements", measurement_id, cloud_id=None)
    ImageDB.update_image(image_id, notes="local note")
    with sqlite3.connect(schema.get_database_path()) as connection:
        connection.execute("UPDATE spore_measurements SET length_um = 12.0 WHERE id = ?", (measurement_id,))
    ObservationDB.update_observation(obs_id, notes="touched")
    cloud.edit_elsewhere("observation_images", cloud_image["id"], notes="cloud note")
    cloud.edit_elsewhere("spore_measurements", cloud_measurement["id"], length_um=14.0)

    result = _sync(cloud.client())
    assert result["errors"], "a differing key-paired child is never linked by ordinary sync"
    assert _row("images", image_id)["cloud_id"] is None
    return (cloud, obs_id, image_id, measurement_id,
            cloud.rows("observation_images")[0], cloud.rows("spore_measurements")[0])


def _plan_for(cloud, obs_id, choice: str) -> dict:
    detail = cloud_sync.get_conflict_detail(cloud.client(), obs_id)
    [image_pair] = [p for p in detail["image_pairs"] if p.get("local")]
    assert image_pair["match_basis"] == "sync_key"
    [measurement_pair] = image_pair["measurement_pairs"]
    assert measurement_pair["pairing"] == "sync_key"
    items = [
        {"kind": "image_metadata", "local_id": image_pair["local"]["local_id"],
         "cloud_id": image_pair["remote"]["cloud_id"], "choice": choice},
        {"kind": "measurement", "side": "matched", "local_id": measurement_pair["local_id"],
         "cloud_id": measurement_pair["cloud_id"], "local_image_id": measurement_pair["local_image_id"],
         "cloud_image_id": measurement_pair["cloud_image_id"], "choice": choice},
    ]
    return {"items": items, "baseline": detail["plan_baseline"], "allow_media_deletion": False}


def test_take_cloud_on_key_paired_children_links_and_applies_the_cloud_rows(two_desktops, tmp_path):
    cloud, obs_id, image_id, measurement_id, cloud_image, cloud_measurement = (
        _lost_link_child_conflict(tmp_path)
    )
    plan = _plan_for(cloud, obs_id, "cloud")
    before = _cloud_state(cloud)
    cloud.requests.clear()

    cloud_sync.resolve_conflict_plan(cloud.client(), obs_id, plan=plan)

    assert str(_row("images", image_id)["cloud_id"]) == str(cloud_image["id"])
    assert _row("images", image_id)["notes"] == "cloud note"
    assert str(_row("spore_measurements", measurement_id)["cloud_id"]) == str(cloud_measurement["id"])
    assert _row("spore_measurements", measurement_id)["length_um"] == 14.0
    for table in ("observation_images", "spore_measurements"):
        assert cloud.writes(table) == [], table
        assert cloud.rows(table) == before[table]
    assert len(_local_rows("images")) == 1 and len(_local_rows("spore_measurements")) == 1
    cloud.requests.clear()
    assert _errors(_sync(cloud.client())) == []
    assert [w for t in ("observation_images", "spore_measurements") for w in cloud.writes(t)] == []


def test_keep_device_on_key_paired_children_patches_the_keyed_rows_by_id(two_desktops, tmp_path):
    cloud, obs_id, image_id, measurement_id, cloud_image, cloud_measurement = (
        _lost_link_child_conflict(tmp_path)
    )
    plan = _plan_for(cloud, obs_id, "local")
    cloud.requests.clear()

    cloud_sync.resolve_conflict_plan(cloud.client(), obs_id, plan=plan, prepare_images_cb=_prepare_all)

    assert str(_row("images", image_id)["cloud_id"]) == str(cloud_image["id"])
    assert str(_row("spore_measurements", measurement_id)["cloud_id"]) == str(cloud_measurement["id"])
    [image_row] = cloud.rows("observation_images")
    [measurement_row] = cloud.rows("spore_measurements")
    assert image_row["notes"] == "local note" and measurement_row["length_um"] == 12.0
    for table, row in (("observation_images", cloud_image), ("spore_measurements", cloud_measurement)):
        assert cloud.writes(table, "POST") == [], table
        patches = cloud.writes(table, "PATCH")
        assert patches and all(("id", f"eq.{row['id']}") in p["params"] for p in patches), table
        assert all("sync_key" not in (p["payload"] or {}) for p in patches)
    cloud.requests.clear()
    assert _errors(_sync(cloud.client())) == []
    assert [w for t in ("observation_images", "spore_measurements") for w in cloud.writes(t, "POST")] == []


def test_review_never_links_a_key_pair_under_another_parent(two_desktops, tmp_path):
    cloud, obs_id, image_id, measurement_id, cloud_image, _cloud_measurement = (
        _lost_link_child_conflict(tmp_path)
    )
    # The keyed cloud image now claims another observation: no link.
    links = cloud_sync.link_reviewed_key_pairs(
        obs_id, "another-observation",
        remote_images=[cloud_image], remote_measurements=[],
    )
    assert links == {"images": [], "spore_measurements": []}
    assert _row("images", image_id)["cloud_id"] is None


def test_whole_observation_take_cloud_links_key_paired_children(two_desktops, tmp_path):
    cloud, obs_id, image_id, measurement_id, cloud_image, cloud_measurement = (
        _lost_link_child_conflict(tmp_path)
    )
    cloud.requests.clear()
    cloud_sync.resolve_conflict_keep_cloud(cloud.client(), obs_id)
    assert str(_row("images", image_id)["cloud_id"]) == str(cloud_image["id"])
    assert str(_row("spore_measurements", measurement_id)["cloud_id"]) == str(cloud_measurement["id"])
    assert _row("spore_measurements", measurement_id)["length_um"] == 14.0
    assert [w for t in ("observation_images", "spore_measurements") for w in cloud.writes(t)] == []


def test_whole_observation_keep_device_patches_key_paired_children(two_desktops, tmp_path):
    cloud, obs_id, image_id, measurement_id, cloud_image, cloud_measurement = (
        _lost_link_child_conflict(tmp_path)
    )
    cloud.requests.clear()
    cloud_sync.resolve_conflict_keep_local(cloud.client(), obs_id, prepare_images_cb=_prepare_all)
    assert str(_row("images", image_id)["cloud_id"]) == str(cloud_image["id"])
    assert cloud.rows("spore_measurements")[0]["length_um"] == 12.0
    assert cloud.rows("observation_images")[0]["notes"] == "local note"
    assert [w for t in ("observation_images", "spore_measurements") for w in cloud.writes(t, "POST")] == []
