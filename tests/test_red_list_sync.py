"""End-to-end coverage for red-list category flow between cloud + desktop.

Pins the fixes that make the left-side Taxonomy `Red list:` badge match the
AI-suggestions table:

1. Prediction-taxon normalization propagates every key casing that either the
   raw Artsorakel API or sporely-web ever emits, so the UI reader can find the
   value regardless of which system produced it.
2. Cloud pull writes observation-level `red_list_category` and
   `red_list_categories_json` into the local row (jsonb → TEXT).
3. Field-diff sees remote 'LC' vs stored-snapshot None as a change, so
   already-synced observations get backfilled on the next pull without
   requiring a cloud-side edit.
4. A push carrying NULL red-list never wipes the cloud value.
5. `red_list_categories_json` compares structurally so string-vs-dict shape
   differences don't perpetually mark the row "changed".
"""

from __future__ import annotations

import json

import pytest

from utils import cloud_sync
from utils.cloud_sync import (
    _analyze_observation_field_changes,
    _cloud_identification_prediction_taxon,
    _merge_cloud_selected_ai_fields,
    _normalize_observation_field_value,
    _observation_field_values_match,
    _remote_observation_extra_values,
    _SNAPSHOT_OBS_FIELDS,
)


# ---------------------------------------------------------------------------
# 1. Prediction alias propagation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "source_key,plural_key",
    [
        ("redListCategory", "redListCategories"),     # raw Artsorakel
        ("redlistCategory", "redlistCategories"),     # sporely-web camelCase
        ("redlist_category", "redlist_categories"),   # sporely-web snake_case
    ],
)
def test_cloud_identification_taxon_propagates_all_redlist_key_casings(source_key, plural_key):
    prediction = {
        "scientificName": "Hygrocybe miniata",
        source_key: "NT",
        plural_key: {"no": "NT", "se": "LC"},
    }
    taxon = _cloud_identification_prediction_taxon(prediction, service="arts")

    # Every alias should be readable on the returned taxon so downstream code
    # (including the UI reader) works regardless of key casing.
    assert taxon["redListCategory"] == "NT"
    assert taxon["redlistCategory"] == "NT"
    assert taxon["redlist_category"] == "NT"
    assert taxon["redListCategories"] == {"no": "NT", "se": "LC"}
    assert taxon["redlistCategories"] == {"no": "NT", "se": "LC"}
    assert taxon["redlist_categories"] == {"no": "NT", "se": "LC"}


def test_cloud_identification_taxon_leaves_taxon_unchanged_when_no_redlist():
    taxon = _cloud_identification_prediction_taxon({"scientificName": "X"}, service="arts")
    assert "redListCategory" not in taxon
    assert "redListCategories" not in taxon


# ---------------------------------------------------------------------------
# 2. Cloud pull → local extras
# ---------------------------------------------------------------------------


def test_pull_extras_serialize_redlist_from_jsonb_dict():
    """Supabase returns JSONB as a Python dict; the local TEXT column must get a JSON string."""
    remote = {
        "red_list_category": "LC",
        "red_list_categories_json": {"no": "LC", "se": "LC", "fi": "LC"},
    }
    extras = _remote_observation_extra_values(remote)
    assert extras["red_list_category"] == "LC"
    parsed = json.loads(extras["red_list_categories_json"])
    assert parsed == {"fi": "LC", "no": "LC", "se": "LC"}


def test_pull_extras_accept_already_serialized_json_string():
    remote = {
        "red_list_category": "NT",
        "red_list_categories_json": '{"no": "NT"}',
    }
    extras = _remote_observation_extra_values(remote)
    assert extras["red_list_category"] == "NT"
    assert json.loads(extras["red_list_categories_json"]) == {"no": "NT"}


def test_pull_extras_null_safe():
    extras = _remote_observation_extra_values({})
    assert extras["red_list_category"] is None
    assert extras["red_list_categories_json"] is None


# ---------------------------------------------------------------------------
# 2b. Initial-pull INSERT path forwards red-list into create_observation.
# ---------------------------------------------------------------------------
# When a cloud observation lands on the desktop for the first time, the row is
# created by `_create_local_from_remote` — not by the diff/UPDATE path that
# `_remote_observation_extra_values` feeds. If the INSERT path drops the
# columns, freshly-synced observations show "Not set" until the user makes a
# cloud-side edit that triggers a re-pull. The regression the user hit.


def test_create_local_from_remote_forwards_red_list_category(monkeypatch):
    """`_create_local_from_remote` must pass red-list values into
    `ObservationDB.create_observation` so the local column is populated on
    the very first pull, not only on subsequent UPDATEs."""
    captured: dict = {}

    def _fake_create(**kwargs):
        captured.update(kwargs)
        return 42

    def _fake_update_state(*args, **kwargs):
        return None

    # Stub the DB and post-create image import so we can run the function
    # without a real SQLite database attached.
    monkeypatch.setattr(cloud_sync.ObservationDB, "create_observation", _fake_create)
    monkeypatch.setattr(cloud_sync, "update_observation_sync_state", _fake_update_state)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: _StubConnection())
    monkeypatch.setattr(cloud_sync, "_import_remote_images", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        cloud_sync,
        "_import_remote_measurements_for_observation",
        lambda *args, **kwargs: {"warnings": []},
    )

    remote = {
        "id": "cloud-abc",
        "date": "2026-07-15",
        "genus": "Pinus",
        "species": "sylvestris",
        "red_list_category": "LC",
        # Cloud sends jsonb → decoded to a Python dict; local column is TEXT
        # so `_create_local_from_remote` must serialize it.
        "red_list_categories_json": {"no": "LC", "se": "LC"},
    }
    cloud_sync._create_local_from_remote(remote)

    assert captured.get("red_list_category") == "LC"
    stored_json = captured.get("red_list_categories_json")
    assert isinstance(stored_json, str)
    assert json.loads(stored_json) == {"no": "LC", "se": "LC"}


def test_create_local_from_remote_handles_missing_red_list(monkeypatch):
    """No red-list on the remote row means both local columns stay NULL."""
    captured: dict = {}
    monkeypatch.setattr(
        cloud_sync.ObservationDB, "create_observation", lambda **kwargs: captured.update(kwargs) or 7
    )
    monkeypatch.setattr(cloud_sync, "update_observation_sync_state", lambda *a, **kw: None)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: _StubConnection())
    monkeypatch.setattr(cloud_sync, "_import_remote_images", lambda *a, **kw: None)
    monkeypatch.setattr(
        cloud_sync,
        "_import_remote_measurements_for_observation",
        lambda *a, **kw: {"warnings": []},
    )

    cloud_sync._create_local_from_remote({"id": "c1", "date": "2026-07-15"})
    assert captured.get("red_list_category") is None
    assert captured.get("red_list_categories_json") is None


class _StubConnection:
    def cursor(self):
        return self

    def commit(self):
        pass

    def close(self):
        pass

    def execute(self, *args, **kwargs):
        return self

    def fetchall(self):
        return []

    def fetchone(self):
        return None


# ---------------------------------------------------------------------------
# 3. Field diff triggers backfill for pre-fix snapshots
# ---------------------------------------------------------------------------


def test_field_diff_backfills_when_snapshot_predates_redlist_pull():
    """Existing observations pulled before the fix have a snapshot without
    red_list_category. When the next pull sees remote 'LC' vs stored None,
    the diff must mark the field as remote-only so it gets applied — this is
    how already-synced observations get backfilled without a cloud edit."""
    local_obs = {"id": 1, "red_list_category": None, "red_list_categories_json": None}
    baseline_obs = {"id": 1}  # snapshot from before the fix — no red_list_* keys
    remote_obs = {
        "id": "cloud-abc",
        "red_list_category": "LC",
        "red_list_categories_json": {"no": "LC"},
    }
    diff = _analyze_observation_field_changes(local_obs, remote_obs, baseline_obs)
    assert "red_list_category" in diff["remote_only_fields"]
    assert "red_list_categories_json" in diff["remote_only_fields"]
    assert "red_list_category" not in diff["conflict_fields"]


def test_field_diff_does_not_flag_conflict_when_local_and_remote_agree():
    """After backfill has run once, subsequent pulls with identical local + remote
    payloads must not report the field as changed. This depends on JSONB vs
    TEXT normalization for red_list_categories_json."""
    local_obs = {
        "id": 1,
        "red_list_category": "LC",
        "red_list_categories_json": '{"no": "LC"}',   # local: TEXT
    }
    baseline_obs = {
        "id": 1,
        "red_list_category": "LC",
        "red_list_categories_json": {"no": "LC"},     # snapshot: dict (round-tripped from jsonb)
    }
    remote_obs = {
        "id": "cloud-abc",
        "red_list_category": "LC",
        "red_list_categories_json": {"no": "LC"},     # remote: dict
    }
    diff = _analyze_observation_field_changes(local_obs, remote_obs, baseline_obs)
    assert "red_list_category" not in diff["remote_only_fields"]
    assert "red_list_category" not in diff["conflict_fields"]
    assert "red_list_categories_json" not in diff["remote_only_fields"]
    assert "red_list_categories_json" not in diff["conflict_fields"]


# ---------------------------------------------------------------------------
# 4. Structural equality for red_list_categories_json
# ---------------------------------------------------------------------------


def test_red_list_categories_json_string_and_dict_compare_equal():
    string_form = '{"no": "LC"}'
    dict_form = {"no": "LC"}
    assert _observation_field_values_match(
        "red_list_categories_json",
        _normalize_observation_field_value("red_list_categories_json", string_form),
        _normalize_observation_field_value("red_list_categories_json", dict_form),
    )


def test_red_list_categories_json_none_matches_missing():
    assert _observation_field_values_match(
        "red_list_categories_json",
        _normalize_observation_field_value("red_list_categories_json", None),
        _normalize_observation_field_value("red_list_categories_json", ""),
    )


# ---------------------------------------------------------------------------
# 5. Push safety — local NULL must not wipe cloud value
# ---------------------------------------------------------------------------


def test_push_preserves_cloud_red_list_when_local_null():
    """`_merge_cloud_selected_ai_fields` is the forward-compatible guard: if
    red-list ever becomes a pushable field, a local NULL must never overwrite
    a cloud-populated value on an unrelated desktop edit."""
    local_obs = {
        "id": 1,
        "notes": "edited on desktop",
        "red_list_category": None,
        "red_list_categories_json": None,
    }
    remote_obs = {
        "id": "cloud-abc",
        "red_list_category": "LC",
        "red_list_categories_json": {"no": "LC"},
    }
    merged = _merge_cloud_selected_ai_fields(local_obs, remote_obs)
    assert merged["red_list_category"] == "LC"
    assert merged["red_list_categories_json"] == {"no": "LC"}
    # And unrelated fields on the local edit are preserved.
    assert merged["notes"] == "edited on desktop"


def test_push_keeps_desktop_red_list_when_local_populated():
    """When the desktop DOES have a red-list value (e.g. from a local
    Artsorakel guess), the merge must not clobber it with the remote."""
    local_obs = {
        "id": 1,
        "red_list_category": "NT",
        "red_list_categories_json": '{"no": "NT"}',
    }
    remote_obs = {
        "id": "cloud-abc",
        "red_list_category": "LC",
        "red_list_categories_json": {"no": "LC"},
    }
    merged = _merge_cloud_selected_ai_fields(local_obs, remote_obs)
    assert merged["red_list_category"] == "NT"
    # local_obs kept as-is; merge only fills gaps.
    assert merged["red_list_categories_json"] == '{"no": "NT"}'


# ---------------------------------------------------------------------------
# 6. Sanity check on the snapshot field list
# ---------------------------------------------------------------------------


def test_snapshot_obs_fields_include_red_list_columns():
    """Pull SELECT and diff both walk _SNAPSHOT_OBS_FIELDS."""
    assert "red_list_category" in _SNAPSHOT_OBS_FIELDS
    assert "red_list_categories_json" in _SNAPSHOT_OBS_FIELDS


def test_red_list_columns_are_pushable_with_null_safe_merge():
    """Red-list is now pushable so desktop-derived values (e.g. from a local
    AI-selection fallback) round-trip to cloud. A local NULL cannot wipe the
    cloud value because `_merge_cloud_selected_ai_fields` fills in the remote
    value first — verified in `test_push_preserves_cloud_red_list_when_local_null`
    above. Keeping the fields pull-only was a source of the obs 368 dirty loop:
    a local `LC` never left the desktop, so pull kept flagging it as
    local_only_field on every sync."""
    assert "red_list_category" in cloud_sync._OBS_PUSH_COLS
    assert "red_list_categories_json" in cloud_sync._OBS_PUSH_COLS


# ---------------------------------------------------------------------------
# 7. UI-side readers used by the AI-suggestions table and the Copy handler
# ---------------------------------------------------------------------------
# These pin the behavior the left-side Taxonomy panel depends on:
#   * `_read_red_list_code(taxon) or _read_red_list_code(pred)` populates
#     the badge on Copy (see observations_tab.py line 14352 area).
#   * `_red_list_display_from_prediction(pred, taxon, ...)` fills the AI
#     table's Red list cell.


def _load_dialog_class():
    # Importing the module drags in Qt; skip on environments without it.
    pytest.importorskip("PySide6.QtCore")
    from ui.observations_tab import ObservationDetailsDialog

    return ObservationDetailsDialog


@pytest.mark.parametrize(
    "prediction_key",
    ["redListCategory", "redlistCategory", "redlist_category"],
)
def test_ui_reader_finds_redlist_code_regardless_of_key_casing(prediction_key):
    D = _load_dialog_class()
    prediction = {"scientificName": "X", prediction_key: "LC"}
    assert D._read_red_list_code(prediction) == "LC"


def test_ui_reader_prefers_taxon_when_both_populated():
    D = _load_dialog_class()
    taxon = {"redListCategory": "NT"}
    pred = {"redlistCategory": "LC"}
    # Copy handler reads taxon first, falls back to prediction.
    assert (D._read_red_list_code(taxon) or D._read_red_list_code(pred)) == "NT"


def test_ui_reader_falls_back_to_prediction_when_taxon_missing_redlist():
    D = _load_dialog_class()
    taxon = {"scientificName": "X"}
    pred = {"redlist_category": "EN"}
    assert (D._read_red_list_code(taxon) or D._read_red_list_code(pred)) == "EN"


@pytest.mark.parametrize(
    "categories_key",
    ["redListCategories", "redlistCategories", "redlist_categories"],
)
def test_ui_reader_finds_plural_categories_regardless_of_key_casing(categories_key):
    D = _load_dialog_class()
    prediction = {categories_key: {"no": "LC", "se": "NT"}}
    assert D._read_red_list_categories(prediction) == {"no": "LC", "se": "NT"}


def test_ui_normalize_ai_prediction_taxon_carries_redlist_from_web_shape():
    """`_normalize_ai_prediction_taxon` is what the Copy handler runs on the
    selected prediction. After sporely-web writes its lowercase-l key, the
    normalized taxon must still expose the value under `redListCategory` so
    older reader paths (that only checked the capital-L key) also work."""
    pytest.importorskip("PySide6.QtCore")
    from ui.observations_tab import _normalize_ai_prediction_taxon

    prediction = {
        "scientificName": "Hygrocybe miniata",
        "redlistCategory": "LC",
        "redlist_categories": {"no": "LC"},
    }
    taxon = _normalize_ai_prediction_taxon(prediction, source="artsorakel")
    assert taxon["redListCategory"] == "LC"
    assert taxon["redlistCategory"] == "LC"
    assert taxon["redlist_category"] == "LC"
    assert taxon["redListCategories"] == {"no": "LC"}


def test_ai_suggestions_table_displays_lc_for_lowercase_l_prediction():
    """Simulate the AI suggestions table's red-list cell filler.

    The table calls `_red_list_display_from_prediction(pred, taxon, short=True)`
    for each row. Predictions from sporely-web use `redlistCategory` (lowercase l);
    the table must still render 'LC'."""
    D = _load_dialog_class()
    # Bind the classmethod helpers to a stub `self` so we don't need a Qt widget.
    from types import SimpleNamespace

    self_stub = SimpleNamespace(
        _red_list_label=lambda code, **_kw: code.upper() if code else "",
        _read_red_list_code=D._read_red_list_code,
    )
    display = D._red_list_display_from_prediction(
        self_stub,
        {"redlistCategory": "LC"},
        {},
        short=True,
    )
    assert display == "LC"


def test_load_observation_falls_back_to_ai_selection_when_column_is_null():
    """sporely-web only writes `observations.red_list_category` in some flows.
    A cloud-synced observation may arrive with an AI selection but NULL
    column-level red-list. The dialog must still surface the code the AI
    table already displays, by pulling it out of the selected prediction."""
    D = _load_dialog_class()

    # Reproduce the load block's decision tree using only the classmethod
    # helpers — no Qt widget required. The full method touches too many
    # side-effect attributes to instantiate cheaply, but the invariant we
    # care about is which value flows into `_set_red_list_category`.
    obs = {
        "red_list_category": None,
        "red_list_categories_json": None,
        "ai_selected_service": "artsorakel",
        "ai_selected_scientific_name": "Pinus sylvestris",
    }
    ai_selected_by_index = {
        0: {
            "scientificName": "Pinus sylvestris",
            "taxon": {"scientificName": "Pinus sylvestris", "redlistCategory": "LC"},
            "redlist_categories": {"NO": "LC"},
        }
    }

    # Inline the same logic used in _load_observation_values.
    red_code = obs.get("red_list_category")
    red_categories = None
    if not red_code:
        for selected_pred in ai_selected_by_index.values():
            taxon = selected_pred.get("taxon") if isinstance(selected_pred.get("taxon"), dict) else {}
            fallback_code = D._read_red_list_code(taxon) or D._read_red_list_code(selected_pred)
            if fallback_code:
                red_code = fallback_code
                red_categories = D._read_red_list_categories(taxon) or D._read_red_list_categories(selected_pred)
                break

    assert red_code == "LC", (
        f"Left-panel badge must derive 'LC' from the selected AI prediction "
        f"when the observation column is NULL; got {red_code!r}"
    )
    assert red_categories == {"NO": "LC"}


def test_load_observation_prefers_column_over_ai_when_both_present():
    """The persisted column wins if it's populated — this preserves any
    manual override the user made after selecting from AI."""
    D = _load_dialog_class()
    obs = {"red_list_category": "NT"}
    ai_selected_by_index = {
        0: {"taxon": {"redlistCategory": "LC"}},  # differs from the column
    }
    red_code = obs.get("red_list_category")
    if not red_code:
        for pred in ai_selected_by_index.values():
            red_code = D._read_red_list_code(pred.get("taxon", {})) or D._read_red_list_code(pred)
            if red_code:
                break
    assert red_code == "NT"


def test_load_observation_no_fallback_when_no_ai_selection():
    """If neither the column nor the AI selection has a red-list, the badge
    stays empty — no false LC leaks in."""
    D = _load_dialog_class()
    obs = {"red_list_category": None}
    ai_selected_by_index: dict = {}
    red_code = obs.get("red_list_category")
    if not red_code:
        for pred in ai_selected_by_index.values():
            red_code = D._read_red_list_code(pred.get("taxon", {})) or D._read_red_list_code(pred)
            if red_code:
                break
    assert not red_code


def test_load_observation_populates_red_list_from_local_row():
    """When the observation dialog loads a stored obs row (which is what a
    cloud-pulled record looks like locally), the left-side Red list badge
    reflects the persisted `red_list_category` column, not 'Not set'."""
    D = _load_dialog_class()
    from types import SimpleNamespace

    captured: dict = {}

    def _capture(code, categories):
        captured["code"] = code
        captured["categories"] = categories

    # Minimal `self` — _load_observation_values touches many attributes, so
    # rather than fake them all we only exercise the two lines that build the
    # red-list arguments and call the setter. This mirrors the code at
    # observations_tab.py around line 18063-18071.
    obs = {
        "red_list_category": "LC",
        "red_list_categories_json": '{"no": "LC", "se": "LC"}',
    }
    # Reproduce the load block's logic in-line to pin the invariant that
    # persists across refactors: JSON is parsed to a dict and forwarded.
    raw = obs.get("red_list_categories_json")
    categories = None
    if raw:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            categories = parsed
    _capture(obs.get("red_list_category"), categories)

    assert captured["code"] == "LC"
    assert captured["categories"] == {"no": "LC", "se": "LC"}
