"""Tests for structured spore_statistics cloud sync (Stage 4)."""

import json
import sqlite3
import types

import pytest

from utils import cloud_sync
from utils.spore_stats import build_structured_spore_statistics, serialise_spore_statistics


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_stats(*, with_widths: bool = True) -> dict:
    """Return a realistic measurement stats dict mirroring MeasurementDB output."""
    stats = {
        "count": 18,
        "length_mean": 13.5,
        "length_std": 1.2,
        "length_min": 11.0,
        "length_max": 15.8,
        "length_p5": 11.5,
        "length_p95": 15.2,
    }
    if with_widths:
        stats.update({
            "width_mean": 5.0,
            "width_std": 0.8,
            "width_min": 4.0,
            "width_max": 6.2,
            "width_p5": 4.2,
            "width_p95": 6.0,
            "ratio_mean": 2.7,
            "ratio_min": 1.8,
            "ratio_max": 3.0,
            "ratio_p5": 1.9,
            "ratio_p95": 2.9,
        })
    return stats


# ---------------------------------------------------------------------------
# Unit tests for build_structured_spore_statistics
# ---------------------------------------------------------------------------

class TestBuildStructuredSporeStatistics:
    def test_full_stats_keys_present(self):
        result = build_structured_spore_statistics(_make_stats(with_widths=True))
        assert result is not None
        for key in (
            "version", "n",
            "length_min_um", "length_max_um", "length_core_min_um", "length_core_max_um", "length_mean_um",
            "width_min_um", "width_max_um", "width_core_min_um", "width_core_max_um", "width_mean_um",
            "q_min", "q_max", "q_mean",
            "rendered", "method", "method_version",
        ):
            assert key in result, f"missing key: {key}"

    def test_full_stats_numeric_types(self):
        result = build_structured_spore_statistics(_make_stats(with_widths=True))
        assert isinstance(result["version"], int)
        assert isinstance(result["n"], int)
        for key in (
            "length_min_um", "length_max_um", "length_core_min_um", "length_core_max_um", "length_mean_um",
            "width_min_um", "width_max_um", "width_core_min_um", "width_core_max_um", "width_mean_um",
            "q_min", "q_max", "q_mean",
        ):
            assert isinstance(result[key], float), f"{key} should be float, got {type(result[key])}"

    def test_full_stats_correct_values(self):
        stats = _make_stats(with_widths=True)
        result = build_structured_spore_statistics(stats)
        assert result["version"] == 1
        assert result["n"] == 18
        assert result["length_min_um"] == round(float(stats["length_min"]), 3)
        assert result["length_max_um"] == round(float(stats["length_max"]), 3)
        assert result["length_core_min_um"] == round(float(stats["length_p5"]), 3)
        assert result["length_core_max_um"] == round(float(stats["length_p95"]), 3)
        assert result["method"] == "sporely-py"
        assert result["method_version"] == "1"

    def test_no_widths_omits_width_and_q_keys(self):
        result = build_structured_spore_statistics(_make_stats(with_widths=False))
        assert result is not None
        for key in ("width_min_um", "width_max_um", "width_core_min_um", "width_core_max_um", "width_mean_um",
                    "q_min", "q_max", "q_mean"):
            assert key not in result, f"unexpected key when no widths: {key}"

    def test_no_widths_length_keys_present(self):
        result = build_structured_spore_statistics(_make_stats(with_widths=False))
        for key in ("length_min_um", "length_max_um", "length_core_min_um", "length_core_max_um", "length_mean_um"):
            assert key in result

    def test_rendered_is_string_with_spores_prefix(self):
        result = build_structured_spore_statistics(_make_stats(with_widths=True))
        assert isinstance(result["rendered"], str)
        assert result["rendered"].startswith("Spores:")

    def test_rendered_contains_n_count(self):
        result = build_structured_spore_statistics(_make_stats(with_widths=True))
        assert "n = 18" in result["rendered"]

    def test_empty_stats_returns_none(self):
        assert build_structured_spore_statistics({}) is None

    def test_none_stats_returns_none(self):
        assert build_structured_spore_statistics(None) is None

    def test_zero_count_returns_none(self):
        stats = _make_stats(with_widths=False)
        stats["count"] = 0
        assert build_structured_spore_statistics(stats) is None

    def test_result_is_json_serialisable(self):
        result = build_structured_spore_statistics(_make_stats(with_widths=True))
        encoded = json.dumps(result, sort_keys=True, ensure_ascii=False)
        decoded = json.loads(encoded)
        assert decoded["n"] == 18
        assert decoded["method"] == "sporely-py"

    def test_numpy_scalars_survive_json_serialisation(self):
        """Numpy floats/ints must not cause json.dumps to raise."""
        import numpy as np
        stats = _make_stats(with_widths=True)
        np_stats = {k: np.float64(v) if isinstance(v, float) else v for k, v in stats.items()}
        np_stats["count"] = np.int64(stats["count"])
        result = build_structured_spore_statistics(np_stats)
        # Must not raise:
        json.dumps(result, sort_keys=True, ensure_ascii=False)


class TestSerialiseSporeStatistics:
    def test_serialise_returns_json_string(self):
        result = serialise_spore_statistics(_make_stats(with_widths=True))
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert parsed["n"] == 18

    def test_serialise_empty_returns_none(self):
        assert serialise_spore_statistics({}) is None

    def test_serialise_none_returns_none(self):
        assert serialise_spore_statistics(None) is None

    def test_serialise_is_valid_json_for_normalize_json_value(self):
        """The serialised string round-trips through cloud_sync's JSON normaliser."""
        result = serialise_spore_statistics(_make_stats(with_widths=True))
        normalised = cloud_sync._normalize_observation_json_value(result)
        assert isinstance(normalised, dict)
        assert normalised["n"] == 18


# ---------------------------------------------------------------------------
# Cloud payload integration
# ---------------------------------------------------------------------------

def _init_db(tmp_path) -> str:
    db_path = str(tmp_path / "sporely.db")
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            spore_statistics TEXT,
            cloud_id TEXT,
            sync_status TEXT,
            synced_at TEXT
        );
        """
    )
    conn.commit()
    conn.close()
    return db_path


def test_cloud_payload_spore_statistics_is_dict_from_json_string(monkeypatch, tmp_path):
    """_observation_push_payload emits spore_statistics as a dict when stored value is JSON."""
    db_path = _init_db(tmp_path)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))

    json_string = serialise_spore_statistics(_make_stats(with_widths=True))
    row = {
        "date": "2026-06-01",
        "spore_statistics": json_string,
        "cloud_id": None,
        "sharing_scope": "public",
        "is_draft": False,
    }
    payload = cloud_sync._observation_push_payload(row, local=True)
    spore = payload["spore_statistics"]

    assert isinstance(spore, dict), f"expected dict, got {type(spore)}: {spore!r}"
    assert spore["n"] == 18
    assert isinstance(spore["n"], int)
    assert isinstance(spore["length_min_um"], float)
    assert spore["method"] == "sporely-py"
    assert "rendered" in spore


def test_cloud_payload_spore_statistics_none_for_empty(monkeypatch, tmp_path):
    """When spore_statistics is None in the local row, the cloud payload also carries None."""
    db_path = _init_db(tmp_path)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))

    row = {
        "date": "2026-06-01",
        "spore_statistics": None,
        "cloud_id": None,
        "sharing_scope": "public",
        "is_draft": False,
    }
    payload = cloud_sync._observation_push_payload(row, local=True)
    assert payload["spore_statistics"] is None


def test_cloud_payload_preserves_legacy_string(monkeypatch, tmp_path):
    """Existing legacy string values in the DB pass through cloud_sync unchanged."""
    db_path = _init_db(tmp_path)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))

    legacy = "Spores: 12.0-15.0 x 4.0-5.0 um  n = 18"
    row = {
        "date": "2026-06-01",
        "spore_statistics": legacy,
        "cloud_id": None,
        "sharing_scope": "public",
        "is_draft": False,
    }
    payload = cloud_sync._observation_push_payload(row, local=True)
    assert payload["spore_statistics"] == legacy


# ---------------------------------------------------------------------------
# _spore_count_from_value — structured JSON compatibility
# ---------------------------------------------------------------------------

def test_spore_count_from_value_reads_n_key():
    from ui.observations_tab import _spore_count_from_value  # noqa: PLC0415
    structured = {"version": 1, "n": 42, "rendered": "Spores: ..., n = 42"}
    assert _spore_count_from_value(structured) == "42"


def test_spore_count_from_value_reads_n_from_json_string():
    from ui.observations_tab import _spore_count_from_value  # noqa: PLC0415
    json_string = json.dumps({"version": 1, "n": 74, "rendered": "Spores: ..., n = 74"})
    assert _spore_count_from_value(json_string) == "74"


def test_spore_count_from_value_reads_legacy_string():
    from ui.observations_tab import _spore_count_from_value  # noqa: PLC0415
    legacy = "Spores: 12.0-15.0 x 4.0-5.0 um  n = 18"
    assert _spore_count_from_value(legacy) == "18"


# ---------------------------------------------------------------------------
# _localize_spore_stats_for_publish — structured JSON compatibility
# ---------------------------------------------------------------------------

def _make_publish_stub(prefers_norwegian: bool = False):
    from ui import observations_tab  # noqa: PLC0415
    stub = types.SimpleNamespace()
    stub._publish_prefers_norwegian_labels = lambda: prefers_norwegian
    stub._localize_spore_stats_for_publish = (
        observations_tab.ObservationsTab._localize_spore_stats_for_publish.__get__(stub)
    )
    return stub


def test_localize_for_publish_extracts_rendered_from_json_string():
    stub = _make_publish_stub(prefers_norwegian=False)
    structured = {
        "version": 1,
        "n": 18,
        "rendered": "Spores: (11.0-)11.5-15.2(-15.8) um, n = 18",
    }
    result = stub._localize_spore_stats_for_publish(json.dumps(structured))
    assert result == "Spores: (11.0-)11.5-15.2(-15.8) um, n = 18"


def test_localize_for_publish_extracts_rendered_from_dict():
    stub = _make_publish_stub(prefers_norwegian=False)
    structured = {
        "version": 1,
        "n": 18,
        "rendered": "Spores: (11.0-)11.5-15.2(-15.8) um, n = 18",
    }
    result = stub._localize_spore_stats_for_publish(structured)
    assert result == "Spores: (11.0-)11.5-15.2(-15.8) um, n = 18"


def test_localize_for_publish_norwegian_rewrites_spores_label():
    stub = _make_publish_stub(prefers_norwegian=True)
    structured = {
        "version": 1,
        "n": 18,
        "rendered": "Spores: (11.0-)11.5-15.2(-15.8) um, n = 18",
    }
    result = stub._localize_spore_stats_for_publish(structured)
    assert result.startswith("Sporer:")
    assert "(11.0-)11.5-15.2(-15.8) um" in result


def test_localize_for_publish_legacy_string_unchanged():
    stub = _make_publish_stub(prefers_norwegian=False)
    legacy = "Spores: (11.0-)11.5-15.2(-15.8) um  n = 18"
    result = stub._localize_spore_stats_for_publish(legacy)
    assert result == legacy


def test_localize_for_publish_none_returns_empty():
    stub = _make_publish_stub(prefers_norwegian=False)
    assert stub._localize_spore_stats_for_publish(None) == ""


# ---------------------------------------------------------------------------
# _format_spore_stats_short — structured JSON compatibility
# ---------------------------------------------------------------------------

def _make_tab_stub():
    from ui import observations_tab  # noqa: PLC0415
    stub = types.SimpleNamespace()
    stub._format_spore_stats_short = observations_tab.ObservationsTab._format_spore_stats_short.__get__(stub)
    stub._format_spore_stats_short_from_values = staticmethod(
        observations_tab.ObservationsTab._format_spore_stats_short_from_values
    )
    return stub


def test_format_spore_stats_short_from_json_string_returns_summary():
    """JSON string with rendered field: helper extracts rendered and returns a short summary."""
    stub = _make_tab_stub()
    structured = {
        "version": 1,
        "n": 18,
        "length_core_min_um": 11.5,
        "length_core_max_um": 15.2,
        "width_core_min_um": 4.2,
        "width_core_max_um": 6.0,
        "rendered": (
            "Spores: (11.0-)11.5-15.2(-15.8) um x (4.0-)4.2-6.0(-6.2) um,"
            " Q = (1.8-)2.0-2.5(-3.0), Qm = 2.3, n = 18"
        ),
    }
    result = stub._format_spore_stats_short(json.dumps(structured))
    assert result is not None
    assert "11.5" in result
    assert "4.2" in result
    assert "18" in result


def test_format_spore_stats_short_from_dict_returns_summary():
    """Dict with rendered field: helper extracts rendered and returns a short summary."""
    stub = _make_tab_stub()
    structured = {
        "version": 1,
        "n": 18,
        "length_core_min_um": 11.5,
        "length_core_max_um": 15.2,
        "width_core_min_um": 4.2,
        "width_core_max_um": 6.0,
        "rendered": (
            "Spores: (11.0-)11.5-15.2(-15.8) um x (4.0-)4.2-6.0(-6.2) um,"
            " Q = (1.8-)2.0-2.5(-3.0), Qm = 2.3, n = 18"
        ),
    }
    result = stub._format_spore_stats_short(structured)
    assert result is not None
    assert "11.5" in result
    assert "4.2" in result
    assert "18" in result


def test_format_spore_stats_short_does_not_raise_on_legacy():
    stub = _make_tab_stub()
    legacy = "Spores: 12.0-15.0 x 4.0-5.0 um  n = 18"
    # Does not raise; return value depends on existing regex.
    stub._format_spore_stats_short(legacy)


def test_format_spore_stats_short_none_returns_none():
    stub = _make_tab_stub()
    assert stub._format_spore_stats_short(None) is None


def test_format_spore_stats_short_from_dict_length_only():
    """Length-only structured JSON (no width/q fields) returns a non-None short summary."""
    stub = _make_tab_stub()
    length_only = {
        "version": 1,
        "n": 18,
        "length_core_min_um": 11.5,
        "length_core_max_um": 15.2,
        "length_mean_um": 13.5,
        "rendered": "Spores: (11.0-)11.5-15.2(-15.8) um, n = 18",
        "method": "sporely-py",
        "method_version": "1",
    }
    result = stub._format_spore_stats_short(length_only)
    assert result is not None
    assert "11.5" in result
    assert "15.2" in result
    assert "18" in result


def test_format_spore_stats_short_from_values_length_only():
    """_format_spore_stats_short_from_values returns a summary even without width fields."""
    from ui.observations_tab import ObservationsTab
    result = ObservationsTab._format_spore_stats_short_from_values({
        "length_p5": 11.5,
        "length_p95": 15.2,
        "count": 18,
    })
    assert result is not None
    assert "11.5" in result
    assert "15.2" in result
    assert "18" in result


# ---------------------------------------------------------------------------
# Mixed-format sync behaviour (known limitation documentation)
# ---------------------------------------------------------------------------

def test_push_payload_legacy_string_and_structured_json_are_not_semantically_equal(monkeypatch, tmp_path):
    """Document known limitation: legacy string and structured JSON compare unequal in push payload.

    In normal usage this cannot occur (the local value is updated to JSON before any dirty push).
    In a rollback scenario the local value could revert to a legacy string while the cloud
    retains the structured JSON.  cloud_sync.py is explicitly out of scope for this feature pass;
    the risk is limited to deliberate app rollbacks and legacy strings remain valid in cloud.
    """
    db_path = _init_db(tmp_path)
    monkeypatch.setattr(cloud_sync, "get_connection", lambda: sqlite3.connect(db_path))

    legacy = "Spores: (11.0-)11.5-15.2(-15.8) um x (4.0-)4.2-6.0(-6.2) um, Q = (1.8-)2.0-2.5(-3.0), Qm = 2.3, n = 18"
    structured_json = json.dumps({
        "version": 1,
        "n": 18,
        "length_min_um": 11.0,
        "length_max_um": 15.8,
        "length_core_min_um": 11.5,
        "length_core_max_um": 15.2,
        "length_mean_um": 13.5,
        "width_min_um": 4.0,
        "width_max_um": 6.2,
        "width_core_min_um": 4.2,
        "width_core_max_um": 6.0,
        "width_mean_um": 5.0,
        "q_min": 1.8,
        "q_max": 3.0,
        "q_mean": 2.7,
        "rendered": legacy,
        "method": "sporely-py",
        "method_version": "1",
    }, sort_keys=True)

    local_row = {
        "date": "2026-06-01",
        "spore_statistics": legacy,  # old local value (rollback scenario)
        "cloud_id": None,
        "sharing_scope": "public",
        "is_draft": False,
    }
    remote_row = {
        "date": "2026-06-01",
        "spore_statistics": json.loads(structured_json),  # structured JSON from cloud
        "cloud_id": None,
        "sharing_scope": "public",
        "is_draft": False,
    }

    local_payload = cloud_sync._observation_push_payload(local_row, local=True)
    remote_payload = cloud_sync._observation_push_payload(remote_row, local=False)

    # Document: the payloads are NOT equal (known limitation; cloud_sync.py excluded from scope).
    # A fix would require _observation_field_values_match to check rendered == legacy,
    # which is deferred to a future pass.
    local_spore = local_payload["spore_statistics"]
    remote_spore = remote_payload["spore_statistics"]
    assert isinstance(local_spore, str), "local legacy string stays as string in payload"
    assert isinstance(remote_spore, dict), "remote structured JSON stays as dict in payload"
    assert local_spore != remote_spore, (
        "KNOWN LIMITATION: legacy string and structured JSON compare as unequal — "
        "a rollback to an old desktop version can overwrite the cloud's structured value"
    )
