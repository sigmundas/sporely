from datetime import datetime, timedelta, timezone

import pytest

import utils.sync_shot_qr as sync_shot_qr
import utils.temporal_matcher as temporal_matcher
from utils.temporal_matcher import TemporalMatcher


def _local_naive(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=None)
    return value.astimezone().replace(tzinfo=None)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(
            datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc),
            _local_naive(datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)),
            id="aware-datetime",
        ),
        pytest.param(
            datetime(2026, 5, 1, 12, 0, 0),
            datetime(2026, 5, 1, 12, 0, 0),
            id="naive-datetime",
        ),
        pytest.param(
            "2026-05-01T12:00:00+02:00",
            _local_naive(datetime.fromisoformat("2026-05-01T12:00:00+02:00")),
            id="iso-offset",
        ),
        pytest.param(
            "2026-05-01T12:00:00Z",
            _local_naive(datetime.fromisoformat("2026-05-01T12:00:00+00:00")),
            id="iso-zulu",
        ),
        pytest.param(
            "2026-05-01 12:00:00",
            datetime(2026, 5, 1, 12, 0, 0),
            id="naive-string",
        ),
    ],
)
def test_parse_timestamp_normalizes_inputs_to_local_naive(value, expected):
    parsed = temporal_matcher._parse_timestamp(value)

    assert parsed is not None
    assert parsed.tzinfo is None
    assert parsed == expected


def test_load_observation_windows_handles_mixed_aware_and_naive_datetimes(monkeypatch):
    matcher = TemporalMatcher()
    monkeypatch.setattr(
        temporal_matcher.ObservationDB,
        "get_all_observations",
        lambda: [{"id": 1, "date": "2026-05-01T12:00:00+02:00"}],
    )
    monkeypatch.setattr(temporal_matcher.ImageDB, "get_images_for_observation", lambda _obs_id: [])

    windows = matcher.load_observation_windows(
        [{"captured_at": datetime(2026, 5, 1, 11, 0, 0)}],
        observation_id=1,
    )

    assert windows == []


def test_prepare_image_rows_normalizes_exif_capture_time(monkeypatch, tmp_path):
    matcher = TemporalMatcher()
    image_path = tmp_path / "capture.jpg"
    image_path.write_bytes(b"")
    exif_dt = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(temporal_matcher, "get_image_datetime", lambda _path: exif_dt)

    rows = matcher.prepare_image_rows([image_path])

    assert len(rows) == 1
    assert rows[0]["captured_at"] == _local_naive(exif_dt)
    assert rows[0]["captured_at"].tzinfo is None
    assert rows[0]["has_capture_time"] is True


def test_observation_window_backfill_normalizes_fallback_capture_time(monkeypatch, tmp_path):
    matcher = TemporalMatcher()
    image_path = tmp_path / "field.jpg"
    image_path.write_bytes(b"")
    fallback_dt = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=2)))
    image_rows = [
        {
            "id": 11,
            "filepath": str(image_path),
            "captured_at": None,
            "image_type": "field",
        }
    ]
    set_calls: list[tuple[int, datetime]] = []
    monkeypatch.setattr(temporal_matcher.ImageDB, "get_images_for_observation", lambda _obs_id: image_rows)
    monkeypatch.setattr(temporal_matcher, "get_image_datetime", lambda _path: fallback_dt)
    monkeypatch.setattr(
        temporal_matcher.ImageDB,
        "set_image_captured_at",
        lambda image_id, captured_at: set_calls.append((int(image_id), captured_at)),
    )

    window = matcher._observation_window_for_observation(1, datetime(2026, 5, 1, 12, 0, 0))

    assert window is not None
    assert window.start_at == _local_naive(fallback_dt)
    assert window.end_at == _local_naive(fallback_dt)
    assert window.start_at.tzinfo is None
    assert set_calls == [(11, _local_naive(fallback_dt))]


def test_match_images_against_sessions_normalizes_captured_and_adjusted_times():
    matcher = TemporalMatcher()
    result = matcher.match_images_against_sessions(
        [
            {
                "filepath": "/tmp/image.jpg",
                "captured_at": "2026-05-01T12:00:00+02:00",
            }
        ],
        sessions=[],
        observation_windows=[],
        offset_seconds=60,
    )

    assert result["matches"] == []
    assert len(result["unmatched"]) == 1
    row = result["unmatched"][0]
    expected_captured = _local_naive(datetime.fromisoformat("2026-05-01T12:00:00+02:00"))
    assert row["captured_at"] == expected_captured
    assert row["captured_at"].tzinfo is None
    assert row["adjusted_at"] == expected_captured + timedelta(seconds=60)
    assert row["adjusted_at"].tzinfo is None


def test_choose_sync_shot_offset_accepts_timezone_aware_captured_at():
    captured_at = datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=2)))
    qr_utc_dt = datetime(2026, 5, 1, 10, 5, 0, tzinfo=timezone.utc)

    result = sync_shot_qr.choose_sync_shot_offset(captured_at, qr_utc_dt)

    assert result["basis"] == "local"
    assert result["offset_seconds"] == pytest.approx(300.0)
    assert result["display_dt"] == qr_utc_dt.astimezone().replace(tzinfo=None)
    assert result["display_dt"].tzinfo is None
