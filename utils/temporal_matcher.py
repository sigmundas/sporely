"""Session-log driven image matching for the Ingestion Hub."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

from database.models import ImageDB, ObservationDB, SessionLogDB
from utils.exif_reader import get_image_datetime


SUPPORTED_IMAGE_EXTENSIONS = {
    ".png",
    ".jpg",
    ".jpeg",
    ".tif",
    ".tiff",
    ".heic",
    ".heif",
    ".orf",
    ".nef",
}


def _normalize_path(path: str | Path | None) -> str:
    if not path:
        return ""
    try:
        return str(Path(path).expanduser().resolve())
    except Exception:
        return str(path)


def _parse_timestamp(value) -> datetime | None:
    if isinstance(value, datetime):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M",
    ):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text)
    except Exception:
        return None


@dataclass
class TimelineSession:
    session_id: str
    observation_id: int
    session_kind: str
    events: list[dict] = field(default_factory=list)
    state_events: list[dict] = field(default_factory=list)
    note_events: list[dict] = field(default_factory=list)
    started_at: datetime | None = None
    ended_at: datetime | None = None
    last_recorded_at: datetime | None = None

    def contains(self, timestamp: datetime, *, grace_seconds: float = 0.0) -> bool:
        if timestamp is None:
            return False
        grace = timedelta(seconds=max(0.0, float(grace_seconds)))
        start = self.started_at or self.last_recorded_at
        end = self.ended_at or self.last_recorded_at
        if start is None and end is None:
            return False
        if start is None:
            start = end
        if end is None:
            end = start
        return (start - grace) <= timestamp <= (end + grace)

    def distance_seconds(self, timestamp: datetime) -> float:
        if timestamp is None:
            return float("inf")
        start = self.started_at or self.last_recorded_at
        end = self.ended_at or self.last_recorded_at
        if start is None and end is None:
            return float("inf")
        if start is None:
            start = end
        if end is None:
            end = start
        if start <= timestamp <= end:
            return 0.0
        if timestamp < start:
            return (start - timestamp).total_seconds()
        return (timestamp - end).total_seconds()

    def state_at(self, timestamp: datetime | None) -> dict:
        if timestamp is None:
            return {}
        state: dict[str, str] = {}
        for row in self.state_events:
            row_dt = row.get("_recorded_dt")
            if row_dt is None or row_dt > timestamp:
                break
            attribute_name = str(row.get("attribute_name") or "").strip()
            if not attribute_name:
                continue
            state[attribute_name] = str(row.get("value") or "").strip() or None
        return {key: value for key, value in state.items() if value}

    def to_summary(self) -> dict:
        return {
            "session_id": self.session_id,
            "observation_id": self.observation_id,
            "session_kind": self.session_kind,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "last_recorded_at": self.last_recorded_at,
            "event_count": len(self.events),
            "note_count": len(self.note_events),
        }


@dataclass
class ObservationWindow:
    observation_id: int
    start_at: datetime
    end_at: datetime
    source: str = "observation"
    observation_date: datetime | None = None

    def contains(self, timestamp: datetime, *, grace_seconds: float = 0.0) -> bool:
        grace = timedelta(seconds=max(0.0, float(grace_seconds)))
        return (self.start_at - grace) <= timestamp <= (self.end_at + grace)

    def distance_seconds(self, timestamp: datetime) -> float:
        if self.start_at <= timestamp <= self.end_at:
            return 0.0
        if timestamp < self.start_at:
            return (self.start_at - timestamp).total_seconds()
        return (timestamp - self.end_at).total_seconds()


class TemporalMatcher:
    """Match image capture times to retrospective Live Lab session logs."""

    def __init__(
        self,
        *,
        session_kind: str = "offline",
        session_grace_seconds: float = 180.0,
    ) -> None:
        self.session_kind = str(session_kind or "").strip().lower() or "offline"
        self.session_grace_seconds = float(session_grace_seconds)
        self._observation_window_cache: dict[int, ObservationWindow | None] = {}

    def load_sessions(self, observation_id: int | None = None) -> list[TimelineSession]:
        events = SessionLogDB.get_events(
            observation_id=observation_id,
            session_kind=self.session_kind if self.session_kind else None,
        )
        grouped: dict[str, list[dict]] = {}
        for row in events:
            session_id = str(row.get("session_id") or "").strip()
            if not session_id:
                continue
            row_copy = dict(row)
            row_copy["_recorded_dt"] = _parse_timestamp(row.get("recorded_at"))
            grouped.setdefault(session_id, []).append(row_copy)

        sessions: list[TimelineSession] = []
        for session_id, rows in grouped.items():
            rows.sort(key=lambda row: (row.get("_recorded_dt") or datetime.min, int(row.get("id") or 0)))
            observation_value = rows[0].get("observation_id")
            try:
                obs_id = int(observation_value or 0)
            except (TypeError, ValueError):
                obs_id = 0
            if obs_id <= 0:
                continue
            kind = str(rows[0].get("session_kind") or self.session_kind or "offline").strip().lower()
            start_dt = None
            end_dt = None
            for row in rows:
                event_type = str(row.get("event_type") or "").strip().lower()
                recorded_dt = row.get("_recorded_dt")
                if event_type == "session_started" and start_dt is None:
                    start_dt = recorded_dt
                if event_type == "session_stopped":
                    end_dt = recorded_dt
            if start_dt is None:
                start_dt = rows[0].get("_recorded_dt")
            if end_dt is None:
                end_dt = rows[-1].get("_recorded_dt")
            sessions.append(
                TimelineSession(
                    session_id=session_id,
                    observation_id=obs_id,
                    session_kind=kind,
                    events=rows,
                    state_events=[
                        row
                        for row in rows
                        if str(row.get("event_type") or "").strip().lower() == "dropdown_change"
                    ],
                    note_events=[
                        row
                        for row in rows
                        if str(row.get("event_type") or "").strip().lower() == "manual_note"
                    ],
                    started_at=start_dt,
                    ended_at=end_dt,
                    last_recorded_at=rows[-1].get("_recorded_dt"),
                )
            )
        sessions.sort(
            key=lambda session: (
                session.started_at or session.last_recorded_at or datetime.min,
                session.session_id,
            )
        )
        return sessions

    def prepare_image_rows(self, paths: Iterable[str | Path]) -> list[dict]:
        rows: list[dict] = []
        seen: set[str] = set()
        for raw_path in paths or []:
            filepath = _normalize_path(raw_path)
            if not filepath or filepath in seen:
                continue
            seen.add(filepath)
            path_obj = Path(filepath)
            if not path_obj.exists() or not path_obj.is_file():
                continue
            if path_obj.suffix.lower() not in SUPPORTED_IMAGE_EXTENSIONS:
                continue
            captured_at = get_image_datetime(filepath)
            rows.append(
                {
                    "filepath": filepath,
                    "filename": path_obj.name,
                    "captured_at": captured_at,
                    "has_capture_time": bool(captured_at),
                }
            )
        rows.sort(
            key=lambda row: (
                row.get("captured_at") or datetime.max,
                str(row.get("filename") or "").casefold(),
            )
        )
        return rows

    def match_images(
        self,
        image_rows: Iterable[dict],
        *,
        observation_id: int | None = None,
        offset_seconds: float = 0.0,
        observation_tolerance_seconds: float = 60.0,
        session_grace_seconds: float | None = None,
        exclude_paths: Iterable[str | Path] | None = None,
    ) -> dict:
        sessions = self.load_sessions(observation_id=observation_id)
        observation_windows = self.load_observation_windows(
            image_rows,
            observation_id=observation_id,
            tolerance_seconds=observation_tolerance_seconds,
        )
        return self.match_images_against_sessions(
            image_rows,
            sessions=sessions,
            observation_windows=observation_windows,
            offset_seconds=offset_seconds,
            observation_tolerance_seconds=observation_tolerance_seconds,
            session_grace_seconds=session_grace_seconds,
            exclude_paths=exclude_paths,
        )

    def load_observation_windows(
        self,
        image_rows: Iterable[dict],
        *,
        observation_id: int | None = None,
        tolerance_seconds: float = 60.0,
    ) -> list[ObservationWindow]:
        timestamps: list[datetime] = []
        for row in image_rows or []:
            captured_at = _parse_timestamp((row or {}).get("captured_at"))
            if captured_at is not None:
                timestamps.append(captured_at)
        if not timestamps:
            return []
        min_dt = min(timestamps) - timedelta(seconds=max(0.0, float(tolerance_seconds)))
        max_dt = max(timestamps) + timedelta(seconds=max(0.0, float(tolerance_seconds)))

        windows: list[ObservationWindow] = []
        seen_ids: set[int] = set()
        for observation in ObservationDB.get_all_observations():
            try:
                obs_id = int((observation or {}).get("id") or 0)
            except Exception:
                obs_id = 0
            if obs_id <= 0 or obs_id in seen_ids:
                continue
            if observation_id is not None and obs_id != int(observation_id):
                continue
            observation_dt = _parse_timestamp((observation or {}).get("date"))
            if observation_dt is None:
                continue
            if observation_dt < min_dt or observation_dt > max_dt:
                continue
            window = self._observation_window_for_observation(obs_id, observation_dt)
            if window is None:
                continue
            seen_ids.add(obs_id)
            windows.append(window)
        windows.sort(
            key=lambda window: (
                window.start_at,
                window.end_at,
                window.observation_id,
            )
        )
        return windows

    def _observation_window_for_observation(
        self,
        observation_id: int,
        observation_dt: datetime | None = None,
    ) -> ObservationWindow | None:
        try:
            obs_id = int(observation_id or 0)
        except (TypeError, ValueError):
            return None
        if obs_id <= 0:
            return None
        if obs_id in self._observation_window_cache:
            return self._observation_window_cache.get(obs_id)

        image_rows = list(ImageDB.get_images_for_observation(obs_id) or [])
        timestamps: list[datetime] = []
        for row in image_rows:
            image_type = str((row or {}).get("image_type") or "").strip().lower()
            if image_type == "microscope":
                continue
            captured_at = _parse_timestamp((row or {}).get("captured_at"))
            if captured_at is None:
                filepath = _normalize_path((row or {}).get("filepath"))
                if not filepath:
                    continue
                captured_at = get_image_datetime(filepath)
                if captured_at is not None:
                    try:
                        ImageDB.set_image_captured_at(int((row or {}).get("id") or 0), captured_at)
                    except Exception:
                        pass
            if captured_at is not None:
                timestamps.append(captured_at)

        if timestamps:
            window = ObservationWindow(
                observation_id=obs_id,
                start_at=min(timestamps),
                end_at=max(timestamps),
                source="images",
                observation_date=observation_dt,
            )
        elif observation_dt is not None:
            window = ObservationWindow(
                observation_id=obs_id,
                start_at=observation_dt,
                end_at=observation_dt,
                source="observation",
                observation_date=observation_dt,
            )
        else:
            window = None
        self._observation_window_cache[obs_id] = window
        return window

    def match_images_against_sessions(
        self,
        image_rows: Iterable[dict],
        *,
        sessions: Iterable[TimelineSession],
        observation_windows: Iterable[ObservationWindow] | None = None,
        offset_seconds: float = 0.0,
        observation_tolerance_seconds: float = 60.0,
        session_grace_seconds: float | None = None,
        exclude_paths: Iterable[str | Path] | None = None,
    ) -> dict:
        exclude = {_normalize_path(path) for path in (exclude_paths or []) if path}
        session_list = list(sessions or [])
        window_list = list(observation_windows or [])
        matches_by_session: dict[str, list[dict]] = {session.session_id: [] for session in session_list}
        field_matches_by_observation: dict[int, list[dict]] = {}
        unmatched: list[dict] = []
        prepared_rows: list[dict] = []
        offset_value = float(offset_seconds or 0.0)
        active_session_grace = self.session_grace_seconds if session_grace_seconds is None else float(session_grace_seconds)

        for row in image_rows or []:
            filepath = _normalize_path(row.get("filepath"))
            if not filepath or filepath in exclude:
                continue
            captured_at = _parse_timestamp(row.get("captured_at"))
            prepared = dict(row)
            prepared["filepath"] = filepath
            prepared["filename"] = str(prepared.get("filename") or Path(filepath).name)
            prepared["captured_at"] = captured_at
            prepared["adjusted_at"] = (
                captured_at + timedelta(seconds=offset_value) if captured_at is not None else None
            )
            prepared["offset_seconds"] = offset_value
            prepared_rows.append(prepared)

        for row in prepared_rows:
            adjusted_at = row.get("adjusted_at")
            if adjusted_at is None:
                unmatched.append(row)
                continue
            best_session = self._best_session_for_timestamp(adjusted_at, session_list, active_session_grace)
            if best_session is None:
                best_window = self._best_observation_window_for_timestamp(
                    adjusted_at,
                    window_list,
                    observation_tolerance_seconds,
                )
                if best_window is None:
                    unmatched.append(row)
                    continue
                field_matches_by_observation.setdefault(best_window.observation_id, []).append(
                    {
                        **row,
                        "match_kind": "observation_window",
                        "image_type": "field",
                        "state": {},
                        "notes": None,
                        "note_events": [],
                        "session_id": None,
                        "session_kind": None,
                        "session_started_at": None,
                        "session_ended_at": None,
                        "window_start_at": best_window.start_at,
                        "window_end_at": best_window.end_at,
                        "window_source": best_window.source,
                    }
                )
                continue
            matches_by_session[best_session.session_id].append(row)

        matches: list[dict] = []
        observation_counts: dict[int, int] = {}
        session_counts: dict[str, int] = {}
        session_summaries: dict[str, dict] = {}
        for session in session_list:
            session_rows = sorted(
                matches_by_session.get(session.session_id, []),
                key=lambda row: (
                    row.get("adjusted_at") or datetime.max,
                    str(row.get("filename") or "").casefold(),
                ),
            )
            session_summaries[session.session_id] = session.to_summary()
            if not session_rows:
                continue

            notes_by_index: dict[int, list[dict]] = {idx: [] for idx in range(len(session_rows))}
            for note_row in session.note_events:
                note_dt = note_row.get("_recorded_dt")
                if note_dt is None or not session_rows:
                    continue
                best_index = min(
                    range(len(session_rows)),
                    key=lambda idx: abs(
                        (
                            (session_rows[idx].get("adjusted_at") or note_dt) - note_dt
                        ).total_seconds()
                    ),
                )
                notes_by_index.setdefault(best_index, []).append(note_row)

            for idx, row in enumerate(session_rows):
                adjusted_at = row.get("adjusted_at")
                state = session.state_at(adjusted_at)
                attached_notes = notes_by_index.get(idx, [])
                note_text = self._format_attached_notes(attached_notes)
                match_row = dict(row)
                match_row.update(
                    {
                        "match_kind": "session",
                        "image_type": "microscope",
                        "session_id": session.session_id,
                        "session_kind": session.session_kind,
                        "observation_id": session.observation_id,
                        "session_started_at": session.started_at,
                        "session_ended_at": session.ended_at,
                        "state": state,
                        "notes": note_text,
                        "note_events": attached_notes,
                    }
                )
                matches.append(match_row)
                observation_counts[session.observation_id] = observation_counts.get(session.observation_id, 0) + 1
                session_counts[session.session_id] = session_counts.get(session.session_id, 0) + 1

        for observation_id, rows in field_matches_by_observation.items():
            rows = sorted(
                rows,
                key=lambda row: (
                    row.get("adjusted_at") or datetime.max,
                    str(row.get("filename") or "").casefold(),
                ),
            )
            for row in rows:
                matches.append(row)
                observation_counts[observation_id] = observation_counts.get(observation_id, 0) + 1

        matches.sort(
            key=lambda row: (
                row.get("adjusted_at") or datetime.max,
                int(row.get("observation_id") or 0),
                str(row.get("filename") or "").casefold(),
            )
        )
        unmatched.sort(
            key=lambda row: (
                row.get("adjusted_at") or row.get("captured_at") or datetime.max,
                str(row.get("filename") or "").casefold(),
            )
        )
        return {
            "matches": matches,
            "unmatched": unmatched,
            "observation_counts": observation_counts,
            "session_counts": session_counts,
            "session_summaries": session_summaries,
        }

    def _best_session_for_timestamp(
        self,
        timestamp: datetime,
        sessions: list[TimelineSession],
        grace_seconds: float,
    ) -> TimelineSession | None:
        best_session: TimelineSession | None = None
        best_distance = float("inf")
        for session in sessions:
            if not session.contains(timestamp, grace_seconds=grace_seconds):
                continue
            distance = session.distance_seconds(timestamp)
            if distance < best_distance:
                best_distance = distance
                best_session = session
        return best_session

    @staticmethod
    def _best_observation_window_for_timestamp(
        timestamp: datetime,
        windows: list[ObservationWindow],
        tolerance_seconds: float,
    ) -> ObservationWindow | None:
        best_window: ObservationWindow | None = None
        best_distance = float("inf")
        for window in windows:
            if not window.contains(timestamp, grace_seconds=tolerance_seconds):
                continue
            distance = window.distance_seconds(timestamp)
            if distance < best_distance:
                best_distance = distance
                best_window = window
        return best_window

    @staticmethod
    def _format_attached_notes(note_rows: list[dict]) -> str | None:
        if not note_rows:
            return None
        parts: list[str] = []
        for row in note_rows:
            value = str(row.get("value") or "").strip()
            if not value:
                continue
            recorded_dt = row.get("_recorded_dt")
            if isinstance(recorded_dt, datetime):
                parts.append(f"[{recorded_dt.strftime('%H:%M:%S')}] {value}")
            else:
                parts.append(value)
        text = "\n".join(part for part in parts if part).strip()
        return text or None
