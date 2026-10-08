"""Observation EXIF injection into pulled field images and the EXIF backfill
of already-downloaded cloud images.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S6 of the cloud-sync extraction.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from database.models import ObservationDB, SettingsDB
from database.schema import get_connection


def _inject_obs_exif_into_field_image(
    image_path: Path,
    obs_lat: float | None,
    obs_lon: float | None,
    obs_altitude: float | None,
    obs_datetime_str: str | None,
    camera_model: str | None = None,
    iso: int | None = None,
    exposure_time: float | None = None,
    f_number: float | None = None,
    gps_accuracy: float | None = None,
) -> None:
    """Write observation GPS/datetime and camera metadata into an image that has no EXIF.

    Called on cloud-synced field images whose EXIF was stripped by the web
    app's conversion.  Only modifies the file when the image has no
    existing DateTimeOriginal AND the observation has GPS or datetime data.
    Does nothing for unsupported files or on any error.
    """
    if not image_path.exists():
        return
    suffix = image_path.suffix.lower()
    if suffix not in {'.jpg', '.jpeg', '.webp'}:
        return
    has_coords = obs_lat is not None and obs_lon is not None
    has_datetime = bool(obs_datetime_str)
    has_camera_data = any(x is not None for x in (camera_model, iso, exposure_time, f_number))
    if not has_coords and not has_datetime and not has_camera_data:
        return
    try:
        from PIL import Image as _PilImage, ExifTags as _ExifTags
        with _PilImage.open(image_path) as img:
            existing_exif = img.getexif()
            existing_tags = {
                _ExifTags.TAGS.get(k, k): v for k, v in existing_exif.items()
            } if existing_exif else {}
            already_has_dt = any(
                t in existing_tags
                for t in ('DateTimeOriginal', 'DateTimeDigitized', 'DateTime')
            )
            try:
                already_has_gps = bool(existing_exif.get_ifd(0x8825))
            except Exception:
                already_has_gps = False
                
            already_has_camera = any(
                t in existing_tags
                for t in ('Model', 'Make', 'ISOSpeedRatings', 'ExposureTime', 'FNumber')
            )
            
            if already_has_dt and already_has_gps and already_has_camera:
                return  # nothing to do

            exif = existing_exif if existing_exif is not None else img.getexif()

            if not already_has_dt and has_datetime:
                try:
                    dt_exif = _exif_datetime_from_text(obs_datetime_str)
                    # Tag 306 = DateTime, 36867 = DateTimeOriginal, 36868 = DateTimeDigitized
                    if dt_exif:
                        exif[306] = dt_exif
                        exif[36867] = dt_exif
                        exif[36868] = dt_exif
                except Exception:
                    pass

            if not already_has_gps and has_coords:
                try:
                    def _deg_to_rational(deg_float):
                        d = int(abs(deg_float))
                        m_float = (abs(deg_float) - d) * 60
                        m = int(m_float)
                        s_float = (m_float - m) * 60
                        s_num = int(round(s_float * 1000))
                        return ((d, 1), (m, 1), (s_num, 1000))

                    gps_ifd = {
                        1: 'N' if obs_lat >= 0 else 'S',    # GPSLatitudeRef
                        2: _deg_to_rational(obs_lat),        # GPSLatitude
                        3: 'E' if obs_lon >= 0 else 'W',    # GPSLongitudeRef
                        4: _deg_to_rational(obs_lon),        # GPSLongitude
                    }
                    if obs_altitude is not None:
                        altitude = float(obs_altitude)
                        gps_ifd[5] = 1 if altitude < 0 else 0  # GPSAltitudeRef
                        gps_ifd[6] = (int(round(abs(altitude) * 100)), 100)
                    if gps_accuracy is not None:
                        acc = float(gps_accuracy)
                        if acc >= 0:
                            gps_ifd[31] = (int(round(acc * 100)), 100)  # GPSHPositioningError
                    exif[34853] = gps_ifd  # GPSInfo
                except Exception:
                    pass
                    
            if not already_has_camera:
                try:
                    if camera_model:
                        exif[272] = camera_model  # Model
                    if iso is not None:
                        exif[34855] = int(iso)  # ISOSpeedRatings
                    if exposure_time is not None:
                        try:
                            ex_time = float(exposure_time)
                            if ex_time > 0:
                                if ex_time >= 1:
                                    exif[33434] = (int(round(ex_time * 1000)), 1000)  # ExposureTime
                                else:
                                    exif[33434] = (1, int(round(1 / ex_time)))
                        except Exception:
                            pass
                    if f_number is not None:
                        try:
                            fn = float(f_number)
                            if fn > 0:
                                exif[33437] = (int(round(fn * 10)), 10)  # FNumber
                        except Exception:
                            pass
                except Exception:
                    pass

            mode = img.mode
            if suffix in {'.jpg', '.jpeg'} and mode not in {'RGB', 'L'}:
                img = img.convert('RGB')
            try:
                exif_bytes = exif.tobytes()
                if suffix == '.webp':
                    save_kwargs = {'format': 'WEBP', 'exif': exif_bytes}
                    if mode == 'RGBA':
                        save_kwargs['lossless'] = True
                    else:
                        save_kwargs['quality'] = 96
                else:
                    save_kwargs = {'format': 'JPEG', 'exif': exif_bytes, 'quality': 92}
                img.save(image_path, **save_kwargs)
            except Exception:
                pass
    except Exception as exc:
        print(f'[cloud_sync] Could not inject EXIF into {image_path.name}: {exc}')


def _exif_datetime_from_text(value: str | None) -> str | None:
    """Return EXIF datetime text (YYYY:MM:DD HH:MM:SS) from ISO/date text."""
    text = str(value or '').strip()
    if not text:
        return None
    try:
        normalized = text.replace('Z', '+00:00')
        parsed = datetime.fromisoformat(normalized)
        return parsed.strftime('%Y:%m:%d %H:%M:%S')
    except Exception:
        pass
    try:
        if 'T' in text:
            date_part, time_part = text.split('T', 1)
        elif ' ' in text:
            date_part, time_part = text.split(' ', 1)
        else:
            date_part, time_part = text, '00:00:00'
        time_part = time_part.split('+', 1)[0].split('-', 1)[0].split('.', 1)[0]
        bits = [part for part in time_part.split(':') if part]
        while len(bits) < 3:
            bits.append('00')
        return f"{date_part.replace('-', ':')} {':'.join(bits[:3])}"
    except Exception:
        return None


def _load_obs_exif_fallback(observation_id: int, fallback_datetime: str | None = None) -> tuple[float | None, float | None, float | None, float | None, str | None]:
    """Return (lat, lon, altitude, gps_accuracy, datetime_str) from local observation data."""
    try:
        obs = ObservationDB.get_observation(observation_id)
        if not obs:
            return None, None, None, None, fallback_datetime
        lat = obs.get('gps_latitude')
        lon = obs.get('gps_longitude')
        altitude = obs.get('gps_altitude')
        accuracy = obs.get('gps_accuracy')
        datetime_str = str(
            obs.get('captured_at')
            or obs.get('date')
            or fallback_datetime
            or ''
        ).strip() or None
        return (float(lat) if lat is not None else None,
                float(lon) if lon is not None else None,
                float(altitude) if altitude is not None else None,
                float(accuracy) if accuracy is not None else None,
                datetime_str)
    except Exception:
        return None, None, None, None, fallback_datetime


_SETTING_CLOUD_EXIF_BACKFILL_STATE = 'cloud_exif_backfill_checked'


def _exif_file_signature(path: Path) -> str | None:
    """Cheap file fingerprint (mtime + size) used to skip already-checked files.

    A `stat()` is orders of magnitude cheaper than opening + decoding the image,
    so it lets EXIF backfill avoid re-reading every cloud field image on a sync
    where nothing has changed.
    """
    try:
        st = path.stat()
    except Exception:
        return None
    return f'{st.st_mtime_ns}:{st.st_size}'


def _load_exif_backfill_state() -> dict:
    try:
        raw = SettingsDB.get_setting(_SETTING_CLOUD_EXIF_BACKFILL_STATE)
        if raw:
            data = json.loads(raw)
            if isinstance(data, dict):
                return {str(k): str(v) for k, v in data.items()}
    except Exception:
        pass
    return {}


def _save_exif_backfill_state(state: dict) -> None:
    try:
        SettingsDB.set_setting(
            _SETTING_CLOUD_EXIF_BACKFILL_STATE,
            json.dumps(state, separators=(',', ':')),
        )
    except Exception:
        pass


def _backfill_missing_exif_on_cloud_images() -> dict:
    """Inject observation GPS/datetime into field images whose EXIF was stripped
    by the web app's 2 MP conversion (cloud_id set but no EXIF datetime/GPS).

    Runs at the start of each pull.  To avoid a multi-second hidden tax on every
    no-change sync, files are fingerprinted by mtime+size: a file whose exact
    version was already checked is skipped without opening/decoding it.  Files
    are only opened when they are new or have changed since the last check.

    Returns a counters dict (scanned / skipped_cached / opened / already_complete
    / updated / missing_file) for instrumentation.
    """
    counters = {
        'scanned': 0,
        'skipped_cached': 0,
        'opened': 0,
        'already_complete': 0,
        'updated': 0,
        'missing_file': 0,
    }
    try:
        from PIL import Image as _PilImg, ExifTags as _ET
        conn = get_connection()
        try:
            rows = conn.execute(
                '''
                SELECT i.id, i.filepath, i.observation_id, i.image_type
                FROM images i
                WHERE i.cloud_id IS NOT NULL
                  AND i.image_type != 'microscope'
                  AND i.filepath IS NOT NULL
                '''
            ).fetchall()
        finally:
            conn.close()
        if not rows:
            return counters

        prev_state = _load_exif_backfill_state()
        new_state: dict[str, str] = {}
        for row in rows:
            counters['scanned'] += 1
            image_id = str(row[0])
            filepath = str(row[1] or '').strip()
            if not filepath:
                continue
            p = Path(filepath)
            if p.suffix.lower() not in {'.jpg', '.jpeg', '.webp'}:
                continue
            sig = _exif_file_signature(p)
            if sig is None:
                # File missing/unreadable: don't cache, so it is retried once it
                # reappears (e.g. after media materialization).
                counters['missing_file'] += 1
                continue
            if prev_state.get(image_id) == sig:
                # Same file version already checked — skip the expensive open.
                counters['skipped_cached'] += 1
                new_state[image_id] = sig
                continue

            counters['opened'] += 1
            try:
                with _PilImg.open(p) as img:
                    exif = img.getexif()
                    tags = {_ET.TAGS.get(k, k): v for k, v in exif.items()} if exif else {}
                    already_has_dt = any(
                        t in tags for t in ('DateTimeOriginal', 'DateTimeDigitized', 'DateTime')
                    )
                    try:
                        already_has_gps = bool(exif.get_ifd(0x8825))
                    except Exception:
                        already_has_gps = False
                if already_has_dt and already_has_gps:
                    counters['already_complete'] += 1
                    new_state[image_id] = sig
                    continue
            except Exception:
                # Leave uncached so a transient read error is retried next sync.
                continue

            obs_id = int(row[2] or 0)
            if obs_id <= 0:
                continue
            lat, lon, altitude, gps_acc, date_str = _load_obs_exif_fallback(obs_id)
            _inject_obs_exif_into_field_image(
                p,
                lat,
                lon,
                altitude,
                date_str,
                gps_accuracy=gps_acc,
            )
            counters['updated'] += 1
            # Injection rewrites the file, so record the post-write signature to
            # skip it next time.
            new_state[image_id] = _exif_file_signature(p) or sig

        if new_state != prev_state:
            _save_exif_backfill_state(new_state)
    except Exception as exc:
        print(f'[cloud_sync] EXIF backfill skipped: {exc}')
    return counters
