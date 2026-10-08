"""Local spore-mosaic signature: compute, load, store, clear and currency check.

The signature records local mosaic inputs only; it never proves remote upload
completeness, and publication selection is not part of it.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S6 of the cloud-sync extraction.
"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from pathlib import Path

from database.schema import get_connection, get_images_dir


def _canonical_signature_value(value):
    """Normalise a single input value for the canonical JSON payload."""
    if value is None:
        return None
    if isinstance(value, bool):
        return bool(value)
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        # Round to 6 dp so a floating-point 12.3400000001 doesn't invalidate
        # a signature that used 12.34 the last time round.
        return round(float(value), 6)
    text = str(value).strip()
    if text == '':
        return None
    return text


def _file_stat_fingerprint(path: Path) -> dict:
    """Return {'mtime_ns': int, 'size_bytes': int} or {} on stat failure.

    Kept small and stable — extra fields would just add churn without
    strengthening change detection.
    """
    try:
        st = path.stat()
    except Exception:
        return {}
    return {
        'mtime_ns': int(getattr(st, 'st_mtime_ns', 0) or 0),
        'size_bytes': int(getattr(st, 'st_size', 0) or 0),
    }


def _resolve_local_image_path(filepath: str) -> Path | None:
    """Best-effort resolution of a local image path.

    Mirrors the fallback the mosaic pipeline uses: absolute paths as-is,
    otherwise join against ``get_images_dir()``.
    """
    raw = str(filepath or '').strip()
    if not raw:
        return None
    path = Path(raw)
    if not path.is_absolute():
        try:
            path = get_images_dir() / path
        except Exception:
            return path
    return path


def _local_spore_mosaic_signature(
    obs_local_id: int,
    eligible_rows: list[dict],
    observation_row: dict,
) -> str:
    """Compute a deterministic SHA-1 signature of the mosaic inputs.

    The pusher must pass its own already-filtered `eligible_rows` (same
    list it feeds into the mosaic builder) so the signature is 1:1 with
    what actually goes out. The signature is stable across process
    restarts as long as the local inputs have not changed.
    """
    from utils.cloud_spore_mosaic import MOSAIC_PIPELINE_VERSION

    obs_visibility = _canonical_signature_value(
        (observation_row or {}).get('spore_data_visibility') or 'public'
    )

    # Sort measurements by stable local id for determinism. The pusher's
    # own SELECT already uses `ORDER BY m.id`, but re-sorting here means
    # the helper is safe to call with rows fetched in any order.
    sorted_rows = sorted(
        (dict(r) for r in (eligible_rows or [])),
        key=lambda r: int(r.get('id') or 0),
    )

    measurements_payload: list[dict] = []
    # Collect per-image fingerprints. Keyed by (local_image_id, cloud_image_id,
    # resolved_path_str) so a rename or a bytes-swap produces a new signature.
    image_fps: dict[tuple, dict] = {}

    for row in sorted_rows:
        local_image_id = _canonical_signature_value(row.get('image_id'))
        image_cloud_id = _canonical_signature_value(row.get('image_cloud_id'))
        filepath = str(row.get('image_filepath') or '').strip()
        resolved = _resolve_local_image_path(filepath)
        resolved_str = str(resolved) if resolved is not None else None

        measurements_payload.append({
            'id': _canonical_signature_value(row.get('id')),
            'cloud_id': _canonical_signature_value(row.get('cloud_id')),
            'image_id': local_image_id,
            'image_cloud_id': image_cloud_id,
            'p1_x': _canonical_signature_value(row.get('p1_x')),
            'p1_y': _canonical_signature_value(row.get('p1_y')),
            'p2_x': _canonical_signature_value(row.get('p2_x')),
            'p2_y': _canonical_signature_value(row.get('p2_y')),
            'p3_x': _canonical_signature_value(row.get('p3_x')),
            'p3_y': _canonical_signature_value(row.get('p3_y')),
            'p4_x': _canonical_signature_value(row.get('p4_x')),
            'p4_y': _canonical_signature_value(row.get('p4_y')),
            'length_um': _canonical_signature_value(row.get('length_um')),
            'width_um': _canonical_signature_value(row.get('width_um')),
            'measurement_type': _canonical_signature_value(row.get('measurement_type')),
            'gallery_rotation': _canonical_signature_value(row.get('gallery_rotation')),
        })

        image_key = (local_image_id, image_cloud_id, resolved_str)
        if image_key not in image_fps:
            fp: dict = {
                'image_id': local_image_id,
                'image_cloud_id': image_cloud_id,
                'source_path': resolved_str,
                'scale_microns_per_pixel': _canonical_signature_value(
                    row.get('scale_microns_per_pixel')
                ),
                'resample_scale_factor': _canonical_signature_value(
                    row.get('resample_scale_factor')
                ),
            }
            if resolved is not None:
                stat_fp = _file_stat_fingerprint(resolved)
                fp['mtime_ns'] = stat_fp.get('mtime_ns')
                fp['size_bytes'] = stat_fp.get('size_bytes')
            else:
                fp['mtime_ns'] = None
                fp['size_bytes'] = None
            image_fps[image_key] = fp

    images_payload = sorted(
        image_fps.values(),
        key=lambda fp: (
            fp.get('image_id') or 0,
            fp.get('image_cloud_id') or '',
            fp.get('source_path') or '',
        ),
    )

    payload = {
        'v': int(MOSAIC_PIPELINE_VERSION),
        'obs': {
            'local_id': int(obs_local_id or 0),
            'spore_data_visibility': obs_visibility,
        },
        'measurements': measurements_payload,
        'images': images_payload,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(',', ':'))
    return hashlib.sha1(canonical.encode('utf-8')).hexdigest()


def _load_spore_mosaic_eligible_rows(cursor, obs_local_id: int) -> list[dict]:
    """The measurement rows that feed the public spore mosaic.

    Single source of the eligibility query, shared by the mosaic pusher and by
    `_current_local_mosaic_signature`, so the rows a signature is computed over
    can never drift from the rows the mosaic is rendered from. The cursor's
    connection must use `sqlite3.Row`.
    """
    cursor.execute(
        '''
        SELECT m.id, m.image_id, m.length_um, m.width_um, m.measurement_type,
               m.p1_x, m.p1_y, m.p2_x, m.p2_y,
               m.p3_x, m.p3_y, m.p4_x, m.p4_y,
               m.gallery_rotation, m.cloud_id,
               i.cloud_id                 AS image_cloud_id,
               i.filepath                 AS image_filepath,
               i.scale_microns_per_pixel  AS scale_microns_per_pixel,
               i.resample_scale_factor    AS resample_scale_factor
        FROM spore_measurements m
        JOIN images i ON i.id = m.image_id
        WHERE i.observation_id = ?
          AND i.image_type = 'microscope'
          AND i.cloud_id IS NOT NULL
          AND m.cloud_id IS NOT NULL
          AND m.length_um IS NOT NULL
          AND m.width_um  IS NOT NULL
          AND m.p1_x IS NOT NULL AND m.p1_y IS NOT NULL
          AND m.p2_x IS NOT NULL AND m.p2_y IS NOT NULL
          AND (
            m.measurement_type IS NULL
            OR m.measurement_type = ''
            OR lower(m.measurement_type) IN ('manual', 'spore', 'spores')
          )
        ORDER BY m.id
        ''',
        (obs_local_id,),
    )
    return [dict(r) for r in cursor.fetchall()]


def _current_local_mosaic_signature(obs_local_id: int) -> str:
    """The mosaic signature the pusher would compute right now, or ''.

    Mirrors the pusher's own gates: a non-public observation or one with no
    eligible measurements has no mosaic, hence no signature.
    """
    if obs_local_id <= 0:
        return ''
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT spore_data_visibility FROM observations WHERE id = ?',
            (obs_local_id,),
        )
        obs_row = cursor.fetchone()
        if obs_row is None:
            return ''
        observation_row = dict(obs_row)
        visibility = str(observation_row.get('spore_data_visibility') or 'public').strip().lower()
        if visibility != 'public':
            return ''
        rows = _load_spore_mosaic_eligible_rows(cursor, obs_local_id)
    finally:
        conn.close()
    if not rows:
        return ''
    return _local_spore_mosaic_signature(obs_local_id, rows, observation_row)


def _local_mosaic_signature_is_current(obs_local_id: int) -> bool:
    """True when the stored mosaic signature matches the current inputs."""
    try:
        stored = _load_local_mosaic_signature(obs_local_id)
        return bool(stored) and _current_local_mosaic_signature(obs_local_id) == stored
    except Exception:
        return False


def _load_local_mosaic_signature(obs_local_id: int) -> str:
    """Read the cached signature from `observations.mosaic_signature`.

    Missing column (older DB that hasn't run init/migration) is treated as
    "no cached signature" so the mosaic pusher still rebuilds cleanly.
    """
    if obs_local_id <= 0:
        return ''
    conn = get_connection()
    try:
        cursor = conn.execute(
            'SELECT mosaic_signature FROM observations WHERE id = ?',
            (int(obs_local_id),),
        )
        row = cursor.fetchone()
    except sqlite3.OperationalError:
        return ''
    finally:
        conn.close()
    if row is None:
        return ''
    return str(row[0] or '').strip()


def _store_local_mosaic_signature(obs_local_id: int, signature: str) -> None:
    """Persist the freshly-computed signature.

    Only called on successful mosaic upload + tile rewrite so partial
    failures never poison the cache.
    """
    if obs_local_id <= 0 or not signature:
        return
    conn = get_connection()
    try:
        conn.execute(
            'UPDATE observations SET mosaic_signature = ? WHERE id = ?',
            (str(signature).strip(), int(obs_local_id)),
        )
        conn.commit()
    except sqlite3.OperationalError:
        pass
    finally:
        conn.close()


def _clear_local_mosaic_signature(obs_local_id: int) -> None:
    if obs_local_id <= 0:
        return
    conn = get_connection()
    try:
        conn.execute(
            'UPDATE observations SET mosaic_signature = NULL WHERE id = ?',
            (int(obs_local_id),),
        )
        conn.commit()
    except sqlite3.OperationalError:
        pass
    finally:
        conn.close()
