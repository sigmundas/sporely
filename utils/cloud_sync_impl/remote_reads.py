"""Shared remote reads of images and measurements used by push and pull.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S3 of the cloud-sync extraction.
"""

from __future__ import annotations


from utils.cloud_sync_impl.common import _safe_int


def _pull_remote_images_for_sync(client: "SporelyCloudClient", cloud_id: str) -> list[dict]:
    """Fetch cloud image rows including deleted ones so tombstones can be recorded."""
    cloud_value = str(cloud_id or '').strip()
    if not cloud_value:
        return []
    return [
        dict(row or {})
        for row in (client.pull_image_metadata(cloud_value, include_deleted_for_sync=True) or [])
    ]


def _pull_remote_measurements_for_images(
    client: "SporelyCloudClient",
    image_cloud_ids: list[str],
) -> list[dict]:
    fetcher = getattr(client, 'pull_measurements_for_images', None)
    if not callable(fetcher):
        return []
    rows = fetcher(image_cloud_ids)
    return [dict(row or {}) for row in (rows or [])]


def _group_remote_measurements_by_observation(
    remote_images: list[dict] | None,
    remote_measurements: list[dict] | None,
) -> dict[str, list[dict]]:
    image_to_obs: dict[str, str] = {}
    for image_row in (remote_images or []):
        cloud_image_id = str(image_row.get('id') or '').strip()
        cloud_obs_id = str(image_row.get('observation_id') or '').strip()
        if cloud_image_id and cloud_obs_id:
            image_to_obs[cloud_image_id] = cloud_obs_id
    grouped: dict[str, list[dict]] = {}
    for measurement_row in (remote_measurements or []):
        cloud_image_id = str(measurement_row.get('image_id') or '').strip()
        cloud_obs_id = image_to_obs.get(cloud_image_id)
        if not cloud_obs_id:
            continue
        grouped.setdefault(cloud_obs_id, []).append(dict(measurement_row or {}))
    for rows in grouped.values():
        rows.sort(
            key=lambda row: (
                str(row.get('image_id') or ''),
                _safe_int(row.get('desktop_id')),
                str(row.get('id') or ''),
            )
        )
    return grouped
