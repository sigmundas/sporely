"""Remote capability probes and metadata-purpose constants.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S3 of the cloud-sync extraction.
"""

from __future__ import annotations



def _owner_sync_parents_supported(client) -> bool:
    """True only when the server confirms the owner-sync parent capability.

    Fails closed: a client without the probe, or any probe failure, means no
    owner-sync parent is created, so an older server can never receive a
    metadata-only row its public RPCs would expose.
    """
    probe = getattr(client, '_observation_images_support_metadata_purpose', None)
    if not callable(probe):
        return False
    try:
        return bool(probe())
    except Exception:
        return False


def _remote_metadata_purpose(client, remote_row: dict) -> str | None:
    """A parent's stored purpose: from the row if read, else a targeted read."""
    if 'metadata_purpose' in remote_row:
        return str(remote_row.get('metadata_purpose') or '') or None
    fetch = getattr(client, 'fetch_image_metadata_purpose', None)
    cloud_image_id = str(remote_row.get('id') or '').strip()
    if not callable(fetch) or not cloud_image_id:
        return None
    purpose = fetch(cloud_image_id)
    remote_row['metadata_purpose'] = purpose
    return purpose


METADATA_PURPOSE_OWNER_SYNC = 'owner_sync'


METADATA_PURPOSE_PUBLIC_MICROSCOPY = 'public_microscopy'
