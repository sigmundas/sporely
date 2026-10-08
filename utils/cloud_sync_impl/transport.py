"""HTTP transport helpers mixed into ``SporelyCloudClient``.

Relocated verbatim from ``utils/cloud_sync.py`` (the compatibility facade)
in Stage S3 of the cloud-sync extraction.
"""

from __future__ import annotations

import json

from utils.cloud_sync_impl.errors import CloudSyncError


# PostgREST silently truncates every response body to ``db-max-rows`` — Supabase's
# default is 1000. Callers of ``_get_paginated`` MUST include a deterministic
# ``order=`` clause; the helper pages with ``limit=1000&offset=N`` and stops when a
# page comes back shorter than this cap. Never treat a cap-sized response as
# complete without paging past it.
_CLOUD_SYNC_MAX_ROWS_PER_PAGE = 1000


class CloudSyncTransportMixin:
    """REST helpers of ``SporelyCloudClient``; the client class stays in the facade."""

    def _get_paginated(
        self,
        path: str,
        *,
        page_size: int = _CLOUD_SYNC_MAX_ROWS_PER_PAGE,
        max_rows: int | None = None,
        max_response_bytes: int | None = None,
    ) -> list:
        """Fully page a PostgREST GET past the server ``db-max-rows`` cap.

        Callers MUST include a deterministic ``order=`` clause (with ``id.asc``
        as tie-breaker) in ``path``; otherwise offset-based paging can skip or
        duplicate rows across pages. On any page failure the exception from
        ``_get`` propagates — partial results are never returned, so callers
        must not treat truncation as an authoritative empty result.
        """
        if (page_size <= 0 or (max_rows is not None and max_rows <= 0)
                or (max_response_bytes is not None and max_response_bytes <= 0)):
            raise CloudSyncError(f'GET {path}: invalid page_size {page_size}')
        all_rows: list = []
        response_bytes = 0
        offset = 0
        sep = '&' if '?' in path else '?'
        while True:
            page_path = f'{path}{sep}limit={page_size}&offset={offset}'
            rows = self._get(page_path)
            if not isinstance(rows, list):
                raise CloudSyncError(
                    f'GET {path}: expected list response for paginated fetch, '
                    f'got {type(rows).__name__}'
                )
            response_bytes += len(json.dumps(
                rows, ensure_ascii=False, separators=(',', ':'),
            ).encode('utf-8'))
            if max_response_bytes is not None and response_bytes > max_response_bytes:
                raise CloudSyncError(
                    f'GET {path}: response exceeds {max_response_bytes} bytes'
                )
            all_rows.extend(rows)
            if max_rows is not None and len(all_rows) > max_rows:
                raise CloudSyncError(f'GET {path}: response exceeds {max_rows} rows')
            if len(rows) < page_size:
                return all_rows
            offset += len(rows)
