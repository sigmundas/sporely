"""Environment-gated crash points for sync recovery verification.

``SPORELY_SYNC_FAULT=<point>`` (optionally ``<point>:<table>``) terminates the
process at that point, so a harness can prove the state machine recovers from
a crash there. ``<point>`` is one of:

- ``before_request``: before a create request is sent;
- ``after_insert``: after the server confirmed an INSERT, before the local
  ``cloud_id`` is committed;
- ``after_upload``: after media bytes were uploaded, before the row that
  confirms them is written;
- ``during_upload``: while a media PUT is in flight, after its first body
  chunk was sent and before the rest;
- ``between_deletes``: after an observation's image-row DELETE, before the
  observation DELETE.

Inert unless the variable is set. It is read from the process environment
only, never from settings or the UI.
"""

from __future__ import annotations

import os

SYNC_FAULT_ENV_VAR = 'SPORELY_SYNC_FAULT'
SYNC_FAULT_POINTS = frozenset({
    'before_request', 'after_insert', 'after_upload', 'during_upload', 'between_deletes',
})
SYNC_FAULT_EXIT_CODE = 86


def _terminate() -> None:
    os._exit(SYNC_FAULT_EXIT_CODE)


def sync_fault_armed(point: str, *, table: str = '') -> bool:
    raw = str(os.environ.get(SYNC_FAULT_ENV_VAR) or '').strip()
    if not raw:
        return False
    name, _, scope = raw.partition(':')
    if name.strip() != point or point not in SYNC_FAULT_POINTS:
        return False
    if scope.strip() and scope.strip() != table:
        return False
    return True


def sync_fault_point(point: str, *, table: str = '') -> None:
    if not sync_fault_armed(point, table=table):
        return
    raw = str(os.environ.get(SYNC_FAULT_ENV_VAR) or '').strip()
    print(
        f'[cloud_sync] {SYNC_FAULT_ENV_VAR}={raw}: terminating at {point} table={table or "-"}',
        flush=True,
    )
    _terminate()


class _FaultingUploadBody:
    """A file body that ends the process when its second chunk is read.

    The HTTP client sends each chunk before reading the next, so the first
    chunk has reached the server and the request is in flight.
    """

    def __init__(self, handle, point: str, table: str):
        self._handle = handle
        self._point = point
        self._table = table
        self._chunks_read = 0

    def read(self, *args):
        if self._chunks_read >= 1:
            sync_fault_point(self._point, table=self._table)
        self._chunks_read += 1
        return self._handle.read(*args)

    def __getattr__(self, name):
        return getattr(self._handle, name)


def fault_upload_body(handle, *, table: str):
    """Return ``handle``, wrapped for ``during_upload`` only when it is armed."""
    if not sync_fault_armed('during_upload', table=table):
        return handle
    return _FaultingUploadBody(handle, 'during_upload', table)
