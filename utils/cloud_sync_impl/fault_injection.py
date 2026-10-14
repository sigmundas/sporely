"""Environment-gated crash points for sync recovery verification.

``SPORELY_SYNC_FAULT=<point>`` (optionally ``<point>:<table>``) terminates the
process at that point, so a harness can prove the state machine recovers from
a crash there. ``<point>`` is one of:

- ``before_request``: before a create request is sent;
- ``after_insert``: after the server confirmed an INSERT, before the local
  ``cloud_id`` is committed;
- ``after_upload``: after media bytes were uploaded, before the row that
  confirms them is written.

Inert unless the variable is set. It is read from the process environment
only, never from settings or the UI.
"""

from __future__ import annotations

import os

SYNC_FAULT_ENV_VAR = 'SPORELY_SYNC_FAULT'
SYNC_FAULT_POINTS = frozenset({'before_request', 'after_insert', 'after_upload'})
SYNC_FAULT_EXIT_CODE = 86


def _terminate() -> None:
    os._exit(SYNC_FAULT_EXIT_CODE)


def sync_fault_point(point: str, *, table: str = '') -> None:
    raw = str(os.environ.get(SYNC_FAULT_ENV_VAR) or '').strip()
    if not raw:
        return
    name, _, scope = raw.partition(':')
    if name.strip() != point or point not in SYNC_FAULT_POINTS:
        return
    if scope.strip() and scope.strip() != table:
        return
    print(
        f'[cloud_sync] {SYNC_FAULT_ENV_VAR}={raw}: terminating at {point} table={table or "-"}',
        flush=True,
    )
    _terminate()
