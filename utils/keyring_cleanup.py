"""Small cross-platform helpers for deleting known system-keyring entries."""
from __future__ import annotations

import sys
from collections.abc import Iterable


def _password_was_already_absent(exc: Exception, service_name: str) -> bool:
    text = str(exc or '').strip().lower()
    if any(token in text for token in ('not found', 'no such password', 'item not found')):
        return True
    # The Windows backend reports a missing credential using only the target
    # service name. Credential Locker deletion itself is non-interactive.
    return sys.platform == 'win32' and text == str(service_name or '').strip().lower()


def delete_password_entries(
    keyring,
    entries: Iterable[tuple[str, str]],
) -> list[str]:
    """Delete entries, ignoring absence but reporting denial/backend failures."""
    failures: list[str] = []
    password_delete_error = getattr(
        getattr(keyring, 'errors', None),
        'PasswordDeleteError',
        (),
    )
    for service_name, account_name in entries:
        try:
            keyring.delete_password(service_name, account_name)
        except Exception as exc:
            if (
                password_delete_error
                and isinstance(exc, password_delete_error)
                and _password_was_already_absent(exc, service_name)
            ):
                continue
            failures.append(f'{service_name}: {exc}')
    return failures
