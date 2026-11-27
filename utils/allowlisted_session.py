"""A requests session that only talks to an explicit list of HTTPS hosts.

Used by the third-party login helpers so that a password is never posted to a
host named by a downloaded page (a form action or a redirect), only to the
service's own hosts and its identity provider.
"""

from __future__ import annotations

from typing import Iterable
from urllib.parse import urlsplit

import requests


class HostNotAllowedError(RuntimeError):
    """Raised before a request to a URL outside the allow-list is sent."""


def check_https_allowed_host(url: str, allowed_hosts: Iterable[str]) -> None:
    parts = urlsplit(str(url or ""))
    host = (parts.hostname or "").lower()
    if parts.scheme.lower() != "https" or host not in {h.lower() for h in allowed_hosts}:
        raise HostNotAllowedError(
            f"Refusing to contact {host or 'an unknown host'}: not an allowed https host for this login."
        )


class AllowListedSession(requests.Session):
    """requests.Session that refuses any request, including each redirect hop,
    whose URL is not https on an allowed host."""

    def __init__(self, allowed_hosts: Iterable[str]):
        super().__init__()
        self.allowed_hosts = frozenset(str(h).lower() for h in allowed_hosts)

    def send(self, request, **kwargs):
        check_https_allowed_host(request.url, self.allowed_hosts)
        return super().send(request, **kwargs)
