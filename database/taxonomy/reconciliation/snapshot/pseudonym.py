"""Deterministic HMAC-keyed observation-reference pseudonymisation.

The key never enters the repository. It is supplied at transform time via
the ``SPORELY_W2DR_PSEUDONYM_KEY`` environment variable (min 32 bytes of
raw entropy, base64-encoded) or via ``--pseudonym-key-file`` pointing at a
file readable only by the operator.

The same raw observation id under the same key produces the same
pseudonym; different keys produce different pseudonyms. The mapping from
pseudonym back to production observation lives outside the repository
with the operator who holds the key.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
from pathlib import Path


PSEUDONYM_LENGTH_HEX = 24  # 12 bytes of HMAC output; ample for < 10^9 observations.
KEY_ENV_VAR = "SPORELY_W2DR_PSEUDONYM_KEY"
MIN_KEY_BYTES = 32


class PseudonymKeyError(ValueError):
    """Raised when the pseudonymisation key is missing or too short."""


def _load_key_bytes(key_file: Path | None) -> bytes:
    if key_file is not None:
        raw = key_file.read_text().strip()
    else:
        raw = os.environ.get(KEY_ENV_VAR, "").strip()
    if not raw:
        raise PseudonymKeyError(
            "pseudonymisation key not supplied — set "
            f"{KEY_ENV_VAR} or pass --pseudonym-key-file"
        )
    try:
        decoded = base64.b64decode(raw, validate=True)
    except Exception as exc:  # noqa: BLE001 — deliberately narrow re-raise
        raise PseudonymKeyError(
            "pseudonymisation key must be base64-encoded raw bytes"
        ) from exc
    if len(decoded) < MIN_KEY_BYTES:
        raise PseudonymKeyError(
            f"pseudonymisation key is {len(decoded)} bytes; "
            f"minimum {MIN_KEY_BYTES} required"
        )
    return decoded


def make_pseudonymiser(key_file: Path | None = None):
    """Return a callable that maps a raw observation id to a stable pseudonym.

    The key is loaded once and captured in the closure; it is never
    logged or returned. The returned callable is safe to reuse across all
    rows of one export.
    """

    key = _load_key_bytes(key_file)

    def pseudonymise(raw_observation_id: str) -> str:
        if not isinstance(raw_observation_id, str) or not raw_observation_id:
            raise ValueError("raw_observation_id must be a non-empty string")
        mac = hmac.new(key, raw_observation_id.encode("utf-8"), hashlib.sha256)
        return "obs_" + mac.hexdigest()[:PSEUDONYM_LENGTH_HEX]

    return pseudonymise


def is_pseudonym(candidate: str) -> bool:
    """True when ``candidate`` matches the pseudonym shape produced above."""

    if not isinstance(candidate, str):
        return False
    if not candidate.startswith("obs_"):
        return False
    suffix = candidate[4:]
    if len(suffix) != PSEUDONYM_LENGTH_HEX:
        return False
    return all(c in "0123456789abcdef" for c in suffix)
