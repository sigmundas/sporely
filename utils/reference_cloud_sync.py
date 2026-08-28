"""Compatibility seam for normalized reference-library cloud sync.

Stage 4a deliberately provides a side-effect-free facade only. Later Stage 4
slices add durable state and remote behavior behind this boundary; production
orchestration does not invoke it until Stage 4h.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ReferenceSyncResult:
    """Typed outcome returned by the normalized reference-sync subsystem."""

    pushed: int = 0
    pulled: int = 0
    errors: tuple[str, ...] = ()


def sync_reference_library(_client: object) -> ReferenceSyncResult:
    """Return the Stage 4a no-op result without inspecting external state."""

    return ReferenceSyncResult()


def merge_reference_sync_result(
    legacy_result: dict[str, object],
    reference_result: ReferenceSyncResult,
) -> dict[str, object]:
    """Preserve the legacy result exactly while Stage 4a remains a no-op."""

    if reference_result != ReferenceSyncResult():
        raise ValueError("Stage 4a supports only empty reference sync results")
    return legacy_result
