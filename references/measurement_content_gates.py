"""Rollout gates for enhanced measurement content.

Two release decisions recorded in
``docs/reference-data/measurement-content-contract.md`` (sections 6, 7 and 13)
are implemented here as explicit switches rather than as prose, a waiting
period or release notes:

``MINIMUM_SUPPORTED_READER_VERSION_GATE_OPEN``
    The *minimum-supported-reader-version* gate of contract section 7. A
    desktop older than rollout step 1 rejects a whole observation-use feed
    that contains one snapshot it does not understand, so snapshot version 2
    must not become frozen evidence until every supported desktop can read it.
    While the gate is closed this desktop refuses to attach, refresh or adopt
    an *enhanced* measurement set instead of freezing a lossy version-1
    snapshot of it. Reading version 2 is never gated: readers ship first.

``MINIMUM_SUPPORTED_DESKTOP_VERSION_GATE_OPEN``
    The *minimum-supported-desktop-version* policy of contract section 6. A
    desktop that has never opened an upgraded library has no write barrier, so
    importing an enhanced bundle there inserts a legacy-only projection of it
    (contract section 11, last row). While the gate is closed this desktop
    refuses to *export* a bundle whose reference library contains an enhanced
    row, so no enhanced bundle exists for such a desktop to mangle.

Both gates ship closed. Opening one is a one-line, reversible edit here plus a
release decision; closing it again disables enhanced editing and enhanced
transfer while leaving every stored extension value untouched, because nothing
in this module rewrites, strips or enriches persisted content.

The accessors are read at call time, so a test may set the module attributes
(``monkeypatch.setattr``) to exercise the open state.
"""
from __future__ import annotations


#: Contract section 7: v2 snapshot emission and enhanced attachments.
MINIMUM_SUPPORTED_READER_VERSION_GATE_OPEN = False

#: Contract section 6: enhanced bundle export.
MINIMUM_SUPPORTED_DESKTOP_VERSION_GATE_OPEN = False


ENHANCED_ATTACHMENT_BLOCKED_MESSAGE = (
    "this reference entry carries reported statistics that this version "
    "cannot attach as frozen evidence yet"
)

ENHANCED_BUNDLE_BLOCKED_MESSAGE = (
    "this reference library carries reported statistics that this version "
    "cannot export to a shareable bundle yet"
)


def enhanced_attachments_enabled() -> bool:
    """Whether enhanced measurement sets may become frozen v2 evidence."""
    return MINIMUM_SUPPORTED_READER_VERSION_GATE_OPEN


def enhanced_bundle_export_enabled() -> bool:
    """Whether a bundle may carry enhanced reference rows."""
    return MINIMUM_SUPPORTED_DESKTOP_VERSION_GATE_OPEN


def enhanced_editing_enabled() -> bool:
    """Whether the two reference editors may persist enhanced content.

    This is not a third switch: it is the reader gate read from the editors'
    side. A measurement set exists to be attached to an observation, and
    :func:`enhanced_attachments_enabled` governs whether an enhanced one may
    become frozen evidence. Storing enhanced rows while that is false would
    only move the refusal later — the user would fill in a literature table,
    save it, and be told at attach time that the evidence cannot be frozen.
    While the gate is closed both editors therefore *show* the reported
    statistics for review and persist the legacy-only projection of them,
    which is exactly what the same source produced before this contract
    existed. Neither editor strips anything already stored.
    """
    return enhanced_attachments_enabled()
