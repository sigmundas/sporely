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

from .measurement_content import MeasurementContent, legacy_projection_losses


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

    One refinement, added with the manual-entry redesign: that legacy-only
    projection is only honest while it still says what the source said.
    For content it cannot represent at all — an explicit percentile
    interval, an interval mean, a median, a standard deviation, a Q
    typical range distinct from the Q extremes — the
    manual entry editor now *declines* the save rather than writing a row
    whose claim differs from the preview beside it. See
    :func:`blocking_projection_losses`. This narrows what gets written; it
    does not open either gate, and it leaves stored content untouched.
    """
    return enhanced_attachments_enabled()


#: Projection losses that must stop a save rather than be written lossily.
#:
#: These are the losses where a version-1 row would either lose a statistic
#: outright or assert something the source did not:
#:
#: - ``percentile_interval`` — an explicit 5–95% (or 10–90%, …) interval
#:   becomes an unlabelled inner pair, which contract N12/N14 forbid being
#:   presented as, or mistaken for, an ordinary published range;
#: - ``mean_interval``, ``median``, ``sd`` — reported statistics with no v1
#:   column at all, so they simply vanish;
#: - ``unsupported_details`` — content written by a newer version, which is
#:   inspect-only and must never be re-encoded from this binary.
#:
#: ``q_core_pair`` is included, and the reason is worth recording because an
#: earlier revision of this list excluded it. The argument for excluding it
#: was that the Q typical range has always fallen back into
#: ``q_min``/``q_max`` (``test_closed_gate_keeps_the_historical_q_bound_fallback``)
#: and is the only Q extent such a row carries anyway, so the fallback merely
#: relabels a core pair as extremes.
#:
#: That is false whenever the source states **both** Q ranges, which the
#: parser fully supports: ``Q = (1.1-)1.2-1.8(-1.9)`` yields outer 1.1/1.9
#: *and* core 1.2/1.8, the v1 fallback keeps the outer pair, and 1.2–1.8 is
#: discarded outright. That is two numbers gone, not a label changed. The
#: cost of including it is that the ordinary
#: ``(extreme–)typical–typical(–extreme) … Q = a–b`` notation is refused
#: while the gate is closed; losing a stated range silently is worse than
#: refusing it visibly.
BLOCKING_PROJECTION_LOSS_KINDS: frozenset[str] = frozenset(
    {
        "unsupported_details",
        "percentile_interval",
        "mean_interval",
        "median",
        "sd",
        "q_core_pair",
    }
)


def blocking_projection_losses(
    content: MeasurementContent | None,
) -> list[tuple[str, str]]:
    """Losses that must stop this content being saved right now.

    Empty while the reader gate is open: an enhanced row keeps everything, so
    there is nothing to lose. While the gate is closed, the editors refuse
    instead of freezing a row whose scientific claim differs from the source
    the user is looking at — the alternative the contract's rollout note
    warns against is moving the refusal to attach time, and the alternative
    this stage forbids is storing a "visually correct but semantically
    degraded row".
    """
    if content is None or enhanced_editing_enabled():
        return []
    return [
        loss
        for loss in legacy_projection_losses(content)
        if loss[1] in BLOCKING_PROJECTION_LOSS_KINDS
    ]
