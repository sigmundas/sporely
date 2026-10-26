"""Enhanced measurement support is active for the single-user desktop.

The reader and bundle rollout policies were retired together on 2026-10-26.
There is no old-client population prerequisite. These accessors remain as the
shared capability boundary used by editors, attachments and exports; the
regression fixtures can still exercise the historical loss refusal.
Unsupported content is still preserved or rejected by the typed codec.
"""
from __future__ import annotations

from .measurement_content import MeasurementContent, legacy_projection_losses


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
    return True


def enhanced_bundle_export_enabled() -> bool:
    """Whether a bundle may carry enhanced reference rows."""
    return True


def enhanced_editing_enabled() -> bool:
    """Full reported content is persisted whenever attachments support it."""
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
