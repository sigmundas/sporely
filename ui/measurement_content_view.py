"""Presentation helpers for typed measurement content (contract version 1).

Both reference editors and the shared preview pane need the same three
things from a :class:`~references.measurement_content.MeasurementContent`:

* a **compact meaning tag** for each column pair the source described, with
  an accessible explanation behind it;
* the **reported median and standard deviation**, rendered apart from any
  mean so the two are never confused;
* one **mean field that accepts a scalar or an interval**, parsed and
  formatted without losing the distinction between ``9.2`` and ``9.2-9.2``.

Everything here is pure: no widget is created and no QApplication is
required, so the rules are unit-testable on their own. User-visible strings
call ``QCoreApplication.translate`` with a literal ``MeasurementContent``
context at every site — deliberately not through a shorter local wrapper,
because ``pyside6-lupdate`` reads the literals statically and a wrapper
would hide every string in this module from the translation files. The
module is listed in ``tools/update_translations.sh`` for the same reason.

Two rules this module exists to keep:

1. **Nothing is derived.** A tag states what the source said about numbers
   that already exist. No midpoint, CV, species mean or matching eligibility
   is computed, and a percentile number is printed only for a descriptor
   that explicitly carries percentile bounds.
2. **A median is never a mean.** :func:`reported_statistics` returns medians
   and standard deviations; :func:`format_mean_cell` and
   :func:`apply_mean_cell` touch only the scalar mean column and the mean
   interval. The two never exchange values.
"""
from __future__ import annotations

from dataclasses import dataclass, replace

from PySide6.QtCore import QCoreApplication

from references.measurement_content import (
    CORE_PAIR,
    METRICS,
    OUTER_PAIR,
    SCALAR_MEAN,
    IntervalStatistic,
    MeasurementContent,
    MeasurementContentError,
    MeasurementDetails,
    MetricDetails,
    RangeDescriptor,
    ScalarStatistic,
    UnsupportedMeasurementDetails,
    clear_pair,
    clear_statistic,
    set_mean_interval,
    set_scalar_mean,
)

#: The interval kind recorded when a user types an interval into a mean
#: field. ``reported_range`` is the honest reading of a printed ``a-b`` mean:
#: it is what the source reported, not a claim about a distribution. The
#: parser uses the same kind for a table's interval mean cell.
MEAN_INTERVAL_KIND = "reported_range"

_DASHES = "‐‑‒–—―−"


def metric_label(metric: str) -> str:
    """Full metric name, e.g. for an explanation sentence."""
    if metric == "length":
        return QCoreApplication.translate("MeasurementContent", "Length")
    if metric == "width":
        return QCoreApplication.translate("MeasurementContent", "Width")
    if metric == "q":
        return QCoreApplication.translate("MeasurementContent", "Q")
    return metric


def metric_short_label(metric: str) -> str:
    """One-character metric prefix for a compact tag."""
    if metric == "length":
        return QCoreApplication.translate("MeasurementContent", "L")
    if metric == "width":
        return QCoreApplication.translate("MeasurementContent", "W")
    if metric == "q":
        return QCoreApplication.translate("MeasurementContent", "Q")
    return metric


@dataclass(frozen=True)
class MeaningTag:
    """A compact label plus the sentence that explains it.

    ``label`` goes on screen; ``explanation`` goes into the tooltip and the
    accessible description, so the compact form is never the only way to
    learn what a tag means.
    """

    label: str
    explanation: str


# --- Number and statistic formatting -----------------------------------------


def format_number(value: float | int | None) -> str:
    """Compact numeric text; empty for ``None``."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return ""
    return "%g" % float(value)


def format_statistic(statistic: ScalarStatistic | IntervalStatistic | None) -> str:
    """Scalar as one number, interval as ``lower-upper``.

    Equal endpoints still render as an interval (``9.2-9.2``), because the
    contract keeps that distinction and collapsing it here would make the
    editor unable to round-trip what the source printed.
    """
    if statistic is None:
        return ""
    if isinstance(statistic, ScalarStatistic):
        return format_number(statistic.value)
    return f"{format_number(statistic.lower)}-{format_number(statistic.upper)}"


# --- Scalar-or-interval mean input -------------------------------------------


def _bad_mean_message(text: str | None) -> str:
    # The literal is repeated at both call sites on purpose: lupdate reads
    # translate() arguments statically, so a shared constant would leave this
    # message out of every translation file.
    return QCoreApplication.translate(
        "MeasurementContent",
        "A mean must be a single value or an interval such as 9.2-11.7, not {value}",
    ).format(value=(text or "").strip())


def _require_float(part: str, original: str | None) -> float:
    try:
        return float(part)
    except (TypeError, ValueError):
        raise MeasurementContentError(_bad_mean_message(original)) from None


def parse_mean_cell(text: str | None) -> ScalarStatistic | IntervalStatistic | None:
    """Parse one mean field: a scalar, an interval, or nothing.

    ``None`` means the field is empty. Any other unrecognised text raises
    :class:`~references.measurement_content.MeasurementContentError` rather
    than silently reading a prefix, so a typo can never become a number the
    user did not enter.
    """
    chunk = (text or "").strip()
    if not chunk:
        return None
    for dash in _DASHES:
        chunk = chunk.replace(dash, "-")
    chunk = chunk.replace(",", ".")
    parts = [part.strip() for part in chunk.split("-")]
    if len(parts) == 1:
        return ScalarStatistic(value=_require_float(parts[0], text))
    if len(parts) == 2:
        lower = _require_float(parts[0], text)
        upper = _require_float(parts[1], text)
        if lower > upper:
            raise MeasurementContentError(
                QCoreApplication.translate(
                    "MeasurementContent",
                    "Mean interval endpoints are in the wrong order: {value}",
                ).format(value=(text or "").strip())
            )
        return IntervalStatistic(lower=lower, upper=upper, kind=MEAN_INTERVAL_KIND)
    raise MeasurementContentError(_bad_mean_message(text))


def format_mean_cell(content: MeasurementContent, metric: str) -> str:
    """Render ``metric``'s mean, whichever shape it has.

    The scalar mean column and the mean interval are mutually exclusive by
    contract rule 4, so at most one of them is ever non-empty.
    """
    scalar = getattr(content, SCALAR_MEAN[metric], None)
    if scalar is not None:
        return format_number(scalar)
    body = _metric_details(content, metric)
    if body is not None and body.mean_interval is not None:
        return format_statistic(body.mean_interval)
    return ""


def apply_mean_cell(
    content: MeasurementContent, metric: str, text: str | None
) -> MeasurementContent:
    """Apply a mean field's text through the contract's edit operations.

    Empty clears the statistic in both its shapes (``clear_statistic``), a
    scalar switches to :func:`set_scalar_mean` and an interval to
    :func:`set_mean_interval`. Each transition removes the other shape, so
    the two can never both be stored.
    """
    statistic = parse_mean_cell(text)
    if statistic is None:
        return clear_statistic(content, metric, "mean")
    if isinstance(statistic, ScalarStatistic):
        return set_scalar_mean(content, metric, statistic.value)
    return set_mean_interval(
        content, metric, statistic.lower, statistic.upper, statistic.kind
    )


# --- Tags ---------------------------------------------------------------------


def _metric_details(content: MeasurementContent, metric: str) -> MetricDetails | None:
    details = content.details
    if not isinstance(details, MeasurementDetails):
        return None
    return details.metrics.get(metric)


def _core_tag(metric: str, descriptor: RangeDescriptor) -> MeaningTag:
    name = metric_label(metric)
    short = metric_short_label(metric)
    if descriptor.kind == "percentile_interval" and descriptor.percentile_bounds:
        low, high = descriptor.percentile_bounds
        low_text, high_text = format_number(low), format_number(high)
        return MeaningTag(
            label=QCoreApplication.translate(
                "MeasurementContent", "{metric} inner {low}–{high}%"
            ).format(metric=short, low=low_text, high=high_text),
            explanation=QCoreApplication.translate(
                "MeasurementContent",
                "{metric}: the source states the inner range is its "
                "{low}th–{high}th percentile interval.",
            ).format(metric=name, low=low_text, high=high_text),
        )
    if descriptor.kind == "typical_range":
        return MeaningTag(
            label=QCoreApplication.translate(
                "MeasurementContent", "{metric} inner: typical"
            ).format(metric=short),
            explanation=QCoreApplication.translate(
                "MeasurementContent",
                "{metric}: the source calls the inner range its typical "
                "range. No percentile is implied.",
            ).format(metric=name),
        )
    if descriptor.kind == "reported_range":
        return MeaningTag(
            label=QCoreApplication.translate(
                "MeasurementContent", "{metric} inner: as reported"
            ).format(metric=short),
            explanation=QCoreApplication.translate(
                "MeasurementContent",
                "{metric}: the inner range is reproduced as the source printed it.",
            ).format(metric=name),
        )
    return MeaningTag(
        label=QCoreApplication.translate(
            "MeasurementContent", "{metric} inner: unspecified"
        ).format(metric=short),
        explanation=QCoreApplication.translate(
            "MeasurementContent",
            "{metric}: inner range — interpretation unspecified. The source "
            "gives no cut-off rule, so this is not a percentile.",
        ).format(metric=name),
    )


def _outer_tag(metric: str, descriptor: RangeDescriptor) -> MeaningTag:
    return MeaningTag(
        label=QCoreApplication.translate(
            "MeasurementContent", "{metric} extremes: reported"
        ).format(metric=metric_short_label(metric)),
        explanation=QCoreApplication.translate(
            "MeasurementContent",
            "{metric}: the outer values are extreme observations reported by "
            "the source, not a calculated bound.",
        ).format(metric=metric_label(metric)),
    )


def metric_tags(content: MeasurementContent, metric: str) -> list[MeaningTag]:
    """Compact meaning tags for one metric, outer pair first.

    A mean reported as an interval also earns a tag, because a reader who
    sees ``9.2-11.7`` in a mean field needs to know it is one statistic and
    not a range of measurements.
    """
    body = _metric_details(content, metric)
    if body is None:
        return []
    tags: list[MeaningTag] = []
    if body.outer_range is not None:
        tags.append(_outer_tag(metric, body.outer_range))
    if body.core_range is not None:
        tags.append(_core_tag(metric, body.core_range))
    if body.mean_interval is not None:
        tags.append(
            MeaningTag(
                label=QCoreApplication.translate(
                    "MeasurementContent", "{metric} mean: interval"
                ).format(metric=metric_short_label(metric)),
                explanation=QCoreApplication.translate(
                    "MeasurementContent",
                    "{metric}: the source reports the mean as an interval "
                    "rather than a single value.",
                ).format(metric=metric_label(metric)),
            )
        )
    return tags


def content_tags(content: MeasurementContent) -> list[MeaningTag]:
    """Every metric's tags in length / width / Q order."""
    tags: list[MeaningTag] = []
    for metric in METRICS:
        tags.extend(metric_tags(content, metric))
    return tags


# --- Reported median and standard deviation ----------------------------------


def reported_statistics(content: MeasurementContent, metric: str) -> list[MeaningTag]:
    """One metric's reported median and standard deviation.

    Deliberately excludes every form of the mean: this list is what the
    details area shows *next to* the mean field, never inside it.
    """
    body = _metric_details(content, metric)
    if body is None:
        return []
    name = metric_label(metric)
    short = metric_short_label(metric)
    entries: list[MeaningTag] = []
    if body.median is not None:
        entries.append(
            MeaningTag(
                label=QCoreApplication.translate(
                    "MeasurementContent", "{metric} median {value}"
                ).format(metric=short, value=format_statistic(body.median)),
                explanation=QCoreApplication.translate(
                    "MeasurementContent",
                    "{metric}: median as reported by the source. A median is "
                    "never used as a mean.",
                ).format(metric=name),
            )
        )
    if body.sd is not None:
        entries.append(
            MeaningTag(
                label=QCoreApplication.translate(
                    "MeasurementContent", "{metric} S.D. {value}"
                ).format(metric=short, value=format_number(body.sd.value)),
                explanation=QCoreApplication.translate(
                    "MeasurementContent",
                    "{metric}: standard deviation as reported by the source.",
                ).format(metric=name),
            )
        )
    return entries


def content_reported_statistics(content: MeasurementContent) -> list[MeaningTag]:
    """Every metric's reported median and standard deviation."""
    entries: list[MeaningTag] = []
    for metric in METRICS:
        entries.extend(reported_statistics(content, metric))
    return entries


# --- Whole-content helpers ----------------------------------------------------


def unsupported_details_notice(content: MeasurementContent) -> str | None:
    """Text for details written by a newer version, otherwise ``None``.

    Such content is inspect-only everywhere: the contract forbids editing or
    re-encoding it, so the editors say so instead of showing a tag list they
    cannot interpret.
    """
    details = content.details
    if not isinstance(details, UnsupportedMeasurementDetails):
        return None
    return QCoreApplication.translate(
        "MeasurementContent",
        "This entry carries reported statistics written by a newer version "
        "of Sporely (format {version}). They are preserved unchanged and "
        "cannot be edited here.",
    ).format(version=details.schema_version)


def describe_projection_losses(losses: list[tuple[str, str]]) -> list[str]:
    """One sentence per thing a version-1 row could not carry.

    Takes the ``(metric, kind)`` pairs
    :func:`~references.measurement_content.legacy_projection_losses` produces
    and words them, so the contract module stays free of translated text and
    the editors do not each invent their own phrasing.

    Grouped by kind, not by metric. A headed literature table typically
    reports the same four things for all three metrics, and one sentence per
    pair turned the notice into twelve near-identical lines that nobody
    reads. The metric names stay in the sentence, because a reader fixing
    the entry still needs to know which rows are affected.
    """
    wording = {
        "percentile_interval": QCoreApplication.translate(
            "MeasurementContent",
            "{metric}: the inner range is an explicit percentile interval, "
            "which would be stored as an ordinary published range.",
        ),
        "mean_interval": QCoreApplication.translate(
            "MeasurementContent",
            "{metric}: the mean is reported as an interval, which this "
            "version has nowhere to store — it would be lost.",
        ),
        "median": QCoreApplication.translate(
            "MeasurementContent", "{metric}: the reported median would be lost."
        ),
        "sd": QCoreApplication.translate(
            "MeasurementContent",
            "{metric}: the reported standard deviation would be lost.",
        ),
        # Two different outcomes behind one message, both bad and both
        # blocked. With no reported extremes the typical range slides into
        # the extreme columns and is relabelled; with extremes *also*
        # reported the extremes win and the typical range is dropped
        # outright. The wording has to hold for both, because an earlier
        # version promised the first and silently did the second.
        "q_core_pair": QCoreApplication.translate(
            "MeasurementContent",
            "{metric}: the typical range has no column of its own in this "
            "version, so it cannot be stored beside the extremes.",
        ),
    }
    grouped: dict[str, list[str]] = {}
    messages: list[str] = []
    for metric, kind in losses:
        if kind == "unsupported_details":
            messages.append(
                QCoreApplication.translate(
                    "MeasurementContent",
                    "This entry carries reported statistics written by a "
                    "newer version of Sporely. They are preserved unchanged "
                    "and cannot be re-saved from here.",
                )
            )
            continue
        if kind not in wording:
            continue
        names = grouped.setdefault(kind, [])
        label = metric_label(metric)
        if label not in names:
            names.append(label)
    separator = QCoreApplication.translate("MeasurementContent", ", ")
    for kind, template in wording.items():
        names = grouped.get(kind)
        if names:
            messages.append(template.format(metric=separator.join(names)))
    return messages


def has_reported_content(content: MeasurementContent) -> bool:
    """Whether anything in this content needs the extension columns."""
    if content.q_core_min is not None or content.q_core_max is not None:
        return True
    details = content.details
    if isinstance(details, UnsupportedMeasurementDetails):
        return True
    if isinstance(details, MeasurementDetails):
        return not details.is_empty()
    return False


def metrics_with_range_tags(content: MeasurementContent) -> list[str]:
    """Metrics whose range interpretation the source (or parser) stated."""
    found: list[str] = []
    for metric in METRICS:
        body = _metric_details(content, metric)
        if body is not None and (
            body.outer_range is not None or body.core_range is not None
        ):
            found.append(metric)
    return found


def without_range_tags(content: MeasurementContent, metric: str) -> MeasurementContent:
    """Retract one metric's range interpretation, keeping everything else.

    This is the correction path for a tag that is simply wrong — a heading
    the parser read as a percentile interval that is not one, or an
    ``unspecified`` tag on a range the reader knows was never examined. The
    measured numbers stay, and so do the reported mean interval, median and
    standard deviation: only the *claim about what the range means* goes.

    Retraction is deliberately the only correction offered. Asserting a
    different descriptor kind would need a set-descriptor operation, which
    contract section 3 does not define, and the plan defers editable advanced
    details; a UI that let a reader type in a percentile the source never
    printed would manufacture exactly the evidence this contract exists to
    keep honest. Dropping a descriptor is always safe — contract rules 1-3
    are triggered by a descriptor being present.
    """
    details = content.details
    if not isinstance(details, MeasurementDetails):
        return content
    body = details.metrics.get(metric)
    if body is None:
        return content
    stripped = replace(body, outer_range=None, core_range=None)
    metrics = {name: value for name, value in details.metrics.items() if name != metric}
    if not stripped.is_empty():
        metrics[metric] = stripped
    return replace(
        content, details=MeasurementDetails(metrics=metrics) if metrics else None
    )


def without_reported_statistics(content: MeasurementContent) -> MeasurementContent:
    """Drop the meaning tags and the reported median / S.D.

    Every measured number survives, including a mean reported as an
    interval: that one is a value the user can see and retype in the mean
    field, so discarding it here would contradict the promise that the
    measured values are left alone. Emptying that field is how a mean
    interval goes away, and ``clear_pair`` is how a column pair does.

    Dropping descriptors is always safe: contract rules 1–3 are triggered
    *by* a descriptor being present, so content without one cannot violate
    them. Rule 4 is unaffected because the mean is not touched.
    """
    details = content.details
    if not isinstance(details, MeasurementDetails):
        return content
    kept: dict[str, MetricDetails] = {}
    for metric, body in details.metrics.items():
        if body is not None and body.mean_interval is not None:
            kept[metric] = MetricDetails(mean_interval=body.mean_interval)
    return replace(
        content, details=MeasurementDetails(metrics=kept) if kept else None
    )


# --- Folding editor values back into typed content ----------------------------


@dataclass(frozen=True)
class MetricInput:
    """One metric's editable values, as either editor holds them on screen.

    The two editors present the same five things in different widgets — a
    table row in the entry editor, a row of line edits in the library
    manager. Both reduce to this, so :func:`fold_metric_inputs` is the single
    owner of the transition rules and neither editor grows its own.
    """

    outer_min: float | None = None
    core_min: float | None = None
    core_max: float | None = None
    outer_max: float | None = None
    mean_text: str | None = None


def fold_metric_inputs(
    content: MeasurementContent,
    inputs: "dict[str, MetricInput]",
    *,
    strict: bool = True,
) -> MeasurementContent:
    """Fold on-screen values into ``content`` through the contract's operations.

    Plain numbers move by assignment — setting a bound is not a statement
    about meaning. Every *transition* goes through contract section 3:
    :func:`clear_pair` when a described pair is emptied, so a descriptor can
    never outlive the numbers it describes, and
    :func:`apply_mean_cell` (``clear_statistic`` / ``set_scalar_mean`` /
    ``set_mean_interval``) for the mean, so its two shapes can never both be
    stored.

    ``strict=True`` propagates a bad mean field as
    :class:`~references.measurement_content.MeasurementContentError`, for a
    form that validates on save. ``strict=False`` keeps that metric's previous
    typed mean instead, for a live editor that has already told the user why
    the text was refused and must not discard the rest of the edit.

    Content written by a newer version is returned untouched: it is
    inspect-only everywhere and must never be rewritten.
    """
    if content.details is not None and not isinstance(
        content.details, MeasurementDetails
    ):
        return content
    for metric in METRICS:
        values = inputs.get(metric)
        if values is None:
            continue
        low, high = CORE_PAIR[metric]
        outer_low, outer_high = OUTER_PAIR[metric]
        content = replace(
            content,
            **{
                outer_low: values.outer_min,
                low: values.core_min,
                high: values.core_max,
                outer_high: values.outer_max,
            },
        )
        if values.outer_min is None and values.outer_max is None:
            content = clear_pair(content, metric, "outer")
        if values.core_min is None and values.core_max is None:
            content = clear_pair(content, metric, "core")
        try:
            content = apply_mean_cell(content, metric, values.mean_text)
        except MeasurementContentError:
            if strict:
                raise
    return content


def compact_tag_text(tags: list[MeaningTag]) -> str:
    """Join tag labels for a one-line summary."""
    return " · ".join(tag.label for tag in tags if tag.label)


def explanation_text(tags: list[MeaningTag]) -> str:
    """Join tag explanations for a tooltip or accessible description."""
    return "\n".join(tag.explanation for tag in tags if tag.explanation)


__all__ = (
    "MEAN_INTERVAL_KIND",
    "MeaningTag",
    "MetricInput",
    "apply_mean_cell",
    "compact_tag_text",
    "content_reported_statistics",
    "content_tags",
    "explanation_text",
    "fold_metric_inputs",
    "format_mean_cell",
    "format_number",
    "format_statistic",
    "has_reported_content",
    "metric_label",
    "metric_short_label",
    "metric_tags",
    "metrics_with_range_tags",
    "parse_mean_cell",
    "reported_statistics",
    "unsupported_details_notice",
    "without_range_tags",
    "without_reported_statistics",
)
