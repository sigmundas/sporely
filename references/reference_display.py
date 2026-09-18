"""One reading of what a reference source actually contains.

Every surface of the Add-reference dialog — the Library list badges, the
comparison table in the shared preview pane and the Community / My-observations
panes — needs the same answer to one question: *what did this source really
report?* Before this module each widget answered it by reaching into database
columns and ``data_kind`` strings on its own, which is how a typical range
printed by a 1960s monograph ends up labelled as if it were a percentile
interval computed from measured spores.

The projection here is deliberately small and deliberately pure:

* it creates no widgets, imports no Qt and translates nothing — a
  :class:`DataLabel` names a *meaning* (``raw_data``, ``published_range``,
  ``percentile_range``) and carries the numbers a label needs, so the UI layer
  owns the wording and the translation catalogue;
* it never computes a statistic the source did not state. There is no midpoint
  mean, no derived median and no inferred percentile;
* it reads stored content through the frozen contract in
  :mod:`references.measurement_content` rather than re-parsing JSON.

``ui/measurement_content_view.py`` remains the *editor's* presentation layer:
it turns typed content into translated meaning tags for the reference editors.
This module answers the chooser's coarser question (which compact badge does a
row deserve, and which values may a comparison row show) and is usable from
non-Qt tests and from ``database/``.

Four rules carry this module's scientific weight:

1. **``Raw data`` requires actual individual points.** A ``data_kind`` column
   saying ``raw_points`` proves nothing; :attr:`SourceDisplay.raw_point_count`
   comes from decoding the stored points.
2. **A percentile label requires explicit percentile bounds.** Only a
   ``percentile_interval`` core descriptor with bounds produces
   :class:`DataLabel` kind ``percentile_range``; ``5–95% range`` is then the
   UI's rendering of bounds ``(5, 95)`` and nothing else.
3. **Typical / reported / unspecified inner ranges are published ranges.** The
   compact badge collapses them to ``published_range`` while
   :attr:`MetricDisplay.core_kind` preserves the precise descriptor for the
   detail view.
4. **``parmasto`` is provenance, not a data kind.** It describes how spores
   were measured and reported; it never changes which badge a row gets.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Iterable, Literal, Mapping

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
    content_from_row,
)

#: Where a candidate came from. This is provenance for the chooser, not a
#: statement about content: a Library row and a Community result can carry
#: exactly the same statistics.
SourceKind = Literal["library", "community", "observation", "manual"]

#: The meaning of a compact badge. ``none`` means the source states no range
#: at all, which is a real state the chooser must be able to show honestly.
DataLabelKind = Literal["raw_data", "published_range", "percentile_range", "none"]


@dataclass(frozen=True)
class DataLabel:
    """What a row's compact badge means, without saying it in any language.

    ``percentile_bounds`` is set only for ``percentile_range`` and always came
    from an explicit descriptor, so the UI can render ``5–95% range`` for
    ``(5.0, 95.0)`` and ``10–90% range`` for ``(10.0, 90.0)`` without ever
    guessing which one a bare inner range meant.
    """

    kind: DataLabelKind
    percentile_bounds: tuple[float, float] | None = None


@dataclass(frozen=True)
class MetricDisplay:
    """What one metric (length / width / Q) of one source contains.

    Ranges are ``(low, high)`` pairs only when *both* stored bounds exist: a
    half-open pair cannot be drawn as an interval and is not silently closed
    with the other pair's value. ``outer_kind`` / ``core_kind`` are the
    explicit descriptor kinds, or ``None`` for a legacy row that stored numbers
    without ever saying what they mean.
    """

    metric: str
    outer_range: tuple[float, float] | None = None
    outer_kind: str | None = None
    core_range: tuple[float, float] | None = None
    core_kind: str | None = None
    percentile_bounds: tuple[float, float] | None = None
    scalar_mean: float | None = None
    mean_interval: IntervalStatistic | None = None
    median: ScalarStatistic | IntervalStatistic | None = None
    sd: ScalarStatistic | None = None

    @property
    def has_any_range(self) -> bool:
        return self.outer_range is not None or self.core_range is not None

    @property
    def has_centre(self) -> bool:
        """Whether the source stated *some* centre for this metric.

        A midpoint of a range is not a centre, so a metric with only an inner
        range answers ``False`` here.
        """
        return (
            self.scalar_mean is not None
            or self.mean_interval is not None
            or self.median is not None
        )

    @property
    def is_percentile_core(self) -> bool:
        return self.core_kind == "percentile_interval" and self.percentile_bounds is not None


@dataclass(frozen=True)
class SourceDisplay:
    """The complete display-semantics projection of one reference source."""

    source_kind: SourceKind
    metrics: Mapping[str, MetricDisplay] = field(default_factory=dict)
    raw_point_count: int = 0
    sample_size: int | None = None
    specimen_count: int | None = None
    measurement_method: str | None = None
    #: ``data_kind`` exactly as stored, for diagnostics only. Never consulted
    #: when choosing a label (rules 1 and 4).
    stored_data_kind: str | None = None
    #: Version of a ``measurement_details_json`` this binary cannot interpret.
    #: Such content is inspect-only: no descriptor is guessed from it.
    unsupported_details_version: int | None = None
    #: True when stored details could not be decoded at all (malformed JSON or
    #: a version-1 object of the wrong shape). The row still shows its plain
    #: numeric columns rather than disappearing from the chooser.
    details_unreadable: bool = False

    @property
    def has_raw_points(self) -> bool:
        return self.raw_point_count > 0

    @property
    def inspect_only(self) -> bool:
        """Whether the stored details must not be edited or re-encoded."""
        return self.unsupported_details_version is not None or self.details_unreadable

    @property
    def has_any_range(self) -> bool:
        return any(m.has_any_range for m in self.metrics.values())

    @property
    def has_reported_descriptor(self) -> bool:
        """Whether any range in this source carries an explicit meaning.

        ``False`` is the legacy state: numbers exist, but the source never said
        whether its inner range is typical, reported or a percentile interval.
        """
        return any(
            m.outer_kind is not None or m.core_kind is not None
            for m in self.metrics.values()
        )

    def metric(self, metric: str) -> MetricDisplay:
        """This source's projection for one metric, empty rather than missing."""
        return self.metrics.get(metric, MetricDisplay(metric=metric))

    @property
    def data_label(self) -> DataLabel:
        """The compact badge this source honestly deserves.

        Precedence: real points beat everything, an explicit percentile
        interval beats a plain range, and every other stated range — typical,
        reported or unspecified — is a published range.
        """
        if self.has_raw_points:
            return DataLabel(kind="raw_data")
        for name in METRICS:
            body = self.metrics.get(name)
            if body is not None and body.is_percentile_core:
                return DataLabel(
                    kind="percentile_range", percentile_bounds=body.percentile_bounds
                )
        if self.has_any_range:
            return DataLabel(kind="published_range")
        return DataLabel(kind="none")


# --- Construction from typed content -----------------------------------------


def _pair(content: MeasurementContent, columns: tuple[str, str]) -> tuple[float, float] | None:
    low = getattr(content, columns[0], None)
    high = getattr(content, columns[1], None)
    if low is None or high is None:
        return None
    return (float(low), float(high))


def _metric_details(details: Any, metric: str) -> MetricDetails | None:
    if not isinstance(details, MeasurementDetails):
        return None
    return details.metrics.get(metric)


def _metric_display(content: MeasurementContent, metric: str) -> MetricDisplay:
    body = _metric_details(content.details, metric)
    core_kind = None
    percentile_bounds = None
    if body is not None and body.core_range is not None:
        core_kind = body.core_range.kind
        if core_kind == "percentile_interval":
            percentile_bounds = body.core_range.percentile_bounds
    mean = getattr(content, SCALAR_MEAN[metric], None)
    return MetricDisplay(
        metric=metric,
        outer_range=_pair(content, OUTER_PAIR[metric]),
        outer_kind=(
            body.outer_range.kind if body is not None and body.outer_range is not None else None
        ),
        core_range=_pair(content, CORE_PAIR[metric]),
        core_kind=core_kind,
        percentile_bounds=percentile_bounds,
        scalar_mean=(float(mean) if mean is not None else None),
        mean_interval=(body.mean_interval if body is not None else None),
        median=(body.median if body is not None else None),
        sd=(body.sd if body is not None else None),
    )


def raw_point_count(raw_points_json: str | None) -> int:
    """Number of individual measurements stored as points, ``0`` when none.

    Only a JSON list counts, and only its length. Malformed text answers ``0``
    rather than raising: an unreadable points blob means the chooser may not
    claim raw data, not that the row vanishes.
    """
    if not raw_points_json or not isinstance(raw_points_json, str):
        return 0
    try:
        decoded = json.loads(raw_points_json)
    except ValueError:
        return 0
    if not isinstance(decoded, list):
        return 0
    return len(decoded)


def display_from_content(
    content: MeasurementContent,
    *,
    source_kind: SourceKind,
    raw_points: int | None = None,
    details_unreadable: bool = False,
) -> SourceDisplay:
    """Project already-decoded typed content.

    ``raw_points`` overrides the count derived from
    ``content.raw_points_json`` for a source that carries its points outside
    the content object (a personal observation, a Community detail).
    """
    details = content.details
    count = raw_points if raw_points is not None else raw_point_count(content.raw_points_json)
    return SourceDisplay(
        source_kind=source_kind,
        metrics={metric: _metric_display(content, metric) for metric in METRICS},
        raw_point_count=max(0, int(count)),
        sample_size=content.sample_size,
        specimen_count=content.specimen_count,
        measurement_method=content.measurement_method,
        stored_data_kind=content.data_kind,
        unsupported_details_version=(
            details.schema_version if isinstance(details, UnsupportedMeasurementDetails) else None
        ),
        details_unreadable=details_unreadable,
    )


def display_from_row(row: Mapping[str, Any], *, source_kind: SourceKind = "library") -> SourceDisplay:
    """Project one stored measurement-set row (or any mapping shaped like one).

    Details that cannot be decoded are reported through
    :attr:`SourceDisplay.details_unreadable` instead of propagating, so one
    corrupt row cannot empty the chooser. The row's numeric columns are still
    projected: they are stored values, not an interpretation.
    """
    try:
        content = content_from_row(row)
    except MeasurementContentError:
        salvaged = dict(row)
        salvaged["measurement_details_json"] = None
        return display_from_content(
            content_from_row(salvaged), source_kind=source_kind, details_unreadable=True
        )
    return display_from_content(content, source_kind=source_kind)


def display_from_points(
    points: Iterable[Any], *, source_kind: SourceKind = "observation"
) -> SourceDisplay:
    """Project a source that *is* a set of individual measurements.

    No range, mean or median is computed from the points: summarising them is
    the plotting layer's job, and a chooser badge must not imply the source
    reported a statistic it never printed.
    """
    count = len(list(points or []))
    return SourceDisplay(
        source_kind=source_kind,
        metrics={metric: MetricDisplay(metric=metric) for metric in METRICS},
        raw_point_count=count,
        sample_size=count or None,
    )


#: The percentile bounds a Community aggregate's ``*_p05`` / ``*_p95`` columns
#: mean. They are computed from the contributors' individual measurements, so
#: unlike a printed monograph range they really are a percentile interval.
COMMUNITY_PERCENTILE_BOUNDS: tuple[float, float] = (5.0, 95.0)


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def community_summary_content(payload: Mapping[str, Any]) -> MeasurementContent:
    """Typed content for one Community aggregate.

    The cloud aggregate carries ``*_min`` / ``*_max`` extremes, a ``*_p05`` /
    ``*_p95`` interval, a ``*_p50`` median and a ``*_avg`` mean per metric.
    Every one of those has an exact home in the frozen contract — the inner
    pair with an explicit ``percentile_interval`` descriptor, the median as a
    reported statistic, the mean in the scalar column — so the Community path
    needs no new column and no new enum. Q carries no percentile pair in the
    aggregate, so it gets extremes, median and mean only.
    """
    columns: dict[str, Any] = {}
    metrics: dict[str, MetricDetails] = {}
    for metric in METRICS:
        low, high = OUTER_PAIR[metric]
        core_low, core_high = CORE_PAIR[metric]
        columns[low] = _number(payload.get(f"{metric}_min"))
        columns[high] = _number(payload.get(f"{metric}_max"))
        p05 = _number(payload.get(f"{metric}_p05"))
        p95 = _number(payload.get(f"{metric}_p95"))
        columns[core_low] = p05
        columns[core_high] = p95
        columns[SCALAR_MEAN[metric]] = _number(payload.get(f"{metric}_avg"))
        p50 = _number(payload.get(f"{metric}_p50"))
        body = MetricDetails(
            outer_range=(
                RangeDescriptor(kind="reported_extremes")
                if columns[low] is not None and columns[high] is not None
                else None
            ),
            core_range=(
                RangeDescriptor(
                    kind="percentile_interval",
                    percentile_bounds=COMMUNITY_PERCENTILE_BOUNDS,
                )
                if p05 is not None and p95 is not None
                else None
            ),
            median=(ScalarStatistic(value=p50) if p50 is not None else None),
        )
        if not body.is_empty():
            metrics[metric] = body
    return MeasurementContent(
        character="spore_size",
        data_kind="summary",
        mount_medium=(payload.get("mount_medium") or None),
        stain=(payload.get("stain") or None),
        details=MeasurementDetails(metrics=metrics) if metrics else None,
        **columns,
    )


def display_from_community_summary(payload: Mapping[str, Any]) -> SourceDisplay:
    """Project one Community aggregate result.

    The aggregate is a summary of other people's points, not points this
    desktop holds, so it never earns the ``raw_data`` badge. The separate
    raw-points mode of the same Community result does, through
    :func:`display_from_points`.
    """
    return display_from_content(
        community_summary_content(payload), source_kind="community", raw_points=0
    )


__all__ = [
    "COMMUNITY_PERCENTILE_BOUNDS",
    "DataLabel",
    "DataLabelKind",
    "MetricDisplay",
    "SourceDisplay",
    "SourceKind",
    "community_summary_content",
    "display_from_community_summary",
    "display_from_content",
    "display_from_points",
    "display_from_row",
    "raw_point_count",
]
