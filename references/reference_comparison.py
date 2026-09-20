"""The comparison the Add-reference preview draws, as numbers rather than pixels.

:mod:`references.reference_display` answers *what did this source report?*. This
module answers the next question — *how does that sit beside the user's own
observation, on an axis that does not move?* — and answers it without importing
Qt, so every rule below is testable without a widget.

Three commitments shape the model:

**Nothing is invented.** A band exists only where a source stated two bounds or
where individual measurements were actually counted. A centre mark exists only
where a source stated a mean or a median. A published range that printed no
centre therefore produces a :class:`MetricSeries` with an empty ``centres``
tuple, and the widget has nothing to draw rather than a midpoint to draw.

**Every drawn thing says what it is.** :class:`BandView` and
:class:`CentreView` carry ``meaning`` / ``statistic`` and a
:data:`~references.reference_display.StatisticsOrigin`, so the caption beside a
band can state exactly which statistic it represents. The min/max of forty-two
measured spores, an author's printed typical range and an explicit 5–95
percentile interval are three different claims and never share a rendering
without a label that separates them.

**The axis is frozen.** :func:`build_domains` is called once per dialog
session, from the observation plus the candidate content already known, and its
:class:`MetricDomain` objects are then reused for every selection. Content that
arrives later and falls outside a domain is *clipped and marked*
(:attr:`BandView.clipped_low` / :attr:`BandView.clipped_high`), never
accommodated by silently rescaling: an axis that moves when the selection moves
makes two sources impossible to compare by eye, which is the whole purpose of
the pane.

A delta between the source and the observation is produced only by
:func:`_delta_between`, and only when both sides hold a scalar value of the
*same* statistic. A source's mean against the user's median is not a
comparison, and this module returns ``None`` rather than a number for it.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Iterable, Literal, Mapping, Sequence

from references.measurement_content import METRICS
from references.reference_display import SourceDisplay, StatisticsOrigin

#: Which half of a metric's rendering a band is. ``outer`` is the widest thing
#: the source stated (reported extremes, or the min/max of measured points);
#: ``core`` is the inner claim (a typical range, or an explicit percentile
#: interval). Both may be present, and when they are they must stay visually
#: distinguishable — contract N15's "min/max and 5–95% must remain
#: distinguishable when both are available".
BandRole = Literal["outer", "core"]

#: Which statistic a centre mark is. Only these two: a mode or a midpoint is
#: not a centre any source in this application reports.
CentreStatistic = Literal["mean", "median"]

#: Meaning of a band derived from individual measurements rather than from a
#: descriptor an author wrote. Kept distinct from the stored descriptor kinds
#: in :mod:`references.measurement_content` precisely so a caption can never
#: present a calculated extent as a published one.
MEASURED_EXTREMES = "measured_extremes"
#: Meaning of the inner band computed from individual measurements. Carries
#: :data:`MEASURED_PERCENTILE_BOUNDS`, so it renders with the same explicit
#: ``5–95%`` wording an explicitly stored percentile interval gets.
MEASURED_PERCENTILE = "measured_percentile_interval"
#: The percentile interval computed from points. Matches the ``p05`` / ``p95``
#: pair ``MainWindow._reference_stats_from_points`` already computes for the
#: plotting path, so the preview and the plot describe one observation the
#: same way.
MEASURED_PERCENTILE_BOUNDS: tuple[float, float] = (5.0, 95.0)

#: Domain used for a metric about which nothing at all is known. A dialog
#: opened on an observation with no measurements and an empty library still has
#: to draw an axis, and an arbitrary-looking but stable one beats a degenerate
#: zero-width one. These are ordinary basidiospore magnitudes in µm (Q being
#: dimensionless), not a claim about any particular taxon.
DEFAULT_DOMAIN: Mapping[str, tuple[float, float]] = {
    "length": (5.0, 15.0),
    "width": (3.0, 9.0),
    "q": (1.0, 2.5),
}

#: Smallest span a domain may have. Without a floor, a single source whose
#: length range is 9.8–10.1 µm would get an axis three tenths of a micrometre
#: wide, on which every later source is off-scale.
MINIMUM_SPAN: Mapping[str, float] = {"length": 4.0, "width": 2.0, "q": 0.4}

#: Fraction of the observed span added at each end, so a band that reaches the
#: extreme of the data does not sit flush against the edge of the track and
#: read as clipped when it is not.
DOMAIN_PADDING = 0.08


def _finite(value: Any) -> float | None:
    """``value`` as a float when it is a real, finite number, else ``None``."""
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


# --- Individual measurements --------------------------------------------------


@dataclass(frozen=True)
class SporePoint:
    """One measured spore, in the shape this module computes from.

    ``q`` is length/width when both were measured, and is never fabricated from
    one dimension. A point that carries only a length is still a point — it
    counts towards the length band — and simply contributes nothing to width
    or Q.
    """

    length: float | None = None
    width: float | None = None
    q: float | None = None

    def value(self, metric: str) -> float | None:
        return getattr(self, metric if metric != "q" else "q", None)


#: Every spelling of a measured dimension this application stores. The
#: ``_um``-suffixed pair is what ``MeasurementDB.get_measurements_for_observation``
#: returns; the bare pair is the curated-snapshot shape decoded out of
#: ``raw_points_json``.
_LENGTH_KEYS: tuple[str, ...] = ("length_um", "length", "l")
_WIDTH_KEYS: tuple[str, ...] = ("width_um", "width", "w")


def _first_number(point: Mapping[str, Any], keys: Sequence[str]) -> float | None:
    for key in keys:
        number = _finite(point.get(key))
        if number is not None:
            return number
    return None


def normalize_points(points: Iterable[Any] | None) -> tuple[SporePoint, ...]:
    """Decode whatever shape a caller holds into :class:`SporePoint` values.

    Accepts the observation table's ``length_um`` / ``width_um`` rows, the
    stored snapshot's ``length`` / ``width`` / ``q`` objects, and a bare number
    (a single-dimension length point, which the curated snapshot shape allows).
    Anything carrying no usable dimension is dropped rather than counted: an
    empty object must not inflate the ``n`` a caption prints.
    """
    decoded: list[SporePoint] = []
    for raw in points or ():
        if isinstance(raw, bool):
            continue
        if isinstance(raw, (int, float)):
            length = _finite(raw)
            if length is not None:
                decoded.append(SporePoint(length=length))
            continue
        if not isinstance(raw, Mapping):
            continue
        length = _first_number(raw, _LENGTH_KEYS)
        width = _first_number(raw, _WIDTH_KEYS)
        q = _finite(raw.get("q"))
        if q is None and length is not None and width:
            q = length / width
        if length is None and width is None and q is None:
            continue
        decoded.append(SporePoint(length=length, width=width, q=q))
    return tuple(decoded)


def point_extents(points: Iterable[Any] | None) -> dict[str, tuple[float, float]]:
    """The span individual measurements occupy, per metric — an *axis* input.

    Deliberately not a :class:`BandView` and deliberately not a field on
    :class:`~references.reference_display.SourceDisplay`. A pair of numbers
    shaped like a range, sitting on the projection every surface reads, is an
    invitation for a later widget to draw it as one — and the min/max of the
    points a row happens to store is not a range its author published.

    It exists for exactly one job: :func:`build_domains` has to know how wide a
    raw-data candidate really is before it freezes the axes, or a Library row
    holding forty measured spores of 40–50 µm contributes nothing to the
    domain and is then drawn entirely clipped against an axis derived from
    everything else.

    Metrics with no measurement are absent from the result rather than
    present as a degenerate pair.
    """
    decoded = normalize_points(points)
    extents: dict[str, tuple[float, float]] = {}
    for metric in METRICS:
        values = [
            value for value in (point.value(metric) for point in decoded) if value is not None
        ]
        if values:
            extents[metric] = (min(values), max(values))
    return extents


def _percentile(sorted_values: Sequence[float], percentile: float) -> float:
    """Linear-interpolation percentile, matching ``numpy.percentile``'s default.

    Written out rather than imported so this module stays dependency-free, and
    deliberately identical to what ``MainWindow._reference_stats_from_points``
    computes with numpy: the preview's 5–95 band and the plot's must be the
    same two numbers, or the user is shown two different observations.
    """
    if not sorted_values:
        raise ValueError("no values")
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    position = (len(sorted_values) - 1) * (percentile / 100.0)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return float(sorted_values[int(position)])
    weight = position - lower
    return float(sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight)


# --- What one party contributes to one metric ---------------------------------


@dataclass(frozen=True)
class BandView:
    """One drawable interval, with the claim it represents attached.

    ``meaning`` is the stored descriptor kind (``typical_range``,
    ``reported_extremes``, ``percentile_interval``, …) or one of this module's
    measured kinds, never a translated word — the widget owns the wording.
    ``None`` is the honest legacy answer: numbers were stored without anybody
    saying what they mean.
    """

    low: float
    high: float
    role: BandRole
    meaning: str | None = None
    percentile_bounds: tuple[float, float] | None = None
    origin: StatisticsOrigin = "unknown"
    clipped_low: bool = False
    clipped_high: bool = False

    @property
    def clipped(self) -> bool:
        return self.clipped_low or self.clipped_high


@dataclass(frozen=True)
class CentreView:
    """A mean or median the source actually stated.

    Exactly one of ``value`` and ``interval`` carries the number. A statistic
    reported as an interval — a mean given as ``9.1–9.4`` — keeps its two
    bounds instead of collapsing to a midpoint, because that midpoint is not a
    number anybody reported.
    """

    statistic: CentreStatistic
    value: float | None = None
    interval: tuple[float, float] | None = None
    origin: StatisticsOrigin = "unknown"
    #: Tracked per side, exactly like :class:`BandView`, because a centre
    #: outside the frozen axis is drawn clamped to its edge and the renderer
    #: has to know *which* edge to mark. A single boolean was enough to know
    #: something was wrong and not enough to say so on screen.
    clipped_low: bool = False
    clipped_high: bool = False

    @property
    def clipped(self) -> bool:
        return self.clipped_low or self.clipped_high


@dataclass(frozen=True)
class MetricSeries:
    """Everything one party (a source, or the user) has to draw for one metric."""

    metric: str
    outer: BandView | None = None
    core: BandView | None = None
    centres: tuple[CentreView, ...] = ()
    #: Number of individual measurements behind this series, when it was
    #: derived from points. ``None`` for a series read out of stored summary
    #: columns, whose ``n`` (if any) belongs to the source as a whole.
    point_count: int | None = None

    @property
    def is_empty(self) -> bool:
        return self.outer is None and self.core is None and not self.centres

    def centre(self, statistic: CentreStatistic) -> CentreView | None:
        return next((c for c in self.centres if c.statistic == statistic), None)


@dataclass(frozen=True)
class MetricDelta:
    """How far the user's value sits from the source's, for *one* statistic.

    Produced only for two scalar values of the same statistic (contract N16).
    ``difference`` is observation minus source, so a positive number means the
    user measured larger spores than the source reported.
    """

    statistic: CentreStatistic
    source_value: float
    observation_value: float

    @property
    def difference(self) -> float:
        return self.observation_value - self.source_value


# --- The axis -----------------------------------------------------------------


@dataclass(frozen=True)
class MetricDomain:
    """A frozen axis for one metric.

    Created once per dialog session by :func:`build_domains` and then never
    replaced. :meth:`position` clamps, so a value outside the domain still
    produces a drawable coordinate; :meth:`clips_below` / :meth:`clips_above`
    are how the caller learns that it did, and must mark it.
    """

    metric: str
    low: float
    high: float

    def __post_init__(self) -> None:
        if not (math.isfinite(self.low) and math.isfinite(self.high)):
            raise ValueError(f"non-finite domain for {self.metric}")
        if self.high <= self.low:
            raise ValueError(f"empty domain for {self.metric}")

    @property
    def span(self) -> float:
        return self.high - self.low

    def position(self, value: float) -> float:
        """``value`` as a 0–1 fraction across the axis, clamped to the axis."""
        return min(1.0, max(0.0, (float(value) - self.low) / self.span))

    def clips_below(self, value: float) -> bool:
        return float(value) < self.low

    def clips_above(self, value: float) -> bool:
        return float(value) > self.high


def _display_values(display: SourceDisplay, metric: str) -> list[float]:
    """Every number a stored source contributes to one metric's extent."""
    body = display.metric(metric)
    values: list[float] = []
    for pair in (body.outer_range, body.core_range):
        if pair is not None:
            values.extend(float(bound) for bound in pair)
    if body.scalar_mean is not None:
        values.append(float(body.scalar_mean))
    if body.mean_interval is not None:
        values.extend((float(body.mean_interval.lower), float(body.mean_interval.upper)))
    median = body.median
    if median is not None:
        value = getattr(median, "value", None)
        if value is not None:
            values.append(float(value))
        else:
            values.extend((float(median.lower), float(median.upper)))
    return [value for value in values if math.isfinite(value)]


def _series_values(series: MetricSeries | None) -> list[float]:
    if series is None:
        return []
    values: list[float] = []
    for band in (series.outer, series.core):
        if band is not None:
            values.extend((band.low, band.high))
    for centre in series.centres:
        if centre.value is not None:
            values.append(centre.value)
        if centre.interval is not None:
            values.extend(centre.interval)
    return values


def build_domains(
    *,
    baseline: "ObservationBaseline | None" = None,
    displays: Iterable[SourceDisplay] = (),
    point_sets: Iterable[Iterable[Any]] = (),
    extents: Iterable[Mapping[str, tuple[float, float]]] = (),
) -> dict[str, MetricDomain]:
    """Derive the session's three axes, once, and hand them back frozen.

    The inputs are deliberately "everything already known when the dialog
    opens": the user's own observation, the projections of the library rows
    loaded into the list, any point sets (personal observations) the host
    injected, and the pre-computed :func:`point_extents` of candidates that
    hold individual measurements rather than stated ranges. Content fetched
    later — a Community search result — is not here and must not cause a
    rebuild; it is clipped and marked instead.

    ``extents`` is separate from ``point_sets`` because a Library row's points
    are not in hand at this moment: the chooser query projects their span once
    while it loads the list, and re-reading every row's ``raw_points_json``
    here would be the duplicate database work the baseline seam exists to
    avoid. Without it a raw-only candidate contributes nothing to the axes and
    is then drawn fully clipped the moment it is selected.

    Every metric always gets a domain, so no caller has to handle a missing
    axis. With nothing known, that is :data:`DEFAULT_DOMAIN`.
    """
    displays = list(displays)
    extents = [span for span in extents if span]
    collected: dict[str, list[float]] = {metric: [] for metric in METRICS}
    for metric in METRICS:
        if baseline is not None:
            collected[metric].extend(_series_values(baseline.series(metric)))
        for display in displays:
            collected[metric].extend(_display_values(display, metric))
        for points in point_sets:
            for point in normalize_points(points):
                value = point.value(metric)
                if value is not None:
                    collected[metric].append(value)
        for span in extents:
            pair = span.get(metric)
            if pair is None:
                continue
            collected[metric].extend(
                value for value in (_finite(pair[0]), _finite(pair[1])) if value is not None
            )
    return {metric: _domain_for(metric, collected[metric]) for metric in METRICS}


def _domain_for(metric: str, values: Sequence[float]) -> MetricDomain:
    default_low, default_high = DEFAULT_DOMAIN.get(metric, (0.0, 1.0))
    if not values:
        return MetricDomain(metric=metric, low=default_low, high=default_high)
    low = min(values)
    high = max(values)
    padding = max((high - low) * DOMAIN_PADDING, 0.0)
    low -= padding
    high += padding
    minimum = MINIMUM_SPAN.get(metric, 1.0)
    if high - low < minimum:
        centre = (high + low) / 2.0
        low = centre - minimum / 2.0
        high = centre + minimum / 2.0
    # No metric here can be negative, and an axis that starts below zero wastes
    # a third of the track on impossible values.
    low = max(low, 0.0)
    if high - low < minimum:
        high = low + minimum
    return MetricDomain(metric=metric, low=low, high=high)


def _clip_centre(
    statistic: CentreStatistic,
    *,
    domain: MetricDomain | None,
    value: float | None = None,
    interval: tuple[float, float] | None = None,
    origin: StatisticsOrigin = "unknown",
) -> CentreView:
    """One centre mark with its clipping decided in a single place.

    Every centre in this module goes through here, so no construction site can
    forget which edge a value fell off — the omission that let a mean of 40 on
    a 5–15 axis render as an ordinary tick sitting on the axis end.
    """
    values = [number for number in (value, *(interval or ())) if number is not None]
    return CentreView(
        statistic=statistic,
        value=value,
        interval=interval,
        origin=origin,
        clipped_low=bool(domain is not None and any(domain.clips_below(v) for v in values)),
        clipped_high=bool(domain is not None and any(domain.clips_above(v) for v in values)),
    )


def _clip_band(
    domain: MetricDomain,
    low: float,
    high: float,
    *,
    role: BandRole,
    meaning: str | None,
    percentile_bounds: tuple[float, float] | None = None,
    origin: StatisticsOrigin = "unknown",
) -> BandView:
    if low > high:
        low, high = high, low
    return BandView(
        low=low,
        high=high,
        role=role,
        meaning=meaning,
        percentile_bounds=percentile_bounds,
        origin=origin,
        clipped_low=domain.clips_below(low),
        clipped_high=domain.clips_above(high),
    )


# --- The user's own observation -----------------------------------------------


@dataclass(frozen=True)
class ObservationBaseline:
    """The current observation, summarized from its actual measured spores.

    This is the one baseline model the stage calls for: built once from points
    the host hands in, never by querying the database a second time from inside
    a widget. Its statistics are all ``computed`` — they are calculations over
    the user's own measurements, which is exactly why they may be stated
    plainly, and exactly why a caption must not print them as though somebody
    published them.

    An observation with no usable measurements is a real state, not an error:
    :attr:`has_measurements` is ``False`` and every series is empty, so the
    no-selection pane says so instead of drawing an empty outline.
    """

    point_count: int = 0
    metrics: Mapping[str, MetricSeries] = field(default_factory=dict)

    @property
    def has_measurements(self) -> bool:
        return self.point_count > 0 and any(
            not series.is_empty for series in self.metrics.values()
        )

    def series(self, metric: str) -> MetricSeries | None:
        series = self.metrics.get(metric)
        return None if series is None or series.is_empty else series


def _series_from_values(
    metric: str, values: Sequence[float], *, domain: MetricDomain | None = None
) -> MetricSeries:
    """Summarize measured values, labelling every statistic as calculated.

    The band is min–max of the measurements and the inner band is their 5–95
    percentile interval; the centre is their median. All three are stated
    explicitly through :attr:`BandView.meaning` and
    :attr:`CentreView.statistic` rather than left for a reader to guess, which
    is the stage's "document exactly which statistic the band and centre
    represent".
    """
    ordered = sorted(float(v) for v in values if math.isfinite(v))
    if not ordered:
        return MetricSeries(metric=metric, point_count=0)
    low, high = ordered[0], ordered[-1]
    p_low = _percentile(ordered, MEASURED_PERCENTILE_BOUNDS[0])
    p_high = _percentile(ordered, MEASURED_PERCENTILE_BOUNDS[1])
    median = _percentile(ordered, 50.0)

    def band(bl: float, bh: float, role: BandRole, meaning: str, bounds=None) -> BandView:
        if domain is None:
            return BandView(
                low=bl,
                high=bh,
                role=role,
                meaning=meaning,
                percentile_bounds=bounds,
                origin="computed",
            )
        return _clip_band(
            domain, bl, bh, role=role, meaning=meaning, percentile_bounds=bounds, origin="computed"
        )

    core = None
    # Two identical bands stacked on one another say nothing and read as a
    # rendering bug; with too few points for the percentiles to differ from
    # the extremes, the extremes are the honest single band.
    if (p_low, p_high) != (low, high):
        core = band(p_low, p_high, "core", MEASURED_PERCENTILE, MEASURED_PERCENTILE_BOUNDS)
    return MetricSeries(
        metric=metric,
        outer=band(low, high, "outer", MEASURED_EXTREMES),
        core=core,
        centres=(
            _clip_centre("median", value=median, origin="computed", domain=domain),
        ),
        point_count=len(ordered),
    )


def observation_baseline_from_points(points: Iterable[Any] | None) -> ObservationBaseline:
    """Build the baseline for the observation the picker was opened on."""
    decoded = normalize_points(points)
    metrics: dict[str, MetricSeries] = {}
    for metric in METRICS:
        values = [
            value for value in (point.value(metric) for point in decoded) if value is not None
        ]
        metrics[metric] = _series_from_values(metric, values)
    return ObservationBaseline(point_count=len(decoded), metrics=metrics)


# --- A candidate source -------------------------------------------------------


def source_series(
    display: SourceDisplay,
    metric: str,
    *,
    domain: MetricDomain,
    points: Iterable[Any] | None = None,
) -> MetricSeries | None:
    """What one source draws for one metric, or ``None`` when it states nothing.

    Stored, explicitly-stated statistics always win. A source's individual
    measurements are summarized **only** for a metric whose ranges it never
    stated — the case where deriving from the points adds information instead
    of contradicting a number the source printed. That derived series is
    labelled ``computed`` and carries the measured meanings, so it can never be
    read as a published range.
    """
    body = display.metric(metric)
    origin = display.statistics_origin
    outer = None
    if body.outer_range is not None:
        outer = _clip_band(
            domain,
            *body.outer_range,
            role="outer",
            meaning=body.outer_kind,
            origin=origin,
        )
    core = None
    if body.core_range is not None:
        core = _clip_band(
            domain,
            *body.core_range,
            role="core",
            meaning=body.core_kind,
            percentile_bounds=body.percentile_bounds,
            origin=origin,
        )
    centres: list[CentreView] = []
    if body.scalar_mean is not None:
        centres.append(
            _clip_centre(
                "mean", value=float(body.scalar_mean), origin=origin, domain=domain
            )
        )
    elif body.mean_interval is not None:
        centres.append(
            _clip_centre(
                "mean",
                interval=(float(body.mean_interval.lower), float(body.mean_interval.upper)),
                origin=origin,
                domain=domain,
            )
        )
    median = body.median
    if median is not None:
        value = getattr(median, "value", None)
        if value is not None:
            centres.append(
                _clip_centre("median", value=float(value), origin=origin, domain=domain)
            )
        else:
            centres.append(
                _clip_centre(
                    "median",
                    interval=(float(median.lower), float(median.upper)),
                    origin=origin,
                    domain=domain,
                )
            )

    if outer is None and core is None and not centres:
        decoded = normalize_points(points)
        values = [
            value for value in (point.value(metric) for point in decoded) if value is not None
        ]
        if values:
            return _series_from_values(metric, values, domain=domain)
        return None
    return MetricSeries(metric=metric, outer=outer, core=core, centres=tuple(centres))


def _delta_between(
    source: MetricSeries | None, observation: MetricSeries | None
) -> MetricDelta | None:
    """The one comparison that is genuinely like-for-like, or nothing.

    Both sides must hold a scalar value of the same statistic. A mean against a
    median, a scalar against an interval, or a band against a centre all return
    ``None``: contract N16 allows a delta only between equivalent statistics,
    and "close enough" is how a comparison becomes a fabrication.
    """
    if source is None or observation is None:
        return None
    for statistic in ("median", "mean"):
        theirs = source.centre(statistic)  # type: ignore[arg-type]
        ours = observation.centre(statistic)  # type: ignore[arg-type]
        if theirs is None or ours is None:
            continue
        if theirs.value is None or ours.value is None:
            continue
        return MetricDelta(
            statistic=statistic,  # type: ignore[arg-type]
            source_value=theirs.value,
            observation_value=ours.value,
        )
    return None


# --- The whole pane's model ---------------------------------------------------


@dataclass(frozen=True)
class MetricComparison:
    """One stacked Length / Width / Q comparison (contract N17)."""

    metric: str
    domain: MetricDomain
    source: MetricSeries | None = None
    observation: MetricSeries | None = None
    delta: MetricDelta | None = None

    @property
    def is_empty(self) -> bool:
        return self.source is None and self.observation is None

    @property
    def clipped(self) -> bool:
        """Whether anything here had to be drawn outside the frozen axis."""
        for series in (self.source, self.observation):
            if series is None:
                continue
            if any(band is not None and band.clipped for band in (series.outer, series.core)):
                return True
            if any(centre.clipped for centre in series.centres):
                return True
        return False


@dataclass(frozen=True)
class ComparisonView:
    """Everything the Summary surface renders for one selection.

    ``has_source`` is the state switch the widget reads: ``False`` is the
    no-selection pane, which shows the observation as chips plus explanatory
    copy and specifically not a table of dashes (contract N21).
    """

    metrics: tuple[MetricComparison, ...]
    has_source: bool = False
    observation_point_count: int = 0
    #: The selected source's projection, so the caption can name its sample
    #: size and say whether its statistics were published or calculated.
    source_display: SourceDisplay | None = None

    def metric(self, name: str) -> MetricComparison | None:
        return next((m for m in self.metrics if m.metric == name), None)


def comparison_view(
    *,
    domains: Mapping[str, MetricDomain],
    baseline: ObservationBaseline | None = None,
    display: SourceDisplay | None = None,
    source_points: Iterable[Any] | None = None,
) -> ComparisonView:
    """Assemble the pane's model against the session's frozen domains.

    ``domains`` is an input, never derived here: the whole point of freezing is
    that this function cannot move the axis no matter what it is handed.
    """
    comparisons: list[MetricComparison] = []
    for metric in METRICS:
        domain = domains.get(metric) or _domain_for(metric, ())
        observation = _rebind(baseline.series(metric), domain) if baseline is not None else None
        source = (
            source_series(display, metric, domain=domain, points=source_points)
            if display is not None
            else None
        )
        comparisons.append(
            MetricComparison(
                metric=metric,
                domain=domain,
                source=source,
                observation=observation,
                delta=_delta_between(source, observation),
            )
        )
    return ComparisonView(
        metrics=tuple(comparisons),
        has_source=display is not None,
        observation_point_count=baseline.point_count if baseline is not None else 0,
        source_display=display,
    )


def _rebind(series: MetricSeries | None, domain: MetricDomain) -> MetricSeries | None:
    """Re-evaluate a series' clipping flags against the session's domain.

    The baseline is built before the domains exist (it is one of their inputs),
    so its bands carry no clipping flags. They cannot actually fall outside a
    domain derived from them — but a caller may supply domains from elsewhere,
    and a flag that is merely assumed correct is not evidence.
    """
    if series is None:
        return None
    return MetricSeries(
        metric=series.metric,
        outer=_reclip(series.outer, domain),
        core=_reclip(series.core, domain),
        centres=tuple(
            _clip_centre(
                centre.statistic,
                value=centre.value,
                interval=centre.interval,
                origin=centre.origin,
                domain=domain,
            )
            for centre in series.centres
        ),
        point_count=series.point_count,
    )


def _reclip(band: BandView | None, domain: MetricDomain) -> BandView | None:
    if band is None:
        return None
    return BandView(
        low=band.low,
        high=band.high,
        role=band.role,
        meaning=band.meaning,
        percentile_bounds=band.percentile_bounds,
        origin=band.origin,
        clipped_low=domain.clips_below(band.low),
        clipped_high=domain.clips_above(band.high),
    )


# --- Raw spores ---------------------------------------------------------------


def raw_point_rows(points: Iterable[Any] | None) -> tuple[tuple[str, str, str, str], ...]:
    """The Raw spores tab's actual rows: ``(#, length, width, Q)``.

    Only measurements that exist are formatted; a point measured in one
    dimension prints ``—`` for the other rather than a number nobody measured.
    An empty result means there is nothing to show, and the caller must say so
    honestly instead of falling back to a range (contract N22).
    """
    rows: list[tuple[str, str, str, str]] = []
    for index, point in enumerate(normalize_points(points), start=1):
        rows.append(
            (
                str(index),
                _format_point(point.length),
                _format_point(point.width),
                _format_point(point.q),
            )
        )
    return tuple(rows)


def _format_point(value: float | None) -> str:
    return "—" if value is None else f"{value:.1f}"


__all__ = [
    "BandRole",
    "BandView",
    "CentreStatistic",
    "CentreView",
    "ComparisonView",
    "DEFAULT_DOMAIN",
    "DOMAIN_PADDING",
    "MEASURED_EXTREMES",
    "MEASURED_PERCENTILE",
    "MEASURED_PERCENTILE_BOUNDS",
    "MINIMUM_SPAN",
    "MetricComparison",
    "MetricDelta",
    "MetricDomain",
    "MetricSeries",
    "ObservationBaseline",
    "SporePoint",
    "build_domains",
    "comparison_view",
    "normalize_points",
    "observation_baseline_from_points",
    "point_extents",
    "raw_point_rows",
    "source_series",
]
