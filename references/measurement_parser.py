"""Parser for fungal spore reference measurement strings and tables.

Mycology literature commonly reports spore dimensions like

    10.2-12.5 x 5.5-6.8 µm

or, more fully,

    (9.5-)9.8-11.3(-11.7) x (7.3-)8.0-9.4(-9.4) µm,
    Q = (1.1-)1.1-1.3(-1.3), Qm = 1.2, n = 36

The parenthesised outer values are extreme observations; the unparenthesised
inner range is the "typical" bulk of measurements (not a strict statistical
percentile — most sources do not publish their cut-off rule).

Some sources publish a small table instead, for example the Hebeloma layout

    Spore   (min) 5%-95% (max)     mean       median     S.D.
    Length  (7.5) 8.4–13.0 (13.2)  9.2–11.7   9.2–11.7   0.600
    Width   (5.0) 5.1–7.2 (7.6)    5.6–6.7    5.6–6.7    0.280
    Q       (1.30) 1.42–1.96 (2.07) 1.55–1.78 1.54–1.79  0.095

Tables are recognised before whitespace folding so that column boundaries
(tabs, Markdown pipes, or value tokens) survive. Columns are mapped by their
headings; there is no positional guessing except the documented headerless
layout (range, mean, median, S.D.), whose range meaning stays unspecified.

The Sporely legacy reference data model uses these field names:

    min   = extreme min          (existing DB column ``*_min``)
    p05   = typical min          (existing DB column ``*_p05`` / ``*_core_min``)
    p50   = centre / median      (existing DB column ``*_p50``)
    p95   = typical max          (existing DB column ``*_p95`` / ``*_core_max``)
    max   = extreme max          (existing DB column ``*_max``)

The legacy ``p05`` / ``p95`` labels are kept for compatibility with existing
data, sync code, and the landing page; this parser populates those fields but
never claims they are statistical percentiles. The meaning of a range is
carried separately as a :class:`~references.measurement_content.RangeDescriptor`
in :attr:`MeasurementParseResult.metric_details`: an explicit ``5%-95%``
heading yields ``percentile_interval``; every other inner range is tagged
``unspecified``; parenthesised extremes on both sides are tagged
``reported_extremes``.

Qm / Qav (reported Q mean) is stored separately from the Q range. A scalar goes
to ``q_mean``; an interval becomes the Q ``mean_interval``. If the source
carries both a Q range and a reported Q mean, both are preserved. Repeated
named values with different numbers warn and keep the first.

Reported means, medians and standard deviations from tables are typed
statistics in ``metric_details`` (scalar versus interval shape preserved,
equal endpoints included). They never populate the legacy ``p50`` centre nor
``q_mean``, except a scalar *mean* cell, which is exactly a scalar mean.

No Q value is ever derived from length / width — the parser only reports what
the source string actually contains. :meth:`MeasurementParseResult.to_content`
emits the typed :class:`~references.measurement_content.MeasurementContent`
for later persistence; nothing in this module persists anything.
"""

from __future__ import annotations

import html
import re
from dataclasses import dataclass, field, asdict
from typing import Iterable

from references.measurement_content import (
    IntervalStatistic,
    MeasurementContent,
    MeasurementDetails,
    MetricDetails,
    RangeDescriptor,
    ScalarStatistic,
)


# --- Normalisation -----------------------------------------------------------

_DASH_RE = re.compile(r"[‐‑‒–—―−]")
_COMMA_DECIMAL_RE = re.compile(r"(?<=\d),(?=\d)")
_MUL_RE = re.compile(r"[×✕✖∗*]")
_UNIT_RE = re.compile(r"[µμ]m|\bum\b", re.IGNORECASE)
_WHITESPACE_RE = re.compile(r"\s+")
_HTML_BREAK_RE = re.compile(r"<br\s*/?\s*>", re.IGNORECASE)


def _normalise_structure(text: str) -> str:
    """Decode HTML and fold glyph variants while keeping table structure.

    Entities are decoded first so that e.g. ``&#x20;`` never contributes a
    literal ``x`` to dimension splitting. HTML line breaks become newlines,
    except inside a Markdown table row (a line containing ``|``) where they
    are soft breaks within a cell. Newlines, tabs and pipes are preserved;
    :func:`_normalise` folds them afterwards for the single-string path.
    """
    s = html.unescape(text or "")
    lines = []
    for line in s.split("\n"):
        if "|" in line:
            lines.append(_HTML_BREAK_RE.sub(" ", line))
        else:
            lines.append(_HTML_BREAK_RE.sub("\n", line))
    s = "\n".join(lines)
    s = _DASH_RE.sub("-", s)
    s = _COMMA_DECIMAL_RE.sub(".", s)
    s = _MUL_RE.sub("x", s)
    s = _UNIT_RE.sub(" ", s)
    s = s.replace(" ", " ").replace(" ", " ")
    return s


def _normalise(text: str) -> str:
    """Fold the input into a single canonical line for downstream parsing."""
    s = _normalise_structure(text)
    s = _WHITESPACE_RE.sub(" ", s)
    return s.strip()


# --- Result types ------------------------------------------------------------


@dataclass
class DimensionRange:
    """One length / width / Q range.

    The field names use the existing DB nomenclature but mean what the
    relabelled UI shows: extreme / typical / centre / typical / extreme.
    """

    min: float | None = None     # extreme min
    p05: float | None = None     # typical min
    p50: float | None = None     # centre / median (only if explicitly given)
    p95: float | None = None     # typical max
    max: float | None = None     # extreme max

    def is_empty(self) -> bool:
        return all(
            getattr(self, name) is None for name in ("min", "p05", "p50", "p95", "max")
        )

    def to_dict(self, prefix: str) -> dict[str, float | None]:
        """Return a flat dict using the legacy column-name prefix.

        For ``prefix='length'`` the keys are ``length_min``, ``length_p05``,
        ``length_p50``, ``length_p95``, ``length_max``.
        """
        return {
            f"{prefix}_min": self.min,
            f"{prefix}_p05": self.p05,
            f"{prefix}_p50": self.p50,
            f"{prefix}_p95": self.p95,
            f"{prefix}_max": self.max,
        }


_METRICS = ("length", "width", "q")


@dataclass
class MeasurementParseResult:
    """Structured result of parsing one literature measurement string.

    ``length`` / ``width`` / ``q`` and ``q_mean`` / ``n`` are the legacy
    fields read by both editors today. ``length_mean`` / ``width_mean`` hold
    scalar means from headed tables. ``metric_details`` holds the typed
    descriptors and reported statistics per metric; it is not read by any
    legacy consumer and reaches persistence only through :meth:`to_content`.
    """

    length: DimensionRange = field(default_factory=DimensionRange)
    width: DimensionRange = field(default_factory=DimensionRange)
    q: DimensionRange = field(default_factory=DimensionRange)
    q_mean: float | None = None
    n: int | None = None
    warnings: list[str] = field(default_factory=list)
    raw: str = ""
    normalised: str = ""
    length_mean: float | None = None
    width_mean: float | None = None
    metric_details: dict[str, MetricDetails] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        """True when at least one numeric value was successfully parsed."""
        return (
            not self.length.is_empty()
            or not self.width.is_empty()
            or not self.q.is_empty()
            or self.q_mean is not None
            or self.length_mean is not None
            or self.width_mean is not None
            or self.n is not None
            or any(not d.is_empty() for d in self.metric_details.values())
        )

    def to_record_dict(self) -> dict[str, float | int | None]:
        """Flat dict using the legacy DB column names.

        Includes ``q_p05`` / ``q_p95`` even though they are added by a small
        schema migration; older code paths that do not know these keys will
        ignore them. Typed statistics are deliberately absent here.
        """
        record: dict[str, float | int | None] = {}
        record.update(self.length.to_dict("length"))
        record.update(self.width.to_dict("width"))
        record.update(self.q.to_dict("q"))
        record["q_avg"] = self.q_mean
        record["n"] = self.n
        return record

    def asdict(self) -> dict:
        return asdict(self)

    def to_content(self) -> MeasurementContent:
        """Typed content for the measurement content contract.

        Outer pairs come from the extremes, core pairs from the inner range,
        scalar means from ``*_mean`` / ``q_mean`` and ``n`` from the sample
        size. The legacy ``p50`` centre has no typed home: its meaning is
        unspecified and the contract carries no untyped scalar, so it stays a
        legacy-editor value only. Nothing is validated here; callers run
        ``validate_measurement_content(..., mode="edit")``.
        """
        metrics = {
            metric: details
            for metric, details in self.metric_details.items()
            if metric in _METRICS and not details.is_empty()
        }
        return MeasurementContent(
            raw_text=self.raw or None,
            length_min=self.length.min,
            length_core_min=self.length.p05,
            length_core_max=self.length.p95,
            length_max=self.length.max,
            width_min=self.width.min,
            width_core_min=self.width.p05,
            width_core_max=self.width.p95,
            width_max=self.width.max,
            q_min=self.q.min,
            q_core_min=self.q.p05,
            q_core_max=self.q.p95,
            q_max=self.q.max,
            q_mean=self.q_mean,
            length_mean=self.length_mean,
            width_mean=self.width_mean,
            sample_size=self.n,
            details=MeasurementDetails(metrics=metrics) if metrics else None,
        )


# --- Range parser ------------------------------------------------------------

_NUM = r"\d+(?:\.\d+)?"

# Full ``(A-)B-C(-D)`` or ``(A) B-C (D)`` shape. The two parenthesised groups
# are optional, the inner ``B-C`` range is required.
_PAREN_RANGE_RE = re.compile(
    rf"""
    ^\s*
    (?:\(\s*(?P<emin>{_NUM})\s*-?\s*\)\s*)?
    (?P<tmin>{_NUM})\s*-\s*(?P<tmax>{_NUM})
    (?:\s*\(\s*-?\s*(?P<emax>{_NUM})\s*\))?
    \s*$
    """,
    re.VERBOSE,
)

# Three-value explicit range ``B-C-D`` where the middle value is the centre.
# Rare in mycology literature but cheap to support.
_TRIPLE_RANGE_RE = re.compile(
    rf"^\s*(?P<a>{_NUM})\s*-\s*(?P<b>{_NUM})\s*-\s*(?P<c>{_NUM})\s*$"
)

_SINGLE_NUM_RE = re.compile(rf"^\s*(?P<v>{_NUM})\s*$")
_INTERVAL_RE = re.compile(rf"^\s*(?P<lo>{_NUM})\s*-\s*(?P<hi>{_NUM})\s*$")


def _parse_range(text: str, *, label: str, warnings: list[str]) -> DimensionRange:
    """Parse one normalised numeric range into a :class:`DimensionRange`."""
    chunk = text.strip().rstrip(",;").strip()
    if not chunk:
        return DimensionRange()

    m = _PAREN_RANGE_RE.match(chunk)
    if m:
        rng = DimensionRange(
            min=float(m.group("emin")) if m.group("emin") else None,
            p05=float(m.group("tmin")),
            p50=None,
            p95=float(m.group("tmax")),
            max=float(m.group("emax")) if m.group("emax") else None,
        )
        if rng.min is None and rng.max is None:
            warnings.append(f"{label}: no extreme values found.")
        return rng

    m = _TRIPLE_RANGE_RE.match(chunk)
    if m:
        return DimensionRange(
            min=None,
            p05=float(m.group("a")),
            p50=float(m.group("b")),
            p95=float(m.group("c")),
            max=None,
        )

    m = _SINGLE_NUM_RE.match(chunk)
    if m:
        return DimensionRange(p50=float(m.group("v")))

    warnings.append(f"{label}: could not parse '{chunk}'.")
    return DimensionRange()


def _parse_statistic(text: str) -> ScalarStatistic | IntervalStatistic | None:
    """Parse a reported statistic, preserving scalar versus interval shape.

    ``9.2`` is a scalar; ``9.2-11.7`` and ``9.2-9.2`` are intervals (equal
    endpoints are never collapsed). Inverted intervals are not recognised.
    """
    chunk = (text or "").strip().rstrip(",;").strip()
    m = _SINGLE_NUM_RE.match(chunk)
    if m:
        return ScalarStatistic(value=float(m.group("v")))
    m = _INTERVAL_RE.match(chunk)
    if m:
        lower, upper = float(m.group("lo")), float(m.group("hi"))
        if lower > upper:
            return None
        return IntervalStatistic(lower=lower, upper=upper, kind="reported_range")
    return None


def _range_descriptors(
    rng: DimensionRange, core: RangeDescriptor | None
) -> tuple[RangeDescriptor | None, RangeDescriptor | None]:
    """Descriptors for a parsed range: the outer tag needs both extremes, the
    core tag needs both inner endpoints. ``core`` is the heading-derived
    descriptor or ``None`` for the default ``unspecified`` tag."""
    outer = (
        RangeDescriptor(kind="reported_extremes")
        if rng.min is not None and rng.max is not None
        else None
    )
    core_descriptor = None
    if rng.p05 is not None and rng.p95 is not None:
        core_descriptor = core or RangeDescriptor(kind="unspecified")
    return outer, core_descriptor


# --- Tagged-value extractor --------------------------------------------------

# One value token: ``(A-)B-C(-D)``, ``(A) B-C (D)``, ``B-C``, ``B-C-D`` or ``B``.
# Named values and table cells are matched against this token so that prose
# and later dimensions are never swallowed into a statistic.
_VALUE_TOKEN = (
    rf"(?:\(\s*{_NUM}\s*-?\s*\)\s*)?"
    rf"{_NUM}(?:\s*-\s*{_NUM}){{0,2}}"
    rf"(?:\s*\(\s*-?\s*{_NUM}\s*\))?"
)
_VALUE_TOKEN_RE = re.compile(_VALUE_TOKEN)

# ``Qav`` and ``Qm`` are aliases for the reported Q mean. Alternation order
# matters: ``Qav`` and ``Qm`` before ``Q``; ``n`` only as a standalone token.
_NAMED_VALUE_RE = re.compile(
    rf"(?<![A-Za-z])(?P<label>Qav|Qm|Q|n)(?![A-Za-z])\s*=\s*(?P<value>{_VALUE_TOKEN})?",
    re.IGNORECASE,
)
_NAMED_VALUE_KEYS = {"qav": "qm", "qm": "qm", "q": "q", "n": "n"}


def _strip_named_values(
    text: str, warnings: list[str]
) -> tuple[str, dict[str, str]]:
    """Pull ``Qm = ...`` / ``Qav = ...``, ``Q = ...`` and ``n = ...`` out.

    Returns the remainder (with those tokens removed) and the first raw value
    per key (``'qm'`` for either alias, ``'q'``, ``'n'``). A repeated key with
    a different value warns and keeps the first; a label without a numeric
    value warns and is removed from the remainder.
    """
    extracted: dict[str, str] = {}
    first_label: dict[str, str] = {}
    spans: list[tuple[int, int]] = []
    for m in _NAMED_VALUE_RE.finditer(text):
        label = m.group("label")
        key = _NAMED_VALUE_KEYS[label.lower()]
        value = (m.group("value") or "").strip()
        if not value:
            # Drop the unparseable value text with its label so it cannot
            # leak into the length/width remainder.
            trailing = re.split(r"[,;]", text[m.end():], maxsplit=1)[0]
            spans.append((m.start(), m.end() + len(trailing)))
            warnings.append(f"{label}: could not parse '{trailing.strip()}'.")
            continue
        spans.append((m.start(), m.end()))
        if key in extracted:
            if _WHITESPACE_RE.sub("", extracted[key]) != _WHITESPACE_RE.sub("", value):
                warnings.append(
                    f"{label}: repeated with a different value "
                    f"('{extracted[key]}' from {first_label[key]}, then '{value}'); "
                    "kept the first."
                )
            continue
        extracted[key] = value
        first_label[key] = label
    remainder = text
    for start, end in reversed(spans):
        remainder = remainder[:start] + " " + remainder[end:]
    remainder = _WHITESPACE_RE.sub(" ", remainder).strip(" ,;")
    return remainder, extracted


def _parse_scalar(text: str) -> float | None:
    """Parse a single numeric value (used for Qm, single Q, S.D.)."""
    m = _SINGLE_NUM_RE.match(text)
    if m:
        return float(m.group("v"))
    return None


def _parse_n(text: str) -> int | None:
    m = re.match(r"^\s*(\d+)\s*$", text or "")
    if m:
        return int(m.group(1))
    return None


# --- Table parser -------------------------------------------------------------

_LABEL_NAMES = {"length": "length", "width": "width", "q": "q"}
_LABEL_TITLES = {"length": "Length", "width": "Width", "q": "Q"}

# A row label at the start of a line or as a whole cell: ``Length``,
# ``Width (µm)`` (unit already folded to ``( )``), ``Q``. ``Q =`` is a named
# value, not a row label.
_LABEL_PREFIX_RE = re.compile(
    r"^\s*(?P<label>length|width|q)(?![A-Za-z])(?!\s*=)\s*(?:[(\[]\s*[)\]])?\s*[:.]?\s*",
    re.IGNORECASE,
)
_LABEL_CELL_RE = re.compile(
    r"^\s*(?P<label>length|width|q)(?![A-Za-z])\s*(?:[(\[]\s*[)\]])?\s*[:.]?\s*$",
    re.IGNORECASE,
)
_INLINE_LABEL_RE = re.compile(
    r"(?<![A-Za-z])(?:length|width|q)(?![A-Za-z])(?!\s*=)", re.IGNORECASE
)

_RANGE_HEADING = (
    rf"\(\s*min\.?\s*\)\s*(?:{_NUM}\s*%\s*-\s*{_NUM}\s*%\s*)?\(\s*max\.?\s*\)"
    rf"|{_NUM}\s*%\s*-\s*{_NUM}\s*%"
    r"|min\.?\s*-\s*max\.?"
    r"|range"
)
_HEADING_TOKEN_RE = re.compile(
    rf"(?P<range>{_RANGE_HEADING})"
    r"|(?P<label>spores?|character|dimension|statistic|parameter|measure(?:ment)?s?)"
    r"|(?P<mean>mean|average|avg\.?)"
    r"|(?P<median>median)"
    r"|(?P<sd>s\.\s*d\.?|(?<![A-Za-z])sd(?![A-Za-z])|std\.?\s*dev\.?|standard\s+deviation)",
    re.IGNORECASE,
)
_PERCENT_BOUNDS_RE = re.compile(rf"(?P<lo>{_NUM})\s*%\s*-\s*(?P<hi>{_NUM})\s*%")

# The pasted Hebeloma heading survives some clipboards glued together. It is
# an explicitly recognised form: label, range, mean, median, S.D.
_GLUED_HEBELOMA_HEADING_RE = re.compile(
    r"^\s*spores?\s*\(\s*min\s*\)\s*5\s*%\s*-\s*95\s*%\s*\(\s*max\s*\)"
    r"\s*mean\s*median\s*s\.?\s*d\.?\s*$",
    re.IGNORECASE,
)
_HEADING_NOISE_RE = re.compile(r"[\s()\[\].,:;|/%-]+")
_PLACEHOLDER_CELL_RE = re.compile(r"^\s*(?:-+|n\.?\s*[ad]\.?|n/a|nd|\.)?\s*$", re.IGNORECASE)
_HEADERLESS_ROLES = ("range", "mean", "median", "sd")


@dataclass
class _TableLine:
    kind: str                       # "header" | "data" | "malformed" | "prose"
    text: str
    label: str | None = None
    cells: list[str] = field(default_factory=list)   # value cells, label removed
    header_cells: list[str] | None = None            # delimited header cells


def _split_cells(line: str) -> list[str] | None:
    """Explicit cells for pipe or tab delimited rows; ``None`` for token mode."""
    if "|" in line:
        inner = line.strip()
        if inner.startswith("|"):
            inner = inner[1:]
        if inner.endswith("|"):
            inner = inner[:-1]
        return [c.strip() for c in inner.split("|")]
    if "\t" in line:
        return [c.strip() for c in line.split("\t")]
    return None


def _split_lines(text: str) -> list[str]:
    """Physical lines, additionally splitting a folded line that carries
    several row labels (``Length ... Width ... Q ...`` on one line)."""
    lines: list[str] = []
    for raw_line in text.split("\n"):
        line = raw_line.strip()
        if not line:
            continue
        if "|" not in line and "\t" not in line:
            starts = [m.start() for m in _INLINE_LABEL_RE.finditer(line)]
            if len(starts) >= 2:
                head = line[: starts[0]].strip()
                if head:
                    lines.append(head)
                for a, b in zip(starts, starts[1:] + [len(line)]):
                    piece = line[a:b].strip()
                    if piece:
                        lines.append(piece)
                continue
        lines.append(line)
    return lines


def _label_of_cell(cell: str) -> str | None:
    m = _LABEL_CELL_RE.match(cell)
    return _LABEL_NAMES[m.group("label").lower()] if m else None


def _is_header_text(text: str) -> bool:
    """A heading line names at least one column and has no digits left once
    the heading tokens (including ``5%-95%``) are removed."""
    roles = {m.lastgroup for m in _HEADING_TOKEN_RE.finditer(text)}
    if not roles & {"range", "mean", "median", "sd"}:
        return False
    return not re.search(r"\d", _HEADING_TOKEN_RE.sub("", text))


def _classify_line(line: str) -> _TableLine:
    cells = _split_cells(line)
    if cells is not None:
        label = _label_of_cell(cells[0]) if cells else None
        values = cells[1:] if label is not None else cells
        if any(_VALUE_TOKEN_RE.fullmatch(c) for c in values) or (
            label is not None and any(values)
        ):
            return _TableLine("data", line, label=label, cells=values)
        if any(_is_header_text(c) for c in cells if c):
            return _TableLine("header", line, header_cells=cells)
        return _TableLine("prose", line)

    m = _LABEL_PREFIX_RE.match(line)
    label = _LABEL_NAMES[m.group("label").lower()] if m else None
    rest = line[m.end():] if m else line
    if label is None and _is_header_text(line):
        return _TableLine("header", line)
    tokens = [t.group() for t in _VALUE_TOKEN_RE.finditer(rest)]
    if not tokens:
        return _TableLine("prose", line, label=label)
    residue = _VALUE_TOKEN_RE.sub("", rest)
    residue = re.sub(r"\(\s*\)", "", residue)
    residue = re.sub(r"[\s,;:|]+", "", residue)
    if residue:
        return _TableLine("malformed", line, label=label)
    return _TableLine("data", line, label=label, cells=tokens)


@dataclass
class _Header:
    roles: list[str | None]                 # per column position
    core: RangeDescriptor | None = None     # heading-derived core descriptor


def _percentile_descriptor(text: str, warnings: list[str]) -> RangeDescriptor | None:
    m = _PERCENT_BOUNDS_RE.search(text)
    if not m:
        return None

    def _num(s: str) -> float:
        return float(s) if "." in s else int(s)

    lo, hi = _num(m.group("lo")), _num(m.group("hi"))
    if not (0 <= lo < hi <= 100):
        warnings.append(
            f"Range heading '{m.group().strip()}' is not a valid percentile "
            "interval; inner ranges left unspecified."
        )
        return None
    warnings.append(
        f"Source heading identifies the inner ranges as the {m.group('lo')}%-"
        f"{m.group('hi')}% percentile interval."
    )
    return RangeDescriptor(kind="percentile_interval", percentile_bounds=(lo, hi))


def _tokenize_heading(text: str, warnings: list[str]) -> list[str | None]:
    """Roles of a glued or space-separated heading, in reading order."""
    if _GLUED_HEBELOMA_HEADING_RE.match(text):
        return ["label", "range", "mean", "median", "sd"]
    roles: list[str | None] = []
    for m in _HEADING_TOKEN_RE.finditer(text):
        roles.append(m.lastgroup)
    leftover = _HEADING_NOISE_RE.sub("", _HEADING_TOKEN_RE.sub(" ", text))
    if leftover:
        warnings.append(f"Unrecognized text in table heading: '{leftover}'.")
    return roles


def _parse_header(line: _TableLine, warnings: list[str]) -> _Header:
    cells = line.header_cells
    non_empty = [c for c in (cells or []) if c]
    if cells is None or len(non_empty) <= 1:
        text = non_empty[0] if non_empty else line.text
        roles = _tokenize_heading(text, warnings)
        range_text = text
    else:
        roles = []
        range_text = ""
        for index, cell in enumerate(cells):
            if not cell:
                roles.append("label" if index == 0 else None)
                if index > 0:
                    warnings.append(f"Table column {index + 1} has no heading; its cells are ignored.")
                continue
            found = {m.lastgroup for m in _HEADING_TOKEN_RE.finditer(cell)}
            if len(found) == 1:
                role = found.pop()
                roles.append(role)
                if role == "range":
                    range_text = cell
            elif not found:
                warnings.append(f"Unknown table column heading '{cell}'; its cells are ignored.")
                roles.append(None)
            else:
                warnings.append(
                    f"Ambiguous table column heading '{cell}'; its cells are ignored."
                )
                roles.append(None)
    # Duplicate headings: neither column can be trusted.
    for role in ("range", "mean", "median", "sd"):
        if roles.count(role) > 1:
            warnings.append(f"Duplicate '{role}' table columns; both are ignored.")
            roles = [None if r == role else r for r in roles]
    core = _percentile_descriptor(range_text, warnings) if "range" in roles else None
    return _Header(roles=roles, core=core)


def _apply_row(
    result: MeasurementParseResult,
    metric: str,
    pairs: list[tuple[str | None, str | None]],
    core: RangeDescriptor | None,
) -> None:
    """Fill ``metric`` from ``(role, cell)`` pairs. Cells are bound to their
    own column only; a missing or malformed cell never shifts a neighbour."""
    title = _LABEL_TITLES[metric]
    rng = DimensionRange()
    mean: ScalarStatistic | IntervalStatistic | None = None
    median: ScalarStatistic | IntervalStatistic | None = None
    sd: ScalarStatistic | None = None
    for role, cell in pairs:
        if role is None or cell is None or _PLACEHOLDER_CELL_RE.match(cell):
            continue
        if role == "range":
            rng = _parse_range(cell, label=title, warnings=result.warnings)
        elif role in ("mean", "median"):
            stat = _parse_statistic(cell)
            if stat is None:
                result.warnings.append(f"{title}: {role} cell '{cell}' not recognized.")
            elif role == "mean":
                mean = stat
            else:
                median = stat
        elif role == "sd":
            value = _parse_scalar(cell)
            if value is None:
                result.warnings.append(f"{title}: S.D. cell '{cell}' not recognized.")
            else:
                sd = ScalarStatistic(value=value)
    setattr(result, metric, rng)
    if isinstance(mean, ScalarStatistic):
        if metric == "q":
            result.q_mean = mean.value
        else:
            setattr(result, f"{metric}_mean", mean.value)
    outer, core_descriptor = _range_descriptors(rng, core)
    details = MetricDetails(
        outer_range=outer,
        core_range=core_descriptor,
        mean_interval=mean if isinstance(mean, IntervalStatistic) else None,
        median=median,
        sd=sd,
    )
    if not details.is_empty():
        result.metric_details[metric] = details


def _parse_table(text: str, result: MeasurementParseResult) -> bool:
    """Recognise a labelled or headed table. Returns ``False`` when the text is
    not a table so the single-string path runs instead."""
    lines = [_classify_line(line) for line in _split_lines(text)]
    first = next((i for i, l in enumerate(lines) if l.kind in ("data", "malformed")), None)
    if first is None:
        return False
    header_line = next((l for l in reversed(lines[:first]) if l.kind == "header"), None)
    rows = [l for l in lines[first:] if l.kind in ("data", "malformed")]
    labels = [row.label for row in rows if row.label is not None]
    labelled_table = "length" in labels and "width" in labels
    if not labelled_table and (
        header_line is None or not any(row.kind == "data" for row in rows)
    ):
        return False

    header = _parse_header(header_line, result.warnings) if header_line is not None else None

    if header is not None and not labels:
        if len(rows) in (2, 3):
            for row, metric in zip(rows, _METRICS):
                row.label = metric
            result.warnings.append(
                "Table rows carry no labels; assumed length, width and Q in that order."
            )
        else:
            result.warnings.append(
                f"Table has {len(rows)} unlabelled row(s); expected labelled rows or "
                "exactly length, width and Q."
            )
            return True

    if header is not None:
        value_roles = list(header.roles)
        if value_roles and value_roles[0] == "label":
            value_roles = value_roles[1:]
        core = header.core
    else:
        value_roles = list(_HEADERLESS_ROLES)
        core = None

    seen: set[str] = set()
    for row in rows:
        if row.label is None:
            result.warnings.append(f"Unlabelled table row ignored: '{row.text}'.")
            continue
        title = _LABEL_TITLES[row.label]
        if row.kind == "malformed":
            result.warnings.append(f"{title}: could not parse table row '{row.text}'.")
            continue
        if row.label in seen:
            result.warnings.append(f"{title}: duplicate row ignored.")
            continue
        seen.add(row.label)
        cells = list(row.cells)
        row_roles = value_roles
        if header is None:
            if len(cells) not in (1, 4):
                result.warnings.append(
                    f"{title}: headerless rows are read as range or range, mean, median, "
                    f"S.D.; {len(cells)} cells found, only the range was used."
                )
                cells = cells[:1]
            # A range-only row is a complete headerless row, not a short one.
            row_roles = value_roles[: len(cells)]
        pairs: list[tuple[str | None, str | None]] = []
        for index, role in enumerate(row_roles):
            if index < len(cells):
                pairs.append((role, cells[index]))
            elif role is not None:
                result.warnings.append(f"{title}: no cell for the '{role}' column.")
        extra = len(cells) - len(row_roles)
        if extra > 0:
            result.warnings.append(f"{title}: {extra} extra cell(s) ignored.")
        _apply_row(result, row.label, pairs, core)

    if header is None and result.metric_details:
        result.warnings.append(
            "Headerless table read with the documented layout (range, mean, median, "
            "S.D.); range meanings are unspecified."
        )
    if any(
        d.mean_interval is not None or d.median is not None or d.sd is not None
        for d in result.metric_details.values()
    ):
        result.warnings.append(
            "Table mean, median and S.D. captured as reported statistics; they are "
            "not applied to the legacy editor fields."
        )
    return True


# --- Public entry point ------------------------------------------------------


def parse_measurement_string(raw: str) -> MeasurementParseResult:
    """Parse a literature measurement string or pasted table.

    The parser is defensive: unrecognised input does not raise — it simply
    yields a result with ``ok == False`` and a warning explaining what could
    not be recognised. The caller can keep the user's manual edits.
    """
    result = MeasurementParseResult(raw=raw or "")
    if not raw or not raw.strip():
        return result

    structured = _normalise_structure(raw)
    normalised = _WHITESPACE_RE.sub(" ", structured).strip()
    result.normalised = normalised
    if not normalised:
        return result

    if _parse_table(structured, result):
        return result

    remainder, named = _strip_named_values(normalised, result.warnings)

    # --- Q and Qm / Qav ------------------------------------------------------
    q_raw = named.get("q")
    qm_raw = named.get("qm")
    q_mean_interval: IntervalStatistic | None = None
    if q_raw:
        result.q = _parse_range(q_raw, label="Q", warnings=result.warnings)
    if qm_raw:
        stat = _parse_statistic(qm_raw)
        if isinstance(stat, ScalarStatistic):
            result.q_mean = stat.value
        elif isinstance(stat, IntervalStatistic):
            q_mean_interval = stat
        else:
            result.warnings.append(f"Qm: could not parse '{qm_raw}'.")
        if result.q.p50 is not None:
            result.warnings.append(
                "Q: a single Q value and a reported Q mean (Qm/Qav) are both given; "
                "both kept, Qm/Qav treated as the mean."
            )
    if not q_raw and not qm_raw:
        result.warnings.append("Q not present in source.")

    n_raw = named.get("n")
    if n_raw:
        result.n = _parse_n(n_raw)
        if result.n is None:
            result.warnings.append(f"n: could not parse '{n_raw}'.")

    # --- Length and Width --------------------------------------------------
    dim_parts = _split_dimensions(remainder)
    if dim_parts:
        if len(dim_parts) >= 1:
            result.length = _parse_range(
                dim_parts[0], label="Length", warnings=result.warnings
            )
            result.warnings.append("Parsed first range as length.")
        if len(dim_parts) >= 2:
            result.width = _parse_range(
                dim_parts[1], label="Width", warnings=result.warnings
            )
        else:
            result.warnings.append("Only one range found; width left empty.")

        if len(dim_parts) > 2:
            result.warnings.append(
                f"Ignored {len(dim_parts) - 2} extra range(s) after width."
            )

        if result.length.p50 is None:
            result.warnings.append("Length: no centre/mean value found.")
        if result.width.p50 is None:
            result.warnings.append("Width: no centre/mean value found.")
    elif not result.ok:
        result.warnings.append("No length / width range found.")

    for metric in _METRICS:
        outer, core = _range_descriptors(getattr(result, metric), None)
        details = MetricDetails(
            outer_range=outer,
            core_range=core,
            mean_interval=q_mean_interval if metric == "q" else None,
        )
        if not details.is_empty():
            result.metric_details[metric] = details

    return result


def _split_dimensions(text: str) -> list[str]:
    """Split the length/width portion on the (already normalised) ``x``.

    The normaliser folds ``×`` to ``x``. A hostname-style 'x' inside numbers
    is impossible because numbers contain only digits and dots.
    """
    if not text:
        return []
    parts = [
        p.strip(" ,;")
        for p in re.split(r"\s*x\s*", text, flags=re.IGNORECASE)
        if p and p.strip()
    ]
    return [p for p in parts if p]


def swap_length_width(result: MeasurementParseResult) -> MeasurementParseResult:
    """Return a new result with length and width swapped.

    Useful when the source string actually lists width first (rare, but some
    older keys do). Numbers, scalar means and typed details move together;
    Q, Qm and n are unchanged.
    """
    details = {
        {"length": "width", "width": "length"}.get(metric, metric): value
        for metric, value in result.metric_details.items()
    }
    swapped = MeasurementParseResult(
        length=result.width,
        width=result.length,
        q=result.q,
        q_mean=result.q_mean,
        n=result.n,
        warnings=list(result.warnings) + ["Length and width swapped."],
        raw=result.raw,
        normalised=result.normalised,
        length_mean=result.width_mean,
        width_mean=result.length_mean,
        metric_details=details,
    )
    return swapped


__all__: Iterable[str] = (
    "DimensionRange",
    "MeasurementParseResult",
    "parse_measurement_string",
    "swap_length_width",
)
