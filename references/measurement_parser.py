"""Parser for fungal spore reference measurement strings.

Mycology literature commonly reports spore dimensions like

    10.2-12.5 x 5.5-6.8 µm

or, more fully,

    (9.5-)9.8-11.3(-11.7) x (7.3-)8.0-9.4(-9.4) µm,
    Q = (1.1-)1.1-1.3(-1.3), Qm = 1.2, n = 36

The parenthesised outer values are extreme observations; the unparenthesised
inner range is the "typical" bulk of measurements (not a strict statistical
percentile — most sources do not publish their cut-off rule).

The Sporely reference data model uses these field names:

    min   = extreme min          (existing DB column ``*_min``)
    p05   = typical min          (existing DB column ``*_p05``)
    p50   = centre / median      (existing DB column ``*_p50``)
    p95   = typical max          (existing DB column ``*_p95``)
    max   = extreme max          (existing DB column ``*_max``)

The legacy ``p05`` / ``p95`` labels are kept for compatibility with existing
data, sync code, and the landing page; this parser populates those fields but
never claims they are statistical percentiles.

Qm (Q mean) is stored separately from Q centre. If the source string carries
both a Q range and an explicit ``Qm = ...`` value, both are preserved.

No Q value is ever derived from length / width — the parser only reports what
the source string actually contains.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, asdict
from typing import Iterable


# --- Normalisation -----------------------------------------------------------

_DASH_RE = re.compile(r"[‐‑‒–—―−]")
_COMMA_DECIMAL_RE = re.compile(r"(?<=\d),(?=\d)")
_MUL_RE = re.compile(r"[×✕✖∗*]")
_UNIT_RE = re.compile(r"[µμ]m|\bum\b", re.IGNORECASE)
_WHITESPACE_RE = re.compile(r"\s+")


def _normalise(text: str) -> str:
    """Fold the input into a single canonical form for downstream parsing."""
    s = text or ""
    s = _DASH_RE.sub("-", s)
    s = _COMMA_DECIMAL_RE.sub(".", s)
    s = _MUL_RE.sub("x", s)
    s = _UNIT_RE.sub(" ", s)
    s = s.replace(" ", " ")
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


@dataclass
class MeasurementParseResult:
    """Structured result of parsing one literature measurement string."""

    length: DimensionRange = field(default_factory=DimensionRange)
    width: DimensionRange = field(default_factory=DimensionRange)
    q: DimensionRange = field(default_factory=DimensionRange)
    q_mean: float | None = None
    n: int | None = None
    warnings: list[str] = field(default_factory=list)
    raw: str = ""
    normalised: str = ""

    @property
    def ok(self) -> bool:
        """True when at least one numeric value was successfully parsed."""
        return (
            not self.length.is_empty()
            or not self.width.is_empty()
            or not self.q.is_empty()
            or self.q_mean is not None
            or self.n is not None
        )

    def to_record_dict(self) -> dict[str, float | int | None]:
        """Flat dict using the legacy DB column names.

        Includes ``q_p05`` / ``q_p95`` even though they are added by a small
        schema migration; older code paths that do not know these keys will
        ignore them.
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


# --- Range parser ------------------------------------------------------------

_NUM = r"\d+(?:\.\d+)?"

# Full ``(A-)B-C(-D)`` shape. The two parenthesised groups are optional, the
# inner ``B-C`` range is required.
_PAREN_RANGE_RE = re.compile(
    rf"""
    ^\s*
    (?:\(\s*(?P<emin>{_NUM})\s*-\s*\)\s*)?
    (?P<tmin>{_NUM})\s*-\s*(?P<tmax>{_NUM})
    (?:\s*\(\s*-\s*(?P<emax>{_NUM})\s*\))?
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


# --- Tagged-value extractor --------------------------------------------------

# Order matters: extract ``Qm`` before ``Q`` so the regex does not eat the
# ``Q`` of ``Qm``. ``n`` is taken last and only matches as a standalone token.
_NAMED_VALUE_PATTERNS = (
    ("qm", re.compile(r"(?<![A-Za-z])Qm\s*=\s*([^,;]+)", re.IGNORECASE)),
    ("q", re.compile(r"(?<![A-Za-z])Q\s*=\s*([^,;]+)", re.IGNORECASE)),
    ("n", re.compile(r"(?<![A-Za-z])n\s*=\s*([^,;]+)", re.IGNORECASE)),
)


def _strip_named_values(text: str) -> tuple[str, dict[str, str]]:
    """Pull ``Qm = ...``, ``Q = ...``, ``n = ...`` out of the string.

    Returns the remainder (with those tokens removed) and a dict of the raw
    extracted value strings keyed by ``'qm'`` / ``'q'`` / ``'n'``.
    """
    remainder = text
    extracted: dict[str, str] = {}
    for key, pattern in _NAMED_VALUE_PATTERNS:
        m = pattern.search(remainder)
        if not m:
            continue
        extracted[key] = m.group(1).strip().rstrip(",;").strip()
        remainder = (remainder[: m.start()] + remainder[m.end() :]).strip(" ,;")
    return remainder, extracted


def _parse_scalar(text: str) -> float | None:
    """Parse a single numeric value (used for Qm, single Q, sample size)."""
    m = _SINGLE_NUM_RE.match(text)
    if m:
        return float(m.group("v"))
    return None


def _parse_n(text: str) -> int | None:
    m = re.match(r"^\s*(\d+)\s*$", text or "")
    if m:
        return int(m.group(1))
    return None


# --- Public entry point ------------------------------------------------------


def parse_measurement_string(raw: str) -> MeasurementParseResult:
    """Parse a literature measurement string.

    The parser is defensive: unrecognised input does not raise — it simply
    yields a result with ``ok == False`` and a warning explaining what could
    not be recognised. The caller can keep the user's manual edits.
    """
    result = MeasurementParseResult(raw=raw or "")
    if not raw or not raw.strip():
        return result

    normalised = _normalise(raw)
    result.normalised = normalised
    if not normalised:
        return result

    remainder, named = _strip_named_values(normalised)

    # --- Q and Qm ----------------------------------------------------------
    q_raw = named.get("q")
    qm_raw = named.get("qm")
    if q_raw:
        result.q = _parse_range(q_raw, label="Q", warnings=result.warnings)
    if qm_raw:
        result.q_mean = _parse_scalar(qm_raw)
        if result.q_mean is None:
            result.warnings.append(f"Qm: could not parse '{qm_raw}'.")
    if not q_raw and not qm_raw:
        result.warnings.append("Q not present in source.")

    n_raw = named.get("n")
    if n_raw:
        result.n = _parse_n(n_raw)
        if result.n is None:
            result.warnings.append(f"n: could not parse '{n_raw}'.")

    # --- Length and Width --------------------------------------------------
    dim_parts = _split_dimensions(remainder)
    if not dim_parts:
        if not result.ok:
            result.warnings.append("No length / width range found.")
        return result

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
    older keys do). Q, Qm and n are unchanged.
    """
    swapped = MeasurementParseResult(
        length=result.width,
        width=result.length,
        q=result.q,
        q_mean=result.q_mean,
        n=result.n,
        warnings=list(result.warnings) + ["Length and width swapped."],
        raw=result.raw,
        normalised=result.normalised,
    )
    return swapped


__all__: Iterable[str] = (
    "DimensionRange",
    "MeasurementParseResult",
    "parse_measurement_string",
    "swap_length_width",
)
