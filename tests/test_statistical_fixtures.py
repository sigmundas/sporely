"""Statistical-fixture pin tests for the Parmasto-style species profile
and Fannechère-style decile summaries.

These are literature-derived reference fixtures. They exist to lock down
the arithmetic that will underpin Stage I (Parmasto matching) BEFORE any
matcher / distance / ranking code is written. If any of these assertions
starts failing, the underlying mean / variance / decile convention has
drifted — investigate before touching the matcher.

Percentile / decile convention: numpy default (`method='linear'`, equivalent
to `numpy.percentile(...)` without explicit `method=`). This matches the
convention pinned by the Stage C computation module
(`utils.spore_summary._percentile_bundle`) and the Stage E public RPC.

Standard deviation convention: sample SD (`ddof=1`) for structured
summaries; population variance (`ddof=0`) is asserted separately in the
Möls fixture only because the Parmasto/Möls paper reports both.

**Stage I guard.** The final test in this module asserts that no matcher /
distance / ranking API has been added to the spore summary modules. This
is a deliberate pre-Stage-I fence — Parmasto matching remains explicitly
deferred per the plan.
"""

from __future__ import annotations

import math

import numpy as np
import pytest


# ---------------------------------------------------------------------------
# Fixture 1 — Parmasto/Möls Auricularia auricula mean-length distribution.
# ---------------------------------------------------------------------------
#
# 33 observations of mean spore length (µm) for Auricularia auricula, as
# published in the Möls dataset that underpins the Parmasto-style
# specimen-balanced profile approach. Numbers reproduced from the source
# table so future refactors of the aggregator can be checked against the
# same reference.

_MOLS_AURICULARIA_LENGTHS = [
    12.8, 13.8, 14.3, 15.5, 18.6, 20.1, 20.7,
    13.2, 13.8, 14.5, 15.6, 19.1, 20.2, 20.7,
    13.7, 13.9, 14.6, 15.6, 19.1, 20.3, 20.9,
    13.7, 14.1, 14.7, 15.7, 19.5, 20.6,
    13.7, 14.1, 15.5, 16.2, 20.0, 20.6,
]


def test_mols_auricularia_basics():
    arr = np.asarray(_MOLS_AURICULARIA_LENGTHS, dtype=float)
    assert arr.size == 33
    assert math.isclose(float(arr.sum()), 549.4, abs_tol=1e-9)
    assert math.isclose(float((arr * arr).sum()), 9415.48, abs_tol=1e-9)
    assert float(arr.mean()) == pytest.approx(16.6484848485, abs=1e-9)


def test_mols_auricularia_sample_variance():
    """Sample variance (Bessel-corrected, ddof=1) — the flavor used by
    all structured summary SD columns (`length_sd_um`, `width_sd_um`,
    `q_sd`)."""
    arr = np.asarray(_MOLS_AURICULARIA_LENGTHS, dtype=float)
    assert float(arr.var(ddof=1)) == pytest.approx(8.4000757576, abs=1e-9)


def test_mols_auricularia_population_variance():
    """Population variance (ddof=0). Not used by summary columns, but
    the Möls / Parmasto tables report both — pinning ensures we never
    accidentally interpret a ddof=0 value as ddof=1 or vice versa."""
    arr = np.asarray(_MOLS_AURICULARIA_LENGTHS, dtype=float)
    assert float(arr.var(ddof=0)) == pytest.approx(8.1455280073, abs=1e-9)


# ---------------------------------------------------------------------------
# Fixture 2 — Fannechère 40-measurement decile fixture.
# ---------------------------------------------------------------------------
#
# 40 spore length measurements used in Fannechère's worked example of the
# decile-based summary format `N = <n> ; D1,9 ; (min) D1-D9 (max)`.

_FANNECHERE_40 = [
    10.5, 8.6, 6.9, 11.7, 12.1, 11.5, 11.1, 10.5, 7.2, 8.3,
    9.8, 8.7, 9.5, 12.4, 9.2, 10.8, 5.6, 11.0, 8.8, 9.8,
    8.7, 10.5, 9.4, 7.5, 11.6, 10.7, 9.2, 7.3, 12.9, 11.5,
    13.2, 9.7, 6.8, 12.3, 13.7, 6.1, 9.3, 10.7, 7.4, 7.5,
]


def test_fannechere_40_basics():
    arr = np.asarray(_FANNECHERE_40, dtype=float)
    assert arr.size == 40
    assert float(arr.mean()) == pytest.approx(9.75, abs=1e-9)
    assert float(arr.std(ddof=1)) == pytest.approx(2.030914913189468, abs=1e-9)
    assert float(arr.min()) == pytest.approx(5.6, abs=1e-9)
    assert float(arr.max()) == pytest.approx(13.7, abs=1e-9)


def test_fannechere_40_deciles_use_linear_convention():
    """D1 / D9 under `numpy.percentile(x, [10, 90])` (linear
    interpolation between order statistics). This is the convention
    Stage C's percentile bundle uses, and it reproduces Fannechère's
    published values D1 = 7.17 and D9 = 12.31 exactly."""
    arr = np.asarray(_FANNECHERE_40, dtype=float)
    assert float(np.percentile(arr, 10)) == pytest.approx(7.17, abs=1e-9)
    assert float(np.percentile(arr, 90)) == pytest.approx(12.31, abs=1e-9)


def test_fannechere_40_summary_string_shape_optional():
    """Optional format check.

    The plan asks for the classic Fannechère summary string
    `N = 40 ; D1,9 ; (5.6) 7.17-12.31 (13.7)` **if the formatter
    exists**. There is no such formatter in sporely-py today — the
    legacy `_format_measurement_stats_string` uses p5/p95 not deciles
    (see `ui/main_window.py`). Test kept as a placeholder that skips
    when the formatter is absent, so a future implementer sees the
    expected shape.
    """
    try:
        from utils.spore_summary import format_fannechere_decile_summary  # type: ignore
    except ImportError:
        pytest.skip("Fannechère decile formatter not implemented yet")
    else:
        arr = np.asarray(_FANNECHERE_40, dtype=float)
        rendered = format_fannechere_decile_summary(arr)
        assert rendered == "N = 40 ; D1,9 ; (5.6) 7.17-12.31 (13.7)"


# ---------------------------------------------------------------------------
# Fixture 3 — Fannechère outlier sensitivity (X = 6 vs. X = 4).
# ---------------------------------------------------------------------------
#
# Same template shifted by a single leading outlier. Confirms decile
# behavior with n = 8 under the linear convention. The template's tail
# is fixed so D9 stays at 10.26 in both cases — only D1 and the mean /
# SD are supposed to move.

_FANNECHERE_TAIL = [8.8, 9.1, 9.4, 9.8, 10.0, 10.2, 10.4]


def _fannechere_outlier_array(x: float) -> np.ndarray:
    return np.asarray([x, *_FANNECHERE_TAIL], dtype=float)


def _round_display(value: float, digits: int = 2) -> float:
    """Match the display-only rounding a UI or literature print would
    apply, so we can pin the reported values without confusing "same
    number, different rounding" for a test failure."""
    return float(f"{value:.{digits}f}")


def test_fannechere_outlier_x_equals_6():
    arr = _fannechere_outlier_array(6.0)
    # Underlying numbers.
    assert float(arr.mean()) == pytest.approx(9.2125, abs=1e-9)
    assert float(arr.std(ddof=1)) == pytest.approx(1.4085833815777975, abs=1e-9)
    assert float(np.percentile(arr, 50)) == pytest.approx(9.6, abs=1e-9)
    assert float(np.percentile(arr, 10)) == pytest.approx(7.96, abs=1e-9)
    assert float(np.percentile(arr, 90)) == pytest.approx(10.26, abs=1e-9)
    # Display-rounded values (two decimals — the literature convention).
    assert _round_display(float(arr.mean())) == 9.21
    assert _round_display(float(arr.std(ddof=1))) == 1.41


def test_fannechere_outlier_x_equals_4():
    """X = 4 case — reproduces the values produced by the project's
    linear decile convention.

    NOTE — NotebookLM / a source summary reported D1 = 7.70 for X = 4,
    but that does not match the same linear decile convention that
    correctly reproduces both the 40-measurement fixture (D1 = 7.17)
    and the X = 6 fixture (D1 = 7.96). Do not assert 7.70 here unless
    the original Fannechère text confirms a different rule (e.g. a
    Hyndman-Fan type other than #7 / numpy default). Under numpy
    linear interpolation the correct D1 is 7.36.
    """
    arr = _fannechere_outlier_array(4.0)
    assert float(arr.mean()) == pytest.approx(8.9625, abs=1e-9)
    assert float(arr.std(ddof=1)) == pytest.approx(2.0784180110294055, abs=1e-9)
    assert float(np.percentile(arr, 50)) == pytest.approx(9.6, abs=1e-9)
    assert float(np.percentile(arr, 10)) == pytest.approx(7.36, abs=1e-9)
    # D9 must be unchanged from the X = 6 case — the outlier only
    # affects the low tail.
    assert float(np.percentile(arr, 90)) == pytest.approx(10.26, abs=1e-9)
    # Display-rounded values.
    assert _round_display(float(arr.mean())) == 8.96
    assert _round_display(float(arr.std(ddof=1))) == 2.08


# ---------------------------------------------------------------------------
# Fixture 4 — Observation-balanced (Parmasto anti-weighting).
# ---------------------------------------------------------------------------
#
# The one fixture that most directly ties into the Stage G aggregator.
# Reuses the same A/B scenario the landing test pins in
# observationBalancedProfile.test.ts, but on the sporely-py side to
# guarantee both writer and reader agree on the arithmetic.


def test_observation_balanced_mean_beats_pooled_weighted_mean():
    """Species Lm from unweighted mean of per-observation means is 6.5.
    The spore-count-weighted (pooled) mean would drift toward
    ~7.8125 because observation B carries 300 spores and A carries 20.
    The observation-balanced canonical mean MUST be 6.5."""
    obs_means = np.asarray([5.0, 8.0], dtype=float)
    obs_ns = np.asarray([20, 300], dtype=float)

    # Observation-balanced: plain arithmetic mean over the per-observation
    # means. Weights are NOT applied.
    canonical = float(obs_means.mean())
    assert canonical == pytest.approx(6.5, abs=1e-12)

    # Pooled/weighted comparison — this value MUST NOT be used as the
    # canonical species mean anywhere in the project. Its expected
    # value is pinned here purely to prevent silent regression into a
    # weighted implementation without a corresponding test update.
    pooled = float((obs_means * obs_ns).sum() / obs_ns.sum())
    assert pooled == pytest.approx(7.8125, abs=1e-12)
    assert canonical != pytest.approx(pooled, abs=1e-6)


def test_observation_balanced_agrees_with_summary_helper():
    """The compute helper's paired-denominator aggregation reduces to
    the same unweighted mean on a synthetic two-observation dataset —
    a smoke test that ties the fixture math to real production code."""
    from utils.spore_summary import compute_observation_spore_summaries

    def _paired(l, w, obs_measurement_type="manual"):
        return {"length_um": l, "width_um": w, "measurement_type": obs_measurement_type}

    # Observation A: 20 paired rows with a mean length of 5.
    obs_a_rows = [_paired(5.0, 2.5) for _ in range(20)]
    # Observation B: 300 paired rows with a mean length of 8.
    obs_b_rows = [_paired(8.0, 4.0) for _ in range(300)]

    # The compute helper returns ONE summary row per observation-context.
    # Its `length_mean_um` IS the per-observation mean; taking the
    # unweighted mean across two observations gives the canonical Lm.
    summary_a = compute_observation_spore_summaries(
        observation_id=1, measurements=obs_a_rows,
    )[0]
    summary_b = compute_observation_spore_summaries(
        observation_id=2, measurements=obs_b_rows,
    )[0]

    assert summary_a["length_mean_um"] == pytest.approx(5.0, abs=1e-12)
    assert summary_b["length_mean_um"] == pytest.approx(8.0, abs=1e-12)
    assert summary_a["n_paired"] == 20
    assert summary_b["n_paired"] == 300

    # Unweighted species-level mean.
    canonical = (summary_a["length_mean_um"] + summary_b["length_mean_um"]) / 2.0
    assert canonical == pytest.approx(6.5, abs=1e-12)


# ---------------------------------------------------------------------------
# Fixture 5 — Stage I guard: no matcher API has been added.
# ---------------------------------------------------------------------------
#
# This test is a fence, not a math check. It fails if any of the
# summary/profile modules gain a matcher / distance / ranking API
# before Stage I is explicitly opened. Update the banned-names list
# only when adding a genuinely non-matcher symbol that happens to
# contain one of these tokens.


_MATCHER_TOKENS = (
    "mahalanobis",
    "z_score",
    "zscore",
    "match_species",
    "species_match",
    "match_score",
    "matcher",
    "parmasto_score",
    "parmasto_match",
    "compute_match",
    "rank_species",
    "species_ranking",
    "distance_to_species",
    "species_distance",
)


def _public_names(module) -> list[str]:
    return [name for name in dir(module) if not name.startswith("_")]


def test_no_parmasto_matcher_api_in_spore_summary_modules():
    """No public function/attribute in the summary modules matches any
    matcher-shaped token. Explicit list — update only when opening
    Stage I."""
    import utils.spore_summary
    import utils.spore_summary_sync

    for module in (utils.spore_summary, utils.spore_summary_sync):
        for name in _public_names(module):
            lower = name.lower()
            for banned in _MATCHER_TOKENS:
                assert banned not in lower, (
                    f"Unexpected matcher-shaped public name '{name}' in "
                    f"{module.__name__} — Stage I is not yet open."
                )
