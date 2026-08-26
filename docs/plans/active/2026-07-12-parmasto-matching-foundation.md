# Parmasto-style matching foundation

Status: Active; explicitly deferred from the completed spore-statistics release cycle.

## Agent handoff

- Status: Proposed.
- Last completed stage: The structured observation-summary pipeline and observation-balanced species profiles are complete.
- Current/next stage: Confirm enough structured summaries exist and define a transparent first scoring slice.
- Relevant commits: `41d3e2c`, `390efe0`, `20ed420`, `d0db5f3`.
- Important decisions: Match only from real measured summaries; use biological between-observation spread; keep explanations dimension-specific.
- Do not: Use midpoint-estimated means, SEM as biological tolerance, or introduce covariance/Mahalanobis scoring in the first slice.
- Remaining acceptance criteria: The acceptance criteria below.

### Goal

Prepare for ID assistance without implementing a full black-box matcher yet.

### Input

A query observation must have a real measured summary:

```text
query_Lm
query_Wm
query_Qm
query_n_paired
```

Do not match from midpoint-estimated means.

### Basic transparent score later

First version can use separate standardized deviations:

```text
zL = (query_Lm - grand_Lm) / max(sd_Lm, minimum_L_tolerance)
zW = (query_Wm - grand_Wm) / max(sd_Wm, minimum_W_tolerance)
zQ = (query_Qm - grand_Qm) / max(sd_Qm, minimum_Q_tolerance)
```

Then:

```text
distance = sqrt(zL² + zW² + zQ²)
```

Later improvement:

```text
Mahalanobis distance using covariance of observation means
```

Do not use SEM alone for matching. SEM becomes too small as the database grows and would make the matcher falsely strict. Matching should use biological between-observation spread.

### Tolerance intervals

Tolerance intervals may be added later:

```text
grand mean ± k * sd
```

But `k` must be chosen for a defined coverage/confidence level and enough observations. For small sample counts, label all intervals provisional.

Acceptance for this stage:

* Data needed for matching exists.
* No fake means are used.
* Species profile exposes between-observation SD.
* Match explanations can eventually say which dimension fits or fails.

---
