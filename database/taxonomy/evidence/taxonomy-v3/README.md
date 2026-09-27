# Taxonomy v3 evidence

Plan: `docs/plans/active/2026-09-27-taxonomy-v3.md`.

## Stage 0 — coverage audit (`stage0/`)

Reproduce from the repository root:

```sh
.venv/bin/python database/taxonomy/evidence/taxonomy-v3/audit_stage0_coverage.py
```

Inputs:

- the tracked release `database/reference_data/generated/taxonomy_v2/tax-2026.09.26-02.sqlite3.gz`,
  checked against `manifest.json`;
- the gitignored source archives `database/taxonomy/sources/nortaxa/1.284/archive.zip`
  and `database/taxonomy/sources/col_xr/2026-07-17-XR/archive.zip`. The script
  refuses to run unless each matches the `archive_sha256` the release records
  in `taxonomy_meta`;
- `database/taxonomy/policies/global-macrofungi-scope.yml` for the cloud scope.

Outputs are byte-deterministic:

- `coverage-report.json` — Group A and Group B, full release and cloud scope,
  by evidence class, with shared-synonym-count distributions, reviewed bridges
  in the release and the regression-species placements;
- `group-a-<evidence-class>.manifest.json` — one immutable candidate manifest
  per evidence class. `members_sha256` is SHA-256 over the canonical JSON of
  `{"columns", "members"}`; `pins` carries the release and archive
  fingerprints. Every member is `needs_review`.

Headline counts against `tax-2026.09.26-02`:

| | Full release | Cloud scope |
|---|---:|---:|
| Group A associations | 19,807 | 7,099 |
| — `reciprocal_accepted_synonymy` | 0 | 0 |
| — `one_directional_accepted_synonymy` | 1 | 0 |
| — `shared_synonymy` | 4,861 | 1,888 |
| — `no_published_cross_reference` | 14,945 | 5,211 |
| Group B pairs (NorTaxa concepts) | 7,423 (7,338) | 2,132 (2,113) |
| — `one_directional_accepted_synonymy` | 62 | 15 |
| — `shared_synonymy` | 2,032 | 574 |
| — `no_published_cross_reference` | 5,329 | 1,543 |

Group B's cloud column counts pairs whose COL side is in scope; no NorTaxa side
is. The cloud scope is 52,917 concepts, all COL-canonical.

Group A has no reciprocal candidate: the only one in the v2 closeout, 52369,
is now a reviewed supersession. Conocybe vexans / Pholiotina vexans is not in
Group B, because the canonical names differ; its pair (NorTaxa 58766, COL XQZ6)
is graded explicitly and is `reciprocal_accepted_synonymy`.
