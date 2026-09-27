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
  by evidence class, with shared-synonym-count distributions, the
  authoritative NorTaxa emission census and the regression-species placements;
- `group-a-<evidence-class>.manifest.json` — one immutable candidate manifest
  per evidence class. `members_sha256` is SHA-256 over the canonical JSON of
  `{"columns", "members"}`; `pins` carries the release and archive
  fingerprints. Every member is `needs_review`.
- `group-a-shared-synonymy--<review-class>.manifest.json` — the
  `shared_synonymy` manifest partitioned by the fixed, precedence-ordered rules
  in `SHARED_REVIEW_CLASSES`, so strong evidence can be approved by
  fingerprint without approving weak cases with it. Each review manifest
  carries its `membership_rule`, the precedence list, the parent's
  `members_sha256` and its cloud-scope count; the audit fails unless the
  classes partition the parent exactly. The parent stays whole for accounting.

`members_sha256` covers only the columns and members, so an empty manifest has
the same value in every class. `file_sha256` (in `coverage-report.json`) also
covers the pins, evidence class and review class: approve by `file_sha256`.

Headline counts against `tax-2026.09.26-02`:

| | Full release | Cloud scope |
|---|---:|---:|
| Group A associations | 19,807 | 7,099 |
| — `reciprocal_accepted_synonymy` | 0 | 0 |
| — `one_directional_accepted_synonymy` | 0 | 0 |
| — `shared_synonymy` | 4,861 | 1,888 |
| — `no_published_cross_reference` | 14,946 | 5,211 |

`shared_synonymy` review classes, in precedence order:

| Review class | Full release | Cloud scope |
|---|---:|---:|
| `non_species_rank` | 12 | 5 |
| `infraspecific_rank` | 35 | 6 |
| `accepted_authorship_disagrees` | 6 | 0 |
| `only_ined` | 15 | 5 |
| `only_invalid_name` | 7 | 1 |
| `only_accepted_name_variant` | 62 | 39 |
| `only_unauthored` | 66 | 8 |
| `only_mixed_weak` | 1 | 0 |
| `single_shared_synonym_low_overlap` | 39 | 13 |
| `ordinary` | 4,618 | 1,811 |

The one former `one_directional_accepted_synonymy` member, NorTaxa 227128 /
COL 35YQ2 (Sporely 3841), was a grading artifact: NorTaxa publishes a synonym
usage (226877) spelled exactly like its own accepted name, which
`cross_reference_evidence.grade` used to count as listing COL's identical
accepted name. It now grades `no_published_cross_reference`. No Group-B pair
changed class.
| Group B pairs (NorTaxa concepts) | 7,423 (7,338) | 2,132 (2,113) |
| — `one_directional_accepted_synonymy` | 62 | 15 |
| — `shared_synonymy` | 2,032 | 574 |
| — `no_published_cross_reference` | 5,329 | 1,543 |

Group B's cloud column counts pairs whose COL side is in scope; no NorTaxa side
is. The cloud scope is 52,917 concepts, all COL-canonical.

Authoritative NorTaxa emission on the cloud scope is exactly 53482 → 7821 and
52369, 58722 → 83668, computed as `cloud_export.emit_taxon_external_id_authoritative`
does. Every raw NorTaxa row on those two concepts is reconciled: 58722 is
published because the reviewed supersession re-keyed it, while the seven
synonym rows on 7821 are `intra_source_synonym`, which
`mapping_policy.yml.authoritative_bridge_emission` does not admit.

Group A has no reciprocal candidate: the only one in the v2 closeout, 52369,
is now a reviewed supersession. Conocybe vexans / Pholiotina vexans is not in
Group B, because the canonical names differ; its pair (NorTaxa 58766, COL XQZ6)
is graded explicitly and is `reciprocal_accepted_synonymy`.

NBIC:56449, which sporely-web's `taxonomy-v2.test.js` uses as an id that does
not resolve, is a real NorTaxa taxon: Gloeophyllum odoratum, NorTaxa-canonical
Sporely 626327, in Group B with COL 3GBK2 (Sporely 11307, `shared_synonymy`).
It is a temporary fixture. If Stage 2 reconciles that pair, the test needs a
synthetic id.
