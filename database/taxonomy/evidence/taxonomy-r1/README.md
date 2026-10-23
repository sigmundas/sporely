# Taxonomy reconciliation R1 (candidate build only)

Owner instruction 2026-10-23: build and test a candidate release that retires
duplicate NorTaxa concepts onto their COL survivors, using evidence already
audited on branch `audit/taxonomy-reconciliation`. **Not a release approval.**
Every ledger record and approval written for R1 carries
`approval_scope: candidate_build_only` and `release_approval: pending`.

| Input | Pairs | Used | Held |
|---|---|---|---|
| S1 (`A0-species`, `A0-infraspecific`) | 988 | 988 | 0 |
| relationship pass 3, `rel3-verified` | 147 | 129 | 18 (live observation references, `reference-holds.json`) |
| decision package E, E1 | 42 | 40 | 2 (family differs, no recorded rationale) |
| decision package E, E0 (pairing corrections) | 4 | 3 | 1 (Henningsomyces puber, family differs) |

No E2/E3 pair is used. Policy: `mapping_policy.yml` `r1_candidate_amendments`
(P1/P4/P5/P7 batch-manifest acceptance, P10 inherited usages).

- `manifests/source/`: byte-identical copies of the audit inputs (pinned in the generator).
- `manifests/r1-*.manifest.json`: derived manifests minus held pairs.
- `generate_r1_supersessions.py`: renders the R1 records into `policies/concept_supersessions.yml` (`--check`).
- `candidate-comparison.json`: installed `tax-2026.10.07-01` vs candidate `tax-2026.10.23-01`.

## Export populations (desktop vs cloud)

P10 inherited names: 1,686 rows in the desktop SQLite and the full cloud
export; 910 after the macrofungi cloud scope. R1 anchor bridges: 1,160 in the
full export; 539 in cloud scope (`candidate-comparison.json`).

## Release guard

The artifact carries `taxonomy_meta.release_approval =
pending:candidate_build_only`. `promote_desktop_bundle.py` and the cloud
export refuse it without `--release-decision` naming its content_release_id
and sqlite_sha256 (`scripts/release_decision.py`); `--candidate-validation-only`
exports for local validation only.

## Fresh reference check (2026-10-23)

`reference-check/r1_reference_check.py` (desktop copies; cloud read-only via
docker psql and the existing env file). `s1-cloud_reference_check.fixed.sql`
is the audit's S1 SQL with `original_namespace` corrected to
`original_source_namespace`. Results: 0 desktop or cloud references to any of
the 2,513 retired ids; the 18 held ids are referenced (desktop 7 observations,
cloud 35 resolved rows on 16 ids, 3 reference uses), so they stay held.
