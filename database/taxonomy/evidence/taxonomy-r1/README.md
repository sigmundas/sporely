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
