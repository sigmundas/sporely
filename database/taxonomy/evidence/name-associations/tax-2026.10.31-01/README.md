# tax-2026.10.31-01 — exact-name association candidate

Candidate only: not approved, promoted, imported or bundled.

Derived from the frozen `tax-2026.10.23-11` SQLite (sha256 `2a81f684…`) by
`scripts/name_associations.py` under `policies/name_association_policy.json`.
No Sporely id is allocated, retired or renumbered; the build adds
`taxon_association_min` and projects associated NorTaxa/Dyntaxa ids, names,
vernaculars and Red List rows onto the primary (COL-backed) concept.

Reproduce from the repository root (normalized inputs are the `-11` build's
`norm/` directory; their sha256 values are in `association-report.json`):

```sh
.venv/bin/python database/taxonomy/scripts/name_associations.py \
  --base-gz database/reference_data/generated/taxonomy_v2/tax-2026.10.23-11.sqlite3.gz \
  --base-sqlite-sha256 2a81f68435d0606633319be0845f9fa1de46ccd9c2442f391af742fad3899a97 \
  --base-manifest database/reference_data/generated/taxonomy_v2/manifest.json \
  --norm-dir <releases>/tax-2026.10.23-11/build/norm \
  --policy database/taxonomy/policies/name_association_policy.json \
  --holds database/taxonomy/policies/source_continuity_holds.json \
  --release-id tax-2026.10.31-01 --out-dir <out>
```

Two clean builds gave byte-identical SQLite, gz, `associations.jsonl` and
report. The full per-association list (`associations.jsonl`, 27 MB) and the
artifact live in `<releases>/tax-2026.10.31-01/build/`.
