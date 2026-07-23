# Stage 0 taxonomy baseline

Captured 2026-07-23 without mutating production.

The Artsorakel evidence in `artsorakel-candolleomyces-sanitized.json` was
sanitized from local observation 611. The response
returned `scientific_name_id = NBIC:54995`, accepted display name
`Candolleomyces candolleanus`, former/source name `Psathyrella candolleana`, and
vernacular name `hvit sprøsopp`.

The bundled SQLite stores NorTaxa Darwin Core `id` as its local primary key.
For this row, `id`, `taxonID`, and `acceptedNameUsageID` all happen to be
`54995`; they remain distinct contracts. SQLite contains `Psathyrella
candolleana` and `hvit sprøsopp`, but neither `NBIC:54995` nor
`Candolleomyces candolleanus`.

The live public RPC reproduced the cloud failure. It returned no rows for the
NBIC identifier, its numeric suffix, or the accepted name. The former and
vernacular names each returned four duplicate rows for taxon 54995. Source
inspection explains this: the importer omits `scientific_name_min`, and
`search_taxa` searches canonical and vernacular names only, not identifiers or
scientific aliases.

Live relation counts, duplicate/orphan aggregates, and sizes are explicitly
`not_measured` in the JSON because public read-only access is constrained by
RLS and does not expose PostgreSQL catalogs. By the 2026-07-23 plan decision,
these are a mandatory Stage 6 entry gate and do not block Stage 1.

`regression-corpus.json` freezes 100 queries: 25 accepted scientific names, 20
scientific synonyms, 25 vernacular names, 15 source-qualified external IDs, and
15 missing/negative controls. It includes diacritics, `se`/`sma`/`smj` Sámi
language coverage controls, and all three Candolleomyces regression names.

Reproduce local results with the statements in `baseline_queries.sql` and:

```text
./.venv/bin/python database/taxonomy/evidence/baseline/validate_regression_corpus.py
```
