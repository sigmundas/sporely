# NorTaxa 1.284 — pinned national-source profile

This directory pins the source profile for NorTaxa release `1.284`, issued
`2026-07-17`. It is the first real consumer of the national-source adapter
toolkit at `database/taxonomy/scripts/national_source.py`.

- `source.json` — pinned profile bound to the identifier namespaces in
  `docs/identity-contract.md`: `nortaxa_dwc_id`, `nortaxa_taxon_id`,
  `nortaxa_accepted_name_usage_id`, `nortaxa_parent_name_usage_id`. External
  identifiers keep the `NBIC:` prefix verbatim. The core lives at
  `taxon.txt`, vernacular at `vernacularname.txt`, distribution at
  `distribution.txt` — matching the archive's declared meta.xml.
- `synthetic-fixture.zip` — small offline test archive mirroring the real
  archive's shape (Taxon core with tab-delimited layout, VernacularName
  under `dwc:vernacularName` + `dc:language`, GBIF-namespace Distribution).
  Not real data; used exclusively by
  `database/taxonomy/tests/test_national_source_nortaxa.py`.

## Status

**Acquisition evidence and SHA-256 are tracked** in
`sources/nortaxa/1.284/manifest.json` (attempts 1–4; the `download` and
`validation` blocks reflect the attempt-4 promotion). The expected archive
is 8,399,460 bytes with SHA-256
`29c11c54d955dc44e4e5a38944dd7932989a256d1b173777579b9f33abd2fe22`.

**The raw archive is Git-ignored** and lives at
`database/taxonomy/sources/nortaxa/1.284/archive.zip` when present. A
fresh checkout may not contain it; the manifest is the source of truth.
On a machine where `archive.zip` exists, another `--execute` run
**refuses** (see Commands below).

Under the compilation-vs-acquisition boundary:

- `compiler_ready: true` — no compilation-blocking checks fail.
- `hierarchy_complete: false` — 72 unresolved parent references remain
  as hierarchy warnings (raw identifiers preserved verbatim; the compiler
  reconciles them).
- `reference_gaps.orphan_parent_reference_count: 72`
- `reference_gaps.orphan_accepted_reference_count: 0`
- `taxon_column_gaps.species_like_rows_missing_genus: 2` (all other column
  gaps 0)

## Commands

On a machine where `archive.zip` already exists, another `--execute` run
**refuses**:

```
error: final archive already exists: …/sources/nortaxa/1.284/archive.zip
```

That refusal is intentional — the promoted archive is one-per-release. To
validate the retained archive against this pinned profile:

```bash
./.venv/bin/python database/taxonomy/scripts/national_source.py validate \
  --profile database/taxonomy/national_sources/nortaxa/1.284/source.json \
  --archive database/taxonomy/sources/nortaxa/1.284/archive.zip
```

To emit normalized compiler input:

```bash
./.venv/bin/python database/taxonomy/scripts/national_source.py normalize \
  --profile database/taxonomy/national_sources/nortaxa/1.284/source.json \
  --archive database/taxonomy/sources/nortaxa/1.284/archive.zip \
  --output build/nortaxa/1.284
```

Both commands are offline. `validate` opens no output; `normalize` writes
`taxa.jsonl`, `vernacular.jsonl`, and `report.json` transactionally (a
temporary sibling directory is atomically renamed to the requested output
only after every step succeeds). Repeat runs against the same input are
deterministically byte-identical.

Downstream compiler work is out of scope; see the `national_sources/README.md`
adapter-contract document for how normalized JSONL feeds one shared Sporely
compiler across all national sources.
