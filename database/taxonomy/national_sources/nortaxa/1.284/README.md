# NorTaxa 1.284 — pinned national-source profile

This directory pins the source profile for NorTaxa release `1.284`, issued
`2026-07-17`. It is the first real consumer of the national-source adapter
toolkit at `database/taxonomy/scripts/national_source.py`.

- `source.json` — pinned profile bound to the identifier namespaces in
  `docs/identity-contract.md`: `nortaxa_dwc_id`, `nortaxa_taxon_id`,
  `nortaxa_accepted_name_usage_id`, `nortaxa_parent_name_usage_id`. The
  external-ID retention prefix is `NBIC:`. Taxon and VernacularName mappings
  are declared explicitly; the Distribution extension is pinned to the
  GBIF 1.0 namespace observed in the real 1.284 archive.
- `synthetic-fixture.zip` — small offline test archive mirroring the real
  archive's shape (Taxon core with CSV/quoted/2 header lines, VernacularName
  extension, GBIF-namespace Distribution). Not real data; used exclusively by
  `database/taxonomy/tests/test_national_source_nortaxa.py`.

The real archive lives at `database/taxonomy/sources/nortaxa/1.284/archive.zip`
after a successful manual acquisition
(`database/taxonomy/scripts/acquire_nortaxa.py --execute`). Once that archive
exists, the adapter commands become:

```bash
# Validate the official archive against this pinned profile.
./.venv/bin/python database/taxonomy/scripts/national_source.py validate \
  --profile database/taxonomy/national_sources/nortaxa/1.284/source.json \
  --archive database/taxonomy/sources/nortaxa/1.284/archive.zip

# Emit normalized compiler input at build/nortaxa/1.284/.
./.venv/bin/python database/taxonomy/scripts/national_source.py normalize \
  --profile database/taxonomy/national_sources/nortaxa/1.284/source.json \
  --archive database/taxonomy/sources/nortaxa/1.284/archive.zip \
  --output build/nortaxa/1.284
```

Both commands are offline. `validate` opens no output; `normalize` writes
`taxa.jsonl`, `vernacular.jsonl`, and `report.json` transactionally (a
temporary sibling directory is atomically renamed to the requested output
only after every step succeeds).

Downstream compiler work is out of scope; see the `national_sources/README.md`
adapter-contract document for how normalized JSONL feeds one shared Sporely
compiler across all national sources.
