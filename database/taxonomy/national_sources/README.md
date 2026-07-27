# National taxonomy source kit

## Pipeline

```
National archive
  → source adapter/profile
  → normalized taxonomy records
  → shared Sporely compiler
  → desktop SQLite and Supabase/cloud export
```

National sources do **not** each have their own compiler. NorTaxa (Norway),
the Swedish source, and any future Danish or other national source are
adapters that feed the single Sporely compiler. The kit here — one CLI
(`scripts/national_source.py`), one profile schema, one reusable Darwin Core
Archive parser — is intentionally the whole framework.

## Contract every national adapter satisfies

The profile is a JSON file. The CLI's `normalize` command reads a profile plus
a Darwin Core Archive and emits normalized JSONL files that share the same
record shape across countries.

**Taxon records** (`taxa.jsonl`, one JSON object per line):

- `source_code` — profile identifier (e.g. `nortaxa`, `dyntaxa`, `example`).
- `source_release` — `{version, issued_date}` from the profile.
- `raw_source_usage_id` — the archive's core row ID, preserved as text.
- `identifier_namespace` — a short prefix (e.g. `NBIC:`, `DYNTAXA:`).
- `accepted_usage_id` — DwC `acceptedNameUsageID` as text or `null`.
- `parent_usage_id` — DwC `parentNameUsageID` as text or `null`.
- `scientific_name`, `authorship`, `rank`, `taxonomic_status`.
- `external_ids` — optional namespaced identifiers preserved verbatim.
- `provenance` — `{source_code, source_release, identifier_namespace, member, row_index}`.

**Vernacular records** (`vernacular.jsonl`):

- `source_code`, `source_release`.
- `accepted_usage_id` — the vernacular row's link to the core.
- `vernacular_name`, `language`, `is_preferred`.
- `provenance` — same shape as above.

## Rules

- Never convert source IDs into Sporely IDs. The central registry (created
  later, in the compiler) owns `sporely_taxon_id`.
- Never assume IDs are integers. Store them as text.
- Never equate identical names with identical taxa.
- Never strip prefixes such as `NBIC:` or `DYNTAXA:`.
- Adapters only normalize; the compiler decides identity.
- Unsupported source structures fail with a useful report rather than being
  guessed at. Unknown extension row types are refused; a Distribution extension
  is validated only and never imported.

## Scope of the first version

Supported today:

1. Darwin Core Archives with a top-level `meta.xml`.
2. Configurable Taxon (core) and VernacularName (extension) term mappings.
3. Optional Distribution extension, validated (member existence, safe path,
   core-id linkage) but never imported.

Deliberately out of scope for the first version: dynamic Python-hook plugins,
a web UI, download-authorization framework, non-DwC-A raw formats. A JSON
profile plus this reusable adapter is enough for Norway, Sweden, and
potentially Denmark. UTF-8 CSV / TSV outside a DwC-A archive can be added if
Denmark actually needs it later.

## Commands

```bash
# Starter profile for a new country
./.venv/bin/python database/taxonomy/scripts/national_source.py \
  init denmark

# Inspect a candidate archive; the JSON output suggests a term mapping
./.venv/bin/python database/taxonomy/scripts/national_source.py \
  inspect --archive danish-taxonomy.zip

# Validate structure + linkage against the profile (no output emitted)
./.venv/bin/python database/taxonomy/scripts/national_source.py \
  validate --profile database/taxonomy/national_sources/denmark/source.json \
           --archive danish-taxonomy.zip

# Produce normalized compiler input at build/denmark/
./.venv/bin/python database/taxonomy/scripts/national_source.py \
  normalize --profile database/taxonomy/national_sources/denmark/source.json \
            --archive danish-taxonomy.zip \
            --output build/denmark
```

Once the shared taxonomy compiler exists, that stage will take one or more
normalized-source directories as input:

```bash
# Not yet implemented; sketched here so the interface is clear.
./.venv/bin/python database/taxonomy/scripts/compile_taxonomy.py \
  --base col_xr \
  --national-source build/denmark \
  --output build/taxonomy.sqlite
```

## Adding another country

1. Run `national_source.py init <code>` to scaffold `national_sources/<code>/source.json`.
2. Point the profile at your archive's actual `location` and `term_mapping` values.
3. Run `national_source.py inspect --archive path/to/archive.zip` and copy the
   suggested term mapping fragments into your profile as needed.
4. Run `national_source.py validate` until it reports `"result": "passed"`.
5. Run `national_source.py normalize` and check the emitted `taxa.jsonl`,
   `vernacular.jsonl`, and `report.json`.
6. Add your archive fixture (or a small synthetic one) plus a focused offline
   test alongside the existing `example/` fixture.

## Relationship to Sporely apps

Normalized output eventually feeds two consumers:

- `sporely-py` (this repository) — bundled SQLite lookup database.
- Supabase — cloud taxonomy tables and the `search_taxa` RPC that the web app
  calls.

The adapter interface itself lives in `sporely-py` because this repository
owns taxonomy compilation. No UI work is required until compiled releases are
actually published and selectable in the app.

## Example fixture

`example/source.json` and `example/fixture.zip` are a tiny synthetic
demonstration source. They are not real data. The fixture is used by
`database/taxonomy/tests/test_national_source.py` for offline round-trip
tests.
