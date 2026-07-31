# Cloud taxonomy export (Stage W1) — contract

## Purpose and boundaries

W1 transforms a compiled taxonomy-v2 SQLite release into a deterministic set of
canonical JSONL datasets. A later stage (W2) is free to import these files into
any Supabase table decomposition it chooses.

W1 does NOT:

* connect to Supabase, execute SQL against Supabase, or author Supabase migrations;
* implement `search_taxa_v2`;
* modify observation identity columns;
* modify web taxonomy UX;
* implement observation backfill;
* encode any specific Model A/B/C decomposition — file shapes mirror the compiler,
  not W2's cloud tables.

W1 emits **files only**. If the compiled release contains multiple Sporely
concepts sharing a scientific name, all are exported unchanged. No repair, no
merging, no reinterpretation.

## Formal inputs

1. Compiled SQLite gzip artifact
   (default: `database/reference_data/generated/taxonomy_v2/<release>.sqlite3.gz`)
2. Outer compiled manifest
   (default: `database/reference_data/generated/taxonomy_v2/manifest.json`)
3. Read-only taxonomy policy directory for provenance hashing
   (default: `database/taxonomy/policies/`)
4. Output directory
   (default: `<manifest_dir>/cloud_export_<content_release_id>/`)

There is no prior-release input and no delta-export mode. Every run produces a
complete release export.

The scope predicate is not runtime-configurable. It is encoded in code under the
stable identifier `fungi_closure_union_nortaxa_v1`.

## Inclusion algorithm (`fungi_closure_union_nortaxa_v1`)

```
S = descendants(taxon_min WHERE taxon_rank='kingdom' AND canonical_scientific_name='Fungi')
    ∪ (taxon_id WHERE canonical_source_system='nortaxa')
```

Implementation:

* every `Fungi` kingdom root is discovered by predicate (no hard-coded IDs);
* recursion follows `parent_taxon_id`;
* traversal is defensive with a `MAX_TREE_DEPTH=200` cycle guard;
* ancestors above a `Fungi` root are not included unless independently included by
  the NorTaxa rule;
* no name-based concept linking is applied.

For the pinned release `tax-2026.07.30-02`:

* concepts included: **634,894**
* concepts excluded: **1** (a single `col_xr` row with `taxon_rank='domain'`,
  above the Fungi kingdom)

The exporter emits a hard regression assertion for this pinned release; other
releases use the same algorithm without the pinned assertion.

The first accepted scope exports nearly the full compiled artifact because all
COL Fungi rows plus all NorTaxa rows are retained. This is deliberate — see § C
of the W0 report. Do not describe the artifact as “compact” without qualification.

## Dataset schemas

Every data file is canonical JSONL (see canonical rules below). Fields are
selected from an explicit column allowlist verified against `PRAGMA table_info`
on the source tables; no `SELECT *` is used.

### `taxon.jsonl` (source: `taxon_min`)

```
taxon_id                          integer
parent_taxon_id                   integer | null
genus                             string
specific_epithet                  string
family                            string | null
norwegian_taxon_id                integer | null
swedish_taxon_id                  integer | null
inaturalist_taxon_id              integer | null
canonical_scientific_name         string | null
taxon_rank                        string | null
taxonomic_status                  string | null
source_system                     string | null
preferred_scientific_name_no      string | null
preferred_scientific_name_sv      string | null
sporely_content_release_id        string | null
canonical_source_system           string
canonical_external_id             string
```

Sort: `taxon_id ASC`.

### `scientific_name.jsonl` (source: `scientific_name_min`)

```
taxon_id                integer
language_code           string          # always "sci" in current release
scientific_name         string
is_preferred_name       boolean
source                  string | null
note                    string | null   # carries `alias_reason` when set:
                                        # synonym_of_accepted / cross_source_automatic_exact / manual_approved_exact
```

Sort: `taxon_id ASC, scientific_name ASC, language_code ASC, source ASC, note ASC`.

### `vernacular.jsonl` (source: `vernacular_min`)

```
taxon_id             integer
language_code        string
vernacular_name      string
is_preferred_name    boolean
source               string | null
```

Sort: `taxon_id ASC, language_code ASC, vernacular_name ASC, source ASC`.

Language codes are preserved verbatim. `nb`, `nn`, `no`, `se`, `sma`, `smj` are
distinct; no truncation, folding, or synthesis.

### `taxon_external_id.jsonl` — authoritative (two sources merged)

External IDs whose namespace is declared by the compiler. Two authoritative
source paths, merged in a single deterministic file:

1. **`taxon_external_id_text_min`** — every scoped row verbatim. Namespace
   is declared per `policies/source_priority.yml.identifier_namespaces`.

2. **Derived NorTaxa row from `taxon_min.norwegian_taxon_id`.** The compiler
   establishes `taxon_min.norwegian_taxon_id` only from a unique preferred
   source usage whose original namespace is `nortaxa_taxon_id` (per
   `docs/identity-contract.md` and the UNIQUE partial index
   `idx_taxon_no_id` in `build_sqlite_candidate.py`). W1 emits one derived
   row per scoped concept with a non-null `norwegian_taxon_id`:

   | Field | Value |
   |---|---|
   | `source_system` | `"nortaxa"` |
   | `namespace` | `"nortaxa_taxon_id"` |
   | `external_id` | `CAST(norwegian_taxon_id AS TEXT)` |
   | `id_role` | `"accepted"` |
   | `is_preferred` | `true` |
   | `external_name` | `canonical_scientific_name` |
   | `note` | `"derived_from_taxon_min.norwegian_taxon_id"` |

No other namespace is derived from the compiler's integer table; those
rows remain in `taxon_external_id_legacy_integer.jsonl` verbatim, even
when the same numeric value exists as a derived authoritative row.

```
taxon_id        integer
source_system   string
namespace       string           # non-null; declared by compiler
external_id     string
id_role         string
is_preferred    boolean
external_name   string | null
note            string | null
```

Sort: `taxon_id, source_system, namespace, external_id, id_role, is_preferred`.

Duplicate authoritative semantic keys
`(source_system, namespace, external_id, taxon_id)` are detected and
cause the export to fail.

### `taxon_external_id_legacy_integer.jsonl` — legacy (source: `taxon_external_id_min`)

External IDs from the compiler's integer table. The compiler does NOT preserve
the originating namespace in this table (e.g. `nortaxa_taxon_id` vs
`nortaxa_dwc_id`); only `source_system` remains. Rows are emitted under an
explicit legacy label so W2 cannot silently treat them as namespaced.

```
taxon_id        integer
source_system   string
external_id     string           # source column is INTEGER; cast to text
id_role         string
is_preferred    boolean
external_name   string | null
note            string | null
```

Sort: `taxon_id, source_system, external_id, id_role, is_preferred`.

Rules (both files):

* Never infer equivalence from numeric equality.
* `NBIC:` prefixes are preserved verbatim (authoritative file).
* NorTaxa IDs are never reinterpreted as Sporely IDs.
* Two rows with identical external_id under different namespaces both survive.
* The two files are semantically distinct sets: authoritative rows carry a
  known namespace and are joinable by `(source_system, namespace, external_id)`;
  legacy rows are only joinable by `(source_system, external_id)` and require
  external knowledge to determine namespace.

### `taxon_redlist.jsonl` (source: `taxon_redlist_min`)

Resolved rows only (`taxon_id IS NOT NULL AND taxon_id ∈ S`):

```
taxon_id
source_system                                 # e.g. "artsdatabanken_redlist"
source_release
assessment_id
assessment_area                               # "Norge" or "Svalbard" — never collapsed
assessed_name_source                          # e.g. "artsdatabanken"
assessed_name_namespace                       # e.g. "artsnavnebase_scientific_name_id"
assessed_name_id                              # verbatim name-id text
scientific_name_snapshot                      # verbatim at assessment time; never rewritten
authorship_snapshot | null
taxon_rank_snapshot | null
category_raw | category_code | category_is_downgraded (bool)
criteria | expert_group | assessment_url
```

Rules:

* No conflict resolution or one-of-many selection.
* Norway and Svalbard rows remain separate.
* Red List presence never establishes identity.
* Snapshots are preserved as recorded by the compiler.

Sort: `assessment_area, taxon_id, source_release, assessment_id, assessed_name_namespace, assessed_name_id`.

### `taxonomy_release.jsonl`

Exactly one canonical JSON object, LF-terminated. Contains only deterministic
release metadata:

```
content_release_id, taxonomy_schema_version, canonical_authority,
checklistbank_dataset_id, doi, nortaxa_release,
sqlite_sha256, gz_sha256, compiler_manifest_sha256, registry_sha256,
compiler_state, compiler_publication,
source_release_{col_xr,nortaxa,redlist}_{id,sha256},
policy_hashes,
scope_predicate_id, export_schema_version, exporter_version
```

Non-deterministic operational metadata (`generated_at`) lives ONLY in the outer
export manifest, never in `taxonomy_release.jsonl`.

### `taxonomy_export_manifest.json`

Canonical JSON with sorted keys and one trailing LF. Contains:

```
manifest_schema_version, export_schema_version, exporter_version
content_release_id, taxonomy_schema_version, scope_predicate_id
source.{artifact_gz_path, manifest_path, gz_sha256, sqlite_sha256}
policy_hashes
included_concept_count, excluded_concept_count, fungi_root_ids
vernacular_language_counts, redlist_area_counts,
external_id_namespace_counts, external_id_source_table_counts
files[]: [{name, row_count, bytes, sha256, sort_keys}, ...]  # fixed order:
                                                              # 1 taxonomy_release
                                                              # 2 taxon
                                                              # 3 scientific_name
                                                              # 4 vernacular
                                                              # 5 taxon_external_id
                                                              # 6 taxon_redlist
whole_export_sha256
generated_at        # only non-deterministic field
```

## Canonical JSONL rules

Every data file satisfies:

* UTF-8, no BOM;
* LF line endings only;
* one JSON object per line;
* final line terminated by LF;
* object keys sorted lexicographically (recursively);
* compact separators `,` and `:` (no whitespace);
* JSON `null` for SQL NULL; empty text as `""` (distinct from `null`);
* integers emitted as JSON integers unless the normalized schema requires text;
* booleans emitted as JSON booleans (SQLite `0`/`1` → `false`/`true`);
* no `NaN`, `Infinity`, `-Infinity`;
* no trailing whitespace.

Serializer (equivalent):

```python
json.dumps(value, ensure_ascii=False, sort_keys=True,
           separators=(",", ":"), allow_nan=False)
```

## Hash definitions

### Per-file

`SHA-256` over the exact uncompressed file bytes. Recorded as lowercase hex.

### Whole-export hash

Length-prefixed concatenation over the six data files in fixed manifest order:

```
for each file in DATASET_FILES order:
    <ascii decimal len(name)> ':' <name UTF-8 bytes> ':'
    <ascii decimal len(bytes)> ':' <raw file bytes> '\n'
```

Then `SHA-256` over the resulting byte stream. The outer manifest is excluded
from this hash to avoid self-reference. Filename identity is bound into the
hash so swapping file names invalidates the whole-export hash.

### Compression

Not enabled in W1. Reserved: `--no-compress` flag documents intent; adding
compression later requires deterministic gzip (`mtime=0`) plus separate hash
records for uncompressed vs compressed bytes.

## Determinism

Two clean runs of the same version against the same source produce:

* byte-identical data files;
* identical per-file `SHA-256`;
* identical `whole_export_sha256`;
* identical `manifest_sha256` when the same `generated_at` is supplied.

## Atomic generation

1. build in a sibling staging directory under the final parent;
2. write every dataset;
3. compute + validate row counts and hashes;
4. write the final manifest;
5. re-verify each file's on-disk hash;
6. atomic rename into the final location;
7. on any failure: remove the staging directory; leave existing outputs untouched.

Rules:

* an existing byte-identical output is a no-op (reports “already valid”);
* replacing a differing output requires `--replace`;
* symlink and path-traversal hazards on the output parent are rejected.

## Validation invariants

Before publishing the final directory the exporter verifies:

* outer manifest parses; required keys present;
* `taxonomy_schema_version == 2`;
* `content_release_id` matches `^tax-YYYY.MM.DD-NN$`;
* gzip artifact SHA-256 matches manifest;
* decompressed SQLite SHA-256 matches manifest;
* SQLite `taxonomy_meta.taxonomy_schema_version == 2`;
* SQLite `taxonomy_meta.content_release_id` matches outer manifest;
* all required tables and columns exist;
* Fungi kingdom root(s) discovered;
* every dependent child table row satisfies the schema `taxon_id NOT NULL`
  invariant and has a matching `taxon_min` row;
* concepts whose `parent_taxon_id` lies outside scope are counted, sampled
  and recorded in the manifest under `dangling_parent_references`. The
  parent value is preserved verbatim in `taxon.jsonl`; W1 never nulls a
  dangling parent;
* after every dataset is written, a **post-emission reference validator**
  streams `scientific_name.jsonl`, `vernacular.jsonl`,
  `taxon_external_id.jsonl`, `taxon_external_id_legacy_integer.jsonl`, and
  `taxon_redlist.jsonl` line-by-line, requires a `taxon_id` field on every
  row, rejects null/boolean/string values, and confirms the integer is a
  member of the exported concept set;
* on a byte-identical rerun, the existing manifest is fully validated
  (file list matches `DATASET_FILES`; every recorded row count, byte
  count, and SHA-256 matches the on-disk files; `whole_export_sha256`
  matches; dangling-parent block matches; every deterministic manifest
  field matches the fresh staged manifest except `generated_at`). The
  original `generated_at` is preserved. Stale or forged manifests fail
  validation and force `--replace`;
* `--verify-only` runs the source/schema checks, builds the concept
  scope, and reports the real dangling-parent audit (never a synthetic
  all-zero report);
* row counts match pinned-release expectations for `tax-2026.07.30-02`;
* per-file SHA-256 re-verifies after write;
* deterministic re-runs produce identical hashes.

For the pinned release the exporter refuses to publish if any of these fails.

## W1 vs W2 responsibilities

| Concern                                     | Stage |
|---|---|
| Read compiler artifact + verify hashes      | W1 |
| Scope predicate + row selection             | W1 |
| Canonical JSONL emission                    | W1 |
| Per-file + whole-export hashes              | W1 |
| Atomic output + manifest                    | W1 |
| Supabase schema, tables, RLS, RPCs          | W2 |
| `search_taxa_v2` implementation             | W2 |
| Import mechanics (COPY / batched INSERT)    | W2 |
| Model choice (A / B / C)                    | W2 |
| Observation identity columns + backfill     | W3 |
| Web UX + service copy                       | W4 |
| Validation and cutover                      | W5 |

## Current-release measurements

Pinned release: `tax-2026.07.30-02`.

| Dataset | Rows |
|---|---:|
| `taxon.jsonl` | 634,894 |
| `scientific_name.jsonl` | 662,649 |
| `vernacular.jsonl` | 10,294  (nb 6,240; nn 3,975; se 79) |
| `taxon_external_id.jsonl` (authoritative) | 634,894 (`col_xr/col_usage_id` 620,975 + derived `nortaxa/nortaxa_taxon_id` 13,919) |
| `taxon_external_id_legacy_integer.jsonl` | 61,583 (all `artsdatabanken` source; namespace lost) |
| `taxon_redlist.jsonl` | 7,866  (Norge 7,198; Svalbard 668) |
| `taxonomy_release.jsonl` | 1 |

Concepts included: 634,894 / total 634,895 (excluded: 1 domain-rank col_xr row).

Exact per-file uncompressed byte counts and SHA-256 values are recorded in
`taxonomy_export_manifest.json` and are W1 acceptance outputs, not fabricated
here.

## Reproducing

```bash
python -m database.taxonomy.scripts.export_cloud_taxonomy \
    --artifact database/reference_data/generated/taxonomy_v2/tax-2026.07.30-02.sqlite3.gz \
    --manifest database/reference_data/generated/taxonomy_v2/manifest.json \
    --output   database/reference_data/generated/taxonomy_v2/cloud_export_tax-2026.07.30-02
```

Add `--replace` when overwriting a differing directory. Use `--verify-only` to
check source hashes and scope without writing.
