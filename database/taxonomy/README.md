# Taxonomy v2 — technical README

This directory owns the taxonomy-v2 build: source acquisition records,
policies, the stable-ID registry, the compiler scripts, and their tests. The
non-technical explanation of sources and releases is
[docs/taxonomy-database.md](../../docs/taxonomy-database.md).

`TAXONOMY_SCHEMA_VERSION` is `2`. Content releases are `tax-YYYY.MM.DD-NN` and
are immutable once promoted ([release-versioning](docs/release-versioning.md)).
The stage-by-stage acquisition journal that used to be in this file is in Git
history (this file at `8cc688f`) and under `evidence/`.

## Layout

| Path | Contents |
|---|---|
| `sources/<source>/<version>/` | Acquisition records per source release: `request.json`, `manifest.json`, approvals, checksums, validation. Raw archives (`archive.zip`) are gitignored and stay on the build machine. |
| `national_sources/` | National source profiles for the generic Darwin Core Archive normalizer ([kit README](national_sources/README.md)), plus the red-list workbook. |
| `policies/` | Machine-readable policy: languages, source priority, scope, manual mappings, concept supersessions, release contract and thresholds. Validate with `validate_policies.py`. |
| `registry/canonical/` | Append-only allocator of `sporely_taxon_id`, as ordered shards ([registry README](registry/README.md)). |
| `scripts/` | Normalizers, compiler, SQLite builder, bundle promotion, acquisition tools. |
| `cloud_export.py`, `macrofungi_scope.py` | Cloud export and the global-macrofungi projection used by Supabase. |
| `desktop-compatibility.json` | Release the desktop app was tested with, and its bundled hash. |
| `docs/` | Contracts (see [Documents](#documents)). |
| `evidence/` | Committed build and reconciliation evidence. |
| `reconciliation/` | Historical observation reconciliation tooling (W2D/W3). |
| `tests/` | Offline tests for all of the above. |

The shipped bundle lives outside this directory, in
`database/reference_data/generated/taxonomy_v2/` (`manifest.json` plus
`<release>.sqlite3.gz`).

## Pipeline

```
COL XR archive ──► normalize_col_xr.py ─────┐
NorTaxa DwC-A ───► national_source.py ──────┼─► compile_release.py ─► build_sqlite_candidate.py ─► <release>.sqlite3
Red-list xlsx ───► normalize_redlist_no.py ─┘        ▲   (registry)                                   │
legacy DB ───────► export_legacy_enrichment.py ──────┘ optional                                       │
                                                                                                      ├─► promote_desktop_bundle.py ─► desktop bundle
                                              cloud_export.run_export ─► macrofungi_scope.py ─────────┴─► sporely-web importer ─► Supabase
```

The compiler never matches by name. Source identifiers are namespaced text;
the registry maps `(source, namespace, identifier)` to one Sporely ID
([identity contract](docs/identity-contract.md)).

## Building a release

What goes into a release is declared in
[`release-recipe.json`](release-recipe.json): the source releases (each with
its normalizer), the red-list workbook, legacy enrichment, the policies and
the registry. Updating a source, or adding one, is a recipe change reviewed
like code. Then, from the repository root:

```bash
.venv/bin/python database/taxonomy/scripts/build_release.py \
    --release-id tax-YYYY.MM.DD-NN --build-dir /path/to/new/build-dir
```

`build_release.py` checks every input against its pinned SHA-256, normalizes
the sources, compiles and builds the SQLite candidate twice from separate
registry copies, and compares the results. It writes `build-report.json` to
the build directory: input hashes, determinism result, registry state, and a
coverage summary (taxa, vernacular languages, external-ID sources) with its
difference from the bundled release. A full build takes about seven minutes.
It changes nothing in the repository until you add `--promote`, which also
refuses a non-deterministic build or one that allocated new IDs. Promotion
writes the deterministic gzip and manifest, removes the superseded gzip and
updates `desktop-compatibility.json`. Use `--archive-root` when the
gitignored archives live in another checkout.

Rules for a release candidate:

- **Determinism.** Every compile output, the post-compile registry and the
  SQLite hash must be identical across the two builds (`build_release.py`
  checks this). `evidence/taxonomy-v2-closeout/stage3-candidate-verification.md`
  is the record for `tax-2026.09.23-01`, which predates the script.
- **Registry.** Promotion refuses an artifact whose `registry_sha256` differs
  from `registry/canonical/manifest.json`. If the compiler allocated new IDs,
  re-shard the post-compile registry with `identity_registry.shard_registry()`
  and commit it with the release.
- **Cloud-export pin.** Add the release's expected counts to
  `PINNED_RELEASE_EXPECTATIONS` in `cloud_export.py`, keyed on its
  `sqlite_sha256`.
- **Tests.** `pytest -q database/taxonomy/tests tests/test_taxonomy_v2_bundle.py
  tests/test_live_provider_identity_resolution.py`.

## Publishing to the cloud

The cloud copy is built from the committed bundle, not from the build
directory. The exact commands, local proof, operator activation and rollback
are in `sporely-web`:
`supabase/taxonomy-v2-production-import-runbook.md`. In short:

1. `cloud_export.run_export(...)` on the bundle gzip, then `macrofungi_scope.py`
   with `policies/global-macrofungi-scope.yml`.
2. `node scripts/taxonomy-v2/prepare-production-release-import.mjs
   --release-id $REL --release-dir <scoped dir> --output <file>.sql` in
   `sporely-web`.
3. Prove it on the local Supabase stack, then a human operator runs the
   generated SQL. It loads, validates and activates the release in one
   transaction and retires the previous one, which stays available for
   rollback.

Activate the cloud release before publishing a desktop version that ships the
same release.

## Desktop runtime

`utils/taxonomy_v2.py` installs the bundled gzip into
`<app data>/taxonomy_v2/vernacular_multilanguage_v2.sqlite3`, verifies it
against the manifest, and writes an install receipt so later starts skip the
full hash. Activation is ON by default; `SPORELY_TAXONOMY_V2=0` or the
`taxonomy_v2_activation` setting turns it off, and any install failure falls
back to the legacy database `database/reference_data/generated/vernacular_multilanguage.sqlite3`.
`SPORELY_TAXONOMY_V2_VERIFY=1` forces a full re-verify.

Lookups go through `utils/vernacular_utils.resolve_vernacular_db_path()` and
`database/taxon_lookup.py`. Provider identities (Artsorakel/NorTaxa) are
resolved at runtime through `taxon_external_id_text_min`
(`TaxonLookupService.resolve_external_identity`).

## Legacy enrichment

`tax-2026.07.30-02` and `tax-2026.09.23-01` (desktop 0.9.23) were compiled
without legacy enrichment. They carry only `nb`, `nn` and `se` vernacular
names and no Artportalen or iNaturalist IDs, so with v2 active the desktop app
has no Swedish/English/other vernacular names and
`ObservationDB.resolve_external_taxon_id(..., "artportalen")` returns `None`.

`release-recipe.json` now enables it. `export_legacy_enrichment.py` exports
the legacy database's vernaculars in languages NorTaxa does not publish
(including `sv`) and its Artportalen and iNaturalist IDs. The compiler
attaches each row to the Sporely concept of its NorTaxa ID and never
allocates. The SQLite builder also fills `taxon_min.inaturalist_taxon_id`
where a concept has exactly one iNaturalist ID. A scratch build
(`tax-2026.09.26-01`, not promoted) adds 4,801 Swedish names, the other
iNaturalist languages, 8,185 Artportalen IDs and 8,178 iNaturalist IDs, and
leaves everything else unchanged.

Still not carried: legacy rows keyed to Artportalen-only concepts (negative
legacy IDs, for example *Amanita muscaria* s.lat./s.str.), about 1,450
fungal Artportalen IDs and 400 Swedish names. They have no NorTaxa ID and
need a reviewed mapping rule. Rows outside the fungal scope (plants,
animals) are skipped by design. See `releaseA/legacy_enrichment_skips.jsonl`
in a build directory.

Do not delete the legacy database or its build scripts
(`database/README.md`) until a release carries this data, or a national
Swedish source replaces it.

## Adding a national source

The normalizer is generic; the compiler still has NorTaxa-specific wiring.
Checklist:

1. **Acquisition record.** Create `sources/<code>/<version>/` with
   `request.json` and `manifest.json` (archive SHA-256, byte size, licence,
   issued date), following `sources/nortaxa/1.284/`. Keep the archive
   gitignored.
2. **Profile.** `national_source.py init <code>`, then `inspect`, `validate`
   and `normalize` (see the [kit README](national_sources/README.md)). Declare
   `identifier_namespace_semantics` as the NorTaxa profile does.
3. **Compiler registration.**
   - `scripts/compile_release.py`: `SOURCE_PRIORITY`, and a bridge entry
     beside `_ARTSNAVNEBASE_BRIDGE` if the source's IDs are an authoritative
     registry.
   - `scripts/build_sqlite_candidate.py`: `SOURCE_SYSTEM_MAP` and
     `INTEGER_NAMESPACES`/`TEXT_NAMESPACES`.
   - `policies/source_priority.yml` and `policies/languages.yml`.
4. **Cloud.** `cloud_export.py` scope and pins; `sporely-web`'s
   `resolve_taxon_external_id_v2` callers and picker for the new namespace.
5. **Review.** Report unmatched and ambiguous records. Resolve them only
   through `policies/manual_mappings.yml`, never by name matching.
6. **Tests.** A small synthetic fixture next to `national_sources/example/`,
   plus compiler and bridge-emission tests for the new namespace.

## Documents

Current contracts:

- [identity-contract.md](docs/identity-contract.md): Sporely IDs, namespaces, bridges.
- [compatibility-contract.md](docs/compatibility-contract.md): desktop/cloud compatibility.
- [release-versioning.md](docs/release-versioning.md): release, schema and source versions.
- [cloud-export-contract.md](docs/cloud-export-contract.md): cloud export format.
- [redlist-overlay.md](docs/redlist-overlay.md): red-list attachment rules.
- [observation-identification.md](docs/observation-identification.md): identification history.
- [ui-menu-recommendation.md](docs/ui-menu-recommendation.md): deferred Help → Taxonomy menu.

Historical (completed W2D/W3A reconciliation; cited by code and evidence):
`w2d-input-snapshot-contract.md`, `w2d-reconciliation-contract.md`,
`w2d-source-recovery-runbook.md`, `w3a-compatibility-audit.md`,
`w3a-operator-runbook.md`, `w3a-rollback-plan.md`.

Architecture decisions: `docs/architecture/decisions/0001`, `0002`, `0005`.

## Validation

```bash
.venv/bin/python database/taxonomy/validate_policies.py
.venv/bin/pytest -q database/taxonomy/tests
```
