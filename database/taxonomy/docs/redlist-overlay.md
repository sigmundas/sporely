# Norwegian Red List 2021 assessment overlay (Stage 3B.4)

## What this is

An additive, versioned **assessment overlay** on the taxonomy-v2 SQLite —
never taxonomy identity, never a column on `taxon_min`. The 2021 Norwegian
Red List is Artsdatabanken's official conservation assessment; Sporely
carries it read-only, at a specific dataset version, and resolves each
assessment to a `sporely_taxon_id` only where an explicit source-identifier
bridge exists. Unresolved assessments remain in the artifact with
`taxon_id IS NULL`.

## Provenance

- **Source**: Artsdatabanken (2021). *Norsk rødliste for arter 2021.*
- **Download**: <https://lister.artsdatabanken.no/rodlisteforarter/2021/>
  (manual, one XLSX export).
- **Local path** (gitignored, 10 MB binary):
  `database/taxonomy/national_sources/artsdatabanken_redlist/2021/redlist-2021.xlsx`
- **Workbook SHA-256**:
  `c2fb6a5f4828defe371b1cb9978974bfd64a23101ad37bb715f260bd658bef04`
- **Sheet consumed**: `Vurderinger` (34,171 rows).

## Identifier namespaces

Two distinct external identifiers are preserved:

| Purpose | source_system | namespace | Example |
|---|---|---|---|
| Assessment row identity | `artsdatabanken_redlist` | `redlist_assessment_id` | `15090` |
| Assessed name (Artsnavnebase) | `artsdatabanken` | `artsnavnebase_scientific_name_id` | `52147` |

These are stored **verbatim** on every normalized row. The workbook's
`Vitenskapelig navn id` is Artsdatabanken's Artsnavnebase scientific-name
id — the same registry that NorTaxa's DwC-A archive publishes as its
`taxonID` column, with the `NBIC:` provenance prefix declared in
`national_sources/nortaxa/1.284/source.json`.

## Identity resolution — the ONLY rule

At compile time, each assessment's `assessed_name_id` (numeric string) is
matched **exactly** against the `artsnavnebase_scientific_name_id`
semantic namespace. Today, exactly one carrier populates that namespace:
NorTaxa's DwC `taxonID` column, per the machine-readable
`identifier_namespace_semantics` block declared in
`national_sources/nortaxa/<release>/source.json`. Both NorTaxa anchors and
NorTaxa aliases (synonyms folded onto their accepted Sporely id) qualify.

Critical distinction — Artsdatabanken maintains **two** independent
registries:

- `artsnavnebase_scientific_name_id` — the name registry. Every distinct
  scientific name + authorship has one. This is what the workbook column
  "Vitenskapelig navn id" carries.
- `artsdatabanken_taxon_concept_id` — the taxon-concept registry. Distinct
  values for the same name (e.g. *Vulpes vulpes* has name-id `48034` and
  concept-id `31176`; *Cladonia chlorophaea* has name-id `69071` and
  concept-id `45044`).

**A concept-id must never be bridged as a name-id.** Numeric equality
between the two registries is coincidence and treating it as identity
would silently misidentify unrelated taxa. Regression tests in
`test_stage_3b_redlist.py` pin this invariant.

Never inferred:

- No scientific-name equality lookup.
- No fuzzy matching.
- No cross-namespace numeric coincidence (e.g. a matching COL usage id is
  ignored — it belongs to a different registry).
- No allocation of a Sporely id for red-list content. The
  `IdentityRegistry` is untouched. A regression test asserts that
  compiling with and without `--redlist` produces bit-identical registry
  files.

Unresolved rows are preserved with `taxon_id = NULL` (26,245 of 34,171 in
the 2021 build; almost all are non-fungal taxa — vascular plants, insects,
crustaceans — that Sporely's fungal scope excludes).

## Category normalization

The 2021 red list uses these nine categories; the workbook is rejected if
any other value appears:

```
RE  CR  EN  VU  NT  DD  LC  NA  NE
```

A trailing degree sign (`°`) on a category indicates a
"nedgradering" — the assessor lowered the category based on
rescue-effect considerations in neighbouring populations. Both forms are
preserved:

- `category_raw` — verbatim workbook value, e.g. `VU°`.
- `category_code` — degree-stripped, e.g. `VU`.
- `category_is_downgraded` — boolean.

Downgraded assessments must never be silently normalized to the plain
category and must never be inferred from taxonomy alone.

## Norway ≠ Svalbard

The workbook assesses two areas independently. They are stored as
**separate rows**, keyed by `assessment_area IN ('Norge','Svalbard')`, and
the runtime query never merges them. `get_redlist_assessment(...)` requires
`area=` explicitly (default `Norge`); passing an unknown area returns
`None` rather than silently defaulting.

## Pipeline

```
redlist-2021.xlsx  (manual download, SHA-pinned)
  │
  ▼  normalize_redlist_no.py --input <xlsx> --output build/redlist_no
build/redlist_no/
  ├── assessments.jsonl       (34,171 canonical rows, deterministic order)
  └── report.json             (workbook SHA-256, sheet, header map, counts)
  │
  ▼  compile_release.py --redlist build/redlist_no ... (all other flags unchanged)
release_dir/
  ├── redlist_no.jsonl                (resolution attempted; taxon_id or null)
  └── redlist_no_diagnostics.json     (resolved/unresolved/ambiguous counts,
                                       first 25 unresolved samples,
                                       first 25 ambiguous samples)
  │
  ▼  build_sqlite_candidate.py --release-dir ... --output candidate.sqlite3
taxon_redlist_min table populated.
```

### Commands (real)

```bash
./.venv/bin/python database/taxonomy/scripts/normalize_redlist_no.py \
  --input database/taxonomy/national_sources/artsdatabanken_redlist/2021/redlist-2021.xlsx \
  --output build/redlist_no

./.venv/bin/python database/taxonomy/scripts/compile_release.py \
  --source build/col_xr_norm \
  --source build/nortaxa_norm \
  --manual-mappings database/taxonomy/policies/manual_mappings.yml \
  --mapping-policy database/taxonomy/policies/mapping_policy.yml \
  --registry <single-file registry.jsonl> \
  --output build/release \
  --release-id tax-YYYY.MM.DD-NN \
  --redlist build/redlist_no

./.venv/bin/python database/taxonomy/scripts/build_sqlite_candidate.py \
  --release-dir build/release \
  --registry <single-file registry.jsonl> \
  --output build/candidate.sqlite3
```

## SQLite contract

```sql
CREATE TABLE taxon_redlist_min (
  redlist_row_id            INTEGER PRIMARY KEY,
  taxon_id                  INTEGER,             -- NULL when unresolved
  source_system             TEXT NOT NULL,       -- 'artsdatabanken_redlist'
  source_release            TEXT NOT NULL,       -- '2021'
  assessment_id             TEXT NOT NULL,
  assessment_area           TEXT NOT NULL,       -- 'Norge' | 'Svalbard'
  assessed_name_source      TEXT NOT NULL,       -- 'artsdatabanken'
  assessed_name_namespace   TEXT NOT NULL,       -- 'artsnavnebase_scientific_name_id'
  assessed_name_id          TEXT NOT NULL,
  scientific_name_snapshot  TEXT NOT NULL,
  authorship_snapshot       TEXT,
  taxon_rank_snapshot       TEXT,                -- whitelist: species|subspecies|variety|form|genus|aggregate else NULL
  category_raw              TEXT NOT NULL,       -- may include '°'
  category_code             TEXT NOT NULL,       -- 2021 whitelist only
  category_is_downgraded    INTEGER NOT NULL DEFAULT 0,
  criteria                  TEXT,
  expert_group              TEXT,
  assessment_url            TEXT,
  FOREIGN KEY (taxon_id) REFERENCES taxon_min(taxon_id)
);

-- Duplicate-guard indexes (compile-time invariants).
CREATE UNIQUE INDEX idx_redlist_assessment_id
  ON taxon_redlist_min(source_system, source_release, assessment_id);
CREATE UNIQUE INDEX idx_redlist_name_area
  ON taxon_redlist_min(source_system, source_release,
                       assessed_name_namespace, assessed_name_id,
                       assessment_area);

-- Runtime lookup index (used by TaxonLookupService.get_redlist_assessment).
CREATE INDEX idx_redlist_taxon_area_release
  ON taxon_redlist_min(taxon_id, assessment_area, source_release);
```

`PRAGMA foreign_key_check` passes for rows with `taxon_id IS NOT NULL`;
unresolved rows are permitted (FK not enforced when child value is NULL).

## Runtime lookup

```python
from database.taxon_lookup import TaxonLookupService, RedlistAssessment

svc.get_redlist_assessment(sporely_taxon_id,
                           area="Norge",       # default; 'Svalbard' also valid
                           source_release="2021")
```

Returns a `RedlistAssessment` or `None`. Contract:

- Absent `taxon_redlist_min` table (legacy DB active) → `None`.
- No assessment for `(taxon, area, release)` → `None`.
- Unknown area → `None` (no silent default to `Norge`).

The runtime API is **read-only** and does **not** write red-list state
into an observation. All existing observation identity-invalidation rules
remain unchanged:

- Genus / species / scientific-name identity changes clear the observation's
  red-list snapshot fields.
- An explicit new taxonomy selection clears old red-list snapshot fields
  before resolving new ones.
- Common-name-only edits never touch red-list fields.
- Load-time restoration under `TaxonInputController._suspended()` cannot
  clear stored values.

## Versioning conclusion

Adding the red-list overlay does **not** bump `TAXONOMY_SCHEMA_VERSION`.
Rationale: the change is a new additive SQLite table that is queried
behind a `_has_local_table("taxon_redlist_min")` guard. The desktop
compatibility contract declares `supported_taxonomy_schema_min = max = 2`
and legacy DBs (which lack the table) are handled by returning `None`,
matching the "no assessment" contract.

It **does** produce a new content release: `tax-2026.07.29-02`.

## Source-row coherence validation

After a workbook `Vitenskapelig navn id` finds a candidate Sporely id
through the Artsnavnebase bridge, the compiler additionally requires that
the source row (NorTaxa) publishes a scientific name that canonically
agrees with the workbook's `scientific_name_snapshot`. Canonicalization
is conservative — case-fold, whitespace collapse, and parenthetical
subgenus notation stripped (`Bufo (Bufo) bufo` → `Bufo bufo`). No
stemming, no authorship substitution.

If the name-id resolves but the names disagree — for example, the
workbook says `Chroogomphus rutilus` while NorTaxa 56055 publishes
`Chroogomphus rutilus coll.` (the species aggregate, a broader concept) —
the assessment is **not** bound to any Sporely id and remains in the
output with `taxon_id = NULL` and `unresolved_reason =
unresolved_name_id_name_mismatch`, preserving both scientific names in
the diagnostics. The compiler never falls back to name matching. The
mismatch is a signal for later curation, not for silent auto-binding.

## Collision audit

After resolution, the compiler groups resolved rows by
`(source_release, assessment_area, sporely_taxon_id)` and reports:

- unique — one assessment per group;
- multiple rows with identical category — the same Sporely concept
  received two or more Artsnavnebase name-ids in the same edition that
  all agree on the category;
- conflicting categories — same Sporely id, same area, different
  categories;
- conflicting ranks — same Sporely id, same area, different ranks.

Conflicting groups are never auto-resolved. The audit lists up to 50
conflict groups in `redlist_no_diagnostics.json.collisions.conflicts` for
manual review.

## 2026-07-30 real build stats (tax-2026.07.30-02)

Rebuilt on 2026-07-30 with the Stage 3B.4 audit follow-up: coherence
check + collision audit added; namespace remains
`artsnavnebase_scientific_name_id`. Compared to `tax-2026.07.30-01`, the
resolved count DROPS by 60 rows that were previously misresolved onto a
Sporely id whose NorTaxa scientific name disagreed with the workbook.
None of those 60 was a correct binding; all are legitimate unresolved
mismatches (46 NorTaxa species-aggregate `coll.` vs strict-species
workbook rows, 14 spelling variants or `nom. dub.` flags).

- Workbook: `redlist-2021.xlsx`, 10,890,135 bytes,
  SHA-256 `c2fb6a5f4828defe371b1cb9978974bfd64a23101ad37bb715f260bd658bef04`.
- Assessments: **34,171** total — 33,047 Norge, 1,124 Svalbard.
- Category counts: `RE 112, CR 309, EN 1001, VU 1570, NT 1446, DD 741, LC 19134, NA 3412, NE 6446`.
- Downgraded (`°`) rows: **63**.
- Resolved / unresolved: **7,866 / 26,305** (was 7,926 / 26,245 in
  `tax-2026.07.30-01`; the 60-row shift is entirely the coherence check).
- Unresolved-reason breakdown:
    - `unresolved_name_id_not_found`: 26,245 (non-fungal taxa outside
      Sporely's fungal scope).
    - `unresolved_name_id_name_mismatch`: 60 (coherence check).
    - `unresolved_name_id_ambiguous`: 0.
- Fungal (Sopper) resolution: **5,149 / 5,256 = 97.9%** (was 99.1% —
  the drop is the honest count now that ambiguous `coll.` aggregates
  are no longer silently bound).
- Duplicate assessment IDs or `(name-id, area)` keys: **0**.
- Collision audit (resolved rows grouped by
  `(source_release, area, sporely_taxon_id)`):
    - `unique`: 7,797.
    - `multiple_rows_same_category`: 12.
    - `conflicting_categories`: 22 (e.g. Sporely 625409 in Norge is
      claimed by workbook name-ids 54079 with category `LC` and 54086
      with `NE`).
    - `conflicting_ranks`: 0.
    - No auto-selection; conflicts are listed in
      `redlist_no_diagnostics.json.collisions.conflicts` for manual
      review.
- Canonical identity registry: **byte-identical**
  (`21b5d39d257799fdd4ea758d857fec4d29758b83dd7ea91bf3ecd24e3d1d3077`).
- SQLite candidate: 319,709,184 bytes,
  SHA-256 `993d1608df1cb0ae93aab7b35a889b29858f0add720b933f3c58ed1e8485b94f`.
- Gzip candidate: 65,481,446 bytes,
  SHA-256 `fb7660c613d0909c22591abe90768a9ae3c0ea88a8b8d5b2ee2bdf6c69cb8938`.

### 2026-07-30 audit outcome

- **Vulpes vulpes** (workbook name-id `48034`, area Norge): unresolved
  with reason `unresolved_name_id_not_found`. NorTaxa 1.284 does not
  publish `48034` (mammals lie outside its scope). The unrelated NorTaxa
  row with `taxonID=31176` (Chironomus pallens) is never considered — the
  bridge is by exact name-id, not by concept-id.
- **Cladonia chlorophaea** (workbook name-id `69071`, areas Norge +
  Svalbard): resolves to Sporely id `628859` via NorTaxa row `69071`
  (`Cladonia chlorophaea (Sommerf.) Sprengel`, valid, Fungi). Names
  agree; coherence check passes. NorTaxa row `45044` is
  `Acrolepiopsis betulella` — never bridged onto Cladonia (regression
  test enforces this; a synthetic workbook row carrying `45044` under a
  Cladonia scientific-name string stays unresolved with reason
  `unresolved_name_id_name_mismatch` and preserves both names).

## Excluded from Stage 3B.4

- Automatic web downloading; scheduled updates.
- Supabase publication and any cloud schema change.
- Web / landing-app integration.
- Observation UI redesign; automatic write of reference red-list into
  observations.
- Fuzzy name matching; name-only identity binding.
- Generalized multi-country conservation-status schema.
- Historical Norwegian red-list releases other than 2021 (schema *can*
  hold future releases; none are imported today).
- New identity-registry entries for assessments.
