# Reference Library and Public Reference Plotting Plan

**Status:** Stage 1 (local schema foundation) implemented; Stages 2–6 still proposed.
**Canonical repository:** `sporely-py`
**Canonical path:** `docs/plans/active/2026-08-05-reference-library-and-public-plots.md`
**Scope:** `sporely-py` → `sporely-web`/Supabase → `sporely-landing`
**Primary outcome:** A reference entered once can be reused across observations, and a public observation can display the exact literature measurement sets used in desktop analysis.

---

## Agent handoff

- Status: Active; Stage 1 and a Stage 2 desktop vertical slice are implemented, while substantial Stage 2 work and Stages 3–6 remain.
- Last completed stage: Stage 1; the documented Stage 2 vertical slice and interactive legacy migration also landed.
- Current/next stage: Finish the remaining Stage 2 desktop library/attachment work before cloud/public stages.
- Relevant commits: `108db20`, `6c9c456`, `08249ec`, `22bd29f`, `f05f2e3`, `2a1ebe3`.
- Important decisions: Preserve stable UUIDs, frozen observation snapshots, revision-aware records, and the distinction between literature ranges and raw observations.
- Do not: Fuzzy-merge bibliographic records, fabricate statistics, or begin public catalogue scope without a separate moderation design.
- Remaining acceptance criteria: The cross-repository definition of done in Section 20.

---

## 1. Problem statement

Sporely currently supports stored reference values and plotting them in desktop analysis, but bibliographic identity and observation-level use are not yet modeled as first-class, reusable data.

Typical friction:

- the same publication title is typed repeatedly;
- source labels may be incomplete or inconsistent;
- a publication, a species treatment within that publication, and the measurements extracted from that treatment are conflated;
- the desktop can plot a reference without necessarily preserving a durable record that this specific observation used that specific reference;
- `sporely-landing` cannot reliably reproduce the exact literature overlays used for a published observation.

The change must separate:

1. the bibliographic work;
2. the taxon treatment within the work;
3. the structured measurement set extracted from that treatment;
4. the use of that measurement set for a particular observation.

---

## 2. Goals

### 2.1 Core goals

- Enter a publication once and reuse it.
- Store complete, structured citations instead of only free-text source labels.
- Preserve the source's original taxon name and page/figure locator.
- Preserve the printed measurement expression verbatim.
- Store parsed values for plotting without losing the original text.
- Attach one or more reference measurement sets to an observation.
- Persist the role of each attachment:
  - `compared`
  - `supports_identification`
  - `contradicts`
- Sync selected reference uses to the cloud.
- Return the exact attached references from the public observation API.
- Plot literature ranges on `sporely-landing` without fabricating raw points.
- Preserve historical interpretation if a library record is corrected later.

### 2.2 Usability goals

- Search recent and frequently used works.
- Select *Danmarks basidiesvampe* or another common work without retyping it.
- Reuse an existing species treatment and measurement set.
- Make full citation details available without crowding plots.
- Keep fast entry possible for an incomplete source, but label it as unverified.

### 2.3 Data-quality goals

- Stable UUID identity across local databases and Supabase.
- Explicit revision and provenance.
- No silent overwriting of a source's printed data.
- No conflation of a taxon's current accepted name with the name used by the source.
- No invented means, percentiles, sample sizes, or point clouds.

---

## 3. Non-goals

This project does not initially:

- create a general-purpose Zotero replacement;
- ingest entire books or copyrighted text;
- infer measurements from scans automatically;
- make all personal references globally public;
- rewrite the existing plotting system wholesale;
- make literature ranges statistically equivalent to raw measured spores;
- solve every reference-data format in the first release;
- add a public `/references` library before observation-level use is proven end to end.

---

## 4. Current-state assumptions to verify

The first implementation stage must verify these assumptions against the current tree and update this section if needed:

- desktop reference values live in the separate `reference_values.db`;
- the legacy `reference_values` table stores taxon, source label, method fields, and summary/range values;
- the Reference Values dialog can browse, edit, save, and plot stored values;
- the add/edit flow can accept min/max summaries, raw spore data, and Parmasto-style biometrics;
- the cloud reference dialog can search observation-derived community datasets and public reference values;
- existing reference plotting may be transient UI state rather than a durable observation-to-reference relationship;
- existing cloud reference rows are not yet a complete bibliographic library contract.

The audit must identify:

- every schema/table used for local references;
- every model/repository method;
- every dialog and analysis call path;
- every cloud RPC/client method related to references;
- whether selected reference rows are currently persisted against observations;
- whether any current public/cloud table already has stable reference UUIDs.

---

## 5. Domain model

## 5.1 Reference work

A bibliographic item entered once.

Suggested entity: `reference_works`

| Field | Notes |
|---|---|
| `id` | UUID, stable across local/cloud |
| `type` | `book`, `article`, `chapter`, `website`, `dataset`, `other` |
| `citation_key` | Human-friendly stable key; optional but unique per owner/library |
| `authors_json` | Ordered structured author list |
| `editors_json` | Ordered structured editor list |
| `title` | Work or chapter title, according to type |
| `container_title` | Journal/book title for article/chapter |
| `year` | Nullable integer |
| `edition` | Nullable text |
| `publisher` | Nullable text |
| `place` | Nullable text |
| `volume` | Nullable text |
| `issue` | Nullable text |
| `pages` | Overall publication pages, not treatment locator |
| `doi` | Normalized DOI, nullable |
| `isbn` | Normalized ISBN, nullable |
| `url` | Nullable |
| `language` | BCP-47 or short code |
| `short_label` | Plot/list label, e.g. `Petersen et al. 1990` |
| `citation_override` | Optional hand-written citation for exceptional cases |
| `owner_id` | Nullable locally; cloud user UUID when personal |
| `created_at` / `updated_at` | Timestamps |
| `revision` | Monotonic integer |

Generated full citations should use structured fields. `citation_override` is an escape hatch, not the default storage model.

> **Removed concepts.** Earlier drafts of the schema included
> `verification_status` (`incomplete` / `unverified` / `verified`) and
> `visibility` (`private` / `shared` / `curated_public`) columns.
> These are no longer part of the product model:
>
> - Reference works do not carry a manually assigned verification status.
> - Reference works do not carry a per-work visibility scope. Public
>   exposure of an attached reference is governed by the observation's
>   own visibility and by its frozen
>   `observation_reference_uses.snapshot_json`.
> - Bibliographic completeness is *derived* from field values at
>   display time (see
>   `ui.reference_library_manager_dialog.reference_work_completeness_hints`)
>   and is a non-blocking hint — it never gates saving or attaching.
> - The two columns remain on the sqlite DDL as compatibility columns
>   only, so old installations continue to load. New INSERTs omit
>   both; new schemas may drop them where doing so does not break
>   existing migration paths.
> - A future public shared reference catalogue, if ever implemented,
>   will have a separate moderation design.

## 5.2 Taxon treatment

A species/taxon entry within a work.

Suggested entity: `reference_taxon_treatments`

| Field | Notes |
|---|---|
| `id` | UUID |
| `reference_work_id` | UUID of work |
| `taxon_id` | Stable Sporely taxon identity when resolvable |
| `name_as_published` | Exact scientific name used by the work |
| `page_from` / `page_to` | Nullable |
| `locator_text` | Free locator such as `p. 214`, `fig. 32`, `plate 8` |
| `treatment_notes` | Notes about concept, synonymy, or interpretation |
| `created_at` / `updated_at` | Timestamps |
| `revision` | Monotonic integer |

Current taxonomy and source taxonomy must remain distinct. A later taxonomic update may change `taxon_id`; it must not rewrite `name_as_published`.

## 5.3 Reference measurement set

A structured set of measurements extracted from one taxon treatment.

Suggested entity: `reference_measurement_sets`

| Field | Notes |
|---|---|
| `id` | UUID |
| `taxon_treatment_id` | UUID |
| `character` | Initially `spore_size`; extensible later |
| `raw_text` | Verbatim printed measurement expression |
| `data_kind` | `range`, `summary`, `raw_points`, `parmasto` |
| `length_min` | Exceptional/absolute minimum, nullable |
| `length_core_min` | Core lower bound, nullable |
| `length_core_max` | Core upper bound, nullable |
| `length_max` | Exceptional/absolute maximum, nullable |
| `width_min` | Nullable |
| `width_core_min` | Nullable |
| `width_core_max` | Nullable |
| `width_max` | Nullable |
| `q_min` / `q_max` / `q_mean` | Nullable |
| `length_mean` / `width_mean` | Nullable |
| `sample_size` | Number of spores if stated |
| `specimen_count` | Number of specimens/collections if stated |
| `mount_medium` | Nullable |
| `stain` | Nullable |
| `preparation` | Nullable |
| `measurement_method` | Nullable |
| `notes` | Interpretation notes |
| `raw_points_json` | Only when genuine individual source points are available |
| `created_at` / `updated_at` | Timestamps |
| `revision` | Monotonic integer |
| `supersedes_id` | Optional prior measurement-set UUID |

All numeric fields are nullable. Absence means “not supplied,” not zero.

## 5.4 Observation reference use

A durable statement that an observation used a particular reference measurement set.

Suggested entity: `observation_reference_uses`

This table belongs in the main observation database (`mushrooms.db`), because observations live there while the reusable library lives in `reference_values.db`.

| Field | Notes |
|---|---|
| `id` | UUID |
| `observation_id` | Local observation primary key |
| `reference_measurement_set_id` | UUID; logical cross-database reference |
| `role` | `compared`, `supports_identification`, `contradicts` |
| `note` | Optional observation-specific note |
| `selected_at` | Timestamp |
| `reference_revision` | Revision used when attached/published |
| `snapshot_json` | Canonical public-safe snapshot |
| `created_at` / `updated_at` | Timestamps |

SQLite cannot enforce a foreign key across the two database files. Integrity must therefore be enforced by repository/service code and tested explicitly.

The snapshot protects historical evidence. A corrected library record may be offered as an update, but must not silently alter an already published comparison.

---

## 6. Canonical snapshot

When a measurement set is attached to an observation, generate a canonical snapshot containing only public-safe scientific and bibliographic fields:

```json
{
  "schema_version": 1,
  "reference_work_id": "uuid",
  "reference_measurement_set_id": "uuid",
  "reference_revision": 3,
  "short_label": "Petersen et al. 1990",
  "full_citation": "...",
  "work_type": "book",
  "year": 1990,
  "doi": null,
  "isbn": "...",
  "taxon_id": "stable-taxon-id",
  "name_as_published": "Russula paludosa",
  "locator_text": "p. 214",
  "character": "spore_size",
  "data_kind": "range",
  "raw_text": "(7.5–)8–10(–10.5) × 5–6(–6.5) µm",
  "measurements": {
    "length_min": 7.5,
    "length_core_min": 8.0,
    "length_core_max": 10.0,
    "length_max": 10.5,
    "width_min": null,
    "width_core_min": 5.0,
    "width_core_max": 6.0,
    "width_max": 6.5,
    "q_min": null,
    "q_max": null,
    "q_mean": null,
    "length_mean": null,
    "width_mean": null,
    "sample_size": null,
    "specimen_count": null
  },
  "method": {
    "mount_medium": null,
    "stain": null,
    "preparation": null,
    "measurement_method": null
  }
}
```

Requirements:

- deterministic key structure;
- schema versioned;
- JSON-serializable without custom objects;
- no local filesystem paths;
- no private owner information;
- no fabricated values;
- generated by one shared service, not duplicated in UI code.

---

## 7. Local database placement

### `reference_values.db`

Add:

- `reference_works`
- `reference_taxon_treatments`
- `reference_measurement_sets`

Keep the legacy `reference_values` table intact during migration.

### `mushrooms.db`

Add:

- `observation_reference_uses`

Do not move observations into the reference database and do not duplicate the full library into the observation database.

---

## 8. Legacy compatibility and migration

The existing `reference_values` table remains readable throughout the transition.

Migration phases:

1. Add new normalized tables without changing existing UI behavior.
2. Add stable UUID support and repository APIs.
3. Build an explicit legacy-to-library migration/import tool.
4. Present a review screen for ambiguous free-text sources.
5. Keep a link such as `legacy_reference_value_id` on migrated measurement sets for traceability.
6. Switch editing and selection UI to the normalized library.
7. Keep legacy reads for at least one release.
8. Remove legacy writes only after import and rollback paths are proven.

Do not auto-deduplicate publications solely by similar titles.

Suggested duplicate checks, in order:

1. normalized DOI;
2. normalized ISBN plus edition;
3. exact normalized title + year + first author;
4. manual confirmation.

---

## 9. Desktop UX

## 9.1 Reference library

Add a desktop library/editor surface with:

- search by title, author, year, DOI, ISBN, short label;
- recent works;
- favourites;
- incomplete/unverified indicator;
- add/edit work;
- taxon treatments nested under work;
- measurement sets nested under treatment;
- duplicate warning before save;
- full citation preview.

The library may initially open from the existing Reference Values dialog.

### 9.1.1 Reference work editor UX (implemented)

The Reference Library work editor is deliberately human-facing rather than
schema-shaped. Its behaviour, as landed in `ui/reference_library_manager_dialog.py`:

- **Basic information** — Type, Title, Authors, Year are always visible.
- **Authors / Editors** — ordered person-list editors with per-row fields for
  *Family*, *Given*, and *Organization*. Users add, remove, reorder, and edit
  rows without seeing JSON. The canonical `authors_json` / `editors_json` shape
  (list of `{family, given, literal}` dicts) is produced on save. Malformed
  existing JSON does **not** crash the dialog and is **not** silently
  discarded: a translated warning is shown and the original raw string is
  preserved verbatim until the user explicitly repairs the value.
- **Publication details** — the section adapts to the selected type:
  - *Article*: Journal / container title, Volume, Issue, Pages.
  - *Book*: Edition, Editors, Publisher, Place.
  - *Chapter or contribution*: Container / book title, Editors, Pages,
    Publisher, Place.
  - *Website*: container/publisher only when useful.
  - *Dataset* and any unknown type: fall back to a general publication section
    showing every field.
  Every widget is instantiated regardless of type — switching type only
  changes visibility and labels. Hidden values are never erased and are still
  collected on save. Every stored field round-trips unchanged when the user
  opens and saves an existing record without editing it.
- **Identifiers** — DOI, ISBN, URL. Blank identifier values are accepted.
- **Advanced citation details** (collapsed by default) — manually overridden
  short label (blank uses the generated value), citation key, language, and
  full citation override. Verification and visibility controls were removed
  with the corresponding concepts (see §5.1); completeness is shown as a
  derived, non-blocking hint underneath the preview instead.
- **Live preview** — driven by the canonical
  `database.reference_citation.build_short_label` /
  `build_full_citation` service. The preview updates as relevant fields
  change and clearly marks when the user has supplied a manual override.
  Missing data produces an honestly incomplete preview; no author, year,
  publisher, page, or identifier is ever fabricated.
- **Human-friendly validation** — title is required; year is blank or a
  whole number; DOI / ISBN / URL accept blank; the first invalid field is
  focused and the dialog stays open on failure. Repository validation
  errors surface via a red error label without closing the dialog.
- **Laptop-sized layout** — the sections sit inside a `QScrollArea` so the
  form fits on a typical laptop screen; Cancel / OK stay pinned. The
  legacy Reference Values dialog is left completely untouched.

## 9.2 Observation analysis

For the active observation:

- search library using current taxon;
- add one or more measurement sets to the plot;
- choose attachment role;
- save attachment to `observation_reference_uses`;
- distinguish “plotted temporarily” from “attached to observation”;
- show attached references when reopening the observation;
- allow detaching without deleting the library record;
- warn when the library has a newer revision than the saved snapshot;
- offer explicit “update this observation to revision N.”

## 9.3 Fast entry

A quick-add flow may accept:

- short source text;
- taxon;
- locator;
- raw measurement expression.

It creates an `incomplete` work and a measurement set, then opens the full editor later. The UI must make incomplete citation status visible.

---

## 10. Plot semantics

Published summary ranges are not raw observations.

For a range such as:

`(7.5–)8–10(–10.5) × 5–6(–6.5) µm`

render:

- measured observation spores as points;
- the source's core L × W range as a translucent rectangle;
- exceptional minima/maxima as an outer outline or whiskers;
- a mean marker only when the source supplies a mean;
- the exact raw expression in the details/card;
- source short label and locator beside or below the plot.

Do not:

- generate synthetic scatter points from ranges;
- infer a normal distribution;
- infer a sample size;
- convert min/max into percentiles;
- merge distinct sources into one range without explicit user action.

Reference raw points may be shown as points only when the source actually provides individual values.

---

## 11. Cloud model (`sporely-web` / Supabase)

After the local model is stable, add corresponding cloud tables:

- `reference_works`
- `reference_taxon_treatments`
- `reference_measurement_sets`
- `observation_reference_uses`

Cloud requirements:

- UUIDs match local identity;
- revision-aware upsert;
- owner-aware RLS;
- public-safe projections;
- curated records are admin-managed initially;
- personal private records are not exposed;
- a public observation may expose the snapshot/citation for references explicitly attached to it;
- deleting a personal library item must not erase a published observation snapshot;
- duplicates are detected, not silently merged.

Public exposure model (post-simplification):

- Reference works no longer carry a per-work visibility scope. All local
  records are treated as owner-private by default.
- A public observation may expose the frozen citation snapshot for
  references explicitly attached to it — public exposure is governed by
  the observation's own visibility, not by any reference-level flag.
- A future public shared reference catalogue, if implemented, will have a
  separate moderation design and does NOT reuse a per-work visibility
  enum on the operator-facing model.

---

## 12. Sync contract (`sporely-py`)

Sync order:

1. reference work;
2. taxon treatment;
3. measurement set;
4. observation reference use.

Each synced row needs:

- stable UUID;
- `updated_at`;
- revision;
- tombstone/deletion semantics;
- conflict policy.

Initial conflict policy:

- bibliographic/measurement edits create a new revision;
- observation attachment edits use last-write-wins only for role/note;
- snapshot changes require explicit update;
- cloud must never replace a newer local revision silently.

---

## 13. Public API contract

The public observation payload should expose only references attached to that observation.

Example:

```json
{
  "references": [
    {
      "use_id": "uuid",
      "role": "supports_identification",
      "note": null,
      "reference_revision": 3,
      "snapshot": {
        "schema_version": 1,
        "short_label": "Petersen et al. 1990",
        "full_citation": "...",
        "name_as_published": "Russula paludosa",
        "locator_text": "p. 214",
        "character": "spore_size",
        "data_kind": "range",
        "raw_text": "(7.5–)8–10(–10.5) × 5–6(–6.5) µm",
        "measurements": {
          "length_min": 7.5,
          "length_core_min": 8.0,
          "length_core_max": 10.0,
          "length_max": 10.5,
          "width_min": null,
          "width_core_min": 5.0,
          "width_core_max": 6.0,
          "width_max": 6.5
        }
      }
    }
  ]
}
```

The public API must not return every reference associated with the species. It returns the references deliberately used for this observation.

A separate future species/library RPC may return curated public reference sets.

---

## 14. `sporely-landing`

### First public surface

Implement on the observation detail page first:

- literature range overlays on the existing L/W plot;
- a “Compared with literature” section;
- source cards with:
  - short label;
  - full citation;
  - locator;
  - name as published;
  - raw measurement text;
  - method fields when supplied;
  - role;
- no reference section when none are attached;
- fail closed if malformed reference data is returned.

### Later surfaces

- species page: curated published ranges;
- Compare: add a literature measurement set;
- `/references`: searchable bibliography and taxon treatments;
- DOI/ISBN links where appropriate;
- citation export formats.

The public library page is deferred until observation-level fidelity is proven.

---

## 15. Staged implementation

## Stage 0 — Audit and contract confirmation

Repository: `sporely-py`

Deliver:

- verified current-state map;
- exact legacy schema;
- exact plot call paths;
- exact persistence behavior;
- exact cloud client methods;
- decision record for unresolved conflicts;
- update this plan where assumptions were wrong.

No behavior changes unless required to add safe characterization tests.

## Stage 1 — Local normalized schema foundation

Repository: `sporely-py`

Deliver:

- normalized tables in `reference_values.db`;
- `observation_reference_uses` in `mushrooms.db`;
- schema versioning/migrations;
- typed/domain repository layer;
- canonical snapshot builder;
- CRUD tests;
- idempotent initialization;
- no UI switch yet;
- no cloud changes;
- legacy table untouched.

## Stage 2 — Desktop library and observation attachment UX

Repository: `sporely-py`

Deliver:

- reusable publication editor;
- taxon treatment editor;
- measurement-set editor using existing parser;
- recent/favourite works;
- attach/detach references to active observation;
- restore attachments when observation reopens;
- plot from attached references;
- explicit snapshot revision update.

## Stage 3 — Cloud schema and public-safe contract

Repository: `sporely-web`

Deliver:

- Supabase migrations;
- RLS;
- revision-aware functions;
- owner CRUD;
- public observation reference projection;
- tests and contract documentation.

## Stage 4 — Desktop cloud sync

Repository: `sporely-py`

Deliver:

- upload/download for library entities;
- upload/download for observation uses;
- conflict handling;
- offline behavior;
- retry/idempotency tests.

## Stage 5 — Public observation rendering

Repository: `sporely-landing`

Deliver:

- API types/normalization;
- literature overlays;
- citation cards;
- malformed-data handling;
- accessibility and responsive behavior;
- tests with real contract fixtures.

## Stage 6 — Curated reference library and Compare integration

Repositories: all three as needed

Deliver:

- curated public library;
- admin workflow;
- species-level reference listing;
- add-reference flow in Compare;
- optional `/references` public route;
- citation export.

---

## 16. Stage 1 acceptance criteria

Stage 1 is complete when:

- existing databases migrate without data loss;
- repeated initialization does not duplicate or mutate rows unexpectedly;
- new work/treatment/measurement-set records use UUIDs;
- two treatments can reuse one work;
- multiple measurement sets can belong to one treatment;
- observation attachments persist in the main database;
- attaching a reference stores a canonical snapshot;
- deleting/detaching an observation use does not delete the library record;
- deleting a library record with active observation uses is blocked or explicitly handled;
- cross-database missing-reference conditions are detectable;
- legacy `reference_values` reads and writes continue unchanged;
- all existing tests pass;
- focused new tests cover schema, CRUD, snapshots, and integrity.

---

## 17. Tests

### Local schema

- fresh database initialization;
- migration from a representative legacy database;
- idempotent migration;
- UUID format and stability;
- uniqueness and duplicate guards;
- revision increments;
- nullable scientific values preserved as null.

### Repository behavior

- create/update/read/delete work;
- create treatment under work;
- create multiple measurement sets;
- search by title/author/year/DOI/ISBN;
- canonical citation generation;
- canonical snapshot generation;
- role validation.

### Cross-database observation links

- attach;
- list in stable display order;
- update role/note;
- detach;
- missing measurement-set detection;
- active-use deletion guard;
- snapshot remains readable if source becomes unavailable.

### Compatibility

- existing Reference Values dialog tests;
- existing Parmasto import tests;
- existing cloud reference tests;
- existing analysis plotting tests;
- backup/export/import tests include new tables.

---

## 18. Open questions

These must be resolved during Stage 0 or explicitly deferred:

1. Does the current desktop persist plotted reference selections anywhere?
2. What is the current cloud schema behind `search_public_reference_values`?
3. Should `citation_key` be user-defined, generated, or both?
4. Which citation style should the UI generate initially?
5. Should favourites be local-only or synced?
6. Can one treatment point to multiple current taxon concepts?
7. How should hybrid, aggregate, `sensu`, and variety names be represented?
8. ~~Should reference works be shareable before admin verification?~~ Resolved by removing verification and per-work visibility from the product; see §5.1.
9. What delete behavior is safest when an observation snapshot exists?
10. Should curated works be editable by users as local overlays/forks?
11. Which existing legacy records can be migrated automatically without bibliographic ambiguity?
12. Does backup/import already preserve unknown/new tables generically?

---

## 19. Risks and mitigations

### Ambiguous legacy sources

**Risk:** Free-text labels do not uniquely identify publications.
**Mitigation:** Never auto-merge on fuzzy title alone; mark migrated rows incomplete and require review.

### Cross-database integrity

**Risk:** SQLite cannot enforce a foreign key from `mushrooms.db` to `reference_values.db`.
**Mitigation:** Stable UUIDs, service-layer validation, deletion guards, snapshots, integrity tests.

### Taxonomic drift

**Risk:** Current names change while old literature names remain fixed.
**Mitigation:** Store both current `taxon_id` and immutable `name_as_published`.

### Historical plots changing

**Risk:** Editing a library record changes old public evidence.
**Mitigation:** Revisioned records plus observation snapshots and explicit update action.

### False statistical precision

**Risk:** Range data is rendered as if it were raw measurements.
**Mitigation:** Separate visual grammar and prohibit synthetic points.

### Scope expansion

**Risk:** Building a full bibliography platform delays the practical feature.
**Mitigation:** Prove observation attachment and public reproduction before `/references`.

---

## 19a. Verified baseline (2026-08-05)

Before landing the Stage 1 corrections in this task, the full test
suite was executed against clean `main` (commit `162b050`, "screen
renderer") with the Stage 1 changes stashed. Two tests failed on that
clean baseline:

1. `tests/test_cloud_visibility_phase7.py::test_apply_remote_images_to_local_applies_metadata_without_downloading`
   — `images.sort_order` remained `0` rather than updating to `3`.
2. `tests/test_sample_source_ui_presence.py::test_live_lab_slide_prep_group_appears_below_microscope_group`
   — layout produced `prep_top=189`, `micro_top=380`, so the
   Slide / Prep group appears above the Microscope group instead of
   below it.

Both failures reproduce identically after the Stage 1 corrections are
reapplied. They are therefore **verified pre-existing on main** and
unrelated to the reference-library work. They are not addressed in
this task and remain tracked for a future fix.

## 19b. Stage 0 verified findings

The Stage 0 audit inspected `database/schema.py`, `database/models.py`,
`database/reference_data_paths.py`, `references/measurement_parser.py`,
`ui/cloud_reference_dialog.py`, the local Reference Values and
Add/Edit Reference dialogs (in `ui/main_window.py`), the analysis
plotting call paths, `utils/db_share.py`, `utils/cloud_sync.py`, and
the reference-related tests in `tests/`.

### Verified facts

1. **Legacy schema.** `reference_values.db` stores exactly one legacy
   table, `reference_values`, with these columns (see
   `database/schema.py::init_reference_database`):
   `id INTEGER PRIMARY KEY AUTOINCREMENT, genus TEXT NOT NULL,
   species TEXT NOT NULL, source TEXT, mount_medium TEXT, stain TEXT,
   plot_color TEXT, parmasto_length_mean/width_mean/q_mean REAL,
   parmasto_v_sp_length/width/q REAL, parmasto_v_ind_length/width/q REAL,
   length_min/p05/p50/p95/max/avg REAL, width_min/p05/p50/p95/max/avg REAL,
   q_min/p05/p50/p95/max/avg REAL, metadata_json TEXT,
   updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP`.
2. **Fresh vs. existing database initialization.** Fresh reference
   databases are copied from the bundled seed (if present) then
   `_ensure_reference_columns` performs additive `ALTER TABLE ... ADD
   COLUMN` upgrades. Fresh main databases are built by
   `init_database()`, which also `ALTER TABLE`s any missing columns.
3. **No versioned migrations.** Schema migrations are inferred from
   `PRAGMA table_info` / `sqlite_master` presence checks; there is no
   `user_version` pragma and no ordered SQL migrations. The reserved
   folder `database/sqlite_migrations/` currently contains only a
   README. Stage 1 continues this "additive, presence-inferred"
   convention rather than introducing a new mechanism.
4. **Plotted reference selections are not persisted.**
   `ui/main_window.py` keeps `self.reference_values` and
   `self.reference_series` as transient in-memory state; the only
   observation-level persistence is `observations.spore_statistics` and
   related fields. Nothing on an observation currently records which
   `reference_values` rows were plotted for it. This is a decisive
   confirmation that Stage 1's `observation_reference_uses` table is
   filling a real gap, not duplicating existing behaviour.
5. **No stable reference UUIDs exist yet.** The legacy
   `reference_values` table keys on `INTEGER AUTOINCREMENT` `id` and on
   the compound `(genus, species, source, mount_medium, stain)` used by
   `ReferenceDB.set_reference` / `delete_reference`. Cloud rows returned
   by `search_public_reference_values` carry a `reference_id` but that
   is scoped to the cloud table; no cross-surface UUID identity exists.
6. **Backup/export/import copies the whole reference file, but imports
   only the legacy table.** `utils/db_share.py::export_database_bundle`
   copies `reference_values.db` into the zip verbatim (`zf.write(ref_path,
   arcname="reference_values.db")`) — so new library tables *will* be
   preserved in exports. `import_database_bundle`, however, only reads
   the `reference_values` table row-by-row into the destination
   database. Stage 1 does not need to change export behaviour, but a
   future stage that wants portable observation ↔ library links via the
   bundle format must extend the import path to copy the new tables
   too. This is documented as an unresolved question for Stage 2.
7. **Deleting or replacing the reference database can invalidate live
   UI state.** The Reference Values dialog and the analysis panel both
   read `ReferenceDB.get_reference(...)` on demand and cache the
   result on `MainWindow.reference_values`. Recreating the file (e.g.
   via database-settings actions or bundle import) without reloading
   the visible dialog leaves stale cached fields until the next taxon
   change. This is a pre-existing behaviour; Stage 1 does not change
   it.
8. **Public/cloud reference search shape.**
   `SporelyCloudClient.search_public_reference_values(genus, species,
   limit)` (`utils/cloud_sync.py`) calls the Supabase RPC
   `search_public_reference_values` with `p_genus`, `p_species`,
   `p_limit` and returns a list of dicts. Callers in
   `ui/cloud_reference_dialog.py` treat each row as `{reference_id,
   measurement_count, length/width/q ranges + means, updated_at, ...}`
   — i.e. the legacy-shaped reference row plus a `reference_id` string.
   No richer bibliographic contract exists on either side yet.

### Deviations from the proposed schema

- **`legacy_reference_value_id` on `reference_measurement_sets`** —
  kept as `INTEGER` (not UUID) because the legacy `reference_values`
  table keys on `INTEGER AUTOINCREMENT` (see finding 5). This is
  additive and does not affect the plan's UUID identity for
  measurement sets themselves.
- **`character` enum initially restricted to `spore_size`.** The plan
  says the field is extensible; the implementation validates against a
  single-value frozenset and leaves widening to a later stage (adding a
  new character is a one-line change plus new tests).
- **Cross-database FK not attempted.** As the plan explicitly requires,
  `observation_reference_uses.reference_measurement_set_id` is a plain
  TEXT UUID with no `FOREIGN KEY` clause. Integrity is enforced in
  `database.reference_library` (validation on attach, deletion guard on
  measurement set, dangling detector for maintenance).
- **`ON DELETE RESTRICT` (not `CASCADE`) on the same-database FKs.**
  The Stage 1 correction switched
  `reference_taxon_treatments.reference_work_id` and
  `reference_measurement_sets.taxon_treatment_id` from `ON DELETE
  CASCADE` to `ON DELETE RESTRICT`. A silent SQL cascade would delete
  library rows that other observations may still be pointing to via
  cross-database UUIDs, without giving the repository/service layer a
  chance to check for observation uses first. The repository `delete()`
  methods perform explicit descendant cleanup after the use-check
  passes, so the "delete when safe" API contract is unchanged for
  callers — only the underlying safety net is stricter.
- **`observation_reference_uses.observation_id` keeps `ON DELETE
  CASCADE`** to observations, matching the plan's rule that "deleting
  an observation should follow existing observation cascade
  conventions." The snapshot dies with the observation because the
  observation itself is the public-facing evidence container.

### Stage 1 status

- **Files added:**
  - `database/reference_library_schema.py` — DDL, allowed enum sets, and
    two idempotent init functions (`init_reference_library_schema`,
    `init_observation_reference_uses_schema`). Includes the
    CASCADE→RESTRICT auto-migration helper
    `_ensure_restrict_foreign_keys` (Stage 1 correction) that rebuilds
    the FK actions on legacy databases without touching data.
  - `database/reference_library.py` — dataclasses
    (`ReferenceWork`, `TaxonTreatment`, `MeasurementSet`,
    `ObservationReferenceUse`) plus repositories
    (`ReferenceWorkRepository`, `TaxonTreatmentRepository`,
    `MeasurementSetRepository`, `ObservationReferenceUseRepository`) and
    typed errors (`ReferenceValidationError`,
    `ReferenceIntegrityError`, `ReferenceInUseError`).
    Repository deletes now perform **explicit** child cleanup after
    the use-check, since the underlying FK is RESTRICT.
  - `database/reference_citation.py` — one shared service:
    `build_short_label`, `build_full_citation`,
    `build_observation_reference_snapshot`, `serialize_snapshot`, and
    `SNAPSHOT_SCHEMA_VERSION = 1`.
  - `tests/test_reference_library_schema.py`,
    `tests/test_reference_library_repository.py`,
    `tests/test_reference_library_snapshot.py`,
    `tests/test_reference_library_delete_semantics.py`,
    `tests/test_reference_library_bundle_roundtrip.py`.
- **Files modified:**
  - `database/schema.py` — hooks
    `init_reference_library_schema` into
    `init_reference_database` (runs on every startup, idempotent) and
    `init_observation_reference_uses_schema` into `init_database`.
  - `utils/db_share.py` — bundle export now includes
    `observation_reference_uses`; bundle import now (a) inserts
    normalized library rows keyed by UUID and revision-aware, (b)
    remaps `observation_id` for imported observation reference uses,
    (c) **preserves** uses whose measurement-set UUID is missing from
    the destination library (public-safe snapshot is retained) and
    (d) **reports** the unresolved UUIDs via the return dict's
    `unresolved_observation_reference_uses` field and a warning. Never
    silently drops the row.
  - `docs/reference-data-dialog.md` — short pointer that the doc still
    describes the legacy UI and the normalized library is not yet
    wired in.
- **Delete/import guarantees pinned by tests:**
  - Direct `DELETE FROM reference_works` with descendant treatments
    raises `sqlite3.IntegrityError`.
  - Direct `DELETE FROM reference_taxon_treatments` with descendant
    measurement sets raises `sqlite3.IntegrityError`.
  - Repository `delete()` still succeeds when nothing is attached,
    performing explicit descendant cleanup.
  - Repository `delete()` raises `ReferenceInUseError` when any
    descendant measurement set has active observation uses.
  - Legacy databases with CASCADE FKs are silently upgraded to
    RESTRICT on next open, without changing row content.
  - Bundle re-import of the same file is a no-op for library tables
    and observation uses.
  - Bundle import with a strictly newer `revision` on a work updates
    the destination row; older/equal revisions leave the destination
    untouched.
  - Bundle import that omits the library still preserves
    observation-side attachments and reports the missing set IDs.
- **Verified constraints held:** legacy `reference_values` untouched;
  all previously passing tests for reference values, Parmasto merge,
  cloud reference lookup, taxon lookup with reference data, image
  provenance bundles, and calibration bundles continue to pass;
  plotting is unmodified.

### Unresolved questions for Stage 2

1. `citation_key` policy — user-defined, generated, or both?
2. Which citation style should the UI generate initially? Stage 1 uses
   a single deterministic house style; Stage 2 should decide whether to
   surface the choice.
3. Should favourites be local-only or synced?
4. Backup/import: extend `utils/db_share.py::import_database_bundle`
   to copy the new library tables and observation uses on import.
   Deferred until observation attachment UI proves the round trip is
   worth exposing to end users.
5. Legacy migration: which existing `reference_values` rows can be
   converted to `reference_measurement_sets` without bibliographic
   ambiguity? Do not auto-merge on fuzzy title in Stage 2.

### Stage 2 vertical-slice status

The Stage 2 desktop slice attaches existing normalized measurement
sets to observations, restores them on observation open, detaches
them without disturbing the shared library, and renders literature
ranges with a dedicated rectangle grammar. It intentionally does not
add a library editor, quick-add flow, favourites, revision-update
UX, or any cloud/public work.

#### Landed in this slice

- **Attachment chooser** (`ui/reference_library_attach_dialog.py`) —
  new `ReferenceLibraryAttachDialog(QDialog)` that lists every
  unattached candidate measurement set via
  `MeasurementSetRepository.list_attachment_candidates(...)` with
  joined source, taxon, locator, kind, and raw-expression columns.
  The dialog returns `(measurement_set_id, role)`; the OK button is
  disabled until a row is selected.
- **Role persistence** — the chooser's role selector is limited to
  the `OBSERVATION_REFERENCE_ROLES` enum
  (`compared`, `supports_identification`, `contradicts`) and stores
  the exact enum value through
  `ObservationReferenceUseRepository.attach(...)`.
- **Snapshot restore** — `MainWindow._restore_reference_uses_for_observation`
  is called from `_on_observation_selected_impl` after
  `apply_gallery_settings()` so gallery settings never overwrite the
  normalized attachments. The restore path translates every stored
  snapshot through `translate_observation_reference_use(...)`,
  drops in-memory entries for the previous observation, and reports
  malformed snapshots without crashing the observation open.
- **Detach semantics** — removing a normalized reference row from
  the Analysis reference-series area resolves
  `observation_reference_use_id` before mutation and calls
  `ObservationReferenceUseRepository.detach(...)`. Detach leaves the
  shared library row intact (verified by the repository tests).
- **Range/summary rendering** — the update_graph_plots path draws
  the core `length_p05/p95` × `width_p05/p95` rectangle with a
  translucent fill and solid edge, plus a distinct outer outline
  from `length_min/max` × `width_min/max`. A `+` mean marker is
  drawn only when both supplied means translated to `length_p50`
  and `width_p50`. Range entries do not go through KDE, ellipses,
  or histogram bars.
- **Raw-points rendering** — `raw_points` snapshots stay on the
  existing observation scatter path because those points are
  genuine measurements. Only paired numeric `length_um/width_um`
  entries are emitted; incomplete pairs are dropped.
- **Gallery-settings guard** — legacy color-menu, row-edit, and
  gallery-settings serialization/apply paths guard on
  `observation_reference_use_id` so normalized entries are not
  written into legacy gallery settings and never open the legacy
  ReferenceDB editor workflow.
- **Focused tests** (`tests/test_reference_library_desktop_slice.py`) —
  translator range/summary mapping, no midpoint synthesis when
  means are absent, `p50` populated only when means are supplied,
  raw_points paired-only mapping, malformed-snapshot handling,
  bulk translator dropping `None` entries, attach → list-for-
  observation preserves the accepted bounds and revision,
  detach preserves the library row, attached-use round trip through
  the translator, `list_attachment_candidates` joins each seeded
  set with treatment/work context, and `exclude_ids` filtering.
  A Qt-guarded test constructs `ReferenceLibraryAttachDialog` with
  stub candidates and asserts `result_pair()` reflects both the
  selected row and the chosen role.
- **Localization** — the new `ui/reference_library_attach_dialog.py`
  is registered with both `tools/update_translations.sh` and
  `tools/update_translations.ps1`. The three locale catalogs
  (`i18n/Sporely_nb_NO.ts`, `i18n/Sporely_sv_SE.ts`,
  `i18n/Sporely_de_DE.ts`) now translate every new string
  introduced by this slice (attach dialog labels + Analysis-tab
  attach/detach messages in `ui/main_window.py`). Norwegian
  Bokmål and Swedish use their standard forms; German uses the
  informal "du" form.

#### Legacy migration workflow (interactive)

The default path for moving legacy `reference_values` rows into the
normalized library is now the interactive terminal walkthrough in
`tools/migrate_legacy_reference_values.py --interactive`. The operator
does not edit JSON:

- `tools/audit_legacy_reference_values.py` produces the read-only
  inventory + migration manifest template (top-level fields:
  `manifest_version`, `rows`; one entry per legacy row).
- `tools/migrate_legacy_reference_values.py --interactive` groups
  unmigrated legacy rows by *exact* normalized source string (no fuzzy
  merging), lists the current `ReferenceWorkRepository` search results
  as human-readable candidates (short-label, title, year, authors; UUID
  as secondary/debug info), and asks the operator to bind each group
  to one work.
- The menu keys are `<n>` (pick candidate), free text (filter), `r`
  (refresh after creating a new work in the desktop Reference Library
  UI), `s` (skip the source group), `u` (leave unresolved), `d N`
  (deselect legacy row N before assigning), and `q` (save progress and
  quit). Confirmation is required before each apply.
- Progress persists to `.legacy-reference-migration/interactive-state.json`
  next to the reference database (or at the path passed via
  `--state-dir`). On restart the CLI re-queries the normalized library,
  re-checks `legacy_reference_value_id` links, marks already-migrated
  rows automatically and never re-asks a completed decision.
- Migrations remain idempotent; legacy rows are never modified or
  deleted; parmasto values, `plot_color` and `metadata_json` contents
  travel into the measurement-set `notes` field as migration
  provenance, together with the original `source` string.
- `--summary` prints the current `Migrated / Remaining / Skipped /
  Unresolved` counts and the next pending source group.
- Manifest-based operation (`--manifest`) is retained for tests,
  reproducibility and recovery, but it is not required for the normal
  workflow.

#### Deferred

- Full library editor UI (create/edit works, treatments, measurement
  sets from within the desktop app) — this slice reuses the
  existing repository test paths for seeding.
- Quick-add flow for entering a new work + treatment + measurement
  set while attaching to an observation.
- Favourites / recently-used measurement sets in the chooser.
- Revision-update UX (surface a note when the attached measurement
  set has been superseded in the library).
- Cloud sync of `observation_reference_uses` — deferred to Stage 4.
  This slice does not touch Supabase, `sporely-web`, or
  `sporely-landing`.

---

## 20. Definition of done

The cross-repository feature is done when:

1. a publication is entered once in `sporely-py`;
2. a taxon treatment and measurement set are stored with full provenance;
3. the measurement set is attached to an observation;
4. closing and reopening the observation restores the same attachment;
5. publishing/syncing preserves stable IDs and a revisioned snapshot;
6. the public observation API returns the attached reference;
7. `sporely-landing` renders the observation points and literature range distinctly;
8. the page displays a complete citation and original measurement text;
9. later library edits do not silently rewrite the published observation;
10. the same publication can be reused for another observation without retyping it.
