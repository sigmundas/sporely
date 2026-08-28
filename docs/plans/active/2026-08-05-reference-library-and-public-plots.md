# Reference Library and Public Reference Plotting Plan

**Status:** Stages 1–3 are implemented in the repositories; Stage 4 desktop sync and Stages 5–6 remain.
**Canonical repository:** `sporely-py`
**Canonical path:** `docs/plans/active/2026-08-05-reference-library-and-public-plots.md`
**Scope:** `sporely-py` → `sporely-web`/Supabase → `sporely-landing`
**Primary outcome:** A reference entered once can be reused across observations, and a public observation can display the exact literature measurement sets used in desktop analysis.

---

## Agent handoff

- Status: Active; Stages 1–3 are implemented in the repositories; Stages 4–6
  remain.
- Last completed stage: Stage 3 owner-private normalized cloud state and the
  frozen-snapshot public observation projection.
- Current/next stage: Stage 4 desktop sync, including its explicit local
  `row_version` and durable-deletion prerequisites. Do not begin it implicitly.
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
  "reference_treatment_id": "uuid",
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
  "page_from": 214,
  "page_to": 214,
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
  },
  "raw_points": null
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
- a server `row_version` for compare-and-set, plus the existing domain
  `revision` on works, treatments, and measurement sets;
- tombstone/deletion semantics;
- conflict policy.

Initial conflict policy:

- bibliographic/measurement edits create a new revision;
- cloud compare-and-set uses a separate server `row_version`; desktop content
  revisions are preserved and never collapsed into the transport token;
- observation attachment role/note edits also use compare-and-set; there is no
  silent last-write-wins exception;
- snapshot changes require explicit update;
- cloud must never replace a newer local revision silently.

Create/restore sync order is work → treatment → measurement set → observation
use. Tombstone order is the reverse: use → measurement set → treatment → work.
The desktop's local observation integer is never uploaded as
`observation_id`; Stage 4 resolves it through the verified observation cloud
identity first.

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
- backup/export/import tests include normalized scientific/library and use
  tables; device-local chooser preferences are not imported.

---

## 18. Later-stage questions and resolved decisions

These must be resolved during Stage 0 or explicitly deferred:

1. ~~Does the current desktop persist plotted reference selections anywhere?~~
   Resolved by `observation_reference_uses` plus frozen snapshots.
2. ~~What is the current cloud schema behind `search_public_reference_values`?~~
   Resolved by the Stage 3 audit in §19c: it is a legacy, ownerless bigint
   table/RPC and remains an additive compatibility surface.
3. ~~Should `citation_key` be user-defined, generated, or both?~~ Resolved:
   optional and user-entered in Stage 2.
4. ~~Which citation style should the UI generate initially?~~ Resolved: the
   deterministic Stage 1 house style, without a selector.
5. ~~Should favourites be local-only or synced?~~ Resolved: local-only in
   Stage 2; cloud sync would need a later contract.
6. Can one treatment point to multiple current taxon concepts?
7. How should hybrid, aggregate, `sensu`, and variety names be represented?
8. ~~Should reference works be shareable before admin verification?~~ Resolved by removing verification and per-work visibility from the product; see §5.1.
9. ~~What delete behavior is safest when an observation snapshot exists?~~
   Resolved: block deletion while used; retained snapshots remain readable if
   source records are externally missing.
10. Should curated works be editable by users as local overlays/forks?
11. ~~Which existing legacy records can be migrated automatically without bibliographic ambiguity?~~
    Resolved by the review-gated interactive migration; no fuzzy auto-merge.
12. ~~Does backup/import already preserve unknown/new tables generically?~~
    Resolved: scientific normalized tables and uses are imported explicitly;
    local preference metadata may be present in a copied database archive but
    is intentionally not merged on import.

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

### Stage 2 decisions and later deferrals

1. `citation_key` remains an optional user-entered value; Stage 2 does not
   generate or require it.
2. The UI uses the canonical deterministic house style from Stage 1. A style
   selector is not part of Stage 2.
3. Favourites and recent-use metadata are local-only in Stage 2; any sync
   policy requires a later explicit contract.
4. Backup/import: extend `utils/db_share.py::import_database_bundle`
   to copy the new library tables and observation uses on import.
   Deferred until observation attachment UI proves the round trip is
   worth exposing to end users.
5. The landed interactive legacy migration requires review for ambiguous
   sources and never auto-merges publications from fuzzy title similarity.

### Stage 2 vertical-slice status

The Stage 2 desktop work is complete: it provides normalized CRUD,
quick-add and existing-set attachment, restore/detach/plot behavior,
explicit revision and successor workflows, and local favourite/recent
conveniences. It intentionally does not add cloud/public behavior.

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

#### Reference Library CRUD completion slice (2026-08-28)

The repository already contained substantially more library UI than the
older deferred-status text below recorded. This slice verified that current
implementation and closed the remaining parser and deletion gaps needed for
normal desktop CRUD:

- `ui/reference_library_manager_dialog.py` provides the existing three-pane
  Work → Taxon Treatment → Measurement Set manager and its human-facing
  `_ReferenceWorkForm`, `_TaxonTreatmentForm`, and `_MeasurementSetForm`.
  Create/edit actions use only `ReferenceWorkRepository`,
  `TaxonTreatmentRepository`, and `MeasurementSetRepository`, retaining UUIDs
  and repository revision increments.
- The measurement-set form now calls
  `references.measurement_parser.parse_measurement_string` from an explicit
  **Parse expression** action. It maps printed extremes, core bounds, explicit
  centres/means, Q values, and `n` without inventing midpoints, means, counts,
  or raw points. `raw_text` remains unchanged and first-class. Because the
  normalized model has no Q core-bound columns, supplied Q endpoints map to
  `q_min`/`q_max`; explicit `Qm` (or an explicitly printed Q centre) maps to
  `q_mean`.
- The manager now offers confirmed deletion for the selected work, treatment,
  or measurement set. Deletes go through repository APIs. Active observation
  uses remain protected by `ReferenceInUseError`, which is surfaced as an
  understandable warning; the UI never bypasses the guard. Successful deletes
  refresh the manager and emit `library_changed`, so attachment candidates are
  refreshed by existing callers.
- Existing Parmasto rows remain viewable/editable for preservation, but users
  still cannot create normalized Parmasto sets until the domain model and
  snapshot/plot contract can represent those values without flattening them.
- Focused coverage in `tests/test_reference_library_manager_dialog.py` now
  includes parser-derived bounds, verbatim printed text, nullable means,
  repository-backed deletion, in-use error surfacing, hierarchy refresh, and
  post-delete attachment-candidate visibility. The broader existing form tests
  cover treatment/set create and edit identity/revision behavior, independent
  `taxon_id` and `name_as_published`, and raw-point preservation.
- `tools/review_ui/scenarios/references.py` registers
  `reference.library-manager`, a deterministic no-network screenshot of the
  complete three-pane hierarchy. All new strings are translated in the
  maintained Bokmål, Swedish, and informal-German catalogs and compiled `.qm`
  files.

The assumption that the full library editor was still absent proved stale:
commits predating this slice had already landed the manager, work/treatment/set
forms, entry paths, and most CRUD tests. No competing second library surface
was created. Existing attached observation snapshots remain frozen when an
underlying library row is edited; the explicit update UX landed in the next
slice below.

#### Attached snapshot revision-awareness slice (2026-08-28)

- Staleness is defined by the **semantic content of the canonical snapshot**,
  not by `reference_measurement_sets.revision` alone. The repository rebuilds
  the candidate snapshot through
  `database.reference_citation.build_observation_reference_snapshot` and
  compares parsed JSON after ignoring only the top-level
  `reference_revision`. This catches citation/work-only and treatment-only
  changes as well as measurement-set changes, while timestamp changes and
  revision-only identical saves do not create false update prompts.
- `observation_snapshots_semantically_equal` owns that comparison contract.
  `ObservationReferenceUseRepository.snapshot_status` reports `current`,
  `update_available`, or `source_missing`, and
  `ObservationReferenceUseRepository.refresh_snapshot` performs the explicit
  canonical rebuild. A no-op comparison performs no database write.
- Explicit refresh changes only `snapshot_json`, `reference_revision`, and
  `updated_at`. The observation-use UUID, observation association,
  measurement-set UUID, role, note, selection time, and creation time remain
  unchanged. A successor measurement-set UUID is not followed implicitly;
  adoption is handled only by the separate explicit workflow below.
- Missing works/treatments/measurement sets leave the historical snapshot
  readable and plotted. They are reported as `source_missing`, do not show an
  update action, and an attempted repository refresh fails without modifying
  the attachment.
- The Analysis reference table now has an unobtrusive Library column. Only a
  semantically stale normalized attachment shows **Update** with an **Update
  from library** hint. The explicit handler protects against active-observation
  drift, refreshes the row and plot from persistence, and retains saved
  enabled/color display overrides. Tooltips report update availability or a
  missing source without changing the frozen row label or plot first.
- Focused tests in `tests/test_reference_snapshot_updates.py`,
  `tests/test_reference_library_desktop_slice.py`, and
  `tests/test_main_window_reference_panel_taxon_lookup.py` cover work-only,
  treatment-only, and measurement-set edits; pre-update immutability; explicit
  refresh; semantic no-ops; missing sources; stable attachment identity and
  metadata; successor isolation; observation drift; UI decoration/action; and
  refresh of the plotted in-memory row.

#### Explicit successor-adoption slice (2026-08-28)

- Successor discovery follows the explicit graph direction
  `successor.supersedes_id → predecessor`; it never guesses from revisions,
  timestamps, content similarity, or UUID ordering. A single chain resolves to
  its terminal successor. Forks, cycles, missing attached sources, and broken
  source bundles at any point in the chain (including the attached source),
  and successors the current plot translator cannot
  render fail closed and leave the historical attachment in place.
- `MeasurementSetRepository.resolve_terminal_successor` owns deterministic
  graph traversal. `ObservationReferenceUseRepository.successor_status`
  validates every source bundle in the chain and builds the terminal preview through the canonical
  snapshot service. `ObservationReferenceUseRepository.adopt_successor`
  re-resolves before writing and rejects a changed lineage, semantic successor
  content that differs from the reviewed canonical snapshot, or a successor
  that is already independently attached to the observation. Revision-only
  churn with identical canonical content does not invalidate confirmation.
- Adoption deliberately retains the existing `observation_reference_uses.id`.
  That UUID identifies the observation's comparison and is also the key for
  per-use display overrides and bundle-import idempotency. After explicit
  confirmation, only `reference_measurement_set_id`, `reference_revision`,
  `snapshot_json`, and `updated_at` change; observation association, role,
  note, selection time, and creation time remain unchanged.
- The Analysis Library column distinguishes in-place **Update** from
  **Review successor…**. The review dialog shows both full canonical citations,
  labels, taxon/locator, and raw expressions and defaults to cancellation. No successor is adopted until the user selects
  **Adopt successor**. Observation-selection drift and lineage changes between
  review and adoption abort safely.
- Tests in `tests/test_reference_successor_adoption.py`,
  `tests/test_reference_library_desktop_slice.py`, and
  `tests/test_main_window_reference_panel_taxon_lookup.py` cover direct and
  chained successors (including a broken intermediate), no successor,
  missing/broken sources, fork/cycle, reviewed-content drift
  protection, explicit adoption, cancellation, stable use identity and
  metadata, action distinction, and reopen/plot behavior.

#### Active-observation quick-add slice (2026-08-28)

- The Analysis panel's **Quick add…** flow now captures the minimum normalized
  hierarchy without leaving the observation: an explicitly selected or newly
  drafted work, editable `name_as_published` and locator, and a verbatim
  measurement expression parsed by the existing measurement parser/editor.
  Bibliographic completeness remains a non-blocking hint in the reused work
  editor.
- New work editing uses the canonical `ReferenceWorkEditor` in draft mode, so
  accepting that nested editor does not write anything before the outer quick
  add is confirmed. Existing works remain searchable and explicitly selected;
  exact normalized DOI/ISBN identity may safely reuse a work, while titles,
  authors, and other fuzzy bibliographic similarities never auto-merge.
- `QuickAddReferenceService` owns normalized persistence. Treatments are reused
  only on an exact selected-work + taxon ID + case-insensitive trimmed
  `name_as_published` + normalized locator match. A new measurement set is
  always created, preserving distinct scientific datasets and leaving
  successor/revision semantics unchanged. Attachment uses the canonical
  snapshot repository path, then the observation rows and plot are refreshed.
- The reference and observation stores are separate SQLite databases, so the
  operation validates all editor output before writing and records ownership
  of every new row. If attachment fails, it compensates in reverse order and
  deletes only the measurement set, treatment, and work created by that
  attempt; reused hierarchy rows are never rollback targets. Cancelling before
  confirmation performs no write. Normalized-intended quick adds are validated
  and attached before any compatibility persistence and do not create a legacy
  reference row, so a validation or attachment failure cannot leave a fallback
  row that looks like a successful add. Explicit legacy-only submissions keep
  their existing behavior.
- Focused tests in `tests/test_reference_quick_add_service.py`,
  `tests/test_reference_add_dialog_normalized.py`,
  `tests/test_reference_library_manager_dialog.py`, and
  `tests/test_reference_panel_taxon_drift_and_retry.py` cover existing
  work/treatment reuse, new and incomplete works, exact duplicate avoidance
  without fuzzy merging, parser success/failure, verbatim `raw_text`, draft
  cancellation, compensating rollback, canonical snapshot attachment, and
  observation/plot refresh.

#### Favourites, recents, and Stage 2 completion audit (2026-08-28)

- Favourite and recent-use metadata is local-only in
  `reference_values.db`. Favourites change only through the chooser's explicit
  star control. Recency uses a monotonic local sequence and advances only after
  a new attachment has successfully translated into the plot, including
  normalized quick-add; browsing, cancellation, restore, detach, revision
  refresh, and failed attachment do not count as use.
- Preference metadata is deliberately device-local convenience state and is
  excluded from cloud sync. A full copied database archive may physically
  contain the local table, but bundle import intentionally does not merge it;
  imported library records therefore start without favourite/recent ranking
  on the receiving device. Scientific records and snapshots are unaffected.
- The existing attachment chooser now offers **All**, **Favourites**, and
  **Recently used** views. Ordering is deterministic: favourites first,
  recency descending, then stable publication/taxon/UUID keys. Missing or
  deleted measurement sets never surface; normal deletion cascades preference
  cleanup. This metadata does not participate in snapshots, revisions,
  successor lineage, legacy storage, or cloud sync.
- The quick-add publication picker derives its leading work order from the
  favourite/recent state of that work's measurement sets, then falls back to
  stable work update/UUID order. This gives the work-level shortcut required
  by Stage 2 without a second preference source of truth.
- The completion audit verified library CRUD; nested treatment and measurement
  editing; attach/detach/restore/plot; quick-add and rollback; canonical
  snapshot updates; successor adoption; legacy compatibility; translations;
  and focused regression coverage. Small audit gaps were closed: work search
  includes year/DOI/ISBN, probable exact title/year/first-author duplicates
  require confirmation, and name-only normalized treatments remain possible
  when an observation has no taxon identifier.

#### Deferred beyond Stage 2

- Cloud sync of `observation_reference_uses` — deferred to Stage 4.
  This slice does not touch Supabase, `sporely-web`, or
  `sporely-landing`.

## 19c. Stage 3 cloud audit and proposed contract (2026-08-28)

This section is the implementation contract for Stage 3. The initial audit
was read-only; the first implementation slice described below landed on
2026-08-28.

### Verified `sporely-web` state

1. **Only the legacy reference table exists.** The baseline migration defines
   `public.reference_values` with a bigint identity and the legacy
   genus/species/source, preparation, Parmasto, and aggregate measurement
   columns. It has no owner, stable cross-client UUID, revision, tombstone,
   bibliography, treatment, normalized measurement-set, or observation-use
   model. No later migration replaces it.
2. **The legacy search RPC is a separate compatibility API.**
   `search_public_reference_values(p_genus text, p_species text,
   p_limit integer default 50)` is a `STABLE SECURITY DEFINER` function. It
   returns the legacy bigint `reference_id` and legacy measurement shape,
   performs case-insensitive exact genus and optional species matching, and
   orders by `updated_at DESC, id DESC`. It is executable by `anon`,
   `authenticated`, and `service_role`. Its limit is lower-bounded but not
   upper-capped.
3. **There is no single JSON publication payload.** Observation detail reads
   the owner table or the community/friend views, while microscopy and other
   public-safe data use separate RPCs. Public eligibility currently means
   `visibility = 'public'`, `NOT coalesce(is_draft, false)`, no banned owner,
   and no applicable block. There is no deployed `published_at` or
   `publish_observation` contract to target.
4. **Raw sync tables are owner-only.** Current hardening restricts raw
   observations, images, and measurements to `auth.uid() = user_id`; public
   consumers use allowlisted views/RPCs. Default privileges no longer expose
   new public-schema tables/functions automatically. Every new table/function
   therefore requires deliberate grants as well as RLS.
5. **Existing deletion precedent is soft deletion.** Media uses `deleted_at`;
   owner reads can retain tombstones while public projections exclude them.
   There is no reusable revision-aware upsert pattern, so this project must
   define one explicitly.
6. **Supabase tests are transactional SQL fixtures.** Tests in
   `supabase/tests/` use fixed UUIDs, JWT claim/role switching, exception
   assertions, and rollback. Privilege tests sometimes need local container
   `psql` after `supabase db reset --local`.

### Normalized private schema

All four tables are private owner-sync state even though they live in the
exposed `public` schema. `anon` receives no table privileges. `authenticated`
receives owner-scoped `SELECT`; mutation is only through the revision-aware
RPCs below. `service_role` retains administrative access.

Common columns on every table:

- `id uuid NOT NULL` supplied by the desktop; the cloud never replaces it;
- `user_id uuid NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE`;
- `row_version bigint NOT NULL DEFAULT 1 CHECK (row_version >= 1)` as the
  server compare-and-set token;
- `created_at timestamptz NOT NULL DEFAULT now()`;
- `updated_at timestamptz NOT NULL DEFAULT now()`;
- `deleted_at timestamptz NULL` as the durable sync tombstone.

The primary key is `(user_id, id)`, not global `id`. Bundle import currently
preserves UUIDs, so two accounts may legitimately own private copies of the
same portable graph. Every point lookup and cursor query is owner-scoped.

`reference_works` adds the Stage 2 fields with these cloud types:
`type text`, `citation_key text`, `authors_json jsonb`, `editors_json jsonb`,
`title text`, `container_title text`, `year integer`, `edition text`,
`publisher text`, `place text`, `volume text`, `issue text`, `pages text`,
`doi text`, `isbn text`, `url text`, `language text`, `short_label text`, and
`citation_override text`. `type` is checked against `book`, `article`,
`chapter`, `website`, `dataset`, and `other`. There is no verification or
visibility column. It also has `revision integer NOT NULL CHECK (revision >=
1)`, preserving the desktop content revision independently of `row_version`.
A partial unique index enforces nonblank live
`citation_key` per owner. DOI/ISBN get normalized lookup indexes, not global
uniqueness: duplicate candidates are reported to the owner and never silently
merged.

`reference_taxon_treatments` adds `reference_work_id uuid NOT NULL`,
`taxon_id text`, `name_as_published text NOT NULL`, `page_from integer`,
`page_to integer`, `locator_text text`, and `treatment_notes text`. The taxon
identifier stays text to preserve the desktop's stable identifier verbatim; it
is not forced into the current cloud taxonomy's integer key. It has its own
desktop-compatible `revision integer NOT NULL CHECK (revision >= 1)`.

`reference_measurement_sets` adds `taxon_treatment_id uuid NOT NULL`,
`character text`, `raw_text text`, `data_kind text`, all numeric/count/method
fields listed in §5.3, `raw_points_json jsonb`, and `supersedes_id uuid`.
It has its own desktop-compatible `revision integer NOT NULL CHECK (revision >=
1)`.
`character` initially permits only `spore_size`; `data_kind` permits `range`,
`summary`, `raw_points`, and `parmasto`. A live successor may point only to a
same-owner set, never itself; a partial unique index on
`(user_id, supersedes_id)` rejects live forks, and the write function rejects
cycles. Adoption is never automatic. `legacy_reference_value_id` is local
migration provenance and is not synced.

`observation_reference_uses` adds `observation_id bigint NOT NULL REFERENCES
public.observations(id) ON DELETE CASCADE`,
`reference_measurement_set_id uuid NOT NULL`, `role text NOT NULL`, `note
text`, `selected_at timestamptz NOT NULL`, `reference_revision integer NOT
NULL`, and `snapshot_json jsonb NOT NULL`. Its `row_version` detects concurrent
role, note, detach, refresh, and successor-adoption writes without inventing a
second desktop content revision. A unique
constraint on `(user_id, observation_id, reference_measurement_set_id)` makes
attach/retry idempotent and resurrection reuse the same use UUID.

Composite foreign keys carry
`user_id` through work → treatment → measurement set → use, so a row cannot
join another owner's graph. `reference_measurement_sets` uses a composite
self-FK `(user_id, supersedes_id)`. Add `UNIQUE (user_id, id)` to observations
and a composite FK `(user_id, observation_id)` from uses, so observation
ownership is enforced even outside the RPC. The use write path additionally
verifies that `observations.user_id = auth.uid()`. Parent FKs use `ON DELETE
RESTRICT`; client-visible hard delete is unsupported.

Owner change-feed indexes end in the UUID tie-breaker:
`(user_id, updated_at, id)`. Child lookup indexes cover the parent FK and
active observation uses. Stage 4 readers must paginate completely in ascending
`updated_at, id` order and must never infer deletion from absence.

### Owner API and revision semantics

Expose one typed, allowlisted mutation RPC per table:

- `sync_reference_work(p_payload jsonb, p_expected_row_version bigint)`;
- `sync_reference_taxon_treatment(p_payload jsonb, p_expected_row_version bigint)`;
- `sync_reference_measurement_set(p_payload jsonb, p_expected_row_version bigint)`;
- `sync_observation_reference_use(p_payload jsonb,
  p_expected_row_version bigint, p_snapshot_mode text default 'current')`.

The functions accept full-row payloads but extract only the documented keys;
unknown keys are rejected. `user_id`, timestamps, and `row_version` are never
trusted from the payload. Library content `revision` is validated as
nondecreasing domain data and may advance multiple times while offline; it is
distinct from server `row_version`. Creation
requires `p_expected_row_version = 0`, a caller-supplied owner-scoped UUID,
and produces row version 1. Update, tombstone, restore,
role/note change, explicit snapshot refresh, and explicit successor adoption
require the current positive row version and atomically produce exactly
`row_version + 1`. A stale expectation returns a structured `conflict` result
with the caller-owned current row; it never overwrites. A semantically
identical retry (including equal domain revision) returns `no_change` and does
not advance `row_version` or
`updated_at`. Successful outcomes are `created`, `updated`, or `no_change`.

Each mutation runs in one owner-scoped transaction, locks the target and
relevant parent/source rows in a documented consistent order, and finishes
with a conditional `UPDATE ... WHERE row_version = p_expected_row_version`.
Attach/restore locks the observation and source graph; tombstone locks live
children/uses before checking; successor graph mutation takes an owner-scoped
transaction advisory lock before its recursive cycle/fork check. Unique races
return same-owner `no_change` or `conflict`, never a raw constraint error.

These RPCs are `SECURITY DEFINER` only because direct table mutation is
revoked. Each uses `SET search_path = ''` with every object schema-qualified,
rejects null `auth.uid()`, derives the
owner from `auth.uid()`, verifies every parent/observation owner, revokes
`EXECUTE` from `PUBLIC`/`anon`, and grants only `authenticated`, not
`service_role` (administrative code uses its direct table access). They do not
use JWT user metadata for authorization.

Owner reads use RLS-protected tables directly, including tombstones. Separate
SELECT/INSERT/UPDATE/DELETE policies are explicit; every predicate is
`(select auth.uid()) = user_id`, and update has both `USING` and `WITH CHECK`.
Because direct mutation grants are revoked, those mutation policies are
defense in depth for privileged/internal callers rather than an alternate
write path.

### Snapshot and deletion contract

- A newly created or explicitly replaced use must reference a live,
  same-owner measurement set and observation. Its snapshot must be a JSON
  object with `schema_version = 1`; embedded work/treatment/set IDs and
  `reference_revision` must equal the row and current source graph. Role is
  checked against `compared`, `supports_identification`, and `contradicts`.
- Snapshot v1 is exactly the shape emitted by
  `build_observation_reference_snapshot`, including
  `reference_treatment_id`, `page_from`, `page_to`, and `raw_points` in
  addition to the fields shown in §6. `p_snapshot_mode = 'current'` requires a
  complete live same-owner source graph and value agreement. The only other
  accepted mode is `historical_import`, used for the first cloud sync of an
  already-frozen local use: it still requires the same-owner source graph and
  validates schema, size, embedded IDs, and owner/observation access, but does
  not demand value equality with a now-newer source. A locally dangling use
  remains local-only until its source graph is restored/synced; its snapshot is
  never discarded or silently rewritten.
- Snapshot replacement occurs only through an explicit use write. Editing any
  work, treatment, or measurement set never updates an existing use.
  Successor adoption keeps the use UUID and changes its set ID, frozen
  revision, and snapshot in one compare-and-set operation.
- Snapshot validation enforces the §6 key/type contract and a conservative
  encoded-size limit. Public projection reconstructs an allowlisted JSON
  object rather than returning arbitrary stored keys. Owner-only `note`,
  treatment/measurement private notes, owner IDs, local provenance, and
  timestamps are not public.
- Detach tombstones the use; it does not delete the library row. Reattach
  restores the same unique use row with a new row version and a newly
  validated explicit snapshot.
- Library deletion is a tombstone transition. A measurement set with a live
  use cannot be tombstoned. A treatment/work cannot be tombstoned until its
  live children are tombstoned, and descendant sets with live uses therefore
  block the operation. This mirrors the desktop's in-use guard while retaining
  durable cross-device deletion intent. Tombstoning or editing a source never
  cascades to, rewrites, or hides an already frozen live snapshot.
- Hard deletion is reserved for account erasure/administration. It is never a
  sync instruction, and a missing/partial/failed remote read is never deletion
  evidence.
- Account erasure calls the service-role-only
  `delete_reference_library_for_account(uuid)` RPC before deleting
  observations or the auth user. In one transaction it takes the same
  owner-scoped advisory lock as all mutation RPCs, persists an
  account-deletion marker, and hard-deletes uses → measurement sets →
  treatments → works. Subsequent owner mutations fail with
  `account_deleting`; deletion of the auth user cascades the marker. This
  prevents authenticated recreation between deletion stages and does not rely
  on competing auth-user cascades through `ON DELETE RESTRICT` parents.

### Public observation projection

Add an additive batch RPC
`search_public_observation_references(p_observation_ids bigint[])` and a
single-observation wrapper
`get_public_observation_references(p_observation_id bigint)`. Do not change the
column order or return type of the frequently replaced
`get_public_observation(bigint)` in the first migration.

The batch RPC is `STABLE SECURITY DEFINER`, uses `SET search_path = ''` with
schema-qualified objects, rejects an oversized raw array with
`cardinality(p_observation_ids)` before unnest/deduplication, also caps the
number of distinct requested observations, and returns one row per eligible
observation with a deterministic `references jsonb` array ordered by
`selected_at, id`. Each array item is exactly:

```json
{
  "use_id": "uuid",
  "role": "supports_identification",
  "reference_revision": 3,
  "snapshot": { "schema_version": 1 }
}
```

Only live uses of observations that are public, non-draft, non-banned, and
not blocked for the authenticated caller are included. The RPC does not join
mutable library fields and continues to expose the frozen snapshot if a source
later becomes unavailable. `note` stays owner-private in Stage 3 because the
Stage 2 field has no explicit public-safe authoring contract. The function
revokes `PUBLIC` execution and explicitly grants `anon`, `authenticated`, and
`service_role`. Stage 5 may compose this result into the landing payload;
there is still no public reference catalogue.

### Legacy compatibility

`public.reference_values` and the
`search_public_reference_values(text, text, integer)` signature, bigint IDs,
normal-call semantics, grants, and result shape remain compatible. A
compensating definition may clamp `p_limit` to a conservative upper bound to
remove the existing anonymous resource-amplification path. The normalized private
tables are not unioned into that RPC, and private records are never copied
into it merely because an observation uses them. The cloud-reference dialog
can therefore continue its existing observation-derived/legacy search while
Stage 4 adds normalized owner sync separately. Any future curated catalogue
requires the separately scoped Stage 6 moderation/publication design.

### Required Stage 3 tests

- migration replay/fresh reset, constraints, grants, indexes, and RLS;
- owner create/read/update/tombstone/restore for all four tables;
- forged owner and cross-owner parent/observation rejection;
- stale row-version conflict, preservation of multi-step offline content
  revisions, retry no-op, and no timestamp/row-version echo bump;
- attach uniqueness/race, explicit snapshot replacement, successor adoption,
  and tombstone resurrection retaining UUID;
- deletion blocked by live descendants/uses and frozen snapshots unchanged by
  source edit/tombstone attempts;
- anonymous and non-owner denial on raw normalized tables;
- public projection for public versus draft/private/friends/banned/blocked
  observations, strict snapshot allowlist, malformed snapshot fail-closed,
  and owner-only note exclusion;
- legacy search signature, bigint identity, grants, and result shape unchanged;
- deterministic pagination/cursor ties and no deletion inference from partial
  results;
- transactional attach/tombstone and concurrent successor-fork/cycle races;
- account deletion in the explicit dependency order, with no RESTRICT failure.

### Corrected assumptions and remaining Stage 4 work

- The cloud public detail path is compositional; Stage 3 should add a focused
  reference RPC instead of modifying every observation view/RPC.
- Cloud taxonomy IDs cannot safely replace the desktop's text `taxon_id`.
- The desktop use table currently lacks a server row-version token, and local
  library delete/detach operations are physical. Before cloud sync ships,
  Stage 4 must store cloud `row_version`/baselines and add a durable
  tombstone/change ledger (or equivalent) for all four entity types. It must
  push creates/restores parent-first and tombstones child-first, translate the
  local observation integer through verified cloud identity, and preserve
  strict pull-only zero-write
  behavior, cloud-account binding, complete pagination, and cursor/baseline
  advancement only after a successful whole-graph pull.
- Public exposure follows observation visibility, not
  `spore_data_visibility`: the frozen values describe published literature,
  not the observer's private measurements. This must be called out in Stage 3
  SQL tests and public API documentation.

### Landed Stage 3 slice: normalized private schema and mutations (2026-08-28)

Migration
`sporely-web/supabase/migrations/20260828143513_add_normalized_reference_library.sql`
now implements the four normalized owner-private tables, composite owner/UUID
keys and foreign keys, live-successor uniqueness, owner cursor indexes,
durable tombstones, independent domain `revision` and server CAS
`row_version`, strict owner-read RLS, and revoked direct authenticated writes.
The four allowlisted `SECURITY DEFINER` sync RPCs are the only authenticated
mutation surface. They serialize per owner, return structured outcomes,
preserve exact retries without version churn, reject cross-owner graph links,
and prevent tombstoning live dependencies.

`current` observation-use writes now require exact equality with a
server-derived canonical snapshot. `historical_import` accepts only a strict
snapshot-v1 shape for first upload/create retry and cannot rewrite existing
evidence. Role/note-only edits and detach retries preserve the frozen snapshot
without depending on a subsequently edited or tombstoned source. A set change
is accepted only when it names the unique terminal successor reached from the
currently attached set; arbitrary same-owner retargeting is rejected.

Account deletion now uses the transactional, service-role-only
`delete_reference_library_for_account` RPC described above. The Edge Function
plan and its focused tests use that single stage rather than four independently
racing deletes.

Transactional coverage is in
`sporely-web/supabase/tests/reference_library_mutation_test.sql`. It exercises
owner isolation, denied anonymous/direct writes, owner-scoped UUID reuse,
parent integrity, CAS success/conflict, domain revisions, lost-response retry
no-ops, create/update/tombstone behavior, frozen current/historical snapshots,
successor fork and adoption rules, and account-deletion exclusion. The focused
delete-account tests cover RPC ordering, failure containment, exact parameters,
and retries. Verification used a fresh local database reset, the transactional
SQL test, the focused Node test files, and `supabase db lint --local --level
warning`.

The security/concurrency review found no cross-owner RLS or definer escape. It
did identify canonical-snapshot validation, frozen-use update semantics,
arbitrary successor retargeting, stale-token retry behavior, raw uniqueness
errors, and an account-deletion race. This slice corrected those issues with
server canonicalization, strict snapshot keys, target-first retry handling,
terminal-successor traversal, structured conflict handling, and the persistent
deletion marker/shared advisory lock.

### Landed Stage 3 slice: public frozen-snapshot projection (2026-08-28)

Migration
`sporely-web/supabase/migrations/20260828172243_add_public_observation_references.sql`
adds the audited APIs without changing any existing observation RPC:

- `search_public_observation_references(bigint[])` returns one row per
  requested eligible observation as `(observation_id, references jsonb)`,
  deduplicates IDs, rejects raw arrays above 200 and distinct sets above 100,
  and orders each attachment array by `selected_at, id`;
- `get_public_observation_references(bigint)` returns that observation's array,
  `[]` for an eligible observation with no valid live attachments, and `NULL`
  when the observation is not publicly readable;
- both functions are `STABLE SECURITY DEFINER`, use an empty search path,
  revoke `PUBLIC`, and explicitly grant `anon`, `authenticated`, and
  `service_role`.

Eligibility exactly matches the existing public observation RPCs: literal
public visibility, non-draft, non-banned owner, and no bidirectional block for
an authenticated caller. There is no owner/private exception. The projection
reads only live `observation_reference_uses`; it never joins works, treatments,
or measurement sets, so a later source tombstone cannot alter or hide frozen
evidence. Each result item contains only `use_id`, `role`,
`reference_revision`, and a reconstructed snapshot-v1 allowlist. Owner note,
owner identity, row/tombstone/version metadata, unknown snapshot keys, and
raw-point metadata are excluded. Malformed or unsupported snapshots are
omitted fail-closed without hiding other valid attachments.

Transactional coverage in
`sporely-web/supabase/tests/public_observation_references_test.sql` verifies
anonymous and authenticated access; public/private/friends/draft boundaries;
bans and blocks; owner isolation; deterministic ordering and deduplication;
empty results; exact public shapes; malformed/unsupported snapshots;
tombstoned uses; frozen evidence after source tombstoning; request caps;
explicit grants; continued privacy of normalized tables; and preservation of
the legacy `search_public_reference_values` surface. Fresh reset, the prior
normalized mutation test, existing public-observation regression tests, and
schema lint all pass. A fresh security/privacy review found no actionable
issues after verifying the definer/search-path boundary, visibility and block
rules, snapshot allowlists, request caps, deterministic aggregation, and the
absence of current-library joins.

No Stage 3 implementation remains. The new migration still requires the normal
reviewed deployment workflow before the API is live. Stage 4 desktop sync,
including its local `row_version` and durable deletion dependencies, remains
explicitly deferred. Historical raw-point entries retain the Stage 2 desktop
validator's compatible stored shape; this projection reconstructs their
public nested shape rather than returning stored metadata blindly.

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
