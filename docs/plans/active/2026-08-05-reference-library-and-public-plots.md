# Reference Library and Public Reference Plotting Plan

**Status:** Stages 1–5 and Stages 6a–6g are implemented and verified. Stages
6h–6l remain.
**Canonical repository:** `sporely-py`
**Canonical path:** `docs/plans/active/2026-08-05-reference-library-and-public-plots.md`
**Scope:** `sporely-py` → `sporely-web`/Supabase → `sporely-admin` →
`sporely-landing`
**Primary outcome:** A reference entered once can be reused across observations, and a public observation can display the exact literature measurement sets used in desktop analysis.

---

## Agent handoff

- Status: Active; Stages 1–5 and Stage 6a–6g are complete, and the remaining
  Stage 6 contract is resolved.
- Last completed slice: Stage 6g exact-taxon public read APIs in `sporely-web`
  (`a2eeea5`). The catalogue and moderation system remain dormant,
  operational memberships/policies and allowed origins are still unsupplied,
  and nothing was deployed.
- Current/next slice: Stage 6h landing curated API/read model and species listing,
  only when separately started from this canonical plan.
- Relevant Stage 4 commits: `199f127`, `69ec641`, `8893007`, `edd9f70`,
  `e8b340b`, `ea1e1b9`, `eaca8e7`, `0277516`, `9c5346b`.
- Relevant Stage 5 commit (`sporely-landing`): `5af3cb8`.
- Important decisions: Preserve stable UUIDs, frozen observation snapshots, revision-aware records, and the distinction between literature ranges and raw observations.
- Comparison baseline: the frozen `cloud-sync-pre-refactor` tag; at the Stage 4
  audit it resolves to `e9accd9`, the audit's starting `refactor/cloud-sync`
  HEAD.
- Do not: Fuzzy-merge bibliographic records, fabricate statistics, expose
  personal libraries, or bypass the Stage 6 moderation/publication boundary.
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

Repositories: `sporely-web`, `sporely-admin`, `sporely-landing`, and
`sporely-py` only in the explicit personal-fork/submission slice.

Deliver:

- separate curated storage and immutable published revisions;
- consent-based personal submission and audited curator workflow;
- exact stable-taxon public API and species-level reference listing;
- frozen-reference Compare items that never become synthetic observations;
- plain-text, BibTeX, and CSL-JSON citation export from curated source data;
- explicit curated-to-personal fork support without identity reuse or fuzzy
  merging;
- no `/references` route in the core Stage 6 delivery; that route remains a
  separately approved optional follow-up.

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
6. ~~Can one treatment point to multiple current taxon concepts?~~ Resolved for
   Stage 6: yes, but only through explicit curator-authored assignments to
   stable `sporely_taxon_id` values. Public lookup never infers descendants or
   matches names fuzzily.
7. ~~How should hybrid, aggregate, `sensu`, and variety names be represented?~~
   Resolved for Stage 6: preserve the source wording in
   `name_as_published`. A record appears on a species surface only when a
   curator explicitly assigns it to an existing stable
   `taxonomy_v3.registry_concept`.
   Unresolved or ambiguous concepts remain reviewable but are not public-list
   candidates.
8. ~~Should reference works be shareable before admin verification?~~ Resolved by removing verification and per-work visibility from the product; see §5.1.
9. ~~What delete behavior is safest when an observation snapshot exists?~~
   Resolved: block deletion while used; retained snapshots remain readable if
   source records are externally missing.
10. ~~Should curated works be editable by users as local overlays/forks?~~
    Resolved: curated rows are read-only. A user may explicitly fork a
    published bundle into a new owner-private graph with new UUIDs and durable
    origin provenance. The fork never follows or overwrites from catalogue
    changes automatically.
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

## 19d. Stage 4 desktop-sync audit and implementation contract (2026-08-28)

This audit/design slice changes no runtime behavior. It was performed on the
clean `refactor/cloud-sync` branch at `e9accd9`; the frozen
`cloud-sync-pre-refactor` tag resolves to the same commit and is the comparison
baseline for the implementation slices below.

### Verified desktop and refactor state

1. **The normalized local model is not sync-ready yet.**
   `database/reference_library_schema.py` stores domain `revision` on works,
   treatments, and measurement sets, but none of the four entity types has a
   cloud `row_version`, accepted baseline, dirty/conflict state, `deleted_at`,
   or durable deletion ledger. An observation use's `reference_revision` is
   the frozen measurement-set revision; it is not an attachment version.
2. **Current deletion is physical.** `ReferenceWorkRepository`,
   `TaxonTreatmentRepository`, and `MeasurementSetRepository` explicitly hard
   delete child-first after in-use checks; `ObservationReferenceUseRepository`
   detaches with `DELETE`. The observation FK also cascades use deletion.
   Therefore changing only the explicit detach method would still lose
   deletion intent when an observation is deleted.
3. **Useful local idempotency is narrower than cloud idempotency.** Attach is
   idempotent for `(observation_id, reference_measurement_set_id)` and archive
   import compares domain revisions. Neither behavior supplies cloud CAS for
   role/note changes, snapshot refresh, successor adoption, tombstones, or
   retries after a lost response.
4. **The graph spans two SQLite databases.** Works, treatments, and sets live
   in `reference_values.db`; uses live in `mushrooms.db`. There is no atomic
   cross-database FK or transaction. Graph planning and recovery must tolerate
   an interruption between parent and child acknowledgements while never
   discarding a frozen use snapshot.
5. **Current cloud orchestration has a stable compatibility boundary.**
   `sync_all` owns account binding and top-level mode selection; `push_all` and
   `pull_all` own the observation pipeline. Pull-only shares the pull engine
   through `PullOnlyCloudClient`. The independent cloud-sync extraction plan
   keeps `utils/cloud_sync.py` as a facade and has not yet started its
   mechanical extraction stages.
6. **Reference sync is a separate graph, not an observation child cursor.**
   Its owner should be a sibling subsystem such as
   `utils/reference_cloud_sync.py`, with a narrow facade called by `sync_all`.
   It must not be distributed through image/measurement loops or overload the
   existing observation child-change cursor and snapshot store.

The existing contracts to preserve are account-binding fail-closed behavior,
strict zero-write pull-only mode, complete deterministic pagination, no
deletion inference from absence, caller-mode independence for `sync_images`,
`materialize_remote_images`, and `full_pull`, the observation no-op fast path,
and retry visibility when required work fails. Helper nesting, progress
percentages, the monolithic file shape, early observation `synced` stamping,
and settings JSON as the storage mechanism are implementation details rather
than Stage 4 compatibility promises.

### Verified Stage 3 owner-sync API

The cloud graph is work → treatment → measurement set → observation use.
Owner reads are direct RLS-protected table reads, including tombstones; there
is no normalized-reference read RPC. Every table has an owner change-feed
index ending in `(updated_at, id)`, so desktop reads must paginate completely
with `updated_at.asc,id.asc` and never treat a missing row as deletion.

The four mutation RPCs are:

- `sync_reference_work(jsonb, bigint)`;
- `sync_reference_taxon_treatment(jsonb, bigint)`;
- `sync_reference_measurement_set(jsonb, bigint)`;
- `sync_observation_reference_use(jsonb, bigint, text)`.

They return `{status, row}`. Create uses expected `row_version = 0`; every
update, restore, tombstone, explicit snapshot refresh, role/note change, and
successor adoption uses the current positive token. A successful state change
increments it exactly once. A semantically identical retry returns
`no_change` without timestamp or version churn. A stale token returns
`conflict` and never overwrites; the current row is normally returned but may
be null for missing-row or uniqueness conflicts. Domain `revision` is separate,
nondecreasing content state and may jump after offline edits.

Payloads are strict allowlists. Desktop must not send owner identity,
timestamps, `row_version`, `deleted_at`, or local-only provenance. It requests
a tombstone with `deleted: true`. Treatment and set parents cannot be changed
in place. Set successor cycles and live forks are rejected. Use writes require
the verified cloud observation bigint; the local observation integer is never
uploaded or used as recovery identity.

`historical_import` is create-only and exists solely for the first upload of
an already-frozen local use. It accepts an older, never future, source revision
after validating the same-owner graph and snapshot shape. All later attach,
reattach, refresh, and successor changes use `current`. Detach retries do not
depend on a still-live source graph. Reattach restores the same use UUID and
does not create a replacement attachment.

### Local sync-state and deletion design

Add transport state without overloading domain fields:

- a nullable last-acknowledged cloud `row_version` for every entity, including
  observation uses;
- a canonical last-accepted baseline payload (or equivalent normalized
  baseline record) sufficient for local-only/remote-only/overlapping-change
  classification;
- explicit dirty, retry, and conflict state owned by the reference-sync
  subsystem, not inferred only from timestamps;
- a durable deletion-intent/tombstone ledger in the database that owns the
  entity: library entities in `reference_values.db`, uses in `mushrooms.db`;
- owner/account binding, expected cloud row version, deletion time, and the
  dependency identities needed after the live row disappears. A use tombstone
  must retain its use UUID, set UUID, and verified observation cloud ID.

The canonical observation-delete path must record use tombstones before the
current FK cascade can remove live rows. All observation deletion callers need
coverage. If that boundary cannot be made exhaustive, use a same-database
SQLite trigger to capture cascade deletions; do not rely on callers that happen
to use `detach`. The current UI starts remote observation deletion concurrently
with local deletion, while the remote hard delete cascades normalized uses.
Stage 4 must replace that race with one ordered owner: either tombstone and
acknowledge live remote uses before deleting the remote observation, or treat
an explicitly successful remote parent deletion as terminal acknowledgement
for its use intents. A complete authoritative owner read that proves the
already-linked parent is absent may provide the same terminal result; a
partial/failed read may not. Local deletion follows only after the needed
identities and intents are durable.

Deletion intent also needs an explicit local-only cancellation state. If a row
is created and deleted before any cloud create attempt or accepted baseline,
there is no remote row to tombstone and the intent can be removed locally once
its dependants are handled. If a create request may have been sent but its
response was lost, first reconcile the same UUID with an authoritative owner
read: tombstone the returned row with its current token, or resolve the intent
when the complete read proves it absent. Never send `deleted: true` with
expected version zero and retain it forever—the Stage 3 RPC correctly rejects
deletion of a row that never existed. Tombstones otherwise remain until cloud
acknowledgement, successful parent hard deletion, or authoritative absence
proof and successful dependent processing. They are backed up with their
owning databases but are device/account sync state and are excluded from
portable library exchange unless a later contract explicitly changes that
policy.

Persist every successful returned row and `row_version` immediately in the
owning database transaction. Because the graph crosses databases, graph-level
progress is resumable, not atomic: after a crash the planner re-reads durable
state, skips acknowledged prerequisites, and retries the same stable UUID.
Never advance an entity baseline merely because a request was sent.

### Ordering, pull, retries, and conflicts

Creates and restores run parent-first:

```text
work -> treatment -> measurement set -> observation use
```

Tombstones run child-first:

```text
observation use -> measurement set -> treatment -> work
```

A child remains dependency-blocked and retryable until its parent is live and
acknowledged. A use additionally waits for a verified local
`observations.cloud_id`; a missing or disputed link is not a create signal.
Pull stages all four complete feeds, validates the graph, applies live rows
parent-first and tombstones child-first, and advances per-table cursors and
baselines only after the complete graph application succeeds. A partial,
bounded, failed, or filtered read never proves deletion.

Retry the identical UUID, payload, and expected token after transport failure.
`created`, `updated`, and `no_change` acknowledge the returned baseline.
`conflict` persists explicit review state for that entity/graph while unrelated
graphs continue. Dependency/validation failures remain retryable or blocked as
typed outcomes and do not advance baselines. Automatic merge is limited to
non-overlapping local/remote changes proven against the accepted baseline;
overlapping edits, identity disagreement, snapshot replacement, and successor
adoption require explicit resolution. Cloud never silently replaces a newer
local domain revision.

Pull-only mode includes reference owner reads but source-gates every reference
writer. Each new writer must be added to `_PULL_ONLY_BLOCKED_CLIENT_METHODS`;
each new read is added to `_PULL_ONLY_ALLOWED_READ_METHODS` only deliberately.
A blocked write attempt remains a bug. Reference metadata sync does not alter
the existing media flags or enable a full observation pull.

### Stage 4 implementation sequence

Each item below is one reviewable, independently revertible commit. Do not
combine persistence migration, network wiring, or orchestration replacement.

1. **Stage 4a — characterization and seam.** Add focused tests for the existing
   `sync_all` call/result contract, pull-only boundary, caller modes, and no-op
   path. Add a no-op reference-sync facade/result that the coordinator can
   merge without changing existing observation/media/calibration behavior.
   Compare outputs with `cloud-sync-pre-refactor`.
2. **Stage 4b — additive local transport state.** Add cloud row versions,
   accepted baselines, dirty/conflict state, and durable tombstone ledgers for
   all four entity types. Cover upgrades, round trips, backup retention,
   portable-export exclusion, account binding, and the observation-delete
   cascade boundary. Model never-attempted, create-outcome-unknown, and
   acknowledged remote identity distinctly. No network calls yet.
3. **Stage 4c — mutation ownership and graph planner.** Route normalized CRUD,
   detach, snapshot refresh, successor adoption, and observation deletion
   through helpers that atomically record sync intent in the owning database.
   Add a pure deterministic planner with parent-first live work, child-first
   tombstones, missing-observation-cloud-ID blocking, and restart tests.
4. **Stage 4d — typed remote adapter.** Add four strict RPC wrappers and four
   completely paginated owner readers, structured result parsing, deterministic
   ordering, and explicit pull-only allow/block registration. Test payload
   allowlists, pagination, status mapping, and zero-write download mode with a
   fake client; do not invoke the adapter from production orchestration yet.
5. **Stage 4e — library push executor.** Behind the facade, push/restore works,
   treatments, and sets parent-first and tombstone them child-first. Persist
   each acknowledgement, suppress exact no-op retries, isolate conflicts, and
   keep failed descendants retryable. Observation uses remain disabled.
6. **Stage 4f — whole-graph pull/reconciliation.** Stage complete feeds for the
   three library tables, validate dependencies, apply live rows parent-first
   and tombstones child-first, and commit cursors/baselines only after success.
   Cover offline edits, stale tokens, non-overlapping auto-merge, overlapping
   conflict, interrupted pagination, and restart convergence.
7. **Stage 4g — observation-use sync.** Add uses last, after verified observation
   cloud identity and source convergence. Cover first `historical_import`,
   current attach/reattach, role/note edits, explicit refresh, successor
   adoption, detach, observation deletion, dangling sources, uniqueness races,
   and frozen-snapshot preservation. Include attach→detach-before-sync,
   library create→delete-before-sync, lost-create-response reconciliation, and
   observation deletion before first reference push. Serialize the existing
   local/remote observation-delete race and verify both tombstone-first and
   acknowledged-parent-delete terminal paths.
8. **Stage 4h — enable and compare.** Enable the facade in normal and pull-only
   coordinator modes, merge typed reference outcomes into the compatible
   result surface, and run the existing fast-path, dirty-loop, child-cursor,
   download-only, image/media policy, and caller-mode suites plus new reference
   integration tests. Compare pre-existing outputs and cloud writes against
   `cloud-sync-pre-refactor`; only intentional normalized-reference operations
   may differ.

This sequence may proceed alongside the cloud-sync extraction plan because the
reference graph has an explicit sibling owner and a narrow facade. Mechanical
movement of `push_all`, `pull_all`, or `sync_all` must not be mixed into a
Stage 4 behavior commit. If the extraction reaches its orchestration checkpoint
first, the reference facade becomes another typed executor; if Stage 4 lands
first, the extraction preserves that facade as an existing owner.

### Required Stage 4 verification

In addition to focused schema/repository/adapter/planner tests, every wiring
change runs the exact caller-mode tests and the cloud safety suites required by
the repository invariants, including `tests/test_cloud_sync_fast_path.py`,
`tests/test_cloud_sync_dirty_loop_steady_state.py`,
`tests/test_child_change_probe.py`, `tests/test_cloud_download_only.py`,
`tests/test_image_tombstones.py`, and the affected media pull/upload policy
tests. Update both copies of `docs/supabase-sync-contract.md` when behavior
lands, and update `docs/cloud-sync-architecture.md` when ownership or
navigation changes. The audit-only commit does not change either contract.

### Landed Stage 4a slice: characterization and dormant seam (2026-08-28)

Stage 4a adds `utils/reference_cloud_sync.py` as the dependency-light sibling
owner for normalized reference sync. Its frozen typed `ReferenceSyncResult`,
side-effect-free `sync_reference_library` facade, and empty-result merge helper
create the boundary needed by later slices without importing
`utils.cloud_sync`, reading local state, inspecting the cloud client, or making
network calls. The merge helper accepts only the empty Stage 4a result and
fails closed on a premature non-empty result so future behavior cannot be
silently discarded.

Production `sync_all` does not invoke the facade yet. Enabling normal and
pull-only orchestration remains explicitly owned by Stage 4h, after durable
state, transport, push, pull, and observation-use semantics exist. This keeps
the executable observation/media/calibration behavior byte-for-byte comparable
with `cloud-sync-pre-refactor` during the intervening slices.

`tests/test_reference_cloud_sync_coordinator.py` characterizes the existing
normal result shape and error ordering, direct flag forwarding, complete
prefetch/push/pull phase order, proven no-op remote-list reuse, and strict
pull-only result/push suppression. Existing Observations-tab tests continue to
characterize the production refresh, background, and Sync-now caller modes. The
new tests also prove that the Stage 4a facade does not inspect its client, that
an empty reference result preserves the legacy result object and shape, and
that non-empty results cannot be ignored accidentally.

The executable comparison used both structural and behavioral evidence:
`utils/cloud_sync.py` and the pre-existing sync tests are unchanged from
`cloud-sync-pre-refactor`, while the new literal result/ordering assertions run
against that unchanged coordinator. Stage 4a adds no import or call from the
coordinator to the dormant facade.

The focused Stage 4a and existing coordinator/caller-mode suite passed. The
broader required regression selection exposed one pre-existing deterministic
failure in `test_cloud_media_materialization_state_detects_missing_and_ready_media`:
the unchanged `cloud_sync.py` baseline references undefined
`suppress_reverse_identity`. Stage 4a does not alter that unrelated media path;
the failure is recorded rather than repaired in this narrow slice.

### Landed Stage 4b slice: additive local transport state (2026-08-28)

Stage 4b adds dormant, account-bound transport state without importing or
calling the cloud client. The reference database owns
`reference_cloud_sync_state` and `reference_cloud_tombstones` for works,
treatments, and measurement sets. The observation database owns
`observation_reference_use_cloud_sync_state` and
`observation_reference_use_cloud_tombstones`. State records keep the accepted
remote payload, positive remote `row_version`, dirty/retry/conflict status,
retry diagnostics, and the bound cloud user separately from domain rows.
Remote identity is explicit: `never_attempted`, `create_outcome_unknown`, or
`acknowledged`; constraints prevent an unknown create from pretending to have
an accepted baseline and require acknowledged rows to have both a positive
version and baseline.

Schema initialization creates and backfills state rows idempotently. Insert
triggers cover later domain inserts. Deletion triggers atomically cancel
never-attempted local rows or retain remotely plausible deletion intent,
including dependency IDs and the last accepted baseline. A parent-observation
`BEFORE DELETE` trigger captures each attached reference use and the
observation cloud ID before SQLite cascades remove the uses; direct detach uses
the same durable ledger. A remotely plausible use with no verified observation
cloud ID fails deletion closed. Transaction rollback restores both the domain
row and its deletion intent.

`database/reference_sync_state.py` is the repository boundary for round trips,
account-binding checks, canonical payload storage, account-scoped tombstone
inspection, and local resolution. It deliberately owns no mutation
orchestration, graph planning, or network behavior; those begin in Stage 4c and
later slices.

Full backups retain all four transport tables and restore pre-Stage-4b backups
through the existing staged additive migration. Modern portable exports clear
the state after domain pruning so delete triggers cannot leak newly generated
tombstones. Legacy bundles stage and sanitize the reference database rather
than archiving the live file, and remain compatible with reference databases
that predate the new tables. The modern portable archive inventory also
explicitly excludes reference measurement-set UI preferences, matching their
established machine-local role.

Focused schema, repository, delete-cascade, backup/restore, portable-export,
legacy-bundle, and existing reference-library regressions passed. The existing
observation/media coordinator remains unwired to the dormant facade and state,
so Stage 4b introduces no cloud requests or sync-result changes.

### Landed Stage 4c slice: mutation ownership and graph planner (2026-08-28)

The existing repositories in `database/reference_library.py` remain the one
application-facing owner for normalized reference mutations. Their work,
treatment, measurement-set, role/note, snapshot-refresh, and successor-adoption
updates now record transport intent through connection-scoped helpers before
the owning transaction commits. The helpers retain account binding, remote
identity, accepted baseline, and row version; reset obsolete retry diagnostics;
and preserve an unresolved conflict until an explicit resolution. Existing
insert/delete triggers continue to own atomic create intent, local-only delete
cancellation, detach tombstones, and observation-delete cascade capture.
Idempotent attach and semantic no-op snapshot refresh remain true no-ops.

The legacy bundle and modern portable-import revision-upgrade paths are the two
intentional repository bypasses. They call the same connection-scoped helpers
inside their existing transactions, so imported higher revisions cannot remain
incorrectly clean. Export/pruning and restore machinery remain outside mutation
ownership because they intentionally preserve or remove transport state as
specified by their archive contracts.

`database/reference_sync_planner.py` adds a read-only graph snapshot loader and
a pure immutable planner. Live work is ordered work → treatment → measurement
set → observation use; tombstones are ordered observation use → measurement set
→ treatment → work, with UUID tie-breaking. Children remain blocked until the
durable parent state is acknowledged, ancestor tombstones remain blocked while
descendant intents exist, and observation uses require a verified observation
cloud ID. Conflicts and account mismatches are explicit blocked outcomes while
unrelated graph work remains eligible. Replanning reloads durable state, making
progress deterministic across process restarts.

Stage 4c does not import a cloud client, execute RPCs, call the dormant facade,
or change `sync_all`. Typed remote transport remains Stage 4d work.

### Landed Stage 4d slice: typed remote adapter (2026-08-28)

Stage 4d adds `utils/reference_cloud_adapter.py` as the strict typed boundary
over the Stage 3 Supabase contract. It accepts only the documented payload
keys, validates nonnegative CAS tokens and tombstone preconditions, and calls
the four named mutation methods with the exact RPC parameter names. Structured
responses are parsed only from the exact `{status, row}` envelope. Successful
rows must retain the requested UUID, authenticated owner, and a positive
`row_version`; tombstone acknowledgements must include `deleted_at`.

The adapter maps `created`, `updated`, and `no_change` to acknowledged results;
keeps `conflict` explicit; distinguishes dependency blocks, validation
rejections, and account deletion; and rejects unknown or malformed responses.
Authentication, account mismatch, transient transport failure, and terminal
transport failure remain distinct typed errors. These classifications expose
transport facts only: retry scheduling, graph ordering, merge policy, and
state persistence remain outside the adapter.

`SporelyCloudClient` now provides four named RPC writers and four direct
owner-table readers. Every reader uses complete `_get_paginated` traversal with
an explicit owner filter, an allowlisted projection, and deterministic
`updated_at.asc,id.asc` ordering. The writer names are explicitly blocked by
`PullOnlyCloudClient`; the reader names are explicitly allowed. This closes the
generic `_rpc` source-gating ambiguity for future reference execution.

The adapter and client methods remain dormant: Stage 4d adds no `sync_all`
call, planner execution, local-state persistence, or automatic retry. Library
push execution begins in Stage 4e.

### Landed Stage 4e slice: library push executor (2026-08-29)

The dormant `sync_reference_library` facade now executes only library work,
taxon-treatment, and measurement-set mutations. It repeatedly rebuilds the
durable graph plan after each outcome, so live mutations become eligible in
work → treatment → measurement-set order and tombstones become eligible in
measurement-set → treatment → work order. Observation-reference-use live rows
and tombstones are deliberately ignored until Stage 4g, and `sync_all` remains
unwired until Stage 4h. A successor measurement set additionally waits for its
`supersedes_id` row to be acknowledged, independent of UUID sort order.

Every first create is durably marked `create_outcome_unknown` before its RPC.
After a lost response, the executor performs a complete typed owner read for
that entity table: a matching row supplies the authoritative token, while
confirmed absence permits retry of the same UUID and payload with expected
version zero. Unknown tombstones use the same complete-read rule and are never
sent with version zero. Recreated same-ID library rows atomically inherit the
old tombstone's token/baseline and remove that ledger row before the restore
RPC, making a crash between those steps restart-safe.

`created`, `updated`, and `no_change` persist the returned positive
`row_version` and a canonical allowlisted baseline immediately. The
acknowledgement transaction compares the current domain payload with the sent
payload, preserving dirty intent if the row changed while the RPC was in
flight. Exact baseline-equal dirty rows become clean without a cloud call.
If a row is deleted or recreated while an RPC is in flight, acknowledgement
atomically transfers to the durable tombstone or the replacement live row so a
stale delete token cannot survive the transition.
Conflicts retain their prior token/baseline and store structured review data;
retryable and terminal failures retain durable intent and are classified in
the typed result. An attempted item runs at most once per invocation, while
unrelated graph branches continue and a later invocation resumes from the
persisted boundary.

### Landed Stage 4f slice: whole-graph pull/reconciliation (2026-08-29)

The dormant reference facade now reads the complete owner feeds for works,
taxon treatments, and measurement sets through the Stage 4d typed adapter
before making any local change. It validates identities, owners, required
payload fields, domain and transport versions, live parent dependencies, and
measurement-set successor chains in memory. A failed page/read, malformed row,
duplicate identity, missing dependency, successor fork/cycle, or stale remote
token leaves domain rows, transport baselines, tombstone state, and cursors
unchanged. Observation-reference-use feeds remain Stage 4g work.

The validated graph is reconciled in one `BEGIN IMMEDIATE` transaction in
`reference_values.db`. Live rows apply work → treatment → measurement set;
explicit remote tombstones apply measurement set → treatment → work. The
transaction re-reads local payload and transport state so edits made after
network staging are not silently overwritten. Accepted baselines drive a
three-way comparison: remote-only changes apply cleanly, local-only changes
remain dirty with the current remote CAS token, disjoint changes merge and
remain dirty for the Stage 4e executor, and overlapping/identity/delete
divergence becomes durable conflict state. A conflicted dependency blocks its
live descendants while unrelated branches continue.

Per-account, per-table `(updated_at,id)` cursors advance over every inspected
row, including tombstones, only in the successful whole-graph commit. Explicit
remote tombstones never echo into outbound deletion intent. A separate durable
remote-tombstone marker retains the positive row version and canonical deleted
baseline needed to restore a subsequently recreated stable UUID; repeated
pulls are no-ops. The direct facade performs reconciliation before its dormant
library push phase, preventing stale CAS writes and allowing a proven
non-overlapping merge to be pushed safely. `sync_all`, observation reference
uses, observation/media cursors, and sync mode flags remain unchanged until
Stages 4g and 4h.

### Landed Stage 4g slice: observation-reference-use sync (2026-08-29)

The dormant facade now stages all four complete owner feeds before any local
application. It reconciles the library graph first, then observation uses only
after the referenced measurement set is account-bound, acknowledged, and
clean and the remote observation bigint maps to exactly one local observation.
Malformed or failed use reads prevent every graph apply. Missing dependencies,
identity ambiguity, and recorded conflicts block the affected use without
inventing rows or treating feed absence as deletion; library cursors are held
until the full four-feed reconciliation is unblocked.

Use transport state now has an idempotent per-owner `(updated_at,id)` pull
cursor and durable remote-tombstone marker. Pull imports the server UUID,
role, note, selected time, reference revision, and stored snapshot verbatim;
it never invokes a snapshot builder. Accepted baselines provide restart-safe
three-way reconciliation. Remote-only changes apply cleanly, local-only
changes keep their intent with the current CAS token, disjoint role/note edits
merge, and overlapping or structural/snapshot divergence becomes durable
conflict state. Explicit remote tombstones remove only baseline-equal local
rows and do not echo an outbound delete; pending local edits remain intact.

Push execution now extends the Stage 4c graph order to live
work → treatment → measurement set → observation use and tombstones in the
reverse order. First use creation is durably marked unknown before the
`historical_import` RPC; every acknowledged update, restore, role/note change,
explicit snapshot refresh, successor adoption, and delete uses `current` with
a positive saved `row_version`. Lost responses are reconciled through a
complete owner read before retrying the identical UUID and payload. Local
transport state advances only from an acknowledged response or an exact
authoritative read, while conflicts and classified failures remain durable.

Detach/reattach of the same observation/set pair reuses the tombstoned use UUID
and atomically claims its saved token. Attach→detach and observation deletion
before first sync leave no remote delete intent. Observation deletion now
commits the local cascade and its child-use tombstones before starting the
remote parent delete, removing the former local/remote race. A confirmed
parent deletion resolves those child tombstones as terminal; if the parent
delete fails, the durable tombstones remain eligible for normal child-first
execution. The reference facade remains absent from `sync_all`; Stage 4h owns
that activation and old-vs-new behavior comparison.

### Stage 4h activation and verification status (2026-08-29)

The normalized-reference facade is now invoked by both production coordinator
modes after the legacy observation pull has established observation cloud
identities. Normal sync completes the existing calibration pull and then runs
the whole-graph pull/reconciliation followed by the deterministic CAS executor.
Download from Cloud passes the fail-closed `PullOnlyCloudClient` and an explicit
`pull_only=True` source gate; the facade performs the four complete owner reads
and local reconciliation, then returns without planning or attempting writes.

Reference outcomes are additive under `result["reference_sync"]`; the existing
top-level observation and calibration counts retain their old meaning.
Reference transport errors, conflicts, and dependency blocks also enter the
existing top-level error channel. Reference-only pushes or pulls count as real
sync activity and trigger an observations-tab refresh so restored attachments
and plots become visible. No media flags, observation/calibration sequencing,
child-cursor semantics, or caller-mode arguments changed.

The final reference/observation-use suite passes 448 tests. The required
legacy fast-path, dirty-loop, child-cursor, download-only, tombstone,
image/media-policy, and caller-mode suite passes 258 tests after deselecting the
one documented baseline defect. The representative
old-vs-new suite was run against frozen baseline `e9accd9` and the Stage 4h
working tree: both produced 258 passes and the same single failure in
`test_cloud_media_materialization_state_detects_missing_and_ready_media`.
That failure is a pre-baseline `NameError` for undefined
`suppress_reverse_identity`, not a Stage 4 regression. Literal legacy
coordinator result/order/caller-mode characterizations also pass identically.
The only intended production deltas are the four complete reference owner
reads in either coordinator mode, CAS reference writers in normal mode only,
the additive typed result, and Stage 4g's tombstone-first observation-delete
ordering.

The first live `sync-test-a` run created and uploaded the work and treatment,
but measurement set `57f47e04-59c9-4fcf-9a30-3eb1d42faabe` returned the Stage
3 domain status `invalid_payload`; its observation use then correctly remained
blocked with `parent_not_acknowledged`. The exact local range payload contained
`"raw_points_json": null`. `sync_reference_measurement_set` inserts that field
with `p_payload->'raw_points_json'`, so JSON `null` did not become SQL `NULL`
and violated the cloud constraint requiring either SQL `NULL` or a JSON array.
The typed adapter now omits this optional field for a create while preserving
genuine arrays and explicit null on acknowledged updates (which must still be
able to clear a prior array). Executor errors and blocked outcomes retain the
exact RPC domain status instead of collapsing it to `remote rejected mutation`
or a bare entity ID. Focused regressions reproduce the live UUID, values, and
domain rejection.

The repaired `create_outcome_unknown` row then converged on retry without a new
identity. The completed disposable-profile gate verified create/retry,
profile A → cloud → profile B pull, pull-only zero-write behavior, CAS update,
an overlapping reference edit becoming a durable conflict while preserving
the cloud row, conflict persistence across restart, and observation-use
detach/tombstone reconciliation through a fresh-profile pull and restart.
Dependency ordering, frozen evidence, tombstones, restart safety, and normal
versus pull-only mode boundaries therefore passed the live activation gate.

Stage 4 is complete. The pre-existing media NameError remains a separately
scoped baseline defect and is not part of the reference-sync activation.

### Stage 5 public read-model and rendering status (2026-08-29)

Stage 5 is implemented on the `sporely-landing` `refactor/cloud-sync` branch
at `5af3cb8`. The existing `get_public_observation` detail read remains
unchanged; the landing client composes it with the dedicated Stage 3
`get_public_observation_references` RPC only after the observation itself is
publicly readable. Search/card reads do not incur the extra request.

The browser read model independently enforces the exact Stage 3 item,
snapshot, measurement, method, UUID, role, schema-version, and revision
contract. Invalid or unsupported items are omitted independently, so a bad
attachment cannot expose extra fields or suppress valid siblings. The client
does not read or join `reference_works`, `reference_taxon_treatments`, or
`reference_measurement_sets`; owner notes and mutable library state therefore
cannot enter the public model. Normalization copies the frozen values and
preserves `raw_text` byte-for-byte, so later private-library edits cannot
rewrite historical public evidence.

Observation detail now draws each attached source separately. Supplied core
L × W bounds are translucent dashed rectangles, supplied exceptional bounds
are an outer outline with whiskers, supplied paired means are `+` markers, and
only genuine paired raw points become diamond marks. Literature values expand
the plot domain but are never converted into synthetic observations,
distributions, sample sizes, or merged ranges. Non-drawable but valid
snapshots still retain their citation card.

The localized “Compared with literature” section displays the source label,
full citation, locator, name as published, exact raw expression, supplied
method fields, and attachment role. It is absent when no valid attachment is
present. Semantic section/article/definition-list markup, SVG labels,
shape/dash distinctions, wrapping cards, and a narrow-screen stacked layout
cover the Stage 5 accessibility and responsive requirements.

Verification used contract fixtures copied from the deployed Stage 3 shape:
the complete `sporely-landing` Vitest suite passes (49 files, 549 tests),
TypeScript type checking passes, and `git diff --check` is clean. The Stage 3
transactional public-projection regression also passes against the local
Supabase stack, covering visibility/draft/ban/block gates, strict allowlists,
owner-note exclusion, tombstones, immutable frozen evidence, grants, and
private-table denial. Fresh review found one raw-expression trimming defect;
the implementation now preserves that field exactly and has a focused
regression. The follow-up review found no remaining material issues.

Stage 5 is complete. Species-level curated references, public catalogue
search, and adding literature sets in Compare remain deliberately deferred to
Stage 6.

## 19f. Stage 6 curated-library audit and implementation contract (2026-08-29)

This section is the authoritative Stage 6 contract. The audit was read-only:
it changed no production behavior, schema, migration, or deployment.

### Verified current state and ownership

- `sporely-py` `refactor/cloud-sync` is clean at `edb5c80`. Its normalized
  work → treatment → measurement-set graph is owner-private. Stable UUIDs and
  semantic revisions are separate from transport `row_version`; attachments
  synchronously store a canonical frozen snapshot before they are plotted.
- The local `reference_works.visibility` and `verification_status` columns are
  compatibility residue. Current repositories deliberately omit them. Stage 6
  must not revive either column or reinterpret `curated_public` on a personal
  row.
- `sporely-web` has the Stage 3 private graph and public frozen-observation
  projection at `68c406d`. Personal tables have composite owner keys,
  owner-only RLS, same-owner parent/use constraints, and no anonymous table
  reads. `reference_taxon_treatments.taxon_id` is free text and is not the
  cloud taxonomy identity.
- The taxonomy-v2 tables are a disposable experiment and are not the Stage 6
  identity boundary. The deployed sparse `taxonomy_v3.registry_concept`
  supplies the current durable positive integer `sporely_taxon_id`, rank, and
  current `canonical_name`. Public curated lookup must use that registry key,
  not desktop text IDs, display names, legacy external IDs, the v2 tables, or
  inferred slug equality.
- Stage 6a verified a migration-safety detail that the v3 registry itself does
  not enforce: `taxonomy_v3.registry_concept.sporely_taxon_id` is an integer
  primary key but has no positive-value check. Curated assignment rows must
  therefore enforce both their v3 foreign key and their own
  `sporely_taxon_id > 0` constraint; tests seed zero/negative registry rows so
  a missing curated check cannot be masked by the foreign key.
- Stage 5 landing code is on `sporely-landing` `refactor/cloud-sync` at
  `5af3cb8`. It has the strict frozen-snapshot parser and non-synthetic plot
  grammar needed by Stage 6. Compare is currently a browser-local
  `sporely.compare.v1` array whose `CompareSet.sourceType` is limited to
  `observation | taxon_filter`; its permissive legacy loader would misclassify
  a new kind without an explicit storage migration.
- The removed mock `SpeciesReferenceDistribution` path generates artificial
  point clouds and is not a Stage 6 input. The legacy ownerless
  `reference_values` table/RPC is also not a curated catalogue or migration
  source.
- Curator UI belongs in `sporely-admin`, not the public landing site or mobile
  application. Its current admin boundary verifies the JWT with `getUser`,
  checks server-side authorization, uses the service credential only inside an
  Edge Function, allowlists actions, and records an admin audit log. Stage 6
  extends that pattern through a separate reference-curation endpoint. The
  existing checkout has unrelated local edits and must not be reused without
  isolation.

### Private, submitted, and curated domains

There are three distinct domains; no row changes domain in place:

1. **Personal library.** Existing `public.reference_works`,
   `reference_taxon_treatments`, and `reference_measurement_sets` remain
   readable only by their owner. Personal sync and observation snapshots keep
   their Stage 3/4 contracts.
2. **Submission candidate.** An authenticated, non-banned owner may explicitly
   submit one synced measurement-set bundle and its two required parents. The
   submission RPC accepts the personal measurement-set UUID plus expected
   work/treatment/set revisions and consent/rights attestations; it does not
   trust an arbitrary client-supplied graph. The server verifies `auth.uid()`
   ownership and copies only the curated-field allowlist into an immutable
   candidate payload and content hash. It excludes personal citation keys,
   favourites/recent state, private notes, observation-use notes, sync state,
   and unrelated siblings beneath the same work. Later personal edits do not
   mutate the candidate. Unsynced local data must sync before submission.
   Candidate content is append-only and versioned: responding to
   `changes_requested` server-reads the graph again at new expected revisions
   and appends a new candidate revision/hash/attestation instead of editing the
   reviewed payload. Every review decision binds one exact candidate revision
   and content hash.
3. **Curated catalogue.** Acceptance creates or explicitly links to separate
   global curated work/treatment/set UUIDs. It never reuses or transfers the
   personal UUIDs and never grants curators general access to personal tables.
   The private origin and submitter association remain moderation-only and are
   not part of any public projection. Admin-authored catalogue drafts use the
   same review and publication machinery without a personal source.

The initial cloud schema uses the following unexposed tables, with all names
schema-qualified under `private`: `curated_reference_works`,
`curated_reference_taxon_treatments`, `curated_reference_measurement_sets`,
`curated_reference_treatment_taxa`, `curated_reference_publications`,
`curated_reference_publication_taxa`, `reference_curator_memberships`,
`reference_curation_submissions`,
`reference_curation_submission_versions`, `reference_curation_reports`, and
`reference_curation_events`. The first three are editable catalogue heads, the
treatment-assignment table contains only explicit stable-concept links, the
publication-assignment table freezes those links per bundle, publications,
publication assignments, candidate versions, and events are append-only, and
submissions keep immutable candidate content separate from their mutable
workflow state.

The idempotency key for an initial submission is `(owner_id,
source_measurement_set_id, source_work_revision, source_treatment_revision,
source_measurement_set_revision, content_hash, attestation_version)`. An exact
retry returns the existing submission and candidate revision, including its
current terminal status, rather than duplicating either row. Resubmission after
`changes_requested` appends a version under the same submission with expected
workflow `row_version`. A rejected or contributor-withdrawn submission is
terminal; a later submission must have changed content or attestation, gets a
new submission UUID, and records the prior submission UUID. Identical terminal
content is returned, not reopened.

Exact DOI, normalized ISBN, or exact structured bibliographic equality may
raise duplicate candidates for human review. They never merge records
automatically. Title/author similarity is never an identity rule.

### Curated identity, revisions, and immutable publications

Curated work, treatment, and measurement-set UUIDs are stable entity IDs.
Draft content changes increment the entity's semantic `revision`; mutable
workflow rows also carry a separate CAS `row_version`. A published measurement
set is represented by an immutable publication bundle keyed by
`(curated_measurement_set_id, bundle_revision)` and records the exact work,
treatment, and measurement revisions used to build it.

`bundle_revision` advances whenever any public field in the effective work →
treatment → measurement-set graph changes. Publication materializes:

- canonical snapshot schema v1, with curated UUIDs and
  `reference_revision = bundle_revision`;
- a strict public citation schema v1 sourced from that exact work revision;
- canonical full/short citations;
- exact taxon assignments and publication timestamp.

Citation export is a derived, append-only artifact keyed to that immutable
bundle, not a second bibliographic source. Stage 6f adds and backfills those
artifacts before any landing consumer is activated.

Published bundle rows are append-only. Editing a work used by several sets
marks those sets as needing review but does not silently republish them; each
set is republished explicitly. `row_version` is never exposed as a scientific
revision.

An ordinary correction keeps the curated entity UUID and publishes a new
bundle revision. A replacement or duplicate retirement creates/uses a
different measurement-set UUID and records `supersedes_id`. Supersession must
be acyclic, remain within the same character, have an explicitly compatible
taxon assignment, and permit only one live successor. Stable IDs are never
recycled.

### Taxon assignment and species lookup

`name_as_published` remains immutable source evidence and is never used as the
lookup key. Curators assign a treatment to one or more stable positive
`sporely_taxon_id` values through an explicit assignment table. Multiple
assignments are allowed only as separate audited curator decisions; there is
no descendant expansion, alias fallback, or fuzzy matching.

An assignment may be published only when a positive integer concept exists in
`taxonomy_v3.registry_concept`, its current `rank` is exactly `species`, and
its `canonical_name` is nonblank. `scope_state` and `cache_state` describe
scope/search materialization and are not alternate identities. Hybrids,
aggregates, varieties, forms, and `sensu` concepts retain their printed
wording, but do not appear on a species page until an exact supported registry
assignment exists. Registry maintenance may change the current display name
without changing the stable concept ID or historical `name_as_published`.

Landing obtains the stable concept through an additive field on the public
species read. For a legacy slug aggregation, the server returns a stable ID
only when its visible observations resolve to exactly one non-null
`public.observations.resolved_sporely_taxon_id` and that ID resolves to an
eligible v3 species registry row; zero or multiple distinct IDs produce null.
The field's current display name comes from
`taxonomy_v3.registry_concept.canonical_name`, not the slug. With no stable ID,
landing issues no curated-reference query. The curated RPC accepts only
`sporely_taxon_id`; it never accepts a name, v2 ID, or slug as fallback.

### Moderation and permissions

Keep curator membership in database state, not `user_metadata` or stale JWT
claims. `reference_reviewer` may claim a submission, request changes, reject,
and edit a catalogue draft. `reference_publisher` includes reviewer powers and
may publish, deprecate, supersede, or withdraw. Existing administrators may
manage memberships and perform publisher actions through an explicit database
check; administrator status is not copied into a browser-controlled claim.
Every moderation and membership action re-reads both current role/admin state
and `profiles.is_banned = false`. A banned privileged account has no curation
authority, and a banned account cannot be assigned a role.

Submission lifecycle:

`submitted → in_review → changes_requested → submitted`, or
`in_review → rejected | accepted`; the contributor may withdraw only before
acceptance. Acceptance creates a catalogue draft and does not publish it.

Catalogue lifecycle:

`draft → published → deprecated | withdrawn`. A deprecated item is always
excluded from new species/Compare selection, whether or not a successor
exists, but every published revision remains retrievable by exact ID and
revision. A withdrawn item is removed from catalogue discovery and its content
is removed from public exact reads; exact read returns only a status tombstone
so a stored client can distinguish withdrawal from an unknown ID. Already
frozen observation snapshots and Compare-tray copies are not rewritten.
Restoration requires a new reviewed publication revision, never a backward
status flip.

Every mutation requires an expected `row_version`; publish, reject,
deprecate, supersede, withdraw, role change, and report resolution also require
a reason. Append-only events record actor, time, action, target, before/after
content hashes, and outcome. Published records are platform catalogue content,
so contributor account deletion does not automatically delete a publication.
Membership rows use `ON DELETE CASCADE`; report `reporter_id`, submission
`contributor_id`, and event `actor_user_id` are nullable `ON DELETE SET NULL`.
Before deleting the auth/profile row, the existing `delete-account` workflow
and `private.reference_account_deletions` race guard must revoke memberships,
withdraw open submissions, remove their personal-source pointer, and apply the
configured report/event text-retention policy. Accepted catalogue provenance
keeps only non-account content hashes and submission/public IDs. Every owner
submission RPC checks the same deletion marker, so candidate creation cannot
race account deletion. Stage 6 extends the existing deletion plan and its
ordering tests instead of relying on FK cascade alone.

### Public read contract

Stage 6 adds two narrow, `SECURITY DEFINER` RPCs with empty `search_path`,
schema-qualified access, `PUBLIC` execute revoked, and explicit
`anon`/`authenticated`/`service_role` grants:

- `search_public_curated_reference_sets(p_sporely_taxon_id integer,
  p_limit integer, p_after_published_at timestamptz, p_after_id uuid)` returns
  only the latest selectable published bundle for exact assignments to that
  concept. The limit is capped, ordering is
  `(published_at DESC, curated_measurement_set_id ASC)`, and the cursor uses
  the same tuple. Deprecated, withdrawn, malformed, unresolved, and
  unassigned drafts are absent.
- `get_public_curated_reference_set(p_curated_measurement_set_id uuid,
  p_bundle_revision integer)` returns an exact published or deprecated bundle
  so persisted clients can explain historical selections. A withdrawn match
  returns only its ID, requested bundle revision, `status = withdrawn`,
  `withdrawn_at`, and optional `superseded_by_id`; an unknown ID/revision
  returns no row. Omitting the revision selects the latest published,
  non-withdrawn revision but never silently updates an already stored client
  snapshot.

Each result item is an exact-key envelope containing only:

```json
{
  "curated_measurement_set_id": "uuid",
  "bundle_revision": 3,
  "status": "published",
  "superseded_by_id": null,
  "published_at": "2026-08-29T12:00:00Z",
  "sporely_taxon_id": 12345,
  "canonical_scientific_name": "Russula paludosa",
  "snapshot": { "schema_version": 1 },
  "citation": { "schema_version": 1 },
  "exports": {
    "plain_text": "…",
    "bibtex": "…",
    "csl_json": {}
  }
}
```

The taxon ID remains the v3 registry's positive 32-bit integer through the
JSON/browser boundary. `snapshot` uses the Stage 3 public snapshot allowlist.
`citation` contains the published structured work fields
needed for display/export: immutable citation key, type, ordered authors and
editors, title, container title, year, edition, publisher, place, volume,
issue, pages, DOI, ISBN, URL, language, short label, and full citation. It
contains no owner, submitter, private origin, curator, moderation reason,
audit, timestamps other than publication time, or private notes.

The immutable structured curated work revision is the citation source of
truth. Stage 6f materializes UTF-8 plain text, BibTeX, and CSL-JSON from that
revision; clients download/copy those exact values and never parse
`full_citation` or re-query DOI metadata. Citation keys become immutable at
first publication and are unique case-insensitively in the curated namespace.
RIS is deferred. DOI links are constructed only from validated normalized DOI
text using `https://doi.org/`; ISBN is displayed/exported as text in this
stage.

No public RPC reads personal tables, submission rows, or moderation tables.
The existing observation-reference RPC remains unchanged and continues to
return only deliberately attached frozen snapshots.

### Compare and personal-copy behavior

“Add reference” on `sporely-landing` means add to the browser-local Compare
tray. It does **not** create an `observation_reference_use`, mutate cloud data,
or copy into a personal library. At click time the tray stores the exact
validated curated envelope, snapshot, and `bundle_revision`; later catalogue
edits, deprecation, or network failure cannot rewrite that selection.
Re-adding the same `(curated_measurement_set_id, bundle_revision)` is
idempotent. A newer revision is a distinct choice and requires an explicit
replace action. Deprecated/withdrawn records already in a tray remain locally
inspectable and, after a successful exact-status refresh, labelled; they
cannot be newly selected. Until that refresh, offline clients preserve the
captured content without claiming knowledge of a later status.

Compare storage becomes a versioned envelope with a discriminated union of
`ObservationSet`, `TaxonFilterSet`, and `CuratedReferenceSet`. The migration
reads valid `sporely.compare.v1` arrays once, maps only known legacy shapes,
and writes `sporely.compare.v2`; unknown entries fail closed. Curated sets
never flow through observation hydration, `obsRows`, community observation
counts, or confidence-ellipse fitting. They reuse the Stage 5 literature
geometry: core/exceptional ranges, supplied paired means, and genuine raw
points only. `parmasto` stays citation/table-only until a separately defined
plot contract exists.

Curated records may also be copied into the desktop personal library, but this
is a separate explicit action, not a side effect of landing Compare. Each
imported bundle gets fresh owner-private work/treatment/set UUIDs and a durable
owner-private mapping from `(curated_measurement_set_id, bundle_revision)` to
those IDs. Reimporting the same bundle revision is idempotent. A newer curated
revision creates a new private graph; it never overwrites an edited fork or an
existing observation snapshot. The mapping participates in private sync and
portable backup/import but is excluded from public snapshots. Existing
same-owner attachment logic then creates a frozen observation snapshot in the
normal way. No curated UUID is attached directly to an owner observation.

### `/references` decision

The global `/references` catalogue does not ship in core Stage 6. Species-level
discovery, Compare, and citation export first establish catalogue quality,
moderation capacity, pagination cost, and abuse controls. A searchable route
is a separately approved optional slice after those gates; Stage 6 can be
complete without it. The old synthetic mock adapter and legacy public
reference-values RPC must not back such a route.

### Security, privacy, and abuse controls

- Put catalogue drafts, immutable publication bundles, submissions,
  memberships, reports, and audit events in an unexposed schema. Enable RLS as
  defense in depth and revoke direct `anon`/`authenticated` access. Public
  clients receive only the allowlisted read RPCs above.
- Because current Supabase Data API behavior no longer guarantees automatic
  exposure for new public objects, migrations must explicitly verify function
  grants and API accessibility in addition to RLS. Never expose a service or
  secret key to browser code.
- Contributor submission is authenticated, owner-checked, ban-checked,
  bounded by payload size and per-account rate limits, and accepts only one
  server-read graph. Curator/admin authorization is checked from current
  database membership on every action; it does not depend on refresh-lagged
  JWT metadata.
- Owner submission, resubmission, withdrawal, and report intake are direct
  caller-JWT `SECURITY DEFINER` RPCs with empty `search_path`, fully qualified
  objects, `PUBLIC`/`anon`/`service_role` execution revoked, and execution
  granted only to `authenticated`. They derive owner/reporter solely from
  `auth.uid()` and never accept a client actor UUID. The moderation Edge
  Function alone may use the service role; it binds the audit actor to the ID
  returned by `auth.getUser(token)` and still rechecks current non-banned
  membership/admin state in the database.
- Public and moderation projections use strict key/type/size/version
  allowlists. Text is rendered/exported as text, never HTML. URLs are limited
  to approved schemes. Search limits, deterministic keyset pagination, and
  indexed taxon/status/order columns prevent anonymous amplification.
- Duplicate detection produces review warnings only. It never merges by fuzzy
  title, author, taxon name, or external-ID coincidence.
- Authenticated users may report a curated set with a bounded reason enum and
  text. Reports are rate-limited, audited, and never hide content
  automatically. Publisher withdrawal is the emergency takedown mechanism;
  any removal of already-published observation evidence remains a separate
  observation-moderation decision.
- No scans, PDFs, full book text, or arbitrary HTML are accepted. Submission
  records retain an explicit rights/consent attestation version.

### Bisectable Stage 6 implementation sequence

Each slice begins with failing contract/characterization tests, changes only
the named behavior, updates this canonical plan, receives a fresh review, and
lands as its own commit. No slice may activate the next slice's behavior.

1. **Stage 6a — dormant curated catalogue foundation
   (`sporely-web`; complete at `11637a4`).** Used `supabase migration new
   add_curated_reference_library` to create only the private curated work,
   treatment, measurement-set, taxon-assignment, immutable-publication,
   membership, and event tables. Add lifecycle, CAS, immutability,
   taxonomy-v3 FK plus `sporely_taxon_id > 0` checks, supersession, size, and
   index constraints; revoke direct client access and test fresh/replay
   migration, zero/negative taxon rejection, all role denies, service access,
   account deletion, and event immutability. No submission/report table, RPC,
   Edge, or public behavior.
2. **Stage 6b — owner submission and report intake (`sporely-web`; complete at
   `6372676`).** Created
   the private submission/version/report tables and the caller-JWT RPCs,
   including
   `submit_private_reference_for_curation`, which server-reads exactly one
   owner graph at expected revisions. Add append-only resubmission, withdrawal,
   report intake, exact idempotency, consent, bounds/rate limit, ownership,
   ban/deletion-marker, grants/search-path, and delete-account ordering tests.
   No curator mutation or public read.
3. **Stage 6c — reviewer workflow and catalogue drafts (`sporely-web`;
   complete at `df22cc0`).** Added a
   dedicated `supabase/functions/reference-curation/` Edge boundary for claim,
   request-changes, reject, accept-to-draft, draft editing, and duplicate
   warnings only. Enforce current non-banned database membership/admin checks,
   JWT-to-actor binding, CAS, exact candidate-version decisions, and append-only
   auditing. SQL and Deno tests cover stale roles/tokens, cross-owner IDs,
   retries, races, invalid transitions, and rollback. No publication,
   lifecycle action, or public read was added.
4. **Stage 6d — publisher materialization and lifecycle (`sporely-web`;
   complete at `341b4e1`).** Added
   publisher/admin-only publish, deprecate, supersede, and withdraw operations.
   Transactionally materialize the immutable snapshot/citation bundle and
   exact taxon assignments, enforce graph revision/supersession rules, and
   emit reasoned audit events. Test stale drafts, ancestor edits, republish,
   CAS races, rollback, banned actors, deprecation without a successor,
   successor constraints, status-only withdrawal tombstones, and preservation
   of already frozen evidence. No public RPC.
5. **Stage 6e — curator workspace (`sporely-admin`; complete at `10f923e`,
   with private read support in `sporely-web` at `6f53b5b`).** Add isolated typed
   `reference-curation` API/model modules and a review queue/detail surface to
   the existing admin application. Reviewers can claim, request changes,
   reject, and edit drafts; publishers/admins get separately confirmed
   publish/deprecate/supersede/withdraw actions with required reasons and
   conflict refresh. Add action-builder, stale-response, permission, keyboard,
   focus, escaped-text, and lifecycle rendering tests. Do not reuse the dirty
   current checkout or expose service credentials.
6. **Stage 6f — citation export artifacts (`sporely-web`; complete at
   `ed04e25`).** Add an append-only
   export artifact keyed to each immutable publication bundle and materialize
   UTF-8 plain text, BibTeX, and CSL-JSON solely from its structured curated
   work revision. Backfill/replay deterministically and test Unicode, escaping,
   stable citation keys, exact versions, DOI normalization, hostile text, and
   source changes. No public API or landing behavior; RIS remains deferred.
7. **Stage 6g — exact-taxon public read APIs (`sporely-web`; complete at
   `a2eeea5`).** Add the two
   curated RPCs above plus an additive public species-to-stable-taxonomy
   identity field. Expose the materialized exports in the exact-key envelope.
   Test exact v3 species-rank matching, zero/ambiguous resolved identities,
   multiple explicit assignments, deterministic pagination/ties, limits,
   malformed publication omission, deprecation/supersession, status-only
   withdrawal, grants, RLS bypass hardening, anonymous abuse bounds, and zero
   leakage of personal/moderation fields. Existing observation and legacy
   reference RPCs remain compatible except for the documented additive species
   identity field.
8. **Stage 6h — landing curated API/read model and species listing
   (`sporely-landing`; complete at `478eb92`).** Extract the Stage 5 snapshot
   validator into a shared module without behavior change, then add strict curated-envelope,
   citation-v1, export, cursor, and taxon-identity adapters. Render localized,
   accessible curated cards on the species page only when an exact stable ID
   is returned, with dependency-free copy/download actions for the server
   exports. Use real Stage 6g fixtures; cover malformed/private-expanded items,
   empty/error/loading pages, pagination, deprecated/withdrawn exact reads,
   MIME types, sanitized filenames, DOI safety, and all Stage 5 observation
   regressions. No Compare mutation yet.
9. **Stage 6i — versioned Compare storage (`sporely-landing`).** Refactor
   `src/lib/compareTray.ts` to a versioned v2 envelope and discriminated union,
   with a one-time fail-closed v1 migration. Add storage/round-trip/restart/
   quota tests and characterization tests proving existing observation and
   taxon-filter behavior is unchanged. Curated items remain constructible in
   tests but are not exposed by UI.
10. **Stage 6j — Compare add/render activation (`sporely-landing`).** Add the
   species-card action that captures the exact selected bundle revision,
   explicit revision replacement, literature badge/cards/tables, and Stage 5
   overlays in Compare. Test duplicate clicks, multiple sources remaining
   separate, restart/offline rendering, deprecated stored items, missing
   revisions, no observation RPC hydration, no synthetic points/statistics,
   responsive layout, keyboard/focus behavior, and localized accessible text.
11. **Stage 6k — desktop submission and curated fork
   (`sporely-py`, with owner-private provenance support in `sporely-web`).**
   Add typed public catalogue reads, dormant local/cloud fork-provenance
   persistence, explicit submit/copy actions, and the transaction that creates
   a fresh private graph. Test exact taxon identity, same-revision idempotency,
   newer-revision explicit copies, edited-fork protection, compensation,
   normal frozen attachment, sync A→cloud→B, backup/import, and account
   isolation. Do not add curated rows to owner mutation RPCs or attach them
   directly to observations.
12. **Stage 6l — activation and cross-repository gate.** Exercise contributor
    submission, review, publication, exact species discovery, landing Compare
    persistence, all exports, deprecation/supersession/withdrawal, personal
    fork, offline/restart behavior, and owner/curator/anonymous isolation with
    disposable accounts. Run existing Stage 3–5 privacy, public-observation,
    sync, Compare, and admin regressions. Record intentional differences and
    known limitations here. Do not add `/references` in this slice.

### Stage 6a completion record (2026-08-29)

- `sporely-web` migration `20260829141735_add_curated_reference_library.sql`
  creates only the eight private foundation tables, including the separate
  immutable `curated_reference_publication_taxa` relation needed to preserve
  each publication bundle's exact many-to-many species assignments.
- Mutable heads have stable UUID/creation provenance, semantic revisions, and
  independent CAS `row_version`. Publication revisions are sequential and bind
  locked current work/treatment/set revisions. Exact copied taxonomy-v3 species
  assignments are complete and sealed with the bundle; same-content restoration
  remains legal as a new bundle revision.
- Catalogue rows must enter as drafts. Lifecycle, acyclic single-live-successor
  rules, compatible explicit taxon assignments, and shared graph locking are
  enforced in the foundation. Publication/event history is append-only, and
  service-role privileges omit update, delete, and truncate on those tables.
- All eight tables are in `private`, have RLS enabled with no client policies,
  and deny `PUBLIC`, `anon`, and `authenticated` direct access. Stage 6a adds no
  public RPC, submission/report intake, Edge Function, UI, or public catalogue
  behavior. It was committed but not deployed.
- Verification passed two fresh local database resets (the second as migration
  replay evidence), four focused schema/security/immutability/account-deletion
  SQL suites, the Stage 3 mutation and Stage 5 public-observation reference SQL
  regressions, all 41 delete-account plan tests, `git diff --check`, and
  `supabase db lint --local --level warning --fail-on error`. Lint reported only
  the pre-existing Stage 3 immutable-versus-stable warnings.
- Fresh correctness and security reviews found and closed privilege,
  publication sealing/order, semantic revision, null-rank taxonomy, lifecycle,
  supersession concurrency, and stable-provenance gaps. Final re-reviews found
  no remaining material issue. Stage 6b is the next independently authorized
  slice.

### Stage 6b completion record (2026-08-29)

- `sporely-web` migration
  `20260829145939_add_reference_curation_intake.sql` creates the private intake
  policy, attestation-version, attempt-ledger, submission-head,
  append-only-submission-version, and report tables. The Stage 6a catalogue
  remains dormant: this slice adds no curator mutation, publication, public
  read, Edge Function, admin/landing UI, or deployment.
- `submit_private_reference_for_curation` takes stable source IDs plus expected
  work, treatment, and measurement-set revisions. Under one owner lock it
  server-reads exactly that same-owner graph, rejects stale or cross-owner
  inputs, and stores a bounded canonical candidate projection, digest, source
  revision tuple, and immutable consent/attestation evidence. Unknown nested
  fields are removed while the established Stage 3 agent and raw-point forms
  remain valid. Private notes and transport state never enter the candidate.
- Initial submission, append-only resubmission, withdrawal, and exact
  publication-revision report intake are caller-JWT `SECURITY DEFINER` RPCs
  with empty `search_path`. They derive the actor from `auth.uid()`, enforce a
  current non-banned profile and the account-deletion marker, use CAS for
  mutable lifecycle heads, distinguish exact retries from idempotency
  conflicts, and serialize per-account attempt limits. Failed expensive
  attempts count; exact successful retries do not consume another allowance.
- Operational inputs are deliberately fail-closed. Submission and report
  intake default disabled, and cannot be enabled until the matching immutable
  attestation wording/version, positive rate window/count, and nonnegative
  retention settings are supplied. No rights text, retention period, or rate
  number was invented in this slice.
- Direct `PUBLIC`, `anon`, `authenticated`, and `service_role` table access is
  denied; caller RPC execution is granted only to `authenticated`. Candidate
  and attestation history is append-only, event history remains immutable, and
  the service-only retention path preserves hashes/revisions while purging due
  candidate bodies and redacting due report detail exactly once.
- Account deletion now locks the owner/deletion marker, revokes memberships,
  withdraws open submissions, removes personal-source and actor pointers,
  schedules configured candidate/report retention, applies immediately due
  retention, clears attempt state, and only then deletes observation uses and
  the owner work graph. The sequence is retry-safe; published catalogue data
  remains independent.
- Verification passed a fresh local migration reset, five focused Stage 6b SQL
  suites, all four Stage 6a schema/security/immutability/account-deletion
  suites, the Stage 3 mutation and Stage 5 public-observation reference SQL
  regressions, all 49 delete-account tests, `git diff --check`, and
  `supabase db lint --local --level warning --fail-on error`. Lint had no
  errors; it reported the existing Stage 3 volatility warnings and equivalent
  warnings for the new pure immutable JSON projection helpers.
- Independent correctness and security reviews found and closed retry
  classification, attestation-version reuse, retention enforcement, source
  bounds, service-side consistency, nested-field projection, ban-race,
  failed-attempt accounting, withdrawal retry, and concurrent retention-event
  gaps. Final re-reviews and a complete security diff scan found no remaining
  material issue or reportable finding. Stage 6c is the next independently
  authorized slice.

### Stage 6c completion record (2026-08-29)

- `sporely-web` migration
  `20260829163642_add_reference_curation_reviewer_workflow.sql` adds the
  private claim/feedback/acceptance provenance and idempotency-collision state,
  exact graph foreign keys, immutable acceptance evidence, and service-only
  reviewer RPCs. Claim, request-changes, reject, accept-to-draft, and draft
  edits use workflow and entity CAS; exact successful retries are no-ops while
  negative retries preserve their domain status. All outcomes and distinct
  request-ID collisions are append-only audited once.
- The dedicated `supabase/functions/reference-curation/` Edge Function exposes
  only those five mutations plus read-only duplicate warnings. It binds
  `getUser` and verified claims to one UUID session, defers service-client
  creation until authentication succeeds, and relies on a second database
  check of the current session, non-banned profile, and current admin/reviewer/
  publisher membership. Requests, action payloads, RPC statuses, successful
  responses, and browser origins are allowlisted and size-bounded; malformed
  or expanded privileged responses fail closed. Dependency versions and the
  Deno lockfile are fixed to the reviewed graph.
- Acceptance server-reads the exact immutable candidate revision/hash and
  atomically creates fresh curated work → treatment → measurement-set drafts,
  or links only an explicit CAS-protected hierarchy prefix with a reason. It
  never publishes, infers a taxon assignment, reuses a personal UUID, or
  exposes a personal source identifier. Draft editing remains restricted to
  allowlisted scientific/bibliographic fields; explicit positive taxonomy-v3
  species assignments are audited, and clearing raw points stores SQL `NULL`.
- Claim and membership changes share serialization so revocation/reassignment
  cannot authorize from a stale membership view. Account deletion releases
  active claims, anonymizes `accepted_by` without changing immutable candidate
  or curated-graph provenance, and cannot bypass the public deletion wrapper.
  Duplicate detection is warning-only: normalized DOI, normalized ISBN, and
  full structured bibliographic equality are deterministically ordered and
  capped at 100; no fuzzy title/author merge exists.
- The slice remains dormant. It adds no publication, deprecate/supersede/
  withdraw lifecycle operation, public read, admin/landing UI, or deployment.
  Browser activation additionally requires an explicit
  `REFERENCE_CURATION_ALLOWED_ORIGINS` value; the empty default denies browser
  origins.
- Verification passed a fresh local migration reset; four focused Stage 6c,
  nine Stage 6a/6b, and the Stage 3/5 reference SQL suites; 30 frozen/locked
  Deno tests; all 49 delete-account tests; formatting and diff checks; and
  `supabase db lint --local --level warning --fail-on error`. Lint reported
  only the previously documented Stage 3/6b immutable-versus-stable warnings.
  Fresh correctness, Edge, and security reviews found and closed retry-status,
  audit amplification, account-deletion, acceptance immutability, response
  projection, body-bound, dependency-lock, duplicate-warning, raw-point-null,
  and membership-race defects. Final re-reviews and the security diff scan
  found no remaining material or reportable finding.

### Stage 6d completion record (2026-08-29)

- `sporely-web` migration
  `20260829190945_add_reference_curation_publication_lifecycle.sql` adds the
  service-only `mutate_reference_curation_lifecycle` RPC and the existing
  `reference-curation` Edge boundary now accepts only publish, deprecate,
  supersede, and withdraw in addition to the Stage 6c actions. Current
  non-banned publisher membership or admin state and the exact live session
  are checked in the database; direct client RPC execution remains denied.
- Publication locks and CAS-checks the measurement set, work, treatment, exact
  sorted taxon-assignment set, and—where a lineage transition mutates it—the
  predecessor. It server-builds and atomically stores the immutable snapshot,
  structured citation (including its nullable curated citation key), content
  hash, exact graph revisions, and copied taxonomy-v3 species assignments.
  Clients cannot supply publication snapshots, citations, hashes, or taxa.
- Supersession uses deterministic target/predecessor locking, compatible exact
  taxon assignments, a single non-withdrawn successor, and atomic predecessor
  deprecation. Restoration appends a new bundle revision, cannot create two
  live ends of a lineage, and requires exact predecessor identity/CAS before
  reapplying predecessor deprecation. Current published bundles cannot be
  republished in place.
- Deprecate and withdraw are reasoned status-only transitions: they advance
  lifecycle CAS state but never rewrite or delete publication bundles, copied
  taxa, frozen observation snapshots, or future Compare evidence. Target and
  predecessor audit events keep their own before/after hashes. Exact retries
  are no-ops, request-ID collisions are audited once, partial failures roll
  back, and transient serialization/deadlock failures remain retryable rather
  than becoming durable terminal results.
- The Edge action boundary strictly validates per-action request and response
  shapes, canonicalizes UUIDs before exact JSON graph comparison, maps graph,
  lineage, and CAS domain conflicts explicitly, and returns only the bounded
  lifecycle result. No public RPC, catalogue read, admin/landing UI, or
  deployment/activation was added.
- Verification passed a fresh local migration reset/replay; the focused Stage
  6d publication/lifecycle SQL suite; all 13 Stage 6a–6c SQL suites; the Stage
  3 mutation and Stage 5 public-observation reference SQL regressions; all 37
  reference-curation Deno tests; all 49 delete-account tests; Deno formatting,
  `git diff --check`, and `supabase db lint --local`. Lint reported only the
  previously documented Stage 3/6b immutable-versus-stable warnings.
- Fresh correctness and security reviews found and closed restoration-lineage
  CAS/locking, supersession state, audit-hash attribution, frozen citation-key,
  UUID normalization, and historical-evidence coverage gaps. Final re-reviews
  and the complete security diff scan found no remaining material or
  reportable issue. Stage 6e is the next independently authorized slice.

### Stage 6e completion record (2026-08-29)

- `sporely-admin` now has isolated typed `reference-curation` API and model
  modules plus a queue/detail workspace. Reviewer controls cover claim,
  reasoned stale-claim takeover, request changes, reject, and draft edits;
  publisher/admin lifecycle controls cover publish, deprecate, supersede, and
  withdraw in a visually separate privileged region. Lifecycle mutations and
  all non-claim reviewer decisions require an explicit reason and confirmation.
- The browser sends only the signed-in caller JWT to the existing
  `reference-curation` Edge Function. UI capability checks are advisory: every
  mutation still relies on Stage 6c/6d live membership, ban/deletion, actor,
  lifecycle, graph-version, and CAS enforcement. Conflict, not-found, and
  changed-permission responses invalidate stale UI state and refetch instead
  of retrying a mutation automatically.
- The verified Stage 6c/6d boundary had mutation operations but no private
  queue/detail/capability read contract. The supporting `sporely-web` migration
  adds only service-role RPC projections routed through that same authenticated
  Edge Function. Direct `PUBLIC`, `anon`, and `authenticated` execution is
  denied; each read rechecks the exact live session, non-banned profile,
  deletion marker, and current database membership/admin state. Projections
  are strictly allowlisted and bounded and exclude contributor/source,
  attestation, audit, and transport data. No public catalogue API was added.
- The queue uses deterministic bounded keyset pagination. Detail reads return
  the exact current candidate and accepted curated graph needed to build CAS
  actions, including sorted taxon assignments. Draft editing supports work,
  treatment, measurement set, individual taxon assignments, and a new exact
  assignment. All untrusted evidence is escaped as text; dialog and queue
  keyboard focus is retained across input, paging, refresh, and conflict
  reconciliation.
- Verification passed a fresh local migration reset/replay, 17 Stage 3/5/6a–6e
  SQL regression files, all 42 reference-curation Deno tests, all 49
  delete-account tests, and all 139 admin Node tests. Syntax, Deno formatting,
  and diff checks passed. Database lint reported only the already documented
  immutable-versus-stable warnings.
- Fresh correctness, security, and accessibility reviews found and closed
  input/queue focus loss, non-draft edit visibility, multi-taxon editing,
  queue truncation, claim-takeover reason, and direct-role test-coverage gaps.
  Final reviews and completed security diff scans found no remaining material
  or reportable issue. No Edge Function, migration, admin UI, or moderation
  policy was deployed or activated. Stage 6f is the next independently
  authorized slice.

### Stage 6f completion record (2026-08-29)

- `sporely-web` migration `20260829212131_add_curated_reference_citation_exports.sql`
  adds one private, append-only export artifact per immutable
  `(curated_measurement_set_id, bundle_revision)`. Each artifact records the
  exact frozen curated work UUID/revision and citation-schema version, a source
  citation hash, per-format SHA-256 hashes, and an aggregate artifact hash.
  It has no CAS row because the immutable publication key is its idempotency
  and revision boundary.
- Publication insertion materializes UTF-8 plain text, BibTeX, and canonical
  CSL-JSON transactionally from the publication row's frozen `citation_json`;
  neither future publication nor replay reads the mutable curated work head.
  Existing bundles are backfilled in deterministic key order. Exact replay is
  a byte-identical no-op, while any mismatch fails closed instead of updating
  historical output.
- Safe explicit ASCII citation keys are retained. Missing, unsafe, or reserved
  keys use `sporely-auto-<hyphenless-curated-work-uuid>`, keeping the key stable
  across bundles while reserving a collision-free generated namespace.
  BibTeX uses fixed type/field ordering and escapes structural characters;
  CSL-JSON projects only its field allowlist and ordered agents. Control and
  bidi characters cannot create extra records or fields, while ordinary
  Unicode remains UTF-8.
- DOI values are prefix-stripped, lower-cased, and syntax-validated for the
  structured exports. The exact terminal DOI suffix produced by Stage 6d is
  replaced with the normalized URL or removed when invalid; internal manual
  citation-override text is not rewritten, and an empty resulting citation
  fails closed. No DOI metadata lookup occurs.
- The artifact table has RLS enabled with no client policy. `PUBLIC`, `anon`,
  and `authenticated` have no access; `service_role` is read-only and cannot
  execute the private builders/materializer. Update, delete, and truncate are
  rejected. Account deletion anonymizes publication attribution without
  deleting its citation artifacts. No public RPC, Edge function, landing or
  Compare behavior, RIS output, deployment, or activation was added.
- Verification passed three fresh local migration reset/replays, the focused
  export test, all 18 reference/curation SQL regression files, all 42
  reference-curation Deno tests, and all 49 delete-account Node tests.
  `git diff --check` passed; database lint reported only the already documented
  Stage 3/6b immutable-versus-stable warnings. Focused tests cover exact-byte
  replay, backfill behavior, Unicode, BibTeX/CSL escaping and type mappings,
  safe and fallback keys, valid/invalid DOI handling, hostile text, exact
  revision/hash binding, later revisions, rollback, immutability, grants, and
  account deletion.
- Fresh correctness and security review found two plain-text DOI edge cases:
  raw Stage 6d DOI suffixes were initially left unnormalized, and the first fix
  could rewrite an internal manual override. Both were reproduced with failing
  tests and fixed with terminal-suffix-only normalization plus a nonempty
  postcondition. Final re-reviews found no remaining material issue.

### Stage 6g completion record (2026-08-30)

- `sporely-web` migration
  `20260829220943_add_public_curated_reference_reads.sql` adds the exact-key
  `search_public_curated_reference_sets(integer, integer, timestamptz, uuid)`
  and `get_public_curated_reference_set(uuid, integer)` RPCs. Discovery accepts
  only a positive current taxonomy-v3 concept whose rank is exactly `species`;
  it has no slug, name, taxonomy-v2, descendant, alias, or fuzzy fallback.
  Discovery returns only each current published head. Exact reads retain an
  immutable published/deprecated revision and frozen publication taxon name;
  an explicit withdrawn revision returns only the documented status tombstone.
- The result envelope is reconstructed from the immutable publication,
  publication-taxon, and Stage 6f artifact rows. Snapshot and citation keys,
  types, sizes, IDs, revisions, normalized DOI/export values, source hashes,
  per-format hashes, and aggregate artifact hash are revalidated before any
  item is exposed. Malformed or provenance-mismatched bundles fail closed.
  Output includes only the frozen snapshot, allowlisted structured citation,
  exact assignment, status/supersession fields, and exact UTF-8 plain-text,
  BibTeX, and parsed CSL-JSON artifacts. It never reads or projects personal
  library, submission, report, membership, curator, audit, or private-note
  state. Draft successors are not exposed.
- Existing public species search/detail signatures retain every prior field and
  ordering and add one nullable `taxonIdentity` object. Identity is derived in
  one set-based pass over the same caller-visible public, non-draft,
  non-banned, block-filtered observation population. Exactly one distinct
  non-null resolved v3 species ID returns its current registry canonical name;
  zero, ambiguity, an ineligible rank, or an invalid name returns null.
  Existing public observation/reference RPCs are unchanged.
- Public functions are postgres-owned `SECURITY DEFINER` routines with empty
  search paths, explicit `anon`/`authenticated`/`service_role` grants, and no
  direct private helper/table path. Renamed legacy species implementations are
  execution-revoked. Discovery validates at most 100 deterministic candidates,
  returns at most the requested 50 items and 1 MiB, and uses bounded overscan
  so malformed leading rows do not normally strand an older valid row. Exact
  reads fail closed above 100 assignments or a 1 MiB serialized response.
  These are technical abuse bounds, not the still-unresolved production rate
  policy or default page size.
- Focused tests cover exact species-rank lookup, zero/ambiguous visible
  identities, caller block filtering, additive legacy field/order parity,
  multiple frozen assignments, keyset ties, bounded malformed omission,
  revisions, deprecation/supersession, draft-successor non-leakage, withdrawn
  tombstones, uppercase HTTPS, hostile text, malformed snapshot/citation/schema,
  artifact-version corruption, assignment and request limits, grants, direct
  role denial, and anonymous/authenticated access. A fresh migration reset and
  21 relevant Stage 3–6g SQL/security regressions passed. Database lint added
  no warning beyond the already documented pre-existing immutable-versus-stable
  findings; `git diff --check` passed.
- Fresh correctness and security/privacy/abuse reviews found and closed
  per-row observation rescans, repeated per-assignment export reconstruction,
  unbounded pre-limit work/response size, valid-source bound mismatches,
  citation schema typing, uppercase URL rejection, draft-successor leakage,
  oversized-assignment discovery, malformed-page stalling, and legacy ordering.
  Final re-reviews found no remaining material issue. No migration, public API,
  landing/Compare behavior, `/references` route, or moderation workflow was
  deployed or activated.

### Stage 6h completion record (2026-08-30)

- `sporely-landing` now consumes the Stage 6g contract through strict public
  taxon-identity, curated-envelope, citation-v1, export, exact-read, and cursor
  adapters. The Stage 5 frozen-snapshot validator moved to a shared module with
  its existing exact-key and raw-text behavior preserved. A malformed or
  privacy-expanded row is omitted; an exact read fails atomically if its rows
  mix revisions/lifecycle state, disagree on immutable bundle content, repeat
  or misorder taxon assignments, or do not match the requested stable ID and
  revision.
- Public species models accept the additive nullable `taxonIdentity` without
  changing legacy rows. The species page issues no catalogue request unless it
  has one valid positive taxonomy-v3 identity and an explicitly configured
  page size from 1 through the Stage 6g hard cap of 50. Requests use only that
  numeric identity and the deterministic `(published_at DESC, id ASC)` cursor;
  there is no slug, name, fuzzy, or taxonomy-v2 fallback and one RPC supplies
  each page.
- The localized Norwegian, Swedish, English, and German listing renders only
  validated current published discovery rows. It has isolated loading, empty,
  retry, and pagination states; preserves prior cards across a failed next
  page; rejects stale completions after a species change; and deduplicates by
  immutable set/revision/taxon identity. Deprecated bundles remain readable
  only through exact reads, while withdrawal is accepted only as the exact
  status-only tombstone and never reconstructed into content.
- Cards render React-escaped immutable citation/snapshot text. DOI links are
  emitted only from the normalized DOI allowlist. Copy and dependency-free
  downloads use the server-provided plain text and BibTeX artifacts and a
  deterministic serialization of the validated Stage 6g CSL object, with
  explicit UTF-8 MIME types and citation-key/revision filenames. No citation
  content is rebuilt from mutable library state, and no Compare mutation or
  `/references` route was added.
- A fresh local Supabase reset succeeded. The Stage 6g public curated-read and
  exact species-identity SQL suites, the Stage 5 public observation-reference
  suite, and curated-library grant/security coverage passed. Landing focused
  coverage passed 27 tests; the full landing suite passed 53 files / 570 tests;
  TypeScript checking, production build, and `git diff --check` passed. The
  existing Vite chunk-size advisory is unchanged.
- Fresh correctness/security/accessibility review found and closed two issues:
  stale load-more completion could strand pagination after a species change,
  and exact-read normalization initially accepted individually valid but
  heterogeneous rows. Final re-review found no remaining concrete defect.
  There was no `sporely-web` schema or RPC change and nothing was deployed.

### Open operational policy inputs

These do not change the technical boundary, but must be supplied before the
corresponding behavior is activated:

- the initial user IDs assigned `reference_reviewer` and
  `reference_publisher`;
- the immutable rights-attestation wording/version used to enable owner
  submissions;
- owner submission and report rate windows/counts, submission candidate
  retention, report-detail retention, and contributor-attribution retention;
- the copyright/takedown response SLA;
- public catalogue rate-limit numbers and default page size within the hard
  database caps.

The recommended next implementation slice is Stage 6i only: versioned Compare
storage in `sporely-landing`. Curated items remain test-constructible but are
not added by UI until Stage 6j.

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
10. the same publication can be reused for another observation without retyping it;
11. owner consent creates an immutable submission candidate without changing
    the personal row into public data;
12. only current, non-banned reviewers/publishers/admins can perform the
    audited lifecycle actions assigned to them;
13. published curated bundles have stable IDs, immutable revisions, exact v3
    species assignments, and no fuzzy bibliographic or taxon merge path;
14. public catalogue reads expose only allowlisted curated publication data,
    never personal-library, submitter, or moderation state;
15. landing Compare captures the chosen frozen bundle revision immediately,
    does not synthesize observations, and does not create an observation use;
16. citation display/export comes from the immutable structured curated work
    revision in plain text, BibTeX, and CSL-JSON;
17. deprecation, supersession, withdrawal, restart, and offline flows preserve
    already frozen observation and Compare evidence; and
18. a curated bundle can be explicitly forked to fresh personal UUIDs without
    automatic follow/update behavior. A global `/references` route is not
    required for Stage 6 completion.
