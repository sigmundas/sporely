# Unified species search in the observation editor

Status: approved 2026-10-11 by the owner for Stages 1-2 only ("just do the
edit observation taxon selector first"). Decision 5 runs with the default
below (old rows kept in a collapsed "Name details" section). Request: "simplify the
species selector in sporely-py: make it the same as in sporely-web: include
both vernacular and scientific type-as-you-search".
Repository: `sporely-py`. Branch: `feature/unified-taxon-search` from
`origin/main` (`ef67374`).
Companion (separate run, already in progress): sporely-web
`docs/plans/active/2026-10-11-taxon-input-clear-button.md`, which adds a clear
(×) button to the web species input. This plan gives the desktop field the
same clear affordance.

## Why

The desktop observation editor asks for a taxon through four separate rows:
Vernacular (+ 🌐 language button), Genus, Species and Scientific
(`ui/observations_tab.py:15340-15405`), each with its own `QCompleter` run by
`TaxonInputController` (`ui/taxon_input_controller.py:146`). The web app has
**one** field. Typing either a vernacular or a scientific name shows a single
dropdown of matching taxa labelled `Vernacular (Scientific)`
(`sporely-web/src/taxonomy-v2.js:24-31`, `src/screens/review.js:1478-1527`).
The web search is prefix-only, case-insensitive, needs at least 2 characters,
ranks exact matches above prefix matches, and covers canonical names,
scientific-name variants and vernacular names in the chosen language
(`search_taxa_v2`, `sporely-web/supabase/schema.sql:5610`).

## Current desktop building blocks (evidence)

- `TaxonLookupService` (`database/taxon_lookup.py:177`):
  `suggest_scientific_names` (`:662`, delegates to
  `VernacularDB.suggest_scientific_names` = `database/vernacular_db.py:784`,
  which uses indexed range scans and a rank whitelist and returns
  `sporely_taxon_id`, `scientific_name`, `taxon_rank_snapshot`, `link_kind`,
  `family`, `authorship`, …), `suggest_common_names` (`:679`),
  `resolve_manual_scientific` (`:727`), `resolve_common_name` (`:1140`),
  `best_common_name_for_taxon` (`:1203`).
- `TaxonInputController` owns the committed-snapshot identity model
  (`:825-1015`). Only an explicit suggestion selection commits
  `sporely_taxon_id`, and manual divergence invalidates it. Callbacks:
  `on_snapshot_committed` / `on_snapshot_invalidated`.
- Dialog wiring: `ObservationDetailsDialog._setup_vernacular_autocomplete`
  (`ui/observations_tab.py:19708-19760`). Manual genus/species/scientific
  edits clear the red-list snapshot. Unambiguous manual entry commits
  immediately (Stage 3B.5 follow-up).
- AI prefill: `_on_ai_copy_to_taxonomy` (`ui/observations_tab.py:16788`).
- Other users of the controller (reference-value editors keyed by
  genus/species): `ui/main_window.py:4702`, `:10653`,
  `ui/cloud_reference_dialog.py:812`. There are also separate completers in
  `ui/add_reference_dialog.py:817-829` and `ui/reference_entry_editor.py:2070`.
- Tests: `tests/test_taxon_input_controller*.py`, `tests/test_taxon_lookup.py`,
  `tests/test_observation_details_taxon_lookup_integration.py`,
  `tests/test_observation_details_ai_copy_and_vernacular_autofill.py`,
  `tests/test_red_list_identity_invariants.py`.

## Fixed decisions

1. **Scope is the observation editor's main taxon only** (Stage 2). The host
   taxon is deferred to a follow-up plan (see "Deferred" at the end). The reference-value editors and the add-reference
   dialog keep their genus/species fields. They edit reference data keyed by
   genus/species, not an observation identification. Converting them is a
   possible later plan, not part of this one.
2. **The identity model does not change.** The persisted fields
   (genus, species, scientific name, vernacular, `sporely_taxon_id`, rank
   snapshot, link kind) and the controller's commit/invalidate semantics
   stay exactly as they are. The new search field is a front end that drives
   the existing controller. It does not write identity on its own. No schema,
   sync or cloud payload changes.
3. **Search semantics match web:** at least 2 characters; prefix match,
   case-insensitive, on canonical scientific names, scientific-name variants
   and vernacular names in the current vernacular language; deduplicated per
   taxon; exact matches before prefix matches; label `Vernacular (Scientific)`
   (just `Scientific` when there is no vernacular name or it equals the
   scientific name), with family as secondary text. Results stay within the
   existing rank whitelist, so genus-rank and infraspecific taxa are included.
4. **Free text is still allowed** (an unlisted name, "Russula sp.", a genus
   only). Text that is never selected is resolved on commit (editing
   finished / dialog save) through the existing manual paths
   (`resolve_manual_scientific`, then `resolve_common_name`). That gives the
   same outcome as typing the same text into the old Genus/Species/Vernacular
   fields today, including "unambiguous manual entry commits; ambiguous
   does not".
5. **The old rows are hidden, not deleted:** they move into a collapsed
   "Name details" section under the search field. It is closed by default and
   remembered per user via the existing settings store. It keeps
   infraspecific/manual editing and the 🌐 vernacular-language button
   available. **Owner to confirm.** The alternative is removing the four rows
   entirely, which is more web-like but drops manual infraspecific entry.
6. **Clear button:** `setClearButtonEnabled(True)` on the search field.
   Clearing must have the same effect as deleting all the text by hand
   (identity invalidated and red-list snapshot cleared, as today).
7. Every new user-facing string goes through `self.tr(...)`, then
   `tools/update_translations.sh` runs and nb_NO/sv_SE/de_DE are translated
   (`.ts` + `.qm` committed together), per `.claude/rules/localization.md`.

## Stage 1 — Combined taxon search in the lookup layer

Add `TaxonLookupService.search_taxa(query, limit=TAXON_COMPLETER_LIMIT)`
returning a small frozen dataclass per taxon (`sporely_taxon_id`,
`scientific_name`, `genus`, `species`, `rank`, `family`, `vernacular_name`,
`matched_on` = `"scientific" | "vernacular"`, `display_label`,
plus whatever the controller needs to commit a snapshot, so the UI never
re-queries for a choice it already has). Build it from the existing
`suggest_scientific_names` and the common-name prefix path. Do not write a
new SQL search unless the existing queries cannot provide vernacular prefix
matching. If a query is needed, use the same indexed range-scan style as
`vernacular_db.py:784`. Cache like the neighbouring `suggest_*` methods and
clear the cache in `clear_cache`/on language change.

No UI changes in this stage.

Tests (`tests/test_taxon_lookup.py` or a new focused file using the existing
fixture DB pattern): vernacular-prefix hit, scientific-prefix hit, variant hit,
deduplication when both match one taxon, exact-before-prefix ordering, <2
chars returns `[]`, language switch changes vernacular hits and labels, no v2
DB returns `[]` without raising.

Verification: focused pytest for the touched/new test files.

## Stage 2 — Single search field for the observation's taxon

In `ObservationDetailsDialog`, add one "Species" search `QLineEdit` (clear
button enabled) above the existing rows. Back it with a completer over
`search_taxa` results (debounced like the controller, popup styled with
`_style_dropdown_popup_readability`). Move the old Vernacular/Genus/Species/
Scientific rows into the collapsed "Name details" section (decision 5).

Behaviour:
- **Selecting a result** commits through the controller's existing
  selection/commit path (add a narrow public method on
  `TaxonInputController` such as `commit_choice(...)` if none exists). The
  backing fields, `on_snapshot_committed`, the red-list badge and the
  vernacular field must end up identical to selecting the same taxon in the
  old completers. The method must not write identity fields itself.
- **Typing** in the search field never commits identity. Leaving it with
  unselected text applies decision 4. Clearing applies decision 6.
- **Programmatic fills** (loading an observation, `_on_ai_copy_to_taxonomy`,
  edits made in "Name details") update the search field's display text from
  the backing state without opening the popup or re-triggering the search.
  Use the controller's suspend mechanism or an equivalent guard.
- The search field's display text is always derived from the backing state
  when that state changes (`Vernacular (Scientific)`), so the two views
  cannot drift.

Tests: extend `test_observation_details_taxon_lookup_integration.py` and
`test_observation_details_ai_copy_and_vernacular_autofill.py`, plus
`test_red_list_identity_invariants.py` where relevant. Cover: select by
vernacular → same snapshot as the old genus/species selection; select by
scientific; free text that resolves unambiguously / ambiguously / not at
all; clear; load of an existing observation shows the right label and opens
no popup; AI copy fills the search field and the backing fields; an edit in
Name details updates the search label; and the save round-trip persists
unchanged fields.

Translations per decision 7.

Human gate (blocks acceptance; it is a real UI change on an identity path):
on the desktop app, (a) create an observation by typing a Norwegian
vernacular name and selecting it, (b) by typing a scientific name, (c) by
AI-ID copy, then save, reopen and confirm the name and red-list badge;
(d) clear the field and save; (e) check the layout at the dialog's minimum
width and with "Name details" open.

## Deferred — host taxon (not part of this run)

Follow-up plan, after Stages 1-2 are accepted and used:

Apply the Stage 2 component to the host taxon rows
(`host_genus_input` / `host_species_input` / `host_vernacular_input`,
`ui/observations_tab.py:15563-15575`, controller at `:20211`). Reuse the
Stage 2 implementation and do not copy it; if Stage 2 left it embedded in the
dialog, extract it first in this stage. Host-specific AI-copy and save
behaviour must be unchanged apart from the new front end.

Tests: host equivalents of the Stage 2 select/free-text/clear/load cases.
Translations per decision 7 if strings change.

Deferred human check (does not block acceptance): repeat (a)/(b) and (d) of
the Stage 2 gate for a host taxon.
