# Unified taxon picker in the observation editor

Status: layout A and Stage 1 approved by the owner on 2026-10-27. Stage 1
(search) is accepted, and Stage 2 (standalone widget) is built, both on
`feature/taxon-picker-search` (draft PR #47). Stage 2 awaits the owner's UI
review. Stage 3 is not started.
Design proposal with renders: https://claude.ai/artifact/FNhyYdKkGVSipUSLshp5mb

This plan replaces the old `feature/unified-taxon-search` plan
(`2026-10-11-unified-taxon-search.md`, on that branch only). That branch is
not rebased or merged. Its search ranking, its accepted-genus filter
(`973b661`) and its uncommitted selection code are not reused.

## Goal

One search field replaces the editable Navn, Slekt, Art and Vitenskapelig
rows in Edit Observation. Picking a result shows a compact identification
card with Endre and ✕. The Reference picker and the host taxon are out of
scope.

## Owner decisions (2026-10-27)

1. Layout A: a search field that becomes an identification card.
2. One row per name. When a COL and a NorTaxa concept share a name, the row
   offers a NorTaxa | COL source choice. A click picks the visibly
   indicated default; the other chip picks the other concept, and the card
   can switch afterwards. A neutral, dismissible first-time note
   ("Flere artsoppføringer") explains this.
3. The scientific name is the most prominent element; the vernacular follows it.
4. The Usikker flag is enough. There is no cf./aff. qualifier and no schema change.
5. There are no manual Slekt/Art rows. Unlinked manual names, genus-only
   picks and Usikker keep working as before.
6. The concept already on an existing observation is never changed silently,
   whether by an Artsorakel id or by the default source.
7. Artsorakel evidence affects ranking and the default only. It never
   removes a valid concept from the results.
8. One row per name is presentation, not an identity merge. Every concept
   keeps its Sporely id. Same-source homonyms, ranks and synonyms stay apart.
9. Later integration uses current main and the PR #39 picker behaviour as the
   safety baseline.

## Stage 1 — search in the lookup layer (done, see branch)

`database/taxon_search.py` (Qt-free), with
`TaxonLookupService.search_taxa(query, offset, limit, evidence_taxon_ids,
current_taxon_id)` caching it. It returns `TaxonSearchPage(rows, has_more)`;
each `TaxonSearchRow` is a name with its ordered `concepts`.

- Sources: canonical and synonym prefix scans, epithet word starts across
  genera, vernacular prefix, and vernacular word part. The word-part search
  also uses the Norwegian plural stem, so `kremler` finds `grønnkremle`.
- Tiers: exact, scientific, vernacular prefix, epithet, vernacular word part.
  Rows holding the current or an evidence concept rank first in their tier.
- Merge rule: same exact name, rank and kind, at most one concept per
  source. Otherwise each concept gets its own row, marked `shares_name`.
- Default order inside a row: current concept, evidence, Artsdatabanken id,
  COL, then taxon id.
- Paging: capped scans widen until the requested page is complete, so a
  page cut never falls inside a name.

Tests: `tests/test_taxon_search.py` (fixture) and
`tests/test_taxon_search_release.py` (bundled `tax-2026.10.23-11`:
representative results and the warm timing of 16 broad queries).

Owner decisions on Stage 1 (2026-10-27): keep same-name COL/NorTaxa concepts
in one row even when their families disagree, and show each source's own
family. Keep the Norwegian plural stem in the lowest tier. Keep alphabetical
order within tiers after the current/evidence boost. Keep species-name and
synonym rows separate when they lead to different concepts.

Data review item: *Cantharellus* is Cantharellaceae in NorTaxa (626242) and
Hydnaceae in COL (129742) in `tax-2026.10.23-11`. In that release, 1,829 of
4,831 same-name groups disagree on family.

## Stage 2 — picker widget (built, awaiting UI review)

`ui/taxon_picker.py` (`TaxonPicker`). It emits `concept_picked(row, concept)`,
`free_text_committed(text)` and `cleared()`, and writes nothing.

- Results appear in an overlay inside the window, never a separate window,
  so the overlay cannot take focus. Keys stay in the field: ↑/↓ and
  PgUp/PgDn move, Enter and Tab pick, and Esc closes the results, or after
  Endre restores the card. ←/→ switch the source once the user has
  navigated the list; Alt/Option+←/→ switch it at any time.
- Rows show the scientific name (bold italic) first, then the vernacular.
  Rank is a pill. A row with several concepts shows one chip per source;
  the chip that Enter or a click on the row picks is filled before
  anything is committed.
- A first-time note ("Flere artsoppføringer") has an "Ikke vis igjen"
  button. The setting is `taxon_picker_source_note_dismissed`.
- The card shows the name (a genus as "Russula sp."), the vernacular, rank,
  family, source, Artsdatabanken id and the Uncertain flag, plus Kilde
  buttons (with each family when the families differ). It also notes a
  chosen synonym and its stored entry.
- Artsorakel chips: a chip with a resolved identifier picks that concept
  on click. A chip without one fills the search. A chip never applies itself.
- Translations: nb/sv/de for the `TaxonPicker` context.

Evidence: `tests/test_taxon_picker.py` (offscreen Qt: picking, keyboard,
focus, chips, card, note, Show more, AI, and the database left
unchanged). Renderer group `taxon-picker` (9 scenarios built on
`tools/review_ui/taxon_picker_sample.json`, a real extract of the release).
`python -m tools.review_ui.taxon_picker_demo` runs it interactively.

Not proven offscreen: native macOS mouse, focus and scroll behaviour of the
overlay. It belongs to the Stage 3 human gate.

## Stage 3 — Edit Observation integration (not started, human-gated)

Replace the four rows. Route picks through one narrow controller method that
reuses the Scientific pick commit; free text and ✕ use the existing paths.
The acceptance criteria and the human checklist are in the design proposal.

## Open questions for Stage 2

See the Stage 1 report in the PR description.
