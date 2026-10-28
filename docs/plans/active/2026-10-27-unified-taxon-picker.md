# Unified taxon picker in the observation editor

Status: layout A and Stages 1 and 2 approved by the owner on 2026-10-27, on
`feature/taxon-picker-search` (draft PR #47). Stage 3 (Edit Observation
integration), the floating results list and the review fixes are committed
on draft PR #47 at the owner's request (2026-10-28). The PR stays unmerged
until the final review and the owner's desktop acceptance check below.
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

## Stage 3 — Edit Observation integration (built, human-gated, uncommitted)

Owner decisions (2026-10-27): keep the source-switching keys and add a
contextual "←/→ Bytt kilde" hint on the highlighted two-source row; up to
nine visible rows with adaptive height inside the dialog; placeholder "Søk
etter sopp eller vitenskapelig navn…"; never infer a taxon from unpicked
text, keep "Bruk som navn uten takson", and ask on Save when search text is
unfinished; Tab must not pick the automatically highlighted first result.

Owner decision (2026-10-28): drop the "Artsorakel-ID peker på …" wording
from rows, the card and tooltips. Artsorakel evidence still sets the
default; a single-concept row names its source (COL or NorTaxa) and a
two-source row shows it with the filled chip.

- The picker replaces the Navn, Slekt, Art and Vitenskapelig rows. Those
  inputs stay as hidden backing state: the controller, `get_data()`, the
  Artsorakel copy and Link read and write them as before.
- A pick goes through `TaxonInputController.commit_picked_concept`, which is
  the Scientific-pick commit (`taxonomy_v2_artifact` proof).
- A kept name goes through `set_unlinked_name`: whole in Species, Genus and
  common name empty, no identity. This is the web client's rule for a manual
  name (`createManualTaxon`). ✕ goes through `clear_identification`.
- A resolved Artsorakel chip runs Copy to taxonomy for that prediction, so
  the provider identifier, `ai_selected_*` and the provider Red List are kept
  (`external_id_resolution` proof). An unresolved chip only searches.
- The picker shows what the backing state holds (`_sync_taxon_picker`).
  Display only, it never commits. The stored concept is the default of its
  row (`set_current_taxon_id` on load).
- Save with unfinished text asks: keep as a name without a taxon, discard
  the text and keep the identification, or go back and choose. Return and
  Escape are claimed from the dialog's shortcuts while the picker uses them.
- The Species tab scrolls instead of squashing the card in a short pane.

Evidence: `tests/test_observation_details_taxon_picker.py` (23 tests: picks,
source switch, synonym, genus only, kept name, clear, unfinished text, the
Artsorakel chips, Unidentified, and round trips through the observations
table). The dialog-level tests of the retired Genus/Species/Scientific
popups were removed from `tests/test_evidence_aware_taxon_suggestions.py`;
the controller completers they drove still serve the other editors.
Renderer scenarios `observations.taxon-picker-*`.

Floating results list (owner request 2026-10-28): the list is a frameless
`Qt.Tool` window with `Qt.WindowDoesNotAcceptFocus` and
`WA_ShowWithoutActivating`, owned by the dialog. It is placed in screen
coordinates within the field's screen's `availableGeometry()`, opens below
the field (past the dialog's edge when needed) and flips up only when the
list does not fit below and there is more room above. It closes on Escape,
a press outside the list and field, the dialog's deactivation, move/hide,
and the application losing activation. Wayland, offscreen and minimal keep
the in-window overlay (`_floating_popup_supported`), as does
`SPORELY_TAXON_POPUP=overlay`. The list scrolls per pixel. No taxonomy or
persistence change.

macOS real input: `tools/review_ui/taxon_picker_mac_check.py` keeps all
input in its own process. Keys use `CGEventPostToPid`; clicks are NSEvents
queued with `[NSApp postEvent:]` and wheel events window-relative NSEvents,
because `CGEventPostToPid` mouse events never reach a Qt window. On
2026-10-28 all 26 steps passed on Cocoa with the floating list: typing,
arrows, ⌥→ source switch, Enter (no save), card Kilde click, Escape, Tab,
the list reaching past the dialog's bottom edge with nine rows, mouse wheel
and pixel (trackpad-style) scrolling, PageDown and Show more, a COL chip
click (exactly one commit), an outside press, an Artsorakel chip (a missing
chip fails), and hiding the dialog; the
field kept focus and the dialog stayed active throughout. The Save prompt
is not driven there: a modal opened by synthetic input never becomes key.
These are agent checks, not the owner's hands-on verification.

Review fixes (Codex review of the integrated diff, 2026-10-28):

- The iNaturalist publishing taxon ID (`inaturalist_taxon_id`) belongs to
  the concept it was stored with. Picking another concept, a name without a
  taxon and ✕ clear it; picking the same concept keeps it. There is no
  verified concept-to-iNaturalist mapping, so publishing then resolves the
  new name as for an observation that never had one. The published
  iNaturalist observation ID (`inaturalist_id`) is untouched.
- ✕ also clears the chosen AI determination (`ai_selected_*` and the
  per-image selected marks), as Unidentified does, so loading cannot refill
  the name from it. The Artsorakel predictions stay.
- The picker's current concept follows the committed one, so Endre + Enter
  after a source switch keeps the switched concept.
- The unfinished-search prompt is on the details dialog's thumbnail Edit
  photo; the main gallery's Edit photo no longer calls it.
- The open list follows the field when a parent scrolls, and closes when the
  field scrolls out of view.
- The edit save's column mapping is `ObservationsTab._edit_observation_updates`,
  shared by the edit accept and the tests, which reopen saved rows in edit
  mode.

Human checklist (the owner, on the desktop) before the PR is merged:

1. Run `tools/review_ui/taxon_picker_mac_check.py` with the screen unlocked;
   every step passes with the window active.
2. Edit an observation bound to *Flammulina velutipes*: switch Kilde to COL
   and back, save, reopen, restart the app; the stored concept is unchanged.
3. Pick a synonym (*Collybia velutipes*), a genus only (*Russula*) with
   Usikker, and an Artsorakel chip; save and reopen each.
4. Keep a free name ("Cortinarius sp. …"), clear one with ✕, and press Save
   with unfinished search text; check each choice of the question.
5. Open an older unlinked observation and save it untouched; nothing
   changes. Check the Red List badge, Bestemmelse and the sync status.
6. Tab through the dialog from the search field; nothing is picked.
7. Floating list: with the dialog low on the screen and high on it, and on a
   second monitor, the list opens below the field and flips up only when
   the screen runs out. Scroll it with the trackpad, click rows and chips,
   and check that typing never leaves the field. Click elsewhere in the
   dialog, switch to another app, and close the dialog with the list open;
   it closes each time. Scroll the Species tab with the list open; it moves
   with the field or closes.
8. Open an observation published to iNaturalist, switch its concept or ✕ it,
   save and publish; the new name is used, not the old taxon.
