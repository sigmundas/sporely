# Unified taxon picker in the observation editor

Status: layout A and Stage 1 approved by the owner on 2026-10-27. Stage 1 is
implemented on `feature/taxon-picker-search`. Stages 2 and 3 are not started.
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

## Stage 2 — picker widget (not started)

`ui/taxon_picker.py`: the field, a popup with a two-line delegate and source
chips, the first-time note, the card with Kilde, and AI chips. It emits
picks; it never writes identity. Add review-renderer scenarios and translations.

## Stage 3 — Edit Observation integration (not started, human-gated)

Replace the four rows. Route picks through one narrow controller method that
reuses the Scientific pick commit; free text and ✕ use the existing paths.
The acceptance criteria and the human checklist are in the design proposal.

## Open questions for Stage 2

See the Stage 1 report in the PR description.
