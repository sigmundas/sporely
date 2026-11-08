# Reference measurement tables and reported statistics

## Current stage / handoff — 2026-09-09

Stage 1: bounded parser extension, implemented locally; human verification pending.
Branch: `feature/reference-measurement-table-parser`.
Base SHA: `40022707108410ca674b04fdcc5b8149c857d75e`.
Candidate SHA: none; changes remain uncommitted under the interactive-behavior gate.

User prompt: accept `(7.5) 8.4–13.0 (13.2)x(5.0) 5.1–7.2 (7.6),
q=(1.30) 1.42–1.96 (2.07)` in Add Reference and pasted Hebeloma tables,
including mean, median and S.D. Clarification: an explicit `(min) 5%-95%
(max)` heading describes actual percentiles, not traditional typical ranges.
The pasted heading can be glued together as `Spore(min) 5%-95% (max)meanmedianS.D.`.

### Implementation

- `references/measurement_parser.py`: `SummaryStatistics` and
  `MeasurementParseResult` capture mean/median intervals, scalar SD and explicit
  `range_semantics=percentile_05_95`; unknown semantics remain `unspecified`.
  `_PAREN_RANGE_RE` accepts optional dashes inside extreme parentheses.
  `_parse_table` recognizes labelled Length/Width/Q rows after clipboard
  whitespace normalization, including Markdown separators. Headerless tables
  assume Hebeloma's range/mean/median/SD ordering; explicitly different summary
  ordering is rejected. Malformed/duplicate rows warn rather than consuming
  arbitrary extra numbers. `parse_measurement_string` reuses `_normalise` and
  `_parse_range`; `swap_length_width` carries statistics and semantics.
- `tests/test_measurement_parser.py`: exact compact example, clipboard shapes,
  Markdown with/without regular headings, glued statistical heading, separate
  summary intervals, swap, malformed rows, reordered columns and unlabelled
  range semantics.
- `tests/test_reference_entry_editor.py::test_hebeloma_table_parse_populates_ranges_and_preserves_source`
  exercises existing `_on_parse_measurement_clicked` / `_reference_record_data`
  and verifies bounds, no invented scalar Q mean/median, retained original
  source and extra-statistics warning. No production UI modules changed.

Parser statistics are **not yet persisted as structured reference fields**.
Existing raw-source preservation remains in place. The parser's explicit
percentile notice appears through the existing warning preview; existing table
headers still say Typical. No GUI layout changes or screenshot evidence.

### Verification

- `./.venv/bin/pytest -q tests/test_measurement_parser.py tests/test_reference_entry_editor.py tests/test_add_reference_dialog.py`: 102 passed.
- `py_compile` on parser and both touched tests; `git diff --check`: passed.
- Manual verification (not established by model-level Qt tests):
  1. Add Reference → Enter manually: paste compact example and parse; check
     L extremes 7.5/13.2, inner 8.4/13.0; W 5.0/7.6, inner 5.1/7.2;
     Q 1.30/2.07, inner 1.42/1.96.
  2. Paste the full headed table directly from the browser and parse; verify
     identical bounds and explicit 5th–95th-percentile notice. Verify the
     extra-statistics limitation notice is readable. Headerless pasted rows
     must not claim a known percentile interpretation.
  3. Paste the supplied Markdown table; verify the same dimensions. Save and
     reopen the reference, checking the source text remains available. Dedicated
     editable statistics and persisted semantic labels are not implemented yet.

### Follow-up — 2026-09-10: exact velutipes Markdown

- Confirmed glued headings already capture reported mean intervals, not scalar
  means. Exact supplied Markdown exposed a rejected Q row caused by `<br>`.
- `_normalise` now treats HTML line breaks (`<br>`, `<br/>`, `<BR />`) as
  whitespace. Existing `_parse_table` and source retention paths are reused.
- Added `test_velutipes_markdown_mean_ranges_with_html_break` covering all
  three mean intervals, Q median/SD/extremes, no inferred scalar, retained raw text.
- Verification: focused parser/editor/dialog suite **107 passed**; parser/test
  py_compile and git diff --check passed. No UI layout changes.
- Candidate SHA: none; existing stage remains uncommitted and human-gated.
  Manual follow-up: paste the exact velutipes Markdown including `<br>`;
  verify Q bounds populate and no Q-row parse warning appears. Mean cells
  remain blank because the source reports intervals. Existing save/reopen
  verification above remains pending.
- Structured interval persistence/display remains deferred to Stage 2.

## Stage 2 — durable reported statistics and semantic display (not implemented)

Design expanded for independent review in
[Reported statistics and explicit range semantics](2026-09-10-reported-statistics-and-range-semantics.md).
That document is the canonical plan for Stage 2 and subsequent work; this file
retains the parser-stage verification record.

Read-only planner found a persistence boundary: legacy reference_values has
metadata_json, but normalized MeasurementSet only has raw_text and an explicit
field set. Adding a legacy metadata key would silently drop the structure at
normalized attachment/sync. Relevant paths: database/reference_library.py
MeasurementSet/schema; ui/reference_entry_editor.py normalized_measurement_set_payload;
reference_sync_state._LIBRARY_PAYLOAD_COLUMNS and
reference_sync_reconciliation._PAYLOAD_COLUMNS. Read sync rules and applicable
Supabase skill before changing any cloud contract.

Plan a typed, validated source-statistics representation and range semantics
for the normalized model (dedicated fields or a versioned structured source
snapshot), migrations/round trips, editor/prefill, previews/citations and sync.
Keep reported mean/median intervals distinct from scalar species means and
observed medians. Preserve SD as reported; do not reinterpret it as CV or
infer its population. Explicitly distinguish source facts from manual edits
and swapped dimensions so edits cannot leave a misleading percentile marker.
Update visible Typical labels only when durable semantics justify percentiles.
Requires a fresh scoped implementation stage and independent final review;
do not treat parser stage completion as completion of this overall request.
