# Design contract — Add reference dialog

Status: approved target for the Add reference redesign, 2026-09-17.

This contract accompanies the approved mockups created before implementation.
The mockups are visual targets; this file states the behavior and scientific
semantics that are not negotiable.

## Non-negotiable requirements

### Dialog and source navigation

**N1.** Keep one Add reference dialog with the four source tabs:
`Library`, `Community`, `My observations`, and `Enter manually`.

**N2.** The comparison/review pane stays visible beside the source pane. The
comparison is the content: the user should be able to inspect a source against
their own observation before committing it.

**N3.** Grouping by relevance and multi-select are required behavior in the
Library list. They are not optional display modes.

### Library rows

**N4.** Taxon is the row headline: italic and visually primary. Citation/source
is smaller grey metadata on the second line.

**N5.** The measurement expression is right-aligned in a monospace face on the
first line. Reserve its width first; the taxon gets the remaining width. The
measurement must not be truncated to make the taxon fit.

**N6.** Library rows are grouped, in this fixed order:

1. `This taxon`
2. `Same genus`
3. `Rest of library`

Each heading shows a count. Empty groups disappear.

**N7.** Row selection and checkbox state are different:
- checkbox = source will be added to the plot;
- selected row = source shown in the right-hand preview;
- clicking a checkbox also selects that row;
- clicking the row must not automatically check it.

**N8.** The selected row uses the approved custom state: a narrow green left
bar plus pale green fill. Qt's default blue selection highlight must not show.

**N9.** Relevance badges are separate from data-semantics badges:
- `Same taxon`
- `Same genus`

### Scientific/data labels

**N10.** User-facing data labels must describe what the source actually
contains. The core labels are:

- `Raw data`
- `Published range`
- `5–95% range`

Do not expose a vague `Summary stats` category in this UI.

**N11.** `Parmasto` is provenance/method, not a generic data type. A Parmasto
reference may show `5–95% range` when that is what its stored statistics mean;
the Method or Provenance tab explains the source/method.

**N12.** Use `5–95% range` only when the stored/reported semantics explicitly
identify the bounds as the 5th and 95th percentiles. Do not infer percentile
meaning from an ordinary inner/typical range.

**N13.** Use `Published range` for a published range whose bounds are not
explicitly a 5–95 percentile interval and which is not raw individual data.
The internal model may preserve more specific semantics such as typical range;
the compact list badge does not erase those stored semantics.

**N14.** If a source explicitly reports a different percentile interval, do not
mislabel it as `5–95% range`. Show the actual bounds (for example
`10–90% range`) or another equally explicit label.

### Statistical honesty

**N15.** Never promote a derived value to a reported statistic.

In particular:
- a range midpoint is not a median;
- a range midpoint is not a mean;
- a typical range is not automatically a 5–95% range;
- a 5–95% range is not automatically a confidence interval;
- a published range does not imply raw observations;
- min/max and 5–95% must remain distinguishable when both are available.

Derived values may be shown only when they are explicitly labelled as derived.

**N16.** A comparison delta such as `yours +0.2 µm` or `same median` may be shown
only when genuinely equivalent statistics are being compared.

### Comparison pane

**N17.** The Summary tab shows three stacked comparisons: Length, Width, and Q.

**N18.** The source and the user's observation must be visually distinct:
filled source band versus outlined observation band, following the approved
mockup. Do not rely on text alone to distinguish them.

**N19.** Axis domains remain fixed while the user moves between sources in one
dialog session. Do not auto-scale per selected row. The actual domains may be
derived once when the dialog opens from sensible metric bounds plus available
data, then frozen for the session.

**N20.** A source's visual shape must reflect the semantics actually stored:
- raw individual data may have statistics calculated from the actual points;
- a published range draws the published range and does not invent a centre;
- an explicit 5–95% range remains distinguishable from min/max when both exist;
- a reported mean/median/mean interval is shown only when genuinely present.

**N21.** With no source row selected, the right pane is not blank and does not
show a table of dashes. It shows the user's own observation as the baseline,
plus short copy explaining that selecting a source will compare against it.

### Raw spores tab

**N22.** Never synthesize per-spore rows merely to make the Raw spores tab look
populated.

For a source with no individual measurements, show an honest state such as:
`Individual spore measurements were not published for this source.`

Where useful, add:
`The published range will be plotted as a range band.`

Where real per-spore data exists, show the actual data. Any reconstruction from
paired reported quantities must preserve their dependency; do not independently
sample plausible length, width and Q values.

### Manual entry

**N23.** Manual entry is paste-and-parse first. People normally copy a
measurement expression from a publication; they should not have to start by
typing fifteen individual values.

**N24.** The manual numeric grid is three metric rows by five plain input
columns, not a table widget:

`extreme min | typical min | mean | typical max | extreme max`

Tab order is explicitly left-to-right across each row.

**N25.** Do not reinterpret `typical min/max` as 5th/95th percentiles. When the
input explicitly identifies a 5–95% interval, preserve that meaning in the
stored content and in the preview.

**N26.** The amber no-taxon warning represents a real identity gap and remains
meaningful, not decorative.

### Footer

**N27.** Footer text states what will happen. Multi-select counts up:
`Add to plot` → `Add 3 to plot`.

Prefer `2 sources selected` over `2 sources ready`, because it corresponds
directly to the checked rows.

**N28.** `Save to library` belongs only to the manual-entry tab. It is hidden,
not disabled, on the other tabs.

### Existing behavior that must survive unless explicitly superseded

**N29.** Community search continues to support genus-only search and refresh
while typing.

**N30.** My observations continues to support genus browsing when no species is
identified.

**N31.** The dialog remains genuinely resizable; the manual editor scrolls when
needed and neither splitter pane can be collapsed away.

**N32.** `+ New publication…` remains deferred out of the selection signal and
one click may queue at most one editor. Cancel closes it on the first click.

**N33.** New or changed user-visible strings are translated for nb_NO, sv_SE and
de_DE using the repository localization workflow. Existing finished
translations must not be overwritten by stale catalogs.

## Decision principles

When a detail is not covered explicitly, decide in this order:

1. Taxon leads; citation is metadata.
2. Relevance ordering beats alphabetical or chronological ordering.
3. The comparison is the content — show it before the user commits.
4. Raw data, a published range and a 5–95% range are different things; show
   what the source actually contains.
5. Never present a derived quantity as though the publication reported it.

Sample taxa, citations and numeric values in the mockups are illustrative.
Use repository fixtures and real stored semantics in implementation and tests.
