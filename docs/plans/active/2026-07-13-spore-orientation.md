# Spore orientation plan

Status: Active; no implementation of the orientation model was found in the current repository.

## Agent handoff

- Status: Proposed.
- Last completed stage: None verified.
- Current/next stage: Phase 1 — database and data model.
- Important decisions: Exactly two user-facing values (`Face` and `Side`); legacy nulls remain internal compatibility data and are never presented as a third option.
- Do not: Pool face/side statistics, infer legacy orientation, add a 3D model, or redesign unrelated measurement UI.
- Remaining acceptance criteria: The phase-specific tests and final reporting requirements below.

Implement orientation-aware spore measurements in `sporely-py`.

The feature has exactly two user-facing orientation values:

- Face
- Side

Do not add an “Unspecified” option anywhere in the UI.

This is not a true 3D measurement tool. Each rectangle still measures two dimensions, `length × width`. The orientation determines what the transverse dimension means:

- Face: length × breadth in face view
- Side: length × width/thickness in side view

Do not attempt to pair face and side measurements as dimensions of the same physical spore.

Work in three strict phases:

1. Database and data model
2. Measure interface
3. Analysis/plot interface, including reference data

Finish and test each phase before starting the next. Do not redesign unrelated parts of the application.

## Phase 1 — database and data model

First inspect the current local database schema, cloud schema, migrations, measurement models, sync payloads, import/export paths and tests. Identify every path through which measurements and reference values are stored or synchronized.

### Individual measurements

Add a language-neutral field to spore measurement records:

```text
spore_view = "face" | "side"
```

Requirements:

* New spore measurements must always be saved as either `face` or `side`.
* Non-spore measurements should not use this field.
* Store stable internal values `face` and `side`; do not store translated labels.
* Include the field in all relevant local/cloud serialization, sync, comparison, merge, export and import paths.
* Changing the view of a measurement must count as a real measurement change and trigger any necessary statistics or summary refresh.
* Make sync backward-compatible with older rows and older clients as far as practical.

Do not guess the orientation of existing measurements.

For migration safety, the database column may remain nullable only for pre-existing legacy rows. This is an internal legacy condition, not a third application state:

* Never display “Unspecified.”
* Never offer NULL as a selectable value.
* Never create a new spore measurement with NULL.
* Legacy NULL rows must not silently be treated as Face or Side.
* Preserve all existing measurements without destructive backfilling.

Add an appropriate database constraint, for example permitting only:

```text
NULL
face
side
```

where NULL is accepted solely for legacy compatibility.

Update local and cloud migrations where applicable.

### Reference-data model

Extend reference data so one reference dataset can hold:

```text
Length
Face breadth
Side width
Q face
Q side
```

The statistical fields should follow the existing reference-value structure, including whichever of these the application currently supports:

```text
extreme minimum
typical minimum
mean
typical maximum
extreme maximum
```

Use stable internal dimension identifiers such as:

```text
length
face_width
side_width
q_face
q_side
```

The exact schema should follow the existing architecture. Do not create a parallel storage system if the current reference model can be extended cleanly.

Preserve:

* the original literature string;
* the source;
* existing ordinary two-dimensional reference records;
* backwards compatibility.

Do not automatically classify old two-dimensional reference widths as Face or Side. They may remain internally legacy/unclassified until edited or explicitly assigned, but “Unspecified” must not become a UI option.

Add migration and model tests covering:

* a legacy database opening without data loss;
* Face measurement round-trip;
* Side measurement round-trip;
* local/cloud serialization;
* sync in both directions;
* editing only the orientation;
* legacy NULL rows;
* rejection of invalid orientation strings.

Stop after Phase 1 if its tests do not pass.

## Phase 2 — Measure interface

Use the existing Measure screen shown in the supplied screenshot.

Do not add anything to the crowded left-hand Measure panel.

### Primary control

Add a compact segmented control to the right-hand `SPORES FINE TUNE` panel for the currently selected spore measurement:

```text
VIEW
[ Face ] [ Side ]
```

Requirements:

* Exactly two choices.
* No Unspecified choice.
* Use localized display labels, but store `face` and `side`.
* Changing the selected value updates the measurement immediately and safely.
* The control appears only for spore rectangle measurements.
* It should not affect Line, Multi-line or non-spore measurements.

### Creating a measurement

The first time the user draws a spore rectangle and no active orientation has yet been chosen:

* Show a small contextual chooser near the newly drawn rectangle:
  `[ Face ] [ Side ]`
* Do not permanently save the new measurement until one has been chosen.
* Keep the rectangle selected while waiting.
* Escape or cancellation should discard the incomplete rectangle cleanly.

After a choice is made:

* Save the measurement with that view.
* Remember the most recently selected view for the current measuring session.
* Subsequent spore rectangles inherit that view automatically.
* The user can change the active orientation before drawing more spores using the Fine Tune control or keyboard shortcut.

Add keyboard shortcuts while measuring spores:

```text
F = Face
S = Side
```

Do not trigger shortcuts while the user is typing in a text field.

### Canvas and table display

Avoid permanent clutter on the microscopy image.

Show an `F` or `S` indicator only where useful:

* on the selected rectangle;
* while hovering a rectangle;
* in the measurements table.

Add a compact `VIEW` column to the measurements table for spore rectangles:

```text
IMG   CAT      VIEW   L     W
4     Spores   F      4.6   4.0
4     Spores   S      4.9   4.3
```

Do not attach persistent Face/Side pills to every rectangle.

Legacy measurements with no stored view:

* must remain visible;
* must not be relabelled automatically;
* should show a neutral legacy marker such as `—` in the table rather than the word “Unspecified”;
* should require choosing Face or Side before orientation-aware editing or plotting.

Make sure changing Face to Side or Side to Face updates all derived statistics and dirty/sync state.

Add tests for:

* first rectangle requiring a choice;
* sticky orientation for subsequent measurements;
* F and S shortcuts;
* cancelling before selection;
* changing orientation in Fine Tune;
* table rendering;
* selected/hover badges;
* legacy rows;
* no new NULL measurements;
* no user-facing “Unspecified” label.

Stop after Phase 2 if its tests do not pass.

## Phase 3 — Analysis and plot interface

Update the Analysis screen shown in the supplied screenshot.

This phase includes:

* observation measurements;
* plots;
* histograms;
* Q statistics;
* gallery;
* statistics text/export;
* literature reference values;
* reference parsing and editing.

### Plot orientation selector

For the Spores category, add a compact selector near the main plot controls:

```text
View: [ Face ] [ Side ]
```

There must be exactly two plot modes:

* Face
* Side

Do not add:

* Unspecified
* All
* Both
* a combined orientation mode
* a 3D plot

Default behavior:

* Prefer the last selected analysis view.
* Otherwise select Face if Face data exists.
* Otherwise select Side.
* If the selected orientation has no data, show a clear empty state rather than silently falling back or mixing data.

### Face plot

Face mode uses only Face measurements:

```text
X = Length (µm)
Y = Breadth — face view (µm)
Qf = Length / face breadth
```

The plot, confidence ellipse, Width histogram and Q histogram must all be calculated only from Face measurements.

Use labels such as:

```text
Breadth — face view (µm)
Qf (L/Bf)
```

### Side plot

Side mode uses only Side measurements:

```text
X = Length (µm)
Y = Width — side view (µm)
Qs = Length / side width
```

The plot, confidence ellipse, Width histogram and Q histogram must all be calculated only from Side measurements.

Use labels such as:

```text
Width — side view (µm)
Qs (L/Ws)
```

Never pool Face and Side transverse dimensions into one ellipse, histogram or Q distribution.

Legacy measurements without a view:

* must not be included in either plot;
* must not be silently assigned;
* should produce a small warning such as:
  `Some legacy measurements need a Face or Side assignment.`
* do not use the word “Unspecified” as a selectable state.

### Gallery

In the spore gallery, show a small orientation badge:

```text
F  12.4 × 8.2
S  12.6 × 7.1
```

The gallery should follow the selected Analysis view, showing only Face or only Side measurements.

Do not add another orientation filter if the main Face/Side plot selector already controls it.

### Dataset and reference overlays

Keep the existing dataset/source color system.

The selected Face/Side view determines which measurements and which reference dimensions are plotted.

For a three-dimensional reference such as:

```text
11.5–14.5 × 5.5–9.5 × 5.5–8 µm
```

interpret it as:

```text
Length × face breadth × side width
```

Therefore:

* Face plot uses the reference Length and Face breadth ranges.
* Side plot uses the same reference Length and Side width ranges.
* Face mode uses Q face values.
* Side mode uses Q side values.

Do not show both reference rectangles simultaneously.

A reference dataset should remain one item in the dataset list. Switching Face/Side changes the dimensions used for its envelope.

Legacy two-dimensional reference data without an assigned orientation must not be silently shown in either mode. Indicate that the reference needs a Face or Side assignment.

### Reference editor

Extend the existing `Edit selected reference data` dialog without making it substantially more cluttered.

For references containing both orientations, expose two compact sections or a Face/Side segmented selector inside the existing Min/max and Spore data workflows:

```text
[ Face ] [ Side ]
```

Face fields:

```text
Length
Breadth — face
Qf
```

Side fields:

```text
Length
Width — side
Qs
```

Length is conceptually shared for three-dimensional literature strings. Avoid creating contradictory duplicate length values unless the source explicitly provides orientation-specific length statistics.

For a simple two-dimensional reference string, require the user to classify it as Face or Side before saving orientation-aware data. Offer only:

```text
[ Face ] [ Side ]
```

No Unspecified option.

### Literature parser

Extend the current measurement-string parser.

It must parse:

```text
11.5–14.5 × 5.5–9.5 × 5.5–8 µm
11.5–14.5 × 5.5–9.5 (f) × 5.5–8 µm (s)
11.5–14.5 × 5.5–9.5 (face) × 5.5–8 (side)
```

For an unmarked three-dimension string, show a visible confirmation message:

```text
Interpreted as length × face breadth × side width.
```

Recognize common aliases case-insensitively.

Face aliases should include at least:

```text
f
face
face view
front
frontal
frontal view
frontalansicht
frontansicht
framifrån
forfra
vue de face
aspectu frontali
```

Side aliases should include at least:

```text
s
side
side view
profile
profile view
lateral
seitenansicht
från sidan
fra siden
vue de profil
vue latérale
aspectu laterali
```

Store normalized values only as `face` and `side`.

Preserve the original pasted string exactly.

### Q handling

Keep Face and Side Q statistics separate:

```text
Qf = L / face breadth
Qs = L / side width
```

The parser should support literature that provides:

```text
Q
Qm
Qf
Qs
Q1
Q2
```

Do not assume naming conventions are globally consistent.

When only `Q` or `Qm` is present in a three-dimensional string:

* compare the supplied value with the parsed dimension means/ranges where possible;
* suggest whether it appears to refer to Face or Side;
* show the interpretation to the user;
* allow correction before saving;
* do not silently assign when the basis is ambiguous.

### Statistics and export

Statistics must remain separate by orientation.

The detailed export should be able to produce sections such as:

```text
Face view, n = 30
L = …
Bf = …
Qf = …

Side view, n = 18
L = …
Ws = …
Qs = …
```

The compact combined taxonomic output may use:

```text
Basidiospores 11.5–14.5 × 5.5–9.5 (f) × 5.5–8.0 (s) µm
```

Do not report a single pooled Width, Q or confidence ellipse across both orientations.

### Tests

Add or update tests for:

* Face plot filtering;
* Side plot filtering;
* switching plot mode;
* separate confidence ellipses;
* separate width histograms;
* separate Qf and Qs statistics;
* gallery filtering and badges;
* three-dimension parser;
* explicit `(f)` and `(s)` parsing;
* multilingual aliases;
* unmarked three-dimension confirmation;
* reference Face envelope;
* reference Side envelope;
* simple two-dimensional reference requiring Face or Side;
* legacy reference behavior;
* statistics export;
* no combined or pooled mode;
* no user-facing Unspecified option.

## General constraints

* Do not add a third orientation state to the UI.
* Do not rename the existing generic measurement `width_um` unless a broader migration is genuinely required.
* Do not implement a 3D scatter plot or estimated spore volume.
* Do not pair Face and Side records as one physical specimen.
* Do not clutter the left Measure panel.
* Do not mix Face and Side values in calculations.
* Do not destructively classify legacy data.
* Keep keyboard navigation and high-DPI rendering working.
* Follow the existing visual components, spacing, fonts and localization conventions.
* Avoid unrelated refactoring.

Run the complete relevant test suite after all three phases.

At the end, report:

* schema and migration decisions;
* legacy-data handling;
* every changed file;
* tests added or changed;
* commands run and results;
* any unresolved edge cases;
* screenshots of the Measure and Analysis interfaces showing both Face and Side states.
