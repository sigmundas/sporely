# Reference data dialogs

> **Scope:** Sporely desktop now ships two parallel reference-data
> workflows that intentionally coexist:
>
> 1. The legacy **Reference Values** panel/dialog, backed by the
>    single-row-per-species `reference_values` table (documented in the
>    "Reference Values dialog" and "Add/Edit reference data dialog"
>    sections below). Unchanged in this pass.
> 2. The normalized **Reference Library manager**
>    (`ui/reference_library_manager_dialog.py`) backed by
>    `reference_works`, `reference_taxon_treatments`,
>    `reference_measurement_sets`, and the observation-side
>    `observation_reference_uses` link table (documented in the
>    "Reference Library manager" and "Attach Library Reference"
>    sections below).
>
> Both workflows are available side-by-side. The legacy panel remains
> the default for single-record edits; the normalized manager is used
> to curate multi-treatment publications and to attach a shared
> measurement set to one or many observations.

Figure 1 placeholder: overview of the Reference Values dialog.

These dialogs are used to review, add, edit, and plot stored spore reference data.

## Reference Values dialog

- Use this dialog to browse an existing reference record for a genus/species.
- You can edit source, mount medium, stain, and percentile values.
- `Plot` sends the current values to the analysis plot.

## Add/Edit reference data dialog

Figure 2 placeholder: overview of the Add/Edit Reference Data dialog.

- `Min/max` tab: enter stored percentile values for length, width, and Q.
- `Spore data` tab: paste raw measurements to create a custom point set.
- `Parmasto Biometrics` tab: enter species-level summary values if available.

## Typical workflow

1. Choose the species.
2. Fill either percentile values, raw spore points, or Parmasto biometrics.
3. Add a source label.
4. Save the record or plot it in Analysis.

## Notes

- `Min/max` is for stored summary ranges.
- `Spore data` is for a custom measured point set.
- The add/edit dialog is compact by design and is mainly for fast data entry.

Figure 3 placeholder: spore-data entry tab.

## Reference Library manager

The Reference Library manager (`ReferenceLibraryManagerDialog`) is a
standalone top-level dialog opened from the reference panel via
**Manage reference library…** or from the Attach Library Reference
chooser via **Manage library…**.

It presents three panes:

1. **Publications** — a searchable table of `reference_works`.
   Substring search matches title, short label, container title, and
   authors. A colored badge (`Incomplete`, `Unverified`, `Verified`)
   surfaces the work's `verification_status`.
2. **Treatments and measurement sets** — a hierarchical tree of
   `reference_taxon_treatments` under the selected work, with each
   treatment's `reference_measurement_sets` shown as children.
3. **Details** — the selected record's fields plus context actions
   (Edit, New treatment, New measurement set, Attach to active
   observation when applicable). Measurement-set details are composed
   through `build_observation_reference_snapshot`, so no UI-side
   fabrication of missing values occurs.

Create/edit forms delegate to the existing repositories:

- `ReferenceWorkRepository.create` / `update` — UUID assigned on create,
  revision preserved and bumped on update.
- `TaxonTreatmentRepository.create` / `update` — inherits
  `reference_work_id`; separate UUID + revision.
- `MeasurementSetRepository.create` / `update` — offers only the
  plot-supported kinds (`range`, `summary`, `raw_points`) for new
  records. Existing `parmasto` records remain visible and editable but
  cannot be created from the UI.

Rules:

- Empty numeric inputs map to SQL `NULL`. The manager never fabricates
  zero, midpoint, or synthesised bound values.
- Editing an existing `raw_points` record preserves any aggregate
  statistics already stored on the row; only an explicit kind
  conversion clears them.
- The detail panel shows a translated "not plottable yet" hint when the
  selected measurement set has no drawable geometry (mirroring the
  translator's finite-positive rule so the desktop plot layer would
  reject it).

## Attach Library Reference

The Attach Library Reference dialog (`ReferenceLibraryAttachDialog`)
lists unattached measurement-set candidates and lets the user pick a
role (`Compared`, `Supports identification`, `Contradicts`) before
attaching to the active observation.

- The **Manage library…** button opens the Reference Library manager as
  a child modal; on close the candidate list refreshes automatically so
  newly-created measurement sets appear immediately.
- The chooser only lists kinds the desktop can plot; a persisted use
  whose snapshot cannot be plotted appears in the reference-series
  table as a visible warning row with a working Detach action
  (see `references/reference_plotting.py`).
