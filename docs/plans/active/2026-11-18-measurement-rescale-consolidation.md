# Calibration measurement and rescale consolidation

Source: external review (its R1), verified on 2026-11-18 against `main` at
`a2cc84f`. Line numbers are as of that commit. The desktop UI redesign plan
(`2026-11-08-desktop-ui-redesign.md`) also records that "measurement rescaling
exists four times"; this plan is where that gets fixed.

## Verified findings

### Calibration (scale-bar) measurement upsert — three copies

`ui/main_window.py:23056` `_upsert_calibration_measurement`,
`ui/image_import_dialog.py:2338` `_upsert_calibration_measurement`,
`ui/observations_tab.py:14031` `_upsert_calibration_measurement_for_image`.

| | main_window | image_import_dialog | observations_tab |
|---|---|---|---|
| DELETE predicate | `measurement_type = 'calibration'` | same | `COALESCE(LOWER(measurement_type),'') = 'calibration'` |
| Width-line half-width | `pixel_dist / 20` | `pixel_dist / 20` | `length_px / 20` |
| Coordinate scaling | none | none | `coordinate_scale` factor |

All three DELETE through a raw `get_connection()` and commit, then call
`MeasurementDB.add_measurement` (`database/models.py:3924`), which opens its own
connection and commits. **Two transactions**: a failure between them leaves the
image with no calibration row. No `try/finally`.

### Measurement rescale — five implementations

| Path | Prompt | Rescales calibration rows | Rewrites `Q=` notes | Updates `images.scale_microns_per_pixel` | Marks observation `dirty` |
|---|---|---|---|---|---|
| `MainWindow._maybe_rescale_current_image` (`main_window.py:22526`) | yes | yes | yes (recomputed from points) | no | not in body |
| `ImageImportDialog._rescale_measurements_for_image` (`image_import_dialog.py:5296`) | no | yes | yes | yes | not in body |
| `ObservationsTab._rescale_measurement_lengths` (`observations_tab.py:14094`) | no | **no** | **no** | no | not in body |
| `CalibrationDB.recalculate_measurements_for_calibration` (`models.py:5404`, via `CalibrationDialog._prompt_recalculate_measurements`) | in dialog | — | — | yes | **yes** |
| `recalculate_measurements_for_objective` (`models.py:5354`) | — | — | — | yes (per objective) | — |

Only the `models.py` calibration path marks observations dirty. **Unverified
and important:** whether the callers of the three UI paths mark the observation
dirty some other way. If not, rescaled lengths may never push to the cloud.
Checking that is Stage 0's first task, and the answer decides whether this is a
sync bug fix or a pure refactor.

No test exercises the three UI upsert or rescale bodies;
`tests/test_microscope_settings_refresh.py:291-294` only stubs them out.
`MeasurementDB` has add/get/delete but no update or replace method, so the UI
paths bypass it for every DELETE and UPDATE.

## Stage 0 — Characterize (no behaviour change)

- Trace each UI rescale/upsert caller for `dirty` marking and sync side effects.
  Read `.claude/rules/cloud-sync.md` first.
- Add characterization tests that drive each of the five paths against a temp
  SQLite DB and pin today's behaviour (rows, notes, image scale, dirty flag).
- Write up for the owner which differences look deliberate and which look like
  drift (questions below). Stop here for answers.

### Owner decisions

1. Half-width of the calibration width line: `pixel_dist / 20` or
   `length_px / 20`? (Affects only the overlay geometry of new calibrations.)
2. Should rescaling touch calibration rows?
3. Should rescaling rewrite `Q=` notes everywhere?
4. Which rescale paths should update `images.scale_microns_per_pixel`?
5. If Stage 0 finds UI rescales not marked dirty: fix that here? (Sync-adjacent,
   so it needs your go-ahead under AGENTS.md.)

### Stage 0 results (2026-11-19)

Characterization tests: `tests/test_measurement_rescale_characterization.py`
(renamed to `tests/test_measurement_rescale_rules.py` when the rules landed)
(real SQLite; UI methods called unbound with a minimal `self`).

- **Dirty marking, normal flows: fine.** `ImageDB.update_image` and
  `MeasurementDB.add_measurement` both mark the observation dirty
  (`_touch_observation`). The main_window rescale calls `update_statistics`,
  which persists the spore summary through
  `ObservationDB.update_spore_statistics` (dirty when the summary changes),
  and is always followed by `update_image` in `apply_objective` /
  `set_custom_scale`; the
  observations_tab rescale is followed by `update_image(..., scale=...)` in
  `_apply_import_results_to_observation`; all three upserts go through
  `add_measurement`.
- **Dirty marking, one gap (suspected defect).** Changing the objective in
  Prepare Images runs `_apply_settings_to_indices` →
  `_rescale_measurements_for_image`, which commits rescaled measurements *and*
  the new `images.scale_microns_per_pixel` immediately, without marking the
  observation dirty. If the dialog is then closed without applying, the local
  DB keeps the change and a cloud-linked observation does not push it until
  something else dirties it. Pinned by
  `test_image_import_dialog_rescale`; not yet reproduced through the real dialog.
- **Notes overwrite (suspected defect).** The image_import and main_window
  rescales set `notes = "Q=…"`, replacing any free text in the measurement's
  notes and the calibration row's `Scale bar: … µm` note.
- **Calibration rows.** image_import and main_window rescale them (main_window
  recomputes from points, so a calibration row's length becomes
  `pixels × new scale`); observations_tab skips them.
- **Width-line drift is real but conditional.** It only differs when
  `pixel_dist` ≠ the selection length. On the traced paths `pixel_dist` is the
  distance between the selection's own two points (`image_import_dialog.py`
  `_on_scale_bar_endpoint_moved` and the click handler near `:6572`;
  `main_window.py` calibration click handler), so the copies agree there. The
  other `_scale_bar_pixel_distance` assignments (`image_import_dialog.py:6485`,
  `:6529`) are not traced.
- **Case drift is real.** With a `Calibration` (capitalised) row, main_window and
  image_import leave it and add a second calibration row; observations_tab
  replaces it.
- **Two-transaction upsert confirmed:** a failing insert after the DELETE leaves
  the image with no calibration row.
- `CalibrationDB.recalculate_measurements_for_objective` has no callers. The
  stale-code sweep (PR #77) deletes it; once that lands, four rescale paths
  remain. If #77 has not landed when Stage 2 starts, delete it there.

## Decisions (owner, 2026-11-20)

1. Cross tick: keep today's look; size it from the selection (identical to
   `pixel_dist` on every traced path).
2. A rescale never touches the scale-bar calibration record.
3. A rescale never rewrites notes. Q is unchanged when length and width scale
   together, and notes also hold multiline paths.
4. Rescale mechanics (image scale, transaction) are an implementation detail.
5. Fix the Prepare Images sync gap.

Found while explaining the questions, confirmed against a test database:
the Prepare Images rescale replaced multiline paths with `Q=0.0` (data loss),
and the Measure tab rescale skipped multiline measurements (length kept the old
scale).

Re-calibration scope (2026-11-21, `claude/recalibration-scope-031114`, waits
for the owner's layout opinion): re-calibrating an objective now asks
`RecalibrationScopeDialog` (`ui/recalibration_scope_dialog.py`) instead of a
yes/no. Choices, as the owner specified: *new calibration only*; *update
existing images* (every image on the previous calibration, the old "yes" and
the default); *update recent images* (taken today or in the previous N days).
The dialog lists the affected observations and images with counts, from the
same query the update runs (`CalibrationDB._recalibration_image_rows`).
Decisions made while building it:

- **Which date.** The day the image was taken (`images.captured_at`, EXIF
  local time), or the local day it was added (`created_at`, stored in UTC)
  when it has no capture date; the list shows those dates in italics. Not the
  observation date: in the owner's data, microscope photos are often taken one
  to two weeks after the observation date, and ~90% of microscope images have
  a capture date.
- **Scope of "recent".** A subset of *update existing images*: only images on
  the previous calibration, never images on older calibrations of the same
  objective.
- **Images left out** keep their `calibration_id` (the previous calibration)
  and their scale: their measurements still were made with it.
- Cancel changes nothing, like *new calibration only*.

**How much an image changes** (fixed 2026-11-21, after PR #82). The update
used to set every chosen image's scale to the objective's new scale. It now
multiplies each image's scale and measurements by the calibration's own
change (new scale ÷ the old calibration's stored scale), so an image keeps its
relation to the calibration. In the owner's data (read-only check) no
scale-bar image is linked to a calibration (those are saved as *Custom* with
no `calibration_id`). Of ~6,350 calibrated images with a scale, the ones
whose scale differs from their own calibration were (2026-11-21): 507 with
another calibration's scale of the same objective (0.2–1.1% off), 8 resampled
on import (scale = calibration ÷ resample factor), 7 with another objective's
calibration scale (for example 63X scales on 40X images) and 6 others. The old
rule reset all of these to the objective scale, which would have changed the
measurements of the resampled and other-objective images by roughly 15–60%.
Existing offsets are kept, not repaired: which scale such an
image should have is not knowable from the database. With no usable old
calibration scale, images are only relinked.

## Stages 1–3 — landed together (2026-11-20)

The decisions came before Stage 1, so the "reproduce today's behaviour, then
unify" split no longer applied; the three stages landed as one change on
`fix/measurement-rescale-consolidation`.

- `MeasurementDB.replace_calibration_measurement`: one transaction,
  case-insensitive delete, tick from the selection. Replaces the three UI copies.
- `MeasurementDB.rescale_image_measurements` (and `_rescale_measurements_on_cursor`):
  ratio rescale whose denominator is the image's *stored* scale (the caller's
  scale only when none is stored), skips calibration rows, keeps notes,
  includes multiline, stores the new image scale in the same transaction, and
  marks the observation dirty only when a value changed. Used by the Measure
  tab and Prepare Images (objective change and apply);
  `CalibrationDB.recalculate_measurements_for_calibration` uses the same
  row rule.
- The Measure tab now rescales by ratio instead of recomputing from the four
  points; that is what makes multiline measurements rescale.
- `MeasurementDB.has_rescalable_measurements` decides whether to ask
  "update previous measurements?" on all paths: an image with only multiline
  measurements now asks; one with only a calibration record does not.
- The Prepare Images objective change still commits before apply, and closing
  the dialog without applying keeps the rescale and the new image scale (the
  objective name updates only on apply). It is now marked dirty, so it syncs.

Codex challenge findings on this change (2026-11-20):

- Fixed: with a ratio rescale, the Measure tab's cached scale could be stale
  after Prepare Images rescaled the image and was closed (0.1 → 0.2, then 0.3
  gave 60 µm instead of 30). The stored-scale denominator fixes it; regression
  test `test_measure_tab_after_prepare_images_was_closed_without_applying`.
- Fixed: the helper could mark an observation dirty when nothing changed.

Pre-existing follow-ups, status (2026-11-21, `fix/rescale-follow-ups`):

1. Fixed: Prepare Images no longer divides a scale-bar scale by the image's
   import resample factor (the preview shows the stored image, so a bar is
   already per stored pixel; objective scales are still divided).
   `_stored_pixel_scale_for_result` follows the apply step's rule, and
   `_collect_rescale_targets` compares against the stored image scale.
2. Fixed on `fix/rescale-prompt-all-selected` (PR #81, a visible dialog change
   that waits for the owner's opinion): the multi-image prompt is now the
   single-image OK/Cancel prompt and OK rescales every affected image. Save also
   reads the current stored scale instead of its pre-dialog snapshot when the
   objective is unchanged, so it no longer overwrites a scale the dialog stored.
3. Fixed: resampling or cropping now transforms multiline JSON paths with the
   point columns (`MeasurementDB.scale_measurement_points` /
   `translate_measurement_points`), each coordinate on its own, behind a write
   lock; malformed paths are left unchanged.

Also fixed on the same branch, found while fixing those:

- Editing the scale bar in the Measure tab had its own rescale loop (notes
  rewritten to `Q=…`, multiline paths destroyed, case-sensitive filter, not
  marked dirty); it now uses `rescale_image_measurements`.
- Moving a scale bar's endpoints in the Measure tab changed its labelled
  length (the preview computes µm from the current scale), so the
  "rescale other measurements?" prompt could never appear. The label is now
  kept and the image scale changes.
- Choosing an objective in the Measure tab ignored the image's import
  resample factor (Prepare Images and image loading already applied it).

Still open (Codex review, 2026-11-21; not reproduced in the app):

4. A legacy image with no stored resample factor: Prepare Images may infer
   one, apply an objective divided by it, and the apply step (which reads the
   original row) rescales back. Switching away from and back to an objective
   whose calibration changed mid-dialog can also be undone on Save.
5. Some scale-bar interactions can record preview-pixel coordinates: a bar
   drawn while a first resize is pending, an endpoint dragged on a cached
   half-size preview of a stored image, or a processed RAW preview after a RAW
   settings edit.
6. `on_dimensions_changed` does not itself mark the observation dirty after an
   edited measurement; it relies on the spore-summary update.
7. After an objective change in the Measure tab, an open measurement preview
   keeps its old µm/px, so dragging it again saves a length at the old scale.
8. A scale-bar edit in the Measure tab updates the calibration measurement but
   not `images.scale_bar_x1..y2`, which Prepare Images restores; reapplying
   that older selection there reverts the scale. ("Set from scalebar" redraws
   instead and takes the length from its input, which may hold a default.)
9. Cancelling the rescale prompt with a mixed-objective selection restores a
   single objective in the selector, so the next unrelated edit can assign
   that objective to every selected image.

Also fixed after the second review: the Measure tab compares an objective
change against the stored image scale (its cached scale could be stale); the
scale-bar rescale refreshes the image overlay; the saved scale-bar width
follows its cross tick; an out-of-range path coordinate no longer aborts the
transform.

## Sequencing

Do this before redesign Stage 9 retires `ImageImportDialog`; that stage then
deletes a caller instead of carrying a third copy forward. Stage 0 and 1 are
safe to start now.
