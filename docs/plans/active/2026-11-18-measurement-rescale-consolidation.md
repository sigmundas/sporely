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
(real SQLite; UI methods called unbound with a minimal `self`).

- **Dirty marking, normal flows: fine.** `ImageDB.update_image` and
  `MeasurementDB.add_measurement` both mark the observation dirty
  (`_touch_observation`). The main_window rescale is always followed by
  `update_image` in `apply_objective` / `set_custom_scale`; the
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
- `CalibrationDB.recalculate_measurements_for_objective` had no callers; it is
  deleted by the stale-code sweep, so four rescale paths remain.

## Stage 1 — One transactional calibration upsert

- Add `MeasurementDB.replace_calibration_measurement(...)` taking plain numbers
  (no `QPointF`), doing DELETE + INSERT in one transaction, with the
  case-insensitive predicate.
- Switch all three callers; keep each caller's current geometry until decision 1
  is made, via an explicit parameter.
- Verification: Stage 0 tests still pass, plus a failure-injection test that
  shows the old row survives a failed insert.

## Stage 2 — One rescale service with explicit options

- Add `MeasurementDB.rescale_image_measurements(image_id, ratio, *,
  include_calibration, rewrite_q_notes, update_image_scale, mark_dirty)`.
- Switch the three UI paths to it with options that reproduce today's
  behaviour exactly. Stage 0 tests must pass unchanged.

## Stage 3 — Unify semantics (only what the owner decided)

- Each changed default is its own commit with an updated test.

## Sequencing

Do this before redesign Stage 9 retires `ImageImportDialog`; that stage then
deletes a caller instead of carrying a third copy forward. Stage 0 and 1 are
safe to start now.
