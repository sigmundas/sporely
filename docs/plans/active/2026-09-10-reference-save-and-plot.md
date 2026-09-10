# Reference identity, saving, and plotting

## Current stage / handoff — 2026-09-10

Manual verification accepted by the user on 2026-09-10: “this works. commit”.
Automated validation passed as recorded below; authorized for commit and push.
Branch: `feature/reference-save-and-plot`.
Base HEAD: `40022707108410ca674b04fdcc5b8149c857d75e`.
Candidate SHA: recorded in the post-commit handoff update.
This is the canonical plan for the user's follow-up to the reference audit.

## Authorized scope

Fix the observation/reference taxon warning, legacy reference ellipses, and
failure propagation. Give manual reference entry separate Save to library and
Add to plot actions. One bounded stage, reusing existing persistence and attach
paths; no schema migration or new dependency.

## Implemented behavior and code locations

- `ui/add_reference_dialog.py`: constructor/footer and callback contract;
  `_update_footer_state`, `_on_save_to_library_clicked`, and
  `_on_add_to_plot_clicked` (~1035–1140). Save creates a library set, keeps the
  dialog open, locks the saved editor/target and disables repeated saves.
  Add to plot then attaches that same ID; a failed save/attachment stays open.
  Existing-set selection skips the save step. Publication is required to save.
- `ui/main_window.py`: `_on_add_reference_clicked` (~9823–9965) wires save-only
  and verified attachment callbacks. `_persist_normalized_reference_from_dialog`
  (~11480–11800) retains the observation identity drift check but excludes
  independent comparison targets from the old observation-name synonym guard.
  Save-only returns the new measurement set ID without plot refresh or use.
  `_submit_reference_editor_result` (~11898–12010) propagates success/failure.
  `_plot_reference_range_shape` (~17987–17996) always uses the existing bounded
  polygon helper, retaining Q constraints, rather than creating an ellipse.
  Obsolete reference shape selector rows are hidden (~7840, ~20884), while their
  compatibility settings widgets remain. Raw point statistical overlays unchanged.
- `database/reference_library.py`: `QuickAddReferenceResult.use` (~305) is
  optional for save-only results. `QuickAddReferenceService.create_in_library`
  and `_create` (~2166–2240) reuse validation, work/treatment resolution,
  measurement-set creation and reverse-order compensation; attachment is optional.
- Tests: `tests/test_add_reference_dialog.py`,
  `tests/test_reference_panel_taxon_drift_and_retry.py`,
  `tests/test_reference_quick_add_service.py`,
  `tests/test_reference_library_desktop_slice.py` cover separate save/attach,
  no duplicate save, failure propagation, independent known/unknown target IDs,
  unchanged observation identity, save-only persistence, and rectangular legacy
  drawing with a saved ellipse preference.
- `tools/review_ui/scenarios/references.py`: real manual editor fixtures now
  include a name and save callback; adds manual-saved scenarios in light/dark.
- `i18n/Sporely_{nb_NO,sv_SE,de_DE}.{ts,qm}`: refreshed/translated new strings.
  Generated TS location updates account for most translation diff lines.

Reused paths: `ReferenceEntryEditor.validate_and_build_result`,
`normalized_measurement_set_payload`, `quick_add_treatment_payload`,
`QuickAddReferenceService._resolve_work` / `_resolve_treatment`,
`MeasurementSetRepository.create`,
`_attach_normalized_reference_to_active_observation`,
`_measurement_set_is_attached_to_observation`, `_constrained_box_polygon`.

## Validation

- Project-venv py_compile passed for the three changed production modules and
  the screenshot scenario module.
- Focused pytest: 123 passed (9.36s final run): quick-add service, Add reference
  dialog, panel taxon drift/retry, and desktop reference library slice.
- `./tools/update_translations.sh` passed: 2332 finished, 0 unfinished in each
  of Norwegian Bokmål, Swedish and German.
- `git diff --check` passed.
- Deterministic screenshot manifest:
  `/tmp/sporely-reference-save-final/manifest.json`.
  Inspected manual-range and manual-saved PNGs: both action labels and footer
  instructions are readable, with appropriate enabled/disabled appearance.
  The analysis-panel fixture confirms the reference shape selector is absent;
  its narrow pre-existing mixed-panel layout has clipping in unrelated controls.
  Screenshots prove layout only; no live production database or UI was exercised.

## Manual verification — user confirmed

1. Open an observation, Add reference, choose a different comparison species,
   select a publication, and enter a range. Save to library: no observation-name
   warning, observation taxon unchanged, no new plot row, dialog stays open.
2. Close without adding to plot. Reopen Add reference and find the saved set in
   Library. Attach it: one reference row and a rectangular range on the plot.
3. Repeat manual entry and save, then Add to plot directly. Exactly one set and
   one plot reference should result; saving again is disabled after success.
4. Check the existing fourth legacy range reference: it draws as a rectangle
   even with an old ellipse preference. Restart and verify the saved set and
   attached reference persist.

## Deviations and deferred work

No scope deviations. Saving locks the current form to prevent an edited preview
from being attached under an already-saved ID; reopen for another new set.
User confirmed the interaction checks and authorized commit. Independent
top-level review remains separate; no merge to main is authorized.
Unrelated pre-existing parser/editor test edits and the parser active plan were
preserved and are not part of this stage. No migration of existing user data.
